use std::io;
use std::ptr;
use std::sync::atomic::{self, AtomicU16};

/// A ring-mapped provided buffer ring for multishot recv operations.
///
/// The kernel picks a buffer from this ring at completion time.
/// We replenish buffers after processing to keep the ring full.
pub struct ProvidedBufRing {
    /// Pointer to the mmap'd ring (shared with kernel).
    ring_ptr: *mut u8,
    /// Size of the mmap'd ring region.
    ring_mmap_len: usize,
    /// Backing memory for all buffers.
    buf_backing: Vec<u8>,
    /// Buffer group ID.
    bgid: u16,
    /// Number of buffers (must be power of 2).
    ring_size: u16,
    /// Size of each buffer.
    buf_size: u32,
    /// Current tail index (we write, kernel reads).
    tail: u16,
    /// Mask for ring index wrapping.
    mask: u16,
    /// Buffers out of the ring: exhausted by the kernel and not yet
    /// returned. `ring_size - outstanding` buffers are free in the ring for
    /// the kernel to pick. Maintained by `complete` and `release_batch`, or
    /// by `on_handout` and `replenish_batch` for a ring that does not track
    /// buffer state (UDP). Backpressure decisions read `free()`.
    outstanding: u32,
    /// Whether the ring is registered with `IOU_PBUF_RING_INC`. On a plain
    /// ring every completion exhausts its buffer.
    incremental: bool,
    /// Per-buffer state, indexed by bid; see `complete` and `release_batch`.
    state: Vec<BufState>,
}

/// What the driver knows about one buffer since it was last posted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BufState {
    /// Bytes the kernel has written into the buffer.
    written: u32,
    /// Completions whose data is still in use: each completion takes one,
    /// and each release drops one.
    holds: u32,
    /// The kernel is done with the buffer.
    exhausted: bool,
}

/// An io_uring buf_ring entry (matches kernel struct io_uring_buf).
#[repr(C)]
struct BufRingEntry {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

impl ProvidedBufRing {
    /// Size of a single ring entry.
    const ENTRY_SIZE: usize = std::mem::size_of::<BufRingEntry>();

    /// Create a new provided buffer ring.
    ///
    /// `ring_size` must be a power of 2.
    /// The ring memory is mmap'd so the kernel can access it directly.
    pub fn new(bgid: u16, ring_size: u16, buf_size: u32) -> io::Result<Self> {
        assert!(ring_size.is_power_of_two(), "ring_size must be power of 2");

        let ring_mmap_len = ring_size as usize * Self::ENTRY_SIZE;
        let buf_backing = vec![0u8; ring_size as usize * buf_size as usize];

        // mmap anonymous memory for the ring (page-aligned, shared with kernel)
        let ring_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                ring_mmap_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            )
        };
        if ring_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let mut ring = ProvidedBufRing {
            ring_ptr: ring_ptr as *mut u8,
            ring_mmap_len,
            buf_backing,
            bgid,
            ring_size,
            buf_size,
            tail: 0,
            mask: ring_size - 1,
            outstanding: 0,
            incremental: false,
            state: vec![BufState::default(); ring_size as usize],
        };

        // Pre-fill the ring with all buffers
        for i in 0..ring_size {
            ring.push_entry(i);
        }
        // Make entries visible to kernel
        ring.commit_tail();

        Ok(ring)
    }

    /// Fault every provided buffer in before the ring is armed.
    ///
    /// The recv path's first touch of a buffer is the *kernel* copying an skb
    /// into it, so an unfaulted page costs a minor fault on the completion
    /// path. See [`crate::buffer::prefault`]; this must run on the worker
    /// thread that owns the ring, which is where the driver is built.
    pub(crate) fn prefault(&mut self) {
        crate::buffer::prefault::prefault(&mut self.buf_backing);
    }

    /// Get the ring pointer for `register_buf_ring()`.
    pub fn ring_addr(&self) -> u64 {
        self.ring_ptr as u64
    }

    /// Get the buffer group ID.
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// Get the ring size (number of entries).
    pub fn ring_entries(&self) -> u32 {
        self.ring_size as u32
    }

    /// Record that the kernel handed one buffer to a completion (consumed one
    /// from the ring), for a ring that does not track buffer state (UDP).
    /// Call exactly once per recv completion that selected a buffer, before it
    /// is processed, and balance it with `replenish_batch`. The TCP ring uses
    /// `complete` and `release_batch` instead.
    #[inline]
    pub fn on_handout(&mut self) {
        self.outstanding += 1;
        debug_assert!(
            self.outstanding <= self.ring_size as u32,
            "provided-ring handout exceeds ring size ({} > {})",
            self.outstanding,
            self.ring_size
        );
    }

    /// Record a receive completion that selected buffer `bid` and delivered
    /// `res` bytes, and take a hold on it for that completion. Returns the
    /// offset of the completion's data in the buffer. `buf_more` is the
    /// completion's `IORING_CQE_F_BUF_MORE`; on a plain ring every completion
    /// exhausts its buffer.
    ///
    /// Call once per completion that carries `IORING_CQE_F_BUFFER`, the
    /// stale-CQE early returns included, before its data is read, and release
    /// the hold through `release_batch`.
    ///
    /// # Panics
    /// If the buffer is already exhausted, or if `written` disagrees with
    /// `buf_more` on an incremental ring.
    pub(crate) fn complete(&mut self, bid: u16, res: u32, buf_more: bool) -> u32 {
        let buf_size = self.buf_size;
        let s = &mut self.state[bid as usize];
        assert!(!s.exhausted, "completion on exhausted buffer {bid}");
        let offset = s.written;
        s.written += res;
        s.holds += 1;
        s.exhausted = !self.incremental || !buf_more;
        if self.incremental {
            assert_eq!(
                s.written == buf_size,
                s.exhausted,
                "buffer {bid}: {} of {buf_size} bytes written, F_BUF_MORE {buf_more}",
                s.written
            );
        }
        if s.exhausted {
            self.outstanding += 1;
            debug_assert!(self.outstanding <= self.ring_size as u32);
        }
        offset
    }

    /// Drop one completion's hold on each bid in `bids`, and post every
    /// buffer that is then exhausted with no holds back to the ring, whole.
    /// A bid may appear more than once, once per hold it drops.
    ///
    /// # Panics
    /// If a bid has no hold to drop: it was released more times than it
    /// completed. A duplicate release that arrives after the buffer was
    /// posted and completed again takes that completion's hold and is not
    /// detected.
    pub(crate) fn release_batch(&mut self, bids: &[u16]) {
        let mut posted = false;
        for &bid in bids {
            let s = &mut self.state[bid as usize];
            assert!(s.holds > 0, "release of buffer {bid}, which has no hold");
            s.holds -= 1;
            if s.exhausted && s.holds == 0 {
                *s = BufState::default();
                self.outstanding -= 1;
                self.push_entry(bid);
                posted = true;
            }
        }
        if posted {
            self.commit_tail();
        }
    }

    /// Test-only: treat the ring as incremental, so driver tests can deliver
    /// data at nonzero offsets.
    #[cfg(test)]
    pub(crate) fn set_incremental_for_test(&mut self) {
        self.incremental = true;
    }

    /// Buffers currently available in the ring for the kernel to select.
    ///
    /// Consumed by the segmented-recv low-water reserve (the hold branch in
    /// `handle_recv_multi` via `recv::occupancy::delivery_decision`).
    #[inline]
    pub fn free(&self) -> u32 {
        self.ring_entries().saturating_sub(self.outstanding)
    }

    /// Test-only: the `(addr, len, bid)` of ring entry `index`, as the
    /// kernel left it. An incremental ring's entry advances in place.
    #[cfg(test)]
    pub(crate) fn entry(&self, index: u16) -> (u64, u32, u16) {
        let off = (index & self.mask) as usize * Self::ENTRY_SIZE;
        // Safety: `off` is inside the mapped ring; the kernel writes the
        // entry, so it is read volatile.
        let e = unsafe { ptr::read_volatile(self.ring_ptr.add(off) as *const BufRingEntry) };
        (e.addr, e.len, e.bid)
    }

    /// Address of the byte at `off` in buffer `bid`: the data of a completion
    /// that `complete` placed at that offset.
    pub(crate) fn data_ptr(&self, bid: u16, off: u32) -> *const u8 {
        debug_assert!(off <= self.buf_size);
        // Safety: `off` is within buffer `bid`, which is inside `buf_backing`.
        unsafe { self.get_buffer(bid).0.add(off as usize) }
    }

    /// Get a pointer and length for a buffer by its ID.
    pub fn get_buffer(&self, bid: u16) -> (*const u8, u32) {
        let offset = bid as usize * self.buf_size as usize;
        let ptr = unsafe { self.buf_backing.as_ptr().add(offset) };
        (ptr, self.buf_size)
    }

    /// Batch replenish multiple buffers counted out by `on_handout`. Returns
    /// them to the ring and accounts them against `outstanding`.
    pub fn replenish_batch(&mut self, bids: &[u16]) {
        for &bid in bids {
            self.push_entry(bid);
        }
        if !bids.is_empty() {
            // Tripwire: replenishing more buffers than are outstanding means a bid
            // was returned to the ring more than once (a double-replenish) — the
            // recurring failure mode for the zero-copy hold/segment/forward paths.
            // `saturating_sub` keeps `free()` sane in release; this catches the bug
            // in debug/tests before it can silently corrupt ring accounting.
            debug_assert!(
                self.outstanding >= bids.len() as u32,
                "double-replenish: returning {} buffers but only {} outstanding",
                bids.len(),
                self.outstanding,
            );
            self.outstanding = self.outstanding.saturating_sub(bids.len() as u32);
            self.commit_tail();
        }
    }

    fn push_entry(&mut self, bid: u16) {
        let ring_idx = (self.tail & self.mask) as usize;
        let entry_ptr = unsafe {
            self.ring_ptr
                .add(ring_idx * Self::ENTRY_SIZE)
                .cast::<BufRingEntry>()
        };
        let buf_offset = bid as usize * self.buf_size as usize;
        let buf_addr = unsafe { self.buf_backing.as_ptr().add(buf_offset) };
        unsafe {
            ptr::write(
                entry_ptr,
                BufRingEntry {
                    addr: buf_addr as u64,
                    len: self.buf_size,
                    bid,
                    resv: 0,
                },
            );
        }
        self.tail = self.tail.wrapping_add(1);
    }

    fn commit_tail(&self) {
        // The tail is at offset 14 within the ring header. The kernel overlays
        // the header with bufs[0]: struct io_uring_buf_ring { union {
        //   struct { u64 resv1; u32 resv2; u16 resv3; u16 tail; };
        //   struct io_uring_buf bufs[0]; }; };
        // io_uring_buf: { u64 addr(0); u32 len(8); u16 bid(12); u16 resv(14); }
        // So tail = bufs[0].resv = offset 14.
        let tail_ptr = unsafe { self.ring_ptr.add(14).cast::<AtomicU16>() };
        unsafe {
            (*tail_ptr).store(self.tail, atomic::Ordering::Release);
        }
    }
}

impl Drop for ProvidedBufRing {
    fn drop(&mut self) {
        if !self.ring_ptr.is_null() {
            unsafe {
                libc::munmap(self.ring_ptr as *mut _, self.ring_mmap_len);
            }
        }
    }
}

// Safety: The ring is only accessed from a single worker thread.
unsafe impl Send for ProvidedBufRing {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn occupancy_tracks_handout_and_replenish() {
        let mut ring = ProvidedBufRing::new(0, 8, 4096).expect("ring");
        // All 8 buffers start free (pre-filled), none outstanding.
        assert_eq!(ring.ring_entries(), 8);
        assert_eq!(ring.free(), 8);

        // Hand out 3.
        for _ in 0..3 {
            ring.on_handout();
        }
        assert_eq!(ring.free(), 5);

        // Replenish 2 (bids are arbitrary here; accounting is by count).
        ring.replenish_batch(&[0, 1]);
        assert_eq!(ring.free(), 7);

        // Replenish the last outstanding one.
        ring.replenish_batch(&[2]);
        assert_eq!(ring.free(), 8);
    }

    /// Replenishing more than are outstanding is a double-replenish; the debug
    /// tripwire fires in debug builds (where tests normally run).
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "double-replenish")]
    fn double_replenish_trips_debug_assert() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        // Nothing outstanding → returning 3 buffers is a double-replenish.
        ring.replenish_batch(&[0, 1, 2]);
    }

    /// In release builds (tripwire compiled out) the `saturating_sub` still keeps
    /// `free()` from underflowing.
    #[test]
    #[cfg(not(debug_assertions))]
    fn replenish_saturates_never_underflows() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        ring.replenish_batch(&[0, 1, 2]);
        assert_eq!(ring.free(), 4);
        assert_eq!(ring.ring_entries(), 4);
    }

    /// A plain ring: each completion exhausts its buffer at offset 0, and the
    /// release returns it.
    #[test]
    fn a_plain_completion_returns_on_release() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        assert_eq!(ring.complete(2, 100, true), 0);
        assert_eq!(ring.free(), 3);
        ring.release_batch(&[2]);
        assert_eq!(ring.free(), 4);
        assert_eq!(ring.state[2], BufState::default());
        // Posted again, it can complete again.
        assert_eq!(ring.complete(2, 50, false), 0);
    }

    /// An incremental ring: completions append at increasing offsets, and the
    /// buffer returns only once it is exhausted and every hold is released.
    #[test]
    fn an_incremental_buffer_returns_when_exhausted_and_released() {
        let mut ring = ProvidedBufRing::new(0, 4, 100).expect("ring");
        ring.incremental = true;
        assert_eq!(ring.complete(1, 30, true), 0);
        assert_eq!(ring.complete(1, 30, true), 30);
        ring.release_batch(&[1, 1]);
        assert_eq!(ring.free(), 4, "partly used: still in the ring");
        assert_eq!(ring.complete(1, 40, false), 60);
        assert_eq!(ring.free(), 3);
        ring.release_batch(&[1]);
        assert_eq!(ring.free(), 4);
        assert_eq!(ring.state[1], BufState::default());
    }

    #[test]
    fn an_exhausted_buffer_waits_for_its_last_hold() {
        let mut ring = ProvidedBufRing::new(0, 4, 100).expect("ring");
        ring.incremental = true;
        ring.complete(0, 60, true);
        ring.complete(0, 40, false);
        ring.release_batch(&[0]);
        assert_eq!(ring.free(), 3);
        ring.release_batch(&[0]);
        assert_eq!(ring.free(), 4);
    }

    #[test]
    #[should_panic(expected = "release of buffer 3, which has no hold")]
    fn a_second_release_panics() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        ring.complete(3, 10, false);
        ring.release_batch(&[3, 3]);
    }

    #[test]
    #[should_panic(expected = "completion on exhausted buffer 1")]
    fn a_completion_on_an_exhausted_buffer_panics() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        ring.complete(1, 10, false);
        ring.complete(1, 10, false);
    }

    #[test]
    #[should_panic(expected = "buffer 0: 50 of 100 bytes written, F_BUF_MORE false")]
    fn an_incremental_buffer_exhausted_early_panics() {
        let mut ring = ProvidedBufRing::new(0, 4, 100).expect("ring");
        ring.incremental = true;
        ring.complete(0, 50, false);
    }

    #[test]
    fn free_reaches_zero_when_fully_drained() {
        let mut ring = ProvidedBufRing::new(0, 4, 4096).expect("ring");
        for _ in 0..4 {
            ring.on_handout();
        }
        assert_eq!(ring.free(), 0);
    }
}
