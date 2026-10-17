//! `Bytes` views of TCP receive ring buffers.
//!
//! A view is a [`Bytes`] whose owner is a [`RecvView`]: a range of one
//! provided buffer, kept alive by an `Arc` of the ring's buffer memory. A view
//! takes one hold on its buffer when it is created, so the buffer does not
//! return to the ring while the view or any clone or slice of it is alive.
//! The hold is released when the last of them drops; see [`release_view`].
//! Design: `docs/recv-segment-views-design.md`, "Views".
//!
//! Only the driver's tests create views until a reader hands them out
//! (landing step 2 of that design).
#![cfg_attr(not(test), allow(dead_code))]

use std::io;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

/// The memory behind a provided-buffer ring's buffers.
///
/// The ring and every view of its buffers hold it through an `Arc`, and the
/// memory is freed when the last of them drops. It is reached only through raw
/// pointers: the kernel writes to buffers in the ring, and under
/// `IOU_PBUF_RING_INC` to later bytes of a buffer while views of its earlier
/// bytes are alive.
pub(crate) struct RingMemory {
    ptr: NonNull<u8>,
    len: usize,
    /// Process-unique, never reused. Releases name the memory by this id, so
    /// a release meant for a freed allocation cannot match a later one at the
    /// same address.
    id: u64,
}

// Safety: `RingMemory` owns its allocation and exposes it as raw pointers,
// and as a slice only through `&mut self`. The code that reads through the
// pointers (views, the receive path) reads ranges the kernel has finished
// writing.
unsafe impl Send for RingMemory {}
unsafe impl Sync for RingMemory {}

impl RingMemory {
    /// Map `len` zeroed, page-aligned bytes. An anonymous mapping, so a page
    /// is backed only once it is touched: the kernel's first write into a
    /// buffer, or `prefault`.
    pub(crate) fn new(len: usize) -> io::Result<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let len = len.max(1);
        // Safety: an anonymous private mapping with no address hint.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let ptr = NonNull::new(ptr.cast::<u8>()).expect("mmap returned null");
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Ok(RingMemory { ptr, len, id })
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The whole memory as a mutable slice. `&mut self` through
    /// `Arc::get_mut` proves no view exists; the caller must not have posted
    /// any buffer to a registered ring yet.
    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: the mapping is `len()` bytes, zeroed by the kernel, and
        // `&mut self` excludes every other reference to it.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len()) }
    }
}

impl Drop for RingMemory {
    fn drop(&mut self) {
        // Safety: mapped in `new` with this length.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// The owner of a view's [`Bytes`]: `len` bytes at `off` in ring memory
/// `mem`, inside buffer `bid`, created by worker `worker`.
pub(crate) struct RecvView {
    mem: Arc<RingMemory>,
    off: usize,
    len: usize,
    bid: u16,
    worker: u32,
}

impl AsRef<[u8]> for RecvView {
    fn as_ref(&self) -> &[u8] {
        // Safety: `[off, off + len)` lies in `mem` (checked in `view`) and
        // held completed bytes when the view was created (checked in
        // `ProvidedBufRing::add_hold`). The view's hold stops the buffer
        // from being posted again, and until then the kernel writes only
        // past the bytes it has completed, so nothing writes the range while
        // the view is alive.
        unsafe { std::slice::from_raw_parts(self.mem.as_ptr().add(self.off), self.len) }
    }
}

impl Drop for RecvView {
    fn drop(&mut self) {
        release_view(self.worker, self.mem.id(), self.bid);
    }
}

/// A view of `len` bytes at `ptr`, inside buffer `bid` of `mem`.
///
/// The caller has taken one hold on `bid` for the view
/// (`ProvidedBufRing::add_hold`); the view releases it when it and every
/// clone and slice of it have dropped. `worker` is the id of the worker whose
/// driver owns the ring (`Driver::worker_id`).
///
/// # Panics
/// If the range is not inside `mem`.
pub(crate) fn view(
    mem: &Arc<RingMemory>,
    ptr: *const u8,
    len: usize,
    bid: u16,
    worker: u32,
) -> Bytes {
    let off = (ptr as usize)
        .checked_sub(mem.as_ptr() as usize)
        .filter(|off| off + len <= mem.len())
        .expect("view outside its ring memory");
    crate::metrics::RECV_RING.increment(crate::metrics::recv_ring::VIEW_CREATED);
    Bytes::from_owner(RecvView {
        mem: Arc::clone(mem),
        off,
        len,
        bid,
        worker,
    })
}

/// Release a view's hold on buffer `bid` of the ring memory with id `alloc`.
///
/// On the thread of the worker that created the view, the release is queued
/// for `runtime::release_orphans`, so a view dropped while the driver is
/// borrowed (inside a parser callback or `with_state`) never takes it. On any
/// other thread it goes to that worker's inbox, which wakes the worker. A
/// worker that has exited has no inbox, and the release is dropped; its ring
/// is gone, and the memory is freed when the last view drops.
fn release_view(worker: u32, alloc: u64, bid: u16) {
    // `worker` is never 0 (`Driver::worker_id`); the check keeps a release
    // tagged 0 off the orphan queue of a thread with no worker.
    if worker != 0 && crate::runtime::waker::current_worker() == worker {
        crate::runtime::defer_release(crate::runtime::Orphan::RecvView { alloc, bid });
    } else {
        crate::runtime::waker::release_view_remote(worker, alloc, bid);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pages of `mem` that are resident, by `mincore`.
    fn resident_pages(mem: &RingMemory) -> usize {
        // Safety: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mut vec = vec![0u8; mem.len().div_ceil(page)];
        // Safety: the range is the mapping, and `vec` has one byte per page.
        let rc = unsafe { libc::mincore(mem.as_ptr().cast(), mem.len(), vec.as_mut_ptr()) };
        assert_eq!(rc, 0, "mincore: {}", io::Error::last_os_error());
        vec.iter().filter(|&&b| b & 1 != 0).count()
    }

    /// A new ring's memory is not resident until it is touched. Zeroing it
    /// eagerly would make every worker's whole ring resident at startup.
    #[test]
    fn new_ring_memory_is_not_resident_until_touched() {
        let mut mem = RingMemory::new(64 << 20).expect("ring memory");
        assert_eq!(resident_pages(&mem), 0);
        mem.as_mut_slice()[0] = 1;
        // One page, or one transparent huge page where THP is `always`.
        // Safety: sysconf has no preconditions.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let huge = std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(page);
        let resident = resident_pages(&mem);
        assert!(
            resident > 0 && resident <= huge.max(page) / page,
            "{resident} pages resident"
        );
    }
}
