//! Memory, provided-buffer rings, receive regions and message framing,
//! shared by the strategies.

use std::sync::atomic::{AtomicU16, Ordering};

pub const PAGE: usize = 4096;
/// `IOU_PBUF_RING_INC`.
pub const PBUF_RING_INC: u16 = 2;

pub fn mmap_anon(bytes: usize) -> *mut u8 {
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            bytes.max(PAGE),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(p, libc::MAP_FAILED, "mmap {bytes} bytes");
    p as *mut u8
}

pub fn munmap(p: *mut u8, bytes: usize) {
    unsafe { libc::munmap(p as *mut libc::c_void, bytes.max(PAGE)) };
}

/// One `io_uring_buf` entry.
#[repr(C)]
pub struct BufEntry {
    pub addr: u64,
    pub len: u32,
    pub bid: u16,
    pub resv: u16,
}

/// A provided-buffer ring in userspace memory: a power-of-two number of
/// entries at `base` (page-aligned), with the tail at offset 14.
pub struct BufRing {
    pub base: *mut u8,
    pub mask: u16,
    pub tail: u16,
}

impl BufRing {
    pub fn new(base: *mut u8, entries: u16) -> Self {
        assert!(entries.is_power_of_two());
        BufRing { base, mask: entries - 1, tail: 0 }
    }

    pub fn entry(&self, i: u16) -> *mut BufEntry {
        unsafe { (self.base as *mut BufEntry).add((i & self.mask) as usize) }
    }

    /// The most recently posted entry.
    pub fn last(&self) -> *mut BufEntry {
        self.entry(self.tail.wrapping_sub(1))
    }

    /// Post one buffer and publish it to the kernel.
    pub fn push(&mut self, addr: u64, len: u32, bid: u16) {
        unsafe {
            let e = self.entry(self.tail);
            (*e).addr = addr;
            (*e).len = len;
            (*e).bid = bid;
        }
        self.tail = self.tail.wrapping_add(1);
        unsafe { (*(self.base.add(14) as *const AtomicU16)).store(self.tail, Ordering::Release) };
    }
}

/// A connection's receive memory for the region strategies: unread bytes
/// are `[head, tail)`, free space `[tail, cap)`. The allocation moves only
/// when the strategy's rules allow it.
pub struct Region {
    pub base: *mut u8,
    pub cap: usize,
    pub head: usize,
    pub tail: usize,
    /// Bytes copied by compaction and growth, for the report.
    pub copied: u64,
}

impl Region {
    pub fn new(cap: usize) -> Self {
        Region { base: mmap_anon(cap), cap, head: 0, tail: 0, copied: 0 }
    }

    pub fn unread(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.base.add(self.head), self.tail - self.head) }
    }

    pub fn spare_ptr(&self, at: usize) -> *mut u8 {
        unsafe { self.base.add(at) }
    }

    /// Move the unread bytes, through `upto` (the kernel's write position,
    /// at or past `tail`), to the front of an allocation of `new_cap`
    /// bytes: the same one when `new_cap == cap`, a new one otherwise.
    /// `head` and `tail` keep their meaning, shifted to the front.
    pub fn relocate(&mut self, new_cap: usize, upto: usize) {
        debug_assert!(upto >= self.tail && upto - self.head <= new_cap);
        let live = upto - self.head;
        if new_cap == self.cap {
            if self.head > 0 && live > 0 {
                unsafe { std::ptr::copy(self.base.add(self.head), self.base, live) };
                self.copied += live as u64;
            }
        } else {
            let fresh = mmap_anon(new_cap);
            if live > 0 {
                unsafe { std::ptr::copy_nonoverlapping(self.base.add(self.head), fresh, live) };
                self.copied += live as u64;
            }
            munmap(self.base, self.cap);
            self.base = fresh;
            self.cap = new_cap;
        }
        self.tail -= self.head;
        self.head = 0;
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        munmap(self.base, self.cap);
    }
}

/// Outcome of parsing a connection's unread bytes.
pub struct Parsed {
    /// Bytes consumed by whole messages.
    pub consumed: usize,
    /// Whole messages consumed.
    pub msgs: u32,
    /// Total size (header + payload) of the next, incomplete message, if
    /// its header has arrived; otherwise the header size.
    pub need: usize,
}

/// The payload byte at index `i` (after the sequence number) of message
/// `seq`, in `--verify` runs.
pub fn pattern(seq: u32, i: usize) -> u8 {
    (seq as u8).wrapping_mul(31).wrapping_add(i as u8).wrapping_add((i >> 8) as u8)
}

/// Lengths above this are corruption, not a message (the largest the
/// client sends is 1 MiB).
pub const MAX_MSG: usize = 64 << 20;

/// Check every message in `data` (whole messages only) against the
/// `--verify` pattern, advancing `expect`. Returns the bad messages.
pub fn verify(data: &[u8], expect: &mut u32) -> u64 {
    let mut bad = 0;
    let mut off = 0;
    while data.len() - off >= 4 {
        let len = u32::from_le_bytes(data[off..off + 4].try_into().unwrap()) as usize;
        let payload = &data[off + 4..off + 4 + len];
        if len >= 4 {
            let seq = u32::from_le_bytes(payload[..4].try_into().unwrap());
            let ok = seq == *expect && payload[4..].iter().enumerate().all(|(i, &b)| b == pattern(seq, i));
            if !ok {
                bad += 1;
            }
            *expect = seq.wrapping_add(1);
        }
        off += 4 + len;
    }
    bad
}

/// Parse length-prefixed messages (`u32` little-endian payload length,
/// then the payload) from `data`. Reads the last payload byte of each
/// message, so delivered memory is touched as an application would.
pub fn parse(data: &[u8], touch: &mut u64) -> Parsed {
    let mut off = 0;
    let mut msgs = 0;
    loop {
        let rest = &data[off..];
        if rest.len() < 4 {
            return Parsed { consumed: off, msgs, need: 4 };
        }
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let total = 4 + len;
        if rest.len() < total {
            return Parsed { consumed: off, msgs, need: total };
        }
        if len > 0 {
            *touch = touch.wrapping_add(rest[total - 1] as u64);
        }
        off += total;
        msgs += 1;
    }
}
