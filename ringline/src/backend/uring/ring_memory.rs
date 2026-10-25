//! The memory behind the TCP receive ring's buffers.

use std::io;
use std::ptr::NonNull;

/// The memory behind a provided-buffer ring's buffers.
///
/// The memory is freed when the ring drops. It is reached only through raw
/// pointers: the kernel writes to buffers in the ring, and under
/// `IOU_PBUF_RING_INC` to later bytes of a buffer while the driver reads
/// earlier bytes of it.
pub(crate) struct RingMemory {
    ptr: NonNull<u8>,
    len: usize,
}

impl RingMemory {
    /// Map `len` zeroed, page-aligned bytes. An anonymous mapping, so a page
    /// is backed only once it is touched: the kernel's first write into a
    /// buffer, or `prefault`.
    pub(crate) fn new(len: usize) -> io::Result<Self> {
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
        Ok(RingMemory { ptr, len })
    }

    pub(crate) fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The whole memory as a mutable slice. The caller must not have posted
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
