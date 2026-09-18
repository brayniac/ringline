//! Fault buffer pages in before traffic arrives.
//!
//! `vec![0u8; n]` goes through `alloc_zeroed`, which for a large allocation
//! hands back mapped-but-untouched pages: the mapping exists, but every page is
//! the shared zero page until something writes to it. The first write takes a
//! minor fault — and for a recv buffer that write is the *kernel* copying an
//! skb into it, so the fault lands on the completion path rather than at
//! startup.
//!
//! Prefaulting moves that cost to launch. What it buys is predictability, not
//! throughput:
//!
//! - **Latency.** Faults stop appearing in the ramp, where they show up as p99
//!   and p999 outliers rather than as a throughput number.
//! - **Honest memory.** RSS after startup equals RSS under load, so a
//!   configuration that asks for more memory than the machine has fails at
//!   launch instead of during a traffic burst. That matches what `launch()`
//!   already does for `RLIMIT_NOFILE` and `RLIMIT_MEMLOCK` — memory is the one
//!   resource currently committed lazily.
//! - **NUMA.** First touch decides the node. Ringline builds each worker's
//!   driver *after* `pin_to_core` (see `worker.rs`), so prefaulting from the
//!   constructor keeps every worker's buffers on its own node. Prefaulting from
//!   an unpinned launching thread would do the opposite — pull every worker's
//!   buffers onto one node — so this must stay on the worker thread.
//!
//! The cost is that over-provisioning stops being free. That is the point:
//! a ring sized `256 × 1 MiB` is 256 MiB per worker whether or not the workload
//! ever touches all of it, and an operator should find that out at startup.

/// Page size, or 4096 if the platform will not say.
///
/// A stride smaller than the true page size only does redundant writes; a
/// larger one would skip pages and silently prefault nothing. 4096 is the
/// smallest page on every platform ringline runs on, so the fallback is safe.
fn page_size() -> usize {
    // SAFETY: `sysconf` with a valid name has no preconditions.
    let n = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if n > 0 { n as usize } else { 4096 }
}

/// Touch every page of `buf` so its pages are resident and private.
///
/// Writes one zero byte per page. The buffer's contents are unchanged — the
/// pages are already zero — so this is safe to call on a live allocation, and
/// callers do it once at construction.
pub(crate) fn prefault(buf: &mut [u8]) {
    if buf.is_empty() {
        return;
    }
    let step = page_size();
    let ptr = buf.as_mut_ptr();
    let len = buf.len();
    let mut off = 0;
    while off < len {
        // SAFETY: `off < len`, so `ptr.add(off)` is within the allocation and
        // writable through the `&mut` borrow. The write is volatile because a
        // plain store of 0 to a zeroed page that nothing subsequently reads is
        // exactly the store an optimizer is entitled to delete — and deleting
        // it would leave the page untouched, which is the whole thing this
        // function exists to prevent.
        unsafe { std::ptr::write_volatile(ptr.add(off), 0) };
        off += step;
    }
    // The final page of a buffer whose length is not a multiple of the page
    // size is covered by the loop above only if it holds a stride boundary;
    // touch the last byte so a partial trailing page is not left cold.
    // SAFETY: `len > 0`, so `len - 1` is a valid offset.
    unsafe { std::ptr::write_volatile(ptr.add(len - 1), 0) };
}

/// Resident pages of a process, from `/proc/self/statm`.
///
/// Test-only: the point of prefaulting is a change in RSS, so the end-to-end
/// test has to observe RSS rather than trust that a flag reached a field.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn resident_bytes() -> usize {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let rss_pages: usize = statm
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    rss_pages * page_size()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pages are resident afterwards, and were not before.
    ///
    /// `mincore` answers exactly the question this module exists to change, so
    /// the test asserts on residency rather than on "it ran". The "before"
    /// half matters as much as the "after": without it, a platform that
    /// eagerly populated every mapping would pass while prefaulting did
    /// nothing at all.
    #[test]
    #[cfg(target_os = "linux")]
    fn prefault_makes_pages_resident() {
        let page = page_size();
        let pages = 512;
        let mut buf = vec![0u8; page * (pages + 1)];

        // mincore needs a page-aligned start; take the first aligned address
        // inside the allocation.
        let base = buf.as_mut_ptr();
        let aligned = (base as usize).div_ceil(page) * page;
        let ptr = aligned as *mut u8;
        let len = page * pages;

        let resident = |ptr: *mut u8, len: usize| -> usize {
            let mut vec = vec![0u8; len / page];
            // SAFETY: `ptr` is page-aligned and `len` bytes from it are inside
            // the live allocation; `vec` has one byte per page, as required.
            let rc = unsafe { libc::mincore(ptr as *mut libc::c_void, len, vec.as_mut_ptr()) };
            assert_eq!(rc, 0, "mincore failed: {}", std::io::Error::last_os_error());
            vec.iter().filter(|b| *b & 1 == 1).count()
        };

        let before = resident(ptr, len);
        assert!(
            before < pages / 2,
            "expected a mostly-cold allocation, found {before}/{pages} pages resident — \
             the test cannot show prefaulting does anything if the pages are already in"
        );

        // SAFETY: the slice covers `len` bytes from an aligned offset inside
        // the allocation, which outlives it.
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        prefault(slice);

        let after = resident(ptr, len);
        assert_eq!(
            after, pages,
            "every page must be resident after prefaulting; {after}/{pages} are"
        );
    }

    /// Prefaulting does not disturb the contents it faults in.
    #[test]
    fn prefault_leaves_the_buffer_zeroed() {
        let mut buf = vec![0u8; 1024 * 1024 + 7];
        prefault(&mut buf);
        assert!(buf.iter().all(|b| *b == 0));
    }

    /// A buffer shorter than a page, and an empty one, must not fault or
    /// over-run.
    #[test]
    fn prefault_handles_small_and_empty_buffers() {
        let mut empty: Vec<u8> = Vec::new();
        prefault(&mut empty);

        let mut tiny = vec![0u8; 3];
        prefault(&mut tiny);
        assert_eq!(tiny, [0, 0, 0]);
    }
}
