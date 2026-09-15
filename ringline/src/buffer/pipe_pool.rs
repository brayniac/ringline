//! Per-worker pool of pipe pairs for splice-backed forwarding.
//!
//! A splice forward borrows a pair for its duration: bytes move
//! socket -> pipe -> sink without entering user space. A pair costs two
//! descriptors and a kernel pipe buffer (64 KiB by default), which is exactly
//! the cost that makes an unpooled splice proxy collapse at high connection
//! counts, so the pool is bounded and exhaustion is a normal, handled state —
//! the caller falls back to the copy-free-but-ring-backed `forward_to` path.
//!
//! See `docs/splice-forward-design.md`.

use std::os::fd::RawFd;

/// One end-pair of a pipe. Read end first, matching `pipe2(2)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PipePair {
    pub(crate) read: RawFd,
    pub(crate) write: RawFd,
}

pub(crate) struct PipePool {
    idle: Vec<PipePair>,
    /// Pairs created so far, borrowed or not. Never exceeds `capacity`.
    created: usize,
    capacity: usize,
}

impl PipePool {
    pub(crate) fn new(capacity: usize) -> Self {
        PipePool {
            idle: Vec::with_capacity(capacity.min(64)),
            created: 0,
            capacity,
        }
    }

    /// Borrow a pair, creating one on demand up to `capacity`.
    ///
    /// `None` means the pool is exhausted or the kernel refused a new pipe;
    /// both are handled the same way by callers (fall back to Mode A), so the
    /// distinction is deliberately not surfaced.
    pub(crate) fn acquire(&mut self) -> Option<PipePair> {
        if let Some(p) = self.idle.pop() {
            return Some(p);
        }
        if self.created >= self.capacity {
            return None;
        }
        let mut fds = [0 as libc::c_int; 2];
        // O_NONBLOCK so a splice into a full pipe reports EAGAIN instead of
        // parking a worker thread.
        let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
        if rc != 0 {
            return None;
        }
        self.created += 1;
        Some(PipePair {
            read: fds[0],
            write: fds[1],
        })
    }

    /// Return a pair that is known to be **empty**.
    ///
    /// A pair still holding bytes must go to [`discard`](Self::discard)
    /// instead: a leftover byte would prepend itself to whatever the next
    /// borrower forwards, silently corrupting that stream.
    pub(crate) fn release(&mut self, pair: PipePair) {
        self.idle.push(pair);
    }

    /// Close a pair instead of returning it, for the teardown path where the
    /// pipe may still hold bytes. The slot is freed so a later `acquire` can
    /// create a fresh one.
    pub(crate) fn discard(&mut self, pair: PipePair) {
        unsafe {
            libc::close(pair.read);
            libc::close(pair.write);
        }
        self.created -= 1;
    }

    #[cfg(test)]
    pub(crate) fn idle_len(&self) -> usize {
        self.idle.len()
    }
}

impl Drop for PipePool {
    fn drop(&mut self) {
        for p in self.idle.drain(..) {
            unsafe {
                libc::close(p.read);
                libc::close(p.write);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_creates_up_to_capacity_then_reports_exhaustion() {
        let mut pool = PipePool::new(2);
        let a = pool.acquire().expect("first pair");
        let b = pool.acquire().expect("second pair");
        assert_ne!(a.read, b.read);
        assert!(
            pool.acquire().is_none(),
            "third acquire must report exhaustion, not create a pair"
        );
        pool.release(a);
        assert_eq!(pool.idle_len(), 1);
        assert!(pool.acquire().is_some(), "a released pair is reusable");
    }

    #[test]
    fn discard_frees_the_slot_for_a_fresh_pair() {
        // Teardown with bytes still in the pipe closes it rather than
        // returning it; the capacity it occupied has to come back, or a proxy
        // that tears down mid-forward would leak its way to a dead pool.
        let mut pool = PipePool::new(1);
        let a = pool.acquire().expect("pair");
        pool.discard(a);
        assert!(
            pool.acquire().is_some(),
            "discard must free the slot it occupied"
        );
    }

    #[test]
    fn released_pairs_are_closed_on_drop() {
        let mut pool = PipePool::new(1);
        let p = pool.acquire().expect("pair");
        pool.release(p);
        drop(pool);
        // The fd is closed, so a second close must fail with EBADF.
        let rc = unsafe { libc::close(p.read) };
        assert_eq!(rc, -1, "pool drop should have closed the read end");
    }
}
