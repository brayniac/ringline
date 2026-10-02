use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::task::{RawWaker, RawWakerVTable, Waker};

// The waker's data is a `usize`: the task id in the low 32 bits and the
// owning worker's id in the high 32.
const _: () = assert!(
    std::mem::size_of::<usize>() >= 8,
    "ringline wakers need a 64-bit usize"
);

thread_local! {
    // Thread-local queue of connection indices whose tasks are ready to poll.
    // Wakers on the owning worker's thread push to this queue; the executor
    // drains it after each CQE batch.
    pub(crate) static READY_QUEUE: std::cell::RefCell<VecDeque<u32>> =
        const { std::cell::RefCell::new(VecDeque::new()) };
    // The id of the worker whose executor runs on this thread, or 0.
    static CURRENT_WORKER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Bit flag that distinguishes standalone tasks from connection tasks
/// in the ready queue. Connection indices use bits 0..23 (max 16M),
/// so bit 31 is always free for connection tasks.
pub(crate) const STANDALONE_BIT: u32 = 1 << 31;

/// Where a waker woken off its worker's thread delivers the task id. One per
/// executor, registered in `INBOXES` under the worker's id while the executor
/// lives.
pub(crate) struct Inbox {
    ids: Mutex<Vec<u32>>,
    /// Set after an id is pushed, so the executor checks the lock only when
    /// something is there.
    nonempty: AtomicBool,
    /// The worker's wake fd, written after a push so a worker blocked in its
    /// event loop wakes. Unset for an executor with no event loop (unit
    /// tests).
    wake: OnceLock<crate::wakeup::WakeHandle>,
}

/// Inboxes of live executors, by worker id.
fn inboxes() -> &'static RwLock<HashMap<u32, Arc<Inbox>>> {
    static INBOXES: OnceLock<RwLock<HashMap<u32, Arc<Inbox>>>> = OnceLock::new();
    INBOXES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// A worker's registration: its id, process-unique and never reused, and its
/// inbox. Registering makes the calling thread the worker's thread; dropping
/// the registration removes the inbox, so a later wake for this worker is
/// dropped.
pub(crate) struct WorkerWakes {
    id: u32,
    inbox: Arc<Inbox>,
}

impl WorkerWakes {
    pub(crate) fn register() -> Self {
        static NEXT_ID: AtomicU32 = AtomicU32::new(1);
        // Stops at the maximum rather than wrapping, so an id is never handed
        // out twice.
        let mut id = NEXT_ID.load(Ordering::Relaxed);
        loop {
            let next = id.checked_add(1).expect("worker ids exhausted");
            match NEXT_ID.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => id = current,
            }
        }
        let inbox = Arc::new(Inbox {
            ids: Mutex::new(Vec::new()),
            nonempty: AtomicBool::new(false),
            wake: OnceLock::new(),
        });
        inboxes()
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::clone(&inbox));
        CURRENT_WORKER.with(|c| c.set(id));
        WorkerWakes { id, inbox }
    }

    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    /// Give the inbox the worker's wake handle, so a cross-thread wake also
    /// wakes the event loop.
    pub(crate) fn set_wake_handle(&self, wake: crate::wakeup::WakeHandle) {
        let _ = self.inbox.wake.set(wake);
    }

    /// Move ids woken from other threads into `buf`.
    pub(crate) fn drain_into(&self, buf: &mut VecDeque<u32>) {
        if self.inbox.nonempty.swap(false, Ordering::Acquire) {
            let mut ids = self.inbox.ids.lock().unwrap_or_else(|e| e.into_inner());
            buf.extend(ids.drain(..));
        }
    }
}

impl Drop for WorkerWakes {
    fn drop(&mut self) {
        inboxes()
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        CURRENT_WORKER.with(|c| {
            if c.get() == self.id {
                c.set(0);
            }
        });
    }
}

/// Create a [`Waker`] for the given connection index on worker `owner`.
///
/// Zero allocation: the index and the worker id are packed into the raw
/// pointer. Woken on the owner's thread, it pushes the index onto the
/// thread-local `READY_QUEUE`; woken on any other thread, it delivers the
/// index to the owner's inbox and wakes the owner's event loop.
pub(crate) fn conn_waker(owner: u32, conn_index: u32) -> Waker {
    debug_assert!(
        conn_index & STANDALONE_BIT == 0,
        "conn_index has standalone bit set"
    );
    make_waker(owner, conn_index)
}

/// Create a [`Waker`] for a standalone task (not bound to a connection).
///
/// The `task_idx` is OR'd with `STANDALONE_BIT` so the executor can
/// distinguish it from connection wakeups.
pub(crate) fn standalone_waker(owner: u32, task_idx: u32) -> Waker {
    debug_assert!(
        task_idx & STANDALONE_BIT == 0,
        "task_idx already has standalone bit"
    );
    make_waker(owner, task_idx | STANDALONE_BIT)
}

fn make_waker(owner: u32, task_id: u32) -> Waker {
    let data = (((owner as usize) << 32) | task_id as usize) as *const ();
    // SAFETY: The vtable functions below follow the RawWaker contract.
    // The "data" is just a usize cast to a pointer — no heap allocation, no
    // lifetime concerns.
    unsafe { Waker::from_raw(RawWaker::new(data, &VTABLE)) }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(clone_fn, wake_fn, wake_by_ref_fn, drop_fn);

unsafe fn clone_fn(data: *const ()) -> RawWaker {
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake_fn(data: *const ()) {
    // SAFETY: wake_by_ref_fn is safe to call with data from our vtable.
    unsafe { wake_by_ref_fn(data) };
}

unsafe fn wake_by_ref_fn(data: *const ()) {
    let data = data as usize;
    let task_id = data as u32;
    let owner = (data >> 32) as u32;
    if CURRENT_WORKER.with(|c| c.get()) == owner {
        READY_QUEUE.with(|q| {
            q.borrow_mut().push_back(task_id);
        });
    } else {
        wake_remote(owner, task_id);
    }
}

/// Deliver a wake from a thread other than the owning worker's. A worker that
/// has exited has no inbox, and the wake is dropped: its tasks are gone.
#[cold]
fn wake_remote(owner: u32, task_id: u32) {
    let inbox = inboxes()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&owner)
        .cloned();
    let Some(inbox) = inbox else {
        return;
    };
    inbox
        .ids
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(task_id);
    // After the push, so the executor that sees the flag finds the id. Only
    // the wake that sets the flag writes the fd: a flag already set means an
    // earlier wake has written it, or is about to, and the executor drains
    // the inbox only after clearing the flag.
    if !inbox.nonempty.swap(true, Ordering::AcqRel)
        && let Some(wake) = inbox.wake.get()
    {
        wake.wake();
    }
}

unsafe fn drop_fn(_data: *const ()) {
    // No resources to free — data is just a usize.
}

/// Drain the thread-local ready queue into the provided buffer.
pub(crate) fn drain_ready_queue(buf: &mut VecDeque<u32>) {
    READY_QUEUE.with(|q| {
        buf.append(&mut q.borrow_mut());
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waker_pushes_to_ready_queue() {
        // Clear any leftover state.
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let wakes = WorkerWakes::register();
        let waker = conn_waker(wakes.id(), 42);
        waker.wake_by_ref();
        waker.wake_by_ref();

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 42);
        assert_eq!(buf[1], 42);
    }

    #[test]
    fn waker_clone_works() {
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let wakes = WorkerWakes::register();
        let waker = conn_waker(wakes.id(), 7);
        let cloned = waker.clone();

        waker.wake_by_ref();
        cloned.wake();

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0], 7);
        assert_eq!(buf[1], 7);
    }

    #[test]
    fn drain_empty_queue() {
        READY_QUEUE.with(|q| q.borrow_mut().clear());

        let mut buf = VecDeque::new();
        drain_ready_queue(&mut buf);
        assert!(buf.is_empty());
    }
}
