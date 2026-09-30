//! Per-listener listen gates.
//!
//! A listener is bound when the runtime launches, which reserves its port, and
//! begins listening when its gate opens. Between the two the kernel does not
//! complete handshakes, so a readiness probe fails while the server is not
//! ready instead of passing against a socket nothing will service.
//!
//! What the peer sees in that window is platform-specific: Linux answers the
//! SYN with RST, so the connect is refused immediately, while macOS and the
//! BSDs drop it and the peer times out. Both fail a probe; only the first
//! fails it quickly.
//!
//! Gates are open by default: [`RinglineBuilder::defer_listen`] closes a gate
//! and [`begin_listening`] opens it. A listener that is not deferred is
//! listening before `launch()` returns.
//!
//! The gate defers `listen(2)`, not `accept(2)`. A socket that is listening
//! but not accepting still completes handshakes, so a TCP readiness probe
//! against it passes.
//!
//! [`RinglineBuilder::defer_listen`]: crate::RinglineBuilder::defer_listen

use std::cell::RefCell;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::handler::ListenerId;

/// The listen gates for one runtime, one entry per `bind*()` call.
pub(crate) struct ListenGates {
    /// Whether each listener has begun listening. Published separately from
    /// `inner` so a worker can read it on its hot path without the lock.
    open: Vec<AtomicBool>,
    inner: Mutex<GateInner>,
    /// Signalled when a gate opens and when the runtime shuts down. Pool-mode
    /// acceptor threads park here.
    cv: Condvar,
}

struct GateInner {
    /// The sockets to `listen(2)` when a gate opens: one per listener in pool
    /// mode, one per worker per listener in merged mode. Empty for a listener
    /// until `launch()` has bound it.
    fds: Vec<Vec<RawFd>>,
    /// Whether `launch()` has bound this listener and recorded its sockets.
    /// Tracked separately from `fds` being non-empty, so an empty socket list
    /// cannot be mistaken for a registered one.
    registered: Vec<bool>,
    /// Whether a handler asked for this listener before it was registered.
    /// `register` performs the listen for anything marked here.
    requested: Vec<bool>,
    /// Whether `listen(2)` has been called. `open` returns early when it is
    /// set. Distinct from `open`, which is the published view.
    listened: Vec<bool>,
    backlog: i32,
    shutdown: bool,
}

impl ListenGates {
    pub(crate) fn new(listeners: usize, backlog: i32) -> Arc<Self> {
        Arc::new(ListenGates {
            open: (0..listeners).map(|_| AtomicBool::new(false)).collect(),
            inner: Mutex::new(GateInner {
                fds: vec![Vec::new(); listeners],
                registered: vec![false; listeners],
                requested: vec![false; listeners],
                listened: vec![false; listeners],
                backlog,
                shutdown: false,
            }),
            cv: Condvar::new(),
        })
    }

    /// Record the bound sockets for a listener.
    ///
    /// If a handler already called [`open`](Self::open) for this listener,
    /// that call recorded its intent and this one performs the `listen(2)`,
    /// so the error surfaces to `launch()`.
    pub(crate) fn register(&self, listener: u32, fds: Vec<RawFd>) -> io::Result<()> {
        let idx = listener as usize;
        {
            let mut inner = self.lock();
            inner.fds[idx] = fds;
            inner.registered[idx] = true;
            if inner.shutdown || inner.listened[idx] || !inner.requested[idx] {
                return Ok(());
            }
            listen_all(&mut inner, idx)?;
        }
        self.publish(idx);
        Ok(())
    }

    /// Publish a gate and wake everything waiting on it. Called after the
    /// lock is released, so a waiter that checks on its own schedule still
    /// sees the flag.
    fn publish(&self, idx: usize) {
        self.open[idx].store(true, Ordering::Release);
        self.cv.notify_all();
    }

    /// Begin listening on a listener's sockets and publish the gate.
    ///
    /// A second call is a no-op. Returns the `listen(2)` error if the syscall
    /// fails, in which case the gate stays closed and the call can be
    /// retried.
    pub(crate) fn open(&self, listener: u32) -> io::Result<()> {
        let idx = listener as usize;
        {
            let mut inner = self.lock();
            if idx >= inner.listened.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("no listener with index {listener}"),
                ));
            }
            if inner.shutdown {
                return Err(io::Error::other("runtime is shutting down"));
            }
            if inner.listened[idx] {
                return Ok(());
            }
            if !inner.registered[idx] {
                // `launch()` has not bound this listener yet. A worker signals
                // startup and then enters its event loop, which polls
                // `on_start` on the first iteration, so a handler that
                // releases immediately gets here before `launch()` reaches the
                // listener loop. Recording the request and letting `register`
                // perform the listen keeps that ordering from mattering.
                // Publishing here instead would listen on nothing and leave
                // the acceptor to fail `accept4` with EINVAL and exit.
                inner.requested[idx] = true;
                return Ok(());
            }
            listen_all(&mut inner, idx)?;
        }
        self.publish(idx);
        Ok(())
    }

    /// Open every gate. Returns the first failure; gates opened before it
    /// stay open.
    pub(crate) fn open_all(&self) -> io::Result<()> {
        for listener in 0..self.open.len() as u32 {
            self.open(listener)?;
        }
        Ok(())
    }

    /// Whether this listener has begun listening. One atomic load.
    pub(crate) fn is_open(&self, listener: u32) -> bool {
        self.open
            .get(listener as usize)
            .map(|f| f.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    /// Block until this listener's gate opens, or the runtime shuts down.
    ///
    /// Returns `true` if the gate opened. A `false` return means shutdown, and
    /// the caller must not touch the listen fd — it may already be closed.
    pub(crate) fn wait_open(&self, listener: u32) -> bool {
        let idx = listener as usize;
        // The common case is a gate that was opened during `launch()`, which
        // costs one atomic load and no lock.
        if self.is_open(listener) {
            return true;
        }
        let mut inner = self.lock();
        loop {
            if inner.shutdown {
                return false;
            }
            if inner.listened.get(idx).copied().unwrap_or(false) {
                return true;
            }
            inner = match self.cv.wait(inner) {
                Ok(guard) => guard,
                // A poisoned mutex means a gate operation panicked. Waiting
                // forever would hang shutdown, so treat it as shutdown.
                Err(_) => return false,
            };
        }
    }

    /// Release every waiter.
    ///
    /// Called by `ShutdownHandle::shutdown` before it closes the listen fds.
    /// Closing an fd wakes a thread inside `accept4`; it does not wake a
    /// thread waiting in `wait_open`.
    pub(crate) fn shutdown(&self) {
        {
            let mut inner = self.lock();
            inner.shutdown = true;
        }
        self.cv.notify_all();
    }

    /// The lock, recovered from poisoning. A panic inside a gate operation
    /// must not make later calls panic; the shutdown path in particular must
    /// still run.
    fn lock(&self) -> std::sync::MutexGuard<'_, GateInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// `listen(2)` every socket of one listener, then mark it listened.
///
/// All of them listen or none does: a partial listen would leave a
/// merged-mode worker unserved with no later trigger, since a gate only rises
/// once.
fn listen_all(inner: &mut GateInner, idx: usize) -> io::Result<()> {
    let backlog = inner.backlog;
    for &fd in &inner.fds[idx] {
        if unsafe { libc::listen(fd, backlog) } < 0 {
            // Name the syscall. `bind(2)` and `listen(2)` both report
            // EADDRINUSE, and a launch failure that does not say which one it
            // came from does not say whether the port was taken before this
            // listener bound it or between its bind and its listen.
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("listen(2) on listener {idx}: {err}"),
            ));
        }
    }
    inner.listened[idx] = true;
    Ok(())
}

thread_local! {
    /// The gates for the runtime this thread belongs to. Installed by each
    /// worker at startup; `None` on any other thread, which
    /// [`begin_listening`] reports as an error.
    static GATES: RefCell<Option<Arc<ListenGates>>> = const { RefCell::new(None) };
}

/// Make the gates reachable from tasks on this worker.
pub(crate) fn install(gates: Arc<ListenGates>) {
    GATES.with(|g| *g.borrow_mut() = Some(gates));
}

fn with_gates<R>(f: impl FnOnce(&ListenGates) -> R) -> Option<R> {
    GATES.with(|g| g.borrow().as_ref().map(|gates| f(gates)))
}

/// Begin listening on a deferred listener.
///
/// Call this once the server can serve. Until it is called the listener's port
/// is bound but not listening, so a TCP readiness probe fails: on Linux the
/// peer is refused, on macOS and the BSDs it times out.
///
/// Calling it for a listener that is already listening returns `Ok(())` and
/// does nothing. A listener that was never deferred is already listening, so
/// passing the wrong index returns `Ok(())` and leaves the deferred listener
/// closed.
///
/// # Errors
///
/// Returns an error if called from a thread that is not a ringline worker, if
/// `listener` names no listener, if the runtime is shutting down, or if
/// `listen(2)` itself fails. A `listen(2)` failure leaves the gate closed, so
/// the call can be retried.
///
/// ```no_run
/// # use std::future::Future;
/// # use std::pin::Pin;
/// # use ringline::{AsyncEventHandler, Connection, ListenerId};
/// # struct Handler;
/// impl AsyncEventHandler for Handler {
///     # fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
///     #     async {}
///     # }
///     fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
///         Some(Box::pin(async {
///             // warm caches, connect to backends, load config
///             ringline::begin_listening(ListenerId::from_index(0)).expect("gate");
///         }))
///     }
///     # fn create_for_worker(_id: usize) -> Self {
///     #     Handler
///     # }
/// }
/// ```
pub fn begin_listening(listener: ListenerId) -> io::Result<()> {
    with_gates(|gates| gates.open(listener.index()))
        .unwrap_or_else(|| Err(io::Error::other("called outside a ringline worker")))
}

/// Begin listening on every deferred listener.
///
/// The [`begin_listening`] contract, applied to all of them. Returns the first
/// failure; listeners opened before it stay open.
///
/// # Errors
///
/// As [`begin_listening`].
pub fn begin_listening_all() -> io::Result<()> {
    with_gates(|gates| gates.open_all())
        .unwrap_or_else(|| Err(io::Error::other("called outside a ringline worker")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A socketpair fd stands in for a listener: `listen(2)` fails on it, so
    /// the bookkeeping tests can check that the gate does not publish when the
    /// syscall fails.
    fn dead_fd() -> RawFd {
        let mut fds = [0 as libc::c_int; 2];
        assert_eq!(
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) },
            0
        );
        unsafe { libc::close(fds[1]) };
        fds[0]
    }

    fn listenable_fd() -> RawFd {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        // Take the fd; the socket outlives the listener object.
        let fd = unsafe { libc::dup(std::os::fd::AsRawFd::as_raw_fd(&listener)) };
        assert!(fd >= 0);
        fd
    }

    #[test]
    fn a_gate_with_no_sockets_opens_and_publishes() {
        let gates = ListenGates::new(2, 128);
        assert!(!gates.is_open(0));
        gates.open(0).expect("open");
        assert!(gates.is_open(0));
        assert!(!gates.is_open(1), "opening one gate must not open another");
    }

    #[test]
    fn opening_twice_is_a_no_op() {
        let gates = ListenGates::new(1, 128);
        let fd = listenable_fd();
        gates.register(0, vec![fd]).expect("register");
        gates.open(0).expect("first open");
        // The second call must not reach `listen(2)` again — on Linux a second
        // listen on a listening socket succeeds, so the assertion that matters
        // is that the gate stays open and no error is produced.
        gates.open(0).expect("second open");
        assert!(gates.is_open(0));
        unsafe { libc::close(fd) };
    }

    #[test]
    fn a_failed_listen_leaves_the_gate_closed() {
        let gates = ListenGates::new(1, 128);
        let fd = dead_fd();
        gates.register(0, vec![fd]).expect("register");
        assert!(gates.open(0).is_err(), "listen on a socketpair must fail");
        assert!(
            !gates.is_open(0),
            "a gate must not publish when listen failed"
        );
        // Retryable: the bookkeeping did not record a listen that never happened.
        assert!(gates.open(0).is_err());
        unsafe { libc::close(fd) };
    }

    #[test]
    fn an_unknown_listener_is_an_error_not_a_panic() {
        let gates = ListenGates::new(1, 128);
        assert!(gates.open(7).is_err());
        assert!(!gates.is_open(7));
    }

    #[test]
    fn wait_returns_immediately_for_an_already_open_gate() {
        let gates = ListenGates::new(1, 128);
        gates.open(0).expect("open");
        assert!(gates.wait_open(0));
    }

    #[test]
    fn wait_wakes_when_the_gate_opens() {
        let gates = ListenGates::new(1, 128);
        let waiter = Arc::clone(&gates);
        let handle = std::thread::spawn(move || waiter.wait_open(0));
        // Not a synchronisation point; it makes the wait usually start
        // first. The test is correct either way: `wait_open` rechecks the
        // predicate before parking.
        std::thread::sleep(std::time::Duration::from_millis(20));
        gates.open(0).expect("open");
        assert!(handle.join().expect("waiter panicked"));
    }

    #[test]
    fn shutdown_releases_a_waiter_on_a_gate_that_never_opens() {
        let gates = ListenGates::new(1, 128);
        let waiter = Arc::clone(&gates);
        let handle = std::thread::spawn(move || waiter.wait_open(0));
        std::thread::sleep(std::time::Duration::from_millis(20));
        gates.shutdown();
        assert!(
            !handle.join().expect("waiter panicked"),
            "a waiter released by shutdown must report the gate as not open"
        );
    }

    #[test]
    fn opening_after_shutdown_is_refused() {
        let gates = ListenGates::new(1, 128);
        gates.shutdown();
        assert!(gates.open(0).is_err());
    }

    #[test]
    fn open_all_opens_every_gate() {
        let gates = ListenGates::new(3, 128);
        gates.open_all().expect("open_all");
        for id in 0..3 {
            assert!(gates.is_open(id));
        }
    }

    /// A handler can release a gate before `launch()` has bound the listener:
    /// a worker signals startup and then polls `on_start` on its first event
    /// loop iteration, while `launch()` is still on its way to `register`.
    ///
    /// Publishing there would listen on nothing, and the acceptor would fail
    /// `accept4` with EINVAL and exit, leaving the port bound and dead with
    /// `begin_listening` having returned `Ok`.
    #[test]
    fn a_gate_released_before_registration_listens_when_it_registers() {
        let gates = ListenGates::new(1, 128);

        gates.open(0).expect("release before register");
        assert!(
            !gates.is_open(0),
            "a gate with no sockets yet must not publish"
        );

        let fd = listenable_fd();
        gates.register(0, vec![fd]).expect("register");
        assert!(
            gates.is_open(0),
            "register must honour a release that arrived first"
        );
        unsafe { libc::close(fd) };
    }

    /// The same path, for a listener nobody asked for: registering must not
    /// open it.
    #[test]
    fn registering_does_not_open_a_gate_nobody_released() {
        let gates = ListenGates::new(1, 128);
        let fd = listenable_fd();
        gates.register(0, vec![fd]).expect("register");
        assert!(
            !gates.is_open(0),
            "register must not open an unreleased gate"
        );
        unsafe { libc::close(fd) };
    }

    #[test]
    fn begin_listening_outside_a_worker_is_an_error() {
        // No `install` on this thread.
        assert!(begin_listening(ListenerId::from_index(0)).is_err());
        assert!(begin_listening_all().is_err());
    }
}
