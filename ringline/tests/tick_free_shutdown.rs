//! With no tick timeout, a worker blocks until a completion arrives, so a
//! shutdown request must not be lost on the way to the blocking wait.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

struct ShutsDownAtStart;

impl AsyncEventHandler for ShutsDownAtStart {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            ringline::request_shutdown().ok();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ShutsDownAtStart
    }
}

/// `request_shutdown()` from a task ends its worker.
#[test]
fn request_shutdown_ends_a_worker_with_no_tick() {
    for round in 0..20 {
        let config = ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(64)
            .recv_buffer(16, 1024)
            .max_connections(16)
            .send_pool(16, 16384)
            .blocking_threads(0)
            .resolver_threads(0)
            .spawner_threads(0)
            .tick_timeout_us(0)
            .build()
            .expect("valid config");
        let (runtime, handles) = RinglineBuilder::new(config)
            .launch::<ShutsDownAtStart>()
            .expect("launch");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while handles.iter().any(|h| !h.is_finished()) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let exited = handles.iter().all(|h| h.is_finished());
        // Free a stuck worker before failing, so the test does not hang.
        runtime.shutdown();
        for h in handles {
            let _ = h.join();
        }
        assert!(
            exited,
            "round {round}: the worker did not exit after request_shutdown"
        );
    }
}

static IN_POLL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Sleeps inside its first poll, so a shutdown can arrive while the worker is
/// polling tasks, then stays pending.
struct BusyThenPending {
    polled: bool,
}

impl Future for BusyThenPending {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if !self.polled {
            self.polled = true;
            IN_POLL.store(true, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(100));
        }
        std::task::Poll::Pending
    }
}

struct BusyAtStart;

impl AsyncEventHandler for BusyAtStart {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(BusyThenPending { polled: false }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BusyAtStart
    }
}

/// `Runtime::shutdown` that arrives while the worker is polling a task ends it.
#[test]
fn runtime_shutdown_while_worker_busy_ends_it() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .tick_timeout_us(0)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<BusyAtStart>()
        .expect("launch");
    while !IN_POLL.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    runtime.shutdown();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while handles.iter().any(|h| !h.is_finished()) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let exited = handles.iter().all(|h| h.is_finished());
    // Leak a stuck worker rather than hang the test on join.
    if exited {
        for h in handles {
            let _ = h.join();
        }
    }
    assert!(
        exited,
        "the worker did not exit after Runtime::shutdown while busy"
    );
}
