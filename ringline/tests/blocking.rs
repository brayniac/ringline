#![allow(clippy::manual_async_fn)]
//! Integration tests for spawn_blocking.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, Config, ConfigBuilder, Connection, RinglineBuilder};

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
        .resolver_threads(0)
        .spawner_threads(0)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

// ── Basic: returns a value ──────────────────────────────────────────

static BLOCKING_RESULT: AtomicU32 = AtomicU32::new(0);

struct BlockingBasicHandler;

impl AsyncEventHandler for BlockingBasicHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let val = ringline::spawn_blocking(|| 42u32)
                .unwrap()
                .await
                .expect("closure returned");
            BLOCKING_RESULT.store(val, Ordering::SeqCst);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockingBasicHandler
    }
}

#[test]
fn spawn_blocking_returns_value() {
    BLOCKING_RESULT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<BlockingBasicHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(BLOCKING_RESULT.load(Ordering::SeqCst), 42);
}

// ── Blocking work doesn't stall the worker ──────────────────────────

static BLOCKING_CONCURRENT: AtomicU32 = AtomicU32::new(0);

struct BlockingConcurrentHandler;

impl AsyncEventHandler for BlockingConcurrentHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            // Spawn a blocking task that sleeps.
            let handle = ringline::spawn_blocking(|| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                10u32
            })
            .unwrap();

            // Meanwhile, do async work (a timer) — this should not be blocked.
            ringline::sleep(std::time::Duration::from_millis(10)).await;

            // Now await the blocking result.
            let val = handle.await.expect("closure returned");
            BLOCKING_CONCURRENT.store(val, Ordering::SeqCst);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockingConcurrentHandler
    }
}

#[test]
fn spawn_blocking_doesnt_stall_worker() {
    BLOCKING_CONCURRENT.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<BlockingConcurrentHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(BLOCKING_CONCURRENT.load(Ordering::SeqCst), 10);
}

// ── Multiple concurrent blocking tasks ──────────────────────────────

static BLOCKING_MULTI: AtomicU32 = AtomicU32::new(0);

struct BlockingMultiHandler;

impl AsyncEventHandler for BlockingMultiHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let h1 = ringline::spawn_blocking(|| 10u32).unwrap();
            let h2 = ringline::spawn_blocking(|| 20u32).unwrap();
            let h3 = ringline::spawn_blocking(|| 30u32).unwrap();

            let (a, b) = ringline::join(h1, h2).await;
            let (a, b) = (a.expect("closure returned"), b.expect("closure returned"));
            let c = h3.await.expect("closure returned");
            BLOCKING_MULTI.store(a + b + c, Ordering::SeqCst);
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockingMultiHandler
    }
}

#[test]
fn spawn_blocking_multiple_concurrent() {
    BLOCKING_MULTI.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<BlockingMultiHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(BLOCKING_MULTI.load(Ordering::SeqCst), 60);
}

// ── Pool disabled ───────────────────────────────────────────────────

static BLOCKING_DISABLED: AtomicU32 = AtomicU32::new(0);

struct BlockingDisabledHandler;

impl AsyncEventHandler for BlockingDisabledHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            match ringline::spawn_blocking(|| 42u32) {
                Err(_) => BLOCKING_DISABLED.store(1, Ordering::SeqCst),
                Ok(_) => BLOCKING_DISABLED.store(99, Ordering::SeqCst),
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockingDisabledHandler
    }
}

#[test]
fn spawn_blocking_disabled() {
    BLOCKING_DISABLED.store(0, Ordering::SeqCst);

    let config = test_config_builder()
        .blocking_threads(0)
        .build()
        .expect("valid config");
    let (_shutdown, handles) = RinglineBuilder::new(config)
        .launch::<BlockingDisabledHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(BLOCKING_DISABLED.load(Ordering::SeqCst), 1);
}

// ── String return type (non-Copy) ───────────────────────────────────

static BLOCKING_STRING: AtomicU32 = AtomicU32::new(0);

struct BlockingStringHandler;

impl AsyncEventHandler for BlockingStringHandler {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let val = ringline::spawn_blocking(|| "hello blocking".to_string())
                .unwrap()
                .await
                .expect("closure returned");
            if val == "hello blocking" {
                BLOCKING_STRING.store(1, Ordering::SeqCst);
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BlockingStringHandler
    }
}

#[test]
fn spawn_blocking_non_copy_type() {
    BLOCKING_STRING.store(0, Ordering::SeqCst);

    let (_shutdown, handles) = RinglineBuilder::new(test_config())
        .launch::<BlockingStringHandler>()
        .expect("launch failed");

    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(BLOCKING_STRING.load(Ordering::SeqCst), 1);
}

// ── A dropped handle releases the result ────────────────────────────

static RESULTS_DROPPED: AtomicUsize = AtomicUsize::new(0);

/// A closure result that counts its own drop.
struct CountedResult;

impl Drop for CountedResult {
    fn drop(&mut self) {
        RESULTS_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

/// Starts a closure that finishes after 200 ms and drops its handle at
/// 20 ms, when the `timeout` around it expires.
struct DropsHandleEarly;

impl AsyncEventHandler for DropsHandleEarly {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let handle = ringline::spawn_blocking(|| {
                std::thread::sleep(Duration::from_millis(200));
                CountedResult
            })
            .unwrap();
            let timed_out = ringline::timeout(Duration::from_millis(20), handle)
                .await
                .is_err();
            assert!(timed_out, "the closure finished before the timeout");
        }))
    }

    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        DropsHandleEarly
    }
}

/// A result that arrives after its handle was dropped is dropped on arrival,
/// not held by the worker until it exits (#582).
#[test]
fn a_result_whose_handle_was_dropped_is_dropped_on_arrival() {
    RESULTS_DROPPED.store(0, Ordering::SeqCst);
    let (runtime, handles) = RinglineBuilder::new(test_config())
        .launch::<DropsHandleEarly>()
        .expect("launch failed");

    let deadline = Instant::now() + Duration::from_secs(3);
    while RESULTS_DROPPED.load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let dropped = RESULTS_DROPPED.load(Ordering::SeqCst);
    runtime.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    assert_eq!(dropped, 1, "the worker held the result of a dropped handle");
}
