//! A runtime `Waker` woken from a thread other than the worker that polled
//! it schedules its task on that worker, as `Waker`'s `Send + Sync` contract
//! allows (#559).

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ringline::{AsyncEventHandler, Config, ConfigBuilder, Connection, RinglineBuilder};

/// No tick: an idle io_uring worker blocks until something arrives, so a
/// cross-thread wake must reach it through its wake fd. (mio polls with a
/// 10 ms cap regardless.)
fn config(workers: usize) -> Config {
    ConfigBuilder::new()
        .workers(workers)
        .tick_timeout_us(0)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .build()
        .expect("valid config")
}

/// A one-shot gate: pending until `open` is set, storing the waker of the
/// task that waits on it so another thread can wake it.
struct Gate {
    open: &'static AtomicBool,
    waker: &'static Mutex<Option<Waker>>,
}

impl Future for Gate {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Store the waker before reading `open`: an opener that sets `open`
        // after the read finds this waker and wakes it.
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

fn open_gate(open: &AtomicBool, waker: &Mutex<Option<Waker>>) {
    open.store(true, Ordering::SeqCst);
    if let Some(w) = waker.lock().unwrap().take() {
        w.wake();
    }
}

/// Wait until the woken task has run, failing instead of hanging if it never
/// does, then shut the runtime down.
fn wait_for(
    done: &AtomicBool,
    runtime: ringline::Runtime,
    handles: Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
    what: &str,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    assert!(
        done.load(Ordering::SeqCst),
        "{what}: the woken task never ran"
    );
}

// ── Woken from a thread outside the runtime ─────────────────────────

static PLAIN_OPEN: AtomicBool = AtomicBool::new(false);
static PLAIN_WAKER: Mutex<Option<Waker>> = Mutex::new(None);
static PLAIN_DONE: AtomicBool = AtomicBool::new(false);

struct WaitsForPlainThread;

impl AsyncEventHandler for WaitsForPlainThread {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            Gate {
                open: &PLAIN_OPEN,
                waker: &PLAIN_WAKER,
            }
            .await;
            PLAIN_DONE.store(true, Ordering::SeqCst);
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        WaitsForPlainThread
    }
}

#[test]
fn a_waker_woken_from_a_plain_thread_runs_its_task() {
    let (runtime, handles) = RinglineBuilder::new(config(1))
        .launch::<WaitsForPlainThread>()
        .expect("launch");
    std::thread::spawn(|| {
        // Let the task park first. Correct either way: `Gate` rechecks.
        std::thread::sleep(Duration::from_millis(50));
        open_gate(&PLAIN_OPEN, &PLAIN_WAKER);
    });
    wait_for(&PLAIN_DONE, runtime, handles, "plain thread");

    // The worker has exited; waking its waker again must not panic.
    let stale = PLAIN_WAKER.lock().unwrap().take();
    if let Some(w) = stale {
        w.wake();
    }
}

// ── Woken from another worker ───────────────────────────────────────

static PEER_OPEN: AtomicBool = AtomicBool::new(false);
static PEER_WAKER: Mutex<Option<Waker>> = Mutex::new(None);
static PEER_DONE: AtomicBool = AtomicBool::new(false);

struct WorkerWakesPeer {
    id: usize,
}

impl AsyncEventHandler for WorkerWakesPeer {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let id = self.id;
        Some(Box::pin(async move {
            if id == 0 {
                Gate {
                    open: &PEER_OPEN,
                    waker: &PEER_WAKER,
                }
                .await;
                PEER_DONE.store(true, Ordering::SeqCst);
            } else {
                // Wait until worker 0 has parked, then wake it from here.
                while PEER_WAKER.lock().unwrap().is_none() {
                    ringline::sleep(Duration::from_millis(5)).await;
                }
                open_gate(&PEER_OPEN, &PEER_WAKER);
            }
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(id: usize) -> Self {
        WorkerWakesPeer { id }
    }
}

#[test]
fn a_waker_woken_from_another_worker_runs_its_task() {
    let (runtime, handles) = RinglineBuilder::new(config(2))
        .launch::<WorkerWakesPeer>()
        .expect("launch");
    wait_for(&PEER_DONE, runtime, handles, "another worker");
}
