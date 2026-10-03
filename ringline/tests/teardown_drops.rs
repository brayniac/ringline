//! Futures owned by a connection's task release what they hold when the
//! connection closes and the task is dropped (#575). That drop runs outside
//! any task poll, where the driver is not reachable.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::Read;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

/// The tests share counters, so they run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

static ARMED: AtomicUsize = AtomicUsize::new(0);
static EXHAUSTED: AtomicUsize = AtomicUsize::new(0);

/// Each connection's task sleeps far longer than the test runs, so it is
/// still sleeping when its connection closes.
struct SleepsInConnection;

impl AsyncEventHandler for SleepsInConnection {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            match ringline::try_sleep(Duration::from_secs(60)) {
                Ok(sleep) => {
                    ARMED.fetch_add(1, Ordering::SeqCst);
                    sleep.await;
                }
                Err(_) => {
                    EXHAUSTED.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SleepsInConnection
    }
}

/// Connect, wait for the server to accept and settle, then close.
fn connect_and_close(addr: std::net::SocketAddr) {
    let mut client = std::net::TcpStream::connect(addr).expect("connect");
    std::thread::sleep(Duration::from_millis(20));
    client.shutdown(std::net::Shutdown::Write).ok();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    // The server closes once it sees our FIN; reading to EOF waits for that.
    let _ = client.read(&mut [0u8; 16]);
}

/// A connection closed while its task sleeps gives the timer slot back, so
/// more connections than slots can each sleep in turn.
#[test]
fn a_sleep_dropped_with_its_connection_frees_its_timer_slot() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    ARMED.store(0, Ordering::SeqCst);
    EXHAUSTED.store(0, Ordering::SeqCst);
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .timer_slots(8)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<SleepsInConnection>()
        .expect("launch");
    let addr = runtime.bound_addr().unwrap();
    for _ in 0..20 {
        connect_and_close(addr);
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while ARMED.load(Ordering::SeqCst) + EXHAUSTED.load(Ordering::SeqCst) < 20
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    assert_eq!(
        (
            ARMED.load(Ordering::SeqCst),
            EXHAUSTED.load(Ordering::SeqCst)
        ),
        (20, 0),
        "(armed, exhausted): closed connections' sleeps kept their timer slots"
    );
}

// ── A spawn whose connection closes ─────────────────────────────────────

static SPAWNED: AtomicUsize = AtomicUsize::new(0);

/// Each connection's task starts a process and holds the `SpawnFuture`
/// without polling it, so the spawn's result arrives with nothing to take
/// it.
struct SpawnsInConnection;

impl AsyncEventHandler for SpawnsInConnection {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _spawn = ringline::process::Command::new("true")
                .spawn()
                .expect("submit spawn");
            SPAWNED.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SpawnsInConnection
    }
}

/// How many pidfds this process holds.
#[cfg(target_os = "linux")]
fn open_pidfds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("read /proc/self/fd")
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|target| target.to_string_lossy() == "anon_inode:[pidfd]")
        .count()
}

/// A spawn whose connection closed before its result was taken closes the
/// child's pidfd.
#[cfg(target_os = "linux")]
#[test]
fn a_spawn_dropped_with_its_connection_closes_its_pidfd() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    SPAWNED.store(0, Ordering::SeqCst);
    let before = open_pidfds();
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(1)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<SpawnsInConnection>()
        .expect("launch");
    let addr = runtime.bound_addr().unwrap();
    for _ in 0..10 {
        connect_and_close(addr);
    }
    // Let the spawns' results arrive and the event loop release them.
    let deadline = Instant::now() + Duration::from_secs(5);
    while open_pidfds() > before && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let leaked = open_pidfds().saturating_sub(before);
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    assert_eq!(
        SPAWNED.load(Ordering::SeqCst),
        10,
        "every connection spawned"
    );
    assert_eq!(
        leaked, 0,
        "spawns dropped with their connections kept their pidfds"
    );
}

// ── A channel sender whose connection closes ────────────────────────────

thread_local! {
    static SENDER: std::cell::RefCell<Option<ringline::oneshot::Sender<u32>>> =
        const { std::cell::RefCell::new(None) };
}
static RECV_OUTCOME: Mutex<Option<String>> = Mutex::new(None);

/// A standalone task waits on a oneshot receiver whose sender a
/// connection's task holds.
struct SenderInConnection;

impl AsyncEventHandler for SenderInConnection {
    fn on_start(&self) -> Option<std::pin::Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let (tx, rx) = ringline::oneshot::channel::<u32>();
            SENDER.with(|s| *s.borrow_mut() = Some(tx));
            // The connection closes about 20 ms after it opens; the timeout
            // only bounds the test. Without the wake, the receiver is polled
            // again only when the timeout fires.
            let started = Instant::now();
            let outcome = match ringline::timeout(Duration::from_secs(5), rx).await {
                Ok(Ok(v)) => format!("received {v}"),
                Ok(Err(_)) if started.elapsed() < Duration::from_secs(2) => {
                    "sender dropped".to_string()
                }
                Ok(Err(_)) => format!("sender dropped, seen only after {:?}", started.elapsed()),
                Err(_) => "receiver never woken".to_string(),
            };
            *RECV_OUTCOME.lock().unwrap() = Some(outcome);
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _tx = SENDER.with(|s| s.borrow_mut().take());
            std::future::pending::<()>().await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        SenderInConnection
    }
}

/// A channel sender dropped with its connection's task wakes the receiver,
/// which sees the channel closed.
#[test]
fn a_sender_dropped_with_its_connection_wakes_the_receiver() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    *RECV_OUTCOME.lock().unwrap() = None;
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
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<SenderInConnection>()
        .expect("launch");
    connect_and_close(runtime.bound_addr().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while RECV_OUTCOME.lock().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    assert_eq!(
        RECV_OUTCOME.lock().unwrap().take().as_deref(),
        Some("sender dropped")
    );
}
