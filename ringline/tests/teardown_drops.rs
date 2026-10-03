//! Futures owned by a connection's task release what they hold when the
//! connection closes and the task is dropped (#575). That drop runs outside
//! any task poll, where the driver is not reachable.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::Read;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

/// The tests share counters, so they run one at a time.
static SERIAL: Mutex<()> = Mutex::new(());

static ARMED: AtomicUsize = AtomicUsize::new(0);
static EXHAUSTED: AtomicUsize = AtomicUsize::new(0);
static SLEEPERS_DROPPED: AtomicUsize = AtomicUsize::new(0);

/// Counts the connection tasks that have been dropped.
struct CountDrop;

impl Drop for CountDrop {
    fn drop(&mut self) {
        SLEEPERS_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

/// Each connection's task sleeps far longer than the test runs, so it is
/// still sleeping when its connection closes.
struct SleepsInConnection;

impl AsyncEventHandler for SleepsInConnection {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _guard = CountDrop;
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

/// With every timer slot in use, closing one connection and opening another
/// gives the new connection's sleep the freed slot: the slot is released
/// before the new connection's task first runs.
#[test]
fn a_freed_timer_slot_is_available_to_the_next_connection() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    ARMED.store(0, Ordering::SeqCst);
    EXHAUSTED.store(0, Ordering::SeqCst);
    SLEEPERS_DROPPED.store(0, Ordering::SeqCst);
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .timer_slots(4)
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
    let wait_for = |n: usize| {
        let deadline = Instant::now() + Duration::from_secs(5);
        while ARMED.load(Ordering::SeqCst) + EXHAUSTED.load(Ordering::SeqCst) < n
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(2));
        }
    };
    let mut open: std::collections::VecDeque<std::net::TcpStream> = (0..4)
        .map(|_| std::net::TcpStream::connect(addr).expect("connect"))
        .collect();
    wait_for(4);
    for round in 0..30 {
        // Close the oldest connection and wait for its task to be dropped.
        drop(open.pop_front().unwrap());
        let deadline = Instant::now() + Duration::from_secs(5);
        while SLEEPERS_DROPPED.load(Ordering::SeqCst) < round + 1 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        open.push_back(std::net::TcpStream::connect(addr).expect("connect"));
        wait_for(5 + round);
    }
    drop(open);
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    assert_eq!(
        (
            ARMED.load(Ordering::SeqCst),
            EXHAUSTED.load(Ordering::SeqCst)
        ),
        (34, 0),
        "(armed, exhausted): a new connection found the pool exhausted"
    );
}

// ── A spawn whose connection closes ─────────────────────────────────────

#[cfg(target_os = "linux")]
static SPAWNED: AtomicUsize = AtomicUsize::new(0);

#[cfg(target_os = "linux")]
/// Each connection's task starts a process and holds the `SpawnFuture`
/// without polling it, so the spawn's result arrives with nothing to take
/// it.
struct SpawnsInConnection;

#[cfg(target_os = "linux")]
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

// ── An outbound connect whose connection closes ─────────────────────────

static CONNECT_TARGET: Mutex<Option<std::net::SocketAddr>> = Mutex::new(None);
static CONNECT_TLS: AtomicBool = AtomicBool::new(false);
static CONNECT_SUBMITTED: AtomicUsize = AtomicUsize::new(0);
static CONNECT_TASKS_DROPPED: AtomicUsize = AtomicUsize::new(0);

struct CountConnectDrop;

impl Drop for CountConnectDrop {
    fn drop(&mut self) {
        CONNECT_TASKS_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

/// Each connection's task starts an outbound connect (TLS when
/// `CONNECT_TLS` is set), polls it once so it is submitted, and never polls
/// it again. The connect completes with no task to take the connection.
struct ConnectsInConnection;

impl AsyncEventHandler for ConnectsInConnection {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _count = CountConnectDrop;
            let target = CONNECT_TARGET.lock().unwrap().expect("target set");
            let connect = if CONNECT_TLS.load(Ordering::SeqCst) {
                ringline::connect(target).tls("localhost")
            } else {
                ringline::connect(target)
            };
            let mut connect = Box::pin(std::future::IntoFuture::into_future(connect));
            std::future::poll_fn(|cx| {
                let _ = connect.as_mut().poll(cx);
                std::task::Poll::Ready(())
            })
            .await;
            CONNECT_SUBMITTED.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        ConnectsInConnection
    }
}

/// A TLS client config whose handshakes never complete against a plain TCP
/// target, which reads the ClientHello and never answers.
fn tls_client() -> ringline::TlsClientConfig {
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    ringline::TlsClientConfig::new(std::sync::Arc::new(config))
}

/// A listener with a backlog of 0 whose accept queue is full, so the kernel
/// drops the next SYN and the connect stays in flight until the SYN is
/// retransmitted, about a second later. Returns the listener and the
/// connections that fill its queue.
#[cfg(target_os = "linux")]
fn full_listener() -> (std::net::TcpListener, Vec<std::net::TcpStream>) {
    use std::os::fd::FromRawFd;
    // SAFETY: plain socket calls on a fresh fd; `addr` outlives `bind`.
    let listener = unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        assert!(fd >= 0, "socket: {}", std::io::Error::last_os_error());
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes([127, 0, 0, 1]),
            },
            sin_zero: [0; 8],
        };
        let rc = libc::bind(
            fd,
            &addr as *const libc::sockaddr_in as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
        assert_eq!(rc, 0, "bind: {}", std::io::Error::last_os_error());
        assert_eq!(libc::listen(fd, 0), 0);
        std::net::TcpListener::from_raw_fd(fd)
    };
    let addr = listener.local_addr().unwrap();
    let mut fillers = Vec::new();
    for _ in 0..8 {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(300)) {
            Ok(stream) => fillers.push(stream),
            Err(_) => return (listener, fillers),
        }
    }
    panic!("the listener's accept queue never filled");
}

/// Launch a server whose connection task connects to `target`, open and
/// close one connection to it, and wait until that task is dropped.
fn drop_a_connect_to(
    target: std::net::SocketAddr,
    tls: bool,
) -> (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    CONNECT_SUBMITTED.store(0, Ordering::SeqCst);
    CONNECT_TASKS_DROPPED.store(0, Ordering::SeqCst);
    CONNECT_TLS.store(tls, Ordering::SeqCst);
    *CONNECT_TARGET.lock().unwrap() = Some(target);
    let mut config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        // No tick: an idle worker blocks until its next event, so the close
        // must not wait for one.
        .tick_timeout_us(0);
    if tls {
        config = config.tls_client(tls_client());
    }
    let config = config.build().expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<ConnectsInConnection>()
        .expect("launch");
    connect_and_close(runtime.bound_addr().unwrap());
    let deadline = Instant::now() + Duration::from_secs(5);
    while CONNECT_TASKS_DROPPED.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < deadline,
            "the connection's task was never dropped"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(CONNECT_SUBMITTED.load(Ordering::SeqCst), 1);
    (runtime, handles)
}

/// Read `stream` until EOF. Returns false if it stays open for 3 s.
fn sees_eof(mut stream: std::net::TcpStream) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return true,
            Ok(_) => continue,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return true,
            Err(_) => return false,
        }
    }
}

fn stop(
    runtime: ringline::Runtime,
    handles: Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
}

/// An outbound connect that completed before its connection's task was
/// dropped is closed with it, not left established with no owner.
fn a_completed_connect_is_closed(tls: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let target = std::net::TcpListener::bind("127.0.0.1:0").expect("bind target");
    let (runtime, handles) = drop_a_connect_to(target.local_addr().unwrap(), tls);
    let (outbound, _) = target.accept().expect("accept the outbound connect");
    let closed = sees_eof(outbound);
    stop(runtime, handles);
    assert!(
        closed,
        "the outbound connection stayed open after its ConnectFuture was dropped"
    );
}

#[test]
fn a_connect_dropped_with_its_connection_is_closed() {
    a_completed_connect_is_closed(false);
}

#[test]
fn a_tls_connect_dropped_with_its_connection_is_closed() {
    a_completed_connect_is_closed(true);
}

/// An outbound connect still in flight when its connection's task was
/// dropped is closed when it completes.
#[cfg(target_os = "linux")]
fn an_in_flight_connect_is_closed(tls: bool) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (target, fillers) = full_listener();
    let (runtime, handles) = drop_a_connect_to(target.local_addr().unwrap(), tls);

    // Empty the queue; the retransmitted SYN then completes the connect.
    let filler_addrs: Vec<_> = fillers.iter().map(|f| f.local_addr().unwrap()).collect();
    target.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let outbound = loop {
        match target.accept() {
            Ok((stream, peer)) if !filler_addrs.contains(&peer) => break stream,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "the outbound connect never arrived"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("accept: {e}"),
        }
    };
    outbound.set_nonblocking(false).unwrap();
    let closed = sees_eof(outbound);
    stop(runtime, handles);
    drop(fillers);
    assert!(
        closed,
        "the outbound connection stayed open after its in-flight ConnectFuture was dropped"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn an_in_flight_connect_dropped_with_its_connection_is_closed() {
    an_in_flight_connect_is_closed(false);
}

#[cfg(target_os = "linux")]
#[test]
fn an_in_flight_tls_connect_dropped_with_its_connection_is_closed() {
    an_in_flight_connect_is_closed(true);
}
