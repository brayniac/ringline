//! `SendHalf::abort` on a peer that has stopped reading
//! (`docs/send-admission-design.md`, section 3). One worker, so that every
//! connection shares a ring: an idle connection opened first keeps its hangup
//! poll pending, which before Linux 6.13 holds a plain or `SO_LINGER` close of
//! a later connection's fixed file. The abort must not wait for it.
//!
//! The aborted connection holds more than the socket buffers take (on
//! io_uring, one zero-copy send), four bounded sends queued behind it and a
//! bounded send waiting for send-pool capacity. Both backends check the reset,
//! the refusal of those sends and of later ones, and, on TLS, that no
//! close_notify is sent.
//!
//! The admission-order tests at the end use the same stalled connection to
//! keep a head waiting in the admission FIFO, and check which sends each
//! `SendAdmissionOrder` lets pass it, and that the slots a passing send holds
//! (B) return when it settles and when its connection is aborted
//! (`docs/send-admission-design.md`, section 4, Experiment C).

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

mod common;

/// The tests share the statics below, and `cargo test` runs them in one
/// process.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// What the aborting task saw, for the test thread to check.
#[derive(Debug, Default)]
struct Seen {
    /// When `abort` returned.
    aborted_at: Option<Instant>,
    /// The result of the sends queued behind the first one.
    queued: Option<String>,
    /// The result of the send waiting for send-pool capacity.
    waiting: Option<String>,
    /// The result of a send made after `abort`.
    late: Option<String>,
    /// The result of a bounded send made after `abort`.
    late_admitted: Option<String>,
}

static SEEN: Mutex<Option<Seen>> = Mutex::new(None);

/// When the zero-copy send's guard was dropped: its slab entry was released.
#[cfg(has_io_uring)]
static GUARD_DROPPED_AT: Mutex<Option<Instant>> = Mutex::new(None);

const SLOT: usize = 32768;
const SLOTS: u16 = 8;

/// Larger than the loopback socket buffers hold, and small enough to fit
/// under an 8 MiB `RLIMIT_MEMLOCK`.
#[cfg(has_io_uring)]
const GUARD_LEN: usize = 4 << 20;

#[cfg(has_io_uring)]
struct VecGuard(Vec<u8>);

#[cfg(has_io_uring)]
impl ringline::SendGuard for VecGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        (self.0.as_ptr(), self.0.len() as u32)
    }
    fn region(&self) -> ringline::RegionId {
        ringline::RegionId::UNREGISTERED
    }
}

#[cfg(has_io_uring)]
impl Drop for VecGuard {
    fn drop(&mut self) {
        *GUARD_DROPPED_AT.lock().unwrap() = Some(Instant::now());
    }
}

/// What `abort`'s sends fail with.
const ABORTED: &str = "ConnectionAborted: connection aborted";

/// A failed send's kind and message, which tells `abort`'s refusal
/// ("connection aborted") from a released connection's ("connection closed").
fn kind(result: io::Result<impl Sized>) -> String {
    match result {
        Ok(_) => panic!("the send succeeded"),
        Err(e) => format!("{:?}: {e}", e.kind()),
    }
}

/// The first byte a peer writes picks the connection's part: `i` reads until
/// the peer closes, and `a` fills its sends and aborts.
struct Aborter;

impl AsyncEventHandler for Aborter {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut first = 0u8;
            conn.with_data(|data| {
                first = data[0];
                ParseResult::Consumed(data.len())
            })
            .await;
            if first == b'i' {
                while conn
                    .with_data(|data| ParseResult::Consumed(data.len()))
                    .await
                    > 0
                {}
                return;
            }
            let mut seen = Seen::default();
            fill_and_abort(&mut conn, &mut seen).await;
            seen.late = Some(kind(conn.send(b"late")));
            *SEEN.lock().unwrap() = Some(seen);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Aborter
    }
}

/// More than the socket buffers take, four bounded sends queued behind it
/// holding send-pool slots, and a bounded send waiting for more slots than are
/// free; then `abort`, the sends' results, and the result of a bounded send
/// made after it.
async fn fill_and_abort(conn: &mut Connection, seen: &mut Seen) {
    static QUEUED: [u8; SLOT] = [1; SLOT];
    static WAITING: [u8; 6 * SLOT] = [2; 6 * SLOT];
    fill(conn);
    let mut queued = Vec::new();
    for _ in 0..4 {
        queued.push(conn.send_admitted(&QUEUED).await.expect("admitted"));
    }
    let mut waiting = std::pin::pin!(conn.send_admitted(&WAITING));
    let first = std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "the send waits for capacity");
    ringline::sleep(Duration::from_millis(100)).await;

    conn.abort();
    seen.aborted_at = Some(Instant::now());

    seen.waiting = Some(kind(waiting.await));
    let mut kinds = Vec::new();
    for send in queued {
        kinds.push(kind(send.await));
    }
    assert!(kinds.iter().all(|k| *k == kinds[0]), "{kinds:?}");
    seen.queued = Some(kinds[0].clone());
    seen.late_admitted = Some(kind(conn.send_admitted(b"late").await));
}

/// On io_uring: a zero-copy send the peer will not take in full.
#[cfg(has_io_uring)]
fn fill(conn: &mut Connection) {
    let guard = ringline::GuardBox::new(VecGuard(vec![7; GUARD_LEN]));
    conn.send_parts()
        .build(|b| b.guard(guard).submit())
        .expect("guard send accepted");
}

/// On mio: 16 MiB of plain sends, which take no send-pool slot. macOS grows
/// loopback socket buffers to 4 MiB a side, and a smaller fill let the bounded
/// sends behind it complete before the abort.
#[cfg(not(has_io_uring))]
fn fill(conn: &mut Connection) {
    for _ in 0..256 {
        conn.send_nowait(&[1; 1 << 16]).expect("queued");
    }
}

fn launch<H: AsyncEventHandler>(
    tls: Option<ringline::TlsConfig>,
) -> (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    let mut config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32);
    if let Some(tls) = tls {
        config = config.tls(tls);
    }
    let config = config.build().expect("valid config");
    RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<H>()
        .expect("launch")
}

fn wait_for_seen() -> Seen {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(seen) = SEEN.lock().unwrap().take() {
            return seen;
        }
        assert!(
            Instant::now() < deadline,
            "the aborting task never finished"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The aborted connection's peer reads a reset, its sends fail with
/// `ConnectionAborted`, and on io_uring its zero-copy send's buffer is
/// released within 100 ms of the abort although the older idle connection's
/// hangup poll is still pending.
#[test]
fn abort_releases_a_stalled_connection_at_once() {
    let _serial = serial();
    *SEEN.lock().unwrap() = None;
    #[cfg(has_io_uring)]
    {
        *GUARD_DROPPED_AT.lock().unwrap() = None;
    }
    let (runtime, handles) = launch::<Aborter>(None);
    let addr = runtime.bound_addr().expect("bound");
    let mut idle = TcpStream::connect(addr).expect("connect idle");
    idle.write_all(b"i").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut peer = TcpStream::connect(addr).expect("connect");
    // Before the reset: macOS refuses the option on a reset socket.
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    peer.write_all(b"a").unwrap();

    let seen = wait_for_seen();
    assert!(seen.aborted_at.is_some());
    let aborted = Some(ABORTED.to_string());
    assert_eq!(seen.queued, aborted);
    assert_eq!(seen.waiting, aborted);
    assert_eq!(seen.late, aborted);
    assert_eq!(seen.late_admitted, aborted);
    #[cfg(has_io_uring)]
    {
        let aborted_at = seen.aborted_at.unwrap();
        // Over `RLIMIT_MEMLOCK` the guard send falls back to a plain send,
        // and no zero-copy notification is held.
        let enomem = ringline::metrics::POOL
            .value(ringline::metrics::pool::SEND_ZC_ENOMEM)
            .unwrap_or(0);
        eprintln!("abort: enomem={enomem}");
        if enomem == 0 {
            let deadline = aborted_at + Duration::from_secs(2);
            let dropped = loop {
                if let Some(at) = *GUARD_DROPPED_AT.lock().unwrap() {
                    break Some(at);
                }
                if Instant::now() > deadline {
                    break None;
                }
                std::thread::sleep(Duration::from_millis(1));
            };
            let dropped = dropped.expect("the zero-copy buffer was not released within 2 s");
            assert!(dropped >= aborted_at, "released before the abort");
            let took = dropped - aborted_at;
            eprintln!("abort: released {took:?} after the abort");
            assert!(
                took < Duration::from_millis(100),
                "released {took:?} after the abort"
            );
        }
    }

    let mut body = Vec::new();
    let read = peer.read_to_end(&mut body);
    // On macOS a peer that only read sometimes saw no reset within the 5 s
    // timeout (13 of 210 runs, and the same with two plain TCP sockets on
    // loopback and no ringline). A write draws a reset from any closed
    // socket, so it is made only after the read timed out: an end of stream
    // still fails the test.
    #[cfg(target_os = "macos")]
    let read = if matches!(&read, Err(e) if e.kind() == io::ErrorKind::WouldBlock) {
        let _ = peer.write(b"x");
        peer.read_to_end(&mut body)
    } else {
        read
    };
    assert!(
        matches!(&read, Err(e) if e.kind() == io::ErrorKind::ConnectionReset),
        "the peer was not reset: {read:?}"
    );
    drop(idle);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// Completes the handshake, reads one request, and aborts.
struct TlsAborter;

impl AsyncEventHandler for TlsAborter {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            conn.abort();
            let seen = Seen {
                aborted_at: Some(Instant::now()),
                late: Some(kind(conn.send(b"late"))),
                late_admitted: Some(kind(conn.send_admitted(b"late").await)),
                ..Seen::default()
            };
            *SEEN.lock().unwrap() = Some(seen);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsAborter
    }
}

/// A TLS connection's abort sends no close_notify: the client reads a reset,
/// not the clean end of stream an alert would give it.
#[test]
fn abort_sends_no_close_notify() {
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

    let _serial = serial();
    *SEEN.lock().unwrap() = None;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let der = CertificateDer::from(cert.cert);
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let tls = ringline::TlsConfig::builder()
        .cert(vec![der.clone()], key.into())
        .build()
        .expect("tls config");
    let (runtime, handles) = launch::<TlsAborter>(Some(tls));

    let mut roots = rustls::RootCertStore::empty();
    roots.add(der).unwrap();
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from("localhost").unwrap();
    let client = rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
    let sock = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut stream = rustls::StreamOwned::new(client, sock);
    stream.write_all(b"request").expect("handshake and write");

    let seen = wait_for_seen();
    assert_eq!(seen.late, Some(ABORTED.to_string()));
    assert_eq!(seen.late_admitted, Some(ABORTED.to_string()));
    let mut buf = [0u8; 64];
    let read = stream.read(&mut buf);
    assert!(
        matches!(&read, Err(e) if e.kind() == io::ErrorKind::ConnectionReset),
        "expected a reset, not a close_notify: {read:?}"
    );
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// Shuts the write half down, aborts, and sends.
struct ShutdownAborter;

impl AsyncEventHandler for ShutdownAborter {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            conn.shutdown_write();
            conn.abort();
            let seen = Seen {
                aborted_at: Some(Instant::now()),
                late: Some(kind(conn.send(b"late"))),
                late_admitted: Some(kind(conn.send_admitted(b"late").await)),
                ..Seen::default()
            };
            *SEEN.lock().unwrap() = Some(seen);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        ShutdownAborter
    }
}

/// After `shutdown_write`, a send made after `abort` still fails with
/// `ConnectionAborted`, a bounded one included, rather than with the
/// `BrokenPipe` of a shut-down write half.
#[test]
fn abort_after_shutdown_write_refuses_sends_as_aborted() {
    let _serial = serial();
    *SEEN.lock().unwrap() = None;
    let (runtime, handles) = launch::<ShutdownAborter>(None);
    let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    peer.write_all(b"x").unwrap();

    let seen = wait_for_seen();
    assert_eq!(seen.late, Some(ABORTED.to_string()));
    assert_eq!(seen.late_admitted, Some(ABORTED.to_string()));
    drop(peer);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// On io_uring a Unix-domain connection closes without a disconnect, and its
/// queued sends fail with `ConnectionAborted`.
#[cfg(has_io_uring)]
#[test]
fn abort_closes_a_unix_connection() {
    use std::os::unix::net::UnixStream;

    struct UnixAborter;

    impl AsyncEventHandler for UnixAborter {
        fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
            async move {
                conn.with_data(|data| ParseResult::Consumed(data.len()))
                    .await;
                // More than a Unix socket's buffer takes, so the next send
                // queues behind it.
                let guard = ringline::GuardBox::new(VecGuard(vec![1; GUARD_LEN]));
                conn.send_parts()
                    .build(|b| b.guard(guard).submit())
                    .expect("guard send accepted");
                let queued = conn.send(&[2; SLOT]).expect("queued");
                ringline::sleep(Duration::from_millis(50)).await;
                conn.abort();
                let seen = Seen {
                    aborted_at: Some(Instant::now()),
                    queued: Some(kind(queued.await)),
                    late: Some(kind(conn.send(b"late"))),
                    ..Seen::default()
                };
                *SEEN.lock().unwrap() = Some(seen);
            }
        }

        fn create_for_worker(_id: usize) -> Self {
            UnixAborter
        }
    }

    let _serial = serial();
    *SEEN.lock().unwrap() = None;
    let dir = std::env::temp_dir().join(format!("ringline-abort-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sock");
    let _ = std::fs::remove_file(&path);
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind_unix(&path)
        .launch::<UnixAborter>()
        .expect("launch");
    let mut peer = UnixStream::connect(&path).expect("connect");
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    peer.write_all(b"a").unwrap();

    let seen = wait_for_seen();
    assert_eq!(seen.queued, Some(ABORTED.to_string()));
    assert_eq!(seen.late, Some(ABORTED.to_string()));
    // The peer reads what reached its buffer, then the end of the stream or
    // a reset; either ends the read.
    let mut body = Vec::new();
    let read = peer.read_to_end(&mut body);
    assert!(
        read.is_ok() || matches!(&read, Err(e) if e.kind() == io::ErrorKind::ConnectionReset),
        "the connection did not close: {read:?}"
    );
    assert!(body.iter().all(|&b| b == 1), "a queued send was written");
    runtime.shutdown();
    common::join_workers(&runtime, handles);
    let _ = std::fs::remove_dir_all(&dir);
}

// ── Admission orders ────────────────────────────────────────────────────

/// What a worker under one admission order did with sends behind a waiting
/// head, for the test thread to check.
#[derive(Debug, Default)]
struct Passing {
    /// The head was still waiting after its first poll.
    head_waited_first: bool,
    /// A waiter behind the head on the head's own connection was still
    /// waiting after its first poll.
    same_connection_waited: bool,
    /// A send to a peer that reads, polled once while the head waited:
    /// `Some(passed_head)` if it was admitted.
    reader: Option<bool>,
    /// That send's result once settled, or why it was refused.
    reader_settled: Option<String>,
    /// B once that send settled.
    held_after_settle: Option<u64>,
    /// A send queued behind a full socket, polled once while the head
    /// waited: `Some(passed_head)` if it was admitted.
    staller: Option<bool>,
    /// B before and after `abort` on that connection, if the send passed.
    held_around_abort: Option<(u64, u64)>,
    /// That send's result after the abort.
    staller_settled: Option<String>,
    /// The head was still waiting at the end.
    head_waited_last: bool,
    /// The worker's counters at the end.
    counters: ringline::SendAdmissionCounters,
}

static PASSING: Mutex<Option<Passing>> = Mutex::new(None);

/// The test thread's listeners: the first one's peer reads, the second's
/// does not.
static PEERS: Mutex<Option<(SocketAddr, SocketAddr)>> = Mutex::new(None);

/// Holds the pool's slots behind a waiting head on its connection and tries
/// sends that could pass it; see [`pass_the_head`].
struct Passer;

impl AsyncEventHandler for Passer {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            let passing = pass_the_head(&mut conn).await;
            *PASSING.lock().unwrap() = Some(passing);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Passer
    }
}

/// Poll `f` once from the current task.
async fn poll_once<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *f).poll(cx))).await
}

/// A send's result, as the test thread compares it.
fn outcome(result: io::Result<u32>) -> String {
    match result {
        Ok(n) => format!("Ok: {n}"),
        Err(e) => format!("{:?}: {e}", e.kind()),
    }
}

/// B on this worker now.
fn passing_held() -> u64 {
    ringline::send_admission_counters().passing_held
}

/// On `conn`, whose peer does not read: more than the socket buffers take,
/// four bounded sends queued behind it holding four of the pool's eight
/// slots, and a head that needs six and waits. Then, while the head waits,
/// one poll each of a waiter behind it on `conn`, a send on a connection
/// whose peer reads, and a send queued behind a full socket on a third
/// connection, which is then aborted.
async fn pass_the_head(conn: &mut Connection) -> Passing {
    static QUEUED: [u8; SLOT] = [1; SLOT];
    static HEAD: [u8; 6 * SLOT] = [2; 6 * SLOT];
    static SMALL: [u8; SLOT] = [3; SLOT];
    let (reader_addr, staller_addr) = PEERS.lock().unwrap().expect("peers");
    let mut out = Passing::default();

    fill(conn);
    let mut queued = Vec::new();
    for _ in 0..4 {
        queued.push(conn.send_admitted(&QUEUED).await.expect("admitted"));
    }
    let mut head = conn.send_admitted(&HEAD);
    out.head_waited_first = poll_once(&mut head).await.is_pending();
    let mut same = conn.send_admitted(&SMALL);
    out.same_connection_waited = poll_once(&mut same).await.is_pending();

    let mut reader = ringline::connect(reader_addr)
        .await
        .expect("connect the reader");
    let mut sent = reader.send_admitted(&SMALL);
    let first = poll_once(&mut sent).await;
    match first {
        Poll::Ready(Ok(completion)) => {
            out.reader = Some(completion.passed_head());
            out.reader_settled = Some(outcome(completion.await));
            out.held_after_settle = Some(passing_held());
        }
        Poll::Ready(Err(e)) => out.reader_settled = Some(format!("refused: {e}")),
        Poll::Pending => {}
    }
    drop(sent);

    let mut staller = ringline::connect(staller_addr)
        .await
        .expect("connect the staller");
    fill(&mut staller);
    let mut stalled = staller.send_admitted(&SMALL);
    let first = poll_once(&mut stalled).await;
    match first {
        Poll::Ready(Ok(completion)) => {
            out.staller = Some(completion.passed_head());
            let before = passing_held();
            staller.abort();
            out.held_around_abort = Some((before, passing_held()));
            out.staller_settled = Some(outcome(completion.await));
        }
        Poll::Ready(Err(e)) => out.staller_settled = Some(format!("refused: {e}")),
        Poll::Pending => {
            staller.abort();
            out.staller_settled = Some(match stalled.await {
                Ok(_) => "admitted after the abort".to_string(),
                Err(e) => format!("{:?}: {e}", e.kind()),
            });
        }
    }

    out.head_waited_last = poll_once(&mut head).await.is_pending();
    out.counters = ringline::send_admission_counters();
    conn.abort();
    out
}

/// Run [`Passer`] on one worker under `order`, with a peer that reads and
/// one that does not for its outbound connections, and return what it saw.
fn run_order(order: ringline::SendAdmissionOrder) -> Passing {
    let _serial = serial();
    *PASSING.lock().unwrap() = None;
    let reader = TcpListener::bind("127.0.0.1:0").expect("bind the reader");
    let staller = TcpListener::bind("127.0.0.1:0").expect("bind the staller");
    *PEERS.lock().unwrap() = Some((reader.local_addr().unwrap(), staller.local_addr().unwrap()));
    // Neither thread is joined: each blocks in `accept` if the handler never
    // connects, and the wait below fails the test instead.
    std::thread::spawn(move || {
        let (mut stream, _) = reader.accept().expect("accept the reader");
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
    });
    let (release, held) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let (_stream, _) = staller.accept().expect("accept the staller");
        let _ = held.recv_timeout(Duration::from_secs(20));
    });

    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32)
        .send_admission_order(order)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Passer>()
        .expect("launch");
    let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    peer.write_all(b"p").unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let passing = loop {
        if let Some(passing) = PASSING.lock().unwrap().take() {
            break passing;
        }
        assert!(
            Instant::now() < deadline,
            "{order:?}: the passing task never finished"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    let _ = release.send(());
    drop(peer);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
    passing
}

/// What every order shares: the head waits throughout, a waiter on the
/// head's own connection does not pass it, and the third connection's send
/// fails with the abort.
fn assert_head_kept(order: ringline::SendAdmissionOrder, passing: &Passing) {
    assert!(passing.head_waited_first, "{order:?}: {passing:?}");
    assert!(passing.same_connection_waited, "{order:?}: {passing:?}");
    assert!(passing.head_waited_last, "{order:?}: {passing:?}");
    assert_eq!(
        passing.staller_settled.as_deref(),
        Some(ABORTED),
        "{order:?}: {passing:?}"
    );
}

/// Under `Strict` nothing passes the waiting head, and the counters stay
/// zero.
#[test]
fn strict_admits_nothing_past_a_waiting_head() {
    let order = ringline::SendAdmissionOrder::Strict;
    let passing = run_order(order);
    assert_head_kept(order, &passing);
    assert_eq!(passing.reader, None, "{passing:?}");
    assert_eq!(passing.staller, None, "{passing:?}");
    assert_eq!(passing.counters, ringline::SendAdmissionCounters::default());
}

/// Under `FirstFit`, `Bounded` and `Budget(2)` a send that fits passes the
/// waiting head from any connection but the head's. The slots it holds count
/// in B until it settles, or until its connection is aborted while it is
/// queued behind a full socket; either way B returns to 0.
#[test]
fn a_send_that_fits_passes_a_waiting_head_and_returns_its_slots() {
    for order in [
        ringline::SendAdmissionOrder::FirstFit,
        ringline::SendAdmissionOrder::Bounded,
        ringline::SendAdmissionOrder::Budget(2),
    ] {
        let passing = run_order(order);
        assert_head_kept(order, &passing);
        assert_eq!(passing.reader, Some(true), "{order:?}: {passing:?}");
        assert_eq!(
            passing.reader_settled.as_deref(),
            Some(format!("Ok: {SLOT}").as_str()),
            "{order:?}"
        );
        assert_eq!(passing.held_after_settle, Some(0), "{order:?}: settled");
        assert_eq!(passing.staller, Some(true), "{order:?}: {passing:?}");
        assert_eq!(
            passing.held_around_abort,
            Some((1, 0)),
            "{order:?}: aborted"
        );
        assert_eq!(passing.counters.passing_admissions, 2, "{order:?}");
        assert_eq!(passing.counters.passing_slots, 2, "{order:?}");
        assert_eq!(passing.counters.passing_held, 0, "{order:?}");
        assert_eq!(passing.counters.passing_held_max, 1, "{order:?}");
    }
}

/// Under `BoundedIdle` and `BudgetIdle(2)` the send to the reading peer
/// passes, and the send queued behind a full socket, on a connection with a
/// send in flight, does not.
#[test]
fn an_idle_order_passes_only_from_an_idle_connection() {
    for order in [
        ringline::SendAdmissionOrder::BoundedIdle,
        ringline::SendAdmissionOrder::BudgetIdle(2),
    ] {
        let passing = run_order(order);
        assert_head_kept(order, &passing);
        assert_eq!(passing.reader, Some(true), "{order:?}: {passing:?}");
        assert_eq!(passing.held_after_settle, Some(0), "{order:?}");
        assert_eq!(passing.staller, None, "{order:?}: {passing:?}");
        assert_eq!(passing.counters.passing_admissions, 1, "{order:?}");
        assert_eq!(passing.counters.passing_held, 0, "{order:?}");
    }
}
