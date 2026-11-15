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

use std::cell::Cell;
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::rc::Rc;
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

/// Under `Strict` nothing passes the waiting head, and every count stays
/// zero; the main region's free slots are the four the stalled sends leave.
#[test]
fn strict_admits_nothing_past_a_waiting_head() {
    let order = ringline::SendAdmissionOrder::Strict;
    let passing = run_order(order);
    assert_head_kept(order, &passing);
    assert_eq!(passing.reader, None, "{passing:?}");
    assert_eq!(passing.staller, None, "{passing:?}");
    assert_eq!(
        passing.counters,
        ringline::SendAdmissionCounters {
            main_free: 4,
            ..ringline::SendAdmissionCounters::default()
        }
    );
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

// ── SplitPool ───────────────────────────────────────────────────────────

/// Bytes of a send that takes a slot of the small region.
const TINY: usize = 512;

/// What a worker under `SplitPool` did, for the test thread to check.
#[derive(Debug, Default)]
struct Split {
    /// The large head was waiting after its first poll.
    head_waited: bool,
    /// The small send behind the head on the head's own connection was
    /// waiting after its first poll.
    small_behind_head_waited: bool,
    /// The small sends to a reading peer admitted at their first poll while
    /// the head waited, of three, and whether any reported passing a head.
    small_admitted_at_once: usize,
    small_passed_head: bool,
    /// The two small sends queued behind a full socket were admitted.
    stalled_small_admitted: usize,
    /// With the small region full, a small send on the stalled connection,
    /// then one on the reading connection, were waiting.
    small_waited_when_full: (bool, bool),
    /// With the large head gone, a main send on the reading connection was
    /// admitted, and one on the stalled connection, behind its small waiter,
    /// was still waiting.
    main_admitted_after_head: bool,
    main_behind_small_waited: bool,
    /// Once the stalled connection was aborted, the small send behind the
    /// head on the head's connection was admitted.
    small_behind_head_admitted: bool,
    counters: ringline::SendAdmissionCounters,
}

static SPLIT: Mutex<Option<Split>> = Mutex::new(None);

/// Runs [`split_the_pool`] on its connection.
struct Splitter;

impl AsyncEventHandler for Splitter {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            let split = split_the_pool(&mut conn).await;
            *SPLIT.lock().unwrap() = Some(split);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Splitter
    }
}

/// On `conn`, whose peer does not read, the main region's four queued sends
/// and a waiting head of six slots, as in [`pass_the_head`]; then small sends
/// to a reading peer and to a stalled one, a main send on each once the head
/// is gone, and the abort of the stalled one.
async fn split_the_pool(conn: &mut Connection) -> Split {
    static QUEUED: [u8; SLOT] = [1; SLOT];
    static HEAD: [u8; 6 * SLOT] = [2; 6 * SLOT];
    static SMALL: [u8; TINY] = [3; TINY];
    let (reader_addr, staller_addr) = PEERS.lock().unwrap().expect("peers");
    let mut out = Split::default();

    fill(conn);
    let mut queued = Vec::new();
    for _ in 0..4 {
        queued.push(conn.send_admitted(&QUEUED).await.expect("admitted"));
    }
    let mut head = conn.send_admitted(&HEAD);
    out.head_waited = poll_once(&mut head).await.is_pending();
    let small_behind_head = park(conn.send_admitted(&SMALL)).await;
    out.small_behind_head_waited =
        small_behind_head.first_waited.get() && !small_behind_head.resolved();

    let mut reader = ringline::connect(reader_addr)
        .await
        .expect("connect the reader");
    for _ in 0..3 {
        let mut sent = reader.send_admitted(&SMALL);
        let first = poll_once(&mut sent).await;
        if let Poll::Ready(Ok(completion)) = first {
            out.small_admitted_at_once += 1;
            out.small_passed_head |= completion.passed_head();
            let _ = completion.await;
        }
    }

    let mut staller = ringline::connect(staller_addr)
        .await
        .expect("connect the staller");
    fill(&mut staller);
    let mut stalled = Vec::new();
    for _ in 0..2 {
        let mut sent = staller.send_admitted(&SMALL);
        if let Poll::Ready(Ok(completion)) = poll_once(&mut sent).await {
            out.stalled_small_admitted += 1;
            stalled.push(completion);
        }
    }
    let mut stalled_small = staller.send_admitted(&SMALL);
    let mut reader_small = reader.send_admitted(&SMALL);
    out.small_waited_when_full = (
        poll_once(&mut stalled_small).await.is_pending(),
        poll_once(&mut reader_small).await.is_pending(),
    );
    let mut stalled_main = staller.send_admitted(&QUEUED);
    let _ = poll_once(&mut stalled_main).await;
    drop(reader_small);
    let reader_main = park(reader.send_admitted(&QUEUED)).await;

    drop(head);
    out.main_admitted_after_head = resolved_soon(&reader_main).await == Some(true);
    out.main_behind_small_waited = poll_once(&mut stalled_main).await.is_pending();

    staller.abort();
    out.small_behind_head_admitted = resolved_soon(&small_behind_head).await == Some(true);
    out.counters = ringline::send_admission_counters();
    conn.abort();
    out
}

/// Under `SplitPool` a large head waits in the main region while small sends
/// are admitted from the small region, whose own FIFO is strict once it is
/// full; each connection's sends keep their order across the two FIFOs, and
/// a waiter held back by its connection's earlier waiter holds back no one.
#[test]
fn split_pool_admits_small_sends_while_a_large_head_waits() {
    let _serial = serial();
    *SPLIT.lock().unwrap() = None;
    let reader = TcpListener::bind("127.0.0.1:0").expect("bind the reader");
    let staller = TcpListener::bind("127.0.0.1:0").expect("bind the staller");
    *PEERS.lock().unwrap() = Some((reader.local_addr().unwrap(), staller.local_addr().unwrap()));
    // Not joined, as in `run_order`.
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
        .send_admission_order(ringline::SendAdmissionOrder::SplitPool {
            small_slots: 2,
            small_slot_size: 1024,
        })
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Splitter>()
        .expect("launch");
    let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    peer.write_all(b"s").unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let split = loop {
        if let Some(split) = SPLIT.lock().unwrap().take() {
            break split;
        }
        assert!(
            Instant::now() < deadline,
            "the splitting task never finished"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    let _ = release.send(());
    drop(peer);
    runtime.shutdown();
    common::join_workers(&runtime, handles);

    assert!(split.head_waited, "{split:?}");
    assert!(
        split.small_behind_head_waited,
        "behind its connection's head: {split:?}"
    );
    assert_eq!(split.small_admitted_at_once, 3, "{split:?}");
    assert!(!split.small_passed_head, "a small send passes no head");
    assert_eq!(split.stalled_small_admitted, 2, "{split:?}");
    assert_eq!(split.small_waited_when_full, (true, true), "{split:?}");
    assert!(split.main_admitted_after_head, "{split:?}");
    assert!(
        split.main_behind_small_waited,
        "behind its connection's small waiter: {split:?}"
    );
    assert!(split.small_behind_head_admitted, "{split:?}");
    assert_eq!(split.counters.small_admissions, 6, "{split:?}");
    assert_eq!(split.counters.passing_admissions, 0, "{split:?}");
}

// ── Wakes of parked waiters ─────────────────────────────────────────────

/// A waiter that a task of its own waits for: whether its first poll left
/// it waiting, and, once it resolves, whether it was admitted.
#[derive(Clone, Default)]
struct Parked {
    first_waited: Rc<Cell<bool>>,
    admitted: Rc<Cell<Option<bool>>>,
}

impl Parked {
    /// The waiter has resolved.
    fn resolved(&self) -> bool {
        self.admitted.get().is_some()
    }
}

/// Polls `inner`, recording whether its first poll returned `Pending`.
struct RecordFirst<F> {
    inner: F,
    first_waited: Option<Rc<Cell<bool>>>,
}

impl<F: Future + Unpin> Future for RecordFirst<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<F::Output> {
        let polled = Pin::new(&mut self.inner).poll(cx);
        if let Some(first) = self.first_waited.take() {
            first.set(polled.is_pending());
        }
        polled
    }
}

/// Wait for `admission` in a new task, which the event loop polls again only
/// when something wakes it, and return what that task records. The task
/// makes the first poll before this returns.
async fn park(admission: ringline::Admission<'static>) -> Parked {
    let parked = Parked::default();
    let recorded = parked.clone();
    ringline::spawn(async move {
        let waited = RecordFirst {
            inner: admission,
            first_waited: Some(recorded.first_waited.clone()),
        };
        let admitted = waited.await;
        recorded.admitted.set(Some(admitted.is_ok()));
    })
    .expect("spawn the waiter");
    ringline::sleep(Duration::from_millis(5)).await;
    parked
}

/// Whether `parked` was admitted, once it resolves within 2 s, or `None`.
async fn resolved_soon(parked: &Parked) -> Option<bool> {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !parked.resolved() && Instant::now() < deadline {
        ringline::sleep(Duration::from_millis(1)).await;
    }
    parked.admitted.get()
}

/// What [`wake_the_parked`] saw.
#[derive(Debug, Default)]
struct Woken {
    /// (a) A passer parked for want of free slots: whether its first poll
    /// waited, then whether it was admitted once the slots came back.
    room: (bool, Option<bool>),
    /// (b) A passer parked behind an earlier waiter on its own connection:
    /// parked at first, then whether it was admitted once that waiter was
    /// dropped.
    own_connection: (bool, Option<bool>),
    /// (c) A passer on a connection whose last send waited for room and then
    /// drained: still parked 20 ms later, then whether it was admitted once a
    /// send on that connection completed without waiting.
    room_waited: (bool, Option<bool>),
}

static WOKEN: Mutex<Option<Woken>> = Mutex::new(None);

/// The test thread's listener whose peer reads only once told to, and the
/// way to tell it.
static DRAINER: Mutex<Option<(SocketAddr, std::sync::mpsc::Sender<()>)>> = Mutex::new(None);

/// Runs [`wake_the_parked`] on its connection; the first byte says which
/// part: `p` for (a) and (b), `i` for (c).
struct Wakeful;

impl AsyncEventHandler for Wakeful {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut part = 0u8;
            conn.with_data(|data| {
                part = data[0];
                ParseResult::Consumed(data.len())
            })
            .await;
            let woken = wake_the_parked(&mut conn, part == b'i').await;
            *WOKEN.lock().unwrap() = Some(woken);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Wakeful
    }
}

/// On `conn`, whose peer does not read, a head of six slots waits behind
/// four held, as in [`pass_the_head`]. Then, unless `idle`, (a) and (b) on a
/// connection whose peer reads; if `idle`, (c) on one whose peer starts
/// reading when told.
async fn wake_the_parked(conn: &mut Connection, idle: bool) -> Woken {
    static QUEUED: [u8; SLOT] = [1; SLOT];
    static HEAD: [u8; 6 * SLOT] = [2; 6 * SLOT];
    static ONE: [u8; SLOT] = [3; SLOT];
    static FOUR: [u8; 4 * SLOT] = [4; 4 * SLOT];
    static FIVE: [u8; 5 * SLOT] = [5; 5 * SLOT];
    let (reader_addr, _) = PEERS.lock().unwrap().expect("peers");
    let mut out = Woken::default();

    fill(conn);
    let mut queued = Vec::new();
    for _ in 0..4 {
        queued.push(conn.send_admitted(&QUEUED).await.expect("admitted"));
    }
    let mut head = conn.send_admitted(&HEAD);
    let _ = poll_once(&mut head).await;

    if !idle {
        let mut reader = ringline::connect(reader_addr)
            .await
            .expect("connect the reader");
        // (a) The four free slots go to one passer; the next parks until they
        // come back.
        let mut all_four = reader.send_admitted(&FOUR);
        if let Poll::Ready(Ok(sent)) = poll_once(&mut all_four).await {
            let waiter = park(reader.send_admitted(&ONE)).await;
            out.room.0 = waiter.first_waited.get();
            let _ = sent.await;
            out.room.1 = resolved_soon(&waiter).await;
        }
        // (b) A waiter that does not fit holds back the next on its
        // connection until it is dropped; no slot comes back meanwhile.
        ringline::sleep(Duration::from_millis(20)).await;
        let mut too_big = reader.send_admitted(&FIVE);
        let _ = poll_once(&mut too_big).await;
        let behind = park(reader.send_admitted(&ONE)).await;
        out.own_connection.0 = behind.first_waited.get() && !behind.resolved();
        drop(too_big);
        out.own_connection.1 = resolved_soon(&behind).await;
    } else {
        let (drainer_addr, drain) = DRAINER.lock().unwrap().take().expect("drainer");
        let mut drained = ringline::connect(drainer_addr)
            .await
            .expect("connect the drainer");
        // (c) More than the socket takes, so a write returns WouldBlock,
        // then an awaited send behind it; the peer then reads it all.
        fill(&mut drained);
        let last = drained.send(b"end").expect("queued");
        let _ = drain.send(());
        let _ = last.await;
        let waiter = park(drained.send_admitted(&ONE)).await;
        ringline::sleep(Duration::from_millis(20)).await;
        out.room_waited.0 = waiter.first_waited.get() && !waiter.resolved();
        let clean = drained.send(b"x").expect("queued");
        let _ = clean.await;
        out.room_waited.1 = resolved_soon(&waiter).await;
    }
    drop(head);
    conn.abort();
    out
}

/// Run [`Wakeful`] under `order`, `part` naming its part, and return what it
/// saw.
fn run_wakes(order: ringline::SendAdmissionOrder, part: u8) -> Woken {
    let _serial = serial();
    *WOKEN.lock().unwrap() = None;
    let reader = TcpListener::bind("127.0.0.1:0").expect("bind the reader");
    let drainer = TcpListener::bind("127.0.0.1:0").expect("bind the drainer");
    *PEERS.lock().unwrap() = Some((reader.local_addr().unwrap(), reader.local_addr().unwrap()));
    let (drain, start) = std::sync::mpsc::channel::<()>();
    *DRAINER.lock().unwrap() = Some((drainer.local_addr().unwrap(), drain));
    // Not joined, as in `run_order`.
    std::thread::spawn(move || {
        let (mut stream, _) = reader.accept().expect("accept the reader");
        let mut sink = Vec::new();
        let _ = stream.read_to_end(&mut sink);
    });
    std::thread::spawn(move || {
        let (mut stream, _) = drainer.accept().expect("accept the drainer");
        if start.recv_timeout(Duration::from_secs(20)).is_ok() {
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
        }
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
        .launch::<Wakeful>()
        .expect("launch");
    let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    peer.write_all(&[part]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let woken = loop {
        if let Some(woken) = WOKEN.lock().unwrap().take() {
            break woken;
        }
        assert!(Instant::now() < deadline, "the waking task never finished");
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(peer);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
    woken
}

/// The event loop wakes a parked passer and admits it: (a) once the slots
/// it waited for come back, and (b) once the earlier waiter on its own
/// connection is dropped, which returns no slot.
#[test]
fn the_event_loop_wakes_a_parked_passer() {
    let woken = run_wakes(ringline::SendAdmissionOrder::FirstFit, b'p');
    assert_eq!(woken.room, (true, Some(true)), "{woken:?}");
    assert_eq!(woken.own_connection, (true, Some(true)), "{woken:?}");
}

/// Under `BoundedIdle` a connection whose last send waited for room, and
/// has drained since, lets no send pass; once a send on it completes
/// without waiting, the event loop wakes the parked one and admits it.
#[test]
fn bounded_idle_admits_once_the_connection_s_last_send_did_not_wait() {
    let woken = run_wakes(ringline::SendAdmissionOrder::BoundedIdle, b'i');
    assert_eq!(woken.room_waited, (true, Some(true)), "{woken:?}");
}

// ── The latch of a head admitted after a large send ─────────────────────

/// What [`Latcher`] saw: whether the head waited, whether a send to a
/// reading peer was admitted past it at its first poll, and the latch
/// closings counted.
static LATCHED: Mutex<Option<(bool, Option<bool>, u64)>> = Mutex::new(None);

/// One 4-slot send on its connection, whose peer does not read, leaves four
/// of the eight slots free and B at 0; then a 6-slot head waits, and a
/// 1-slot send to a reading peer is polled once.
struct Latcher;

impl AsyncEventHandler for Latcher {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            static FOUR: [u8; 4 * SLOT] = [1; 4 * SLOT];
            static HEAD: [u8; 6 * SLOT] = [2; 6 * SLOT];
            static ONE: [u8; SLOT] = [3; SLOT];
            let (reader_addr, _) = PEERS.lock().unwrap().expect("peers");
            fill(&mut conn);
            let queued = conn.send_admitted(&FOUR).await.expect("admitted");
            let mut head = conn.send_admitted(&HEAD);
            let head_waited = poll_once(&mut head).await.is_pending();
            let mut reader = ringline::connect(reader_addr)
                .await
                .expect("connect the reader");
            let mut small = reader.send_admitted(&ONE);
            let passed = match poll_once(&mut small).await {
                Poll::Ready(Ok(sent)) => Some(sent.passed_head()),
                Poll::Ready(Err(_)) => Some(false),
                Poll::Pending => None,
            };
            let closings = ringline::send_admission_counters().latch_closings;
            drop(small);
            drop(head);
            drop(queued);
            conn.abort();
            *LATCHED.lock().unwrap() = Some((head_waited, passed, closings));
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Latcher
    }
}

/// Under `Bounded` and `BoundedIdle` a head that becomes the head after a
/// large admission starts with its latch open: F + B is 4 < 6 throughout its
/// wait, so a 1-slot send passes it and no latch closes, as under
/// `FirstFit`.
#[test]
fn a_head_after_a_large_admission_starts_with_its_latch_open() {
    for order in [
        ringline::SendAdmissionOrder::FirstFit,
        ringline::SendAdmissionOrder::Bounded,
        ringline::SendAdmissionOrder::BoundedIdle,
    ] {
        let _serial = serial();
        *LATCHED.lock().unwrap() = None;
        let reader = TcpListener::bind("127.0.0.1:0").expect("bind the reader");
        *PEERS.lock().unwrap() = Some((reader.local_addr().unwrap(), reader.local_addr().unwrap()));
        // Not joined, as in `run_order`.
        std::thread::spawn(move || {
            let (mut stream, _) = reader.accept().expect("accept the reader");
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
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
            .launch::<Latcher>()
            .expect("launch");
        let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
        peer.write_all(b"l").unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let seen = loop {
            if let Some(seen) = LATCHED.lock().unwrap().take() {
                break seen;
            }
            assert!(
                Instant::now() < deadline,
                "{order:?}: the task never finished"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        drop(peer);
        runtime.shutdown();
        common::join_workers(&runtime, handles);
        assert_eq!(seen, (true, Some(true), 0), "{order:?}");
    }
}

// ── An older waiter its task does not poll ──────────────────────────────

/// What [`Unpolled`] saw: whether the older waiter's first poll waited,
/// whether the newer one's did, and whether the newer one was admitted
/// while the older one was still held, unpolled.
static UNPOLLED: Mutex<Option<(bool, bool, Option<bool>)>> = Mutex::new(None);

/// On its connection, whose peer does not read, four slots held and a
/// 6-slot head waiting, as in [`pass_the_head`]. A 4-slot send to a reading
/// peer takes the four free slots; a 1-slot waiter on another connection is
/// polled once and then held unpolled; once the four slots come back, a
/// 4-slot waiter is parked.
struct Unpolled;

impl AsyncEventHandler for Unpolled {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            static QUEUED: [u8; SLOT] = [1; SLOT];
            static HEAD: [u8; 6 * SLOT] = [2; 6 * SLOT];
            static ONE: [u8; SLOT] = [3; SLOT];
            static FOUR: [u8; 4 * SLOT] = [4; 4 * SLOT];
            let (first_addr, second_addr) = PEERS.lock().unwrap().expect("peers");
            fill(&mut conn);
            let mut queued = Vec::new();
            for _ in 0..4 {
                queued.push(conn.send_admitted(&QUEUED).await.expect("admitted"));
            }
            let mut head = conn.send_admitted(&HEAD);
            let _ = poll_once(&mut head).await;
            let mut first = ringline::connect(first_addr)
                .await
                .expect("connect the first reader");
            let mut second = ringline::connect(second_addr)
                .await
                .expect("connect the second reader");
            let mut all_four = first.send_admitted(&FOUR);
            let Poll::Ready(Ok(sent)) = poll_once(&mut all_four).await else {
                *UNPOLLED.lock().unwrap() = Some((false, false, None));
                return;
            };
            let mut older = second.send_admitted(&ONE);
            let older_waited = poll_once(&mut older).await.is_pending();
            let _ = sent.await;
            let newer = park(first.send_admitted(&FOUR)).await;
            let newer_waited = newer.first_waited.get();
            let admitted = resolved_soon(&newer).await;
            drop(older);
            drop(head);
            conn.abort();
            *UNPOLLED.lock().unwrap() = Some((older_waited, newer_waited, admitted));
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Unpolled
    }
}

/// Under `FirstFit` an older waiter that its task holds without polling
/// keeps the room it was woken for only until the next scan: a newer waiter
/// that fits is then admitted while the older one is still held.
#[test]
fn an_older_waiter_its_task_does_not_poll_does_not_hold_back_a_newer_one() {
    let _serial = serial();
    *UNPOLLED.lock().unwrap() = None;
    let first = TcpListener::bind("127.0.0.1:0").expect("bind the first reader");
    let second = TcpListener::bind("127.0.0.1:0").expect("bind the second reader");
    *PEERS.lock().unwrap() = Some((first.local_addr().unwrap(), second.local_addr().unwrap()));
    // Not joined, as in `run_order`.
    for listener in [first, second] {
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept a reader");
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink);
        });
    }
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32)
        .send_admission_order(ringline::SendAdmissionOrder::FirstFit)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Unpolled>()
        .expect("launch");
    let mut peer = TcpStream::connect(runtime.bound_addr().expect("bound")).expect("connect");
    peer.write_all(b"u").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let seen = loop {
        if let Some(seen) = UNPOLLED.lock().unwrap().take() {
            break seen;
        }
        assert!(Instant::now() < deadline, "the task never finished");
        std::thread::sleep(Duration::from_millis(1));
    };
    drop(peer);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
    assert_eq!(
        seen,
        (true, true, Some(true)),
        "(older waited, newer waited, newer admitted)"
    );
}

// ── SplitPool and TLS ───────────────────────────────────────────────────

/// What [`TlsSplitter`] saw: the result of a small bounded send on its TLS
/// connection, and the small FIFO's admissions after it.
static TLS_SPLIT: Mutex<Option<(String, u64)>> = Mutex::new(None);

/// Reads one request on its TLS connection and answers it with a small
/// bounded send.
struct TlsSplitter;

impl AsyncEventHandler for TlsSplitter {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            let sent = match conn.send_admitted(b"small reply").await {
                Ok(sent) => outcome(sent.await),
                Err(e) => format!("refused: {e}"),
            };
            let admissions = ringline::send_admission_counters().small_admissions;
            *TLS_SPLIT.lock().unwrap() = Some((sent, admissions));
            let _ = conn
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsSplitter
    }
}

/// Under `SplitPool` a bounded send on a TLS connection takes the main
/// region whatever its size, so its records reach the peer encrypted and the
/// small FIFO admits nothing.
#[test]
fn split_pool_keeps_tls_sends_in_the_main_region() {
    use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName};

    let _serial = serial();
    *TLS_SPLIT.lock().unwrap() = None;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let der = CertificateDer::from(cert.cert);
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let tls = ringline::TlsConfig::builder()
        .cert(vec![der.clone()], key.into())
        .build()
        .expect("tls config");
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32)
        .send_admission_order(ringline::SendAdmissionOrder::SplitPool {
            small_slots: 2,
            small_slot_size: 1024,
        })
        .tls(tls)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<TlsSplitter>()
        .expect("launch");

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
    let mut reply = [0u8; 11];
    stream.read_exact(&mut reply).expect("the reply, decrypted");
    assert_eq!(&reply, b"small reply");

    let deadline = Instant::now() + Duration::from_secs(10);
    let seen = loop {
        if let Some(seen) = TLS_SPLIT.lock().unwrap().take() {
            break seen;
        }
        assert!(Instant::now() < deadline, "the TLS task never finished");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(
        seen,
        ("Ok: 11".to_string(), 0),
        "(the send, small admissions)"
    );
    drop(stream);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}
