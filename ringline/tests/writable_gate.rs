//! `SendHalf::writable`, the wait for room in a socket that a server awaits
//! before each piece of a large reply, so that a piece takes send-pool slots
//! only once its socket has room, and `SendCompletion::waited_for_room`, on
//! whichever backend the build has.
//!
//! Each test opens one connection to a one-worker server. The first byte the
//! client writes picks what the server does. Most cases fill the server's
//! socket first: the server sends 16 KiB at a time, eight sends a
//! millisecond, to a client that does not read, until the last send reads
//! `waited_for_room` before it has settled. The unit tests in the io_uring
//! event loop cover the multishot poll and its probe with injected
//! completions.
//!
//! The client leaves its receive buffer to the kernel. On Linux, a
//! `SO_RCVBUF` cut to 4096 after the connect keeps the client's window below
//! the server's MSS, which the kernel caps at half the largest window the
//! client has advertised (about 64 KiB at the handshake). The server then
//! sends only when its zero-window persist timer fires, a few KiB each
//! time, so the full socket takes minutes to drain and reports no `POLLOUT`
//! within the tests' bounds. `loopback_drain_with_a_receive_buffer_shrunk_after_connect`
//! measures this with plain sockets.
//!
//! The tests share the statics below, and `cargo test` runs them in one
//! process.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

mod common;

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// What the server saw, for the test thread to check.
#[derive(Debug, Default)]
struct Seen {
    /// `waited_for_room` of a 5-byte send to an empty socket, once settled.
    first_waited: Option<bool>,
    /// Whether the gate was still pending 200 ms after the socket filled.
    pending_while_full: Option<bool>,
    /// The gate's result: `Ok`, the error's kind, or `TimedOut` if the
    /// server's bound on it elapsed.
    gate: Option<Result<(), io::ErrorKind>>,
    /// When `abort` returned, when the server told the client to drain or
    /// reset (or, for an empty socket, began the gate), and when the gate
    /// resolved.
    aborted_at: Option<Instant>,
    filled_at: Option<Instant>,
    resolved_at: Option<Instant>,
    /// When `close` returned.
    closed_at: Option<Instant>,
    /// Whether each of two sends made before the gate had settled when it
    /// resolved.
    queued_settled: Option<(bool, bool)>,
    /// Whether a send of the second fill, which should go in whole, waited.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    refill_waited: Option<bool>,
    /// Why the server gave up before the gate.
    note: Option<String>,
}

static SEEN: Mutex<Option<Seen>> = Mutex::new(None);
/// The server has filled the socket and its gate waits.
static FILLED: AtomicBool = AtomicBool::new(false);
/// The bytes the server sent in its first fill.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
static TOTAL: AtomicUsize = AtomicUsize::new(0);
/// The client has read the first fill in full.
static READ_ALL: AtomicBool = AtomicBool::new(false);
/// The server has filled the socket a second time, with whole sends.
static REFILLED: AtomicBool = AtomicBool::new(false);

const CHUNK_LEN: usize = 16 * 1024;
const SLOT: u32 = 16 * 1024;
const SLOTS: u16 = 64;
/// At most 128 MiB: loopback buffers take a few MiB.
const MAX_ROUNDS: usize = 1024;

fn record(f: impl FnOnce(&mut Seen)) {
    let mut seen = SEEN.lock().unwrap();
    f(seen.get_or_insert_with(Seen::default));
}

fn gate_result(r: Result<io::Result<()>, ringline::Elapsed>) -> Result<(), io::ErrorKind> {
    match r {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.kind()),
        Err(_elapsed) => Err(io::ErrorKind::TimedOut),
    }
}

/// Send until the socket is full: 16 KiB a send, eight sends a millisecond,
/// until the last one reads `waited_for_room` before it settles. Panics if
/// that does not happen within `MAX_ROUNDS`.
///
/// The completions are dropped, so no send's completion wakes the task: a
/// gate the task then awaits ends its wait only through its own wakes.
async fn fill(conn: &mut Connection) {
    static CHUNK: [u8; CHUNK_LEN] = [7; CHUNK_LEN];
    for _ in 0..MAX_ROUNDS {
        let mut last = None;
        for _ in 0..8 {
            last = Some(conn.send(&CHUNK).expect("send"));
        }
        ringline::sleep(Duration::from_millis(1)).await;
        if last.expect("eight sends").waited_for_room() {
            return;
        }
    }
    panic!("no send waited for room after {MAX_ROUNDS} rounds");
}

/// `fill`, keeping the last send's completion and counting the bytes sent:
/// before the round whose last send waited for room, and in all.
async fn fill_keeping(conn: &mut Connection) -> (usize, usize, ringline::SendCompletion) {
    static CHUNK: [u8; CHUNK_LEN] = [7; CHUNK_LEN];
    let mut sent = 0;
    for _ in 0..MAX_ROUNDS {
        let before = sent;
        let mut last = None;
        for _ in 0..8 {
            last = Some(conn.send(&CHUNK).expect("send"));
            sent += CHUNK_LEN;
        }
        ringline::sleep(Duration::from_millis(1)).await;
        let last = last.expect("eight sends");
        if last.waited_for_room() {
            return (before, sent, last);
        }
    }
    panic!("no send waited for room after {MAX_ROUNDS} rounds");
}

/// Whether `sent` has settled, from one poll. Poll each completion once.
async fn settled(sent: &mut ringline::SendCompletion) -> bool {
    std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *sent).poll(cx).is_ready())).await
}

/// A 5-byte send to an empty socket settles without waiting for room.
async fn first_send(conn: &mut Connection) {
    let mut first = conn.send(b"first").expect("send");
    let settled = ringline::timeout(Duration::from_secs(5), &mut first).await;
    assert!(matches!(settled, Ok(Ok(5))), "{settled:?}");
    let waited = first.waited_for_room();
    record(|s| s.first_waited = Some(waited));
}

struct Gate;

impl AsyncEventHandler for Gate {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut cmd = 0u8;
            let n = conn
                .with_data(|data| {
                    cmd = data[0];
                    ParseResult::Consumed(1)
                })
                .await;
            if n == 0 {
                return;
            }
            match cmd {
                b'w' => {
                    // Before any write, so that no writable event from the
                    // peer's acknowledgement can wake a gate that did not look
                    // at the socket on its first poll.
                    let began = Instant::now();
                    let gate = ringline::timeout(Duration::from_secs(1), conn.writable()).await;
                    let gate = gate_result(gate);
                    record(|s| {
                        s.gate = Some(gate);
                        s.filled_at = Some(began);
                        s.resolved_at = Some(Instant::now());
                    });
                    first_send(&mut conn).await;
                }
                b'd' => full_then_drain(&mut conn).await,
                b'a' => abort_while_waiting(&mut conn).await,
                b'r' => {
                    fill(&mut conn).await;
                    FILLED.store(true, Ordering::SeqCst);
                    let gate = ringline::timeout(Duration::from_secs(10), conn.writable()).await;
                    let gate = gate_result(gate);
                    record(|s| {
                        s.gate = Some(gate);
                        s.resolved_at = Some(Instant::now());
                    });
                }
                b'q' => queued_bytes_come_first(&mut conn).await,
                b'p' => probe_on_a_drained_multishot_connection(&mut conn).await,
                b'c' => close_while_waiting(&mut conn).await,
                #[cfg(target_os = "linux")]
                b'f' => whole_sends_fill(&mut conn).await,
                b's' => {
                    // The gate is polled first after the connection is
                    // released.
                    let gate = conn.writable();
                    ringline::spawn(async move {
                        ringline::sleep(Duration::from_millis(300)).await;
                        let gate = ringline::timeout(Duration::from_secs(1), gate).await;
                        record(|s| s.gate = Some(gate_result(gate)));
                    })
                    .expect("spawn");
                }
                _ => {}
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Gate
    }
}

/// Fill the socket, check that the gate waits, then let the client drain
/// and check that it resolves.
async fn full_then_drain(conn: &mut Connection) {
    first_send(conn).await;
    fill(conn).await;
    let mut gate = std::pin::pin!(conn.writable());
    let early = ringline::timeout(Duration::from_millis(200), gate.as_mut()).await;
    record(|s| {
        s.pending_while_full = Some(early.is_err());
        s.filled_at = Some(Instant::now());
    });
    FILLED.store(true, Ordering::SeqCst);
    let gate = ringline::timeout(Duration::from_secs(10), gate).await;
    let gate = gate_result(gate);
    record(|s| {
        s.gate = Some(gate);
        s.resolved_at = Some(Instant::now());
    });
}

/// Fill the socket, wait on the gate from another task, and abort.
async fn abort_while_waiting(conn: &mut Connection) {
    fill(conn).await;
    let gate = conn.writable();
    ringline::spawn(async move {
        let gate = ringline::timeout(Duration::from_secs(5), gate).await;
        let gate = gate_result(gate);
        record(|s| {
            s.gate = Some(gate);
            s.resolved_at = Some(Instant::now());
        });
    })
    .expect("spawn");
    ringline::sleep(Duration::from_millis(200)).await;
    let pending = SEEN
        .lock()
        .unwrap()
        .as_ref()
        .is_none_or(|s| s.gate.is_none());
    record(|s| s.pending_while_full = Some(pending));
    conn.abort();
    record(|s| s.aborted_at = Some(Instant::now()));
}

/// Two 64 KiB sends, then the gate, in one poll: the gate resolves only
/// once both are written and the socket has room.
async fn queued_bytes_come_first(conn: &mut Connection) {
    static PART: [u8; 4 * CHUNK_LEN] = [3; 4 * CHUNK_LEN];
    let mut a = conn.send(&PART).expect("send");
    let mut b = conn.send(&PART).expect("send");
    let gate = ringline::timeout(Duration::from_secs(5), conn.writable()).await;
    let settled_a = settled(&mut a).await;
    let settled_b = settled(&mut b).await;
    record(|s| {
        s.gate = Some(gate_result(gate));
        s.queued_settled = Some((settled_a, settled_b));
    });
}

/// Fill the socket of a peer that has half-closed and reads, wait for the
/// last send to settle and the socket to drain, then wait on the gate. On
/// io_uring the connection's poll is multishot by then and its POLLOUT
/// changes have been reported, so only the probe reports the room.
async fn probe_on_a_drained_multishot_connection(conn: &mut Connection) {
    let (_, _, last) = fill_keeping(conn).await;
    let settled = ringline::timeout(Duration::from_secs(10), last).await;
    if !matches!(settled, Ok(Ok(_))) {
        record(|s| s.note = Some(format!("the fill did not settle: {settled:?}")));
        return;
    }
    ringline::sleep(Duration::from_millis(100)).await;
    let began = Instant::now();
    let gate = ringline::timeout(Duration::from_secs(1), conn.writable()).await;
    let gate = gate_result(gate);
    record(|s| {
        s.gate = Some(gate);
        s.filled_at = Some(began);
        s.resolved_at = Some(Instant::now());
    });
}

/// Fill the socket, wait on the gate from another task, and request a
/// close, whose drain to a peer that does not read never ends.
async fn close_while_waiting(conn: &mut Connection) {
    fill(conn).await;
    let gate = conn.writable();
    ringline::spawn(async move {
        let gate = ringline::timeout(Duration::from_secs(5), gate).await;
        let gate = gate_result(gate);
        record(|s| {
            s.gate = Some(gate);
            s.resolved_at = Some(Instant::now());
        });
    })
    .expect("spawn");
    ringline::sleep(Duration::from_millis(200)).await;
    let pending = SEEN
        .lock()
        .unwrap()
        .as_ref()
        .is_none_or(|s| s.gate.is_none());
    record(|s| s.pending_while_full = Some(pending));
    conn.close();
    record(|s| s.closed_at = Some(Instant::now()));
}

/// Linux: with the peer half-closed, fill the socket until a send waits,
/// which leaves the connection's poll multishot; let the client read it all,
/// so the poll reports POLLOUT; then fill it again with whole sends, to 0.9
/// of what the first fill held before its first wait, which on Linux leaves
/// more than two thirds of the send buffer queued and no send waiting. The
/// gate must wait until the client reads again.
#[cfg(target_os = "linux")]
async fn whole_sends_fill(conn: &mut Connection) {
    static CHUNK: [u8; CHUNK_LEN] = [7; CHUNK_LEN];
    let (capacity, total, last) = fill_keeping(conn).await;
    TOTAL.store(total, Ordering::SeqCst);
    FILLED.store(true, Ordering::SeqCst);
    let settled = ringline::timeout(Duration::from_secs(15), last).await;
    if !matches!(settled, Ok(Ok(_))) {
        record(|s| s.note = Some(format!("the first fill did not settle: {settled:?}")));
        return;
    }
    while !READ_ALL.load(Ordering::SeqCst) {
        ringline::sleep(Duration::from_millis(1)).await;
    }
    let mut refill = Vec::new();
    for _ in 0..capacity * 9 / 10 / CHUNK_LEN {
        refill.push(conn.send(&CHUNK).expect("send"));
    }
    let mut waited = false;
    for sent in refill.iter_mut() {
        let done = ringline::timeout(Duration::from_secs(5), &mut *sent).await;
        if !matches!(done, Ok(Ok(_))) {
            record(|s| {
                s.note = Some(format!(
                    "a send of the second fill did not settle: {done:?}"
                ))
            });
            return;
        }
        waited |= sent.waited_for_room();
    }
    let mut gate = std::pin::pin!(conn.writable());
    let early = ringline::timeout(Duration::from_millis(300), gate.as_mut()).await;
    record(|s| {
        s.refill_waited = Some(waited);
        s.pending_while_full = Some(early.is_err());
        s.filled_at = Some(Instant::now());
    });
    REFILLED.store(true, Ordering::SeqCst);
    let gate = ringline::timeout(Duration::from_secs(10), gate).await;
    let gate = gate_result(gate);
    record(|s| {
        s.gate = Some(gate);
        s.resolved_at = Some(Instant::now());
    });
}

fn launch() -> (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(SLOTS, SLOT)
        .build()
        .expect("valid config");
    RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Gate>()
        .expect("launch")
}

#[cfg(target_os = "linux")]
fn set_rcvbuf(s: &TcpStream, size: i32) {
    // SAFETY: a live socket, and `size` outlives the call.
    let rc = unsafe {
        libc::setsockopt(
            s.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&size as *const i32).cast(),
            std::mem::size_of::<i32>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "setsockopt(SO_RCVBUF) failed");
}

/// Close `client` with a reset: `SO_LINGER` with a zero timeout.
fn reset(client: TcpStream) {
    let linger = libc::linger {
        l_onoff: 1,
        l_linger: 0,
    };
    // SAFETY: a live socket, and `linger` outlives the call.
    let rc = unsafe {
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            (&linger as *const libc::linger).cast(),
            std::mem::size_of::<libc::linger>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "setsockopt(SO_LINGER) failed");
    drop(client);
}

/// A connection to a fresh server that does not read, with the first byte
/// written.
fn start(
    cmd: u8,
) -> (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
    TcpStream,
) {
    *SEEN.lock().unwrap() = None;
    FILLED.store(false, Ordering::SeqCst);
    READ_ALL.store(false, Ordering::SeqCst);
    REFILLED.store(false, Ordering::SeqCst);
    let (runtime, handles) = launch();
    let addr = runtime.bound_addr().expect("bound");
    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(&[cmd]).unwrap();
    (runtime, handles, client)
}

/// Wait until `done` holds for what the server saw, for at most 15 s.
fn wait_for(what: &str, done: impl Fn(&Seen) -> bool) -> Seen {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        {
            let mut seen = SEEN.lock().unwrap();
            if seen.as_ref().is_some_and(&done) {
                return seen.take().unwrap();
            }
        }
        assert!(Instant::now() < deadline, "the server never {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_filled() {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !FILLED.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the server never filled the socket"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Read until the server closes, for at most 15 s.
fn drain(client: &mut TcpStream) {
    client
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let began = Instant::now();
    let mut buf = vec![0u8; 1 << 16];
    let mut read = 0usize;
    let mut first_mib = None;
    loop {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                read += n;
                if read >= 1 << 20 && first_mib.is_none() {
                    first_mib = Some(began.elapsed());
                }
            }
            Err(e) => panic!("drain: {e} after {read} bytes in {:?}", began.elapsed()),
        }
    }
    eprintln!(
        "drain: {read} bytes in {:?}, the first MiB in {first_mib:?}",
        began.elapsed()
    );
}

/// The gate resolved within 5 s of the client starting to drain, well inside
/// the server's 10 s bound. A `timeout` polls its future once more when its
/// bound elapses, so a gate that nothing woke would still resolve `Ok` then.
fn assert_resolved_promptly(seen: &Seen) {
    let took = seen.resolved_at.unwrap() - seen.filled_at.unwrap();
    assert!(
        took < Duration::from_secs(5),
        "resolved {took:?} after the drain began"
    );
}

/// A fresh socket is writable: the gate resolves at once, and a send that
/// went in whole did not wait for room.
#[test]
fn an_empty_socket_is_writable() {
    let _serial = serial();
    let (runtime, handles, _client) = start(b'w');
    let seen = wait_for("sent the first send", |s| s.first_waited.is_some());
    assert_eq!(seen.first_waited, Some(false));
    assert_eq!(seen.gate, Some(Ok(())));
    // Well inside the 1 s bound, whose expiry polls the gate once more.
    let took = seen.resolved_at.unwrap() - seen.filled_at.unwrap();
    assert!(took < Duration::from_millis(500), "resolved after {took:?}");
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// The gate waits while the socket is full and the peer does not read, and
/// resolves once the peer drains it.
#[test]
fn a_full_socket_that_drains() {
    let _serial = serial();
    let (runtime, handles, mut client) = start(b'd');
    wait_filled();
    drain(&mut client);
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(seen.first_waited, Some(false));
    assert_eq!(
        seen.pending_while_full,
        Some(true),
        "it resolved on a full socket"
    );
    assert_eq!(seen.gate, Some(Ok(())));
    assert_resolved_promptly(&seen);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// As above with a peer that has half-closed: on io_uring the connection's
/// poll is multishot by the time the gate waits.
#[test]
fn a_half_closed_peer_with_a_full_socket() {
    let _serial = serial();
    let (runtime, handles, mut client) = start(b'd');
    client.shutdown(Shutdown::Write).unwrap();
    wait_filled();
    drain(&mut client);
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(
        seen.pending_while_full,
        Some(true),
        "it resolved on a full socket"
    );
    assert_eq!(seen.gate, Some(Ok(())));
    assert_resolved_promptly(&seen);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// `abort` while another task's gate waits fails the gate with
/// `ConnectionAborted`, well before the 5 s bound.
#[test]
fn an_abort_while_waiting() {
    let _serial = serial();
    let (runtime, handles, client) = start(b'a');
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(
        seen.pending_while_full,
        Some(true),
        "it resolved before the abort"
    );
    assert_eq!(seen.gate, Some(Err(io::ErrorKind::ConnectionAborted)));
    let took = seen.resolved_at.unwrap() - seen.aborted_at.unwrap();
    assert!(
        took < Duration::from_secs(1),
        "resolved {took:?} after the abort"
    );
    drop(client);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// A peer that resets while the gate waits fails it with the error the
/// connection recorded: `ConnectionReset` from a receive or the poll, or
/// `BrokenPipe` where a write after the reset recorded `EPIPE` first, as mio's
/// flush does on macOS.
#[test]
fn a_peer_reset_while_waiting() {
    let _serial = serial();
    let (runtime, handles, client) = start(b'r');
    wait_filled();
    std::thread::sleep(Duration::from_millis(100));
    let reset_at = Instant::now();
    reset(client);
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    eprintln!("reset: the gate resolved {:?}", seen.gate);
    assert!(
        matches!(
            seen.gate,
            Some(Err(
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ))
        ),
        "{:?}",
        seen.gate
    );
    let took = seen.resolved_at.unwrap() - reset_at;
    assert!(
        took < Duration::from_secs(5),
        "resolved {took:?} after the reset"
    );
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// A gate first polled after its connection was released fails with
/// `NotConnected`.
#[test]
fn a_stale_connection() {
    let _serial = serial();
    let (runtime, handles, client) = start(b's');
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(seen.gate, Some(Err(io::ErrorKind::NotConnected)));
    drop(client);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// Not part of the suite: a check, with plain sockets and no ringline, of why
/// the client above leaves its receive buffer to the kernel. A server socket
/// filled to `WouldBlock` towards a client that does not read reports
/// `POLLOUT` within 1 s of the client starting to read when the client keeps
/// the kernel's buffer, and not within 2 s when the client cut its
/// `SO_RCVBUF` to 4096 after the connect. Run on a Linux host with
/// `cargo test -p ringline --test writable_gate loopback_drain -- --ignored --nocapture`.
#[cfg(target_os = "linux")]
#[test]
#[ignore = "a check of the kernel for a Linux host; run with --ignored"]
fn loopback_drain_with_a_receive_buffer_shrunk_after_connect() {
    let kernel = time_to_pollout(None);
    let shrunk = time_to_pollout(Some(4096));
    eprintln!(
        "POLLOUT after the client began to read: kernel buffer {kernel:?}, 4096 after the connect {shrunk:?}"
    );
    assert!(
        kernel.is_some_and(|t| t < Duration::from_secs(1)),
        "with the kernel's buffer: {kernel:?}"
    );
    assert!(
        shrunk.is_none_or(|t| t > Duration::from_secs(2)),
        "with 4096 set after the connect: {shrunk:?}"
    );
}

/// Fill a loopback server socket to `WouldBlock` towards a client that does
/// not read, with the client's `SO_RCVBUF` set to `rcvbuf` after the connect;
/// wait 200 ms and fill it again, as the tests above do; then let the client
/// read, and return how long the server's socket took to report `POLLOUT`,
/// or `None` if it did not within 10 s.
#[cfg(target_os = "linux")]
fn time_to_pollout(rcvbuf: Option<i32>) -> Option<Duration> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (mut server, _) = listener.accept().unwrap();
    server.set_nodelay(true).unwrap();
    if let Some(size) = rcvbuf {
        set_rcvbuf(&client, size);
    }
    server.set_nonblocking(true).unwrap();
    let chunk = [7u8; CHUNK_LEN];
    let fill = |server: &mut TcpStream| loop {
        match server.write(&chunk) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("fill: {e}"),
        }
    };
    fill(&mut server);
    std::thread::sleep(Duration::from_millis(200));
    fill(&mut server);
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let reader = {
        let stop = stop.clone();
        let mut client = client;
        std::thread::spawn(move || {
            client
                .set_read_timeout(Some(Duration::from_millis(100)))
                .unwrap();
            let mut buf = vec![0u8; 1 << 16];
            while !stop.load(Ordering::SeqCst) {
                match client.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(_) => break,
                }
            }
        })
    };
    let began = Instant::now();
    let mut pfd = libc::pollfd {
        fd: server.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: one valid `pollfd` for the call.
    let n = unsafe { libc::poll(&mut pfd, 1, 10_000) };
    let took = (n > 0 && pfd.revents & libc::POLLOUT != 0).then(|| began.elapsed());
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();
    took
}

/// Two sends made just before the gate, in the same poll, are written
/// before it resolves.
#[test]
fn sends_queued_before_the_gate_are_written_first() {
    let _serial = serial();
    let (runtime, handles, _client) = start(b'q');
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(seen.gate, Some(Ok(())));
    assert_eq!(
        seen.queued_settled,
        Some((true, true)),
        "the gate resolved before the sends"
    );
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// After the peer half-closes and the socket drains, the gate resolves at
/// once: on io_uring the multishot poll reports no change, and the probe
/// reports the room.
#[test]
fn a_drained_socket_after_a_half_close_is_writable() {
    let _serial = serial();
    let (runtime, handles, mut client) = start(b'p');
    client.shutdown(Shutdown::Write).unwrap();
    // Read at about 32 MiB/s, slower than the server fills, until the end.
    let reader = std::thread::spawn(move || {
        client
            .set_read_timeout(Some(Duration::from_secs(15)))
            .unwrap();
        let mut buf = vec![0u8; 1 << 16];
        while let Ok(n) = client.read(&mut buf) {
            if n == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    });
    let seen = wait_for("resolved the gate", |s| {
        s.gate.is_some() || s.note.is_some()
    });
    assert_eq!(seen.note, None);
    assert_eq!(seen.gate, Some(Ok(())));
    let took = seen.resolved_at.unwrap() - seen.filled_at.unwrap();
    assert!(took < Duration::from_millis(200), "resolved after {took:?}");
    runtime.shutdown();
    common::join_workers(&runtime, handles);
    reader.join().unwrap();
}

/// `close` while another task's gate waits fails the gate at once, though
/// the close's drain to a peer that does not read never ends.
#[test]
fn a_close_while_waiting() {
    let _serial = serial();
    let (runtime, handles, client) = start(b'c');
    let seen = wait_for("resolved the gate", |s| s.gate.is_some());
    assert_eq!(
        seen.pending_while_full,
        Some(true),
        "it resolved before the close"
    );
    assert_eq!(seen.gate, Some(Err(io::ErrorKind::NotConnected)));
    let took = seen.resolved_at.unwrap() - seen.closed_at.unwrap();
    assert!(
        took < Duration::from_secs(1),
        "resolved {took:?} after the close"
    );
    drop(client);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// Linux: a socket refilled by whole sends, with no send waiting and the
/// connection's poll multishot after a POLLOUT, is not writable until the
/// peer reads again. A gate that resolved on the poll's earlier POLLOUT
/// would let a piece into the full socket.
#[cfg(target_os = "linux")]
#[test]
fn a_socket_filled_by_whole_sends_is_not_writable() {
    let _serial = serial();
    *SEEN.lock().unwrap() = None;
    FILLED.store(false, Ordering::SeqCst);
    READ_ALL.store(false, Ordering::SeqCst);
    REFILLED.store(false, Ordering::SeqCst);
    let (runtime, handles) = launch();
    let addr = runtime.bound_addr().expect("bound");
    // Set before the connect, so the window and the server's MSS agree.
    let mut client = connect_with_rcvbuf(addr, 256 << 10);
    client.write_all(b"f").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    wait_filled();
    client
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mut left = TOTAL.load(Ordering::SeqCst);
    let mut buf = vec![0u8; 1 << 16];
    while left > 0 {
        let n = client
            .read(&mut buf[..left.min(1 << 16)])
            .expect("read the first fill");
        assert!(n > 0, "the server closed early");
        left -= n;
    }
    READ_ALL.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !REFILLED.load(Ordering::SeqCst) {
        if SEEN
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|s| s.note.is_some())
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the server never refilled the socket"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    drain(&mut client);
    let seen = wait_for("resolved the gate", |s| {
        s.gate.is_some() || s.note.is_some()
    });
    assert_eq!(seen.note, None);
    assert_eq!(
        seen.refill_waited,
        Some(false),
        "precondition: a send of the second fill waited for room"
    );
    assert_eq!(
        seen.pending_while_full,
        Some(true),
        "it resolved on a full socket"
    );
    assert_eq!(seen.gate, Some(Ok(())));
    assert_resolved_promptly(&seen);
    runtime.shutdown();
    common::join_workers(&runtime, handles);
}

/// Connect to `addr` with `SO_RCVBUF` set to `size` before the connect.
#[cfg(target_os = "linux")]
fn connect_with_rcvbuf(addr: std::net::SocketAddr, size: i32) -> TcpStream {
    use std::os::fd::FromRawFd;
    let std::net::SocketAddr::V4(v4) = addr else {
        panic!("the test binds IPv4 loopback");
    };
    // SAFETY: socket(2) takes no pointers.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
    // SAFETY: `fd` is a socket this function owns; the stream closes it.
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    set_rcvbuf(&stream, size);
    let sin = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: v4.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(v4.ip().octets()),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: `sin` is a valid IPv4 address and outlives the call.
    let rc = unsafe {
        libc::connect(
            fd,
            (&sin as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "connect: {}", io::Error::last_os_error());
    stream
}
