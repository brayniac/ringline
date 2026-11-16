//! `SendHalf::writable`, the wait for room in a socket that a server awaits
//! before each piece of a large reply, and `SendCompletion::waited_for_room`
//! (`docs/send-admission-design.md`, section 4, Experiment D), on whichever
//! backend the build has.
//!
//! Each test opens one connection to a one-worker server. The first byte the
//! client writes picks what the server does. Most cases fill the server's
//! socket first: the server sends 16 KiB at a time, eight sends a
//! millisecond, to a client that does not read, until the last send reads
//! `waited_for_room` before it has settled. The unit tests in the io_uring
//! event loop cover the multishot poll and its probe with injected
//! completions.
//!
//! The tests share the statics below, and `cargo test` runs them in one
//! process.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
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
}

static SEEN: Mutex<Option<Seen>> = Mutex::new(None);
/// The server has filled the socket and its gate waits.
static FILLED: AtomicBool = AtomicBool::new(false);

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
    let (runtime, handles) = launch();
    let addr = runtime.bound_addr().expect("bound");
    let mut client = TcpStream::connect(addr).expect("connect");
    set_rcvbuf(&client, 4096);
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
    let mut buf = vec![0u8; 1 << 16];
    loop {
        match client.read(&mut buf) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e) => panic!("drain: {e}"),
        }
    }
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
