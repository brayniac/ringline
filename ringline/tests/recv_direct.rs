//! Direct receives (`TcpConnect::recv_direct`): one-shot receives into the
//! connection's own accumulator.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

mod common;

use std::future::Future;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use bytes::Bytes;
use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// Payload byte `i` of message `m`.
fn pattern(m: usize, i: usize) -> u8 {
    (m.wrapping_mul(31).wrapping_add(i) % 251) as u8
}

/// Message sizes: small, around the 64 KiB region, and larger than it.
fn sizes() -> Vec<usize> {
    let mut v = Vec::new();
    for k in 0..400 {
        v.push(match k % 8 {
            0 => 1,
            1 => 17,
            2 => 200,
            3 => 4096,
            4 => 65_536 - 4,
            5 => 100_000,
            6 => (1 << 20) + 3,
            _ => 31,
        });
    }
    v
}

/// Serve one connection: every message is a 4-byte big-endian length and a
/// patterned payload, written in uneven pieces with pauses between some, so
/// receives end at arbitrary points in a message.
fn serve_frames(listener: TcpListener) {
    let (mut s, _) = listener.accept().unwrap();
    let mut out = Vec::new();
    for (m, &n) in sizes().iter().enumerate() {
        out.extend_from_slice(&(n as u32).to_be_bytes());
        out.extend((0..n).map(|i| pattern(m, i)));
    }
    let mut at = 0;
    let mut step = 1usize;
    while at < out.len() {
        step = (step * 7 + 13) % 300_000 + 1;
        let end = (at + step).min(out.len());
        s.write_all(&out[at..end]).unwrap();
        at = end;
        if step.is_multiple_of(5) {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}

static SERVER: Mutex<Option<SocketAddr>> = Mutex::new(None);
static RESULT: Mutex<Option<Result<usize, String>>> = Mutex::new(None);

/// Parse one frame from `b`. A complete frame's payload is checked against
/// the pattern for message `m`; `keep` holds every fourth payload slice
/// alive across the next read, so the accumulator's remainder is sometimes
/// shared and has to be copied rather than reclaimed in place.
fn parse(m: usize, b: &[u8]) -> Result<ParseResult, String> {
    if b.len() < 4 {
        return Ok(ParseResult::NeedMore);
    }
    let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
    if n != sizes()[m] {
        return Err(format!("message {m}: length {n}, expected {}", sizes()[m]));
    }
    if b.len() < 4 + n {
        return Ok(ParseResult::NeedAtLeast(4 + n - b.len()));
    }
    if let Some(i) = (0..n).find(|&i| b[4 + i] != pattern(m, i)) {
        return Err(format!("message {m} of {n} bytes differs at {i}"));
    }
    Ok(ParseResult::Consumed(4 + n))
}

async fn read_all(mut conn: Connection, use_bytes: bool) -> Result<usize, String> {
    let total = sizes().len();
    let mut keep: Vec<Bytes> = Vec::new();
    for m in 0..total {
        let mut err = None;
        let n = if use_bytes {
            conn.with_bytes(|b| match parse(m, &b) {
                Ok(ParseResult::Consumed(c)) => {
                    if m % 4 == 0 {
                        keep.push(b.slice(4..c));
                    }
                    ParseResult::Consumed(c)
                }
                Ok(r) => r,
                Err(e) => {
                    err = Some(e);
                    ParseResult::Consumed(b.len())
                }
            })
            .await
        } else {
            conn.with_data(|b| match parse(m, b) {
                Ok(r) => r,
                Err(e) => {
                    err = Some(e);
                    ParseResult::Consumed(b.len())
                }
            })
            .await
        };
        if let Some(e) = err {
            return Err(e);
        }
        if n == 0 {
            return Err(format!("EOF before message {m}"));
        }
        if keep.len() > 2 {
            keep.remove(0);
        }
    }
    Ok(total)
}

fn run_client<H: AsyncEventHandler>() -> Result<usize, String> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    *RESULT.lock().unwrap() = None;
    let server = std::thread::spawn(move || serve_frames(listener));
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config).launch::<H>().expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    RESULT
        .lock()
        .unwrap()
        .take()
        .expect("client did not finish")
}

struct BytesClient;

impl AsyncEventHandler for BytesClient {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = match ringline::connect(addr).recv_direct(true).await {
                Ok(conn) => read_all(conn, true).await,
                Err(e) => Err(format!("connect: {e}")),
            };
            *RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        BytesClient
    }
}

struct DataClient;

impl AsyncEventHandler for DataClient {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = match ringline::connect(addr).recv_direct(true).await {
                Ok(conn) => read_all(conn, false).await,
                Err(e) => Err(format!("connect: {e}")),
            };
            *RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        DataClient
    }
}

static TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn with_bytes_reads_every_byte_in_order() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(run_client::<BytesClient>(), Ok(sizes().len()));
}

#[test]
fn with_data_reads_every_byte_in_order() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    assert_eq!(run_client::<DataClient>(), Ok(sizes().len()));
}

// ── Close with a receive in flight ──────────────────────────────────

/// The first connection parks a read (its receive is armed), drops the read
/// on a timeout and closes. The second connection, which takes the same
/// slot, must receive only its own bytes.
struct CloseWhileArmed;

static SERVER2: Mutex<Option<SocketAddr>> = Mutex::new(None);

impl AsyncEventHandler for CloseWhileArmed {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER2.lock().unwrap().unwrap();
            let r = async {
                let mut first = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect 1: {e}"))?;
                let waited = ringline::timeout(
                    Duration::from_millis(100),
                    first.with_bytes(|b| ParseResult::Consumed(b.len())),
                )
                .await;
                if waited.is_ok() {
                    return Err("the first connection received bytes".to_string());
                }
                first.close();
                drop(first);
                // Let the Close complete so the slot is free to reuse.
                ringline::sleep(Duration::from_millis(50)).await;
                let mut second = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect 2: {e}"))?;
                let mut got = Vec::new();
                while got.len() < 4 {
                    let n = second
                        .with_bytes(|b| {
                            got.extend_from_slice(&b);
                            ParseResult::Consumed(b.len())
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
                if got != b"two!" {
                    return Err(format!("second connection read {got:?}"));
                }
                Ok(1)
            }
            .await;
            *RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        CloseWhileArmed
    }
}

#[test]
fn a_receive_armed_at_close_does_not_reach_the_next_connection() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER2.lock().unwrap() = Some(listener.local_addr().unwrap());
    *RESULT.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        // The first connection gets bytes only after the client has closed
        // it, so they could reach only a receive still armed for it.
        let (mut a, _) = listener.accept().unwrap();
        let (mut b, _) = listener.accept().unwrap();
        let _ = a.write_all(b"one!");
        std::thread::sleep(Duration::from_millis(50));
        b.write_all(b"two!").unwrap();
        std::thread::sleep(Duration::from_millis(200));
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(1)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<CloseWhileArmed>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(1)
    );
}

// ── A NeedAtLeast larger than the message ───────────────────────────

/// Frames of `OVER_SIZE` payload bytes, parsed by a reader that announces
/// `OVER_NEED` missing bytes until the frame is complete. The documented
/// contract is that an overestimate costs memory; a direct connection must
/// still wake the reader with each receive.
const OVER_SIZE: usize = 200_000;
const OVER_COUNT: usize = 5;
const OVER_NEED: usize = 10 << 20;

struct Overestimate;

impl AsyncEventHandler for Overestimate {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                for m in 0..OVER_COUNT {
                    let n = conn
                        .with_bytes(|b| {
                            if b.len() < 4 + OVER_SIZE {
                                return ParseResult::NeedAtLeast(OVER_NEED);
                            }
                            ParseResult::Consumed(4 + OVER_SIZE)
                        })
                        .await;
                    if n == 0 {
                        return Err(format!("EOF before frame {m}"));
                    }
                }
                Ok(OVER_COUNT)
            }
            .await;
            *RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        Overestimate
    }
}

#[test]
fn an_overestimated_need_does_not_stall_a_direct_read() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    *RESULT.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        for _ in 0..OVER_COUNT {
            s.write_all(&(OVER_SIZE as u32).to_be_bytes()).unwrap();
            s.write_all(&vec![0x5a; OVER_SIZE]).unwrap();
        }
        // Hold the connection open: the reader must finish on the bytes
        // alone, not on EOF.
        let mut rest = [0u8; 1];
        let _ = std::io::Read::read(&mut s, &mut rest);
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<Overestimate>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(OVER_COUNT)
    );
}

// ── A reset in the middle of a message ──────────────────────────────

struct ResetMidMessage;

static RESET_RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

impl AsyncEventHandler for ResetMidMessage {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                // A 100,000-byte frame of which the peer sends 2,000 bytes,
                // in two writes, before resetting the connection.
                let r = conn
                    .with_data_result(|b| {
                        if b.len() < 4 {
                            return ParseResult::NeedMore;
                        }
                        let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
                        if b.len() < 4 + n {
                            return ParseResult::NeedAtLeast(4 + n - b.len());
                        }
                        ParseResult::Consumed(4 + n)
                    })
                    .await;
                match r {
                    Ok(0) => Err("the reset was reported as a clean EOF".into()),
                    Ok(n) => Err(format!("read a whole frame of {n} bytes")),
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => Ok(()),
                    Err(e) => Err(format!("unexpected error: {e}")),
                }
            }
            .await;
            *RESET_RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ResetMidMessage
    }
}

#[test]
fn a_reset_in_the_middle_of_a_message_is_an_error() {
    use std::os::fd::AsRawFd;
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    *RESET_RESULT.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.write_all(&100_000u32.to_be_bytes()).unwrap();
        s.write_all(&[0x5a; 1000]).unwrap();
        // By now the reader has parsed the header and armed a receive for
        // the rest of the frame; it gets 1,000 more bytes before the reset.
        std::thread::sleep(Duration::from_millis(100));
        s.write_all(&[0x5a; 1000]).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        // SO_LINGER with a zero timeout: close sends a RST.
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        let rc = unsafe {
            libc::setsockopt(
                s.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "setsockopt SO_LINGER");
        drop(s);
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<ResetMidMessage>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        RESET_RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(())
    );
}

// ── Reads a direct connection refuses ───────────────────────────────

struct Refusals;

static REFUSALS: Mutex<Option<Result<(), String>>> = Mutex::new(None);

fn expect_unsupported<T>(what: &str, r: std::io::Result<T>) -> Result<(), String> {
    match r {
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => Ok(()),
        Err(e) => Err(format!("{what}: {e}")),
        Ok(_) => Err(format!("{what}: accepted")),
    }
}

impl AsyncEventHandler for Refusals {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                expect_unsupported(
                    "tls",
                    ringline::connect(addr)
                        .tls("localhost")
                        .recv_direct(true)
                        .await,
                )?;
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                expect_unsupported("segments", conn.segments().map(|_| ()))?;
                expect_unsupported("recv_owned_segment", conn.recv_owned_segment().map(|_| ()))?;
                expect_unsupported(
                    "with_bytes_segments",
                    conn.with_bytes_segments(|_| ParseResult::NeedMore).await,
                )?;
                expect_unsupported(
                    "with_segments",
                    conn.with_segments(|_| ringline::SegConsumed(0)).await,
                )?;
                conn.enable_recv_forward();
                expect_unsupported("forward_held", conn.forward_held().map(|_| ()))?;
                let sink = ringline::connect(addr)
                    .await
                    .map_err(|e| format!("connect sink: {e}"))?;
                let (mut sink_tx, _sink_rx) = sink.split();
                expect_unsupported(
                    "forward_to_conn",
                    conn.forward_to_conn(&mut sink_tx, 10).await,
                )?;
                // Resolves at once rather than waiting for an EOF.
                ringline::timeout(Duration::from_secs(5), conn.run_direct_echo())
                    .await
                    .map_err(|_| "run_direct_echo did not resolve".to_string())?;
                // The connection still reads with `with_data`.
                let mut got = Vec::new();
                while got.len() < 4 {
                    let n = conn
                        .with_data(|b| {
                            got.extend_from_slice(b);
                            ParseResult::Consumed(b.len())
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
                if got != b"ping" {
                    return Err(format!("read {got:?} after the refusals"));
                }
                Ok(())
            }
            .await;
            *REFUSALS.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        Refusals
    }
}

#[test]
fn a_direct_connection_refuses_reads_that_need_the_ring() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    *REFUSALS.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        // The direct connection, then the sink.
        let (mut direct, _) = listener.accept().unwrap();
        let (_sink, _) = listener.accept().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        direct.write_all(b"ping").unwrap();
        std::thread::sleep(Duration::from_millis(500));
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<Refusals>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        REFUSALS
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(())
    );
}

static READY: Mutex<Option<Result<(), String>>> = Mutex::new(None);

/// Read part of a message, give up on the rest so the direct receive stays
/// armed with the part in its buffer, then wait with `recv_ready`, which
/// resolves at once because the part counts as buffered.
struct ReadyWithPart;

impl AsyncEventHandler for ReadyWithPart {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                let read = ringline::timeout(
                    Duration::from_millis(300),
                    conn.with_data(|b| {
                        if b.len() < 200 {
                            ParseResult::NeedMore
                        } else {
                            ParseResult::Consumed(200)
                        }
                    }),
                )
                .await;
                if read.is_ok() {
                    return Err("the read finished without the rest of the message".into());
                }
                ringline::timeout(Duration::from_secs(2), conn.recv_ready())
                    .await
                    .map_err(|_| {
                        "recv_ready did not resolve with part of a message held".to_string()
                    })
            }
            .await;
            *READY.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ReadyWithPart
    }
}

#[test]
fn recv_ready_counts_the_bytes_a_direct_receive_holds() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    *READY.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.write_all(&[7u8; 100]).unwrap();
        // Hold the rest until the client has finished or 5 s have passed.
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let _ = std::io::Read::read(&mut s, &mut [0u8; 1]);
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<ReadyWithPart>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        READY.lock().unwrap().take().expect("client did not finish"),
        Ok(())
    );
}
