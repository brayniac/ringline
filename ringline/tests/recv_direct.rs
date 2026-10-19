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
