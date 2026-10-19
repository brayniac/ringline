//! What direct receives (`TcpConnect::recv_direct`) copy, read from the
//! process-wide `ringline/recv_direct` and `ringline/recv_ring` counters. In
//! its own test binary, with one test, so no other launch moves them.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

mod common;

use std::future::Future;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Mutex;

use ringline::metrics::{RECV_DIRECT, RECV_RING, recv_direct, recv_ring};
use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

static SERVER: Mutex<Option<(SocketAddr, usize, usize)>> = Mutex::new(None);
static RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

/// Read `count` frames (a 4-byte length and a payload) with `with_bytes`,
/// returning `NeedAtLeast` for a frame whose header is in.
struct Client;

impl AsyncEventHandler for Client {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let (addr, count, _) = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                for m in 0..count {
                    let n = conn
                        .with_bytes(|b| {
                            if b.len() < 4 {
                                return ParseResult::NeedMore;
                            }
                            let n = u32::from_be_bytes(b[..4].try_into().unwrap()) as usize;
                            if b.len() < 4 + n {
                                ParseResult::NeedAtLeast(4 + n - b.len())
                            } else {
                                ParseResult::Consumed(4 + n)
                            }
                        })
                        .await;
                    if n == 0 {
                        return Err(format!("EOF before frame {m}"));
                    }
                }
                Ok(())
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
        Client
    }
}

struct Counts {
    received: u64,
    copied: u64,
    armed: u64,
    waitall: u64,
}

fn counts() -> Counts {
    Counts {
        received: RECV_DIRECT.value(recv_direct::BYTES).unwrap_or(0),
        copied: RECV_RING.value(recv_ring::BYTES_COPIED).unwrap_or(0),
        armed: RECV_DIRECT.value(recv_direct::ARMED).unwrap_or(0),
        waitall: RECV_DIRECT.value(recv_direct::WAITALL).unwrap_or(0),
    }
}

/// Send `count` frames of `size` payload bytes, in writes of `chunk` bytes,
/// to a client reading with direct receives, and return the counter deltas.
fn run(count: usize, size: usize, chunk: usize) -> Counts {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some((listener.local_addr().unwrap(), count, size));
    *RESULT.lock().unwrap() = None;
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut out = Vec::with_capacity(count * (size + 4));
        for _ in 0..count {
            out.extend_from_slice(&(size as u32).to_be_bytes());
            out.resize(out.len() + size, 0x5a);
        }
        for piece in out.chunks(chunk) {
            s.write_all(piece).unwrap();
        }
    });
    let before = counts();
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<Client>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(())
    );
    let after = counts();
    eprintln!(
        "{count} x {size} B: received {}, copied {}, armed {}, waitall {}",
        after.received - before.received,
        after.copied - before.copied,
        after.armed - before.armed,
        after.waitall - before.waitall
    );
    Counts {
        received: after.received - before.received,
        copied: after.copied - before.copied,
        armed: after.armed - before.armed,
        waitall: after.waitall - before.waitall,
    }
}

#[test]
fn direct_receives_copy_little() {
    // Small frames: a receive holds many frames, and the only copies are
    // the partial frame moved when the accumulator's buffer is grown.
    // 70,000-byte frames sent back to back: a receive ends inside a frame,
    // and that part is moved when the buffer grows again. Receives several
    // frames long make the move once per several frames.
    for chunk in [1 << 16, 1 << 20] {
        let pipelined = run(500, 70_000, chunk);
        assert!(
            pipelined.copied * 4 <= pipelined.received,
            "70,000-byte frames in {chunk}-byte writes: {} bytes copied of {} received",
            pipelined.copied,
            pipelined.received
        );
    }
    let small = run(20_000, 500, 1 << 16);
    assert_eq!(small.received, 20_000 * 504, "bytes received");
    assert!(small.armed > 0);
    assert!(
        small.copied * 20 <= small.received,
        "small frames: {} bytes copied of {} received",
        small.copied,
        small.received
    );

    // 1 MiB frames: once the header is in, the rest of the frame arrives in
    // one exact-length receive, and the bytes already received for it are
    // copied once when the buffer grows to the frame's size.
    let large = run(32, 1 << 20, 1 << 16);
    assert_eq!(large.received, 32 * ((1 << 20) + 4), "bytes received");
    assert!(large.waitall > 0, "no exact-length receive was armed");
    assert!(
        large.copied * 5 <= large.received,
        "1 MiB frames: {} bytes copied of {} received",
        large.copied,
        large.received
    );
}
