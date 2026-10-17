//! `read_value_from` over `with_bytes_segments`.
//!
//! Runs on both backends against an in-process stub server that answers
//! `GET k:<n>` with an `n`-byte value. Values from empty to larger than the
//! hold cap arrive whole and in order, one at a time and pipelined. On
//! io_uring a value shorter than `recv_zc_threshold` is copied out of its
//! view, and a value that spans ring buffers is copied by the parser; on mio
//! neither happens.
//!
//! The counters are process-wide, so this file holds one test.

mod common;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::OnceLock;

use resp_proto::Value;
use ringline::metrics::{RECV_RING, recv_ring};
use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, ParseResult, RinglineBuilder,
};
use ringline_redis::{Client, CompletedOp};

/// 64 buffers of 4 KiB: a 16 KiB value spans four buffers, and the hold cap
/// (a quarter of the ring) is 64 KiB. A reserve of 8 lets the ring lend
/// buffers as views; at the default of 64 every completion is copied.
fn test_config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .recv_segment_reserve(8)
        .max_connections(16)
        .send_pool(128, 16384)
        .build()
        .expect("valid config")
}

const SIZES: [usize; 8] = [0, 1, 100, 3000, 4096, 16 * 1024, 70 * 1024, 200 * 1024];

fn value_of(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn key_of(len: usize) -> Vec<u8> {
    format!("k:{len}").into_bytes()
}

/// `GET k:<n>` gets an `n`-byte bulk string.
fn reply(items: &[Value]) -> Vec<u8> {
    let len = match items {
        [Value::BulkString(verb), Value::BulkString(key)] if verb.eq_ignore_ascii_case(b"GET") => {
            std::str::from_utf8(key)
                .ok()
                .and_then(|k| k.strip_prefix("k:"))
                .and_then(|n| n.parse::<usize>().ok())
        }
        _ => None,
    };
    let Some(len) = len else {
        return b"-ERR unknown\r\n".to_vec();
    };
    let mut out = format!("${len}\r\n").into_bytes();
    out.extend_from_slice(&value_of(len));
    out.extend_from_slice(b"\r\n");
    out
}

struct StubServer;

impl AsyncEventHandler for StubServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let mut out = Vec::new();
                let n = rx
                    .with_bytes(|bytes| {
                        let len = bytes.len();
                        let mut at = 0;
                        while at < len {
                            match Value::parse_bytes(bytes.slice(at..)) {
                                Ok((Value::Array(items), consumed)) => {
                                    out.extend_from_slice(&reply(&items));
                                    at += consumed;
                                }
                                Ok((_, consumed)) => at += consumed,
                                Err(e) if e.is_incomplete() => break,
                                Err(_) => at = len,
                            }
                        }
                        ParseResult::Consumed(at)
                    })
                    .await;
                if n == 0 {
                    break;
                }
                if tx.send_nowait(&out).is_err() {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        StubServer
    }
}

static SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static RESULT: OnceLock<Result<(), String>> = OnceLock::new();

struct ClientHandler;

impl AsyncEventHandler for ClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let addr = *SERVER_ADDR.get().expect("server addr");
        Some(Box::pin(async move {
            RESULT.set(run(addr).await).ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        ClientHandler
    }
}

fn check(len: usize, got: Option<&[u8]>) -> Result<(), String> {
    match got {
        Some(v) if v == value_of(len) => Ok(()),
        Some(v) => Err(format!("k:{len}: {} bytes, wrong content", v.len())),
        None => Err(format!("k:{len}: miss")),
    }
}

async fn run(addr: SocketAddr) -> Result<(), String> {
    let conn = ringline::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut client = Client::new(conn);

    // One at a time. Each small value is kept until the end, so a view that
    // was not copied would still hold its buffer then.
    let mut kept = Vec::new();
    for len in SIZES {
        let value = client
            .get(key_of(len))
            .await
            .map_err(|e| format!("get k:{len}: {e}"))?;
        check(len, value.as_deref())?;
        kept.push((len, value));
    }

    // Pipelined: several replies share buffers, and a reply can start in the
    // buffer the previous one ends in.
    for (i, len) in SIZES.iter().chain(SIZES.iter().rev()).enumerate() {
        client
            .fire_get(&key_of(*len), i as u64)
            .map_err(|e| format!("fire k:{len}: {e}"))?;
    }
    for (i, len) in SIZES.iter().chain(SIZES.iter().rev()).enumerate() {
        match client
            .recv()
            .await
            .map_err(|e| format!("recv k:{len}: {e}"))?
        {
            CompletedOp::Get {
                result, user_data, ..
            } if user_data == i as u64 => {
                let value = result.map_err(|e| format!("recv k:{len}: {e}"))?;
                check(*len, value.as_deref())?;
            }
            _ => return Err(format!("recv k:{len}: not the GET fired as {i}")),
        }
    }

    for (len, value) in kept {
        check(len, value.as_deref())?;
    }
    Ok(())
}

fn count(slot: usize) -> u64 {
    RECV_RING.value(slot).unwrap_or(0)
}

#[test]
fn values_of_every_size_read_through_segment_views() {
    let threshold_copied = count(recv_ring::THRESHOLD_COPIED);
    let parse_copied = count(recv_ring::SEGMENT_PARSE_COPIED);
    let views = count(recv_ring::VIEW_CREATED);
    let hold_cap_copied = count(recv_ring::HOLD_CAP_COPIED);

    let (s_shutdown, s_handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<StubServer>()
        .expect("server launch failed");
    SERVER_ADDR
        .set(s_shutdown.bound_addr().expect("bound address"))
        .ok();

    let (c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<ClientHandler>()
        .expect("client launch failed");
    common::join_workers(&c_shutdown, c_handles);

    if let Err(e) = RESULT.get().expect("client did not set result") {
        panic!("{e}");
    }
    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }

    let threshold_copied = count(recv_ring::THRESHOLD_COPIED) - threshold_copied;
    let parse_copied = count(recv_ring::SEGMENT_PARSE_COPIED) - parse_copied;
    let views = count(recv_ring::VIEW_CREATED) - views;
    let hold_cap_copied = count(recv_ring::HOLD_CAP_COPIED) - hold_cap_copied;
    if cfg!(has_io_uring) {
        assert!(views > 0, "no value was read as a view");
        assert!(threshold_copied > 0, "no short view was copied");
        // The 70 KiB and 200 KiB values are longer than the 64 KiB hold cap.
        assert!(hold_cap_copied > 0, "no value reached the hold cap");
        // The 16 KiB value alone spans four 4 KiB buffers.
        assert!(
            parse_copied >= 16 * 1024,
            "the parser copied {parse_copied} bytes"
        );
    } else {
        assert_eq!(threshold_copied, 0);
        assert_eq!(hold_cap_copied, 0);
        assert_eq!(parse_copied, 0);
    }
}
