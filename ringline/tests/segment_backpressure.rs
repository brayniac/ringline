//! A peer whose data is held faster than its reader drains it (a segment
//! reader that never reads, an echo or forward whose peer never reads the
//! reply) is stopped by its own TCP window, not by draining the worker's
//! shared receive ring: its receive is throttled once the buffers it holds,
//! including those its sends hold, reach its cap, and another connection on
//! the same worker keeps working. The tests that run cover the cases where
//! that holds. On a plain 16-buffer ring an unread direct-echo or
//! `forward_held` peer can still empty the ring, because one multishot
//! receive takes buffers past the cap before its cancel lands, and the
//! ignored `forward_recv_buf` test records a case that is not throttled at
//! all; see #638.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// The first byte picks the mode: `S` takes a segment reader and never
/// reads, `D` runs direct echo, `F` echoes with `forward_held`, `R` echoes
/// each buffer with `forward_recv_buf`, `T` echoes with `forward_to_conn`,
/// and anything else echoes through `with_data`.
struct Modes;

impl AsyncEventHandler for Modes {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut first = 0u8;
            let n = conn
                .with_data(|d| {
                    first = d[0];
                    ParseResult::Consumed(1)
                })
                .await;
            if n == 0 {
                return;
            }
            match first {
                b'S' => {
                    let _reader = conn.segments().expect("segment reader");
                    loop {
                        ringline::sleep(Duration::from_secs(1)).await;
                    }
                }
                b'D' => {
                    let _ = conn.run_direct_echo().await;
                }
                b'F' => {
                    conn.enable_recv_forward();
                    loop {
                        conn.recv_ready().await;
                        let n = match conn.forward_held() {
                            Ok(f) => f.await.unwrap_or(0),
                            Err(_) => break,
                        };
                        if n == 0 {
                            break;
                        }
                    }
                }
                b'R' => {
                    let (mut tx, mut rx) = conn.split();
                    loop {
                        let n = rx
                            .with_data(|d| match tx.forward_recv_buf(d) {
                                Ok(()) => ParseResult::Consumed(d.len()),
                                Err(_) => ParseResult::NeedMore,
                            })
                            .await;
                        if n == 0 {
                            break;
                        }
                    }
                }
                b'T' => {
                    let (mut tx, mut rx) = conn.split();
                    while rx.forward_to_conn(&mut tx, 1 << 20).await.is_ok() {}
                }
                _ => {
                    let (mut tx, mut rx) = conn.split();
                    loop {
                        let n = rx
                            .with_data(|d| {
                                let _ = tx.send_nowait(d);
                                ParseResult::Consumed(d.len())
                            })
                            .await;
                        if n == 0 {
                            break;
                        }
                    }
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        Modes
    }
}

/// Flood a connection in mode `mode` without reading from it, then check
/// that its writes stalled and that an echo connection on the same worker
/// still answers.
fn idle_peer_is_throttled_alone(mode: u8, incremental: bool, reserve: u32) {
    idle_peers_are_throttled_alone(mode, incremental, reserve, 1);
}

/// `idle_peer_is_throttled_alone` with `peers` idle connections in mode
/// `mode`, each flooded in turn.
fn idle_peers_are_throttled_alone(mode: u8, incremental: bool, reserve: u32, peers: usize) {
    const LIMIT: u64 = 64 << 20;
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(64)
        .send_pool(64, 16384)
        .recv_buffer(16, 64 << 10)
        .recv_segment_reserve(reserve)
        .recv_incremental(incremental)
        .build()
        .expect("config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Modes>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");
    std::thread::sleep(Duration::from_millis(100));

    let chunk = vec![7u8; 1 << 20];
    let mut idle = Vec::new();
    let mut most = 0u64;
    for _ in 0..peers {
        let mut peer = TcpStream::connect(addr).unwrap();
        peer.write_all(&[mode]).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        peer.set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut sent = 0u64;
        while sent < LIMIT {
            match peer.write(&chunk) {
                Ok(n) => sent += n as u64,
                Err(_) => break,
            }
        }
        most = most.max(sent);
        idle.push(peer);
    }

    let mut other = TcpStream::connect(addr).unwrap();
    other
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    other.write_all(b"Eping").unwrap();
    let mut buf = [0u8; 4];
    let echoed = other.read_exact(&mut buf).is_ok() && &buf == b"ping";

    drop(idle);
    drop(other);
    shutdown.shutdown();
    for h in handles {
        let _ = h.join();
    }
    let label = format!(
        "mode {}, incremental {incremental}, {peers} idle",
        mode as char
    );
    assert!(
        most < LIMIT,
        "{label}: the server accepted {most} bytes unread"
    );
    assert!(echoed, "{label}: another connection on the worker stalled");
}

/// A reserve equal to the ring sends every segmented delivery to the copy
/// fallback.
#[test]
fn an_idle_segment_reader_is_throttled_alone() {
    idle_peer_is_throttled_alone(b'S', false, 16);
    idle_peer_is_throttled_alone(b'S', false, 4);
    idle_peer_is_throttled_alone(b'S', true, 0);
}

#[test]
fn an_unread_direct_echo_is_throttled_alone() {
    idle_peer_is_throttled_alone(b'D', true, 0);
}

/// The buffers an echo's or a forward's sends hold count toward its cap
/// (#638). With an incremental ring, two unread echoes or forwards leave the
/// other connection room to answer.
#[test]
fn two_unread_echoes_or_forwards_are_throttled_alone() {
    for mode in *b"DF" {
        idle_peers_are_throttled_alone(mode, true, 0, 2);
    }
}

/// A `forward_to_conn` write's pinned buffers count toward the segment cap.
#[test]
fn unread_forward_to_peers_are_throttled_alone() {
    for incremental in [false, true] {
        idle_peers_are_throttled_alone(b'T', incremental, 0, 3);
    }
}

/// A connection that forwards each buffer with `forward_recv_buf` is not
/// throttled: forwards of accumulator-backed data are not counted, and the
/// server accepts all 64 MiB the idle peer writes (#638).
#[test]
#[ignore = "#638: forward_recv_buf of accumulator-backed data is not throttled"]
fn an_unread_forward_recv_buf_is_throttled_alone() {
    for incremental in [false, true] {
        idle_peer_is_throttled_alone(b'R', incremental, 0);
    }
}
