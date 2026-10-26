//! A peer whose data arrives faster than its reader drains it (a reader that
//! never reads, a forward whose peer never reads the reply) is stopped by its
//! own TCP window, and another connection on the same worker keeps working.
//! The accepted connections here are plaintext, so they receive directly: a
//! receive is armed when a read parks, and a reader that does not read takes
//! nothing off the socket. The ignored `forward_recv_buf` test
//! records a case that is not throttled at all; see #638.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// The first byte picks the mode: `N` never reads again, `R` echoes each
/// buffer with `forward_recv_buf`, `T` echoes with `forward_to_conn`, and
/// anything else echoes through `with_data`.
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
                b'N' => loop {
                    ringline::sleep(Duration::from_secs(1)).await;
                },
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
fn idle_peer_is_throttled_alone(mode: u8) {
    idle_peers_are_throttled_alone(mode, 1);
}

/// `idle_peer_is_throttled_alone` with `peers` idle connections in mode
/// `mode`, each flooded in turn.
fn idle_peers_are_throttled_alone(mode: u8, peers: usize) {
    const LIMIT: u64 = 64 << 20;
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(64)
        .send_pool(64, 16384)
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
    let label = format!("mode {}, {peers} idle", mode as char);
    assert!(
        most < LIMIT,
        "{label}: the server accepted {most} bytes unread"
    );
    assert!(echoed, "{label}: another connection on the worker stalled");
}

/// A reader that never reads arms no receive, so its peer's writes stall.
#[test]
fn an_idle_reader_is_throttled_alone() {
    idle_peer_is_throttled_alone(b'N');
}

/// A `forward_to_conn` from a direct connection stops receiving while its
/// sends to a peer that does not read are stuck, so that peer's writes
/// stall.
#[test]
fn unread_forward_to_peers_are_throttled_alone() {
    idle_peers_are_throttled_alone(b'T', 3);
}

/// A connection that forwards each buffer with `forward_recv_buf` is not
/// throttled: the server accepts all 64 MiB the idle peer writes (#638).
#[test]
#[ignore = "#638: forward_recv_buf is not throttled"]
fn an_unread_forward_recv_buf_is_throttled_alone() {
    idle_peer_is_throttled_alone(b'R');
}
