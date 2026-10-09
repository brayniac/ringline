//! A peer whose data is held faster than its reader drains it (a segment
//! reader that never reads, a direct-echo peer that never reads the echo)
//! is stopped by its own TCP window, not by draining the worker's shared
//! receive ring: its receive is throttled at `forward_hold_cap` held entries,
//! and another connection on the same worker keeps working.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// The first byte picks the mode: `S` takes a segment reader and never
/// reads, `D` runs direct echo, anything else echoes through `with_data`.
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

    let mut idle = TcpStream::connect(addr).unwrap();
    idle.write_all(&[mode]).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    idle.set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let chunk = vec![7u8; 1 << 20];
    let mut sent = 0u64;
    while sent < LIMIT {
        match idle.write(&chunk) {
            Ok(n) => sent += n as u64,
            Err(_) => break,
        }
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
    let label = format!("mode {}, incremental {incremental}", mode as char);
    assert!(
        sent < LIMIT,
        "{label}: the server accepted {sent} bytes unread"
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
