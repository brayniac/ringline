//! `ConfigBuilder::recv_incremental` end to end (#622). In its own test
//! binary because it reads the process-wide `ringline/recv_ring` counters,
//! which every other launch in the same process would also move.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let n = rx
                    .with_data(|data| {
                        let _ = tx.send_nowait(data);
                        ParseResult::Consumed(data.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncEcho
    }
}

/// Echo through the recv-forward path: held receive buffers are sent back
/// with one gathered write.
struct RecvForwardEcho;

impl AsyncEventHandler for RecvForwardEcho {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
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
    }
    fn create_for_worker(_id: usize) -> Self {
        RecvForwardEcho
    }
}

/// Echo through the direct-echo path, sent from the completion handler.
struct DirectEcho;

impl AsyncEventHandler for DirectEcho {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _ = conn.run_direct_echo().await;
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        DirectEcho
    }
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

fn echo_round_trip(addr: &str, msg: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(msg).unwrap();
    let mut buf = vec![0u8; msg.len()];
    let mut total = 0;
    while total < msg.len() {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => panic!("read error: {e}"),
        }
    }
    buf.truncate(total);
    buf
}

/// With `recv_incremental(true)` the runtime echoes messages from 22 B to
/// 1 MiB, with the geometry that follows the ring kind (64 × 1 MiB on a
/// kernel with incremental rings) and with a small explicit one (16 × 1 KiB,
/// which runs out of buffers within each message). Each worker records the
/// ring kind it selected; from Linux 6.12, an incremental ring with no failed
/// preflight step.
#[test]
fn async_echo_with_recv_incremental() {
    use ringline::metrics::{RECV_PREFLIGHT_FAILED, RECV_RING, recv_preflight, recv_ring};
    let count = |slot| RECV_RING.value(slot).unwrap_or(0);
    let failed = || {
        (0..recv_preflight::COUNT)
            .map(|s| RECV_PREFLIGHT_FAILED.value(s).unwrap_or(0))
            .sum::<u64>()
    };
    let (inc_before, plain_before, failed_before) = (
        count(recv_ring::INCREMENTAL),
        count(recv_ring::PLAIN),
        failed(),
    );
    let has_incremental = kernel_at_least(6, 12);
    let geometries: [Option<(u16, u32)>; 2] = [None, Some((16, 1024))];
    for (i, geometry) in geometries.into_iter().enumerate() {
        let mut builder = ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(64)
            .max_connections(64)
            .send_pool(128, 16384)
            .recv_incremental(true);
        if let Some((ring_size, buffer_size)) = geometry {
            builder = builder.recv_buffer(ring_size, buffer_size);
        }
        let (shutdown, handles) = RinglineBuilder::new(builder.build().expect("config"))
            .bind("127.0.0.1:0".parse().unwrap())
            .launch::<AsyncEcho>()
            .expect("launch failed");
        let addr = shutdown.bound_addr().expect("bound address").to_string();
        wait_for_server(&addr);
        for len in [22usize, 1000, 8192, 100_000, 1 << 20] {
            let msg: Vec<u8> = (0..len).map(|k| (k % 251) as u8).collect();
            assert_eq!(
                echo_round_trip(&addr, &msg),
                msg,
                "{geometry:?}, {len} bytes"
            );
        }
        shutdown.shutdown();
        for h in handles {
            h.join().unwrap().unwrap();
        }
        let launches = i as u64 + 1;
        let (inc, plain) = (
            count(recv_ring::INCREMENTAL) - inc_before,
            count(recv_ring::PLAIN) - plain_before,
        );
        assert_eq!(inc + plain, launches, "one selection per worker");
        if has_incremental {
            assert_eq!(inc, launches, "an incremental ring on Linux 6.12+");
            assert_eq!(failed(), failed_before, "no failed preflight step");
        }
    }
}

/// Whether the running kernel is at least `major.minor`.
fn kernel_at_least(major: u32, minor: u32) -> bool {
    let release = std::fs::read_to_string("/proc/sys/kernel/osrelease").unwrap_or_default();
    let mut parts = release.split(|c: char| !c.is_ascii_digit());
    let mut next = || {
        parts
            .next()
            .and_then(|p| p.parse::<u32>().ok())
            .unwrap_or(0)
    };
    (next(), next()) >= (major, minor)
}

/// Echo `total` bytes through a 16 × 1 KiB incremental ring, whose lend cap
/// is 8 buffers, with handler `H`, and check the bytes and that the cap
/// refused lends (so owned copies carried part of the stream).
fn echo_over_the_lend_cap<H: AsyncEventHandler>(total: usize) {
    use ringline::metrics::{RECV_RING, recv_ring};
    let refused = || RECV_RING.value(recv_ring::LEND_REFUSED).unwrap_or(0);
    let before = refused();
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(256)
        .max_connections(64)
        .send_pool(128, 16384)
        .recv_incremental(true)
        .recv_buffer(16, 1024)
        .build()
        .expect("config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<H>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address").to_string();
    wait_for_server(&addr);
    let msg: Vec<u8> = (0..total).map(|k| (k % 251) as u8).collect();
    assert_eq!(echo_round_trip(&addr, &msg), msg);
    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
    if kernel_at_least(6, 12) {
        assert!(refused() > before, "the lend cap refused no lend");
    }
}

#[test]
fn recv_forward_echoes_over_the_lend_cap() {
    echo_over_the_lend_cap::<RecvForwardEcho>(320 << 10);
}

#[test]
fn direct_echo_echoes_over_the_lend_cap() {
    echo_over_the_lend_cap::<DirectEcho>(320 << 10);
}
