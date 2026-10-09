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
/// ring kind it selected.
#[test]
fn async_echo_with_recv_incremental() {
    use ringline::metrics::{RECV_RING, recv_ring};
    let selections = || {
        RECV_RING.value(recv_ring::INCREMENTAL).unwrap_or(0)
            + RECV_RING.value(recv_ring::PLAIN).unwrap_or(0)
    };
    let before = selections();
    let geometries: [Option<(u16, u32)>; 2] = [None, Some((16, 1024))];
    for (i, geometry) in geometries.into_iter().enumerate() {
        let mut builder = ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(64)
            .max_connections(64)
            .send_pool(64, 16384)
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
        assert_eq!(
            selections(),
            before + i as u64 + 1,
            "one selection per worker"
        );
    }
}
