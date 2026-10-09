//! A segment reader that never reads is held back by TCP backpressure: the
//! segmented copy fallback stops at one ring's worth of owned copies, then
//! holds ring buffers until the ring drains.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

/// Takes a segment reader and never reads from it.
struct IdleSegmentReader;

impl AsyncEventHandler for IdleSegmentReader {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let _reader = conn.segments().expect("segment reader");
            loop {
                ringline::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        IdleSegmentReader
    }
}

#[test]
fn writes_stall_when_a_segment_reader_does_not_read() {
    const LIMIT: u64 = 64 << 20;
    // A reserve equal to the ring sends every delivery to the copy fallback.
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(64)
        .recv_buffer(16, 64 << 10)
        .recv_segment_reserve(16)
        .build()
        .expect("config");
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<IdleSegmentReader>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");
    std::thread::sleep(Duration::from_millis(100));
    let mut client = TcpStream::connect(addr).unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let chunk = vec![7u8; 1 << 20];
    let mut sent = 0u64;
    while sent < LIMIT {
        match client.write(&chunk) {
            Ok(n) => sent += n as u64,
            Err(_) => break,
        }
    }
    drop(client);
    shutdown.shutdown();
    for h in handles {
        let _ = h.join();
    }
    assert!(
        sent < LIMIT,
        "the server accepted {sent} bytes without a read"
    );
}
