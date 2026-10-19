//! Allocations made by direct receives (`TcpConnect::recv_direct`) when every
//! receive ends at a message boundary and `with_bytes` consumes all of it. In
//! its own test binary, for the counting global allocator.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// Counts allocations of at least 32 KiB, the size of a direct receive's
/// buffer and not of anything else the client loop allocates.
struct CountLarge;

static LARGE: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountLarge {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() >= 32 * 1024 {
            LARGE.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= 32 * 1024 {
            LARGE.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: CountLarge = CountLarge;

const BATCHES: usize = 2_000;
const FRAMES: usize = 16;
const SIZE: usize = 60;

static SERVER: Mutex<Option<SocketAddr>> = Mutex::new(None);
static RESULT: Mutex<Option<Result<u64, String>>> = Mutex::new(None);

/// Read `BATCHES` batches of `FRAMES` frames (a 4-byte length and `SIZE`
/// payload bytes) with `with_bytes`, dropping each payload before the next
/// read, and send one byte after each batch to ask for the next.
struct Client;

impl AsyncEventHandler for Client {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut conn = ringline::connect(addr)
                    .recv_direct(true)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                let mut start = 0;
                for batch in 0..BATCHES {
                    if batch == 1 {
                        // The first receive allocates the buffer.
                        start = LARGE.load(Ordering::Relaxed);
                    }
                    for m in 0..FRAMES {
                        let mut value = None;
                        let n = conn
                            .with_bytes(|b| {
                                if b.len() < 4 + SIZE {
                                    return ParseResult::NeedMore;
                                }
                                value = Some(b.slice(4..4 + SIZE));
                                ParseResult::Consumed(4 + SIZE)
                            })
                            .await;
                        if n == 0 {
                            return Err(format!("EOF in batch {batch}, frame {m}"));
                        }
                        drop(value);
                    }
                    conn.send_nowait(b"n").map_err(|e| format!("send: {e}"))?;
                }
                Ok(LARGE.load(Ordering::Relaxed) - start)
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

#[test]
fn a_direct_receive_reuses_a_consumed_buffer() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        let mut batch = Vec::new();
        for _ in 0..FRAMES {
            batch.extend_from_slice(&(SIZE as u32).to_be_bytes());
            batch.resize(batch.len() + SIZE, 0x5a);
        }
        let mut ask = [0u8; 1];
        for b in 0..BATCHES {
            s.write_all(&batch).unwrap();
            if b + 1 < BATCHES {
                s.read_exact(&mut ask).unwrap();
            }
        }
    });
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
    let large = RESULT
        .lock()
        .unwrap()
        .take()
        .expect("client did not finish")
        .expect("client");
    eprintln!("{BATCHES} batches: {large} allocations of 32 KiB or more");
    assert!(
        large * 20 <= BATCHES as u64,
        "{large} allocations of 32 KiB or more in {BATCHES} batches"
    );
}
