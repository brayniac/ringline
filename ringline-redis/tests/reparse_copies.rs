//! An array reply that arrives in many pieces is not copied again on each
//! arrival.
//!
//! A stub server sends an `MGET` reply of 12 values of 4096 bytes in 4 KiB
//! chunks, 2 ms apart, so the client reads after each chunk. Each value
//! spans two receive buffers, so a parse of the whole reply copies about
//! 48 KiB. A parse after every chunk would copy the values buffered so far
//! each time, several times that. The allocator counts bytes allocated on
//! the client's thread during the call. The bound applies on io_uring.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use resp_proto::Value;
use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};
use ringline_redis::Client;

struct Counting;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATED.with(|a| a.set(a.get() + layout.size()));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.with(Cell::get) {
            ALLOCATED.with(|a| a.set(a.get() + new_size));
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const VALUES: usize = 12;
const VALUE_LEN: usize = 4096;
const CHUNK: usize = 4096;

fn reply() -> Vec<u8> {
    let mut out = format!("*{VALUES}\r\n").into_bytes();
    for i in 0..VALUES {
        out.extend_from_slice(format!("${VALUE_LEN}\r\n").as_bytes());
        out.extend(std::iter::repeat_n(b'a' + i as u8, VALUE_LEN));
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn test_config() -> ringline::Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .recv_segment_reserve(8)
        .max_connections(16)
        .send_pool(64, 16384)
        .build()
        .expect("valid config")
}

/// Answers any command array with `reply()`, sent in `CHUNK`-byte pieces.
struct StubServer;

impl AsyncEventHandler for StubServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let mut requests = 0;
                let n = rx
                    .with_bytes(|bytes| match Value::parse_bytes(bytes) {
                        Ok((_, consumed)) => {
                            requests += 1;
                            ParseResult::Consumed(consumed)
                        }
                        Err(_) => ParseResult::NeedMore,
                    })
                    .await;
                if n == 0 {
                    break;
                }
                for _ in 0..requests {
                    for chunk in reply().chunks(CHUNK) {
                        if tx.send_nowait(chunk).is_err() {
                            return;
                        }
                        ringline::sleep(Duration::from_millis(2)).await;
                    }
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        StubServer
    }
}

static SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static RESULT: OnceLock<Result<usize, String>> = OnceLock::new();

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

/// The bytes allocated on this thread while one `MGET` reply is read.
async fn run(addr: SocketAddr) -> Result<usize, String> {
    let conn = ringline::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut client = Client::new(conn);
    let keys: Vec<Vec<u8>> = (0..VALUES).map(|i| format!("k{i}").into_bytes()).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    // The first read decides the read path; it is not counted.
    client
        .mget(&key_refs)
        .await
        .map_err(|e| format!("mget: {e}"))?;

    ALLOCATED.with(|a| a.set(0));
    COUNTING.with(|c| c.set(true));
    let values = client.mget(&key_refs).await;
    COUNTING.with(|c| c.set(false));
    let values = values.map_err(|e| format!("mget: {e}"))?;
    for (i, value) in values.iter().enumerate() {
        let want = vec![b'a' + i as u8; VALUE_LEN];
        if value.as_deref() != Some(&want[..]) {
            return Err(format!("value {i} is wrong"));
        }
    }
    Ok(ALLOCATED.with(Cell::get))
}

#[test]
fn an_array_reply_in_many_pieces_is_copied_once() {
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
    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }

    let allocated = match RESULT.get().expect("client did not set result") {
        Ok(allocated) => *allocated,
        Err(e) => panic!("{e}"),
    };
    let reply_len = reply().len();
    println!("allocated {allocated} bytes for a {reply_len}-byte reply");
    // On mio the reply is read into the accumulator, which reallocates as it
    // grows; the values are slices of it.
    if cfg!(has_io_uring) {
        assert!(
            allocated < 2 * reply_len,
            "allocated {allocated} bytes for a {reply_len}-byte reply"
        );
    }
}
