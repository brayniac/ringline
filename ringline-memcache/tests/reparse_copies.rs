//! A multi-key reply that arrives in many pieces is parsed once, when
//! complete.
//!
//! A stub server answers `gets` in 4 KiB chunks, 2 ms apart, so the client
//! reads after each chunk. The allocator counts bytes allocated on the
//! client's thread during one call. The bounds apply on io_uring.
//!
//! - 12 values of 4096 bytes, 49 KB: below the hold cap, so the reply is
//!   presented in several pieces. Each value spans two receive buffers, so
//!   one parse copies about 48 KiB; a parse after every chunk would copy the
//!   values buffered so far each time.
//! - 5,000 values of 10 bytes, 159 KB: above the hold cap, so after the
//!   hold-cap copy the reply is presented as one piece. A parse after every
//!   chunk would rebuild the values each time.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};
use ringline_memcache::Client;

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

/// A reply's shape: the values and their length.
#[derive(Clone, Copy)]
struct Shape {
    values: usize,
    len: usize,
}

/// Asked for with any key but `small`.
const LARGE: Shape = Shape {
    values: 12,
    len: 4096,
};

/// Asked for with the key `small`.
const SMALL: Shape = Shape {
    values: 5_000,
    len: 10,
};

const CHUNK: usize = 4096;

fn reply(shape: Shape) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..shape.values {
        out.extend_from_slice(format!("VALUE k{i} 0 {} 7\r\n", shape.len).as_bytes());
        out.extend(std::iter::repeat_n(b'a' + (i % 26) as u8, shape.len));
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"END\r\n");
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

/// Answers each request line with a reply of the shape it asks for, sent in
/// `CHUNK`-byte pieces.
struct StubServer;

impl AsyncEventHandler for StubServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let mut shapes = Vec::new();
                let n = rx
                    .with_data(|bytes| {
                        let mut at = 0;
                        while let Some(end) = bytes[at..].windows(2).position(|w| w == b"\r\n") {
                            let line = &bytes[at..at + end];
                            let small = line.split(|&b| b == b' ').any(|word| word == b"small");
                            shapes.push(if small { SMALL } else { LARGE });
                            at += end + 2;
                        }
                        ParseResult::Consumed(at)
                    })
                    .await;
                if n == 0 {
                    break;
                }
                for shape in shapes {
                    for chunk in reply(shape).chunks(CHUNK) {
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
static RESULT: OnceLock<Result<[usize; 2], String>> = OnceLock::new();

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

/// The bytes allocated on this thread while one reply of `shape` is read.
async fn counted(client: &mut Client, key: &[u8], shape: Shape) -> Result<usize, String> {
    ALLOCATED.with(|a| a.set(0));
    COUNTING.with(|c| c.set(true));
    let values = client.gets(&[key]).await;
    COUNTING.with(|c| c.set(false));
    let values = values.map_err(|e| format!("gets: {e}"))?;
    if values.len() != shape.values {
        return Err(format!("{} values, want {}", values.len(), shape.values));
    }
    for (i, value) in values.iter().enumerate() {
        let want = vec![b'a' + (i % 26) as u8; shape.len];
        if value.data[..] != want[..] {
            return Err(format!("value {i} is wrong"));
        }
    }
    Ok(ALLOCATED.with(Cell::get))
}

async fn run(addr: SocketAddr) -> Result<[usize; 2], String> {
    let conn = ringline::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let mut client = Client::new(conn);
    // The first read decides the read path; it is not counted.
    counted(&mut client, b"large", LARGE).await?;
    Ok([
        counted(&mut client, b"large", LARGE).await?,
        counted(&mut client, b"small", SMALL).await?,
    ])
}

#[test]
fn a_multi_key_reply_in_many_pieces_is_parsed_once() {
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

    let [large, small] = match RESULT.get().expect("client did not set result") {
        Ok(allocated) => *allocated,
        Err(e) => panic!("{e}"),
    };
    let large_len = reply(LARGE).len();
    let small_len = reply(SMALL).len();
    println!("allocated {large} bytes for a {large_len}-byte reply of 12 values");
    println!("allocated {small} bytes for a {small_len}-byte reply of 5,000 values");
    // On mio the reply is read into the accumulator, which reallocates as it
    // grows, so the first phase's bound applies on io_uring only. The second
    // phase's reply is one piece on both backends: on mio it is in one piece
    // from the start, on io_uring after the hold-cap copy.
    if cfg!(has_io_uring) {
        assert!(large < 2 * large_len, "{large} for {large_len} bytes");
    }
    // The values, the `Vec` `gets` returns with a copy of each short key, and
    // the accumulator's growth: about 15 bytes per reply byte on io_uring and
    // 16 on mio. A parse after every chunk allocates about 177 and 148.
    let bound = if cfg!(has_io_uring) { 32 } else { 64 };
    assert!(small < bound * small_len, "{small} for {small_len} bytes");
}
