#![allow(clippy::manual_async_fn)]
//! One `forward_held` call forwards at most 32 receive buffers' worth of
//! bytes on both backends, and a backlog larger than that is forwarded in
//! full over successive calls.

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

const BUFFER: usize = 256;
/// The most one call forwards: 32 buffers of `BUFFER` bytes.
const CAP: usize = 32 * BUFFER;
/// The backlog the client sends before the handler forwards anything.
const BACKLOG: usize = 3 * CAP + 100;

/// What each `forward_held` call resolved with, in order.
static FORWARDED: Mutex<Vec<usize>> = Mutex::new(Vec::new());

/// Waits for the backlog to build up, then forwards it back in calls to
/// `forward_held` until the client closes.
struct DelayedForward;

impl AsyncEventHandler for DelayedForward {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            conn.enable_recv_forward();
            ringline::sleep(Duration::from_millis(300)).await;
            loop {
                conn.recv_ready().await;
                let n = match conn.forward_held() {
                    Ok(f) => f.await.unwrap_or(0),
                    Err(_) => break,
                };
                if n == 0 {
                    break;
                }
                FORWARDED.lock().unwrap().push(n as usize);
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        DelayedForward
    }
}

#[test]
fn forward_held_forwards_at_most_32_buffers_per_call() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        // Enough buffers to hold the whole backlog, so the io_uring hold
        // exceeds the cap.
        .recv_buffer(256, BUFFER as u32)
        .max_connections(16)
        .send_pool(64, 16384)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<DelayedForward>()
        .expect("launch");
    let mut stream = TcpStream::connect(runtime.bound_addr().expect("bound address")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let payload: Vec<u8> = (0..BACKLOG).map(|i| (i % 251) as u8).collect();
    stream.write_all(&payload).unwrap();

    let mut echoed = vec![0u8; BACKLOG];
    stream
        .read_exact(&mut echoed)
        .expect("the whole backlog came back");
    assert_eq!(echoed, payload, "forwarded bytes in order");

    drop(stream);
    let deadline = Instant::now() + Duration::from_secs(5);
    while FORWARDED.lock().unwrap().iter().sum::<usize>() < BACKLOG {
        assert!(Instant::now() < deadline, "the forwards never all resolved");
        std::thread::sleep(Duration::from_millis(1));
    }
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }

    let calls = FORWARDED.lock().unwrap().clone();
    assert!(
        calls.iter().all(|&n| n <= CAP),
        "a call forwarded more than 32 buffers' worth: {calls:?}"
    );
    assert!(
        calls.len() >= BACKLOG.div_ceil(CAP),
        "the backlog needs at least {} calls: {calls:?}",
        BACKLOG.div_ceil(CAP)
    );
    // mio holds bytes, so a full backlog forwards exactly the cap.
    #[cfg(not(has_io_uring))]
    assert_eq!(calls[0], CAP, "the first call forwards the full cap");
}

/// The backlog for the timing test: 400 calls' worth.
const LARGE_BACKLOG: usize = 400 * CAP;

/// Forwards everything it receives, from the first byte, until the client
/// closes.
struct ForwardAll;

impl AsyncEventHandler for ForwardAll {
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
        ForwardAll
    }
}

/// A backlog that takes hundreds of calls goes back without the event loop
/// waiting between them. Forwarding 3.2 MB over loopback takes well under a
/// second; a loop that slept its idle poll timeout (10 ms) every couple of
/// calls would take about two.
#[test]
fn a_large_backlog_forwards_without_stalling_between_calls() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(256, BUFFER as u32)
        .max_connections(16)
        .send_pool(64, 16384)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<ForwardAll>()
        .expect("launch");
    let mut stream = TcpStream::connect(runtime.bound_addr().expect("bound address")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let payload: Vec<u8> = (0..LARGE_BACKLOG).map(|i| (i % 251) as u8).collect();
    let mut writer = stream.try_clone().unwrap();
    let to_send = payload.clone();

    let started = Instant::now();
    let write = std::thread::spawn(move || writer.write_all(&to_send));
    let mut echoed = vec![0u8; LARGE_BACKLOG];
    stream
        .read_exact(&mut echoed)
        .expect("the whole backlog came back");
    let took = started.elapsed();
    write.join().unwrap().expect("the client wrote the backlog");

    drop(stream);
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
    assert_eq!(echoed, payload, "forwarded bytes in order");
    assert!(
        took < Duration::from_secs(1),
        "forwarding {LARGE_BACKLOG} bytes took {took:?}"
    );
}
