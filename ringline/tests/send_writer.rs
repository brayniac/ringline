#![allow(clippy::manual_async_fn)]
//! `SendHalf::send_with`: bytes written into the writer reach the peer in
//! order, and a writer that fails or panics gives its slots back.

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

const SLOT: usize = 4096;
const SLOTS: u16 = 8;
/// The whole pool, written by one `send_with`.
const FULL: usize = SLOT * SLOTS as usize;

/// Write a pattern across several slots with uneven writes and one
/// `contiguous` region.
const PATTERN: u8 = b'p';
/// Write one slot more than the pool holds, then the whole pool.
const TOO_LARGE_THEN_FULL: u8 = b'l';
/// Write two slots, then panic inside the closure.
const PANIC: u8 = b'x';
/// Write the whole pool.
const FULL_POOL: u8 = b'f';
/// Write nothing, then `send_nowait` one byte.
const EMPTY: u8 = b'e';

const PATTERN_LEN: usize = 3 * SLOT + 100;

/// Errors `send_with` returned, by scenario.
static ERRORS: Mutex<Vec<(u8, std::io::ErrorKind)>> = Mutex::new(Vec::new());

fn byte(i: usize) -> u8 {
    (i % 251) as u8
}

fn full(w: &mut ringline::SendWriter<'_>) -> std::io::Result<()> {
    let data: Vec<u8> = (0..FULL).map(byte).collect();
    w.write_all(&data)
}

struct Scenarios;

impl AsyncEventHandler for Scenarios {
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            let mut trigger = 0u8;
            let n = rx
                .with_data(|data| {
                    trigger = data[0];
                    ParseResult::Consumed(data.len())
                })
                .await;
            if n == 0 {
                return;
            }
            let record = |r: std::io::Result<std::io::Result<()>>| {
                if let Err(e) | Ok(Err(e)) = r {
                    ERRORS.lock().unwrap().push((trigger, e.kind()));
                }
            };
            match trigger {
                PATTERN => record(tx.send_with(|w| {
                    let data: Vec<u8> = (0..PATTERN_LEN).map(byte).collect();
                    let mut at = 0;
                    for step in [1, 4000, 3, 5000] {
                        w.write_all(&data[at..at + step])?;
                        at += step;
                    }
                    // A region that does not fit the current slot's tail.
                    let n = w.contiguous(SLOT, |buf| {
                        let n = 2000;
                        buf[..n].copy_from_slice(&data[at..at + n]);
                        n
                    })?;
                    at += n;
                    w.write_all(&data[at..])
                })),
                TOO_LARGE_THEN_FULL => {
                    record(tx.send_with(|w| {
                        full(w)?;
                        w.write_all(&[0])
                    }));
                    record(tx.send_with(full));
                }
                PANIC => {
                    let _ = tx.send_with(|w| {
                        w.write_all(&[0; 2 * SLOT]).unwrap();
                        panic!("inside send_with");
                    });
                }
                FULL_POOL => record(tx.send_with(full)),
                EMPTY => {
                    record(tx.send_with(|_| Ok(())));
                    tx.send_nowait(b"z").unwrap();
                }
                other => panic!("unknown scenario {other}"),
            }
            while rx.with_data(|data| ParseResult::Consumed(data.len())).await > 0 {}
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Scenarios
    }
}

fn launch() -> (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 4096)
        .max_connections(16)
        .send_pool(SLOTS, SLOT as u32)
        .build()
        .expect("valid config");
    RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Scenarios>()
        .expect("launch")
}

fn connect(runtime: &ringline::Runtime, scenario: u8) -> TcpStream {
    let mut s = TcpStream::connect(runtime.bound_addr().expect("bound address")).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(&[scenario]).unwrap();
    s
}

fn read_n(s: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut got = vec![0u8; n];
    s.read_exact(&mut got).expect("every byte delivered");
    got
}

fn errors(scenario: u8) -> Vec<std::io::ErrorKind> {
    ERRORS
        .lock()
        .unwrap()
        .iter()
        .filter(|(s, _)| *s == scenario)
        .map(|(_, k)| *k)
        .collect()
}

fn stop(
    runtime: ringline::Runtime,
    handles: Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
}

#[test]
fn bytes_written_across_slots_arrive_in_order() {
    let (runtime, handles) = launch();
    let mut s = connect(&runtime, PATTERN);
    let got = read_n(&mut s, PATTERN_LEN);
    let want: Vec<u8> = (0..PATTERN_LEN).map(byte).collect();
    assert!(got == want, "the bytes differ from what was written");
    assert!(errors(PATTERN).is_empty(), "{:?}", errors(PATTERN));
    drop(s);
    stop(runtime, handles);
}

/// On mio the pool does not limit `send`, so the wide send succeeds there.
#[cfg(has_io_uring)]
#[test]
fn a_send_wider_than_the_pool_fails_and_releases_its_slots() {
    let (runtime, handles) = launch();
    let mut s = connect(&runtime, TOO_LARGE_THEN_FULL);
    // The second send_with needs every slot, so it succeeds only if the
    // first gave all of them back.
    let got = read_n(&mut s, FULL);
    assert!(got.iter().enumerate().all(|(i, b)| *b == byte(i)));
    assert_eq!(
        errors(TOO_LARGE_THEN_FULL),
        vec![std::io::ErrorKind::InvalidInput]
    );
    drop(s);
    stop(runtime, handles);
}

#[test]
fn a_panic_inside_send_with_releases_its_slots() {
    let (runtime, handles) = launch();
    let mut s = connect(&runtime, PANIC);
    // The panic ends the task and closes the connection without sending.
    let mut rest = Vec::new();
    let _ = s.read_to_end(&mut rest);
    assert!(
        rest.is_empty(),
        "the panicked writer sent {} bytes",
        rest.len()
    );
    let mut s = connect(&runtime, FULL_POOL);
    let got = read_n(&mut s, FULL);
    assert!(got.iter().enumerate().all(|(i, b)| *b == byte(i)));
    assert!(errors(FULL_POOL).is_empty(), "{:?}", errors(FULL_POOL));
    drop(s);
    stop(runtime, handles);
}

#[test]
fn an_empty_send_with_sends_nothing() {
    let (runtime, handles) = launch();
    let mut s = connect(&runtime, EMPTY);
    assert_eq!(read_n(&mut s, 1), b"z");
    assert!(errors(EMPTY).is_empty(), "{:?}", errors(EMPTY));
    drop(s);
    stop(runtime, handles);
}
