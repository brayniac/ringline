//! `Client::recv_ready` against a stub server on a std thread, on both
//! backends.
//!
//! Each test's server reads a fixed number of requests and then writes
//! replies on a schedule, so the client can check when `recv_ready` resolves
//! and that requests fired while it is pending reach the server.

mod common;

use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, Config, ConfigBuilder, Connection, Either, RinglineBuilder};
use ringline_redis::{Client, CompletedOp, Error};

fn test_config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(16)
        .send_pool(16, 16384)
        .build()
        .expect("valid config")
}

/// How long the client checks that `recv_ready` stays pending.
const QUIET: Duration = Duration::from_millis(100);

/// How long the server waits between its writes.
const GAP: Duration = Duration::from_millis(300);

/// Bound on every wait that should succeed.
const LIMIT: Duration = Duration::from_secs(5);

/// Read from `stream` until `n` GET requests have arrived. A RESP GET is five
/// CRLF-terminated lines.
fn read_requests(stream: &mut TcpStream, n: usize) {
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    while seen.windows(2).filter(|w| w == b"\r\n").count() < 5 * n {
        let k = stream.read(&mut buf).expect("server read");
        assert!(k > 0, "client closed before sending {n} requests");
        seen.extend_from_slice(&buf[..k]);
    }
}

/// Accept one connection on a std thread and run `script` on it.
fn stub_server(script: fn(&mut TcpStream)) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        stream.set_nodelay(true).unwrap();
        script(&mut stream);
        // Hold the connection open until the client closes it.
        let mut buf = [0u8; 64];
        while matches!(stream.read(&mut buf), Ok(k) if k > 0) {}
    });
    addr
}

type ClientFuture = Pin<Box<dyn Future<Output = Result<(), String>>>>;

/// Run `client` on a ringline worker connected to `addr` and fail the test
/// with its error.
macro_rules! run_client {
    ($addr:expr, $client:expr) => {{
        static ADDR: Mutex<Option<SocketAddr>> = Mutex::new(None);
        static RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

        struct Handler;

        impl AsyncEventHandler for Handler {
            #[allow(clippy::manual_async_fn)]
            fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
                async {}
            }

            fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
                let addr = ADDR.lock().unwrap().expect("server address");
                Some(Box::pin(async move {
                    let result = match ringline::connect(addr).await {
                        Ok(conn) => {
                            let f: fn(Connection) -> ClientFuture = $client;
                            f(conn).await
                        }
                        Err(e) => Err(format!("connect: {e}")),
                    };
                    *RESULT.lock().unwrap() = Some(result);
                    ringline::request_shutdown().ok();
                }))
            }

            fn create_for_worker(_id: usize) -> Self {
                Handler
            }
        }

        *ADDR.lock().unwrap() = Some($addr);
        let (shutdown, handles) = RinglineBuilder::new(test_config())
            .launch::<Handler>()
            .expect("client launch failed");
        common::join_workers(&shutdown, handles);
        if let Err(e) = RESULT.lock().unwrap().take().expect("client set no result") {
            panic!("{e}");
        }
    }};
}

/// Check `op` is a GET with this `user_data` and value.
fn expect_get(op: CompletedOp, want_ud: u64, want: Option<&[u8]>) -> Result<(), String> {
    match op {
        CompletedOp::Get {
            result: Ok(value),
            user_data,
            ..
        } if user_data == want_ud && value.as_deref() == want => Ok(()),
        CompletedOp::Get {
            result, user_data, ..
        } => Err(format!(
            "got GET user_data {user_data} result {result:?}, want user_data {want_ud} value {want:?}"
        )),
        _ => Err("not a GET".into()),
    }
}

/// The server writes the first half of the reply `GAP` after the request and
/// the rest `GAP` later. `recv_ready` stays pending until the first half
/// arrives, and the `recv` started then waits for the second.
#[test]
fn resolves_on_the_first_reply_bytes() {
    let addr = stub_server(|s| {
        read_requests(s, 1);
        std::thread::sleep(GAP);
        s.write_all(b"$5\r\nhel").unwrap();
        std::thread::sleep(GAP);
        s.write_all(b"lo\r\n").unwrap();
    });
    run_client!(addr, |conn| Box::pin(async move {
        let mut client = Client::builder(conn).build();
        client.fire_get(b"k", 1).map_err(|e| format!("fire: {e}"))?;
        let mut ready = client
            .recv_ready()
            .map_err(|e| format!("recv_ready: {e}"))?;
        if let Either::Left(()) = ringline::select(&mut ready, ringline::sleep(QUIET)).await {
            return Err("recv_ready resolved before any reply bytes".into());
        }
        ringline::timeout(LIMIT, ready)
            .await
            .map_err(|_| "recv_ready did not resolve")?;
        let resolved = Instant::now();
        let op = ringline::timeout(LIMIT, client.recv())
            .await
            .map_err(|_| "recv timed out")?
            .map_err(|e| format!("recv: {e}"))?;
        if resolved.elapsed() < GAP / 2 {
            return Err("recv_ready resolved only after the whole reply".into());
        }
        expect_get(op, 1, Some(b"hello"))
    }));
}

/// At batch 16 a single fire stays in the write buffer; `recv_ready` sends
/// it.
#[test]
fn sends_a_buffered_request() {
    let addr = stub_server(|s| {
        read_requests(s, 1);
        s.write_all(b"$-1\r\n").unwrap();
    });
    run_client!(addr, |conn| Box::pin(async move {
        let mut client = Client::builder(conn).max_batch_size(16).build();
        client.fire_get(b"k", 1).map_err(|e| format!("fire: {e}"))?;
        let ready = client
            .recv_ready()
            .map_err(|e| format!("recv_ready: {e}"))?;
        ringline::timeout(LIMIT, ready)
            .await
            .map_err(|_| "recv_ready did not resolve")?;
        let op = client.recv().await.map_err(|e| format!("recv: {e}"))?;
        expect_get(op, 1, None)
    }));
}

/// The server answers only once both requests have arrived, so the second,
/// fired while `recv_ready` is pending, must reach it. Replies come back in
/// fire order.
async fn fire_while_pending(conn: Connection, batch: usize) -> Result<(), String> {
    let mut client = Client::builder(conn).max_batch_size(batch).build();
    client.fire_get(b"a", 1).map_err(|e| format!("fire: {e}"))?;
    let mut ready = client
        .recv_ready()
        .map_err(|e| format!("recv_ready: {e}"))?;
    if let Either::Left(()) = ringline::select(&mut ready, ringline::sleep(QUIET)).await {
        return Err("recv_ready resolved before the server replied".into());
    }
    client.fire_get(b"b", 2).map_err(|e| format!("fire: {e}"))?;
    if batch > 1 {
        client.flush().map_err(|e| format!("flush: {e}"))?;
    }
    ringline::timeout(LIMIT, ready)
        .await
        .map_err(|_| "recv_ready did not resolve")?;
    expect_get(
        client.recv().await.map_err(|e| e.to_string())?,
        1,
        Some(b"a"),
    )?;
    expect_get(
        client.recv().await.map_err(|e| e.to_string())?,
        2,
        Some(b"b"),
    )
}

fn answer_two(s: &mut TcpStream) {
    read_requests(s, 2);
    s.write_all(b"$1\r\na\r\n$1\r\nb\r\n").unwrap();
}

#[test]
fn fire_while_pending_at_batch_1() {
    let addr = stub_server(answer_two);
    run_client!(addr, |conn| Box::pin(fire_while_pending(conn, 1)));
}

#[test]
fn fire_while_pending_at_batch_16_then_flush() {
    let addr = stub_server(answer_two);
    run_client!(addr, |conn| Box::pin(fire_while_pending(conn, 16)));
}

#[test]
fn no_pending_with_nothing_in_flight() {
    let addr = stub_server(|_| {});
    run_client!(addr, |conn| Box::pin(async move {
        let mut client = Client::builder(conn).build();
        match client.recv_ready() {
            Err(Error::NoPending) => Ok(()),
            Err(e) => Err(format!("want NoPending, got {e}")),
            Ok(_) => Err("want NoPending, got a future".into()),
        }
    }));
}

/// A `recv_ready` dropped while pending leaves its registration behind. The
/// `recv` that follows still wakes for the reply and reads all of it.
#[test]
fn a_dropped_recv_ready_loses_nothing() {
    let addr = stub_server(|s| {
        read_requests(s, 1);
        std::thread::sleep(GAP);
        s.write_all(b"$5\r\nhello\r\n").unwrap();
    });
    run_client!(addr, |conn| Box::pin(async move {
        let mut client = Client::builder(conn).build();
        client.fire_get(b"k", 1).map_err(|e| format!("fire: {e}"))?;
        let ready = client
            .recv_ready()
            .map_err(|e| format!("recv_ready: {e}"))?;
        if let Either::Left(()) = ringline::select(ready, ringline::sleep(QUIET)).await {
            return Err("recv_ready resolved before any reply bytes".into());
        }
        let op = ringline::timeout(LIMIT, client.recv())
            .await
            .map_err(|_| "recv timed out")?
            .map_err(|e| format!("recv: {e}"))?;
        expect_get(op, 1, Some(b"hello"))
    }));
}
