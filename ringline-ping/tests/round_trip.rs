//! Round-trip integration tests for ringline-ping.
//!
//! Spins up a ringline server that speaks the ping protocol (responds
//! `PONG\r\n` to `PING\r\n`), then connects a ringline ping client
//! through `on_start` in client-only mode.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, ParseResult, RinglineBuilder,
};
use ringline_ping::{Pool, PoolConfig};

// ── Helpers ─────────────────────────────────────────────────────────────

static TEST_SERIALIZE: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
}

fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

fn free_port() -> u16 {
    // Ports come from *below* the ephemeral range (Linux's `ip_local_port_range`
    // starts at 32768, macOS at 49152). That is the whole fix for #431: the
    // kernel never auto-assigns a port down here, so the probe-bind/drop/rebind
    // window stops being a race. Nothing can take one of these out from under
    // the caller except another process asking for it by number.
    //
    // The old version probed with `bind(":0")` and dropped the listener, which
    // left the port in the ephemeral pool. Between the drop and the server's
    // real bind, the kernel could hand it to anyone — surfacing either as an
    // `AddrInUse` launch failure, or (worse, in
    // `async_outbound_connect_refused`) as a connection *succeeding* to a port
    // the test believed was dead.
    //
    // `cargo test` runs binaries concurrently, so the window is offset per
    // process; `CLAIMED` keeps threads inside one binary from colliding.
    use std::sync::Mutex;
    static CLAIMED: Mutex<Option<std::collections::HashSet<u16>>> = Mutex::new(None);
    const BASE: u16 = 20_000;
    const SPAN: u16 = 10_000;

    let stride = ((std::process::id() % 40) as u16).saturating_mul(250);
    for step in 0..SPAN {
        let port = BASE + (stride + step) % SPAN;
        {
            let mut guard = CLAIMED.lock().unwrap();
            if !guard.get_or_insert_with(Default::default).insert(port) {
                continue;
            }
        }
        // Confirm nothing currently holds it. Unlike the old probe, dropping
        // this listener does not return the port to a pool anyone draws from.
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in the test range {BASE}..{}", BASE + SPAN);
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

// ── Ping Server Handler ─────────────────────────────────────────────────

/// Minimal server: parses `PING\r\n` and responds `PONG\r\n`.
struct PingServer;

impl AsyncEventHandler for PingServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let n = rx
                    .with_data(|data| {
                        // Look for PING\r\n
                        if data.len() < 6 {
                            return ParseResult::NeedMore;
                        }
                        if data.starts_with(b"PING\r\n") {
                            let _ = tx.send_nowait(b"PONG\r\n");
                            ParseResult::Consumed(6)
                        } else {
                            let _ = tx.send_nowait(b"-ERR\r\n");
                            ParseResult::Consumed(data.len())
                        }
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        PingServer
    }
}

// ── Client-only handler for ping round-trip ─────────────────────────────

static PING_SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static PING_RESULT: OnceLock<String> = OnceLock::new();

struct PingClientHandler;

impl AsyncEventHandler for PingClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let server_addr = *PING_SERVER_ADDR.get().expect("server addr not set");
        Some(Box::pin(async move {
            let conn = match ringline::connect(server_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        PING_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    PING_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            let mut client = ringline_ping::Client::new(conn).expect("split the connection");
            match client.ping().await {
                Ok(()) => {
                    PING_RESULT.set("OK".to_string()).ok();
                }
                Err(e) => {
                    PING_RESULT.set(format!("PING_ERR:{e}")).ok();
                }
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        PingClientHandler
    }
}

#[test]
fn ping_round_trip() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    // Start ping server.
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (s_shutdown, s_handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<PingServer>()
        .expect("server launch failed");
    wait_for_server(&addr);

    PING_SERVER_ADDR.set(addr.parse().unwrap()).ok();

    // Launch client-only (no .bind()).
    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<PingClientHandler>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = PING_RESULT.get().expect("on_start did not set result");
    assert_eq!(result, "OK", "expected OK, got: {result}");

    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Pool round-trip ─────────────────────────────────────────────────────

static POOL_SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static POOL_RESULT: OnceLock<String> = OnceLock::new();

struct PingPoolClientHandler;

impl AsyncEventHandler for PingPoolClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let server_addr = *POOL_SERVER_ADDR.get().expect("server addr not set");
        Some(Box::pin(async move {
            let config = PoolConfig::new(server_addr, 2).connect_timeout_ms(5000);
            let mut pool = Pool::new(config);

            if let Err(e) = pool.connect_all().await {
                POOL_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                ringline::request_shutdown().ok();
                return;
            }

            assert_eq!(pool.connected_count(), 2);
            assert_eq!(pool.pool_size(), 2);

            // Ping via pool.
            match pool.client().await {
                Ok(mut client) => match client.ping().await {
                    Ok(()) => {}
                    Err(e) => {
                        POOL_RESULT.set(format!("PING_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    POOL_RESULT.set(format!("CLIENT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            }

            pool.close_all();
            assert_eq!(pool.connected_count(), 0);

            POOL_RESULT.set("OK".to_string()).ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        PingPoolClientHandler
    }
}

#[test]
fn ping_pool() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    // Start ping server.
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (s_shutdown, s_handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<PingServer>()
        .expect("server launch failed");
    wait_for_server(&addr);

    POOL_SERVER_ADDR.set(addr.parse().unwrap()).ok();

    // Launch client-only.
    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<PingPoolClientHandler>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = POOL_RESULT.get().expect("on_start did not set result");
    assert_eq!(result, "OK", "expected OK, got: {result}");

    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}

// ── Parse error test ────────────────────────────────────────────────────

/// Server that responds with garbage instead of PONG\r\n.
struct BadPingServer;

impl AsyncEventHandler for BadPingServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            let n = rx
                .with_data(|data| {
                    if data.len() < 6 {
                        return ParseResult::NeedMore;
                    }
                    // Respond with malformed data instead of PONG\r\n.
                    let _ = tx.send_nowait(b"GARBAGE_NOT_A_PONG\r\n");
                    ParseResult::Consumed(data.len())
                })
                .await;
            if n > 0 {
                // Keep connection open briefly so client can read the bad response.
                ringline::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        BadPingServer
    }
}

static BAD_SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static BAD_RESULT: OnceLock<String> = OnceLock::new();

struct BadPingClientHandler;

impl AsyncEventHandler for BadPingClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let server_addr = *BAD_SERVER_ADDR.get().expect("bad server addr not set");
        Some(Box::pin(async move {
            let conn = match ringline::connect(server_addr) {
                Ok(fut) => match fut.await {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        BAD_RESULT.set(format!("CONNECT_ERR:{e}")).ok();
                        ringline::request_shutdown().ok();
                        return;
                    }
                },
                Err(e) => {
                    BAD_RESULT.set(format!("SUBMIT_ERR:{e}")).ok();
                    ringline::request_shutdown().ok();
                    return;
                }
            };

            let mut client = ringline_ping::Client::new(conn).expect("split the connection");
            match client.ping().await {
                Ok(()) => {
                    BAD_RESULT.set("UNEXPECTED_OK".to_string()).ok();
                }
                Err(_e) => {
                    BAD_RESULT.set("ERROR".to_string()).ok();
                }
            }
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        BadPingClientHandler
    }
}

#[test]
fn parse_error_returns_error_not_hang() {
    let _guard = TEST_SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let (s_shutdown, s_handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<BadPingServer>()
        .expect("server launch failed");
    wait_for_server(&addr);

    BAD_SERVER_ADDR.set(addr.parse().unwrap()).ok();

    let (_c_shutdown, c_handles) = RinglineBuilder::new(test_config())
        .launch::<BadPingClientHandler>()
        .expect("client launch failed");

    for h in c_handles {
        h.join().unwrap().unwrap();
    }

    let result = BAD_RESULT.get().expect("on_start did not set result");
    assert_eq!(result, "ERROR", "expected parse error, got: {result}");

    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}
