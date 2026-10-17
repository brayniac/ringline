//! The text-protocol `read_reply` over `with_bytes_segments`.
//!
//! Runs on both backends against an in-process stub server that answers
//! `get k:<n>` with an `n`-byte value and `gets` with each key's value. Values from empty to larger than the
//! hold cap arrive whole and in order, one at a time and pipelined. On
//! io_uring a value shorter than `recv_zc_threshold` is copied out of its
//! view, and a value that spans ring buffers is copied by the parser; on mio
//! neither happens.
//!
//! The counters are process-wide, so this file holds one test.

mod common;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use ringline::metrics::{RECV_RING, recv_ring};
use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, ParseResult, RinglineBuilder, TlsConfig,
};
use ringline_memcache::{Client, CompletedOp};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// 64 buffers of 4 KiB: a 16 KiB value spans four buffers, and the hold cap
/// (a quarter of the ring) is 64 KiB. A reserve of 8 lets the ring lend
/// buffers as views; at the default of 64 every completion is copied.
fn test_config() -> Config {
    test_config_builder().build().expect("valid config")
}

fn test_config_builder() -> ConfigBuilder {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .recv_segment_reserve(8)
        .max_connections(16)
        .send_pool(128, 16384)
}

const SIZES: [usize; 8] = [0, 1, 100, 3000, 4096, 16 * 1024, 70 * 1024, 200 * 1024];

fn value_of(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn key_of(len: usize) -> Vec<u8> {
    format!("k:{len}").into_bytes()
}

/// The length a key `k:<n>` names.
fn len_of(key: &[u8]) -> Option<usize> {
    std::str::from_utf8(key)
        .ok()?
        .strip_prefix("k:")?
        .parse()
        .ok()
}

/// `get` and `gets` get a `VALUE` block per key, `gets` with a CAS token.
fn reply(line: &[u8]) -> Vec<u8> {
    let mut words = line.split(|&b| b == b' ');
    let cas = match words.next() {
        Some(b"get") => false,
        Some(b"gets") => true,
        _ => return b"ERROR\r\n".to_vec(),
    };
    let mut out = Vec::new();
    for key in words.filter(|k| !k.is_empty()) {
        let Some(len) = len_of(key) else {
            return b"CLIENT_ERROR bad key\r\n".to_vec();
        };
        out.extend_from_slice(b"VALUE ");
        out.extend_from_slice(key);
        out.extend_from_slice(format!(" 0 {len}").as_bytes());
        if cas {
            out.extend_from_slice(b" 7");
        }
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&value_of(len));
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"END\r\n");
    out
}

struct StubServer;

impl AsyncEventHandler for StubServer {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let mut out = Vec::new();
                let n = rx
                    .with_data(|bytes| {
                        let mut at = 0;
                        while let Some(end) = bytes[at..].windows(2).position(|w| w == b"\r\n") {
                            out.extend_from_slice(&reply(&bytes[at..at + end]));
                            at += end + 2;
                        }
                        ParseResult::Consumed(at)
                    })
                    .await;
                if n == 0 {
                    break;
                }
                if tx.send_nowait(&out).is_err() {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        StubServer
    }
}

static SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static RESULT: OnceLock<Result<(), String>> = OnceLock::new();
static TLS_SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();
static TLS_RESULT: OnceLock<Result<(), String>> = OnceLock::new();

struct ClientHandler;

impl AsyncEventHandler for ClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let addr = *SERVER_ADDR.get().expect("server addr");
        Some(Box::pin(async move {
            let result = match ringline::connect(addr).await {
                Ok(conn) => run(conn, false).await,
                Err(e) => Err(format!("connect: {e}")),
            };
            RESULT.set(result).ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        ClientHandler
    }
}

struct TlsClientHandler;

impl AsyncEventHandler for TlsClientHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        let addr = *TLS_SERVER_ADDR.get().expect("server addr");
        Some(Box::pin(async move {
            let result = match ringline::connect(addr).tls("localhost").await {
                Ok(conn) => run(conn, true).await,
                Err(e) => Err(format!("connect: {e}")),
            };
            TLS_RESULT.set(result).ok();
            ringline::request_shutdown().ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsClientHandler
    }
}

fn check(len: usize, got: Option<&[u8]>) -> Result<(), String> {
    match got {
        Some(v) if v == value_of(len) => Ok(()),
        Some(v) => Err(format!("k:{len}: {} bytes, wrong content", v.len())),
        None => Err(format!("k:{len}: miss")),
    }
}

async fn run(conn: Connection, tls: bool) -> Result<(), String> {
    let mut client = Client::new(conn);

    // One at a time. Each small value is kept until the end, so a view that
    // was not copied would still hold its buffer then.
    let mut kept = Vec::new();
    for len in SIZES {
        let value = client
            .get(key_of(len))
            .await
            .map_err(|e| format!("get k:{len}: {e}"))?;
        let value = value.map(|v| v.data);
        check(len, value.as_deref())?;
        kept.push((len, value));
    }

    // Pipelined: several replies share buffers, and a reply can start in the
    // buffer the previous one ends in.
    for (i, len) in SIZES.iter().chain(SIZES.iter().rev()).enumerate() {
        client
            .fire_get(&key_of(*len), i as u64)
            .map_err(|e| format!("fire k:{len}: {e}"))?;
    }
    for (i, len) in SIZES.iter().chain(SIZES.iter().rev()).enumerate() {
        match client
            .recv()
            .await
            .map_err(|e| format!("recv k:{len}: {e}"))?
        {
            CompletedOp::Get {
                result, user_data, ..
            } if user_data == i as u64 => {
                let value = result.map_err(|e| format!("recv k:{len}: {e}"))?;
                check(*len, value.map(|v| v.data).as_deref())?;
            }
            _ => return Err(format!("recv k:{len}: not the GET fired as {i}")),
        }
    }

    // One reply with every size, keys kept with their values.
    let keys: Vec<Vec<u8>> = SIZES.iter().map(|&len| key_of(len)).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let values = client
        .gets(&key_refs)
        .await
        .map_err(|e| format!("gets: {e}"))?;
    if values.len() != SIZES.len() {
        return Err(format!("gets: {} values", values.len()));
    }
    for (len, value) in SIZES.iter().zip(values) {
        if value.key != key_of(*len) {
            return Err(format!("gets k:{len}: key {:?}", value.key));
        }
        check(*len, Some(&value.data))?;
    }

    // Over plaintext a reply that fits one buffer is one view, so each key
    // and each non-empty value shorter than the threshold is copied out of
    // it. Over TLS the reply is a slice of the accumulator.
    let small = [0usize, 1, 100, 3000];
    let keys: Vec<Vec<u8>> = small.iter().map(|&len| key_of(len)).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    let before = count(recv_ring::THRESHOLD_COPIED);
    let values = client
        .gets(&key_refs)
        .await
        .map_err(|e| format!("small gets: {e}"))?;
    let copied = count(recv_ring::THRESHOLD_COPIED) - before;
    for (len, value) in small.iter().zip(&values) {
        check(*len, Some(&value.data))?;
    }
    let want = if cfg!(has_io_uring) && !tls { 4 + 3 } else { 0 };
    if copied != want {
        return Err(format!("small gets: {copied} copies, want {want}"));
    }

    for (len, value) in kept {
        check(len, value.as_deref())?;
    }
    Ok(())
}

fn count(slot: usize) -> u64 {
    RECV_RING.value(slot).unwrap_or(0)
}

#[test]
fn values_of_every_size_read_through_segment_views() {
    let threshold_copied = count(recv_ring::THRESHOLD_COPIED);
    let parse_copied = count(recv_ring::SEGMENT_PARSE_COPIED);
    let views = count(recv_ring::VIEW_CREATED);
    let hold_cap_copied = count(recv_ring::HOLD_CAP_COPIED);

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

    if let Err(e) = RESULT.get().expect("client did not set result") {
        panic!("{e}");
    }
    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }

    let threshold_copied = count(recv_ring::THRESHOLD_COPIED) - threshold_copied;
    let parse_copied = count(recv_ring::SEGMENT_PARSE_COPIED) - parse_copied;
    let views = count(recv_ring::VIEW_CREATED) - views;
    let hold_cap_copied = count(recv_ring::HOLD_CAP_COPIED) - hold_cap_copied;
    if cfg!(has_io_uring) {
        assert!(views > 0, "no value was read as a view");
        assert!(threshold_copied > 0, "no short view was copied");
        // The 70 KiB and 200 KiB values are longer than the 64 KiB hold cap.
        assert!(hold_cap_copied > 0, "no value reached the hold cap");
        // The 16 KiB value alone spans four 4 KiB buffers.
        assert!(
            parse_copied >= 16 * 1024,
            "the parser copied {parse_copied} bytes"
        );
    } else {
        assert_eq!(threshold_copied, 0);
        assert_eq!(hold_cap_copied, 0);
        assert_eq!(parse_copied, 0);
    }

    // TLS connections read with `with_bytes`: plaintext is copied into the
    // accumulator and sliced, with no views and no parser copies.
    let threshold_copied = count(recv_ring::THRESHOLD_COPIED);
    let parse_copied = count(recv_ring::SEGMENT_PARSE_COPIED);
    let views = count(recv_ring::VIEW_CREATED);
    run_tls();
    assert_eq!(count(recv_ring::THRESHOLD_COPIED) - threshold_copied, 0);
    assert_eq!(count(recv_ring::SEGMENT_PARSE_COPIED) - parse_copied, 0);
    assert_eq!(count(recv_ring::VIEW_CREATED) - views, 0);
}

/// Runs the same client against the stub server behind TLS.
fn run_tls() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key: PrivateKeyDer<'static> =
        PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into();
    let certs = vec![CertificateDer::from(cert.cert)];
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key)
        .unwrap();
    let (s_shutdown, s_handles) = RinglineBuilder::new(
        test_config_builder()
            .tls(TlsConfig::new(Arc::new(server)))
            .build()
            .expect("valid config"),
    )
    .bind("127.0.0.1:0".parse().unwrap())
    .launch::<StubServer>()
    .expect("server launch failed");
    TLS_SERVER_ADDR
        .set(s_shutdown.bound_addr().expect("bound address"))
        .ok();

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs[0].clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let (c_shutdown, c_handles) = RinglineBuilder::new(
        test_config_builder()
            .tls_client(ringline::TlsClientConfig::new(Arc::new(client)))
            .build()
            .expect("valid config"),
    )
    .launch::<TlsClientHandler>()
    .expect("client launch failed");
    common::join_workers(&c_shutdown, c_handles);

    if let Err(e) = TLS_RESULT.get().expect("client did not set result") {
        panic!("TLS: {e}");
    }
    s_shutdown.shutdown();
    for h in s_handles {
        h.join().unwrap().unwrap();
    }
}
