//! `with_bytes_segments` over real sockets, on both backends and over TLS.
//!
//! The server reads with `with_bytes_segments` and records what its parser
//! was shown. On io_uring a line split over two writes is presented as two
//! pieces, a value kept from one piece stays intact after the ring's buffers
//! have been reused, and a message longer than the hold cap completes through
//! the hold-cap copy. On mio every call presents one piece.

use std::future::Future;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, ParseResult, RinglineBuilder, TlsConfig,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};

/// A 64-buffer ring of 4 KiB buffers: the hold cap is 16 entries. The
/// default `recv_segment_reserve` (64) would copy every segmented delivery
/// on a ring this size; at 8, deliveries are held in place and presented as
/// views until 8 buffers are left free.
fn test_config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .recv_segment_reserve(8)
        .max_connections(16)
        .send_pool(16, 16384)
        .build()
        .expect("valid config")
}

fn stop(
    shutdown: ringline::Runtime,
    handles: Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
) {
    shutdown.shutdown();
    for h in handles {
        h.join().unwrap().unwrap();
    }
}

/// Wait until `seen` holds at least `count` records and return them.
fn wait_for<T: Clone>(seen: &Mutex<Vec<T>>, count: usize) -> Vec<T> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let got = seen.lock().unwrap().clone();
        if got.len() >= count {
            return got;
        }
        assert!(
            Instant::now() < deadline,
            "server recorded {} of {count}",
            got.len()
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Pause between client writes, so each write arrives in its own completion.
const GAP: Duration = Duration::from_millis(30);

fn write_chunks(stream: &mut impl Write, chunks: &[&[u8]]) -> Vec<Instant> {
    let mut sent = Vec::new();
    for (i, chunk) in chunks.iter().enumerate() {
        if i > 0 {
            std::thread::sleep(GAP);
        }
        sent.push(Instant::now());
        stream.write_all(chunk).expect("client write");
        stream.flush().expect("client flush");
    }
    sent
}

/// The first line in `pieces`: a slice of the first piece when the line ends
/// inside it, otherwise a copy.
fn first_line(pieces: &[Bytes]) -> Option<Bytes> {
    assert!(!pieces.is_empty(), "the parser is called with no pieces");
    assert!(
        pieces.iter().all(|p| !p.is_empty()),
        "the parser is shown an empty piece"
    );
    let mut copy = Vec::new();
    for (k, p) in pieces.iter().enumerate() {
        if let Some(i) = p.iter().position(|&b| b == b'\n') {
            if k == 0 {
                return Some(p.slice(..=i));
            }
            copy.extend_from_slice(&p[..=i]);
            return Some(Bytes::from(copy));
        }
        copy.extend_from_slice(p);
    }
    None
}

/// One line the server read: its bytes, the pieces the parser was shown
/// for it, and `recv_arrival` after the read.
type Line = (Vec<u8>, usize, Option<(Instant, Instant)>);

/// Read lines from `conn` until end of stream, recording each.
async fn read_lines(conn: &mut Connection, seen: &Mutex<Vec<Line>>) {
    loop {
        let mut line = None;
        let mut shown = 0;
        let r = conn
            .with_bytes_segments(|pieces| {
                shown = pieces.len();
                match first_line(pieces) {
                    Some(l) => {
                        let n = l.len();
                        line = Some(l);
                        ParseResult::Consumed(n)
                    }
                    None => ParseResult::NeedMore,
                }
            })
            .await;
        match r {
            Ok(0) => return,
            Ok(_) => {}
            Err(e) => panic!("with_bytes_segments: {e}"),
        }
        let arrival = conn.recv_arrival().map(|a| (a.first(), a.last()));
        let line = line.expect("a consumed line").to_vec();
        seen.lock().unwrap().push((line, shown, arrival));
    }
}

/// A line, a line split over two writes, and two lines in one write.
const CHUNKS: [&[u8]; 4] = [b"one\n", b"tw", b"o\n", b"three\nfour\n"];

/// Check the lines of `CHUNKS`, the pieces the split line was shown in, and
/// that no line's arrival precedes the write that sent it.
fn check_lines(seen: &[Line], sent: &[Instant]) {
    let lines: Vec<&[u8]> = seen.iter().map(|(l, _, _)| l.as_slice()).collect();
    assert_eq!(lines, [&b"one\n"[..], b"two\n", b"three\n", b"four\n"]);
    if cfg!(has_io_uring) {
        assert_eq!(seen[1].1, 2, "the split line is shown as two pieces");
    } else {
        assert!(seen.iter().all(|(_, shown, _)| *shown == 1));
    }
    let times: Vec<(Instant, Instant)> = seen
        .iter()
        .map(|(l, _, a)| a.unwrap_or_else(|| panic!("no arrival for {l:?}")))
        .collect();
    assert!(times[0].0 >= sent[0], "one: arrival before it was sent");
    assert!(times[1].0 >= sent[1], "two: arrival before it was sent");
    assert!(times[1].1 >= sent[2], "two: last byte before its write");
    assert!(times[2].0 >= sent[3], "three: arrival before it was sent");
    assert!(times[3].0 >= sent[3], "four: arrival before it was sent");
}

// ── Lines, plaintext ────────────────────────────────────────────────────

#[cfg(not(has_io_uring))]
static PLAIN: Mutex<Vec<Line>> = Mutex::new(Vec::new());

#[cfg(not(has_io_uring))]
struct PlainLines;

#[cfg(not(has_io_uring))]
impl AsyncEventHandler for PlainLines {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move { read_lines(&mut conn, &PLAIN).await }
    }

    fn create_for_worker(_id: usize) -> Self {
        PlainLines
    }
}

// On io_uring an accepted plaintext connection receives directly and refuses
// segmented reads (`recv_direct.rs`); the TLS tests cover io_uring.
#[cfg(not(has_io_uring))]
#[test]
fn plaintext_lines_are_read_in_order() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<PlainLines>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");

    let mut client = TcpStream::connect(addr).expect("connect");
    let sent = write_chunks(&mut client, &CHUNKS);
    check_lines(&wait_for(&PLAIN, 4), &sent);

    drop(client);
    stop(shutdown, handles);
}

// ── Lines, TLS ──────────────────────────────────────────────────────────

static TLS: Mutex<Vec<Line>> = Mutex::new(Vec::new());

struct TlsLines;

impl AsyncEventHandler for TlsLines {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move { read_lines(&mut conn, &TLS).await }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsLines
    }
}

fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    (vec![CertificateDer::from(cert.cert)], key.into())
}

/// A running server, its workers, and a client connection that has finished
/// its TLS handshake.
type TlsServer = (
    ringline::Runtime,
    Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
    rustls::ClientConnection,
    TcpStream,
);

/// Launch `H` behind TLS and connect a client to it.
fn tls_server<H: AsyncEventHandler>(config: Config) -> TlsServer {
    let (certs, key) = self_signed();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key)
        .unwrap();
    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind_tls(
            "127.0.0.1:0".parse().unwrap(),
            TlsConfig::new(Arc::new(server)),
        )
        .launch::<H>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");

    let mut roots = rustls::RootCertStore::empty();
    roots.add(certs[0].clone()).unwrap();
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name: ServerName<'_> = "localhost".try_into().unwrap();
    let mut tls = rustls::ClientConnection::new(Arc::new(client_config), name).unwrap();
    let mut tcp = TcpStream::connect(addr).expect("connect");
    tcp.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    while tls.is_handshaking() {
        tls.complete_io(&mut tcp).expect("handshake");
    }
    (shutdown, handles, tls, tcp)
}

#[test]
fn tls_lines_are_read_in_order() {
    let (shutdown, handles, mut tls, mut tcp) = tls_server::<TlsLines>(test_config());
    let mut stream = rustls::Stream::new(&mut tls, &mut tcp);
    let sent = write_chunks(&mut stream, &CHUNKS);
    check_lines(&wait_for(&TLS, 4), &sent);

    drop(tcp);
    stop(shutdown, handles);
}

// ── end_segments, then with_data ────────────────────────────────────────

#[cfg(not(has_io_uring))]
static AFTER_END: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

/// Reads the first line with `with_bytes_segments`, calls `end_segments`,
/// and reads the next line with `with_data`.
#[cfg(not(has_io_uring))]
struct EndThenData;

#[cfg(not(has_io_uring))]
impl AsyncEventHandler for EndThenData {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut line = None;
            let n = conn
                .with_bytes_segments(|pieces| match first_line(pieces) {
                    Some(l) => {
                        let n = l.len();
                        line = Some(l.to_vec());
                        ParseResult::Consumed(n)
                    }
                    None => ParseResult::NeedMore,
                })
                .await
                .expect("with_bytes_segments");
            assert!(n > 0);
            AFTER_END.lock().unwrap().push(line.expect("a line"));
            conn.end_segments().expect("end_segments");
            let mut rest = Vec::new();
            let n = conn
                .with_data(|d| match d.iter().position(|&b| b == b'\n') {
                    Some(i) => {
                        rest.extend_from_slice(&d[..=i]);
                        ParseResult::Consumed(i + 1)
                    }
                    None => ParseResult::NeedMore,
                })
                .await;
            assert!(n > 0);
            AFTER_END.lock().unwrap().push(rest);
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        EndThenData
    }
}

// On io_uring an accepted plaintext connection receives directly and refuses
// segmented reads (`recv_direct.rs`); the TLS tests cover io_uring.
#[cfg(not(has_io_uring))]
#[test]
fn with_data_after_end_segments_reads_the_held_bytes() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<EndThenData>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"first\nsecond\n").expect("write");
    let seen = wait_for(&AFTER_END, 2);
    assert_eq!(seen, [b"first\n".to_vec(), b"second\n".to_vec()]);

    drop(client);
    stop(shutdown, handles);
}

// ── A kept value outlives the reuse of the ring ─────────────────────────

/// Lines the client sends after the kept one: 3000 bytes each, about 150
/// buffers of the 64-buffer ring.
#[cfg(not(has_io_uring))]
const FILLER_LINES: usize = 200;

#[cfg(not(has_io_uring))]
static KEPT: Mutex<Vec<bool>> = Mutex::new(Vec::new());

/// Keeps the first line, reads `FILLER_LINES` more, then records whether the
/// kept line is unchanged.
#[cfg(not(has_io_uring))]
struct KeepFirst;

#[cfg(not(has_io_uring))]
impl AsyncEventHandler for KeepFirst {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut kept = None;
            for i in 0..=FILLER_LINES {
                let n = conn
                    .with_bytes_segments(|pieces| match first_line(pieces) {
                        Some(l) => {
                            let n = l.len();
                            if i == 0 {
                                kept = Some(l);
                            }
                            ParseResult::Consumed(n)
                        }
                        None => ParseResult::NeedMore,
                    })
                    .await
                    .expect("with_bytes_segments");
                assert!(n > 0, "end of stream after {i} lines");
            }
            let kept = kept.expect("the first line");
            KEPT.lock().unwrap().push(&kept[..] == b"keep-this-value\n");
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        KeepFirst
    }
}

// On io_uring an accepted plaintext connection receives directly and refuses
// segmented reads (`recv_direct.rs`); the TLS tests cover io_uring.
#[cfg(not(has_io_uring))]
#[test]
fn a_kept_value_is_unchanged_after_the_ring_is_reused() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<KeepFirst>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");

    let mut client = TcpStream::connect(addr).expect("connect");
    client.write_all(b"keep-this-value\n").expect("write");
    std::thread::sleep(GAP);
    let mut filler = vec![b'x'; 2999];
    filler.push(b'\n');
    for _ in 0..FILLER_LINES {
        client.write_all(&filler).expect("write");
    }
    assert_eq!(wait_for(&KEPT, 1), [true]);
    if cfg!(has_io_uring) {
        assert!(
            recv_ring_count(ringline::metrics::recv_ring::VIEW_CREATED) > 0,
            "the lines were presented as views"
        );
    }

    drop(client);
    stop(shutdown, handles);
}

// ── A message longer than the hold cap ──────────────────────────────────

/// 1 MiB: 256 buffers of 4 KiB, or 64 TLS records, against a hold cap of 16.
const LONG: usize = 1 << 20;

fn long_message() -> Vec<u8> {
    let mut m = format!("{LONG}\n").into_bytes();
    m.extend((0..LONG).map(|i| (i % 251) as u8));
    m
}

/// Parse one `<len>\n<payload>` message: `NeedAtLeast` for the rest of the
/// payload, and on completion whether the payload has the expected bytes.
fn parse_long(pieces: &[Bytes], ok: &mut Option<bool>) -> ParseResult {
    let total: usize = pieces.iter().map(Bytes::len).sum();
    let head: Vec<u8> = pieces
        .iter()
        .flat_map(|p| p.iter().copied())
        .take(16)
        .collect();
    let Some(nl) = head.iter().position(|&b| b == b'\n') else {
        return ParseResult::NeedMore;
    };
    let len: usize = std::str::from_utf8(&head[..nl]).unwrap().parse().unwrap();
    let want = nl + 1 + len;
    if total < want {
        return ParseResult::NeedAtLeast(want - total);
    }
    let payload = pieces
        .iter()
        .flat_map(|p| p.iter().copied())
        .skip(nl + 1)
        .take(len);
    *ok = Some(payload.enumerate().all(|(i, b)| b == (i % 251) as u8));
    ParseResult::Consumed(want)
}

fn recv_ring_count(slot: usize) -> u64 {
    use metriken::CounterGroupMetric;
    ringline::metrics::RECV_RING
        .counter_value(slot)
        .unwrap_or(0)
}

fn hold_cap_copied() -> u64 {
    recv_ring_count(ringline::metrics::recv_ring::HOLD_CAP_COPIED)
}

#[cfg(not(has_io_uring))]
static LONG_PLAIN: Mutex<Vec<bool>> = Mutex::new(Vec::new());
static LONG_TLS: Mutex<Vec<bool>> = Mutex::new(Vec::new());

async fn read_long(conn: &mut Connection, seen: &Mutex<Vec<bool>>) {
    let mut ok = None;
    let n = conn
        .with_bytes_segments(|pieces| parse_long(pieces, &mut ok))
        .await
        .expect("with_bytes_segments");
    assert!(n > 0, "end of stream before the message completed");
    seen.lock().unwrap().push(ok.expect("parsed"));
}

#[cfg(not(has_io_uring))]
struct LongPlain;

#[cfg(not(has_io_uring))]
impl AsyncEventHandler for LongPlain {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move { read_long(&mut conn, &LONG_PLAIN).await }
    }

    fn create_for_worker(_id: usize) -> Self {
        LongPlain
    }
}

struct LongTls;

impl AsyncEventHandler for LongTls {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move { read_long(&mut conn, &LONG_TLS).await }
    }

    fn create_for_worker(_id: usize) -> Self {
        LongTls
    }
}

// The hold-cap counter is process-wide. Under nextest each test runs in its
// own process, so a count here is this test's.

// On io_uring an accepted plaintext connection receives directly and refuses
// segmented reads (`recv_direct.rs`); the TLS tests cover io_uring.
#[cfg(not(has_io_uring))]
#[test]
fn a_message_longer_than_the_hold_cap_completes() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<LongPlain>()
        .expect("launch failed");
    let addr = shutdown.bound_addr().expect("bound address");

    let mut client = TcpStream::connect(addr).expect("connect");
    for chunk in long_message().chunks(64 * 1024) {
        client.write_all(chunk).expect("write");
    }
    assert_eq!(wait_for(&LONG_PLAIN, 1), [true]);
    if cfg!(has_io_uring) {
        assert!(
            hold_cap_copied() > 0,
            "the message needed the hold-cap copy"
        );
    }

    drop(client);
    stop(shutdown, handles);
}

#[test]
fn a_tls_message_longer_than_the_hold_cap_completes() {
    let (shutdown, handles, mut tls, mut tcp) = tls_server::<LongTls>(test_config());
    let mut stream = rustls::Stream::new(&mut tls, &mut tcp);
    for chunk in long_message().chunks(64 * 1024) {
        stream.write_all(chunk).expect("write");
    }
    stream.flush().expect("flush");
    assert_eq!(wait_for(&LONG_TLS, 1), [true]);
    if cfg!(has_io_uring) {
        assert!(
            hold_cap_copied() > 0,
            "the message needed the hold-cap copy"
        );
    }

    drop(tcp);
    stop(shutdown, handles);
}
