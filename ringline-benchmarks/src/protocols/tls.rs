//! TLS echo benchmark — the harness for the unbuffered-TLS GO/NO-GO.
//!
//! # What this measures, and why it is shaped this way
//!
//! `ringline`'s `tls-unbuffered` feature removes one copy from the TLS
//! **application-data send** path (`docs/journal/2026-09-unbuffered-tls.md`).
//! Its GO/NO-GO criterion is "TLS sends measurably drop to 1 copy with no
//! regression in throughput or latency". Removing a copy is a **CPU-efficiency**
//! win first and a throughput win only if the server is CPU-bound, so the
//! primary metric here is **server CPU time per operation**, not ops/sec.
//!
//! Three properties fall out of that:
//!
//! 1. **The server runs in a child process.** Server CPU has to be isolated
//!    from client CPU. Every other bench in this crate runs both sides in one
//!    process and reports `process_cpu_time_ns()`, which is client + server
//!    added together — useless for attributing a server-side copy. Here the
//!    parent spawns `current_exe() --tls-server-child`, and the child reports
//!    its own `getrusage(RUSAGE_SELF)`.
//!
//!    That CPU number is **exact, not sampled**. A previous experiment sampled
//!    `/proc/<pid>/stat` from outside; that quantises to clock ticks (10 ms)
//!    and races the window boundaries. `getrusage` is the kernel's own
//!    accounting for the process, read by the process itself at the two window
//!    edges on request from the parent over a control socket — so the delta
//!    covers exactly the measurement window and nothing else. It is also
//!    portable (Linux and macOS), unlike `/proc`.
//!
//! 2. **The client is tokio + rustls, never ringline.** Both sides of a
//!    ringline-vs-ringline run would move when the engine changes, and the
//!    result would be unattributable. The client is held fixed across arms by
//!    construction: it does not link the TLS engine under test at all.
//!
//! 3. **The plaintext cell is a control.** `BenchmarkCombination::tls` selects
//!    TLS or plaintext on *both* ends. The `tls-unbuffered` feature cannot
//!    touch the plaintext path, so the plaintext row should be identical across
//!    the two arms. If it is not, the difference is run-to-run noise (or an
//!    environment artifact) and bounds how much of the TLS delta you may
//!    believe.
//!
//! # Running it
//!
//! See `ringline-benchmarks/README.md`. In short, from the workspace root:
//!
//! ```text
//! cargo run --release -p ringline-benchmarks -- \
//!     --tls --sizes 64,1024,16384,262144 --clients 8 --duration 10 \
//!     --json /tmp/tls-buffered.json
//!
//! cargo run --release -p ringline-benchmarks --features tls-unbuffered -- \
//!     --tls --sizes 64,1024,16384,262144 --clients 8 --duration 10 \
//!     --json /tmp/tls-unbuffered.json
//! ```
//!
//! Then diff `cpu_ns_per_op` per row. The child reports which engine it was
//! built with in its `READY` line, and the harness prints and records it, so an
//! arm mislabelled by a forgotten `--features` is visible in the output rather
//! than silently folded into the numbers.
//!
//! # Caveats a reader of the numbers needs
//!
//! - **Closed loop by default.** With `--tls-rate 0` (the default) each client
//!   is a synchronous request/response loop, so a faster server is offered more
//!   load. Per-op CPU normalises for that, but batching effects do not fully
//!   normalise: more ops per event-loop iteration amortise the loop better.
//!   `--tls-rate N` paces the clients to a fixed aggregate ops/sec so both arms
//!   do the same work, which is the cleaner comparison when the server has
//!   headroom.
//! - **Co-located.** Client and server share the host's cores. That inflates
//!   both arms roughly equally but caps the achievable rate.
//! - **On the mio backend this is not a clean copy-count experiment.** mio has
//!   no send-pool slot to encrypt into, so `unbuffered::encrypt_to_vec` grows a
//!   `Vec` with a size-independent 32 KiB zero-fill per chunk — a memset bought
//!   back in exchange for the copy removed, worst at small messages. See
//!   `README.md`, "The mio backend has a confound the io_uring backend does
//!   not". The io_uring path (`encrypt_to_sends`) has no such step.
//! - The child's CPU includes its acceptor and control threads. Both are idle
//!   during the window (the control thread blocks on a socket read between the
//!   two `stats` requests), so this is a constant well under the measurement.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::bench::TlsConfig;
use crate::stats::{BenchResult, LatencyHistogram, LatencyStats, self_cpu_time_ns};

/// Which TLS record-layer engine the `ringline` in this build compiles in.
///
/// Read from the child process rather than assumed by the parent: both are the
/// same binary, but printing it makes "did I actually pass `--features`?"
/// answerable from the output file instead of from shell history.
pub fn engine_name() -> &'static str {
    if cfg!(feature = "tls-unbuffered") {
        "unbuffered"
    } else {
        "buffered"
    }
}

// ── Server-side counters (child process only) ───────────────────────────

static SRV_OPS: AtomicU64 = AtomicU64::new(0);
static SRV_BYTES: AtomicU64 = AtomicU64::new(0);
static SRV_CONNS: AtomicU64 = AtomicU64::new(0);
/// Sends the server could not queue (send pool exhausted). Non-zero means the
/// server dropped work and the row must not be trusted; the parent flags it.
static SRV_SEND_FAILS: AtomicU64 = AtomicU64::new(0);
static SRV_MSG_SIZE: AtomicUsize = AtomicUsize::new(0);

// ── Child: the ringline echo server ─────────────────────────────────────

/// ringline echo handler, framed at exactly `msg_size` per operation.
///
/// Framing matters for the measurement. An unframed echo (`Consumed(data.len())`
/// on whatever arrived) turns one 256 KiB client message into an
/// arrival-dependent number of `send_nowait` calls, so "operations" would
/// depend on TCP segmentation and differ between arms. Consuming in exact
/// `msg_size` units makes one operation exactly one TLS encrypt of `msg_size`
/// bytes — the quantity whose copy count this experiment is about.
struct TlsEchoHandler;

impl ringline::AsyncEventHandler for TlsEchoHandler {
    #[allow(clippy::manual_async_fn)]
    fn on_accept(
        &self,
        conn: ringline::ConnCtx,
    ) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            SRV_CONNS.fetch_add(1, Ordering::Relaxed);
            let msg_size = SRV_MSG_SIZE.load(Ordering::Relaxed);
            let mut failed = false;
            loop {
                let consumed = conn
                    .with_data(|data| {
                        let mut off = 0;
                        while data.len() - off >= msg_size {
                            if conn.send_nowait(&data[off..off + msg_size]).is_err() {
                                // Do not tear the connection down silently:
                                // that would quietly reduce concurrency
                                // mid-run. Count it, stop, and let the parent
                                // decide the row is untrustworthy.
                                SRV_SEND_FAILS.fetch_add(1, Ordering::Relaxed);
                                failed = true;
                                break;
                            }
                            off += msg_size;
                            SRV_OPS.fetch_add(1, Ordering::Relaxed);
                            SRV_BYTES.fetch_add(msg_size as u64, Ordering::Relaxed);
                        }
                        if off == 0 {
                            ringline::ParseResult::NeedMore
                        } else {
                            ringline::ParseResult::Consumed(off)
                        }
                    })
                    .await;
                if consumed == 0 || failed {
                    break;
                }
            }
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        TlsEchoHandler
    }
}

/// Arguments for the child server process, parsed from argv by `main`.
#[derive(Clone, Copy, Debug)]
pub struct ChildArgs {
    pub tls: bool,
    pub msg_size: usize,
    pub clients: usize,
}

fn free_port() -> Option<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let port = l.local_addr().ok()?.port();
    drop(l);
    Some(port)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn make_self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("self-signed cert");
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    (CertificateDer::from(cert.cert), key.into())
}

/// Child-process entry point: run the ringline echo server until told to quit.
///
/// Prints one `READY ...` line on stdout (data port, control port, engine name,
/// and the DER of the self-signed cert as hex so the parent can trust it
/// without a PEM file on disk), then serves. Never returns.
pub fn run_server_child(args: ChildArgs) -> ! {
    SRV_MSG_SIZE.store(args.msg_size, Ordering::Relaxed);

    let ctrl_listener = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(e) => {
            println!("ERROR control listener bind failed: {e}");
            std::process::exit(1);
        }
    };
    let ctrl_port = ctrl_listener.local_addr().expect("ctrl addr").port();

    let (cert, key) = make_self_signed();
    let cert_hex = hex_encode(cert.as_ref());

    // Send-pool geometry. The slot *size* is deliberately left at ringline's
    // 16 KiB default rather than grown to fit `msg_size`: on io_uring it is
    // both the ciphertext chunk granularity for the unbuffered engine
    // (`encrypt_chunk` writes into one slot) and for the buffered one
    // (`ciphertext_to_sends` chunks at `slot_size`), so holding it fixed is
    // what keeps the two arms comparable. Sizing it per message would silently
    // change the chunk size along with the engine.
    //
    // The slot *count* has to cover every in-flight message: one op per
    // connection, each spanning `ceil(msg_size / slot_size)` slots, doubled for
    // headroom. Too few slots and the server drops sends — which the parent
    // reports as a warning rather than quietly folding into the numbers.
    const SLOT_SIZE: u32 = 16384;
    // INVESTIGATION ONLY: override the *send* pool slot size (recv buffers and
    // the slot count stay pinned to SLOT_SIZE so only the one variable moves).
    let send_slot_size: u32 = std::env::var("RL_SEND_SLOT_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(SLOT_SIZE);
    eprintln!(
        "CHILD send_slot_size={send_slot_size} msg_size={}",
        args.msg_size
    );
    let chunks_per_msg = args.msg_size.div_ceil(SLOT_SIZE as usize).max(1);
    let slots = (args.clients * chunks_per_msg * 2 + 64).clamp(256, 8192) as u16;
    let recv_bufs = (args.clients * 4).clamp(512, 4096) as u16;
    let sq_entries = (args.clients * chunks_per_msg * 2)
        .next_power_of_two()
        .clamp(1024, 8192) as u32;

    // Bind is racy against every other process on the box, so retry with a
    // fresh port rather than failing the whole sweep on one collision.
    let mut launched = None;
    for _ in 0..32 {
        let Some(port) = free_port() else { continue };
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");

        let mut builder = ringline::ConfigBuilder::new()
            .workers(1)
            .pin_to_core(false)
            .sq_entries(sq_entries)
            .recv_buffer(recv_bufs, SLOT_SIZE)
            .max_connections(16384)
            .send_pool(slots, send_slot_size);
        if args.tls {
            let server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(vec![cert.clone()], key.clone_key())
                .expect("server tls config");
            builder = builder.tls(ringline::TlsConfig::new(Arc::new(server_config)));
        }
        let config = builder.build().expect("valid config");

        match ringline::RinglineBuilder::new(config)
            .bind(addr)
            .launch::<TlsEchoHandler>()
        {
            Ok((shutdown, handles)) => {
                launched = Some((addr, shutdown, handles));
                break;
            }
            Err(_) => continue,
        }
    }

    let Some((data_addr, _shutdown, handles)) = launched else {
        println!("ERROR ringline server failed to launch");
        std::process::exit(1);
    };

    // Control thread: the parent's only channel into this process. Line
    // protocol, one connection, kept open for the whole run.
    std::thread::spawn(move || {
        for stream in ctrl_listener.incoming() {
            let Ok(stream) = stream else { continue };
            stream.set_nodelay(true).ok();
            let Ok(mut out) = stream.try_clone() else {
                continue;
            };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
                match line.trim() {
                    "stats" => {
                        let reply = format!(
                            "cpu_ns={} ops={} bytes={} conns={} send_fails={}\n",
                            self_cpu_time_ns(),
                            SRV_OPS.load(Ordering::Relaxed),
                            SRV_BYTES.load(Ordering::Relaxed),
                            SRV_CONNS.load(Ordering::Relaxed),
                            SRV_SEND_FAILS.load(Ordering::Relaxed),
                        );
                        if out.write_all(reply.as_bytes()).is_err() {
                            break;
                        }
                        out.flush().ok();
                    }
                    "quit" => {
                        out.write_all(b"bye\n").ok();
                        out.flush().ok();
                        std::process::exit(0);
                    }
                    _ => {
                        out.write_all(b"err unknown\n").ok();
                        out.flush().ok();
                    }
                }
            }
        }
    });

    // Give the workers a moment to reach their accept loop before advertising.
    std::thread::sleep(Duration::from_millis(100));
    println!(
        "READY data={} ctrl={} engine={} tls={} cert={}",
        data_addr.port(),
        ctrl_port,
        engine_name(),
        u8::from(args.tls),
        cert_hex,
    );
    std::io::stdout().flush().ok();

    for h in handles {
        h.join().ok();
    }
    // The server only stops when the control thread calls `exit`; if every
    // worker somehow exits first, do not linger as an orphan.
    std::process::exit(0);
}

// ── Parent: child lifecycle + control channel ───────────────────────────

/// A snapshot of the server process's accounting at one instant.
#[derive(Clone, Copy, Debug, Default)]
struct ServerSnapshot {
    cpu_ns: u64,
    ops: u64,
    bytes: u64,
    conns: u64,
    send_fails: u64,
}

struct ServerChild {
    child: Child,
    ctrl_w: TcpStream,
    ctrl_r: BufReader<TcpStream>,
    data_addr: SocketAddr,
    cert: CertificateDer<'static>,
    engine: String,
}

impl ServerChild {
    fn spawn(tls: bool, msg_size: usize, clients: usize) -> Result<Self, String> {
        let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
        let mut cmd = Command::new(exe);
        cmd.arg("--tls-server-child")
            .arg("--sizes")
            .arg(msg_size.to_string())
            .arg("--clients")
            .arg(clients.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if tls {
            cmd.arg("--tls");
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("spawn server child: {e}"))?;

        let stdout = child.stdout.take().ok_or("child stdout")?;

        // One thread owns the child's stdout for the child's whole life: it
        // hands the first line back over a channel and then drains the rest,
        // so the pipe can never fill and wedge the child on a stray println.
        //
        // The first line is read behind a deadline. The child prints READY
        // once its listeners are up and ERROR if it could not start, so both
        // outcomes arrive promptly — but a child that hung before printing
        // anything would otherwise wedge the entire sweep on a pipe read that
        // has no timeout of its own.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            let _ = ready_tx.send(reader.read_line(&mut line).map(|_| line));
            let mut sink = String::new();
            while reader.read_line(&mut sink).unwrap_or(0) > 0 {
                sink.clear();
            }
        });

        let line = match ready_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(line)) if line.starts_with("READY ") => line,
            other => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(match other {
                    Ok(Ok(line)) => format!("server child did not start: {}", line.trim()),
                    Ok(Err(e)) => format!("reading child READY: {e}"),
                    Err(_) => "server child printed no READY line within 30s".to_string(),
                });
            }
        };

        let mut data_port = None;
        let mut ctrl_port = None;
        let mut cert_hex = None;
        let mut engine = String::from("unknown");
        for field in line.split_whitespace().skip(1) {
            let Some((k, v)) = field.split_once('=') else {
                continue;
            };
            match k {
                "data" => data_port = v.parse::<u16>().ok(),
                "ctrl" => ctrl_port = v.parse::<u16>().ok(),
                "cert" => cert_hex = Some(v.to_string()),
                "engine" => engine = v.to_string(),
                _ => {}
            }
        }
        let (Some(data_port), Some(ctrl_port), Some(cert_hex)) = (data_port, ctrl_port, cert_hex)
        else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("malformed READY line: {}", line.trim()));
        };

        let cert = hex_decode(&cert_hex)
            .map(CertificateDer::from)
            .ok_or("malformed cert hex")?;

        let ctrl_w = TcpStream::connect(("127.0.0.1", ctrl_port))
            .map_err(|e| format!("connect control port: {e}"))?;
        ctrl_w.set_nodelay(true).ok();
        ctrl_w
            .set_read_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| format!("control read timeout: {e}"))?;
        let ctrl_r = BufReader::new(
            ctrl_w
                .try_clone()
                .map_err(|e| format!("clone control socket: {e}"))?,
        );

        Ok(ServerChild {
            child,
            ctrl_w,
            ctrl_r,
            data_addr: format!("127.0.0.1:{data_port}").parse().expect("addr"),
            cert,
            engine,
        })
    }

    fn snapshot(&mut self) -> Result<ServerSnapshot, String> {
        self.ctrl_w
            .write_all(b"stats\n")
            .map_err(|e| format!("stats write: {e}"))?;
        self.ctrl_w.flush().ok();
        let mut line = String::new();
        self.ctrl_r
            .read_line(&mut line)
            .map_err(|e| format!("stats read: {e}"))?;
        let mut snap = ServerSnapshot::default();
        for field in line.split_whitespace() {
            let Some((k, v)) = field.split_once('=') else {
                continue;
            };
            let n = v.parse::<u64>().unwrap_or(0);
            match k {
                "cpu_ns" => snap.cpu_ns = n,
                "ops" => snap.ops = n,
                "bytes" => snap.bytes = n,
                "conns" => snap.conns = n,
                "send_fails" => snap.send_fails = n,
                _ => {}
            }
        }
        Ok(snap)
    }

    fn shutdown(&mut self) {
        let _ = self.ctrl_w.write_all(b"quit\n");
        let _ = self.ctrl_w.flush();
        // Give the child a moment to exit on its own, then insist.
        for _ in 0..100 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(_) => break,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Shutdown is in `Drop` rather than only on the happy path: a sweep bails out
/// of a cell on any control-socket error, and a leaked server child would then
/// keep a core busy underneath every subsequent cell — silently inflating the
/// CPU-per-op of rows measured after it.
impl Drop for ServerChild {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ── Parent: the tokio + rustls client ───────────────────────────────────

fn client_tls_config(cert: &CertificateDer<'static>) -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).expect("add self-signed root");
    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

struct ClientShared {
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    /// First connection's negotiated parameters, used to *prove* the TLS arm
    /// really handshook rather than falling through to a plaintext echo.
    detail: Arc<std::sync::Mutex<Option<String>>>,
}

async fn echo_loop<S>(
    stream: &mut S,
    msg_size: usize,
    pace: Option<Duration>,
    shared: &ClientShared,
) -> LatencyHistogram
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let msg = vec![0xABu8; msg_size];
    let mut recv_buf = vec![0u8; msg_size];
    let mut histogram = LatencyHistogram::new();
    let mut local_ops: u64 = 0;
    let mut next = tokio::time::Instant::now();

    while !shared.stop.load(Ordering::Relaxed) {
        if let Some(pace) = pace {
            next += pace;
            let now = tokio::time::Instant::now();
            if next > now {
                tokio::time::sleep_until(next).await;
            } else {
                // Behind schedule: do not accumulate debt to burn off in a
                // burst later, which would misreport the offered rate.
                next = now;
            }
        }

        let t0 = Instant::now();
        if stream.write_all(&msg).await.is_err() {
            break;
        }
        if stream.read_exact(&mut recv_buf).await.is_err() {
            break;
        }
        histogram.record(t0.elapsed().as_nanos() as u64);

        local_ops += 1;
        if local_ops & 0xFF == 0 {
            shared.ops.fetch_add(256, Ordering::Relaxed);
        }
    }

    shared.ops.fetch_add(local_ops & 0xFF, Ordering::Relaxed);
    histogram
}

async fn client_task(
    addr: SocketAddr,
    msg_size: usize,
    tls: Option<Arc<rustls::ClientConfig>>,
    pace: Option<Duration>,
    shared: ClientShared,
) -> LatencyHistogram {
    let stream = match tokio::net::TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  client connect failed: {e}");
            return LatencyHistogram::new();
        }
    };
    stream.set_nodelay(true).ok();

    match tls {
        None => {
            {
                let mut d = shared.detail.lock().expect("detail lock");
                if d.is_none() {
                    *d = Some("plaintext".to_string());
                }
            }
            let mut stream = stream;
            echo_loop(&mut stream, msg_size, pace, &shared).await
        }
        Some(config) => {
            let connector = tokio_rustls::TlsConnector::from(config);
            let server_name = rustls::pki_types::ServerName::try_from("localhost")
                .expect("server name")
                .to_owned();
            let mut stream = match connector.connect(server_name, stream).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  TLS handshake failed: {e}");
                    return LatencyHistogram::new();
                }
            };
            {
                let mut d = shared.detail.lock().expect("detail lock");
                if d.is_none() {
                    let (_, conn) = stream.get_ref();
                    *d = Some(format!(
                        "{} / {}",
                        conn.protocol_version()
                            .map(|v| format!("{v:?}"))
                            .unwrap_or_else(|| "?".into()),
                        conn.negotiated_cipher_suite()
                            .map(|s| format!("{:?}", s.suite()))
                            .unwrap_or_else(|| "?".into()),
                    ));
                }
            }
            echo_loop(&mut stream, msg_size, pace, &shared).await
        }
    }
}

// ── Result type ─────────────────────────────────────────────────────────

/// One TLS (or plaintext control) echo cell.
///
/// `cpu_ns_per_op` and `cpu_ns_per_byte` are the headline numbers: server-side
/// CPU divided by work the *server* observed in the same window. Throughput and
/// latency are the GO/NO-GO's "no regression" guard, not the claim.
#[derive(Clone, serde::Serialize)]
pub struct TlsEchoResult {
    pub engine: String,
    pub tls: String,
    /// Negotiated TLS version/cipher suite, or `plaintext`. Evidence that the
    /// TLS arm actually handshook.
    pub negotiated: String,
    pub msg_size: usize,
    pub clients: usize,
    pub target_rate: u64,
    pub client: BenchResult,
    pub server_cpu_ns: u64,
    pub server_ops: u64,
    pub server_bytes: u64,
    pub server_conns: u64,
    pub server_send_fails: u64,
    pub cpu_ns_per_op: f64,
    pub cpu_ns_per_byte: f64,
    /// Set when the row must not be trusted (server dropped sends, no ops
    /// completed, or the server saw fewer connections than clients).
    pub warning: Option<String>,
}

fn empty_latency() -> LatencyStats {
    LatencyStats {
        p50_ns: 0,
        p90_ns: 0,
        p99_ns: 0,
        p999_ns: 0,
        p9999_ns: 0,
        max_ns: 0,
        count: 0,
    }
}

/// Run one TLS-echo cell end to end: spawn the server child, drive it with
/// `num_clients` tokio clients, and return client-side throughput/latency
/// alongside the server process's own CPU accounting for the window.
#[allow(clippy::too_many_arguments)]
pub fn run_tls_echo(
    tls: TlsConfig,
    num_clients: usize,
    msg_size: usize,
    warmup: Duration,
    duration: Duration,
    client_threads: usize,
    target_rate: u64,
) -> Result<TlsEchoResult, String> {
    let use_tls = tls == TlsConfig::Required;
    let mut server = ServerChild::spawn(use_tls, msg_size, num_clients)?;

    let tls_config = use_tls.then(|| client_tls_config(&server.cert));
    let pace = (target_rate > 0)
        .then(|| Duration::from_secs_f64(num_clients as f64 / target_rate.max(1) as f64));

    let shared_stop = Arc::new(AtomicBool::new(false));
    let shared_ops = Arc::new(AtomicU64::new(0));
    let detail = Arc::new(std::sync::Mutex::new(None));

    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(client_threads.max(1))
        .enable_all()
        .build()
        .map_err(|e| format!("client runtime: {e}"))?;

    let mut handles = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        let shared = ClientShared {
            stop: shared_stop.clone(),
            ops: shared_ops.clone(),
            detail: detail.clone(),
        };
        handles.push(client_rt.spawn(client_task(
            server.data_addr,
            msg_size,
            tls_config.clone(),
            pace,
            shared,
        )));
    }

    std::thread::sleep(warmup);

    // Window: both the client op counter and the server's own counters are
    // sampled at the same two instants.
    shared_ops.store(0, Ordering::Relaxed);
    let before = server.snapshot()?;
    let start = Instant::now();
    std::thread::sleep(duration);
    let elapsed = start.elapsed();
    let after = server.snapshot()?;
    shared_stop.store(true, Ordering::Relaxed);

    let mut merged = LatencyHistogram::new();
    client_rt.block_on(async {
        // One deadline for the whole gather, not one per task: a per-task
        // timeout multiplies by the client count, and at high concurrency the
        // teardown would dominate the run.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        for handle in handles {
            if let Ok(Ok(h)) = tokio::time::timeout_at(deadline, handle).await {
                for &s in h.samples() {
                    merged.record(s);
                }
            }
        }
    });
    client_rt.shutdown_timeout(Duration::from_secs(2));
    let engine = server.engine.clone();
    drop(server);

    let client_ops = shared_ops.load(Ordering::Relaxed);
    let server_cpu_ns = after.cpu_ns.saturating_sub(before.cpu_ns);
    let server_ops = after.ops.saturating_sub(before.ops);
    let server_bytes = after.bytes.saturating_sub(before.bytes);
    let send_fails = after.send_fails.saturating_sub(before.send_fails);

    let achieved_rate = client_ops as f64 / elapsed.as_secs_f64();
    let mut warning = None;
    if server_ops == 0 {
        warning = Some("server completed no operations in the window".into());
    } else if send_fails > 0 {
        warning = Some(format!(
            "server dropped {send_fails} sends (pool exhausted)"
        ));
    } else if after.conns < num_clients as u64 {
        warning = Some(format!(
            "server saw {} connections, expected {num_clients}",
            after.conns
        ));
    } else if target_rate > 0
        && (achieved_rate - target_rate as f64).abs() > 0.1 * target_rate as f64
    {
        // A paced run that silently misses its target is not the fixed-load
        // comparison it claims to be, and the two arms can miss it by different
        // amounts. Two ways to miss: the server could not keep up, or the pacer
        // could not go fast enough. `tokio::time::sleep_until` runs on a ~1 ms
        // timer wheel, so one client tops out near 1000 ops/s and a target
        // above `1000 * clients` is unreachable however idle the server is.
        warning = Some(format!(
            "paced target {target_rate} ops/s, achieved {achieved_rate:.0} \
             (pacer ceiling is ~1000 ops/s per client, and there are {num_clients})"
        ));
    }

    let negotiated = detail
        .lock()
        .expect("detail lock")
        .clone()
        .unwrap_or_else(|| "none".into());

    Ok(TlsEchoResult {
        engine,
        tls: tls.to_string(),
        negotiated,
        msg_size,
        clients: num_clients,
        target_rate,
        client: BenchResult {
            ops_per_sec: client_ops as f64 / elapsed.as_secs_f64(),
            latency: if merged.samples().is_empty() {
                empty_latency()
            } else {
                merged.finalize()
            },
            // Parent-process CPU is client CPU here; the server number is
            // reported separately and is the one that matters.
            cpu_ns: 0,
        },
        server_cpu_ns,
        server_ops,
        server_bytes,
        server_conns: after.conns,
        server_send_fails: send_fails,
        cpu_ns_per_op: if server_ops == 0 {
            0.0
        } else {
            server_cpu_ns as f64 / server_ops as f64
        },
        cpu_ns_per_byte: if server_bytes == 0 {
            0.0
        } else {
            server_cpu_ns as f64 / server_bytes as f64
        },
        warning,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips() {
        let bytes: Vec<u8> = (0u8..=255).collect();
        let encoded = hex_encode(&bytes);
        assert_eq!(encoded.len(), 512);
        assert_eq!(hex_decode(&encoded).expect("decode"), bytes);
    }

    #[test]
    fn hex_decode_rejects_garbage() {
        assert!(hex_decode("abc").is_none());
        assert!(hex_decode("zz").is_none());
    }

    #[test]
    fn engine_name_matches_feature() {
        #[cfg(feature = "tls-unbuffered")]
        assert_eq!(engine_name(), "unbuffered");
        #[cfg(not(feature = "tls-unbuffered"))]
        assert_eq!(engine_name(), "buffered");
    }
}
