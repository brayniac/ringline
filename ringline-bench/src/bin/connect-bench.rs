//! Connection-establishment rate: connect, optionally handshake TLS, send one
//! request, read the response, close. No connection reuse.
//!
//! This is the half of the accept-mode decision that per-connection throughput
//! cannot answer. A long-lived connection amortises `accept` over millions of
//! requests; one request per connection makes the accept path the work, which
//! is where the acceptor pool's per-connection channel hop and wake should
//! show up against merged mode's in-ring accept.
//!
//! Deliberately synchronous std threads rather than a runtime. The subject is
//! the *server's* accept path, and a blocking client removes a whole class of
//! client-side scheduling confound from the measurement.
//!
//! ## The thing that invalidates this benchmark
//!
//! Every connection closes, so whoever closes first holds the 4-tuple in
//! TIME_WAIT for 60s. The client closes here, so the client's ephemeral range
//! is the binding constraint: at 28k ports that is roughly 470 connections a
//! second sustained, far below anything worth measuring — and *both arms would
//! report that same number*, which reads as a confident null result.
//!
//! So the harness must widen `ip_local_port_range` and set
//! `net.ipv4.tcp_tw_reuse=1`, and this binary counts every connect failure and
//! reports it. A run with a non-zero `connect_errors` measured the client's
//! port table, not the server.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;

#[derive(Parser)]
#[command(about = "Connection-establishment rate, one request per connection")]
struct Args {
    /// Server address.
    #[arg(long)]
    addr: String,

    /// Connecting threads.
    #[arg(long, default_value_t = 8)]
    threads: usize,

    /// Measured seconds.
    #[arg(long, default_value_t = 20)]
    duration: u64,

    /// Unmeasured seconds before the window opens.
    #[arg(long, default_value_t = 5)]
    warmup: u64,

    /// Request bytes sent on each connection.
    #[arg(long, default_value_t = 64)]
    msg_size: usize,

    /// Negotiate TLS on every connection. The server's certificate is not
    /// verified — this measures handshake cost, and saying so is better than
    /// pretending a benchmark has a trust store.
    #[arg(long, default_value_t = false)]
    tls: bool,

    /// Write the result JSON here instead of stdout.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

/// Accepts any certificate. Only ever used by this benchmark, against a
/// self-signed cert the server generated seconds earlier.
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls_pki_types::CertificateDer<'_>,
        _intermediates: &[rustls_pki_types::CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls_pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Default)]
struct Counters {
    completed: AtomicU64,
    connect_errors: AtomicU64,
    io_errors: AtomicU64,
    handshakes: AtomicU64,
    latency_ns_sum: AtomicU64,
    latency_ns_max: AtomicU64,
}

fn client_config() -> Arc<rustls::ClientConfig> {
    let mut cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    // One connection, one request: session resumption would turn most
    // handshakes into an abbreviated one and stop this measuring what it says.
    cfg.resumption = rustls::client::Resumption::disabled();
    Arc::new(cfg)
}

#[allow(clippy::too_many_arguments)]
fn one_connection(
    addr: SocketAddr,
    payload: &[u8],
    tls: Option<&Arc<rustls::ClientConfig>>,
    counters: &Counters,
) {
    let started = Instant::now();
    let mut sock = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(_) => {
            counters.connect_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let _ = sock.set_nodelay(true);
    let _ = sock.set_read_timeout(Some(Duration::from_secs(5)));

    let mut buf = vec![0u8; payload.len()];
    let ok = match tls {
        None => sock
            .write_all(payload)
            .and_then(|_| sock.read_exact(&mut buf))
            .is_ok(),
        Some(cfg) => {
            let name = rustls_pki_types::ServerName::try_from("localhost").expect("server name");
            match rustls::ClientConnection::new(cfg.clone(), name) {
                Ok(mut conn) => {
                    let mut stream = rustls::Stream::new(&mut conn, &mut sock);
                    let done = stream
                        .write_all(payload)
                        .and_then(|_| stream.flush())
                        .and_then(|_| stream.read_exact(&mut buf))
                        .is_ok();
                    if done {
                        counters.handshakes.fetch_add(1, Ordering::Relaxed);
                    }
                    done
                }
                Err(_) => false,
            }
        }
    };

    if !ok {
        counters.io_errors.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let ns = started.elapsed().as_nanos() as u64;
    counters.completed.fetch_add(1, Ordering::Relaxed);
    counters.latency_ns_sum.fetch_add(ns, Ordering::Relaxed);
    counters.latency_ns_max.fetch_max(ns, Ordering::Relaxed);
}

fn main() {
    let args = Args::parse();
    let addr: SocketAddr = args.addr.parse().expect("addr");
    let payload = vec![b'x'; args.msg_size];
    let tls_cfg = args.tls.then(client_config);

    let warm = Arc::new(Counters::default());
    let measured = Arc::new(Counters::default());
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::with_capacity(args.threads);
    for _ in 0..args.threads {
        let (warm, measured) = (warm.clone(), measured.clone());
        let (measuring, stop) = (measuring.clone(), stop.clone());
        let payload = payload.clone();
        let tls_cfg = tls_cfg.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let c = if measuring.load(Ordering::Relaxed) {
                    &measured
                } else {
                    &warm
                };
                one_connection(addr, &payload, tls_cfg.as_ref(), c);
            }
        }));
    }

    std::thread::sleep(Duration::from_secs(args.warmup));
    measuring.store(true, Ordering::Relaxed);
    let window = Instant::now();
    std::thread::sleep(Duration::from_secs(args.duration));
    let elapsed = window.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let completed = measured.completed.load(Ordering::Relaxed);
    let connect_errors = measured.connect_errors.load(Ordering::Relaxed);
    let io_errors = measured.io_errors.load(Ordering::Relaxed);
    let handshakes = measured.handshakes.load(Ordering::Relaxed);
    let mean_ns = measured
        .latency_ns_sum
        .load(Ordering::Relaxed)
        .checked_div(completed)
        .unwrap_or(0);

    let json = format!(
        r#"{{
  "addr": "{}",
  "tls": {},
  "threads": {},
  "duration_secs": {:.3},
  "conns_per_sec": {:.2},
  "completed": {},
  "handshakes": {},
  "connect_errors": {},
  "io_errors": {},
  "mean_ns": {},
  "max_ns": {}
}}"#,
        args.addr,
        args.tls,
        args.threads,
        elapsed,
        completed as f64 / elapsed,
        completed,
        handshakes,
        connect_errors,
        io_errors,
        mean_ns,
        measured.latency_ns_max.load(Ordering::Relaxed),
    );

    match args.out {
        Some(ref path) => std::fs::write(path, &json).expect("write result"),
        None => println!("{json}"),
    }

    // A run that could not open sockets measured the client's port table, not
    // the server's accept path — and would report a number that looks like a
    // result. Fail loudly instead.
    if connect_errors > 0 {
        eprintln!(
            "connect-bench: {connect_errors} connect failures — widen \
             ip_local_port_range and set net.ipv4.tcp_tw_reuse=1; this run does \
             not measure the server"
        );
        std::process::exit(2);
    }
    // Likewise a "TLS" run where no handshake completed.
    if args.tls && handshakes == 0 {
        eprintln!("connect-bench: --tls set but no handshake completed");
        std::process::exit(3);
    }
}
