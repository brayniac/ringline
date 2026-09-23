//! Standalone echo server for distributed benchmarking.
//!
//! Usage:
//!   bench-server --runtime ringline --addr 0.0.0.0:7878 --workers 4 --msg-size 64
//!   bench-server --runtime tokio --addr 0.0.0.0:7878 --workers 4

use ringline::{ConfigBuilder, Connection};
use std::net::SocketAddr;

use clap::Parser;

/// Everything the proxy mode needs, bundled because the argument list had
/// outgrown what clippy will accept and most of it travels together anyway.
#[derive(Clone)]
struct ProxyCfg {
    addr: SocketAddr,
    workers: usize,
    msg_size: usize,
    backend: SocketAddr,
    recv_buffer_bytes: u32,
    recv_ring_size: u16,
    conn_chunk_size: usize,
    pin_to_core: bool,
    prefault_buffers: bool,
    api: ProxyApi,
    metrics_out: Option<std::path::PathBuf>,
}

/// A self-signed server config for the connect-rate arm.
///
/// Generated per process: the connect-rate benchmark is about handshake cost,
/// not about key management, and generating here keeps cert material out of
/// the repo and off the guests.
fn self_signed_server_config() -> std::sync::Arc<rustls::ServerConfig> {
    use rustls_pki_types::{CertificateDer, PrivatePkcs8KeyDer};
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate self-signed cert");
    let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let chain = vec![CertificateDer::from(cert.cert)];
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(chain, key.into())
        .expect("valid server config");
    std::sync::Arc::new(config)
}

/// Everything the echo arm needs. A struct for the same reason `ProxyCfg` is
/// one: the argument list had outgrown what clippy accepts, and these travel
/// together anyway.
#[derive(Clone)]
struct EchoCfg {
    addr: SocketAddr,
    workers: usize,
    msg_size: usize,
    metrics_out: Option<std::path::PathBuf>,
    recv_buffer_bytes: u32,
    recv_ring_size: u16,
    echo_mode: EchoMode,
    conn_chunk_size: usize,
    pin_to_core: bool,
    prefault_buffers: bool,
    accept_mode: AcceptModeArg,
    /// Terminate TLS on the listener, with a self-signed certificate generated
    /// at startup so nothing has to be staged onto a guest.
    tls: bool,
}

/// Which accept path the echo arm runs, so a pool-vs-merged A/B does not need
/// two builds. Mirrors `ringline::AcceptMode`; `Merged` is io_uring only and is
/// ignored on a mio build.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum AcceptModeArg {
    /// One acceptor thread per listener, explicit round-robin placement.
    Pool,
    /// Each worker owns a SO_REUSEPORT listener and accepts on its own ring.
    Merged,
}

impl From<AcceptModeArg> for ringline::AcceptMode {
    fn from(a: AcceptModeArg) -> Self {
        match a {
            AcceptModeArg::Pool => ringline::AcceptMode::Pool,
            AcceptModeArg::Merged => ringline::AcceptMode::Merged,
        }
    }
}

/// Which forwarding entry point the proxy arm drives.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ProxyApi {
    /// `forward_to` with a `SinkFd` over a blocking `std::net::TcpStream`.
    /// io_uring only — `SinkFd` is.
    SinkFd,
    /// `forward_to_conn` with a ringline connection as the sink. The only form
    /// that exists on both backends, so the only one an io_uring-vs-mio A/B
    /// can use: on io_uring it holds provided buffers, on mio it copies into
    /// the sink's send queue.
    Conn,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum TokioScheduler {
    MultiThread,
    PerCore,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum TokioEcho {
    Copy,
    Splice,
}

/// Which ringline echo strategy `bench-server` drives.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum EchoMode {
    /// `run_direct_echo` — submit the echo from the CQE handler.
    Direct,
    /// `with_data` + `forward_recv_buf`.
    Forward,
    /// `enable_recv_forward` + `forward_held`.
    RecvForward,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Runtime {
    Ringline,
    /// tokio on its own io_uring runtime (`tokio-uring`). Linux only, and only
    /// when built with `--features tokio-uring-arm`.
    TokioUring,
    Tokio,
}

#[derive(Parser)]
#[command(
    name = "bench-server",
    about = "Echo server for distributed benchmarking"
)]
struct Args {
    /// Server runtime
    #[arg(long, required_unless_present = "print_backend")]
    runtime: Option<Runtime>,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:7878")]
    addr: SocketAddr,

    /// Number of worker threads (0 = available parallelism)
    #[arg(long, default_value_t = 0)]
    workers: usize,

    /// Message size hint for buffer tuning (bytes)
    #[arg(long, default_value_t = 4096)]
    msg_size: usize,

    /// (ringline only) Echo via the multi-buffer zero-copy recv-forward path.
    /// Equivalent to `--echo-mode recv-forward`; kept because the campaign
    /// specs pass it.
    #[arg(long, default_value_t = false)]
    recv_forward: bool,

    /// (ringline only) Which echo strategy the handler uses. These are three
    /// genuinely different runtime paths, and which one a measurement
    /// exercises has been easy to get wrong:
    ///
    /// - `direct` (default): `run_direct_echo`, echo submitted straight from
    ///   the CQE handler with no task wakeup. io_uring only.
    /// - `forward`: `with_data` + `forward_recv_buf` — the ordinary
    ///   parse-then-forward loop a protocol server would write.
    /// - `recv-forward`: `enable_recv_forward` + `forward_held`. A byte pipe;
    ///   `with_data`/`with_bytes` observe nothing while it is on.
    #[arg(long, value_enum, default_value_t = EchoMode::Direct)]
    echo_mode: EchoMode,

    /// (ringline only) Which accept path the echo arm runs. `merged` is
    /// io_uring only and is ignored on a mio build.
    #[arg(long, value_enum, default_value_t = AcceptModeArg::Pool)]
    accept_mode: AcceptModeArg,

    /// (ringline only) Terminate TLS, with a self-signed certificate
    /// generated at startup.
    #[arg(long, default_value_t = false)]
    tls: bool,

    /// (tokio only) Scheduler shape. `multi-thread` is tokio's default
    /// work-stealing runtime; `per-core` gives each core its own
    /// `current_thread` runtime and `SO_REUSEPORT` listener, matching
    /// ringline's thread-per-core structure. Isolates scheduler shape from
    /// I/O interface in the comparison.
    #[arg(long, value_enum, default_value_t = TokioScheduler::MultiThread)]
    tokio_scheduler: TokioScheduler,

    /// (tokio only) How the bytes move. `copy` is the canonical echo loop
    /// (read into a reused buffer, write back out); `splice` moves them
    /// socket -> pipe -> socket without entering user memory, the counterpart
    /// to ringline's recv-forward byte pipe. Linux only.
    #[arg(long, value_enum, default_value_t = TokioEcho::Copy)]
    tokio_echo: TokioEcho,

    /// (ringline, io_uring only) Run as a one-way proxy to this backend
    /// instead of echoing: every accepted connection is forwarded to a fresh
    /// connection to `--proxy-backend`. This is what `forward_to` is for, and
    /// the only shape that exercises it.
    #[arg(long)]
    proxy_backend: Option<SocketAddr>,

    /// Print which ringline backend this binary was built against and exit.
    ///
    /// The backend is a compile-time choice, so an io_uring build and a mio
    /// build are two different binaries that look identical from the outside.
    /// An A/B harness that builds both must verify it got both — a build that
    /// fails and leaves the previous binary in place otherwise measures the
    /// same arm twice and reports a dead heat.
    #[arg(long)]
    print_backend: bool,

    /// (ringline) Write ringline's runtime counters to this path as JSON when
    /// the server shuts down, and print the interesting ones to stderr.
    ///
    /// `buffer_ring_empty`, `recv_parked`, `recv_fallback`,
    /// `forward_throttled` and friends are what separate "this arm was slower"
    /// from "this arm starved its provided ring", which a throughput number
    /// alone cannot say. A sweep over buffer geometry is guesswork without
    /// them.
    #[arg(long)]
    metrics_out: Option<std::path::PathBuf>,

    /// (ringline, `--proxy-backend` only) Which forwarding API the proxy uses.
    /// `conn` (`forward_to_conn`) runs on both backends and is what an
    /// io_uring-vs-mio comparison must use; `sink-fd` (`forward_to` over a
    /// blocking `TcpStream`) is io_uring only.
    #[arg(long, value_enum, default_value_t = ProxyApi::Conn)]
    proxy_api: ProxyApi,

    /// (ringline) Provided recv buffer size in bytes. 0 (default) derives it
    /// from `--msg-size`, which is the right default for echo but ties Mode A's
    /// per-completion payload to the message size — at 256 B messages it gets
    /// 4 KiB buffers against splice's 64 KiB pipe chunk, so a forwarding A/B
    /// that does not set this is partly measuring the harness.
    #[arg(long, default_value_t = 0)]
    recv_buffer_bytes: u32,

    /// (ringline) Number of provided recv buffers. Total pinned memory is this
    /// times `--recv-buffer-bytes`, so sweeping buffer size at a fixed count
    /// also sweeps total memory — this makes the two separable.
    #[arg(long, default_value_t = 256)]
    recv_ring_size: u16,

    /// (ringline only) Connections assigned to each worker before moving to the next.
    /// 1 = classic round-robin. Higher values pack connections onto fewer workers
    /// at low connection counts, keeping per-worker CQE density high for batching.
    #[arg(long, default_value_t = 1)]
    conn_chunk_size: usize,

    /// (ringline) Touch every provided-recv-buffer and send-pool page at
    /// startup so the minor faults are paid before traffic instead of on the
    /// completion path. The knob under measurement in #419.
    #[arg(long, default_value_t = false)]
    prefault_buffers: bool,

    /// Restrict the whole process to these logical CPUs, e.g. `0-7,16-23` or
    /// `12,13,14,15` (the "taskset the task" model). When set, the process
    /// affinity mask is applied before launch and ringline's per-worker core
    /// pinning is disabled (so it doesn't pin to cores outside the mask).
    /// Pass `--workers N` to match the number of physical cores in the list.
    #[arg(long)]
    cpu_list: Option<String>,
}

/// Parse a cpu-list spec (`0-7,16-23` / `12,13,14,15`) into logical CPU ids.
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let lo: usize = lo.trim().parse().expect("invalid cpu-list range start");
            let hi: usize = hi.trim().parse().expect("invalid cpu-list range end");
            cpus.extend(lo..=hi);
        } else {
            cpus.push(part.parse().expect("invalid cpu-list entry"));
        }
    }
    cpus
}

/// Pin the current process to `cpus` via `sched_setaffinity` (taskset-equivalent,
/// in-process). Worker threads spawned afterwards inherit this mask.
#[cfg(target_os = "linux")]
fn apply_cpu_affinity(cpus: &[usize]) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if ret != 0 {
            panic!(
                "sched_setaffinity({cpus:?}) failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

/// Process CPU affinity is not supported on this platform (no-op).
#[cfg(not(target_os = "linux"))]
fn apply_cpu_affinity(_cpus: &[usize]) {
    eprintln!("bench-server: --cpu-list ignored (CPU affinity unsupported on this platform)");
}

/// Print ringline's runtime counters, and write them to `path` if one was
/// given. Called after the shutdown signal so the numbers cover the whole run.
fn dump_runtime_metrics(path: Option<&std::path::Path>) {
    ringline_bench::runtime_metrics::print_summary();
    if let Some(path) = path {
        match ringline_bench::runtime_metrics::dump_to(path) {
            Ok(()) => eprintln!("bench-server: wrote runtime metrics to {}", path.display()),
            // Loud: an arm whose counters did not land cannot be explained
            // later, and a silently missing file looks like a healthy run.
            Err(e) => eprintln!(
                "bench-server: FAILED to write runtime metrics to {}: {e}",
                path.display()
            ),
        }
    }
}

/// The backend this binary was compiled against, as a word.
const RINGLINE_BACKEND: &str = if cfg!(has_io_uring) {
    "io_uring"
} else {
    "mio"
};

fn main() {
    let args = Args::parse();

    if args.print_backend {
        println!("{RINGLINE_BACKEND}");
        return;
    }

    // Apply process CPU affinity before launch so worker threads inherit it.
    // Disables ringline's own per-worker pinning (see run_ringline) to avoid
    // pinning workers to cores outside the requested mask.
    let pin_to_core = match &args.cpu_list {
        Some(spec) => {
            let cpus = parse_cpu_list(spec);
            assert!(!cpus.is_empty(), "--cpu-list parsed to an empty set");
            apply_cpu_affinity(&cpus);
            eprintln!("bench-server: pinned process to CPUs {cpus:?}");
            false
        }
        None => true,
    };

    let workers = if args.workers == 0 {
        ringline::physical_core_count()
    } else {
        args.workers
    };

    // `--print-backend` is the only path that may omit it, and that returned
    // above.
    let runtime = args.runtime.expect("--runtime is required");
    let runtime_name = match runtime {
        Runtime::Ringline => "ringline",
        Runtime::Tokio => "tokio",
        Runtime::TokioUring => "tokio-uring",
    };

    eprintln!(
        "bench-server: {} runtime, {} workers, listening on {}",
        runtime_name, workers, args.addr,
    );

    match runtime {
        Runtime::Ringline if args.proxy_backend.is_some() => run_ringline_proxy(ProxyCfg {
            addr: args.addr,
            workers,
            msg_size: args.msg_size,
            backend: args.proxy_backend.expect("checked"),
            recv_buffer_bytes: args.recv_buffer_bytes,
            recv_ring_size: args.recv_ring_size,
            conn_chunk_size: args.conn_chunk_size,
            pin_to_core,
            prefault_buffers: args.prefault_buffers,
            api: args.proxy_api,
            metrics_out: args.metrics_out.clone(),
        }),
        Runtime::Ringline => run_ringline(EchoCfg {
            addr: args.addr,
            workers,
            msg_size: args.msg_size,
            metrics_out: args.metrics_out.clone(),
            recv_buffer_bytes: args.recv_buffer_bytes,
            recv_ring_size: args.recv_ring_size,
            prefault_buffers: args.prefault_buffers,
            echo_mode: if args.recv_forward {
                EchoMode::RecvForward
            } else {
                args.echo_mode
            },
            conn_chunk_size: args.conn_chunk_size,
            pin_to_core,
            accept_mode: args.accept_mode,
            tls: args.tls,
        }),
        Runtime::Tokio => {
            use ringline_bench::servers::tokio_arms;
            tokio_arms::run(
                args.addr,
                workers,
                args.msg_size,
                match args.tokio_scheduler {
                    TokioScheduler::MultiThread => tokio_arms::TokioScheduler::MultiThread,
                    TokioScheduler::PerCore => tokio_arms::TokioScheduler::PerCore,
                },
                match args.tokio_echo {
                    TokioEcho::Copy => tokio_arms::TokioEcho::Copy,
                    TokioEcho::Splice => tokio_arms::TokioEcho::Splice,
                },
                pin_to_core,
            )
        }
        Runtime::TokioUring => run_tokio_uring(args.addr, workers, args.msg_size, pin_to_core),
    }
}

#[cfg(all(target_os = "linux", feature = "tokio-uring-arm"))]
fn run_tokio_uring(addr: SocketAddr, workers: usize, msg_size: usize, pin_to_core: bool) {
    ringline_bench::servers::tokio_uring_arm::run(addr, workers, msg_size, pin_to_core)
}

#[cfg(not(all(target_os = "linux", feature = "tokio-uring-arm")))]
fn run_tokio_uring(_addr: SocketAddr, _workers: usize, _msg_size: usize, _pin_to_core: bool) {
    eprintln!(
        "bench-server: --runtime tokio-uring needs a Linux build with \
         --features tokio-uring-arm"
    );
    std::process::exit(2);
}

/// One-way proxy: forward every accepted connection's stream to a fresh
/// connection to `backend`, using whichever forwarding API `--proxy-api` names.
///
/// `--proxy-api conn` (`forward_to_conn`) exists on both backends, so this is
/// the arm an io_uring-vs-mio comparison runs. `sink-fd` (`forward_to` over a
/// blocking `TcpStream`) is io_uring only and is rejected on mio rather than
/// silently measured as something else.
#[allow(clippy::manual_async_fn)]
fn run_ringline_proxy(cfg: ProxyCfg) {
    let ProxyCfg {
        addr,
        workers,
        msg_size,
        backend,
        recv_buffer_bytes,
        recv_ring_size,
        conn_chunk_size,
        pin_to_core,
        prefault_buffers,
        api,
        metrics_out,
    } = cfg;
    use ringline::{AsyncEventHandler, Connection, RinglineBuilder};

    /// Forward for the life of the connection: the caller asks for a byte
    /// count, and a proxy does not know one, so ask for more than any run will
    /// carry and let the peer's FIN end it (both APIs resolve short on FIN).
    const UNTIL_EOF: usize = usize::MAX / 2;

    static BACKEND: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
    let _ = BACKEND.set(backend);

    /// `forward_to` with a borrowed descriptor. The backend connection is a
    /// blocking `std::net::TcpStream` because a ringline outbound connection's
    /// fd is deliberately not public — which is the whole reason
    /// `forward_to_conn` exists.
    #[cfg(has_io_uring)]
    struct SinkFdProxy;
    #[cfg(has_io_uring)]
    impl AsyncEventHandler for SinkFdProxy {
        fn on_accept(
            &self,
            mut conn: Connection,
        ) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                use std::os::fd::AsFd;
                let addr = *BACKEND.get().expect("backend set before launch");
                let Ok(sink_sock) = std::net::TcpStream::connect(addr) else {
                    eprintln!("proxy: backend connect failed");
                    return;
                };
                sink_sock.set_nodelay(true).ok();
                let sink = ringline::SinkFd::socket(sink_sock.as_fd());
                if let Err(e) = conn.forward_to(&sink, UNTIL_EOF).await {
                    eprintln!("proxy: forward failed: {e}");
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            SinkFdProxy
        }
    }

    /// `forward_to_conn` with a ringline connection as the sink — the form
    /// both backends have. The connect is the runtime's, so it costs one
    /// async round trip instead of a blocking syscall on the worker.
    struct ConnProxy;
    impl AsyncEventHandler for ConnProxy {
        fn on_accept(
            &self,
            mut conn: Connection,
        ) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let addr = *BACKEND.get().expect("backend set before launch");
                let sink = match conn.connect(addr) {
                    Ok(fut) => match fut.await {
                        Ok(ctx) => ctx,
                        Err(e) => {
                            eprintln!("proxy: backend connect failed: {e}");
                            return;
                        }
                    },
                    Err(e) => {
                        eprintln!("proxy: backend connect failed: {e}");
                        return;
                    }
                };
                // The sink is presented as its write half: holding it is the
                // permission to write, and the borrow keeps anything else from
                // sending to that socket while the forward runs.
                let mut sink_tx = match sink.take_send() {
                    Ok(tx) => tx,
                    Err(e) => {
                        eprintln!("proxy: backend take_send failed: {e}");
                        return;
                    }
                };
                if let Err(e) = conn.forward_to_conn(&mut sink_tx, UNTIL_EOF).await {
                    eprintln!("proxy: forward failed: {e}");
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            ConnProxy
        }
    }

    let recv_buf = if recv_buffer_bytes > 0 {
        recv_buffer_bytes
    } else {
        msg_size.next_power_of_two().max(4096) as u32
    };
    let config = ConfigBuilder::new()
        .workers(workers)
        .pin_to_core(pin_to_core)
        .sq_entries(256)
        .recv_buffer(recv_ring_size, recv_buf)
        .prefault_buffers(prefault_buffers)
        .max_connections(16384)
        .send_pool(512, msg_size.next_power_of_two().max(4096) as u32)
        .conn_chunk_size(conn_chunk_size)
        .build()
        .expect("valid config");

    let builder = RinglineBuilder::new(config).bind(addr);
    let (shutdown, handles) = match api {
        ProxyApi::Conn => builder.launch::<ConnProxy>(),
        #[cfg(has_io_uring)]
        ProxyApi::SinkFd => builder.launch::<SinkFdProxy>(),
        #[cfg(not(has_io_uring))]
        ProxyApi::SinkFd => {
            eprintln!("bench-server: --proxy-api sink-fd needs an io_uring build; use `conn`");
            std::process::exit(2);
        }
    }
    .expect("failed to launch ringline proxy");
    let api_name = match api {
        ProxyApi::Conn => "forward_to_conn",
        ProxyApi::SinkFd => "forward_to",
    };
    eprintln!(
        "bench-server: ready (backend={RINGLINE_BACKEND}, proxy -> {backend}, api={api_name}, recv_buffer={recv_ring_size}x{recv_buf} = {} MiB)",
        (recv_ring_size as u64 * recv_buf as u64) / (1024 * 1024)
    );
    shutdown.wait_on_signal();
    dump_runtime_metrics(metrics_out.as_deref());
    for h in handles {
        h.join().ok();
    }
}

#[allow(clippy::manual_async_fn)]
fn run_ringline(cfg: EchoCfg) {
    let EchoCfg {
        addr,
        workers,
        msg_size,
        metrics_out,
        recv_buffer_bytes,
        recv_ring_size,
        echo_mode,
        conn_chunk_size,
        pin_to_core,
        prefault_buffers,
        accept_mode,
        tls,
    } = cfg;
    use ringline::ParseResult;
    use ringline::{AsyncEventHandler, RinglineBuilder};

    // Direct-echo path (default): no task wakeup per message — echo SQEs are
    // submitted directly from handle_recv_multi, bypassing collect_wakeups and
    // poll_ready_tasks entirely. Falls back to the forward_recv_buf loop on the
    // mio backend (macOS / non-io_uring builds).
    struct EchoHandler;
    impl AsyncEventHandler for EchoHandler {
        fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                #[cfg(has_io_uring)]
                {
                    // No `return` needed: the fallback below is cfg'd out
                    // whenever this arm is compiled in. (Never linted before
                    // #402, because this block was dead on every platform.)
                    conn.as_conn().run_direct_echo().await;
                }
                #[cfg(not(has_io_uring))]
                forward_echo_loop(conn).await;
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            EchoHandler
        }
    }

    /// `with_data` + `forward_recv_buf` — what a protocol server's read loop
    /// looks like, and the only mode available on the mio backend.
    async fn forward_echo_loop(conn: Connection) {
        // The send happens inside the recv closure, so the two halves have to
        // be separately borrowable.
        let (mut tx, mut rx) = conn.split();
        loop {
            let n = rx
                .with_data(|data| {
                    if let Err(e) = tx.forward_recv_buf(data) {
                        eprintln!("echo: forward_recv_buf failed: {e}");
                        return ParseResult::NeedMore;
                    }
                    ParseResult::Consumed(data.len())
                })
                .await;
            if n == 0 {
                break;
            }
        }
    }

    struct ForwardEchoHandler;
    impl AsyncEventHandler for ForwardEchoHandler {
        fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
            async move { forward_echo_loop(conn).await }
        }
        fn create_for_worker(_id: usize) -> Self {
            ForwardEchoHandler
        }
    }

    // Multi-buffer zero-copy recv-forward path: hold provided recv buffers and
    // scatter-gather them back in one sendmsg — no accumulator copy at all.
    struct RecvForwardEchoHandler;
    impl AsyncEventHandler for RecvForwardEchoHandler {
        fn on_accept(
            &self,
            mut conn: Connection,
        ) -> impl std::future::Future<Output = ()> + 'static {
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
            RecvForwardEchoHandler
        }
    }

    let recv_buf = if recv_buffer_bytes > 0 {
        recv_buffer_bytes
    } else {
        msg_size.next_power_of_two().max(4096) as u32
    };
    let staged = ConfigBuilder::new()
        .workers(workers)
        // When --cpu-list set a process affinity mask, leave the OS to schedule
        // workers within it; otherwise pin each worker to its own core (0..N).
        .pin_to_core(pin_to_core)
        .sq_entries(256)
        // Honour the geometry flags, exactly as the proxy arm does. These
        // used to be proxy-only and silently ignored here, so a
        // buffer-geometry sweep over the echo path would have run every arm at
        // the same derived size and could only ever have reported "no effect".
        .recv_buffer(recv_ring_size, recv_buf)
        .prefault_buffers(prefault_buffers)
        .max_connections(16384)
        .send_pool(512, msg_size.next_power_of_two().max(4096) as u32)
        .conn_chunk_size(conn_chunk_size)
        .accept_mode(accept_mode.into());
    let staged = if tls {
        staged.tls(ringline::TlsConfig::new(self_signed_server_config()))
    } else {
        staged
    };
    let config = staged.build().expect("valid config");

    let builder = RinglineBuilder::new(config).bind(addr);
    let (shutdown, handles) = match echo_mode {
        EchoMode::RecvForward => builder.launch::<RecvForwardEchoHandler>(),
        EchoMode::Forward => builder.launch::<ForwardEchoHandler>(),
        EchoMode::Direct => builder.launch::<EchoHandler>(),
    }
    .expect("failed to launch ringline server");

    let mode = match echo_mode {
        EchoMode::Direct => "direct",
        EchoMode::Forward => "forward",
        EchoMode::RecvForward => "recv-forward",
    };
    // Say which path is live: on a non-io_uring build `direct` silently means
    // the forward loop, and a run that reported the wrong path is how #397
    // ended up with the wrong root cause.
    let effective = if cfg!(has_io_uring) {
        mode
    } else {
        "forward (no io_uring)"
    };
    eprintln!(
        "bench-server: ready (backend={RINGLINE_BACKEND}, echo_mode={mode}, effective={effective}, recv_buffer={recv_ring_size}x{recv_buf} = {} MiB/worker)",
        (recv_ring_size as u64 * recv_buf as u64) / (1024 * 1024)
    );

    // Block until SIGINT/SIGTERM, then trigger graceful shutdown so each
    // worker's event loop runs its shutdown path — including the
    // `[ringline diag]`/`[ringline stall]` counter dump. (A SIGKILL at
    // teardown skips that, hiding the server-side loop diagnostics.)
    shutdown.wait_on_signal();
    dump_runtime_metrics(metrics_out.as_deref());

    for h in handles {
        h.join().ok();
    }
}
