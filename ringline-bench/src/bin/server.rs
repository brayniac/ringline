//! Standalone echo server for distributed benchmarking.
//!
//! Usage:
//!   bench-server --runtime ringline --addr 0.0.0.0:7878 --workers 4 --msg-size 64
//!   bench-server --runtime tokio --addr 0.0.0.0:7878 --workers 4

use ringline::ConfigBuilder;
use std::net::SocketAddr;

use clap::Parser;

/// Everything the proxy mode needs, bundled because the argument list had
/// outgrown what clippy will accept and most of it travels together anyway.
// Every field is read by the io_uring definition of `run_ringline_proxy`,
// which is the only one that exists on Linux. The allow is scoped to builds
// where that definition is compiled out, so it cannot mask an unused field on
// the platform the proxy actually runs on.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
#[derive(Clone, Copy)]
struct ProxyCfg {
    addr: SocketAddr,
    workers: usize,
    msg_size: usize,
    backend: SocketAddr,
    mode: ForwardMode,
    recv_buffer_bytes: u32,
    conn_chunk_size: usize,
    pin_to_core: bool,
}

/// Which forwarding API the proxy mode drives. The two differ only in the
/// call, which is the point: it isolates the forward path from everything else
/// in the measurement.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum ForwardMode {
    /// `forward_to` — Mode A, provided buffers held and written to the sink.
    ModeA,
    /// `forward_to_splice` — socket -> pipe -> sink, no provided-buffer ring.
    Splice,
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
    #[arg(long)]
    runtime: Runtime,

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

    /// (ringline, io_uring only) Which forwarding API `--proxy-backend` uses.
    #[arg(long, value_enum, default_value_t = ForwardMode::ModeA)]
    forward_mode: ForwardMode,

    /// (ringline) Provided recv buffer size in bytes. 0 (default) derives it
    /// from `--msg-size`, which is the right default for echo but ties Mode A's
    /// per-completion payload to the message size — at 256 B messages it gets
    /// 4 KiB buffers against splice's 64 KiB pipe chunk, so a forwarding A/B
    /// that does not set this is partly measuring the harness.
    #[arg(long, default_value_t = 0)]
    recv_buffer_bytes: u32,

    /// (ringline only) Connections assigned to each worker before moving to the next.
    /// 1 = classic round-robin. Higher values pack connections onto fewer workers
    /// at low connection counts, keeping per-worker CQE density high for batching.
    #[arg(long, default_value_t = 1)]
    conn_chunk_size: usize,

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

fn main() {
    let args = Args::parse();

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

    let runtime_name = match args.runtime {
        Runtime::Ringline => "ringline",
        Runtime::Tokio => "tokio",
        Runtime::TokioUring => "tokio-uring",
    };

    eprintln!(
        "bench-server: {} runtime, {} workers, listening on {}",
        runtime_name, workers, args.addr,
    );

    match args.runtime {
        Runtime::Ringline if args.proxy_backend.is_some() => run_ringline_proxy(ProxyCfg {
            addr: args.addr,
            workers,
            msg_size: args.msg_size,
            backend: args.proxy_backend.expect("checked"),
            mode: args.forward_mode,
            recv_buffer_bytes: args.recv_buffer_bytes,
            conn_chunk_size: args.conn_chunk_size,
            pin_to_core,
        }),
        Runtime::Ringline => run_ringline(
            args.addr,
            workers,
            args.msg_size,
            if args.recv_forward {
                EchoMode::RecvForward
            } else {
                args.echo_mode
            },
            args.conn_chunk_size,
            pin_to_core,
        ),
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
/// connection to `backend`, using whichever forwarding API `mode` names.
///
/// io_uring only, because `forward_to` and `forward_to_splice` are (#410).
#[cfg(has_io_uring)]
#[allow(clippy::manual_async_fn)]
fn run_ringline_proxy(cfg: ProxyCfg) {
    let ProxyCfg {
        addr,
        workers,
        msg_size,
        backend,
        mode,
        recv_buffer_bytes,
        conn_chunk_size,
        pin_to_core,
    } = cfg;
    use ringline::{AsyncEventHandler, ConnCtx, RinglineBuilder, SinkFd};
    use std::os::fd::AsFd;

    /// Forward for the life of the connection: the caller asks for a byte
    /// count, and a proxy does not know one, so ask for more than any run will
    /// carry and let the peer's FIN end it (both APIs resolve short on FIN).
    const UNTIL_EOF: usize = usize::MAX / 2;

    static BACKEND: std::sync::OnceLock<SocketAddr> = std::sync::OnceLock::new();
    static MODE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
    let _ = BACKEND.set(backend);
    MODE.store(
        match mode {
            ForwardMode::ModeA => 0,
            ForwardMode::Splice => 1,
        },
        std::sync::atomic::Ordering::SeqCst,
    );

    struct ProxyHandler;
    impl AsyncEventHandler for ProxyHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let addr = *BACKEND.get().expect("backend set before launch");
                // Blocking connect, once per connection, before any traffic —
                // the alternative is an outbound ringline connection whose fd
                // cannot be named as a sink (#410).
                let Ok(sink_sock) = std::net::TcpStream::connect(addr) else {
                    eprintln!("proxy: backend connect failed");
                    return;
                };
                sink_sock.set_nodelay(true).ok();
                let sink = SinkFd::socket(sink_sock.as_fd());
                let splice = MODE.load(std::sync::atomic::Ordering::SeqCst) == 1;
                let res = if splice {
                    conn.forward_to_splice(&sink, UNTIL_EOF).await
                } else {
                    conn.forward_to(&sink, UNTIL_EOF).await
                };
                if let Err(e) = res {
                    eprintln!("proxy: forward failed: {e}");
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            ProxyHandler
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
        .recv_buffer(256, recv_buf)
        .max_connections(16384)
        .send_pool(512, msg_size.next_power_of_two().max(4096) as u32)
        .conn_chunk_size(conn_chunk_size)
        .build()
        .expect("valid config");

    let (shutdown, handles) = RinglineBuilder::new(config)
        .bind(addr)
        .launch::<ProxyHandler>()
        .expect("failed to launch ringline proxy");
    eprintln!(
        "bench-server: ready (proxy -> {backend}, forward_mode={}, recv_buffer={recv_buf})",
        if mode == ForwardMode::Splice {
            "splice"
        } else {
            "mode-a"
        }
    );
    shutdown.wait_on_signal();
    for h in handles {
        h.join().ok();
    }
}

#[cfg(not(has_io_uring))]
fn run_ringline_proxy(cfg: ProxyCfg) {
    // Naming the fields keeps them read on this platform too: the io_uring
    // definition is compiled out here, and an unused-field lint would
    // otherwise fire on a struct that is fully used where it matters.
    eprintln!(
        "bench-server: would proxy {} -> {} ({} workers)",
        cfg.addr, cfg.backend, cfg.workers
    );
    eprintln!("bench-server: --proxy-backend needs an io_uring build (see #410)");
    std::process::exit(2);
}

#[allow(clippy::manual_async_fn)]
fn run_ringline(
    addr: SocketAddr,
    workers: usize,
    msg_size: usize,
    echo_mode: EchoMode,
    conn_chunk_size: usize,
    pin_to_core: bool,
) {
    use ringline::ParseResult;
    use ringline::{AsyncEventHandler, ConnCtx, RinglineBuilder};

    // Direct-echo path (default): no task wakeup per message — echo SQEs are
    // submitted directly from handle_recv_multi, bypassing collect_wakeups and
    // poll_ready_tasks entirely. Falls back to the forward_recv_buf loop on the
    // mio backend (macOS / non-io_uring builds).
    struct EchoHandler;
    impl AsyncEventHandler for EchoHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                #[cfg(has_io_uring)]
                {
                    // No `return` needed: the fallback below is cfg'd out
                    // whenever this arm is compiled in. (Never linted before
                    // #402, because this block was dead on every platform.)
                    conn.run_direct_echo().await;
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
    async fn forward_echo_loop(conn: ConnCtx) {
        loop {
            let n = conn
                .with_data(|data| {
                    if let Err(e) = conn.forward_recv_buf(data) {
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
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
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
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
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

    let config = ConfigBuilder::new()
        .workers(workers)
        // When --cpu-list set a process affinity mask, leave the OS to schedule
        // workers within it; otherwise pin each worker to its own core (0..N).
        .pin_to_core(pin_to_core)
        .sq_entries(256)
        .recv_buffer(256, msg_size.next_power_of_two().max(4096) as u32)
        .max_connections(16384)
        .send_pool(512, msg_size.next_power_of_two().max(4096) as u32)
        .conn_chunk_size(conn_chunk_size)
        .build()
        .expect("valid config");

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
    eprintln!("bench-server: ready (echo_mode={mode}, effective={effective})");

    // Block until SIGINT/SIGTERM, then trigger graceful shutdown so each
    // worker's event loop runs its shutdown path — including the
    // `[ringline diag]`/`[ringline stall]` counter dump. (A SIGKILL at
    // teardown skips that, hiding the server-side loop diagnostics.)
    shutdown.wait_on_signal();

    for h in handles {
        h.join().ok();
    }
}
