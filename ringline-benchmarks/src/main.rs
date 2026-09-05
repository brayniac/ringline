use clap::Parser;
use std::time::Duration;

use ringline_benchmarks::bench::{BenchmarkDefinition, ClientRuntime, ServerRuntime};
use ringline_benchmarks::output::{
    BenchReport, ConfigResult, git_commit, print_tls_table, timestamp, write_json,
};
use ringline_benchmarks::port_manager::PortManager;
use ringline_benchmarks::protocols::http1;
use ringline_benchmarks::protocols::http2;
use ringline_benchmarks::protocols::http3;
use ringline_benchmarks::protocols::memcache;
use ringline_benchmarks::protocols::quic;
use ringline_benchmarks::protocols::redis;
use ringline_benchmarks::protocols::tcp;
use ringline_benchmarks::protocols::tls;
use ringline_benchmarks::protocols::udp;
use ringline_benchmarks::stats::format_ns;
use std::sync::Arc;

#[derive(clap::Parser)]
#[command(
    name = "ringline-benchmarks",
    about = "Comprehensive performance benchmarking for ringline"
)]
struct Args {
    /// Test duration per configuration (seconds)
    #[arg(long, default_value_t = 5)]
    duration: u64,

    /// Warmup duration (seconds)
    #[arg(long, default_value_t = 2)]
    warmup: u64,

    /// Number of server worker threads (0 = available parallelism)
    #[arg(long, default_value_t = 0)]
    workers: usize,

    /// Comma-separated client counts
    #[arg(long, default_value = "1,10,50,200", value_delimiter = ',')]
    clients: Vec<usize>,

    /// Comma-separated message sizes in bytes
    #[arg(long, default_value = "64,512,4096,32768", value_delimiter = ',')]
    sizes: Vec<usize>,

    /// Write JSON results to file
    #[arg(long)]
    json: Option<String>,

    /// Run a single quick config (4 clients, 64B)
    #[arg(long)]
    quick: bool,

    /// Skip ringline server (e.g. on non-Linux)
    #[arg(long)]
    tokio_only: bool,

    /// Skip tokio server
    #[arg(long)]
    ringline_only: bool,

    /// Run only specific benchmark categories (comma-separated: tcp,udp,quic,http1,http2,http3,redis,memcache,all)
    #[arg(long)]
    only: Option<String>,

    /// Run *only* the TLS echo benchmark: a ringline TLS echo server in a
    /// child process, driven by a tokio + rustls client, reporting the
    /// server's own CPU time per operation. This is the A/B harness for
    /// ringline's `tls-unbuffered` feature — run it twice, once with
    /// `--features tls-unbuffered` and once without, and diff `cpu_ns_per_op`.
    /// A plaintext control cell runs alongside each TLS cell.
    #[arg(long)]
    tls: bool,

    /// Tokio worker threads for the TLS bench's client side. The client is
    /// co-located with the server, so this trades offered load against
    /// contention. 0 = half of available parallelism (min 2).
    #[arg(long, default_value_t = 0)]
    tls_client_threads: usize,

    /// Aggregate target rate for the TLS bench, in ops/sec across all clients.
    /// 0 (default) is closed-loop: each client sends the next request as soon
    /// as the previous response lands, so a faster server is offered more
    /// load. A non-zero rate paces the clients so both arms do the same work,
    /// which is the cleaner comparison when the server has headroom.
    #[arg(long, default_value_t = 0)]
    tls_rate: u64,

    /// Internal: run as the TLS bench's server child process. Not for direct
    /// use; the parent spawns itself with this flag.
    #[arg(long, hide = true)]
    tls_server_child: bool,
}

fn main() {
    let mut args = Args::parse();

    // Child-server mode: never returns. Must be handled before anything else
    // so the child does not print the banner the parent parses around.
    if args.tls_server_child {
        tls::run_server_child(tls::ChildArgs {
            tls: args.tls,
            msg_size: args.sizes.first().copied().unwrap_or(64),
            clients: args.clients.first().copied().unwrap_or(1),
        });
    }

    if args.quick {
        args.clients = vec![4];
        args.sizes = vec![64];
    }

    let workers = if args.workers == 0 {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    } else {
        args.workers
    };

    let duration = Duration::from_secs(args.duration);
    let warmup = Duration::from_secs(args.warmup);

    eprintln!(
        "Benchmarks: worker counts {:?}, {}s warmup + {}s per config",
        if args.workers == 0 {
            vec![1]
        } else {
            vec![workers]
        },
        args.warmup,
        args.duration,
    );
    eprintln!("  clients: {:?}", args.clients);
    eprintln!("  sizes:   {:?}", args.sizes);
    eprintln!();

    // ── TLS echo benchmark (exclusive mode) ───────────────────────
    //
    // This is the A/B harness for ringline's `tls-unbuffered` feature. It runs
    // alone rather than as one more section of the matrix because it needs a
    // quiet box: its metric is the *server's* CPU time, and a co-resident
    // benchmark would land in the same accounting for the process only by
    // accident of scheduling, but would certainly steal cores.
    if args.tls {
        // The TLS axis is driven through `BenchmarkDefinition` so
        // `TlsConfig::Required` has a real consumer: `.with_tls()` makes
        // `combinations()` emit both a plaintext control and a TLS cell for
        // every (size, concurrency), and `protocols::tls` selects the server
        // and client for each.
        //
        // The client runtime is pinned to tokio and cannot be overridden. A
        // ringline client would link the very engine under test, so an engine
        // change would move both ends of the measurement at once.
        let definition = BenchmarkDefinition::new()
            .with_sizes(args.sizes.clone())
            .with_concurrencies(args.clients.clone())
            .with_ringline_server()
            .with_tokio_client()
            .with_tls()
            .with_timing(warmup, duration);

        let client_threads = if args.tls_client_threads == 0 {
            (workers / 2).max(2)
        } else {
            args.tls_client_threads
        };

        eprintln!("=== TLS Echo Benchmark ===");
        eprintln!("  engine:         {}", tls::engine_name());
        eprintln!("  client:         tokio + rustls ({client_threads} threads)");
        eprintln!("  server:         ringline, child process, 1 worker");
        eprintln!(
            "  offered load:   {}",
            if args.tls_rate == 0 {
                "closed loop".to_string()
            } else {
                format!("{} ops/s (paced)", args.tls_rate)
            }
        );
        eprintln!();

        let mut tls_results = Vec::new();
        for combo in definition.combinations() {
            match tls::run_tls_echo(
                combo.tls,
                combo.concurrency,
                combo.size,
                definition.warmup,
                definition.duration,
                client_threads,
                args.tls_rate,
            ) {
                Ok(result) => {
                    eprintln!(
                        "  {:>5} {:>4}c x {:>7}: {:>9.0} ops/s  cpu {:>8.1} ns/op  {:>7.4} ns/B  p50 {}  p99 {}",
                        result.tls,
                        result.clients,
                        format_size(result.msg_size),
                        result.client.ops_per_sec,
                        result.cpu_ns_per_op,
                        result.cpu_ns_per_byte,
                        format_ns(result.client.latency.p50_ns),
                        format_ns(result.client.latency.p99_ns),
                    );
                    if let Some(w) = &result.warning {
                        eprintln!("        !! {w}");
                    }
                    tls_results.push(result);
                }
                Err(e) => eprintln!("  TLS cell failed ({}, {}B): {e}", combo.tls, combo.size),
            }
        }

        print_tls_table(&tls_results);

        if let Some(ref path) = args.json {
            let report = BenchReport {
                timestamp: timestamp(),
                git_commit: git_commit(),
                tls_engine: tls::engine_name().to_string(),
                configs: Vec::new(),
                tls_echo: tls_results,
            };
            write_json(path, &report);
        }
        return;
    }

    // Determine which benchmarks to run
    let (do_tcp, do_udp, do_quic, do_http1, do_http2, do_http3, do_redis, do_memcache, do_all) =
        match &args.only {
            None => (true, true, true, true, true, true, true, true, true),
            Some(only) => {
                let parts: Vec<&str> = only.split(',').collect();
                (
                    parts.contains(&"tcp"),
                    parts.contains(&"udp"),
                    parts.contains(&"quic"),
                    parts.contains(&"http1"),
                    parts.contains(&"http2"),
                    parts.contains(&"http3"),
                    parts.contains(&"redis"),
                    parts.contains(&"memcache"),
                    parts.contains(&"all"),
                )
            }
        };

    let port_manager = Arc::new(PortManager::new(19400));
    let mut all_results: Vec<ConfigResult> = Vec::new();

    // ── TCP echo benchmarks ───────────────────────────────────────
    if do_tcp || do_all {
        eprintln!("=== TCP Echo Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime, &str, ServerRuntime)] = &[
                    (
                        "ringline",
                        ClientRuntime::Ringline,
                        "ringline",
                        ServerRuntime::Ringline,
                    ),
                    (
                        "ringline",
                        ClientRuntime::Ringline,
                        "tokio",
                        ServerRuntime::Tokio,
                    ),
                    (
                        "tokio",
                        ClientRuntime::Tokio,
                        "ringline",
                        ServerRuntime::Ringline,
                    ),
                    ("tokio", ClientRuntime::Tokio, "tokio", ServerRuntime::Tokio),
                ];

                for &(client_name, client_rt, server_name, server_rt) in combos {
                    if args.tokio_only && server_rt == ServerRuntime::Ringline {
                        continue;
                    }
                    if args.ringline_only && server_rt == ServerRuntime::Tokio {
                        continue;
                    }

                    let (result, _) = tcp::run_tcp_echo(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        server_rt,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        server_name,
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (tokio_ringline, tokio_tokio, ringline_ringline, ringline_tokio) =
                        match (client_name, server_name) {
                            ("ringline", "ringline") => (None, None, Some(result), None),
                            ("ringline", "tokio") => (None, None, None, Some(result)),
                            ("tokio", "ringline") => (Some(result), None, None, None),
                            _ => (None, Some(result), None, None),
                        };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: server_name.to_string(),
                        transport: "tcp".to_string(),
                        protocol: "echo".to_string(),
                        tls: "none".to_string(),
                        tokio_ringline,
                        tokio_tokio,
                        ringline_ringline,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── UDP echo benchmarks ───────────────────────────────────────
    if do_udp || do_all {
        eprintln!("\n=== UDP Echo Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime, &str, ServerRuntime)] = &[
                    (
                        "ringline",
                        ClientRuntime::Ringline,
                        "ringline",
                        ServerRuntime::Ringline,
                    ),
                    (
                        "ringline",
                        ClientRuntime::Ringline,
                        "tokio",
                        ServerRuntime::Tokio,
                    ),
                    (
                        "tokio",
                        ClientRuntime::Tokio,
                        "ringline",
                        ServerRuntime::Ringline,
                    ),
                    ("tokio", ClientRuntime::Tokio, "tokio", ServerRuntime::Tokio),
                ];

                for &(client_name, client_rt, server_name, server_rt) in combos {
                    if args.tokio_only && server_rt == ServerRuntime::Ringline {
                        continue;
                    }
                    if args.ringline_only && server_rt == ServerRuntime::Tokio {
                        continue;
                    }

                    let (result, _) = udp::run_udp_echo(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        server_rt,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        server_name,
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (tokio_ringline, tokio_tokio, ringline_ringline, ringline_tokio) =
                        match (client_name, server_name) {
                            ("ringline", "ringline") => (None, None, Some(result), None),
                            ("ringline", "tokio") => (None, None, None, Some(result)),
                            ("tokio", "ringline") => (Some(result), None, None, None),
                            _ => (None, Some(result), None, None),
                        };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: server_name.to_string(),
                        transport: "udp".to_string(),
                        protocol: "echo".to_string(),
                        tls: "none".to_string(),
                        tokio_ringline,
                        tokio_tokio,
                        ringline_ringline,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── QUIC echo benchmarks ──────────────────────────────────────
    //
    // Same shape as the HTTP/2 bench. The server is a ringline QUIC
    // echo built on `ringline_quic::QuicEndpoint`; the bench varies
    // the client runtime. ringline-quic is sans-IO so the ringline
    // client is a single async task that drives the QUIC state
    // machine and keeps `num_clients` bidirectional streams in flight
    // at a time. The tokio reference uses quinn.
    if do_quic || do_all {
        eprintln!("\n=== QUIC Echo Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = quic::run_quic(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "ringline",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_ringline, tokio_ringline) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "ringline".to_string(),
                        transport: "quic".to_string(),
                        protocol: "echo".to_string(),
                        tls: "rustls".to_string(),
                        tokio_ringline,
                        tokio_tokio: None,
                        ringline_ringline,
                        ringline_tokio: None,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── HTTP/1.1 benchmarks ───────────────────────────────────────
    //
    // Same shape as Redis / Memcache: a single tokio server is the
    // target; we drive it with both a `ringline-http` HTTP/1.1
    // client and a hand-rolled keep-alive tokio TCP client so the
    // per-cell row pair shows which client runtime wins on the same
    // wire format.
    if do_http1 || do_all {
        eprintln!("\n=== HTTP/1.1 Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = http1::run_http1(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "tokio",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_tokio, tokio_tokio) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "tokio".to_string(),
                        transport: "http1".to_string(),
                        protocol: "get".to_string(),
                        tls: "none".to_string(),
                        tokio_ringline: None,
                        tokio_tokio,
                        ringline_ringline: None,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── HTTP/2 benchmarks ─────────────────────────────────────────
    //
    // Same shape as HTTP/1.1, but the bench server is hyper-over-TLS
    // (HTTP/2 requires TLS as far as ringline-http is concerned —
    // there is no h2c path). Self-signed cert at startup, both
    // clients trust it explicitly. Reqwest is the reference; same
    // builder + structured response as ringline-http.
    if do_http2 || do_all {
        eprintln!("\n=== HTTP/2 Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = http2::run_http2(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "tokio",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_tokio, tokio_tokio) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "tokio".to_string(),
                        transport: "http2".to_string(),
                        protocol: "get".to_string(),
                        tls: "rustls".to_string(),
                        tokio_ringline: None,
                        tokio_tokio,
                        ringline_ringline: None,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── HTTP/3 benchmarks ─────────────────────────────────────────
    if do_http3 || do_all {
        eprintln!("\n=== HTTP/3 Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = http3::run_http3(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "ringline",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_ringline, tokio_ringline) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "ringline".to_string(),
                        transport: "http3".to_string(),
                        protocol: "echo".to_string(),
                        tls: "rustls".to_string(),
                        tokio_ringline,
                        tokio_tokio: None,
                        ringline_ringline,
                        ringline_tokio: None,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── Redis benchmarks ───────────────────────────────────────────
    //
    // The redis bench has a single server implementation (a tokio
    // RESP responder in `redis::run_redis`), so the meaningful axis
    // is *client* runtime: ringline-redis::Client vs a hand-rolled
    // tokio RESP client. Both go over the same wire format against
    // the same server.
    if do_redis || do_all {
        eprintln!("\n=== Redis Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = redis::run_redis(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "tokio",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_tokio, tokio_tokio) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "tokio".to_string(),
                        transport: "redis".to_string(),
                        protocol: "get".to_string(),
                        tls: "none".to_string(),
                        tokio_ringline: None,
                        tokio_tokio,
                        ringline_ringline: None,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── Memcache benchmarks ────────────────────────────────────────
    //
    // Same shape as the Redis bench: a single tokio server is the
    // target; we drive it with both a ringline-memcache client and a
    // hand-rolled tokio TCP client so the per-cell row pair shows
    // which client runtime wins on the same wire format.
    if do_memcache || do_all {
        eprintln!("\n=== Memcache Benchmarks ===\n");

        let port_manager = port_manager.clone();
        for &num_clients in &args.clients {
            for &msg_size in &args.sizes {
                let combos: &[(&str, ClientRuntime)] = &[
                    ("ringline", ClientRuntime::Ringline),
                    ("tokio", ClientRuntime::Tokio),
                ];

                for &(client_name, client_rt) in combos {
                    let result = memcache::run_memcache(
                        &port_manager,
                        workers,
                        num_clients,
                        msg_size,
                        warmup,
                        duration,
                        client_rt,
                        ServerRuntime::Ringline,
                    );

                    eprintln!(
                        "  {:>8} -> {:<8}  {:>4}c x {:>5}: {:>9.0} ops/s  p50: {}  p99: {}",
                        client_name,
                        "tokio",
                        num_clients,
                        format_size(msg_size),
                        result.ops_per_sec,
                        format_ns(result.latency.p50_ns),
                        format_ns(result.latency.p99_ns),
                    );

                    let (ringline_tokio, tokio_tokio) = match client_name {
                        "ringline" => (Some(result), None),
                        _ => (None, Some(result)),
                    };

                    all_results.push(ConfigResult {
                        workers,
                        clients: num_clients,
                        msg_size,
                        client_runtime: client_name.to_string(),
                        server_runtime: "tokio".to_string(),
                        transport: "memcache".to_string(),
                        protocol: "get".to_string(),
                        tls: "none".to_string(),
                        tokio_ringline: None,
                        tokio_tokio,
                        ringline_ringline: None,
                        ringline_tokio,
                    });
                }

                eprintln!();
            }
        }
    }

    // ── JSON output ───────────────────────────────────────────────
    if let Some(ref path) = args.json {
        let report = BenchReport {
            timestamp: timestamp(),
            git_commit: git_commit(),
            tls_engine: tls::engine_name().to_string(),
            configs: all_results,
            tls_echo: Vec::new(),
        };
        write_json(path, &report);
    }
}

fn format_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}
