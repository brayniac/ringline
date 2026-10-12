//! Standalone echo client for distributed benchmarking.
//!
//! Drives load against a server, measures latency and throughput,
//! and outputs JSON results to stdout. The `--runtime` flag selects the
//! load-generator runtime (ringline or tokio), mirroring `bench-server`.
//!
//! Usage:
//!   bench-client --runtime tokio    --addr 10.0.1.5:7878 --clients 100  --msg-size 64   --duration 10
//!   bench-client --runtime ringline --addr 10.0.1.5:7878 --clients 1000 --msg-size 4096 --duration 30 --warmup 5
//!   bench-client --runtime ringline --addr 10.0.1.5:7878 --clients 1000 --msg-size 4096 \
//!       --search --search-start 50000 --search-factor 1.25 --slo-p99-us 1000

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use clap::Parser;
use serde::Serialize;

use ringline_bench::client::{LiveOpenLoop, OpenLoop, run_bench};
use ringline_bench::search::{RateSearch, SearchOutcome, SearchPhase, Slo};
use ringline_bench::stats::LatencyStats;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Runtime {
    Ringline,
    Tokio,
}

#[derive(Parser)]
#[command(
    name = "bench-client",
    about = "Echo client for distributed benchmarking"
)]
struct Args {
    /// Load-generator runtime
    #[arg(long)]
    runtime: Runtime,

    /// Server address
    #[arg(long)]
    addr: String,

    /// Number of concurrent client connections
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Message size in bytes
    #[arg(long, default_value_t = 64)]
    msg_size: usize,

    /// Test duration in seconds
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Warmup duration in seconds
    #[arg(long, default_value_t = 3)]
    warmup: u64,

    /// Number of load-generation worker threads (applies to both runtimes;
    /// connections are distributed across them). 0 = physical core count.
    #[arg(long, default_value_t = 0)]
    threads: usize,

    /// Closed-loop pipeline depth: requests sent per batch on each connection,
    /// with each response's latency recorded against the batch's send time.
    /// 1 (the default) is send-one-wait-one. Higher values amortize the send
    /// and recv syscalls across `depth` requests, which is what makes the
    /// syscall-per-op claim measurable in closed loop — `--max-inflight`
    /// only applies to `--open`.
    #[arg(long, default_value_t = 1)]
    depth: usize,

    /// Open-loop mode: offer requests at a fixed rate instead of one in-flight
    /// per connection (closed loop). Requires --rate.
    #[arg(long)]
    open: bool,

    /// Aggregate offered requests/sec (open-loop only), split across connections.
    #[arg(long, default_value_t = 0)]
    rate: u64,

    /// Max outstanding requests per connection in open-loop mode. When reached
    /// the sender stalls and falls behind schedule (coordinated-omission-free).
    /// Kept modest so the per-connection send queue (each entry holds a
    /// send-copy-pool slot) can't exhaust the pool under overload.
    #[arg(long, default_value_t = 64)]
    max_inflight: usize,

    /// Connections assigned to each ringline worker before moving to the next.
    /// 1 = classic round-robin. Higher values pack connections onto fewer workers
    /// at low connection counts, keeping per-worker CQE density high enough for
    /// io_uring batching to pay off. Has no effect once connections exceed
    /// chunk_size * threads. (ringline client only)
    #[arg(long, default_value_t = 1)]
    conn_chunk_size: usize,

    /// SLO-limited throughput search, using cachecannon's method. The offered
    /// rate starts at `--search-start` and is multiplied by `--search-factor`
    /// after each step that passes. After the first step that fails, the rate
    /// is bisected between the last passing and the first failing rate until
    /// they are within `--search-tolerance` of each other or
    /// `--search-max-bisect` bisection steps have run. Each step is a
    /// separate open-loop run with new connections: `--warmup` seconds, then
    /// `--duration` seconds measured; with `--search-reuse`, the steps share
    /// one set of connections. Prints one JSON line per step and a final
    /// `"type":"result"` line with the highest passing rate.
    #[arg(long, conflicts_with_all = ["open", "rate"])]
    search: bool,

    /// Keep one set of connections open for the whole search and change the
    /// offered rate in place, as cachecannon does. `--warmup` runs once, at
    /// the start rate. Each step then waits `--drain-window-ms` for requests
    /// sent at the previous rate, and measures `--duration` seconds. ringline
    /// runtime only.
    #[arg(long, requires = "search")]
    search_reuse: bool,

    /// With `--search-reuse`: milliseconds after a rate change before a
    /// step's measurement starts.
    #[arg(long, default_value_t = 500)]
    drain_window_ms: u64,

    /// First offered rate of the search, requests/s.
    #[arg(long, default_value_t = 1000)]
    search_start: u64,

    /// Rate multiplier between climb steps. Must be above 1.
    #[arg(long, default_value_t = 1.05)]
    search_factor: f64,

    /// Highest rate the search offers. Reaching it with a passing step ends
    /// the search.
    #[arg(long, default_value_t = 100_000_000)]
    search_max_rate: u64,

    /// Bisection stops when (fail - pass) / fail is at or below this.
    #[arg(long, default_value_t = 0.05)]
    search_tolerance: f64,

    /// Most bisection steps after the climb.
    #[arg(long, default_value_t = 8)]
    search_max_bisect: u32,

    /// Seconds the whole search may take. A step that would end past it is
    /// not started; the result reports the highest passing rate so far.
    /// 0 is no limit.
    #[arg(long, default_value_t = 0)]
    search_time_limit: u64,

    /// SLO: largest passing p50 of the corrected response time, µs.
    #[arg(long)]
    slo_p50_us: Option<u64>,

    /// SLO: largest passing p99 of the corrected response time, µs.
    #[arg(long)]
    slo_p99_us: Option<u64>,

    /// SLO: largest passing p99.9 of the corrected response time, µs.
    #[arg(long)]
    slo_p999_us: Option<u64>,

    /// SLO: a step fails when achieved throughput is below this fraction of
    /// the offered rate.
    #[arg(long, default_value_t = 0.9)]
    min_throughput_ratio: f64,

    /// Seconds past `--warmup` + `--duration` a search step may run before
    /// the process prints a `"type":"step_timeout"` line and exits with
    /// status 3.
    #[arg(long, default_value_t = 60)]
    step_grace: u64,
}

/// One search step, printed as a JSON line.
#[derive(Serialize)]
struct StepReport {
    r#type: &'static str,
    index: usize,
    phase: &'static str,
    offered_rate: u64,
    ops_per_sec: f64,
    total_ops: u64,
    mean_ns: u64,
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    p9999_ns: u64,
    max_ns: u64,
    sample_count: u64,
    pass: bool,
    fail_reason: Option<String>,
    /// With `--search-reuse`: connections open at the end of the step.
    #[serde(skip_serializing_if = "Option::is_none")]
    connections: Option<u64>,
}

/// The search's final line.
#[derive(Serialize)]
struct SearchReport {
    r#type: &'static str,
    addr: String,
    runtime: String,
    clients: usize,
    msg_size: usize,
    warmup_secs: u64,
    duration_secs: u64,
    reuse_connections: bool,
    drain_window_ms: Option<u64>,
    slo_p50_ns: Option<u64>,
    slo_p99_ns: Option<u64>,
    slo_p999_ns: Option<u64>,
    min_throughput_ratio: f64,
    /// Highest offered rate whose step passed; `null` if none did.
    knee: Option<u64>,
    steps: usize,
    /// `converged` (bisection finished), `max_rate` (passed at
    /// `--search-max-rate`), or `time_limit`.
    stopped: &'static str,
}

#[derive(Serialize)]
struct ClientReport {
    addr: String,
    runtime: String,
    mode: String,
    offered_rate: u64,
    clients: usize,
    msg_size: usize,
    duration_secs: u64,
    ops_per_sec: f64,
    total_ops: u64,
    /// Arithmetic mean, for the closed-loop Little's law gate. Reported here
    /// and not only in `LatencyStats`, because this flat struct -- not
    /// `BenchResult` -- is what the client actually serializes. #404 added the
    /// field to `LatencyStats` alone, so it never reached a results file and
    /// the gate silently passed every arm.
    mean_ns: u64,
    p50_ns: u64,
    p90_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    p9999_ns: u64,
    max_ns: u64,
    sample_count: u64,
}

fn main() {
    let args = Args::parse();

    let threads = if args.threads == 0 {
        ringline::physical_core_count()
    } else {
        args.threads
    };

    let runtime_name = match args.runtime {
        Runtime::Ringline => "ringline",
        Runtime::Tokio => "tokio",
    };

    if args.search {
        run_search(&args, runtime_name, threads);
        return;
    }

    if args.open && args.rate == 0 {
        eprintln!("error: --open requires --rate > 0");
        std::process::exit(2);
    }
    let open = if args.open {
        Some(OpenLoop {
            rate: args.rate,
            max_inflight: args.max_inflight,
        })
    } else {
        None
    };
    let mode = if args.open { "open" } else { "closed" };

    eprintln!(
        "bench-client: {} runtime, {} mode{}, {} clients, {}B messages, {}s warmup + {}s test -> {}",
        runtime_name,
        mode,
        if args.open {
            format!(" @ {} rps", args.rate)
        } else {
            String::new()
        },
        args.clients,
        args.msg_size,
        args.warmup,
        args.duration,
        args.addr,
    );

    let result = run_bench(
        &args.addr,
        args.clients,
        args.msg_size,
        args.depth,
        Duration::from_secs(args.warmup),
        Duration::from_secs(args.duration),
        args.runtime == Runtime::Ringline,
        threads,
        open,
        args.conn_chunk_size,
    );

    let report = ClientReport {
        addr: args.addr,
        runtime: runtime_name.to_string(),
        mode: mode.to_string(),
        offered_rate: args.rate,
        clients: args.clients,
        msg_size: args.msg_size,
        duration_secs: args.duration,
        ops_per_sec: result.ops_per_sec,
        total_ops: result.total_ops,
        mean_ns: result.latency.mean_ns,
        p50_ns: result.latency.p50_ns,
        p90_ns: result.latency.p90_ns,
        p99_ns: result.latency.p99_ns,
        p999_ns: result.latency.p999_ns,
        p9999_ns: result.latency.p9999_ns,
        max_ns: result.latency.max_ns,
        sample_count: result.latency.count,
    };

    // JSON to stdout for machine consumption.
    println!("{}", serde_json::to_string_pretty(&report).unwrap());

    // Human-readable summary to stderr.
    eprintln!(
        "bench-client: {:.0} ops/s, mean={:.1}us p50={:.1}us p99={:.1}us p999={:.1}us ({} samples)",
        report.ops_per_sec,
        report.mean_ns as f64 / 1000.0,
        report.p50_ns as f64 / 1000.0,
        report.p99_ns as f64 / 1000.0,
        report.p999_ns as f64 / 1000.0,
        report.sample_count,
    );
}

fn run_search(args: &Args, runtime_name: &str, threads: usize) {
    let slo = Slo {
        p50_ns: args.slo_p50_us.map(|us| us * 1000),
        p99_ns: args.slo_p99_us.map(|us| us * 1000),
        p999_ns: args.slo_p999_us.map(|us| us * 1000),
        min_throughput_ratio: args.min_throughput_ratio,
    };
    let invalid = if slo.p50_ns.is_none() && slo.p99_ns.is_none() && slo.p999_ns.is_none() {
        Some("--search needs at least one of --slo-p50-us, --slo-p99-us, --slo-p999-us")
    } else if args.search_factor.is_nan() || args.search_factor <= 1.0 {
        Some("--search-factor must be above 1")
    } else if !(args.min_throughput_ratio > 0.0 && args.min_throughput_ratio <= 1.0) {
        Some("--min-throughput-ratio must be in (0, 1]")
    } else if !(0.0..1.0).contains(&args.search_tolerance) {
        Some("--search-tolerance must be in [0, 1)")
    } else if args.search_start == 0 || args.search_max_rate < args.search_start {
        Some("--search-start must be at least 1 and at most --search-max-rate")
    } else if args.search_reuse && args.runtime != Runtime::Ringline {
        Some("--search-reuse needs --runtime ringline")
    } else {
        None
    };
    if let Some(msg) = invalid {
        eprintln!("error: {msg}");
        std::process::exit(2);
    }

    let drain = Duration::from_millis(args.drain_window_ms);
    let step_secs = if args.search_reuse {
        (drain + Duration::from_secs(args.duration)).as_secs() + 1
    } else {
        args.warmup + args.duration
    };
    let step_limit = Duration::from_secs(step_secs + args.step_grace);
    let watchdog = StepWatchdog::start(step_limit);

    let mut search = RateSearch::new(
        args.search_start,
        args.search_factor,
        args.search_max_rate,
        args.search_tolerance,
        args.search_max_bisect,
    );
    let began = Instant::now();
    let live = if args.search_reuse {
        eprintln!(
            "bench-client: search opens {} connections at {} rps, {} s warmup",
            args.clients,
            search.current_rate(),
            args.warmup
        );
        watchdog.arm(0, search.current_rate());
        let live = LiveOpenLoop::start(
            &args.addr,
            args.clients,
            args.msg_size,
            threads,
            args.max_inflight,
            args.conn_chunk_size,
            search.current_rate(),
        )
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
        std::thread::sleep(Duration::from_secs(args.warmup));
        watchdog.disarm();
        Some(live)
    } else {
        None
    };
    let mut index = 0;
    let (knee, stopped) = loop {
        let rate = search.current_rate();
        if args.search_time_limit > 0
            && began.elapsed() + Duration::from_secs(step_secs)
                > Duration::from_secs(args.search_time_limit)
        {
            break (search.last_good(), "time_limit");
        }
        let phase = search.phase();
        eprintln!(
            "bench-client: search step {index} ({}) @ {rate} rps, {} clients, {}B -> {}",
            phase.as_str(),
            args.clients,
            args.msg_size,
            args.addr,
        );
        watchdog.arm(index, rate);
        let (ops_per_sec, total_ops, latency, connections) = match &live {
            Some(live) => {
                live.set_rate(rate);
                std::thread::sleep(drain);
                let before = live.snapshot();
                std::thread::sleep(Duration::from_secs(args.duration));
                let after = live.snapshot();
                let (ops_per_sec, latency) = LiveOpenLoop::step(&before, &after);
                (
                    ops_per_sec,
                    latency.count,
                    latency,
                    Some(live.connections()),
                )
            }
            None => {
                let result = run_bench(
                    &args.addr,
                    args.clients,
                    args.msg_size,
                    args.depth,
                    Duration::from_secs(args.warmup),
                    Duration::from_secs(args.duration),
                    args.runtime == Runtime::Ringline,
                    threads,
                    Some(OpenLoop {
                        rate,
                        max_inflight: args.max_inflight,
                    }),
                    args.conn_chunk_size,
                );
                (result.ops_per_sec, result.total_ops, result.latency, None)
            }
        };
        watchdog.disarm();
        let fail_reason = slo.check(rate, ops_per_sec, &latency);
        print_step(
            index,
            phase,
            rate,
            ops_per_sec,
            total_ops,
            &latency,
            connections,
            fail_reason.clone(),
        );
        index += 1;
        match search.advance(fail_reason.is_none()) {
            SearchOutcome::Probe(_) => {}
            SearchOutcome::Done { knee } => {
                let stopped = match search.phase() {
                    SearchPhase::Climb => "max_rate",
                    SearchPhase::Bisect => "converged",
                };
                break (knee, stopped);
            }
        }
    };

    if let Some(live) = live {
        watchdog.arm(index, 0);
        live.stop();
        watchdog.disarm();
    }

    let report = SearchReport {
        r#type: "result",
        addr: args.addr.clone(),
        runtime: runtime_name.to_string(),
        clients: args.clients,
        msg_size: args.msg_size,
        warmup_secs: args.warmup,
        duration_secs: args.duration,
        reuse_connections: args.search_reuse,
        drain_window_ms: args.search_reuse.then_some(args.drain_window_ms),
        slo_p50_ns: slo.p50_ns,
        slo_p99_ns: slo.p99_ns,
        slo_p999_ns: slo.p999_ns,
        min_throughput_ratio: slo.min_throughput_ratio,
        knee,
        steps: index,
        stopped,
    };
    println!("{}", serde_json::to_string(&report).unwrap());
    eprintln!(
        "bench-client: search {stopped} after {index} steps, knee {}",
        knee.map_or("none".to_string(), |k| format!("{k} rps")),
    );
}

#[allow(clippy::too_many_arguments)]
fn print_step(
    index: usize,
    phase: SearchPhase,
    rate: u64,
    ops_per_sec: f64,
    total_ops: u64,
    lat: &LatencyStats,
    connections: Option<u64>,
    fail_reason: Option<String>,
) {
    let step = StepReport {
        r#type: "step",
        index,
        phase: phase.as_str(),
        offered_rate: rate,
        ops_per_sec,
        total_ops,
        mean_ns: lat.mean_ns,
        p50_ns: lat.p50_ns,
        p90_ns: lat.p90_ns,
        p99_ns: lat.p99_ns,
        p999_ns: lat.p999_ns,
        p9999_ns: lat.p9999_ns,
        max_ns: lat.max_ns,
        sample_count: lat.count,
        pass: fail_reason.is_none(),
        fail_reason,
        connections,
    };
    // stdout is line-buffered, so each step is written out before the next
    // starts, and a step timeout does not lose the earlier ones.
    println!("{}", serde_json::to_string(&step).unwrap());
    eprintln!(
        "bench-client: step {index} @ {rate} rps: {:.0} ops/s, p99={:.1}us {}",
        ops_per_sec,
        lat.p99_ns as f64 / 1000.0,
        step.fail_reason.as_deref().unwrap_or("pass"),
    );
}

/// Ends the process if one search step runs past its limit. A client that
/// hangs (a connect that never completes, a worker that does not join) would
/// otherwise hold the load host until the experiment's own timeout.
struct StepWatchdog {
    armed: Arc<Mutex<Option<(Instant, usize, u64)>>>,
}

impl StepWatchdog {
    fn start(limit: Duration) -> Self {
        let armed: Arc<Mutex<Option<(Instant, usize, u64)>>> = Arc::new(Mutex::new(None));
        let watched = armed.clone();
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_secs(1));
                if let Some((started, index, rate)) = *watched.lock().unwrap()
                    && started.elapsed() > limit
                {
                    println!(
                        "{{\"type\":\"step_timeout\",\"index\":{index},\"offered_rate\":{rate},\"limit_secs\":{}}}",
                        limit.as_secs()
                    );
                    eprintln!(
                        "bench-client: search step {index} @ {rate} rps ran past {} s; exiting",
                        limit.as_secs()
                    );
                    std::process::exit(3);
                }
            }
        });
        Self { armed }
    }

    fn arm(&self, index: usize, rate: u64) {
        *self.armed.lock().unwrap() = Some((Instant::now(), index, rate));
    }

    fn disarm(&self) {
        *self.armed.lock().unwrap() = None;
    }
}
