use serde::Serialize;
use std::time::Instant;

use crate::stats::{BenchResult, format_ns};

#[derive(Serialize)]
pub struct ConfigResult {
    pub workers: usize,
    pub clients: usize,
    pub msg_size: usize,
    pub client_runtime: String,
    pub server_runtime: String,
    pub transport: String,
    pub protocol: String,
    pub tls: String,
    pub tokio_ringline: Option<BenchResult>,
    pub tokio_tokio: Option<BenchResult>,
    pub ringline_ringline: Option<BenchResult>,
    pub ringline_tokio: Option<BenchResult>,
}

#[derive(Serialize)]
pub struct BenchReport {
    pub timestamp: String,
    pub git_commit: String,
    /// Which ringline TLS record layer this binary was built with —
    /// `buffered` (default) or `unbuffered` (`--features tls-unbuffered`).
    /// Recorded so a JSON file is self-describing and two arms cannot be
    /// diffed without noticing they are the same build.
    pub tls_engine: String,
    pub configs: Vec<ConfigResult>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tls_echo: Vec<crate::protocols::tls::TlsEchoResult>,
}

pub fn timestamp() -> String {
    let now = Instant::now();
    let duration = now.elapsed();
    let (hours, minutes, seconds) = (
        duration.as_secs() / 3600,
        (duration.as_secs() % 3600) / 60,
        duration.as_secs() % 60,
    );
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

pub fn git_commit() -> String {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output();
    match output {
        Ok(o) if o.status.success() => String::from_utf8(o.stdout).unwrap_or_default(),
        _ => "unknown".to_string(),
    }
}

pub fn print_table(results: &[ConfigResult]) {
    // Summary row
    let total_configs = results.len();
    eprintln!("\n=== Summary: {} configurations ===\n", total_configs);

    for r in results {
        eprintln!(
            "  {}x{}  {} -> {}  {}",
            r.clients, r.msg_size, r.transport, r.protocol, r.tls,
        );

        if let Some(ringline_ringline) = &r.ringline_ringline {
            eprintln!(
                "    ringline -> ringline:  {:>9.0} ops/s  p50: {}  p99: {}",
                ringline_ringline.ops_per_sec,
                format_ns(ringline_ringline.latency.p50_ns),
                format_ns(ringline_ringline.latency.p99_ns),
            );
        }

        if let Some(tokio_tokio) = &r.tokio_tokio {
            eprintln!(
                "    tokio -> tokio:        {:>9.0} ops/s  p50: {}  p99: {}",
                tokio_tokio.ops_per_sec,
                format_ns(tokio_tokio.latency.p50_ns),
                format_ns(tokio_tokio.latency.p99_ns),
            );
        }
    }
}

/// Print the TLS-echo table.
///
/// `cpu/op` is the headline: server-process CPU time divided by operations the
/// server itself completed in the same window. Throughput and latency are the
/// GO/NO-GO's "no regression" guard, printed alongside so a CPU win bought with
/// a latency loss is visible.
pub fn print_tls_table(results: &[crate::protocols::tls::TlsEchoResult]) {
    if results.is_empty() {
        return;
    }
    eprintln!("\n=== TLS echo: server CPU per operation ===");
    eprintln!(
        "  engine={}  (server runs as a child process; CPU is its own getrusage delta)\n",
        results[0].engine
    );
    eprintln!(
        "  {:>5} {:>8} {:>6} {:>10} {:>11} {:>10} {:>9} {:>9}",
        "tls", "size", "conns", "ops/s", "cpu ns/op", "cpu ns/B", "p50", "p99"
    );
    for r in results {
        eprintln!(
            "  {:>5} {:>8} {:>6} {:>10.0} {:>11.1} {:>10.4} {:>9} {:>9}",
            r.tls,
            crate::stats::format_size(r.msg_size),
            r.clients,
            r.client.ops_per_sec,
            r.cpu_ns_per_op,
            r.cpu_ns_per_byte,
            format_ns(r.client.latency.p50_ns),
            format_ns(r.client.latency.p99_ns),
        );
        if let Some(w) = &r.warning {
            eprintln!("        !! {w}");
        }
    }
    // Evidence that the TLS rows really are TLS: what rustls actually
    // negotiated on the client side, not what the flag asked for.
    match results.iter().find(|r| r.tls == "tls") {
        Some(r) => eprintln!("\n  TLS rows negotiated: {}", r.negotiated),
        None => eprintln!("\n  no TLS rows in this run"),
    }
}

pub fn write_json(path: &str, report: &BenchReport) {
    let content = serde_json::to_string_pretty(report).unwrap();
    std::fs::write(path, content).expect("failed to write JSON output");
}
