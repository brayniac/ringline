use serde::Serialize;

/// Per-operation latency histogram backed by a raw sample vector.
pub struct LatencyHistogram {
    samples: Vec<u64>, // nanoseconds per op
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        LatencyHistogram {
            samples: Vec::with_capacity(1_000_000),
        }
    }

    pub fn record(&mut self, ns: u64) {
        self.samples.push(ns);
    }

    pub fn samples(&self) -> &[u64] {
        &self.samples
    }

    pub fn finalize(&mut self) -> LatencyStats {
        self.samples.sort_unstable();
        let n = self.samples.len();
        if n == 0 {
            return LatencyStats {
                p50_ns: 0,
                p90_ns: 0,
                p99_ns: 0,
                p999_ns: 0,
                p9999_ns: 0,
                max_ns: 0,
                count: 0,
            };
        }
        LatencyStats {
            p50_ns: self.samples[n * 50 / 100],
            p90_ns: self.samples[n * 90 / 100],
            p99_ns: self.samples[n * 99 / 100],
            p999_ns: self.samples[n.saturating_sub(1).min(n * 999 / 1000)],
            p9999_ns: self.samples[n.saturating_sub(1).min(n * 9999 / 10000)],
            max_ns: self.samples[n - 1],
            count: n as u64,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct LatencyStats {
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub p9999_ns: u64,
    pub max_ns: u64,
    pub count: u64,
}

#[derive(Clone, Serialize)]
pub struct BenchResult {
    pub ops_per_sec: f64,
    pub latency: LatencyStats,
    pub cpu_ns: u64,
}

/// Calling process CPU time (user + system) in nanoseconds, from
/// `getrusage(RUSAGE_SELF)`.
///
/// This is the kernel's own accounting for the whole process (all threads),
/// read directly rather than sampled or parsed. Preferred over the
/// `/proc/self/stat` reader it replaces for two reasons: `/proc` quantises to
/// `_SC_CLK_TCK` ticks (10 ms on most kernels), which is coarse next to a
/// few-second measurement window, and it does not exist on macOS — where the
/// old reader silently returned 0 and every `cpu_ns` in the output was a
/// zero that looked like a measurement.
///
/// Returns 0 only if `getrusage` itself fails.
pub fn self_cpu_time_ns() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `usage` is a valid, correctly-sized, zero-initialised rusage.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0;
    }
    let to_ns = |tv: libc::timeval| {
        (tv.tv_sec as u64).wrapping_mul(1_000_000_000) + (tv.tv_usec as u64).wrapping_mul(1_000)
    };
    to_ns(usage.ru_utime) + to_ns(usage.ru_stime)
}

/// Read process CPU time (user + system).
///
/// Note that in the in-process benches this counts **client and server
/// together** — both run in this process — so it cannot attribute CPU to one
/// side. The TLS bench (`protocols::tls`) runs the server as a child process
/// precisely to get around that.
pub fn process_cpu_time_ns() -> u64 {
    self_cpu_time_ns()
}

pub fn format_size(bytes: usize) -> String {
    if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

pub fn format_ns(ns: u64) -> String {
    if ns >= 1_000_000 {
        format!("{:.2}ms", ns as f64 / 1_000_000.0)
    } else if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{}ns", ns)
    }
}
