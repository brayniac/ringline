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
                mean_ns: 0,
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
            // Little's law is stated in the mean, not the median: a closed-loop
            // arm is only measuring what it claims to if
            // `connections == throughput * mean latency`. Checking it against
            // p50 passes arms whose tail has quietly taken over.
            mean_ns: (self.samples.iter().map(|&s| s as u128).sum::<u128>() / n as u128) as u64,
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
    /// Arithmetic mean, for the closed-loop Little's law check.
    pub mean_ns: u64,
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
    pub total_ops: u64,
    pub latency: LatencyStats,
    pub cpu_ns: u64,
    /// Responses recorded per connection in the measured window. ringline
    /// client only.
    pub per_conn: Option<ConnCounts>,
}

/// How responses in the measured window were spread over the connections
/// that reported. A response that never arrives has no latency sample, so a
/// connection that got none is visible here and not in [`LatencyStats`].
#[derive(Clone, Serialize)]
pub struct ConnCounts {
    /// Connections that reported samples (connected and ran to the end).
    pub conns: u64,
    /// Connections with no response in the measured window.
    pub zero: u64,
    pub min: u64,
    pub p10: u64,
    pub median: u64,
    pub max: u64,
}

impl ConnCounts {
    /// `None` when no connection reported.
    pub fn from_counts(mut counts: Vec<u64>) -> Option<Self> {
        if counts.is_empty() {
            return None;
        }
        counts.sort_unstable();
        let at = |q: f64| counts[((counts.len() - 1) as f64 * q) as usize];
        Some(Self {
            conns: counts.len() as u64,
            zero: counts.iter().filter(|&&c| c == 0).count() as u64,
            min: counts[0],
            p10: at(0.1),
            median: at(0.5),
            max: counts[counts.len() - 1],
        })
    }
}

/// Read process CPU time (user + system) from /proc/self/stat.
pub fn process_cpu_time_ns() -> u64 {
    let stat = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0,
    };
    let fields: Vec<&str> = stat.split_whitespace().collect();
    if fields.len() < 15 {
        return 0;
    }
    let utime: u64 = fields[13].parse().unwrap_or(0);
    let stime: u64 = fields[14].parse().unwrap_or(0);
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    if ticks_per_sec == 0 {
        return 0;
    }
    (utime + stime) * 1_000_000_000 / ticks_per_sec
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
