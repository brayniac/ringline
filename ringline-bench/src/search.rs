//! SLO-limited throughput search for the open-loop client.
//!
//! [`RateSearch`] is cachecannon's search (`src/saturation.rs`): climb
//! geometrically from a start rate until the first step that fails, then
//! bisect between the last passing rate and the failing one until the
//! interval is within a relative tolerance or a probe budget runs out. The
//! reported knee is always a rate that passed.

use crate::stats::LatencyStats;

/// Phase of the rate search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchPhase {
    /// Geometric climb until the first failing step.
    Climb,
    /// Bisecting between the last passing and the first failing rate.
    Bisect,
}

impl SearchPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchPhase::Climb => "climb",
            SearchPhase::Bisect => "bisect",
        }
    }
}

/// What to do after measuring the current rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchOutcome {
    /// Measure this rate next.
    Probe(u64),
    /// The search finished. `knee` is the highest rate that passed, if any.
    Done { knee: Option<u64> },
}

/// Climb-then-bisect rate search. The caller measures `current_rate()` and
/// passes the result to [`advance`](Self::advance).
pub struct RateSearch {
    phase: SearchPhase,
    current_rate: u64,
    last_good: Option<u64>,
    lo: u64,
    hi: u64,
    bisect_steps: u32,
    factor: f64,
    max_rate: u64,
    tolerance: f64,
    max_bisect_steps: u32,
}

impl RateSearch {
    pub fn new(
        start_rate: u64,
        factor: f64,
        max_rate: u64,
        tolerance: f64,
        max_bisect_steps: u32,
    ) -> Self {
        let start_rate = start_rate.clamp(1, max_rate.max(1));
        Self {
            phase: SearchPhase::Climb,
            current_rate: start_rate,
            last_good: None,
            lo: 0,
            hi: 0,
            bisect_steps: 0,
            factor,
            max_rate,
            tolerance,
            max_bisect_steps,
        }
    }

    /// The rate to measure next.
    pub fn current_rate(&self) -> u64 {
        self.current_rate
    }

    pub fn phase(&self) -> SearchPhase {
        self.phase
    }

    /// The highest rate that has passed so far.
    pub fn last_good(&self) -> Option<u64> {
        self.last_good
    }

    /// Record whether `current_rate()` passed and choose the next move.
    pub fn advance(&mut self, passed: bool) -> SearchOutcome {
        if passed {
            self.last_good = Some(
                self.last_good
                    .map_or(self.current_rate, |g| g.max(self.current_rate)),
            );
        }
        match self.phase {
            SearchPhase::Climb => {
                if !passed {
                    self.lo = self.last_good.unwrap_or(0);
                    self.hi = self.current_rate;
                    self.phase = SearchPhase::Bisect;
                    return self.bisect_probe();
                }
                if self.current_rate >= self.max_rate {
                    return SearchOutcome::Done {
                        knee: self.last_good,
                    };
                }
                let next = ((self.current_rate as f64) * self.factor) as u64;
                self.current_rate = next.max(self.current_rate + 1).min(self.max_rate);
                SearchOutcome::Probe(self.current_rate)
            }
            SearchPhase::Bisect => {
                if passed {
                    self.lo = self.current_rate;
                } else {
                    self.hi = self.current_rate;
                }
                self.bisect_steps += 1;
                let width = (self.hi - self.lo) as f64 / self.hi as f64;
                if width <= self.tolerance
                    || self.bisect_steps >= self.max_bisect_steps
                    || self.hi - self.lo <= 1
                {
                    return SearchOutcome::Done {
                        knee: self.last_good,
                    };
                }
                self.bisect_probe()
            }
        }
    }

    fn bisect_probe(&mut self) -> SearchOutcome {
        // A failure at the start rate leaves lo = 0; probing below 1 request/s
        // measures nothing.
        if self.hi <= 1 {
            return SearchOutcome::Done {
                knee: self.last_good,
            };
        }
        let mid = self.lo + (self.hi - self.lo) / 2;
        self.current_rate = mid.max(self.lo + 1).min(self.hi - 1).max(1);
        SearchOutcome::Probe(self.current_rate)
    }
}

/// The pass condition for one step: latency thresholds on the
/// coordinated-omission-corrected response time, and a floor on achieved
/// throughput as a fraction of the offered rate.
#[derive(Debug, Clone, Copy)]
pub struct Slo {
    pub p50_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub p999_ns: Option<u64>,
    pub min_throughput_ratio: f64,
}

impl Slo {
    /// `None` if the step passed, otherwise why it failed. A step with no
    /// samples fails.
    pub fn check(&self, offered: u64, achieved: f64, lat: &LatencyStats) -> Option<String> {
        if lat.count == 0 {
            return Some("no samples".to_string());
        }
        let ratio = achieved / offered as f64;
        if ratio < self.min_throughput_ratio {
            return Some(format!(
                "throughput {:.1}% of offered (need {:.1}%)",
                ratio * 100.0,
                self.min_throughput_ratio * 100.0
            ));
        }
        for (name, limit, value) in [
            ("p50", self.p50_ns, lat.p50_ns),
            ("p99", self.p99_ns, lat.p99_ns),
            ("p999", self.p999_ns, lat.p999_ns),
        ] {
            if let Some(limit) = limit
                && value > limit
            {
                return Some(format!("{name} {value} ns > {limit} ns"));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a search against an oracle in which every rate up to `knee`
    /// passes. Returns the reported knee and the probes made.
    fn run(knee: u64, start: u64, factor: f64, max_rate: u64) -> (Option<u64>, Vec<u64>) {
        let mut s = RateSearch::new(start, factor, max_rate, 0.05, 8);
        let mut probes = vec![s.current_rate()];
        loop {
            assert!(
                probes.len() < 200,
                "search did not terminate for knee {knee}"
            );
            match s.advance(*probes.last().unwrap() <= knee) {
                SearchOutcome::Probe(r) => probes.push(r),
                SearchOutcome::Done { knee } => return (knee, probes),
            }
        }
    }

    #[test]
    fn converges_within_tolerance_below_the_true_knee() {
        let (found, _) = run(30_000, 1000, 2.0, 1_000_000);
        let found = found.expect("a knee");
        assert!(found <= 30_000, "reported knee {found} did not pass");
        assert!((30_000 - found) as f64 / 30_000.0 <= 0.05, "found {found}");
    }

    #[test]
    fn reported_knee_always_passed() {
        for knee in [0, 1, 500, 1000, 1500, 7777, 30_000, 999_999, 5_000_000] {
            let (found, probes) = run(knee, 1000, 1.25, 1_000_000);
            if let Some(k) = found {
                assert!(k <= knee, "knee {k} reported for true knee {knee}");
                assert!(probes.contains(&k), "knee {k} was never probed");
            }
        }
    }

    #[test]
    fn start_rate_failing_reports_no_knee_and_probes_nothing_below_one() {
        let (found, probes) = run(0, 1000, 2.0, 1_000_000);
        assert_eq!(found, None);
        assert!(probes.iter().all(|&r| r >= 1), "probes {probes:?}");
    }

    #[test]
    fn climb_to_max_rate_without_failure_reports_max_rate() {
        let (found, probes) = run(u64::MAX, 1000, 2.0, 100_000);
        assert_eq!(found, Some(100_000));
        assert_eq!(*probes.last().unwrap(), 100_000);
    }

    #[test]
    fn a_factor_close_to_one_still_climbs() {
        // 10 * 1.01 truncates to 10; the climb must still move.
        let (_, probes) = run(20, 10, 1.01, 1000);
        assert!(probes.windows(2).take(5).all(|w| w[1] > w[0]), "{probes:?}");
    }

    #[test]
    fn bisect_budget_bounds_the_probe_count() {
        let mut s = RateSearch::new(1000, 2.0, 1_000_000, 0.0, 4);
        let mut probes = 1;
        let mut rate = s.current_rate();
        while let SearchOutcome::Probe(r) = s.advance(rate <= 12_345) {
            rate = r;
            probes += 1;
            assert!(probes < 50);
        }
        // Climb: 1000, 2000, 4000, 8000, 16000 (fails). Then 4 bisect probes.
        assert_eq!(probes, 5 + 4);
    }

    fn stats(count: u64, p50: u64, p99: u64, p999: u64) -> LatencyStats {
        LatencyStats {
            mean_ns: p50,
            p50_ns: p50,
            p90_ns: p99,
            p99_ns: p99,
            p999_ns: p999,
            p9999_ns: p999,
            max_ns: p999,
            count,
        }
    }

    #[test]
    fn slo_checks_throughput_then_each_threshold() {
        let slo = Slo {
            p50_ns: None,
            p99_ns: Some(1_000_000),
            p999_ns: None,
            min_throughput_ratio: 0.9,
        };
        assert_eq!(
            slo.check(1000, 950.0, &stats(1, 1, 900_000, 5_000_000)),
            None
        );
        assert!(
            slo.check(1000, 899.0, &stats(1, 1, 1, 1))
                .unwrap()
                .starts_with("throughput")
        );
        assert!(
            slo.check(1000, 1000.0, &stats(1, 1, 1_000_001, 1))
                .unwrap()
                .starts_with("p99")
        );
        assert_eq!(
            slo.check(1000, 1000.0, &stats(0, 0, 0, 0)).as_deref(),
            Some("no samples")
        );
    }
}
