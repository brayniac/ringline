//! A simulated clock for this crate's QUIC tests.
//!
//! The tests drive two sans-IO endpoints against each other in memory. Every
//! `now` they pass comes from this clock, never from the real one, so CPU load
//! between rounds cannot fire a timer or stretch a transfer: a test fails the
//! same way every time. The clock is per thread, and each test runs on its own
//! thread (`cargo test`) or in its own process (nextest).

#![allow(dead_code)]

use std::cell::Cell;
use std::time::{Duration, Instant};

/// How far [`tick`] advances the clock. The pump helpers call [`tick`] at
/// the start of each round of ferrying packets; some ferry several times
/// within one round.
pub const ROUND: Duration = Duration::from_millis(1);

thread_local! {
    static NOW: Cell<Option<Instant>> = const { Cell::new(None) };
}

/// The simulated time. The first call on a thread fixes its starting point.
pub fn now() -> Instant {
    NOW.with(|n| match n.get() {
        Some(t) => t,
        None => {
            let t = Instant::now();
            n.set(Some(t));
            t
        }
    })
}

/// Move the simulated time forward by `d`.
pub fn advance(d: Duration) {
    let t = now() + d;
    NOW.with(|n| n.set(Some(t)));
}

/// Advance by one [`ROUND`] and return the new time. Pump loops call this
/// once per round.
pub fn tick() -> Instant {
    advance(ROUND);
    now()
}
