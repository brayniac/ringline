//! Helpers shared by this crate's integration tests.

use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long a test's workers have to exit on their own before the test
/// shuts the runtime down and fails.
pub const WORKER_EXIT_DEADLINE: Duration = Duration::from_secs(30);

/// Join the workers of a runtime whose handler requests its own shutdown.
///
/// If the workers have not all exited within [`WORKER_EXIT_DEADLINE`], this
/// shuts the runtime down, waits briefly for them, and fails the test. A
/// handler that never requests shutdown then fails the test with a message,
/// where an unbounded join would hang it forever (#447).
///
/// Panics if a worker panicked or returned an error.
#[allow(dead_code)]
pub fn join_workers(
    runtime: &ringline::Runtime,
    handles: Vec<JoinHandle<Result<(), ringline::Error>>>,
) {
    join_workers_within(runtime, handles, WORKER_EXIT_DEADLINE);
}

/// [`join_workers`] with a deadline other than [`WORKER_EXIT_DEADLINE`].
#[allow(dead_code)]
pub fn join_workers_within(
    runtime: &ringline::Runtime,
    handles: Vec<JoinHandle<Result<(), ringline::Error>>>,
    limit: Duration,
) {
    let deadline = Instant::now() + limit;
    while !handles.iter().all(|h| h.is_finished()) {
        if Instant::now() >= deadline {
            runtime.shutdown();
            // Give the workers a moment to exit after the shutdown, so they
            // do not outlive the test holding its ports.
            let grace = Instant::now() + Duration::from_secs(5);
            while !handles.iter().all(|h| h.is_finished()) && Instant::now() < grace {
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!(
                "the workers did not exit within {limit:?}: the handler never requested \
                 shutdown"
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
}
