//! Shared benchmark building blocks used by the `ringline-bench` matrix
//! binary and the standalone `bench-server` / `bench-client` binaries.

pub mod client;
pub mod output;
pub mod runtime_metrics;
pub mod search;

/// Re-export from ringline for use in bench binaries.
pub use ringline::physical_core_count;
pub mod servers;
pub mod stats;
