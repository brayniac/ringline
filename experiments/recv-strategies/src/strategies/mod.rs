//! The receive strategies. Each is a small, self-contained program against
//! io_uring; their size and per-connection state are part of what the
//! benchmark compares.

mod oneshot;
mod ring;
mod shared;

use crate::arg;
use crate::server::Strategy;

pub fn build(name: &str, args: &[String], nconns: usize, sqpoll: bool) -> Box<dyn Strategy> {
    let region: usize = arg(args, "--region", Some(4096));
    let region_max: usize = arg(args, "--region-max", Some(4 << 20));
    let hold_every: usize = arg(args, "--hold-every", Some(0));
    let hold_us: u64 = arg(args, "--hold-us", Some(0));
    let lend_cap: f64 = arg(args, "--lend-cap", Some(1.0));
    match name {
        "shared" => Box::new(shared::Shared::new(
            false,
            arg(args, "--shared-bufs", Some(256)),
            arg(args, "--shared-buf-size", Some(16 * 1024)),
            nconns,
            hold_every,
            hold_us,
            lend_cap,
        )),
        "shared_inc" => Box::new(shared::Shared::new(
            true,
            arg(args, "--shared-bufs", Some(64)),
            arg(args, "--shared-buf-size", Some(64 * 1024)),
            nconns,
            hold_every,
            hold_us,
            lend_cap,
        )),
        "ring" | "ring_norewrite" => {
            assert!(!sqpoll, "the ring strategies rely on DEFER_TASKRUN");
            Box::new(ring::Ring::new(name == "ring", nconns, region, region_max))
        }
        "oneshot" => Box::new(oneshot::Oneshot::new(nconns, region, region_max)),
        s => panic!("unknown strategy {s}"),
    }
}
