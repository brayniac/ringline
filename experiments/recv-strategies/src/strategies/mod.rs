//! The receive strategies. Each is a small, self-contained program against
//! io_uring; their size and per-connection state are part of what the
//! benchmark compares.

mod oneshot;
mod ring;
mod shared;

use crate::server::Strategy;
use crate::{arg, flag};

pub fn build(name: &str, args: &[String], nconns: usize, sqpoll: bool) -> Box<dyn Strategy> {
    let region: usize = arg(args, "--region", Some(4096));
    let region_max: usize = arg(args, "--region-max", Some(4 << 20));
    let hold_every: usize = arg(args, "--hold-every", Some(0));
    let hold_us: u64 = arg(args, "--hold-us", Some(0));
    let lend_cap: f64 = arg(args, "--lend-cap", Some(1.0));
    match name {
        "shared" | "shared_inc" | "two_ring" | "two_ring_inc" => {
            let inc = name.ends_with("_inc");
            let large = name.starts_with("two_ring").then(|| {
                (
                    arg(args, "--large-bufs", Some(256)),
                    arg(args, "--large-buf-size", Some(1 << 20)),
                    shared::Promote {
                        bytes: arg(args, "--promote-bytes", Some(64 * 1024)),
                        after: arg(args, "--promote-after", Some(4)),
                        demote_after: arg(args, "--demote-after", Some(64)),
                        on_hold: flag(args, "--promote-on-hold"),
                    },
                )
            });
            let (bufs, size) = if inc {
                (64, 64 * 1024)
            } else {
                (256, 16 * 1024)
            };
            Box::new(shared::Shared::new(
                inc,
                arg(args, "--shared-bufs", Some(bufs)),
                arg(args, "--shared-buf-size", Some(size)),
                nconns,
                hold_every,
                arg(args, "--hold-first", Some(0)),
                hold_us,
                lend_cap,
                flag(args, "--no-thp"),
                arg(args, "--recv-len", Some(0)),
                flag(args, "--bounded-acc"),
                large,
            ))
        }
        "ring" | "ring_norewrite" => {
            assert!(!sqpoll, "the ring strategies rely on DEFER_TASKRUN");
            Box::new(ring::Ring::new(
                name == "ring",
                flag(args, "--adapt"),
                nconns,
                region,
                region_max,
            ))
        }
        "oneshot" => Box::new(oneshot::Oneshot::new(nconns, region, region_max)),
        s => panic!("unknown strategy {s}"),
    }
}
