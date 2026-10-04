//! The test helper that joins a runtime's workers fails a test whose handler
//! never requests shutdown, instead of hanging it (#447).

#![allow(clippy::manual_async_fn)]

mod common;

use std::future::Future;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};

/// A client-only handler that never requests shutdown.
struct NeverShutsDown;

impl AsyncEventHandler for NeverShutsDown {
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        NeverShutsDown
    }
}

#[test]
#[should_panic(expected = "the workers did not exit within")]
fn workers_that_never_exit_fail_the_test() {
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .build()
        .expect("valid config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<NeverShutsDown>()
        .expect("launch");
    common::join_workers_within(&runtime, handles, Duration::from_millis(200));
}
