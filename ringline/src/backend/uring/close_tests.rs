//! Each kernel's close path, end to end on whatever kernel runs the tests.
//!
//! `CloseLead` is chosen from the running kernel, so a host runs one close
//! path. These tests force each lead through `Config::close_lead_override`
//! and check that a connection whose task ends sends its FIN. They use one
//! connection: with two, a pre-6.13 kernel holds the closed socket open
//! through the other connection's recv, whatever the lead (#581).

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use super::ring::CloseLead;
use crate::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

/// Reads until it has seen `bye`, then returns, which closes the
/// connection.
struct EndsOnBye;

impl AsyncEventHandler for EndsOnBye {
    fn on_accept(&self, mut conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let mut seen = Vec::new();
            while !seen.ends_with(b"bye") {
                let n = conn
                    .with_data(|d| {
                        seen.extend_from_slice(d);
                        ParseResult::Consumed(d.len())
                    })
                    .await;
                if n == 0 {
                    return;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        EndsOnBye
    }
}

/// Whether a connection closed by its task, with `lead` ahead of the
/// `Close`, reaches the peer as EOF within 3 s.
fn fin_arrives(lead: CloseLead, timestamps: bool) -> bool {
    let mut config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .blocking_threads(0)
        .resolver_threads(0)
        .spawner_threads(0)
        .build()
        .expect("valid config");
    #[cfg(feature = "timestamps")]
    {
        config.timestamps = timestamps;
    }
    #[cfg(not(feature = "timestamps"))]
    assert!(!timestamps, "timestamps need the `timestamps` feature");
    config.close_lead_override = Some(lead);
    let (runtime, handles) = RinglineBuilder::new(config)
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<EndsOnBye>()
        .expect("launch");
    let mut conn = TcpStream::connect(runtime.bound_addr().unwrap()).expect("connect");
    conn.write_all(b"bye").unwrap();
    conn.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let closed = matches!(conn.read(&mut [0u8; 16]), Ok(0));
    runtime.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker failed");
    }
    closed
}

#[test]
fn a_close_led_by_a_shutdown_sends_its_fin() {
    assert!(fin_arrives(CloseLead::Shutdown, false));
}

#[test]
fn a_close_led_by_a_cancel_sends_its_fin() {
    assert!(fin_arrives(CloseLead::CancelAll, false));
}

/// A timestamped connection's recv is a `RecvMsgMulti`, which the close's
/// own recv cancel does not reach. The lead must end it.
#[cfg(feature = "timestamps")]
#[test]
fn a_timestamped_close_led_by_a_cancel_sends_its_fin() {
    assert!(fin_arrives(CloseLead::CancelAll, true));
}

#[cfg(feature = "timestamps")]
#[test]
fn a_timestamped_close_led_by_a_shutdown_sends_its_fin() {
    assert!(fin_arrives(CloseLead::Shutdown, true));
}

/// The control: with nothing ahead of the `Close`, the timestamped recv
/// holds the socket open and no FIN arrives. Without this, the two tests
/// above could pass for a reason other than the lead.
#[cfg(feature = "timestamps")]
#[test]
fn a_timestamped_close_led_by_nothing_sends_no_fin() {
    assert!(!fin_arrives(CloseLead::Nothing, true));
}
