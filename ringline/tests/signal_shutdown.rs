#![allow(clippy::manual_async_fn)]
//! `wait_on_signal`, in a test binary of its own.
//!
//! This test sends **SIGTERM to its own process**. A signal is delivered to
//! the process, not to a thread, so every ringline server alive anywhere in
//! that process is torn down — including servers belonging to other tests
//! that `cargo test` is running in parallel threads of the same binary.
//! Whichever of those is mid-round-trip when the signal lands sees
//! `ECONNRESET`, which is how this test used to make *other* tests in
//! `echo.rs` fail intermittently on the `Test (mio)` CI job
//! (ringline-rs/ringline#386).
//!
//! Cargo runs each integration-test binary as its own process, so giving
//! this test a file to itself contains the blast radius by construction.
//! **Keep it alone here**: any test added to this file is a test that can be
//! killed mid-flight by the signal below.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ringline::{
    AsyncEventHandler, Config, ConfigBuilder, Connection, ParseResult, RinglineBuilder,
};

struct AsyncEcho;

impl AsyncEventHandler for AsyncEcho {
    fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            let (mut tx, mut rx) = conn.split();
            loop {
                let n = rx
                    .with_data(|data| {
                        let owned = data.to_vec();
                        let _ = tx.send_nowait(&owned);
                        ParseResult::Consumed(owned.len())
                    })
                    .await;
                if n == 0 {
                    break;
                }
            }
        }
    }
    fn create_for_worker(_id: usize) -> Self {
        AsyncEcho
    }
}

fn test_config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(64, 4096)
        .max_connections(64)
        .send_pool(64, 16384)
        .build()
        .expect("valid config")
}

fn free_port() -> u16 {
    // Ports come from *below* the ephemeral range (Linux's `ip_local_port_range`
    // starts at 32768, macOS at 49152). That is the whole fix for #431: the
    // kernel never auto-assigns a port down here, so the probe-bind/drop/rebind
    // window stops being a race. Nothing can take one of these out from under
    // the caller except another process asking for it by number.
    //
    // The old version probed with `bind(":0")` and dropped the listener, which
    // left the port in the ephemeral pool. Between the drop and the server's
    // real bind, the kernel could hand it to anyone — surfacing either as an
    // `AddrInUse` launch failure, or (worse, in
    // `async_outbound_connect_refused`) as a connection *succeeding* to a port
    // the test believed was dead.
    //
    // `cargo test` runs binaries concurrently, so the window is offset per
    // process; `CLAIMED` keeps threads inside one binary from colliding.
    use std::sync::Mutex;
    static CLAIMED: Mutex<Option<std::collections::HashSet<u16>>> = Mutex::new(None);
    const BASE: u16 = 20_000;
    const SPAN: u16 = 10_000;

    let stride = ((std::process::id() % 40) as u16).saturating_mul(250);
    for step in 0..SPAN {
        let port = BASE + (stride + step) % SPAN;
        {
            let mut guard = CLAIMED.lock().unwrap();
            if !guard.get_or_insert_with(Default::default).insert(port) {
                continue;
            }
        }
        // Confirm nothing currently holds it. Unlike the old probe, dropping
        // this listener does not return the port to a pool anyone draws from.
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in the test range {BASE}..{}", BASE + SPAN);
}

fn wait_for_server(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("server did not start on {addr}");
}

/// `wait_on_signal` shuts down workers when SIGTERM is sent to self.
#[test]
fn signal_wait_on_signal_shutdown() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind(addr.parse().unwrap())
        .launch::<AsyncEcho>()
        .expect("launch failed");

    wait_for_server(&addr);

    // Verify the server is running.
    let mut stream = TcpStream::connect(&addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"hi").unwrap();
    let mut got = [0u8; 2];
    stream.read_exact(&mut got).unwrap();
    assert_eq!(&got, b"hi");
    drop(stream);

    // Send SIGTERM to self from a background thread after a short delay.
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(100));
        unsafe {
            libc::kill(libc::getpid(), libc::SIGTERM);
        }
    });

    let sig = shutdown.wait_on_signal();
    assert_eq!(sig, ringline::Signal::Terminate);

    for h in handles {
        h.join().unwrap().unwrap();
    }
}
