//! `RinglineBuilder::defer_listen` holds a port without listening on it, and
//! `begin_listening()` starts serving.
//!
//! The assertions are made from a client socket rather than by reading a flag:
//! what matters is that a peer cannot connect while the server is not ready,
//! which is what makes a TCP readiness probe fail honestly. A deferred
//! *accept* would pass that probe, which is the failure this feature exists to
//! prevent — see `docs/journal/2026-09-deferred-listen.md`.

#![allow(clippy::manual_async_fn)]

use std::future::Future;
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::pin::Pin;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ringline::{AsyncEventHandler, Config, ConfigBuilder, Connection, ListenerId, RinglineBuilder};

// ── Helpers ─────────────────────────────────────────────────────────────

fn test_config() -> Config {
    ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .sq_entries(64)
        .recv_buffer(16, 1024)
        .max_connections(16)
        .send_pool(16, 16384)
        .build()
        .expect("valid config")
}

/// Connect and read the server's greeting. `Ok(())` means the listener served.
fn probe(addr: SocketAddr) -> std::io::Result<()> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = [0u8; 2];
    stream.read_exact(&mut buf)?;
    assert_eq!(&buf, b"ok", "server sent something unexpected");
    Ok(())
}

/// A connect attempt that must not be served, because the port is bound and
/// not listening.
///
/// *How* it fails is platform-specific, and the difference is the point of
/// deferring `listen` rather than `accept`, so the assertion is specific per
/// platform rather than loosened to "some error".
///
/// Linux answers a SYN to a bound, non-listening port with RST, so the peer is
/// refused immediately — a readiness probe fails fast and cleanly. Darwin
/// drops the SYN instead, so the peer times out. Measured on Darwin 25.6 with
/// no ringline involved: a socket bound without `listen` times out, an unbound
/// port on the same host is refused, and the same socket connects once it
/// listens. The Linux half of this claim is asserted here so CI checks it.
fn assert_not_served(addr: SocketAddr, context: &str) {
    match TcpStream::connect_timeout(&addr, NOT_SERVED_TIMEOUT) {
        Ok(_) => panic!("{context}: connect succeeded on a listener that has not been released"),
        Err(e) => {
            #[cfg(target_os = "linux")]
            assert_eq!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused,
                "{context}: Linux must refuse a bound, non-listening port, got {e:?}"
            );
            #[cfg(not(target_os = "linux"))]
            assert!(
                matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::TimedOut
                ),
                "{context}: expected refused or timed out, got {e:?}"
            );
        }
    }
}

/// Long enough for a Linux RST to arrive, short enough that the three Darwin
/// call sites that wait it out do not dominate the suite.
const NOT_SERVED_TIMEOUT: Duration = Duration::from_millis(400);

/// Greets every connection with `ok` so a client can tell "served" from
/// "connected to something that never answers".
struct Greeter;

impl AsyncEventHandler for Greeter {
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, _rx) = conn.split();
            let _ = tx.send_nowait(b"ok");
            // Hold the connection open long enough for the client's read.
            let _ = ringline::sleep(Duration::from_millis(200)).await;
        }
    }

    fn create_for_worker(_id: usize) -> Self {
        Greeter
    }
}

// ── The gate opens when the handler says so ─────────────────────────────

static RELEASE: AtomicBool = AtomicBool::new(false);
static RELEASE_RESULT: OnceLock<String> = OnceLock::new();

/// Waits for the test to say go, then opens listener 0's gate.
struct ReleaseOnCue;

impl AsyncEventHandler for ReleaseOnCue {
    fn on_accept(&self, conn: Connection) -> impl Future<Output = ()> + 'static {
        async move {
            let (mut tx, _rx) = conn.split();
            let _ = tx.send_nowait(b"ok");
            let _ = ringline::sleep(Duration::from_millis(200)).await;
        }
    }

    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            while !RELEASE.load(Ordering::Acquire) {
                let _ = ringline::sleep(Duration::from_millis(5)).await;
            }
            let outcome = match ringline::begin_listening(ListenerId::from_index(0)) {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("ERR:{e}"),
            };
            RELEASE_RESULT.set(outcome).ok();
        }))
    }

    fn create_for_worker(_id: usize) -> Self {
        ReleaseOnCue
    }
}

#[test]
fn a_gated_listener_refuses_until_the_handler_releases_it() {
    // Port 0, resolved after launch. `free_port`-style probing binds a socket
    // and drops it before the runtime binds, and another test binary can take
    // the port in that window — which is how this file first made
    // `throughput.rs` fail with `AddrInUse`. Letting the kernel choose leaves
    // no window: the runtime holds the port from `bind(2)` onward.
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .defer_listen()
        .launch::<ReleaseOnCue>()
        .expect("launch");
    let addr = shutdown
        .bound_addr_of(ListenerId::from_index(0))
        .expect("bound address");

    // The port is held — nothing else can bind it — but not listening.
    assert!(
        std::net::TcpListener::bind(addr).is_err(),
        "a gated listener must still reserve its port"
    );
    assert_not_served(addr, "before release");

    RELEASE.store(true, Ordering::Release);

    // The handler polls on a 5 ms tick, so give it room without pinning the
    // test to that number.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut served = Err(std::io::Error::other("never attempted"));
    while Instant::now() < deadline {
        served = probe(addr);
        if served.is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        served.is_ok(),
        "listener did not serve after release: {served:?} (gate said {:?})",
        RELEASE_RESULT.get()
    );
    assert_eq!(RELEASE_RESULT.get().map(String::as_str), Some("OK"));

    shutdown.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
}

// ── Control: without `defer_listen` nothing changes ─────────────────────

/// The control the mechanism predicts *no* effect for. If this failed, the
/// test above would be measuring a broken launch rather than a working gate.
#[test]
fn an_ungated_listener_serves_as_soon_as_launch_returns() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .launch::<Greeter>()
        .expect("launch");
    let addr = shutdown
        .bound_addr_of(ListenerId::from_index(0))
        .expect("bound address");

    probe(addr).expect("an ungated listener must serve immediately");

    shutdown.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
}

// ── Only the deferred listener is gated ─────────────────────────────────

#[test]
fn defer_listen_applies_to_one_listener_not_the_process() {
    // The health-port case: one listener serving at once, one held back.
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .bind("127.0.0.1:0".parse().unwrap())
        .defer_listen()
        .launch::<Greeter>()
        .expect("launch");
    let open_addr = shutdown
        .bound_addr_of(ListenerId::from_index(0))
        .expect("bound address 0");
    let gated_addr = shutdown
        .bound_addr_of(ListenerId::from_index(1))
        .expect("bound address 1");

    probe(open_addr).expect("the ungated listener must serve");
    assert_not_served(gated_addr, "the gated listener");

    shutdown.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
}

// ── A zero port still resolves while gated ──────────────────────────────

#[test]
fn a_gated_listener_resolves_its_zero_port() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .defer_listen()
        .launch::<Greeter>()
        .expect("launch");

    // `bind(2)` assigns the port, so it is known before `listen(2)`.
    let resolved = shutdown
        .bound_addr_of(ListenerId::from_index(0))
        .expect("a gated TCP listener still reports its bound address");
    assert_ne!(resolved.port(), 0, "port 0 was not resolved");
    assert_not_served(resolved, "a zero-port gated listener");

    shutdown.shutdown();
    for h in handles {
        h.join().expect("worker panicked").expect("worker error");
    }
}

// ── Shutdown works with a gated listener present ────────────────────────

/// Shutting down a runtime whose listener was never released completes and
/// the workers join.
///
/// This does **not** prove that `shutdown` releases the gate. The acceptor
/// thread is detached, so one parked on a gate does not hold up `join()` on
/// the worker handles, and the listen fd is closed either way — verified by
/// mutation: removing `ShutdownHandle::shutdown`'s call to
/// `ListenGates::shutdown` leaves this test green. What that call prevents is
/// a stranded thread, and no observable API state reflects it. The mechanism
/// is covered by `acceptor::tests::a_gated_acceptor_exits_on_shutdown`.
#[test]
fn shutdown_terminates_a_listener_that_was_never_released() {
    let (shutdown, handles) = RinglineBuilder::new(test_config())
        .bind("127.0.0.1:0".parse().unwrap())
        .defer_listen()
        .launch::<Greeter>()
        .expect("launch");
    let addr = shutdown
        .bound_addr_of(ListenerId::from_index(0))
        .expect("bound address");

    assert_not_served(addr, "before shutdown");
    shutdown.shutdown();

    // Joined on a helper thread so a regression fails the test instead of
    // hanging the binary.
    let joiner = std::thread::spawn(move || {
        for h in handles {
            h.join().expect("worker panicked").expect("worker error");
        }
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !joiner.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        joiner.is_finished(),
        "shutdown did not release the acceptor parked on an unopened gate"
    );
    joiner.join().expect("joiner panicked");
}

// ── Misuse is reported, not ignored ─────────────────────────────────────

#[test]
fn defer_listen_before_any_bind_is_a_launch_error() {
    let result = RinglineBuilder::new(test_config())
        .defer_listen()
        .launch::<Greeter>();

    match result {
        Ok(_) => panic!("defer_listen() with no listener must not launch"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("defer_listen"),
                "the error must name the call that was misused, got: {msg}"
            );
        }
    }
}
