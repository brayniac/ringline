//! Behaviour preflight for incremental provided-buffer rings (#622).
//!
//! The 6.12.y incremental-buffer fixes ran through at least 6.12.81, so no
//! kernel version marks a kernel that behaves as the receive driver relies
//! on. Before a worker registers its TCP ring as incremental, it checks
//! that behaviour on the running kernel with a one-entry ring and a
//! loopback TCP pair (`docs/recv-incremental-ring-design.md`, "Selecting
//! the ring kind"). It runs on the worker's ring before anything else is
//! armed, since it reaps every completion the ring holds.

use std::io::Write;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use super::{Engine, RingKind};
use crate::backend::ProvidedBufRing;
use crate::backend::uring::abi::cqueue;
use crate::backend::uring::sqe::{Fd, Op, Sqe};
use crate::error::Error;

/// The group the preflight registers, which `Config` reserves.
const BGID: u16 = u16::MAX;
/// The preflight buffer's size.
const SIZE: u32 = 64;
/// The preflight receive's user_data.
const USER_DATA: u64 = u64::MAX - 1;
/// How long the preflight waits for one completion.
const WAIT: Duration = Duration::from_secs(1);

/// What the preflight found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncPreflight {
    /// The kernel behaves as the receive driver relies on.
    Passed,
    /// The kernel refuses incremental rings.
    Unsupported,
    /// The step that did not behave as relied on.
    Failed(&'static str),
}

/// Check the incremental-ring behaviour the receive driver relies on.
///
/// `Err` is a failure to run the check (the registration failing other
/// than with `EINVAL`, or a socket error), which fails the worker's startup
/// as any registration failure does.
pub(crate) fn inc_preflight<E: Engine>(engine: &mut E) -> Result<IncPreflight, Error> {
    if !engine
        .incremental_buffers()
        .map_err(|e| Error::buffer_registration(e, 0, None))?
    {
        return Ok(IncPreflight::Unsupported);
    }
    let mut ring = ProvidedBufRing::new(BGID, 1, SIZE).map_err(Error::Io)?;
    ring.set_incremental();
    engine.register_buf_ring(&ring, RingKind::Incremental)?;
    let result = match run(engine, &mut ring) {
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
            Ok(IncPreflight::Failed("no completion within 1 s"))
        }
        other => other.map_err(Error::Io),
    };
    // A failed unregister leaves the group registered on pages the kernel
    // has pinned, which is safe for the reason `register_buf_ring` gives.
    let _ = engine.unregister_buf_ring(BGID);
    result
}

fn run<E: Engine>(e: &mut E, ring: &mut ProvidedBufRing) -> std::io::Result<IncPreflight> {
    use IncPreflight::Failed;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut client = TcpStream::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept()?;
    client.set_nodelay(true)?;
    let base = ring.data_ptr(0, 0) as u64;

    // Two writes append at increasing offsets, the entry advancing in place.
    arm(e, &server)?;
    client.write_all(b"abc")?;
    if !delivered(wait(e)?, 3, true, true) {
        return Ok(Failed("first completion"));
    }
    ring.complete(0, 3, true);
    client.write_all(b"defg")?;
    if !delivered(wait(e)?, 4, true, true) {
        return Ok(Failed("second completion"));
    }
    ring.complete(0, 4, true);
    // Safety: 7 bytes were received into buffer 0, which is not posted again
    // until it is released below.
    if unsafe { std::slice::from_raw_parts(ring.data_ptr(0, 0), 7) } != b"abcdefg" {
        return Ok(Failed("data at the completions' offsets"));
    }
    if ring.entry(0) != (base + 7, SIZE - 7, 0) {
        return Ok(Failed("ring entry advance"));
    }

    // The completion that uses the buffer up clears F_BUF_MORE, and the next
    // data ends the arm with ENOBUFS.
    client.write_all(&[b'x'; SIZE as usize - 7])?;
    if !delivered(wait(e)?, SIZE as i32 - 7, false, true) {
        return Ok(Failed("exhausting completion"));
    }
    ring.complete(0, SIZE - 7, false);
    client.write_all(b"y")?;
    let (res, flags) = wait(e)?;
    if res != -libc::ENOBUFS || cqueue::more(flags) {
        return Ok(Failed("ENOBUFS after exhaustion"));
    }

    // Posted again and partly filled, the buffer stays posted at its used
    // length when the peer half-closes.
    ring.release_batch(&[0, 0, 0]);
    arm(e, &server)?;
    if !delivered(wait(e)?, 1, true, true) {
        return Ok(Failed("completion after reposting"));
    }
    ring.complete(0, 1, true);
    client.shutdown(Shutdown::Write)?;
    let (res, flags) = wait(e)?;
    if res != 0 || cqueue::buffer_select(flags).is_some() || cqueue::more(flags) {
        return Ok(Failed("EOF completion"));
    }
    if ring.entry(0) != (base + 1, SIZE - 1, 0) {
        return Ok(Failed("ring entry at EOF"));
    }
    ring.release_batch(&[0]);
    Ok(IncPreflight::Passed)
}

/// Whether `(res, flags)` delivered `len` bytes into buffer 0 with the given
/// `F_BUF_MORE` and `F_MORE`.
fn delivered((res, flags): (i32, u32), len: i32, buf_more: bool, more: bool) -> bool {
    res == len
        && cqueue::buffer_select(flags) == Some(0)
        && cqueue::buf_more(flags) == buf_more
        && cqueue::more(flags) == more
}

fn arm<E: Engine>(e: &mut E, server: &TcpStream) -> std::io::Result<()> {
    let sqe = Sqe::new(
        Op::RecvMulti {
            fd: Fd::Raw(server.as_raw_fd()),
            buf_group: BGID,
        },
        USER_DATA,
    );
    // Safety: a multishot recv references no caller memory.
    unsafe { e.push(&sqe) }
}

/// The next completion of the preflight receive, or `TimedOut` after
/// `WAIT`.
fn wait<E: Engine>(e: &mut E) -> std::io::Result<(i32, u32)> {
    let deadline = Instant::now() + WAIT;
    let mut out = Vec::new();
    loop {
        e.submit_and_get_events()?;
        e.reap(&mut out);
        if let Some(&(_, res, flags)) = out.iter().find(|c| c.0 == USER_DATA) {
            return Ok((res, flags));
        }
        out.clear();
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "no preflight completion within 1 s",
            ));
        }
        std::thread::yield_now();
    }
}

#[cfg(all(test, uring_engine))]
mod tests {
    use super::*;
    use crate::backend::uring::engine::ActiveEngine;
    use crate::backend::uring::ring::is_memlock_enomem;
    use crate::config::ConfigBuilder;

    fn engine() -> ActiveEngine {
        let config = ConfigBuilder::new().workers(1).build().expect("config");
        // Up to 5 s for earlier rings' memlock charge to be released (#589).
        for _ in 0..50 {
            match ActiveEngine::setup(&config) {
                Err(e) if is_memlock_enomem(&e) => std::thread::sleep(Duration::from_millis(100)),
                result => return result.expect("engine"),
            }
        }
        ActiveEngine::setup(&config).expect("engine")
    }

    /// The preflight passes on a kernel with incremental rings, reports
    /// `Unsupported` on one without, and leaves no completion behind.
    #[test]
    fn the_preflight_matches_the_kernel() {
        let mut e = engine();
        let inc = e.incremental_buffers().expect("probe");
        let start = Instant::now();
        let found = inc_preflight(&mut e).expect("preflight");
        eprintln!("preflight: {found:?} in {:?}", start.elapsed());
        let expected = if inc {
            IncPreflight::Passed
        } else {
            IncPreflight::Unsupported
        };
        assert_eq!(found, expected);
        e.submit_and_get_events().expect("enter");
        let mut left = Vec::new();
        e.reap(&mut left);
        assert!(left.is_empty(), "{left:?}");
    }
}
