//! Behaviour preflight for incremental provided-buffer rings (#622).
//!
//! The 6.12.y incremental-buffer fixes ran through at least 6.12.81, so no
//! kernel version marks a kernel that behaves as the receive driver relies
//! on. Before a worker registers its TCP ring as incremental, it checks
//! that behaviour on the running kernel with a one-entry ring and an
//! `AF_UNIX` stream socketpair, which needs no network configuration
//! (`docs/recv-incremental-ring-design.md`, "Selecting the ring kind"). It
//! runs on the worker's ring before anything else is armed, since it reaps
//! every completion the ring holds.

use std::io::Write;
use std::net::Shutdown;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use super::{Engine, RingKind};
use crate::backend::ProvidedBufRing;
use crate::backend::uring::abi::cqueue;
use crate::backend::uring::sqe::{Fd, Op, Sqe};
use crate::error::Error;
use crate::metrics::recv_preflight as step;

/// The group the preflight registers, which `Config` reserves.
const BGID: u16 = u16::MAX;
/// The preflight buffer's size.
const SIZE: u32 = 64;
/// The preflight receive's user_data.
const USER_DATA: u64 = u64::MAX - 1;
/// The user_data of the cancel that tears a live preflight receive down.
const CANCEL_USER_DATA: u64 = u64::MAX - 2;
/// How long the preflight waits for one completion.
const WAIT: Duration = Duration::from_secs(1);

/// What the preflight found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncPreflight {
    /// The kernel behaves as the receive driver relies on.
    Passed,
    /// The kernel refuses incremental rings.
    Unsupported,
    /// The step that did not behave as relied on: a
    /// `metrics::recv_preflight` slot.
    Failed(usize),
}

/// Check the incremental-ring behaviour the receive driver relies on.
///
/// `Err` is the `incremental_buffers` probe failing other than with
/// `EINVAL`, the preflight ring's registration failing, or a receive the
/// preflight could not cancel within 1 s; each fails the worker's startup.
/// A socketpair or I/O failure is reported as a failed step, which selects
/// plain rings.
pub(crate) fn inc_preflight<E: Engine>(engine: &mut E) -> Result<IncPreflight, Error> {
    if !engine.incremental_buffers().map_err(|e| {
        Error::BufferRegistration(format!(
            "incremental provided buffer ring probe (bgid {BGID}): {e}"
        ))
    })? {
        return Ok(IncPreflight::Unsupported);
    }
    let Ok((mut client, server)) = UnixStream::pair() else {
        return Ok(IncPreflight::Failed(step::SOCKETPAIR));
    };
    let mut ring = ProvidedBufRing::new(BGID, 1, SIZE).map_err(Error::Io)?;
    ring.set_incremental();
    engine.register_buf_ring(&ring, RingKind::Incremental)?;
    let mut armed = false;
    let mut reaped = Reaped::default();
    let result = match run(
        engine,
        &mut ring,
        &mut client,
        &server,
        &mut reaped,
        &mut armed,
    ) {
        Ok(found) => Ok(found),
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
            Ok(IncPreflight::Failed(step::TIMEOUT))
        }
        Err(_) => Ok(IncPreflight::Failed(step::IO_ERROR)),
    };
    // A completion that ends the arm may already be reaped and unconsumed.
    armed &= reaped.0.iter().all(|&(_, flags)| cqueue::more(flags));
    let disarmed = !armed || disarm(engine);
    let unregistered = engine.unregister_buf_ring(BGID).is_ok();
    if !disarmed || !unregistered {
        // The kernel may still reach the ring's entry, which points into the
        // ring's buffer; leak both rather than free memory it can write.
        std::mem::forget(ring);
    }
    if !disarmed {
        // Its last completion would otherwise reach the event loop.
        return Err(Error::RingSetup(
            "the incremental-ring preflight could not cancel its receive within 1 s".into(),
        ));
    }
    result
}

// Test-only: make the preflight report `step` as failed when it reaches
// that step, with its receive still armed.
#[cfg(test)]
thread_local! {
    static FAIL_AT: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Whether a test asked the preflight to fail at `at`.
fn injected(at: usize) -> bool {
    #[cfg(test)]
    {
        FAIL_AT.with(|f| f.get()) == Some(at)
    }
    #[cfg(not(test))]
    {
        let _ = at;
        false
    }
}

fn run<E: Engine>(
    e: &mut E,
    ring: &mut ProvidedBufRing,
    client: &mut UnixStream,
    server: &UnixStream,
    reaped: &mut Reaped,
    armed: &mut bool,
) -> std::io::Result<IncPreflight> {
    use IncPreflight::Failed;
    let base = ring.data_ptr(0, 0) as u64;

    // Write, reap, write, reap: the completions append at increasing
    // offsets, and the entry advances in place.
    arm(e, server)?;
    *armed = true;
    if injected(step::APPEND) {
        return Ok(Failed(step::APPEND));
    }
    client.write_all(b"abc")?;
    if !delivered(wait(e, reaped, armed)?, 3, true, true) {
        return Ok(Failed(step::APPEND));
    }
    ring.complete(0, 3, true);
    client.write_all(b"defg")?;
    if !delivered(wait(e, reaped, armed)?, 4, true, true) {
        return Ok(Failed(step::APPEND));
    }
    ring.complete(0, 4, true);
    // Safety: 7 bytes were received into buffer 0, which is not posted again
    // until it is released below.
    if unsafe { std::slice::from_raw_parts(ring.data_ptr(0, 0), 7) } != b"abcdefg" {
        return Ok(Failed(step::OFFSETS));
    }
    if ring.entry(0) != (base + 7, SIZE - 7, 0) {
        return Ok(Failed(step::OFFSETS));
    }

    // More than the space left: the completion delivers exactly the space
    // left with F_BUF_MORE clear, and the excess ends the arm with ENOBUFS.
    const EXCESS: u32 = 5;
    if injected(step::EXHAUSTION) {
        return Ok(Failed(step::EXHAUSTION));
    }
    client.write_all(&[b'x'; (SIZE - 7 + EXCESS) as usize])?;
    if !delivered(wait(e, reaped, armed)?, (SIZE - 7) as i32, false, true) {
        return Ok(Failed(step::EXHAUSTION));
    }
    ring.complete(0, SIZE - 7, false);
    let (res, flags) = wait(e, reaped, armed)?;
    if res != -libc::ENOBUFS || cqueue::more(flags) {
        return Ok(Failed(step::EXHAUSTION));
    }

    // Posted again and re-armed, the excess lands at offset 0, and a
    // half-close leaves the buffer posted at its used length.
    ring.release_batch(&[0, 0, 0]);
    arm(e, server)?;
    *armed = true;
    if injected(step::REPOST) {
        return Ok(Failed(step::REPOST));
    }
    if !delivered(wait(e, reaped, armed)?, EXCESS as i32, true, true) {
        return Ok(Failed(step::REPOST));
    }
    ring.complete(0, EXCESS, true);
    if injected(step::EOF) {
        return Ok(Failed(step::EOF));
    }
    client.shutdown(Shutdown::Write)?;
    let (res, flags) = wait(e, reaped, armed)?;
    if res != 0 || cqueue::buffer_select(flags).is_some() || cqueue::more(flags) {
        return Ok(Failed(step::EOF));
    }
    if ring.entry(0) != (base + EXCESS as u64, SIZE - EXCESS, 0) {
        return Ok(Failed(step::EOF));
    }
    ring.release_batch(&[0]);
    Ok(IncPreflight::Passed)
}

/// Cancel a live preflight receive and reap its last completion and the
/// cancel's, so neither reaches the event loop. Returns whether both were
/// reaped within `WAIT`.
fn disarm<E: Engine>(e: &mut E) -> bool {
    let cancel = Sqe::new(Op::Cancel { target: USER_DATA }, CANCEL_USER_DATA);
    // Safety: a cancel references no caller memory.
    if unsafe { e.push(&cancel) }.is_err() {
        return false;
    }
    let deadline = Instant::now() + WAIT;
    let mut out = Vec::new();
    let (mut ended, mut cancelled) = (false, false);
    while !(ended && cancelled) && Instant::now() < deadline {
        if e.submit_and_get_events().is_err() {
            return false;
        }
        e.reap(&mut out);
        for &(ud, _, flags) in &out {
            ended |= ud == USER_DATA && !cqueue::more(flags);
            cancelled |= ud == CANCEL_USER_DATA;
        }
        out.clear();
        std::thread::yield_now();
    }
    ended && cancelled
}

/// Whether `(res, flags)` delivered `len` bytes into buffer 0 with the given
/// `F_BUF_MORE` and `F_MORE`.
fn delivered((res, flags): (i32, u32), len: i32, buf_more: bool, more: bool) -> bool {
    res == len
        && cqueue::buffer_select(flags) == Some(0)
        && cqueue::buf_more(flags) == buf_more
        && cqueue::more(flags) == more
}

fn arm<E: Engine>(e: &mut E, server: &UnixStream) -> std::io::Result<()> {
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

/// Completions reaped but not yet consumed: one reap can return several
/// of the preflight receive's completions.
#[derive(Default)]
struct Reaped(std::collections::VecDeque<(i32, u32)>);

/// The next completion of the preflight receive, in order, or `TimedOut`
/// after `WAIT`. Clears `armed` when the completion ends the arm.
fn wait<E: Engine>(
    e: &mut E,
    reaped: &mut Reaped,
    armed: &mut bool,
) -> std::io::Result<(i32, u32)> {
    let deadline = Instant::now() + WAIT;
    let mut out = Vec::new();
    loop {
        if let Some((res, flags)) = reaped.0.pop_front() {
            if !cqueue::more(flags) {
                *armed = false;
            }
            return Ok((res, flags));
        }
        e.submit_and_get_events()?;
        e.reap(&mut out);
        reaped.0.extend(
            out.drain(..)
                .filter(|c| c.0 == USER_DATA)
                .map(|(_, res, flags)| (res, flags)),
        );
        if reaped.0.is_empty() {
            if Instant::now() >= deadline {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "no preflight completion within 1 s",
                ));
            }
            std::thread::yield_now();
        }
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

    /// A preflight that fails with its receive armed cancels it: nothing is
    /// left to reap, and the preflight group can be registered again.
    #[test]
    fn a_failed_step_leaves_nothing_behind() {
        for at in [step::APPEND, step::EXHAUSTION, step::REPOST, step::EOF] {
            let mut e = engine();
            if !e.incremental_buffers().expect("probe") {
                return;
            }
            FAIL_AT.with(|f| f.set(Some(at)));
            let found = inc_preflight(&mut e);
            FAIL_AT.with(|f| f.set(None));
            assert_eq!(found.expect("preflight"), IncPreflight::Failed(at));
            e.submit_and_get_events().expect("enter");
            let mut left = Vec::new();
            e.reap(&mut left);
            assert!(left.is_empty(), "step {at}: {left:?}");
            let again = ProvidedBufRing::new(BGID, 1, SIZE).expect("ring");
            e.register_buf_ring(&again, RingKind::Incremental)
                .unwrap_or_else(|err| panic!("step {at}: group still registered: {err}"));
            e.unregister_buf_ring(BGID).expect("unregister");
        }
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
