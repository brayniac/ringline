# Worker startup errors: report the typed cause, survive a panic

**Status:** approved design, 2026-09-12. Series PR 2 of
`docs/backpressured-sends-series-design.md` (worker startup rollback
diagnostics), redesigned against #360/#361 rather than ported from #318.

## Today

`RinglineBuilder::launch_inner` spawns one thread per worker. Each thread
reports its setup outcome on a bounded channel of `Result<(), ()>`; the
launching thread waits for one message per worker, and on the first `Err`
or disconnect calls `rollback_workers`, which sets the shutdown flag, wakes
every worker, joins every handle, and returns the **first joined thread's**
`Err` (in handle order), or nothing. `launch` returns that, or a generic
"worker setup failed".

Two gaps:

1. **A panic during startup loses its cause.** The thread never sends; the
   channel disconnects; rollback joins a `Err(panic payload)` which the
   `if let Ok(Err(error))` in `rollback_workers` drops. The caller gets
   "worker setup failed"; the payload survives only as the default panic
   hook's stderr line.
2. **The reporting worker's error is not necessarily the one returned.**
   `first_error` is the first handle in order that returned `Err`, which
   in a mixed failure can be a different worker than the one that reported.

#318 fixed both by sending a `String` on the channel. Since then #360 and
#361 made setup failures typed and actionable (`Error::RingSetup` names the
sysctl/seccomp cause, `Error::ResourceLimit` names the `ulimit`); a `String`
channel would flatten those. So:

## Decision

- The startup channel carries `Result<(), crate::error::Error>`. The three
  failure sites (pin_to_core, event-loop construction, `prepare_run`) send
  the error they hit and return a placeholder `Error::Io` from the thread
  ("startup failure reported to launch()") — `crate::error::Error` is not
  `Clone`, and after a reported failure the thread's return value is only
  ever read by `rollback_workers` as a fallback.
- The spawned closure wraps `worker_fn` in `catch_unwind`. A panic becomes
  `Error::Io(io::Error::other("ringline worker {id} panicked during
  startup: {payload}"))`, sent on a clone of the channel sender taken
  before `worker_fn` consumes the original, and returned from the thread.
  `worker_fn` itself is `AssertUnwindSafe`: nothing it borrows outlives the
  thread.
- `launch_inner`'s wait loop keeps the reported error
  (`Ok(Err(e))` → `Some(e)`) and, on a bare disconnect, falls back to
  `rollback_workers`' first joined error, then to the generic message.
  Rollback runs in every failure case, as today.
- `WorkerHandle`'s join result on the success path is unchanged.

## Tests (`ringline/src/worker.rs`, existing `startup_gate_tests` module)

- `startup_returns_the_reported_error_after_rollback`: the injected
  `worker_fn` sends `Err(Error::Io("reported setup failure"))` and returns
  a different `Err`; `launch` must surface the reported one and not the
  joined one.
- `worker_startup_panic_payload_is_preserved`: the injected `worker_fn`
  panics with a message; `launch` must fail with an error whose text
  contains the message and the worker id.
- The three existing tests' `Err(())` injections become typed errors.
  `panic_payload` helper unit-tested for `String`, `&'static str`, and
  other payloads.

## Verification

mio locally (worker.rs is backend-neutral apart from cfg'd eventfd
plumbing; the tests run on both platforms). One anvil VM check on the
validation host for the `has_io_uring` branches (`ensure_memlock_limit`,
`transfer_to_driver`) that the closure change touches, then Linux CI.

## Docs

CHANGELOG Fixed; series journal PR 2 bullet.
