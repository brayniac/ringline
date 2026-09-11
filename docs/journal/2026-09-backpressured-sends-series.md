# Result-aware receives and backpressured sends, landed as a series

- **Status:** open
- **Span:** 2026-09-10 → · PRs #367 (design), PR 1 of 9 (this PR) · unreleased (post-0.6.3)

## Goal

Land the work in ringline-rs/ringline#318 (@thinkingfish): a receive that
distinguishes EOF from transport error, and a FIFO-admitted, copy-once
bounded send, plus the internal guarantees underneath them. #318 stalled 41
commits behind `main` with a blocking finding (a bounded TLS send could
advance rustls state, discard the ciphertext, and retry — `bad_record_mac`
at the peer). The design is accepted; the branch is not. The decomposition
and the six deliberate departures from #318 are in
[`docs/backpressured-sends-series-design.md`](../backpressured-sends-series-design.md).

GO criterion for the series: PR 9 lands `send_backpressured` with the
end-to-end regression for the blocking finding (forced push failure after
TLS encryption fails the operation, closes the connection, peer sees no
record gap) green on both backends.

## What happened

- **Prerequisite — #368, mio close lifecycle.** PR 1's adversarial review
  found that mio never tore down a peer-first-closed or read-errored
  connection (`Closed` set directly at the read sites; `close_connection`
  early-returned on it). Fixed by unifying on io_uring's model rather than
  adding a mio-only flag: `Closed` has one setter, `close_pending` marks
  teardown requested, and mio's `finish_close` is now deferred until
  `pending_sends` drain. CI then showed io_uring does *not* give that
  window when the queue was empty at the FIN (`try_finalize_close` runs
  synchronously and commits the Close before the task polls; a post-EOF
  response is never delivered) — #371, to be fixed with PR 7's handler
  rework. **Closed the same day (#371 PR):** `close_connection`'s
  empty-queue branch now defers to the event loop's `pending_finalize_closes`
  drain instead of finalizing synchronously — a three-line change plus an
  io_uring unit test — and the two post-FIN-send tests run on both backends.
  Review also caught that the deferral test's 4 MiB send would exhaust the
  default 64-slot test copy pool on io_uring (one slot per 16 KiB chunk,
  taken synchronously) — the test now uses an 8 MiB pool and asserts the
  send outcome. Design: `docs/mio-close-lifecycle-design.md`. PR 3 (mio
  `shutdown_write` deferral) and PR 6 build on this.
- **Prerequisite — connection state model.** After #368/#371 the owner
  asked whether the state names matched TCP. They did not: `RecvMode::Closed`
  meant "read half finished", "close requested", and "nothing armed" at once.
  Split into `recv_arm` / `read: ReadHalf` / `lifecycle` with `recv_finished()`
  and `close_requested()` helpers, behaviour-preserving, verified on io_uring
  in an anvil VM on the validation host before the PR. Design:
  `docs/connection-state-model-design.md`. Follow-ups recorded there:
  half-close policy (should a peer FIN request teardown at all?), `WriteHalf`
  with PR 3, folding `active`/`recv_multishot_armed` after PR 7.
- **PR 1 — `with_data_result`.** `Executor` gains a generation-tagged
  `recv_errors` slot written by `fail_recv` at the five real socket-read
  failure sites (mio: TLS and plaintext read errors in
  `backend/mio/event_loop.rs`; io_uring: the fallback-recv `result < 0`
  branch, the multishot-recv errno branch, and the multishot-recvmsg errno
  branch under the `timestamps` feature in `backend/uring/event_loop.rs`).
  `WithDataResultFuture` wraps `WithDataFuture` and consults the slot, with
  the generation captured at construction, only when the inner future
  reports `0`. Departures 7 and 8 in the design doc: no pre-poll check of the
  slot (redundant — every `fail_recv` site sets `RecvMode::Closed` or calls
  `close_connection`, so the inner future reports `0` on the same poll), and
  the slot is not cleared on teardown (the generation tag makes that safe,
  and a poll that lands after teardown still learns the cause). Tests:
  four executor unit tests; `with_data_result_returns_ok_zero_on_clean_close`
  and `with_data_result_surfaces_tcp_reset` (client sets `SO_LINGER 0` so
  `close()` sends RST) in `ringline/tests/echo.rs`, both gated on the
  handler having actually accepted so they cannot race the acceptor thread.

## Outcome

Open.

## Lessons / open questions

- The io_uring sites cannot be type-checked on the macOS development host;
  Linux CI is the authority for those three edits.
- **Pre-existing hazard, found in review, not introduced here:** the
  `RecvMulti` user_data carries no generation, and `handle_recv_multi`
  validates a CQE only with `connections.get(conn_index).is_none()`.
  Ordinarily that is safe: `close_connection` submits `async_cancel` and
  `Close` together, `DEFER_TASKRUN` runs the cancel at submit, and the recv's
  terminal CQE after that is `ECANCELED`, which the handler returns on. The
  escape is `close_connection`'s `let _ = self.ring.submit_async_cancel(..)`
  failing on a full SQ (`backend/uring/driver.rs`): the uncancelled recv
  outlives the fixed-file close and a later `-ECONNRESET` could reach the
  error branch under a reused slot — before this PR that path did
  `wake_recv` + `close_connection` on the new occupant; now it would also
  store an error under the new occupant's generation. Fix candidates:
  encode the generation in `RecvMulti` user_data, or check
  `recv_multishot_armed` before the error branch. Tracked for a later PR in
  this series (PR 7 touches the same handlers).
