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

- **PR 1 — `with_data_result`.** `Executor` gains a generation-tagged
  `recv_errors` slot written by `fail_recv` at the five real socket-read
  failure sites (mio: TLS and plaintext read errors in
  `backend/mio/event_loop.rs`; io_uring: the fallback-recv `result < 0`
  branch, the multishot-recv errno branch, and the multishot-recvmsg errno
  branch under the `timestamps` feature in `backend/uring/event_loop.rs`).
  `WithDataResultFuture` wraps `WithDataFuture` and consults the slot, with
  the generation captured at construction, only when the inner future
  reports `0`. Two departures from #318's version: no pre-poll check of the
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
