# Result-aware receives and backpressured sends, as a series

**Status:** approved design, 2026-09-10.
**Origin:** ringline-rs/ringline#318 by @thinkingfish. The design there is
accepted; the branch is stale (41 commits behind, conflicts in the io_uring
send path and the TLS module split) and carries one blocking correctness
finding. This document decomposes that work into nine independently
reviewable PRs written against current `main`, and records where the series
deliberately departs from #318.

## Goal

Land everything #318 set out to do:

- `ConnCtx::with_data_result`: a receive that distinguishes a clean peer close
  (`Ok(0)`) from a transport error, without changing `with_data`.
- `ConnCtx::send_backpressured`: a worker-local future that waits until one
  complete logical message can be admitted to the copied-send pool, then
  copies once, submits, and resolves with the exact result of that one
  operation.
- The internal guarantees those APIs rest on: copied sends reserve
  transactionally (no transmitted prefix on failure), one completion belongs
  to one operation across partial writes, cancellation, half-close and slot
  reuse, and TLS admission is decided before rustls state is mutated.
- Two adjacent fixes #318 carried: mio `shutdown_write` after a partial write,
  and worker-startup rollback diagnostics.

## Departures from #318

These are design decisions, not porting details. Each PR that touches one of
them cites this section.

1. **A full submission queue is a terminal error.** #318 changed
   `Ring::push_sqe` to return `WouldBlock` so the bounded future would park
   and retry. The retry re-runs the whole logical send, including TLS
   encryption, after rustls has already advanced its record sequence for the
   discarded ciphertext; the peer sees a sequence gap and fails with
   `bad_record_mac` (review on #318, 2026-08-27). The error kind stays
   `Other`. Once a bounded send has committed anything (pool slots, a
   transmitted prefix, rustls state), any later failure fails *that
   operation* and closes the connection through the `close_submitted` /
   `pending_finalize_closes` machinery from #328. Nothing parks after
   mutation.
2. **The TLS ciphertext bound gates only bounded sends.** The bound is
   conservative (record header plus rustls' maximum overhead per 16 KiB
   record, rounded to whole slots). #318 applied it to `send`/`send_nowait`
   too, which makes plain TLS sends fail in a band of pool occupancy where
   they succeed today. Plain sends keep their current admission behaviour.
3. **Transactional reservation without allocation.** #318 built a
   `Vec<(slot, ptr, len)>` and a `Vec<BuiltSend>` per copied send. The series
   reserves the slot count first and then streams `data.chunks()` into the
   reserved slots exactly as today. No per-send heap allocation, including
   on the single-chunk path.
4. **On mio, a real completion overwrites the synthetic abort.** Teardown
   marks bounded operations `Err(ConnectionAborted)` before the final
   `flush_sends` can report `Ok(len)`, so a handler closing right after a
   bounded send cannot tell written from lost. The fix lets a real result
   overwrite the synthetic one. The executor-before-driver teardown order is
   guarding slot-reuse safety and does not change.
5. **Coalesced entries are handled explicitly.** On io_uring a coalesced send
   entry's `pool_slot` is the `u16::MAX` sentinel. The bounded-id lookup must
   not read or release it as a slot.
6. **Both new futures are documented** (admission semantics, cancellation,
   error kinds). #318 moved `SendFuture`'s comment onto the new future and
   left `WithDataResultFuture` bare.
7. **`WithDataResultFuture` does not pre-check the error slot.** #318's
   wrapper looked at the slot before polling the inner future. Redundant:
   every `fail_recv` site marks the read half finished and calls
   `close_connection`, so the inner future reports `0` on the same poll and
   the wrapper consults the slot then, with the generation it captured at
   construction.
8. **The recv error slot is not cleared on teardown.** The generation tag
   makes clearing unnecessary (a reused slot cannot hand the previous
   occupant's error to the new connection), and leaving the entry lets a
   poll that lands after teardown still learn the cause instead of a bare
   `0`. Last writer wins.

## The series

Sizes are approximate net lines from the #318 diff, re-derived on `main`.

| # | PR | Needs | Size | Backend-sensitive |
|---|----|-------|------|-------------------|
| 1 | `with_data_result` | – | ~120 | no |
| 2 | Worker startup rollback diagnostics | – | ~140 | no |
| 3 | mio `shutdown_write` deferral | – | ~40 | mio |
| 4 | Transactional copied-send reservation + ring push-failure test hook | – | ~80 | both |
| 5 | Executor send-capacity FIFO (no callers) | – | ~300 | no |
| 6 | mio completion identity + mio bounded-send driver path | 4, 5 | ~200 | mio |
| 7 | io_uring completion identity, on top of #325/#328 | 4, 5 | ~300 | io_uring |
| 8 | TLS pre-mutation ciphertext bound, both engines | 4 | ~120 | both |
| 9 | Public `send_backpressured`, tests, docs, diagram, changelog | 5–8 | ~250 + tests | both |

PRs 1–5 have no ordering between them. 6–9 are sequential on their
prerequisites. mio precedes io_uring because it is testable on the
development machine and simpler, and because the io_uring PR is written
against current `main` rather than ported from #318.

### PR 1 — `with_data_result`

`ConnCtx::with_data_result<F: FnMut(&[u8]) -> ParseResult>(&self, f: F) ->
WithDataResultFuture<F>`, resolving to `io::Result<usize>`: `Ok(n)` bytes
consumed, `Ok(0)` clean EOF, `Err(e)` for a non-`WouldBlock` socket error.
Executor gains `recv_errors: Vec<Option<(generation, io::Error)>>`,
`fail_recv`, `take_recv_error`. Both event loops call `fail_recv` where they
currently `wake_recv` on a recv error (three sites on io_uring, two on mio).
`with_data` is unchanged. Re-export `WithDataResultFuture`.
Departures 7 and 8 apply. Tests: `with_data_result_returns_ok_zero_on_clean_close`
and `with_data_result_surfaces_tcp_reset` (peer sets `SO_LINGER 0` so `close()`
sends RST), both gated on the handler having accepted.
Changelog: Added.

### PR 2 — Worker startup rollback diagnostics

`worker.rs`: startup returns `Result<(), String>`; `catch_unwind` around the
worker body with a `panic_payload` helper; a setup error from one worker is
propagated with its cause instead of surfacing as a bare join failure after
transactional rollback. Two tests. Changelog: Fixed. Rebases over #361's
MEMLOCK changes without conflict.

### PR 3 — mio `shutdown_write` deferral

Today mio `shutdown_write` drains pending sends through one non-blocking
`write_all`; a partial write followed by `WouldBlock` discards the rest and
issues the FIN early. Change: set `ConnSendState::shutdown_pending`,
re-register WRITABLE, and let `flush_sends` issue `Shutdown::Write` once the
queue is empty. Test uses plain `send` (the #318 test used the future):
`mio_half_close_waits_for_partial_send_to_finish_before_fin`. Changelog:
Fixed (mio).

### PR 4 — Transactional copied-send reservation

`SendCopyPool::reserve_slots(n) -> Option<SlotReservation>`; the reservation
either receives every chunk or is dropped and releases. `free_count` and
`slot_count` become `pub(crate)` outside `cfg(test)`. io_uring
`DriverCtx::send` reserves `ceil(len / slot_size)` slots before copying, then
streams chunks as today (departure 3). Ring gains
`#[cfg(test)] force_push_failures(n)`; the error kind is unchanged
(departure 1). Unit tests on the pool. No changelog line (no observable
change on success paths; on failure the observable change is "no partial
send", which PR 9's changelog line describes).

### PR 5 — Executor send-capacity FIFO

`BoundedSendId(u64)` newtype (monotonic per worker). `SendCapacityQueue` in
`runtime/mod.rs`: `enqueue(conn, id, required_slots, waker)`, `turn(id) ->
bool` (is this id the head and is capacity available), `mark_submitted(id)`,
`complete(id, io::Result<u32>)`, `take_result(id)`, `cancel(id)` (drop while
waiting: promote the next head), `abandon(id)` (drop while submitted:
result is discarded when it lands), `remove_connection(conn)` (fail waiting
entries, wake the head even if another connection released the permits),
`wake_head()`. `Executor::collect_wakeups` drains the queue's pending wakes.
No caller in this PR; five unit tests cover promotion, cancel-at-head,
abandon-while-submitted, removal wakes an unrelated head, and take-after-
complete. No changelog line.

### PR 6 — mio completion identity and bounded-send path

`mio/driver.rs`: `PendingSend` struct replacing the tuple, carrying
`Option<BoundedSendId>`; `bounded_send_completions` drained by the event loop
after every `flush_sends`; `clear_pending_sends` fans out the ids it drops
(the empty-iovec bail-out included); `pending_bounded_send_ids`. mio
`DriverCtx::send_bounded(conn, data, id)` reserves via PR 4 and queues.
Departure 4 applied at the teardown site in `mio/event_loop.rs`.
Test: `canceled_submitted_backpressured_send_cannot_complete_the_next_send`
is deferred to PR 9 (needs the future); this PR's tests are `#[cfg(test)]`
unit tests in the mio driver and event loop that drive `send_bounded`
directly and assert the completion fan-out. No changelog line.

### PR 7 — io_uring completion identity

Re-derived on `main`, not ported. Pool-slot sends carry the bounded id in
`SendCopyPool::slot_bounded_send_id`; slab-backed entries record it in the
entry. Every send-family CQE handler that today calls `wake_send` (send,
POLLOUT resubmit, coalesced, TLS send, the three retry drains) calls
`complete_bounded_send` with the exact result after the #325 identity check
and the #328 `close_submitted` gate. `bounded_send_failures: VecDeque<(id,
io::Error)>` for failures raised outside a CQE (SQ full at submit); drained
after each CQE batch. SQ full after a committed prefix: release remaining
resources, fail the operation, and request close through
`pending_finalize_closes` (departure 1). Coalesced sentinel handled
(departure 5). Tests: `submit_next_queued_sq_full_closes_torn_bounded_send`
via the PR 4 hook, plus one CQE-misattribution regression. Verified by Linux
CI and an anvil VM job. No changelog line.

### PR 8 — TLS pre-mutation ciphertext bound

`TlsTable::ciphertext_capacity(conn, plaintext_len) -> usize` in slots:
`ceil(plaintext / max_fragment) * (header + max_fragment + max_overhead)`,
using the connection's configured fragment size, implemented for the
buffered engine and the unbuffered engine (whose whole-record emission may
justify a tighter bound; if so, document it, do not share one constant).
`TlsConn.max_fragment_size` recorded at `create`/`create_client`. mio
`encrypt_for_send_mio_bounded`: reserve `capacity` slots, encrypt, release
the unused tail. io_uring bounded TLS sends check `free_count() >= capacity`
*before* `encrypt_to_sends`. Plain sends are untouched (departure 2). Three
unit tests against real rustls records, including a fragment-size override.
No changelog line.

### PR 9 — Public `send_backpressured`

`ConnCtx::send_backpressured<'a>(&self, data: &'a [u8]) ->
BackpressuredSendFuture<'a>`, resolving to `io::Result<u32>`. Errors:
`InvalidInput` when the message can never fit the pool (checked before
enqueue), `ConnectionAborted` on teardown while waiting or submitted,
`BrokenPipe` after half-close. Construction is lazy; an unpolled drop is
inert; the first poll registers the polling task and re-registers if the
future moves between tasks. Once `turn` is true the future calls
`send_bounded` exactly once and then only awaits `take_result`. Re-export
`BackpressuredSendFuture`; both futures documented (departure 6).
`shutdown_write` fails waiting entries for that connection.
Tests (from #318, all backends unless noted):
`backpressured_send_waits_for_pool_capacity_without_duplication`,
`..._construction_is_lazy_and_unpolled_drop_is_inert`,
`..._registers_the_first_polling_task_after_move`,
`..._refreshes_owner_after_first_poll_move`,
`..._rejects_oversize_before_writing`,
`shutdown_drops_parked_backpressured_send_without_hanging`,
`canceled_submitted_backpressured_send_cannot_complete_the_next_send`,
`mio_half_close_completes_submitted_send_and_cancels_capacity_waiter` (mio),
and the end-to-end regression for departure 1: forced push failure after
TLS encryption on a bounded send fails the operation, closes the
connection, and the peer observes no record gap. Docs: `architecture.md`
section, `request-flow.svg` panel, `tools/doc-diagrams` needle checks.
Changelog: Added (both APIs are user-visible here; PR 1's line already
exists), plus a Fixed line for "copied sends can no longer transmit a prefix
and then fail".

## Verification

- Every PR: `cargo fmt`, clippy `-D warnings` on default and `force-mio`,
  `cargo test --all` on both, per `CLAUDE.md`. The `tls-unbuffered` feature
  is included for PRs 8 and 9.
- mio is verified on the development machine. io_uring-sensitive PRs (4, 7,
  8, 9) rely on Linux CI and an anvil VM job (custom-skills `vm-job`) before
  merge; never on a direct hypervisor run.
- The io_uring PR is reviewed against `docs/send-completion-design.md` and
  the Domain Invariants list in `CLAUDE.md`, invariants 1–5 in particular.

## Process

- One worktree and branch per PR, from current `upstream/main`; push to the
  `brayniac` fork, open against `ringline-rs/ringline` with the fork head.
- Each PR description credits #318 and @thinkingfish as the origin of the
  design and links this document.
- #318 is closed with a comment linking the series once PR 1 is open.
- Changelog lines land per PR under `Unreleased` as noted above; internal
  PRs carry none.
- A `docs/journal` entry is opened with PR 1 and updated as PRs merge, so the
  departures above and any measured findings (the TLS bound's cost, the
  allocation-free reservation) have a durable record.
