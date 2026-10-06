# Send completion slots

Every awaited send owns an entry in a per-worker completion table. The
driver settles that entry when the send it names completes, and the
send's future resolves from it. This replaces the per-connection send
waiter (`Executor::send_waiters`, the `IoResult::Send` slot in
`io_results`, and `wake_send`).

Issue: #617.

## Why the per-connection waiter does not work

A connection has one send waiter. Any send completion on the connection
wakes it, whichever send completed. On io_uring an awaited `send()` queued
behind a `send_nowait()` resolves with the `send_nowait`'s byte count, and
the awaited send's own completion then finds no waiter. The same happens
behind an unawaited chain, a direct echo, a `forward_recv_buf`, or a
bounded send. Two awaited sends in flight at once on one connection share
the waiter, so only one of them is woken.

Bounded sends (`send_backpressured`) do not have this problem. Each one
has a `BoundedSendId` that rides the resource its completion releases, and
the completion settles exactly that operation. This design extends that
mechanism to every awaited send.

## The table

`runtime::send_completion::SendCompletions`, owned by the `Executor`.

- A `SendId` is a slot index and a generation, 32 bits each. A slot's
  generation advances each time the slot is freed, so a stale id never
  matches a reused slot.
- Each live slot holds the target connection index, the owning task id,
  and a state:

| State | Meaning |
|---|---|
| `Waiting` | A bounded send waiting for admission (in the capacity FIFO). |
| `InFlight` | Submitted; the driver owns the operation. |
| `Done(result)` | The driver reported. Waits for the future to take it. |
| `Aborted(err)` | Teardown recorded this before the driver reported. A driver result overwrites it. |
| `Abandoned` | The future was dropped while the operation was in flight. The driver's completion frees the slot. |

Lookups are by index, so `complete`, `take_result`, `set_owner` and
`cancel` are O(1). `remove_connection` scans the live slots once per
connection teardown.

The state rules are the ones `send_capacity` already has for bounded
sends, unchanged: a driver result wins over a teardown abort, the first
driver result wins over a second, and teardown drops the entries owned by
the connection's own task (its future is gone and nothing can take them).

The bounded-send FIFO (`SendCapacityQueue`) keeps only admission: the
ordered list of waiting ids and the copy-pool slots each needs. The task
to wake and the state live in the table.

## What carries the id

The id rides the resource whose completion ends the logical send,
together with the length the send reports on success.

| Send | Carrier |
|---|---|
| `send`, `send_backpressured`, copy-only batch (io_uring) | End-of-send pool slot (`SendCopyPool`), lifted onto the coalesced slab entry when the run is coalesced |
| TLS send (io_uring) | The final ciphertext chunk's pool slot (`OpTag::Send`) |
| Zero-copy batch (io_uring) | The `SendMsgZc` slab entry |
| `forward_held` (io_uring) | The recv-forward slab entry |
| `send_chain` (io_uring) | The chain's `ChainState` |
| Any awaited send (mio) | The last `PendingSend` the call queued |

Sends nothing awaits (`send_nowait`, `send_chain_nowait`, direct echo,
`forward_recv_buf`, TLS handshake and alert records) carry no id, and
their completions settle nothing.

## The reported length

A send settles with the length its caller passed: the plaintext length
under TLS, the sum of the parts for a batch, the chain's total. This is
the number bounded sends already report. Under TLS a plain `send()` used
to report the ciphertext bytes of its records, which no caller can relate
to what it sent.

## Failure

- The in-flight operation fails: its id settles with the errno
  (`WriteZero` for a zero-byte completion).
- A failure, close or give-up drains the connection's queue: every id on
  a drained entry settles `ConnectionAborted`.
- A completion that arrives after `close_submitted`: its id settles
  `ECANCELED`.
- Connection teardown: `Executor::remove_connection` records a provisional
  `Aborted(ConnectionAborted)` for every entry on that connection.
- Worker shutdown: the table is dropped with the executor.

Every disposal path that releases a carrier must take its id first. The
copy pool's `release` debug-asserts that the slot no longer carries one;
the slab entry and the chain state get the same check.

## The futures

`SendFuture` holds either a `SendId` or a result known at submission (an
empty `forward_held` resolves `Ok(0)` without a send). Polling takes the
result or records the polling task as the owner. Dropping it cancels the
id: the entry becomes `Abandoned`, and the send itself continues.

Two awaited sends on one connection each resolve with their own result.

## What goes away

- `Executor::send_waiters`, `IoResult::Send`, `Executor::wake_send`.
- `ConnSendState::acked_bytes` (the carried length replaces the
  accumulated wire count).
- mio's `PendingSend::notify_len`, `send_completions` and
  `DriverCtx::mark_last_send_awaited`.
- The `bounded_` prefix: `BoundedSendId` becomes `SendId`, and
  `take_bounded_send` / `set_bounded_send` / `bounded_send_completions`
  become `take_send_id` / `set_send_id` / `send_completions`.

## Landing

1. The table. `SendCompletions` replaces `SendCapacityQueue::submitted`,
   and `BoundedSendId` becomes `SendId`. No behaviour change.
2. Awaited sends. Every awaited API takes a `SendId`, every carrier
   carries one, and the per-connection send waiter is removed, on both
   backends. Fixes #617.
