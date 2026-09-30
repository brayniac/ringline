# Deferred listen: a per-listener readiness gate

- **Status:** **open** — intent landed before building, per this journal's own rule
- **Span:** 2026-09-30 → · issue #534 · this PR · targets 0.7.0

Recording the design before implementing, for two reasons. The first is that the
public documentation is currently wrong about this, so there is a correction to
make whether or not the feature is built. The second is that reading the listener
setup changed the design: most of the mechanism already exists in the tree for
one accept mode, and the issue's option list was written without that.

## Goal

A server that must do work before it can serve — warm a connection pool, load
config, run migrations — should be able to hold its port without accepting on
it, and start serving when it says so. Per listener, so a health port and a data
port can differ.

## What happens today

`RinglineBuilder::launch()` binds, listens, and starts accepting, with no point
in between. Workers spawn their `on_start` future as a **standalone task on the
ready queue** (`backend/uring/event_loop.rs:250`, `backend/mio/event_loop.rs:130`),
which by construction does not block the loop, so `on_start` and accept run
concurrently.

**The `on_start` doc says otherwise** (`runtime/handler.rs:110-111`):

> Return `Some(future)` to spawn a standalone task that runs before the event
> loop begins accepting connections.

It runs before the loop *starts*, not before the loop *accepts*. Anyone who reads
that and puts warmup in `on_start` believes they have a readiness gate and does
not have one.

The obvious workaround — initialize before `launch()` — fails for the case that
most needs it. `connect()`, the timers and the fs APIs all require the runtime,
so warming a backend pool can **only** be done from inside it, which is exactly
where accept is already live.

## It is deferred `listen`, not deferred `accept`

`bind()` reserves the port. `listen()` is what makes the kernel complete
handshakes and queue connections. `accept()` only dequeues them.

| | port reserved | client sees | TCP readiness probe |
|---|---|---|---|
| bind, defer `listen` | yes | refused (Linux) / times out (Darwin) | fails, correctly |
| bind + `listen`, defer `accept` | yes | handshake completes, then silence | succeeds, wrongly |

Deferring `accept` defeats a readiness probe: the kernel completes the handshake
without the server, so a load balancer's TCP check passes and it routes to an
instance that is not ready. When the backlog fills the kernel drops SYNs, so
clients get timeouts rather than a clean refusal — worse to diagnose as well.

**What the peer sees is platform-specific, and the first version of this entry
said otherwise.** Measured on Darwin 25.6 with no ringline involved — a plain
socket bound without `listen`, plus two controls:

| | result |
|---|---|
| bound, not listening | times out (1.5 s limit reached) |
| nothing bound on that port | `ECONNREFUSED` |
| same socket after `listen(2)` | connects |

So Darwin drops the SYN rather than answering with RST. Linux answers with RST,
which is where the `ECONNREFUSED` claim came from. Both fail a readiness probe,
which is what the feature needs; only Linux fails it *quickly*, and a probe
with a generous timeout on Darwin spends that timeout. The integration test
asserts the Linux behaviour specifically under `cfg(target_os = "linux")` so CI
checks the claim rather than the code carrying it as a comment.

Holding clients in the backlog is the right behaviour when *draining* an old
instance during a handover. Refusing is the right behaviour when a new instance
is *not yet ready*. This entry is the second case; the first, if wanted, is a
separate control and should be named separately.

## The mechanism is already half-built, for merged mode

This is the finding that shrank the work, and it is the reason the design below
is not any of the three options in the issue as written.

`AcceptMode::Merged` already separates bind from listen:

1. Every `SO_REUSEPORT` socket is bound **before the workers start**
   (`worker.rs:1031-1067`), one per worker per listener.
2. `listen()` is **not** called there. It is deferred to after the workers are
   up (`worker.rs:1318`), with the comment already in the tree: *"Merged
   listeners were bound before the workers started; all that is left is to
   listen, which is the moment the runtime becomes reachable."*
3. A latch already exists — `merged_accept_live: Arc<AtomicBool>`, created
   `false` at `worker.rs:1069`, stored `true` at `worker.rs:1410` followed by a
   wake of every worker.
4. Workers already consume it: `arm_merged_accepts()`
   (`backend/uring/event_loop.rs:2555`) does a relaxed load each iteration and
   arms multishot accept on the rising edge, all-or-none, with an early return
   once armed.

So merged mode does bind → defer listen → publish a latch → workers arm. The
only thing that stops it being a readiness gate is **who releases the latch**:
`launch()` does, immediately after it listens, rather than the handler.

The tree already states the intent this feature extends. From the comment above
that bind loop (`worker.rs:1018-1024`):

> listening is what makes the runtime reachable, and that waits until every
> worker has reported ready. "Listening" and "ready to serve" stay the same
> instant, which is the guarantee the pool mode gets from creating its listener
> after the startup barrier.

Both modes already tie listening to a notion of readiness. Today that notion is
"the worker threads exist". The change is to let the handler be the one that
says ready, without moving the instant at which listening and serving coincide.

Pool mode — the default, and the only mode on mio — has none of this.
`create_listener` (`worker.rs:1620`) and `create_unix_listener` (`worker.rs:1672`)
each call `bind()` and then `listen()` back to back (`worker.rs:1657`, `:1694`),
and the acceptor thread enters its blocking `accept4` loop as soon as it is
spawned (`worker.rs:1378`, `acceptor.rs:94`).

One constraint worth recording as **checked rather than assumed**: deferring
`listen` does not break zero-port resolution. `.bind("127.0.0.1:0")` learns its
port from `getsockname`, and merged mode already calls `getsockname_v4_v6(fd)`
on a socket that is bound and not yet listening (`worker.rs:1044`). The port is
assigned by `bind`, not by `listen`.

## Design

**A per-listener `AcceptGate`, released by the handler.**

1. **Split bind from listen in Pool mode.** `create_listener` and
   `create_unix_listener` return a bound, unlistened fd. The acceptor thread
   calls `listen()` itself once its gate opens, then enters the accept loop.
   One call site per listener, which is also where the accept loop lives.
2. **Generalise the latch to a per-listener gate both modes consult.** Merged
   mode's consumer already exists and needs only to read a per-listener flag
   instead of one process-wide one; `merged_accept_fds` is already
   `(listener_index, fd)` pairs, so the indexing is there.
3. **Expose it keyed by `ListenerId`.** `ListenerSpec` already documents that a
   listener's position in `bind*()` call order *is* its `ListenerId`
   (`worker.rs:455-464`, `handler.rs:144`), so per-listener needs no new
   identity scheme.

**Default is open.** A listener gates only if the builder asked it to, so every
existing server is unaffected and the change is additive. Opting in is per
listener, on the builder, next to `bind()`.

### Decisions

**Per-listener, not per-process.** A server with a health port and a data port
wants the health port serving immediately and the data port gated — that is the
configuration the feature exists for, and a per-process gate cannot express it.
A convenience that opens every gate at once is cheap to add on top; the reverse
is not.

**The handler releases it, `on_start` does not become blocking.** See the
rejected alternatives below.

**Shutdown must not deadlock on an unopened gate.** `ShutdownHandle::shutdown`
currently wakes a parked acceptor by `shutdown(SHUT_RD)` on the listen fd
(`worker.rs:378-397`), which works because the thread is inside `accept4`. A
thread parked on a gate is not, so the gate's wait has to be interruptible by
the shutdown flag. A server that is shut down before it ever becomes ready is
an ordinary case — a failing initializer — not an edge case.

**No per-connection cost in the default path.** Pool mode checks the gate once,
before the accept loop, so there is nothing to pay per connection. Merged mode
already pays one relaxed load per iteration today. This is a property of where
the check sits, so it is asserted by construction rather than measured.

## Rejected alternatives

**Await `on_start` to completion before arming accept** (option 2 in #534).
Rejected. `on_start` today returns a task that may legitimately never complete,
and the repo relies on that: `ringline-ping/tests/round_trip.rs` and
`ringline-redis/tests/integration.rs` both use `on_start` as the entire client
body, ending in `request_shutdown()`. Under this option a server whose
`on_start` is a long-lived background task — a periodic refresher, a
control-plane watcher — never accepts at all, and nothing reports why. That is
not a tightening of the existing guarantee, it is a different contract with a
silent failure mode. It would also serialise startup across workers unless each
worker gated only its own accepts, which reintroduces a partial-readiness state
the gate is meant to remove.

**Extend `set_worker_accepting` to Pool mode** (option 3 in #534). Rejected as
the primary mechanism. It is per-*worker*, not per-listener, so it cannot
express the health-port case; it lives on `ShutdownHandle`, which is an odd home
for a readiness control; and its implementation is `SO_REUSEPORT` BPF steering
(`worker.rs:232`), which has no analogue in Pool mode. It also does not drain —
connections already queued on an excluded worker are still accepted — so it
answers "stop accepting", not "do not start".

## GO/NO-GO

GO requires all of:

1. A gated listener refuses with `ECONNREFUSED` before release and serves after
   it, asserted directly by a client connect attempt in both states — not by
   inspecting a flag.
2. Both accept modes and both backends. Pool mode is the one that needs new
   code; merged mode is io_uring-only and keeps an acceptor thread for Unix
   sockets even when merged (`worker.rs:1026-1027`), so the Pool path has to
   work regardless.
3. `.bind("127.0.0.1:0")` still reports its resolved port while gated.
4. Shutdown of a never-released gate terminates, with a test that would hang if
   it did not.
5. The ungated path is untouched: existing accept tests pass unmodified.

NO-GO if the gate cannot be made interruptible by shutdown without a timeout —
a mandatory timeout would make the port silently start serving before the server
is ready, which is the failure the feature exists to prevent.

## Open questions

- **Whether an optional timeout is offered at all**, and if so whether expiry
  opens the gate or fails the launch. Failing is the honest default; opening is
  what someone will ask for.
- **What observability reports while gated.** A listener that is bound and not
  listening is a state nothing currently names.
- **Whether `AcceptGate` release is idempotent and what a second release means.**
  Likely a no-op, matching `set_worker_accepting`'s early return at
  `worker.rs:240`.

## The `on_start` documentation

Independent of the above, `runtime/handler.rs:110-111` is false today and is
corrected as part of this work: `on_start` runs concurrently with accept, and
the doc points at the gate for readiness. This is the part of #534 that is
genuinely 0.7.0-blocking. The gate itself is additive and could ship later —
but shipping a release whose documentation promises a readiness gate that does
not exist is worse than shipping without the feature.

## Implementation notes

Landed for `AcceptMode::Pool` (the default, and the only mode on mio).
`defer_listen()` with `AcceptMode::Merged` is refused at launch rather than
silently not deferring; merged mode's arming is a single process-wide
`merged_accept_armed` bool with two re-arm sites, and making it per-listener is
a second change.

**GO criterion 4 is met at the unit level, not the integration level, and that
distinction was found by mutation.** The integration test asserts that shutting
down a runtime with an unreleased gate completes and the workers join — and it
stays green with `ShutdownHandle::shutdown`'s call to `ListenGates::shutdown`
deleted. The acceptor thread is detached, so one parked on a gate never holds
up `join()` on the worker handles, and the listen fd is closed by `shutdown`
whether the thread woke or not. What the call actually prevents is a stranded
thread, which no observable API state reflects. The mechanism therefore has its
own unit test (`acceptor::tests::a_gated_acceptor_exits_on_shutdown`), and the
wiring from `ShutdownHandle::shutdown` to it is covered by inspection only.
Recorded because a green test that passes with the code removed is worse than
no test.

The other mutation behaved as intended: making `defer_listen()` a no-op turns
four of the six integration tests red and leaves the ungated control green.

**What the peer sees is platform-specific**, which the entry above records with
its measurement. The consequence for the feature is that only Linux fails a
readiness probe quickly; Darwin makes the prober wait out its own timeout.
