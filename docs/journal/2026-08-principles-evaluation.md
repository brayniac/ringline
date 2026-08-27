# 2026-08: Evaluating the code against its own principles

- **Status:** shipped
- **Span:** 2026-08-26 → 2026-08-28 · PRs #322–#325 · unreleased (next release carries the batched breaking changes)

## Goal

PR #317 wrote down the ten design principles (`docs/principles/PRINCIPLES.md`);
PR #320 backed the io_uring primer with counted-and-measured syscall/copy
numbers. The natural next question: does the code actually conform to the
principles? Evaluate each principle against the tree, and for every real
deviation found, land a fix through the full loop — PR, adversarial subagent
review, merge — one finding at a time.

## What happened

The evaluation (targeted spot-checks, not an exhaustive audit — the deep P1
sweep remains the 2026-07 audit) found the code strong on P2/P3/P4/P8/P10 and
produced four findings. Each shipped as its own PR:

**1. P7 — `recv_accumulator_max` defaulted to `usize::MAX` (6e295cc, #322).**
The overflow machinery (close on cap breach) existed on every append path;
only the default disabled it, and the field's own doc admitted a bound "should
be set." The adversarial review of the first version (64 MiB) found two
criticals that reshaped the fix:

- The protocol clients parse whole replies resident in the accumulator, and
  Redis's own `proto-max-bulk-len` defaults to 512 MiB — 64 MiB would have
  broken replies the 0.5.5 client handled. Shipped default: **1 GiB**
  (bounded per P7, above the server-side authority).
- The mio plaintext recv path ignored `append`'s overflow result — bytes
  already read off the socket were dropped and the connection left in a
  read-and-discard spin instead of closing. Latent while the default was
  unbounded; fixed to close+wake, matching the io_uring and mio-TLS paths,
  with a both-backends regression test
  (`ringline/tests/recv_accumulator_cap.rs`).

Also: `recv_accumulator_max >= recv_buffer_size` is now validated (the
pending-buffer flush paths assume one buffer's append into an empty
accumulator cannot fail).

**2. P9 — `ringline-h2`/`ringline-http` had zero `#[non_exhaustive]`
(114840b, #323).** `H2Error`, `H2Event`, `HttpError` — plus, after the review
showed the sibling precedent extends to grow-prone value enums
(`GrpcStatus`, `CompletedOp`, `ParseResult`) — `Protocol`, `Body`, and
`StreamingResponse`. Deliberate exhaustiveness is now documented rather than
implicit: `ErrorCode` and `Frame` both fold unknown wire values into an
existing variant instead of growing. `H2Event` carries a written contract for
new variants (purely informational — never the sole carrier of termination),
since downstream wildcard arms now compile silently. Deferred, not decided:
`PoolConfig` with pub fields turned out to be a workspace-wide client-crate
pattern (ping/redis/http all document struct-literal construction), so making
those opaque is a coordinated breaking decision, not a drive-by.

**3. P5 — the mio large-TLS busy-spin comment was stale (52bd616, #324).**
The evaluation flagged a backend asymmetry documented only in a test comment
("gated to io_uring; mio busy-spins, tracked separately"). Investigation
inverted the finding: the busy-spin was fixed by #241 (a7801a9, July's mio
TLS output queueing) and the gate removed — only the comment claiming the
phantom bug survived. A stated difference that is no longer real misleads the
same way an unstated one does.

The close-out then bit back (see Lessons): strengthening the test with a
1 MiB payload hung the io_uring CI jobs for six hours (run 33052650385). The
mechanism, verified on hv01 against real io_uring: the test config's send
pool (64×16 KiB) is exactly 1 MiB, the synchronous client writes the whole
payload before reading so >64 echo chunks are in flight at peak,
`TlsEchoHandler` swallows `send_nowait` errors — so pool exhaustion silently
dropped chunks — and the client's unbounded `WouldBlock` retry burned the
full 10s `SO_RCVTIMEO` per attempt. The runtime itself was healthy (worker
parked in `io_cqring_wait`; `send_nowait` had correctly reported exhaustion —
the *test* swallowed it). Fixed with 4× pool headroom and a 30s no-progress
deadline in the read loop.

**4. P1/P5 — send-family CQE handlers validated liveness, never identity
(2218321, #325).** The evaluation noticed `handle_send` guards a Send CQE
only by `send_copy_pool.in_use(slot)` while `handle_recv_fallback` validates
slot ownership — the P5 pattern of "each half looks reasonable alone." An
adversarial investigation pass classified every `send_copy_pool`/`send_slab`
release site (all pre-submission, in-the-op's-own-handler, or at shutdown —
the exact ABA I first hypothesized is foreclosed by that invariant) and found
the adjacent window that is real: two paths produce send CQEs that outlive
their connection slot — `send_chain` never set `in_flight`, so a close raced
active chains; and `DriverCtx::close` forced `in_flight = false` and
submitted Close directly. A stale CQE then passed the liveness guard and was
misattributed to whatever connection reused the index: the partial-send path
would resubmit the dead connection's bytes onto the new occupant's socket;
the error path spuriously drained/failed the new occupant's sends.

The fix: connection generation validated on every send-family CQE (packed
into the UserData payload for pool-slot ops, enforced by submit-fn
signatures; recorded in the slab entry for ZC/coalesced ops), both producers
closed (chain-aware close deferral; ctx-close through the same
`close_pending` path as `close_connection`), the close-queue kick serialized
(the parallel push was "mistake (b)" in the code's own comment), and the TLS
`close_notify` deadline — dead code until then, its only arming path never
set `close_pending` — made live as a force-close whose orphaned CQEs are
exactly what the identity checks render safe.

Verification found the branch's own regression: `async_select_with_sleep`
hung deterministically whenever any test preceded it. Traced on hv01 with
targeted instrumentation: **main had been depending on the misattribution.**
A stale CQE's wrongly-attributed error path was what cleared a recycled
slot's `in_flight`/`close_pending` flags; with identity checks correctly
refusing to touch another occupant's state, the next occupant inherited
`in_flight = true` and its first send parked forever. The principled
replacement is `reset_send_state` at slot reactivation (alongside the
existing `reset_segment_state`). Six regression tests dispatch stale CQEs
through the real ring.

## Outcome

All four findings shipped: #322 (6e295cc), #323 (114840b), #324 (52bd616),
#325 (2218321). CHANGELOG carries the batched breaking notes (bounded accumulator
default, `non_exhaustive` additions) for the next release. The strongest
validation of the principles document itself came from finding 4: the one
handler that already validated identity was right, and making the other seven
match surfaced a second latent bug (the inherited send state) for free —
P5's "asymmetry is presumed defect" working as designed.

## Lessons / open questions

- **Adversarial review earns its cost on defaults.** Both review saves on
  #322 (the 64 MiB default breaking the in-tree clients; the mio overflow
  ignore) were invisible from the diff alone — they required tracing who
  consumes the default and comparing backends.
- **A latent bug can be load-bearing.** The identity checks exposed cleanup
  that only happened via misattributed stale CQEs. When removing a wrong
  behavior, ask what accidentally depended on it.
- **Backend-affecting test changes must run on the affected backend before
  the PR.** The #324 1 MiB case was verified only on mio (macOS); the
  io_uring hang cost two 6-hour CI jobs. The hv01 rsync loop exists for
  exactly this.
- **Bound test waits by time, not iteration count.** Each `WouldBlock` on a
  socket with `SO_RCVTIMEO` costs the full timeout; a 2000-iteration bound
  is a 5.5-hour bound.
- **Baseline before blaming the branch.** hv01's echo suite flakes at
  roughly 1-in-8 under tight loops on main too (varying tests, plus the
  known `buffer_ring_exhaustion_recovers` flake); the deterministic
  regression was distinguishable from the background rate only by running
  main under the identical loop.
- **Open backlog** (from the #325 reviews; reopen conditions in the PR):
  suppress wasteful resubmit SQEs from pre-bump CQEs on force-closed
  connections; pack real generations in the `mixed_operations` proptest's
  reused-index sends; a P6 checked-arithmetic sweep of `ringline-grpc` was
  never done; the `PoolConfig` opaque-config decision awaits a coordinated
  breaking release.
