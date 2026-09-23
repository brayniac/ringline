# Listeners and the accept path

- **Status:** proposal — intent only, nothing implemented
- **Motivated by:** the single-listener limit found while reviewing whether
  `Connection` should split per transport

## The problem in one sentence

A ringline process can listen on exactly one socket, and the code that accepts
on it is the only part of an io_uring runtime that never touches the ring.

## Evidence

**One listener, last call wins.** `bind_addr` is a single `Option<BindAddr>`
(`worker.rs:474`); `bind()` (`:488`) and `bind_unix()` (`:497`) each *assign*
it. Calling both silently discards the first. Three methods away in the same
builder, `bind_udp()` (`:506`) pushes onto a `Vec` — the right pattern already
exists, in the other half of the API.

**Everything per-connection is therefore process-global, with the transport
difference papered over at run time:**

| what | today | consequence |
|---|---|---|
| TLS | `config.tls: Option<TlsConfig>` (`config.rs:203`) | cannot serve plaintext on 8080 and TLS on 8443 |
| `TCP_NODELAY` | `tcp_nodelay: if is_unix { false } else { … }` (`worker.rs:980`) | `ConfigBuilder::tcp_nodelay(true)` is accepted, documented, and silently ignored under `bind_unix` |
| bound address | `getsockname_v4_v6` → `Option`, `None` for Unix (`worker.rs:962`) | one address for one listener |
| peer address | acceptor sends a fabricated `0.0.0.0:0` for Unix (`acceptor.rs:104`) | the channel payload is `Sender<(RawFd, SocketAddr)>` — TCP-shaped |

**Accepts never enter the ring.** There is no accept opcode anywhere in
`backend/uring/`. The path is a dedicated thread blocking in `accept4`
(`acceptor.rs:33`), then a crossbeam send plus an eventfd write to wake the
target worker — a cross-thread handoff per connection, in a runtime whose
entire premise is that nothing crosses threads. The thread is also unpinned,
which is the hazard class behind the single-core funnel we root-caused
(i40e Flow-Director ATR plus unpinned threads under `isolcpus`).

**The crate's own architecture diagram describes a design we do not have.**
`lib.rs:53` reads *"Acceptors Thread (accept4() with SO_REUSEPORT)"*. There is
one acceptor thread, one socket, and `create_listener` is explicitly documented
*"without SO_REUSEPORT (just SO_REUSEADDR)"* (`worker.rs:1169`) with no
recorded reason.

**TLS handshakes run on the worker event loop**, inline in the completion path:
*"TLS path: defer accept until handshake completes"* (`backend/uring/event_loop.rs:1882`).
`on_accept` is not called until the handshake finishes, so a TLS connect storm
puts all of the asymmetric crypto on the workers before any user task exists.

## Design

### 1. A listener list

`bind()` and `bind_unix()` push rather than assign, matching `bind_udp`. Any
mix of TCP and Unix listeners, any number of each. This is the piece everything
else follows from, and it settles a question raised separately: because one
`on_accept` now receives every transport, `Connection` **stays transport-erased**.
A `TcpConnection` / `UnixConnection` split — which std and tokio do, and which
is defensible under the one-listener limit — stops working the moment a process
can serve both, and ringline has no `AsyncRead`/`AsyncWrite` to re-converge on.

### 2. Per-listener configuration

TLS, `TCP_NODELAY`, backlog and bound address move from `Config` onto the
listener entry. The `if is_unix` branch at `worker.rs:980` then does not need
another arm — a Unix listener simply has no `tcp_nodelay` to set. Process-wide
`ConfigBuilder::tls()` stays as the default for listeners that do not override
it, so existing single-listener code is unaffected.

### 3. `ListenerId` on `Connection`

A handler serving plaintext, TLS and a Unix admin socket cannot dispatch
without knowing which listener a connection arrived on. `Connection::listener()
-> ListenerId` (opaque, bind order) alongside the existing `peer_addr()`. The
`AsyncEventHandler` signature is unchanged — that RPITIT is the most delicate
declaration in the crate and this does not need to touch it.

The acceptor channel payload becomes `(RawFd, ListenerId, PeerAddr)`, which
also retires the fabricated `0.0.0.0:0`.

### 4. Two accept modes, configurable

**Pool** — N acceptor threads, one per listener, each blocking in `accept4`,
all feeding the existing per-worker channels. Keeps explicit round-robin
placement and the existing full/dead-worker fallback. Unix listeners need this
mode or the shared-fd variant, since `SO_REUSEPORT` does not apply to them.

**Merged** — each worker binds its own listener with `SO_REUSEPORT` and arms a
multishot accept on its own ring. No thread, no channel, no wake fd, no
handoff. Kernel ≥6.0 is already required, so multishot accept is available.
This mirrors what the UDP path does today (`backend/uring/driver.rs:2291`).

For a listener that cannot use `SO_REUSEPORT` (Unix), the merged mode degrades
to every worker arming multishot accept on one **shared** listen fd; the kernel
hands each connection to exactly one waiter.

mio has no multishot accept. It keeps the pool, or registers the shared
listener per worker with `EPOLLEXCLUSIVE`. Correct and non-pathological is the
bar there, not optimal.

### 5. Placement, and why it is not a detail

The pool mode's real advantage over the merged mode is *placement*: explicit
round-robin versus the kernel's 4-tuple hash. That difference is much larger
than it sounds, because of the topology every real client uses.

**A pooled client is the adversarial case for hashing.** N connections from one
client to one host vary only in source port, so `SO_REUSEPORT` spreads them
approximately uniformly at random. That is balls-in-bins: at N = W — the
canonical "one connection per server core" pool — the expected fraction of
*empty* workers is (1 - 1/W)^W → 1/e ≈ **37%**. Eight connections across eight
workers typically leaves three workers idle and stacks three on one. Round-robin
gives exactly one each, deterministically.

A third of the machine dark on the most common client pattern is not a marginal
distribution difference, and it is the topology all of our load generators use.
So **BPF steering is a prerequisite for the merged mode, not a refinement of
it** — without explicit placement the merged mode cannot be the default.

`SO_REUSEPORT` groups accept a BPF program that chooses the target socket:

- `SO_ATTACH_REUSEPORT_CBPF` — classic BPF returning an index into the group.
  Unprivileged. Cannot read a map, so changing policy means re-attaching a new
  program via `setsockopt`. Coarse, but enough for "exclude worker 3".
- `SO_ATTACH_REUSEPORT_EBPF` — `BPF_PROG_TYPE_SK_REUSEPORT` with a
  `BPF_MAP_TYPE_REUSEPORT_SOCKARRAY` and `bpf_sk_select_reuseport()`. Userspace
  updates the map at run time, so a worker leaves the rotation with no
  `setsockopt` and no socket close. Needs BPF load privileges.

**Measured, 2026-09-22** — Debian 13, kernel 6.12.63, four listeners in one
reuseport group, 40 connections, `experiments/reuseport-steering-spike.toml`:

| case | Recv-Q across the four listeners | accepted |
|---|---|---|
| CBPF selects index 0 | `0, 0, 0, 40` | `40, 0, 0, 0` |
| CBPF selects index 2 | `0, 40, 0, 0` | `0, 0, 40, 0` |
| no BPF (kernel hash) | `12, 10, 8, 10` | `10, 8, 10, 12` |

**A socket the selection program never picks accrues nothing** — zero pending
backlog, zero accepted. "Not selected" and "not queued on" are the same thing,
so tier 2 works: a worker leaves the rotation without closing its listener, and
nothing is sitting there to be reset. The no-BPF row is what makes that
trustworthy — the same measurement reports non-zero backlogs on all four
sockets, so the zeros are a result rather than an instrument that can only
print zero.

Re-attaching a different program moves the target, so **unprivileged CBPF is
enough to take a worker out of the rotation** — no BPF load privileges needed.
CBPF cannot read a userspace map, so a policy change re-attaches a program
mapping to the remaining set; eBPF with a `REUSEPORT_SOCKARRAY` stays the
option if per-connection policy is ever wanted, at the cost of those
privileges.

**The index source decides whether exclusion funnels (measured 2026-09-23).**
Excluding a socket is only half of what the program must do; the other half is
spreading over the ones that remain. Two ancillary loads were tried, same
program shape otherwise (`idx = <source> % live_count`, then a jump chain to
the live socket's group index), 40 connections over 4 listeners,
`experiments/reuseport-exclude-spike.toml`:

| index source | excluded socket | live set |
|---|---|---|
| `SKF_AD_CPU` | 0 | `0, 40, 0` |
| `SKF_AD_RANDOM` | 0 | `12, 12, 16` |
| `SKF_AD_RANDOM` (different exclusion) | 0 | `16, 10, 14` |

Exclusion is absolute under both — the excluded socket accepts nothing. But
`SKF_AD_CPU` is the *receiving* CPU, which is constant for a loopback client, so
every connection funnels onto one live socket. Tier 1's handoff would then
redistribute nearly every connection at a channel hop each, which is the
single-core funnel shape this project has already root-caused once.

So tier 2 attaches **`SKF_AD_RANDOM % live_count`**, unprivileged CBPF, with a
jump chain mapping the result to the live socket's index. No packet parsing, no
map, no privileges.

Not yet measured, and load-bearing before tier 2 ships: what happens to
connections **already queued** on a socket when it leaves the rotation. They
stay where they landed, so exclusion stops new arrivals but does not empty the
queue — taking a worker out still needs a drain before its listener closes.

Recorded hazard: closing a `SO_REUSEPORT` listener **resets** connections still
queued on it. Graceful shutdown in merged mode must stop accepting, drain, then
close.

### 5a. Spread versus pack

Spreading a client's pool across workers is not unconditionally right, and the
codebase already contains the opposing argument. `conn_chunk_size`
(`config.rs:148`) exists to do the opposite:

> Higher values pack connections onto fewer workers at low connection counts,
> keeping each active worker's CQE density high enough for io_uring batching to
> pay off. Rule of thumb: set to the minimum connections-per-worker at which
> your workload sees good batching (typically 16-64).

Follow that rule of thumb and a 16-64 connection client pool lands **entirely on
one worker**. The knob's own caveat — no effect once total connections exceed
`conn_chunk_size * num_workers` — covers the many-client case and is exactly
wrong for the single pooled client.

The tension is real, not an oversight. Packing buys CQE batching density;
spreading buys parallelism and tail latency. Both regimes have been measured
here: the throughput work where batching paid, and the server-latency finding
where ringline lost ~28% to tokio in the idle-CPU, latency-bound case — which is
precisely where packing hurts.

This does not resolve to a single default. It resolves to **placement policy as
the configured thing**, with `conn_chunk_size` as one point in that space, and
documentation that names the regime each setting serves rather than a rule of
thumb that is hostile to pooled clients.

### 6. Rebalancing, in four tiers

Live migration of a running connection is impossible and is not proposed. The
`ConnectionTable` slot, `RecvAccumulator`, send queue and task-slab entry are
thread-local, and `Connection`, `SendHalf`, `RecvHalf` and `ConnCtx` are
`!Send` via `PhantomData<*const ()>`.

But the *connection* is not what blocks a move — the *task* is. Cheapest first:

**Tier 1 - prevent.** Place at accept time, before the connection has any
state. In pool mode that is the existing round-robin. In merged mode, a worker
accepting while over its share forwards the **raw fd** to the least-loaded
worker over the existing channel; at that point nothing exists but an integer.
One mechanism, two policies.

**Tier 2 - steer.** Take an overloaded worker out of the accept rotation via
§5. No closes, no handshakes, reversible.

**Tier 3 - park and adopt.** Move the connection to another worker without the
client noticing.

Everything the connection *is* can move. Inside one process an fd is an integer,
so there is no `SCM_RIGHTS` dance; the accumulator is `BytesMut`; the TLS state
is a `rustls::ServerConnection`. What cannot move is the future `on_accept`
returned — it is `!Send` and `'static` and lives in worker A's `TaskSlab`.
Making it `Send` would put `Send` bounds across the whole user-facing API, which
is the thread-per-core design's entire benefit. So the future is **dropped on A
and recreated on B**, which makes park a cooperative operation at a point where
the handler holds no state it cannot hand over.

The quiesce this needs already exists. Deferred teardown is implemented and
tested — `ctx_close_defers_behind_in_flight_send`,
`close_defers_while_chain_active`, `close_while_segment_pinned_defers_bid_release`
— because Domain Invariant 1 requires waiting for in-flight sends, active chains
and pinned segments before releasing a slot. Park is that path stopped one step
early: quiesce, then hand the fd over instead of closing it. Fd movement between
workers is likewise already supported —
`register_files_update(fd_index, &[fd])` (`backend/uring/driver.rs:2380`) is how
an fd enters a worker's fixed-file table.

The gate is not new either — it is `try_finalize_close`'s predicate
(`backend/uring/driver.rs:1670`), which is already the definition of "this
connection is quiescent":

```rust
!in_flight && queue.is_empty() && forward_write.is_none() && !chain_table.is_active()
```

with **one term that close does not need**: `segment_pinned[conn].is_none()`.
A pinned entry is `HeldRecvBuf::Pinned { bid, len }`, a bid index into *this
worker's* `ProvidedBufRing`; on the target worker it addresses a different
ring's buffer. Close can defer until the reader releases it, but park has a
cheaper option, and it is the rule segmented recv already follows: **convert
`Pinned` to `Owned` by copying** before the move. Bounded work, always
succeeds, and no connection becomes permanently unparkable because a reader is
slow. `HeldRecvBuf::Owned(Bytes)` needs nothing.

Sequence: quiesce on the predicate above → convert any pinned segment to owned
→ cancel the multishot recv and unregister the fd from A → package
`(OwnedFd, leftover accumulator bytes, TLS state, ListenerId, PeerAddr,
handler payload)`, all `Send` → send over the tier-1 channel → B allocates a
slot, registers the fd, seeds the accumulator, re-arms multishot recv, and calls
a new `on_adopt` hook.

"TLS state" is the whole `UnbufferedConn` (or its buffered counterpart) moved as
one value, not a reconstructed session — see open question 4. Both engines take
the same path.

`on_adopt` is **optional**, and the handler payload is the new public surface:
since the future dies, anything the handler kept — negotiated protocol,
authenticated identity, subscriptions — is rebuilt or carried as an opaque
`Box<dyn Any + Send>` returned at park and handed back at adopt.

Viability is not cache-versus-web, it is **request/response versus multiplexed**:

| shape | quiescent point | payload | verdict |
|---|---|---|---|
| RESP, memcache | after every response | empty, or a DB index / auth flag | easy |
| HTTP/1.1 keep-alive | after every response | near none | easy |
| HTTP/2 | zero open streams | `H2Connection` is plain data (`ringline-h2/src/connection.rs:190`) — sans-IO, so it moves | idle connections yes, saturated no |
| HTTP/3 | — | rides `UdpCtx`, not the connection table; QUIC migrates at the protocol level | out of scope |

The h2 gate predicate is already written and public: `H2Conn::pending_count()`
(`ringline-http/src/h2_conn.rs:457`). `pending_count() == 0` is the park gate.

The requirement is also less restrictive than it first appears, because **you
park idle connections, not busy ones**. Rebalancing targets long-lived
connections, and long-lived connections are idle between bursts.

**Tier 4 - shed.** For handlers that cannot park, hang up slowly so clients
reconnect elsewhere. A hint, not an action: "graceful" is protocol-specific —
H2 has GOAWAY, H1 has `Connection: close`, raw TCP has only FIN, and a request
may be in flight. The runtime knows the load; only the handler knows what a
polite goodbye looks like.

Shed is last because it is expensive and imprecise. Each shed connection costs a
reconnect plus a full TLS handshake — the expense §7 exists to reduce — and
under an unsteered hash the replacement lands uniformly, so with 8 workers it
has a 1-in-8 chance of returning to the same worker. Park has neither problem:
nothing is client-visible, and placement is **exact** because you choose the
target worker. That is also why park, not steering, is what makes rebalance
converge.

Guardrails for tiers 3 and 4: rate limit, hysteresis so it does not oscillate,
prefer idle connections, floor on per-worker count.

**Non-goal, stated so it is not filed as a bug later.** If a worker is hot
because of *one* saturated connection, none of this helps. Park cannot — there
is no quiescent point. Shed cannot — it relocates the load and charges a
handshake for it. Rebalancing addresses **count** imbalance, not **load**
imbalance from a single heavy connection. That is inherent to thread-per-core
without work stealing.

### 7. Handshake offload as its own knob

Where handshake crypto runs is orthogonal to how connections are accepted, and
should be configured separately rather than bundled into "pool mode".

A handshake is ~2 RTT of latency plus a burst of CPU (ECDHE and a signature).
Those want opposite treatment. A pool that blocks across the RTTs needs roughly
a thread per in-flight handshake — a 1000-connection storm is 1000 threads. A
pool that goes async to avoid that is an event loop, i.e. a worker. So the
offloadable part is the CPU burst, and the tool already exists:
`blocking_threads` (default 4, `config.rs:308`) with `spawn_blocking`.
`rustls::ServerConnection` is `Send`, so moving `process_new_packets` off the
event loop and back is mechanically possible.

Two handoffs per handshake is not obviously a win. This needs a prototype and a
measurement before it is more than a knob.

## Open questions

1. ~~Does a `SO_REUSEPORT` socket excluded by a BPF selection program still
   accrue a backlog?~~ **Answered 2026-09-22: no** — zero Recv-Q, zero
   accepted (§5). Tier 2 works.
2. ~~Is unprivileged steering practical, or does useful steering need BPF
   privileges?~~ **Answered: unprivileged CBPF suffices** to exclude a worker;
   re-attach moves the target.
3. What happens to connections **already queued** on a socket when it leaves
   the rotation? They stay, so tier 2 needs a drain before any close. Not
   measured.
4. ~~Does the `tls-unbuffered` engine hold partial-record state that complicates
   moving a `ServerConnection` between workers?~~ **Answered 2026-09-23: it holds
   the state, and it moves anyway.** `UnbufferedConn`
   (`tls/unbuffered/mod.rs:133`) keeps `incoming: CiphertextBuf` — received
   ciphertext awaiting `process_tls_records`, i.e. exactly the partial records
   this asked about — plus `pending_plaintext: VecDeque<Vec<u8>>`. Both are
   plain owned data: `CiphertextBuf` is a `Vec<u8>` and three `usize`
   (`tls/ciphertext.rs:80`). The two cache fields
   (`max_plaintext_per_chunk`, `chunk_basis`) are keyed on the `SendCopyPool`
   slot size, which is uniform for every slot today, so they stay valid on the
   target worker. Verified by compiling a `Send` bound over `UnbufferedConn`,
   `CiphertextBuf` and `RecvAccumulator`, and by then adding an `Rc<u8>` to
   confirm the assertion could fail. **Tier 3 therefore needs no
   engine-specific path** — move the whole `UnbufferedConn` as one value
   rather than reconstructing it. Revisit only if TLS ever gets its own slot
   class, which would invalidate the chunk cache across workers; that is
   already flagged at the field.
5. ~~Can a `Bytes` handed out by `with_bytes` still be alive at a park
   point?~~ **Answered 2026-09-23: it can, and it is not the hazard.** The
   accumulator copies into its own `BytesMut` (`accumulator.rs:73`), so a
   `Bytes` from `with_bytes` is a refcounted slice of owned memory — `Send`,
   and dropped with the future that park discards regardless. The blocker is
   one level down: `segment_pinned[conn]` can hold
   `HeldRecvBuf::Pinned { bid, len }`, a bid index into **this worker's**
   `ProvidedBufRing`, which means nothing on the target.
   `HeldRecvBuf::Owned(Bytes)` moves fine. See the gate in §6.
6. Does any client crate need to be generic over transport (redis and memcached
   both speak Unix sockets)? Decides whether §1's transport erasure is also
   needed on the outbound side.
7. Is per-listener TLS reachable without restructuring the handshake path, or
   does `TlsConfig` need to become per-connection state?

## Measurement

**Run 2026-09-23.** Two X710 guests, one client host opening a pool against an
8-worker server, `experiments/accept-mode-ab.toml`. Distribution read as
per-core busy time from `/proc/stat` diffed across the measured window —
workers are pinned, so core *N* is worker *N*.

### At 64 connections over 8 workers: no difference, and no information

| | throughput | worker cores busy |
|---|---|---|
| pool | 294,321 ops/s | 8/8, 71.5–81.8% |
| merged | 294,447 ops/s | 8/8, 71.6–82.2% |

That looks like a clean pass and is worth nothing. At N=64 over W=8 the
kernel's hash leaves a worker idle with probability about 0.2%, so this run
cannot distinguish placement working from placement doing nothing. It is the
"measured from N independent clients" mistake in another form: the topology was
right, the *scale* was not.

### At 8 connections over 8 workers: merged leaves workers idle

Worker-core busy %, three interleaved reps each:

| rep | pool | merged |
|---|---|---|
| a | 31.9 42.9 32.1 32.0 30.9 30.1 31.7 29.6 | 31.6 42.6 35.6 32.3 **4.8** 30.6 **4.0** 39.1 |
| b | 35.0 32.6 30.1 30.4 32.0 49.2 29.1 27.4 | 39.5 **2.5** 27.1 26.5 35.6 38.9 34.6 **8.1** |
| c | 30.5 28.3 23.9 24.7 30.6 29.5 29.8 37.0 | 40.0 37.9 **4.1** 35.7 29.4 39.2 39.4 **4.3** |

**Pool leaves no worker idle, 3/3. Merged leaves exactly two idle, 3/3** — a
different pair each time, so it is the hash rather than a fixed bug. Two of
eight is what (1-1/W)^W predicts.

### After the fix (2026-09-23)

`HANDOFF_MARGIN` drops to 1 when the quietest worker is idle (#457). Nine
interleaved reps per arm, same harness, same N=W=8. The idle threshold was
fixed at 10% busy *before* the data was read, justified by the pre-fix numbers
— idle cores sat at 2.5–8.1% against 24–49% busy, so the line falls in a wide
empty gap rather than being a knob to turn afterwards.

| arm | reps | min | max | median | median spread | idle (<10%) |
|---|---|---|---|---|---|---|
| pool | 9 | 24.6 | 53.3 | 30.6 | 13.5 | **0** |
| merged | 9 | 23.2 | 63.8 | 30.8 | 13.1 | **0** |

**Merged now leaves no worker idle, 9 of 9**, against 2 idle in 3 of 3 before.
Spread and median match pool's within noise. Both arms throw the occasional
high outlier (pool 53.3, merged 63.8), so that is the rig, not the mode.

This clears the distribution half of the default-mode decision. Connect rate
remains unmeasured, so `Pool` stays the default until that exists.

### Conclusion

**Merged accept mode stays behind its flag** — though the distribution defect
below was fixed in #457; see "After the fix" above. As first measured, tier 1's
accept-time handoff did not fix the case it was built for. The mechanism is visible in its own
constants: a worker sheds only when it is `HANDOFF_MARGIN` (2) ahead of the
quietest, but at N≈W most workers hold 0 or 1 connection, so a worker holding 2
sheds only if it *sees* a zero — and the whole population arrives within
microseconds, before any worker has published a load update. The optimistic
claim added for bursts spreads work across many connections; it has nothing to
work with when there are only eight.

Connect rate is still unmeasured — `bench-client` opens its pool once and holds
it, so a connect storm needs load-generator work that has not been done.

## Staging

1. Listener list + per-listener config (TLS, nodelay, backlog, bound address)
   + `ListenerId` on `Connection` (breaking builder change).
2. Acceptor payload `(RawFd, ListenerId, PeerAddr)`; retire the fake peer addr.
3. Merged accept mode behind config, io_uring multishot; mio keeps the pool.
4. BPF steering → tier 1 and tier 2 placement. The kernel-behaviour spike is
   done (§5); what remains is wiring it into the runtime.
5. Park and adopt (tier 3): quiesce reuse, fd handover, `on_adopt`.
6. Shed hint (tier 4).
7. Handshake offload knob, gated on its prototype measuring well.

Fix the stale `lib.rs:53` diagram in step 1 — it is the crate's front page and
it currently describes `SO_REUSEPORT` that does not exist.

Placement policy documentation (§5a) belongs with step 3 or 4, whichever lands
the second placement mechanism: the existing `conn_chunk_size` rule of thumb is
actively wrong for pooled clients and should not survive this work unqualified.

This is a breaking builder change, so it batches with the naming work already
queued for the next release: `connect() -> Connection`, `UdpCtx -> UdpSocket`,
`ConnToken -> ConnId`.
