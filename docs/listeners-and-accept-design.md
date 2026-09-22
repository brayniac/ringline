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

Sequence: quiesce via the deferred-close path → unregister the fd from A →
package `(OwnedFd, leftover accumulator bytes, TLS state, ListenerId, PeerAddr,
handler payload)`, all `Send` → send over the tier-1 channel → B allocates a
slot, registers the fd, seeds the accumulator, re-arms multishot recv, and calls
a new `on_adopt` hook.

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

1. Does a `SO_REUSEPORT` socket excluded by a BPF selection program still
   accrue a backlog? Decides whether tier 2 works at all.
2. CBPF re-attach cost under churn — is unprivileged steering practical, or
   does useful steering require BPF privileges we do not want to demand of a
   server process?
3. Does the `tls-unbuffered` engine hold partial-record state that complicates
   moving a `ServerConnection` between workers? (Tier 3.)
4. Can a `Bytes` handed out by `with_bytes` still be alive at a park point? The
   quiescent-point rule should preclude it; it needs to be an assertion, not an
   assumption. (Tier 3.)
5. Does any client crate need to be generic over transport (redis and memcached
   both speak Unix sockets)? Decides whether §1's transport erasure is also
   needed on the outbound side.
6. Is per-listener TLS reachable without restructuring the handshake path, or
   does `TlsConfig` need to become per-connection state?

## Measurement

Two measurements decide the default accept mode. The second is the one that
constrains the design, and **its topology is the whole point**: measured from N
independent client addresses, hash placement looks fine and hides §5 entirely.
It must be a *single client host opening a pool*, which is the adversarial case
and also what every load generator here actually does.

- **Connect rate** under a connect storm — what merged mode should improve.
- **Per-worker connection counts**, one client host opening 8, 16 and 64
  connections to 8 workers. Expect round-robin to give exact thirds-free
  placement and an unsteered hash to leave ~37% of workers idle at N = W. A run
  that cannot show that difference is not evidence.

Both on two X710 guests, io_uring and mio, before and after, per the standing
send-path A/B mandate.

## Staging

1. Listener list + per-listener config (TLS, nodelay, backlog, bound address)
   + `ListenerId` on `Connection` (breaking builder change).
2. Acceptor payload `(RawFd, ListenerId, PeerAddr)`; retire the fake peer addr.
3. Merged accept mode behind config, io_uring multishot; mio keeps the pool.
4. BPF steering spike → tier 1 and tier 2 placement. Gates whether merged mode
   can ever be the default.
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
