# Listeners and the accept path

- **Status:** proposal — intent only, nothing implemented
- **Motivated by:** the single-listener limit found while reviewing whether
  `Connection` should split per transport

## The problem in one sentence

A ringline process can listen on exactly one socket, and the code that accepts
on it is the only part of an io_uring runtime that never touches the ring.

## Evidence

**One listener, last call wins.** `bind_addr` is a single `Option<BindAddr>`
(`worker.rs:442`); `bind()` (`:456`) and `bind_unix()` (`:465`) each *assign*
it. Calling both silently discards the first. Three methods away in the same
builder, `bind_udp()` (`:474`) pushes onto a `Vec` — the right pattern already
exists, in the other half of the API.

**Everything per-connection is therefore process-global, with the transport
difference papered over at run time:**

| what | today | consequence |
|---|---|---|
| TLS | `config.tls: Option<TlsConfig>` (`config.rs:200`) | cannot serve plaintext on 8080 and TLS on 8443 |
| `TCP_NODELAY` | `tcp_nodelay: if is_unix { false } else { … }` (`worker.rs:906`) | `ConfigBuilder::tcp_nodelay(true)` is accepted, documented, and silently ignored under `bind_unix` |
| bound address | `getsockname_v4_v6` → `Option`, `None` for Unix (`worker.rs:888`) | one address for one listener |
| peer address | acceptor sends a fabricated `0.0.0.0:0` for Unix (`acceptor.rs:104`) | the channel payload is `Sender<(RawFd, SocketAddr)>` — TCP-shaped |

**Accepts never enter the ring.** There is no accept opcode anywhere in
`backend/uring/`. The path is a dedicated thread blocking in `accept4`
(`acceptor.rs:33`), then a crossbeam send plus an eventfd write to wake the
target worker — a cross-thread handoff per connection, in a runtime whose
entire premise is that nothing crosses threads. The thread is also unpinned,
which is the hazard class behind the single-core funnel we root-caused
(i40e Flow-Director ATR plus unpinned threads under `isolcpus`).

**The crate's own architecture diagram describes a design we do not have.**
`lib.rs:52` reads *"Acceptors Thread (accept4() with SO_REUSEPORT)"*. There is
one acceptor thread, one socket, and `create_listener` is explicitly documented
*"without SO_REUSEPORT (just SO_REUSEADDR)"* (`worker.rs:1096`) with no
recorded reason.

**TLS handshakes run on the worker event loop**, inline in the completion path:
*"TLS path: defer accept until handshake completes"* (`backend/uring/event_loop.rs:1685`).
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
listener entry. The `if is_unix` branch at `worker.rs:906` then does not need
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
This mirrors what the UDP path does today (`backend/uring/driver.rs:1546`).

For a listener that cannot use `SO_REUSEPORT` (Unix), the merged mode degrades
to every worker arming multishot accept on one **shared** listen fd; the kernel
hands each connection to exactly one waiter.

mio has no multishot accept. It keeps the pool, or registers the shared
listener per worker with `EPOLLEXCLUSIVE`. Correct and non-pathological is the
bar there, not optimal.

### 5. Placement and steering

The pool mode's real advantage over the merged mode is *placement*: explicit
round-robin versus the kernel's 4-tuple hash, which is not uniform at low
connection counts. Connection distribution is the axis on which this project
has had its worst performance bug, so the merged mode needs an answer.

`SO_REUSEPORT` groups accept a BPF program that chooses the target socket:

- `SO_ATTACH_REUSEPORT_CBPF` — classic BPF returning an index into the group.
  Unprivileged. Cannot read a map, so changing the policy means re-attaching a
  new program via `setsockopt`. Coarse but sufficient for "exclude worker 3".
- `SO_ATTACH_REUSEPORT_EBPF` — `BPF_PROG_TYPE_SK_REUSEPORT` with a
  `BPF_MAP_TYPE_REUSEPORT_SOCKARRAY` and `bpf_sk_select_reuseport()`. Userspace
  updates the map at run time, so a worker leaves the rotation with no
  `setsockopt` and no socket close. Needs BPF load privileges.

Either gives the merged mode explicit placement. **Both need a kernel-behavior
spike before anything depends on them** — in particular whether a worker
removed from selection still accrues a backlog on its own socket.

Recorded hazard: closing a `SO_REUSEPORT` listener **resets** connections still
queued on it. Graceful shutdown in merged mode must stop accepting, drain the
queue, then close.

### 6. Rebalancing, in three tiers

Live migration is impossible by construction and is not proposed. The
`ConnectionTable` slot, `RecvAccumulator`, send queue and task-slab entry are
all thread-local, and `Connection`, `SendHalf`, `RecvHalf` and `ConnCtx` are
`!Send` via `PhantomData<*const ()>`. Moving a live connection means moving all
of that across a thread boundary, which is the negation of the design.

What is possible, cheapest first:

**Tier 1 — prevent.** Place at accept time, before the connection has any
state. In pool mode that is the existing round-robin. In merged mode, a worker
that accepts while over its share forwards the **raw fd** to the least-loaded
worker via the existing channel — at that point nothing exists but an integer,
so it is cheap and safe. One mechanism, two policies: the fd channel is the
primary path in pool mode and the exception path in merged mode.

**Tier 2 — steer.** Take an overloaded worker out of the accept rotation via
§5. No closes, no handshakes, reversible.

**Tier 3 — shed.** For imbalance that has already formed under long-lived
connections, hang up slowly so clients reconnect elsewhere.

Tier 3 is a **hint, not an action**: the runtime marks a worker as shedding and
the handler sheds when convenient. "Graceful" is protocol-specific — H2 has
GOAWAY, H1 has `Connection: close`, raw TCP has only FIN, and a request may be
in flight. The runtime knows the load; only the handler knows what a polite
goodbye looks like. A hint also keeps this inside the thread-local model:
nothing crosses a thread, a task simply decides to finish.

The economics are why this is last. Each shed connection costs a reconnect plus
a full TLS handshake — the expense §7 exists to reduce. And under an unsteered
hash, the replacement lands uniformly: with 8 workers a shed connection has a
1-in-8 chance of returning to the same worker, so it is a random walk toward
balance paid for in handshakes. Tier 2 makes it targeted — a shed connection
cannot return to the worker that shed it — which is the main reason tier 2 is
worth building before tier 3.

Guardrails: rate limit, hysteresis so it does not oscillate, prefer idle
connections, floor on per-worker count. Only triggers on live-connection counts
under heterogeneous lifetimes; uniform short-lived traffic re-hashes itself.

### 7. Handshake offload as its own knob

Where handshake crypto runs is orthogonal to how connections are accepted, and
should be configured separately rather than bundled into "pool mode".

A handshake is ~2 RTT of latency plus a burst of CPU (ECDHE and a signature).
Those want opposite treatment. A pool that blocks across the RTTs needs roughly
a thread per in-flight handshake — a 1000-connection storm is 1000 threads. A
pool that goes async to avoid that is an event loop, i.e. a worker. So the
offloadable part is the CPU burst, and the tool already exists:
`blocking_threads` (default 4, `config.rs:305`) with `spawn_blocking`.
`rustls::ServerConnection` is `Send`, so moving `process_new_packets` off the
event loop and back is mechanically possible.

Two handoffs per handshake is not obviously a win. This needs a prototype and a
measurement before it is more than a knob.

## Open questions

1. Does a `SO_REUSEPORT` socket excluded by a BPF selection program still
   accrue a backlog? Decides whether tier 2 is sufficient alone.
2. CBPF re-attach cost under churn — is unprivileged steering practical, or
   does useful steering require BPF privileges we do not want to demand?
3. Does any client crate need to be generic over transport (redis and
   memcached both speak Unix sockets)? Decides whether §1's transport erasure
   is also needed on the outbound side.
4. Is per-listener TLS reachable without restructuring the handshake path, or
   does `TlsConfig` need to become per-connection state?

## Measurement

Two measurements decide the default accept mode, and the second must be able
to show a regression or it is not evidence:

- **Connect rate** under a connect storm — what merged mode should improve.
- **Per-worker connection counts** at 8, 16 and 64 connections across 8
  workers — what merged mode could regress, and the axis of the single-core
  funnel bug.

Both on two X710 guests, io_uring and mio, before and after, per the standing
send-path A/B mandate.

## Staging

1. Listener list + per-listener config + `ListenerId` (breaking builder change).
2. Acceptor payload `(RawFd, ListenerId, PeerAddr)`; retire the fake peer addr.
3. Merged accept mode behind config, io_uring multishot; mio keeps the pool.
4. BPF steering spike → tier 1 and tier 2 placement.
5. Shed hint (tier 3).
6. Handshake offload knob, gated on its prototype measuring well.

Fix the stale `lib.rs:52` diagram in step 1 — it is the crate's front page and
it currently describes `SO_REUSEPORT` that does not exist.

This is a breaking builder change, so it batches with the naming work already
queued for the next release: `connect() -> Connection`, `UdpCtx -> UdpSocket`,
`ConnToken -> ConnId`.
