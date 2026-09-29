# `connect()` resolves to a `Connection`, behind two builders

- **Status:** open — intent landed before building, per this journal's own rule
- **Span:** 2026-09-29 → · issue #528 · this PR · targets 0.7.0 (breaking)

Recording the design *before* implementing, because the change touches a public
surface across five crates and roughly 118 call sites, and because two of the
decisions below are ones a reader would otherwise have to re-derive: why the
argument is not the one the issue leads with, and why `Connection` is **not**
becoming generic over its transport.

## Goal

An accepted connection arrives as an owned `Connection`. An outbound one arrives
as a `ConnCtx`, and the caller claims the halves with a fallible `split()`. Make
`connect` resolve to a `Connection` too, and collapse the connect surface while
the window for a breaking change is open.

### The surface as it stands, measured

Fifteen entry points for one operation: five free functions (`io.rs:408`–`512`)
and five method forms each on `ConnCtx` (`io.rs:2371`–`2567`) and on `Connection`
(`io.rs:3739`–`3845`). **None of the ten method forms uses its receiver** —
`ConnCtx::connect` ignores `&self` and goes straight to `with_state`;
`Connection::connect` forwards to `self.tx.connect(addr)`, which ignores it too.
`Connection::connect()` currently returns a `ConnCtx`, which is the sharpest
single instance of the inconsistency this entry is about.

The five variants are an **incomplete cross product** of three axes — transport ×
TLS × timeout — at 5 of 8. The missing cell is load-bearing:
**`connect_unix_with_timeout` does not exist**, so a Unix connect cannot take a
timeout, and nothing in the naming says so. That is the failure mode of
combinatorial naming, and it is the argument for a builder: not that fifteen
names is a lot, but that the set is silently incomplete.

Submission is also **eager** — `connect()` pushes the SQE inside `with_state` and
returns `io::Result<ConnectFuture>`, so there are two error sites. In handler
code, which returns `()`, that is written in-tree as a nested double match
(`ringline/tests/echo.rs:4185`).

## The argument, which is not symmetry

Symmetry is what found this, and it is a good smell detector, but it is more often
a *consequence* of good reasoning than evidence of it. The test is whether an
asymmetry encodes a real difference. Here it does not — and it runs **backwards
relative to its own constraint**: accept is the side that cannot easily claim the
halves, because `spawn_accept_task` runs outside a task poll with no
`CURRENT_DRIVER` to reach, which is the entire reason `Connection::for_accept`
exists. `ConnectFuture::poll` already runs inside `with_state`. If either side had
a reason to hand back the unclaimed name, it was accept.

The load-bearing argument is that **`split()` at construction manufactures a
failure the caller did not cause**, and it is not hypothetical: `ConnCtx` is
`#[derive(Clone, Copy)]` (`io.rs:818`), so two copies can race `take_recv`/
`take_send` and one gets `EBUSY`. A non-`Copy` `Connection` makes that
unrepresentable rather than merely unlikely.

That distinction decides what "done" means. Chasing symmetry alone would change
the return type and stop, leaving the client constructors fallible — keeping the
exact cost the change exists to remove. Verified that `split()?` is the **only**
fallible step in every one of them, so each can become infallible:

| crate | constructor | fallible step |
|---|---|---|
| `ringline-redis` | `Client::new`, `builder(..).build()` | `lib.rs:535`, `lib.rs:715` — `split()?` only |
| `ringline-memcache` | `Client::new`, `build()`, `build_binary()` | `lib.rs:509`, `lib.rs:612`; `build_binary` calls `build()?` |
| `ringline-ping` | `Client::new`, `build()` | `lib.rs:179`, `lib.rs:228` — `split()?` only |
| `ringline-http` | `H2Conn::from_conn` | `h2_conn.rs:415` — `split()?` only |

## Plan

Two entry points, each returning its own builder type, both `IntoFuture`
(MSRV is 1.88, well past 1.64; this would be the crate's first `IntoFuture`):

```rust
let conn = connect(addr).await?;                               // Connection
let conn = connect(addr).timeout_ms(500).await?;
let conn = connect(addr).tls("example.com").await?;
let conn = connect(addr).tls("example.com").timeout_ms(500).await?;
let conn = connect_unix(path).await?;
let conn = connect_unix(path).timeout_ms(500).await?;          // the missing cell
```

**Type-state, not one flat builder.** `connect` yields a TCP builder and
`connect_unix` a Unix one, specifically so `connect_unix(path).tls(..)` stays
*unrepresentable* rather than becoming a runtime refusal. That matches the driver,
where `DriverCtx::connect_tls` takes a `SocketAddr` and no `connect_unix_tls`
exists. Collapsing the typed cross product into a single builder would have traded
a compile error for an `EINVAL`.

Then: the four client constructors take `Connection` and lose their `Result`; the
ten receiver-ignoring method forms go; callers needing the name keep
`Connection::token()` / `as_conn()`.

### GO criteria

Checkable, and the first is the one that matters:

1. The four client constructors are **infallible**. If some other fallible step
   turns up in one of them, the change loses its justification and should be
   reconsidered rather than shipped as a rename.
2. Entry points for connect: 15 → 2.
3. `connect_unix(path).timeout_ms(..)` compiles — the missing cell is closed.
4. `connect_unix(path).tls(..)` does **not** compile — no runtime refusal was
   introduced in exchange.
5. Tests and clippy green on **both** backends; no behavioural change to the mio
   path beyond the shared claim.

### NO-GO / reopen conditions

- If lazy submission proves to break a real usage pattern (below), keep eager
  submission and accept two error sites — the surface collapse still stands on
  its own, and the spurious-failure fix is independent of laziness.

## Rejected: making `Connection` generic over its transport

Considered `Connection<Tcp>`, `Connection<Unix>`, `Connection<Tls<Tcp>>`,
`Connection<Udp>`. Rejected, for four reasons, and the last is the one that would
have made it actively harmful:

1. **`on_accept` is one method serving every listener.** `bind()` and `bind_tls()`
   coexist — `worker.rs:693` documents serving "plaintext on one port and TLS on
   another" — so a single handler receives both, and which it is is not knowable
   until accept time. A generic `Connection` forces either an `on_accept` the
   runtime cannot monomorphize or a trait-surface explosion. `ListenerId` on
   `Connection` exists *because* that dispatch is runtime.
2. **The win is two accessors, and they are already honest.**
   `peer_addr() -> Option<PeerAddr>` and `tls_info() -> Option<TlsInfo>`: the enum
   and the `Option` are the correct encodings of a runtime property.
3. **`Udp` is not a `Connection`.** It is `send_to`/`send_to_gso`/`UdpToken`,
   datagram semantics with no accumulator and no ordered per-connection send
   queue. Including it to make the parameter list look complete would unify two
   different things.
4. **The type parameter would be unenforceable — it could lie.** `Connection` is
   `{ tx, rx }` over `ConnCtx { conn_index, generation }`: an index into one
   monomorphic driver table shared by every transport. TLS-ness is
   `tls_table.has(conn_index)`, a runtime lookup. `Connection<Tls<Tcp>>` would be
   `PhantomData` over a slot whose truth stays that lookup, and generation-based
   slot reuse means the tag can outlive the fact. This codebase's correctness
   model *is* generation-checked runtime validation (Domain Invariant 3). A
   compile-time tag the runtime cannot uphold is the "holds data it cannot
   retrieve" failure class wearing a safety costume; an enum that cannot lie beats
   a type parameter that can.

**The principle worth keeping**, because it is the same technique reaching
opposite verdicts a page apart: type-state pays where the **caller supplies** the
information and fails where the **runtime discovers** it. The connect builders
know their transport because the caller chose it. A handler does not know its
connection's transport or TLS-ness until the runtime tells it.

A narrower version stays available and is deliberately *not* folded in here:
`Connection::into_tls() -> Option<TlsConnection>`, one runtime check at the
boundary, after which TLS-only methods are available by type. That asserts nothing
the driver cannot back. Separate question, later.

## Costs, stated before building

- **Lazy submission is a behaviour change, not a rename.** Today `connect()`
  submits at call time, so `let a = connect(x)?; let b = connect(y)?;` has both in
  flight before either is awaited. A builder submits at first poll, so that
  pattern serializes unless the two are `join`ed — `join` preserves the
  parallelism, since each is polled and each submits before either waits. No
  in-tree caller relies on the old shape, but it belongs in the CHANGELOG as a
  behaviour change.
- Two new public types, and the crate's first `IntoFuture`.
- Breaking for the four client crates, so `ringline-redis`, `ringline-memcache`,
  `ringline-ping` and `ringline-http` all need version bumps in the same
  coordinated release.
- Roughly 118 `connect(` call sites across ringline's tests, examples and the
  bench crates.

## Outcome

Not yet implemented. This entry will be closed out in the implementing PR with
the GO criteria answered one by one.
