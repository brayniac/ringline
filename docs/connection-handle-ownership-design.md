# Connection handle ownership

- **Status:** proposal — intent only, nothing implemented
- **Motivated by:** #423, and four sibling defects found reviewing its fix

## The problem in one sentence

`ConnCtx` is `Copy`, and it is a handle to state that is exclusive — so every
exclusivity rule in the runtime has to be re-derived at run time, and the ones
we miss are silent hangs or wire corruption rather than compile errors.

## Evidence

`ConnCtx` derives `Clone, Copy`. It exposes **nine** recv entry points, all
`&self`, all mutually exclusive in practice:

`with_data`, `with_data_result`, `with_bytes`, `segments`,
`recv_owned_segment`, `with_segments`, `forward_to`, `forward_to_conn`,
`recv_ready`

Any two of them live at once is a bug. Nothing prevents any pairing, because a
second handle costs a `let`. Five compositions are known to be reachable, and
each one has already cost something:

| composition | consequence | how it is handled today |
|---|---|---|
| two `SegmentReader`s | one drops, settles the hold into the accumulator, the other strands | runtime check → `Err` |
| `with_segments` + live reader | remainder left in the accumulator, domain still segmented | nothing — latent |
| `with_data` then `segments()` | bytes stranded in the accumulator, connection hangs | **#423** — the adoption fix |
| `segments()` during a Mode A forward | adoption injects post-`len` overshoot into the forward's write | runtime refusal, added in #425 |
| stale `ConnCtx` after slot recycle | drains a *different* connection's accumulator | runtime generation check |

The send side is not exempt, which is the part that makes this a handle problem
rather than a recv problem. `forward_to_conn`'s own documentation states:

> The forward writes to the sink directly rather than through its send queue,
> so a `send` on the sink from elsewhere while a forward is running is not
> ordered against it and will interleave on the wire. **Forward *or* send on a
> given connection, not both at once.**

That rule is enforced by that sentence and nothing else.

## Why "the send queue already serialises sends" is not an answer

The per-connection send queue provides **non-corruption**: one `send()`'s bytes
reach the wire contiguously, and TLS records do not interleave. It says nothing
about the **order** two tasks' messages arrive in — that is whichever task the
executor polled first.

For a protocol, message order *is* the correctness property. A shared send
handle therefore advertises a guarantee the transport cannot supply. Ordering
is an application decision and needs an application-visible place to live.

## Proposal

**One owner per connection, for both directions.**

- Split `ConnCtx` into a send half and a recv half; make **both** non-`Copy`
  and non-`Clone`. Methods take `&mut self`.
- Fan-in (several tasks producing for one connection) becomes an explicit
  queue, using the `Sender`/`Receiver` already in `runtime/channel.rs`: many
  producers, one owning task, ordering decided by the queue's owner.
- `forward_to_conn` takes the sink's send half by `&mut`, so "forward or send,
  not both" becomes a borrow error.

What this converts from runtime to compile time:

| composition | after |
|---|---|
| two `SegmentReader`s | compile error |
| `with_segments` + live reader | compile error |
| `with_data` then `segments()` | compile error |
| `segments()` during a forward | compile error |
| send while the connection is a forward sink | compile error |

## What it does not fix

**Staleness.** A handle can outlive its connection, slots recycle, and handler
futures are `'static`, so a lifetime cannot bind a handle to a connection. The
generation check stays, permanently, and every entry point still needs a way to
*report* a stale handle — which several (`segments()`, `recv_owned_segment()`)
currently lack.

## Staging

This changes `AsyncEventHandler::on_accept`'s signature, so it reaches every
handler, example and test plus the six client crates. It does not have to be a
flag day:

1. **Make mode entry fallible** — `segments()` / `recv_owned_segment()` return
   `io::Result<_>`, refusing `EBUSY` on a live reader or forward and `EPIPE` on
   a stale generation. This is needed regardless of the rest: #425 had to refuse
   silently only because the signature has no error channel.
2. Add `ConnCtx::split()` (or `take_recv()`) additively, returning the halves.
   Both forms coexist; the old methods delegate.
3. Migrate the client crates and examples onto the halves.
4. Deprecate the direct methods; remove them in the next breaking release,
   flipping `on_accept` to hand out the halves directly.

Step 1 is independently valuable and cheap. Steps 2–4 are the real fix.

## Open questions

- Does any in-tree or downstream user need fan-out send badly enough that
  `Rc<SendHalf>` should be blessed rather than the channel pattern?
- Should the recv half be a single type with `&mut self` methods, or a typestate
  (`RecvHalf<Default>` / `RecvHalf<Segmented>`) so a mode switch is visible in
  the type? Typestate is stricter but interacts badly with `'static` futures.
- `forward_to` takes a `SinkFd`, not a connection; does it need the same
  treatment, or is the borrow of the `SinkFd` already sufficient?

## Why this is worth doing

Three separate bugs in one day traced to the same shape: a rule that was known,
written down in one place, and unenforced everywhere else — Mode A's
`arm_forward_source` had the adoption, the generation check and the `EBUSY`
refusal, and Modes B and C, written later against the same driver state,
inherited none of them. Divergent siblings against shared mutable state is the
recurring defect. Ownership is the mechanism that makes divergence impossible
rather than merely discouraged.
