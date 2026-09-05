# Kernel TLS (kTLS) — scoping

**Status:** Scoping only. No code, no plan of tasks, no commitment. Every
load-bearing premise below carries a verification record; the ones that could
not be verified are labelled, not smoothed over.
**Date:** 2026-09-05
**Related:** [`tls-unbuffered-design.md`](tls-unbuffered-design.md),
[`journal/2026-09-unbuffered-tls.md`](journal/2026-09-unbuffered-tls.md),
[`send-completion-design.md`](send-completion-design.md),
[`syscalls-and-copies.md`](syscalls-and-copies.md)
**Premise-check experiments:** `experiments/ktls-premise-probe.toml`,
`experiments/ktls-uring-probe.toml` (SystemsLab, hv01 / `z2.baremetal`)

---

## Why this document is shaped the way it is

kTLS was the original request. It was deferred in favour of an unbuffered TLS
engine whose design doc claimed a 2 → 1 copy reduction. **That premise was
false**, and it survived twenty-five mutation checks, four adversarial
reviewers and two review gates per task, because every check pointed inward at
ringline while the false claim was about a *dependency*. It was caught only by
an allocation counter: 263,976 vs 264,000 bytes/op across the two engines.
See [`journal/2026-09-unbuffered-tls.md`](journal/2026-09-unbuffered-tls.md),
"Plan 4 — measurement".

So this document's organising rule is: **a premise is not usable until it has
been read in the primary source, and the document must say where.** Section
[Premise register](#premise-register) is the index; every claim in the body
carries a `[P-n]` tag pointing at it.

Two of the prior effort's own kTLS claims turn out to be **wrong**, and they
are the two that mattered most:

> **Correction 1 — "Software kTLS does not remove the remaining copy."**
> ([`tls-unbuffered-design.md:142`](tls-unbuffered-design.md), and the copy
> table at `:120`.) It removes **two of the three** memcpy-class passes on the
> send path, and on the fast path it hands the AEAD the caller's pages
> directly. The design doc's reasoning — "`sendmsg` copies plaintext into the
> kernel, which encrypts in place, the same count as userspace" — got the
> kernel half roughly right and the userspace half wrong, in the same way the
> falsified copy claim did. See [P-1], [P-2], [P-3].

> **Correction 2 — "Only NIC crypto offload (`TLS_HW`) reaches zero copies."**
> (`tls-unbuffered-design.md:121`, `:143`, `:598`.) `TLS_HW` does **not** reach
> zero copies on the ordinary `sendmsg` path: `tls_device.c` copies plaintext
> into skb frags exactly as `tls_sw.c` does, and the NIC encrypts during DMA.
> Zero requires `MSG_SPLICE_PAGES`, which is a `sendfile`/`vmsplice` path
> ringline does not have and cannot easily get (see
> [Question 4](#4-what-breaks)). `TLS_HW`'s win is **removing the CPU
> crypto**, not removing copies. See [P-6], [P-7].

Neither correction rescues the project: see
[Question 2](#2-is-tls_hw-reachable-on-hardware-we-own) and the
[recommendation](#recommendation).

---

## GO/NO-GO criteria

Stated first, and deliberately falsifiable. The prior effort's criterion was
well-formed and the effort still failed it — that is the system working, and
the criteria below are written to fail the same way if the premises are wrong.

**G0 — premise gate. Runnable before a line of implementation code, and it
must be run first.** Extend the microbenchmark harness that already exists on
the local `bench/tls-send-microbench` branch (the one that produced the
allocation counter) with a third arm: a real kTLS socket, keys installed by
hand, plaintext sent with `IORING_OP_SEND` **and no `MSG_WAITALL`** — kTLS
refuses it [P-19], so ringline's own `STREAM_SEND_FLAGS` cannot be used
verbatim. On hv01, at 1 KiB / 16 KiB / 64 KiB / 256 KiB / 1 MiB, report:

- **bytes allocated per operation inside the measured call**, the number that
  settled the last effort. Prediction from source: the kTLS arm allocates
  approximately **zero** in userspace, against ~264,000 for both rustls
  engines at 256 KiB. If the kTLS arm's allocation is comparable to the rustls
  arms, [P-1]/[P-2] are wrong and the effort stops here.
- **the cold-payload control.** Run the payload cold so each copy costs ~4.5×
  more (the journal's measured factor). Prediction: the kTLS arm's advantage
  **widens sharply**, because the copies it removes are real. If the gap does
  not move, no copy was removed — the exact discriminator that caught the last
  false premise.
- **`/proc/net/tls_stat`** before and after, to prove the traffic went through
  `TlsTxSw` and not through some fallback. [P-8]

**GO** only if *all* of:

1. **G1 — copies.** G0 shows the kTLS arm allocating ~0 in userspace **and**
   the cold-payload control widening the gap. Both, not either.
2. **G2 — throughput.** A two-machine TLS benchmark on the rig (never
   co-located — this project withdrew `BENCHMARKS.md` in #205 over exactly
   that) shows the kTLS server beating the rustls server by a margin larger
   than the run-to-run spread, at ≥ 2 payload sizes. The bar is
   [`BENCHMARKS.md`](../BENCHMARKS.md)'s methodology, not its numbers.
3. **G3 — correctness envelope.** Interop against a rustls peer and an OpenSSL
   peer, both directions, including: a TLS 1.3 `key_update` from the peer, a
   `close_notify` from each side, a mid-stream `new_session_ticket`, and a peer
   that coalesces its final handshake flight with application data. A `close()`
   from inside a handler still sends `close_notify` (note the pre-existing
   io_uring bug at `runtime/io.rs` `ConnCtx::close()`, journal "Backlog" item 3
   — kTLS must not be built on top of it).
4. **G4 — fallback.** Every unsupported case (non-AES-GCM/ChaCha suite, kernel
   without `CONFIG_TLS`, `setsockopt` refusal, `TLS_HW` absent, rekey needed on
   a kernel < 6.14) silently and correctly falls back to the userspace engine,
   with a test per case.

**NO-GO** if any of:

- **N1** — G0's allocation counter shows no reduction, or the cold-payload
  control does not widen the gap. *(Falsifies the whole premise, cheaply.)*
- **N2** — the rekey constraint [P-5] cannot be met on the target kernel and
  the fallback is "tear the connection down", **and** the deployment target is
  long-lived connections. At 2^24 records × 16 KiB = **256 GiB per direction**
  before AES-GCM's confidentiality limit forces a rekey [P-9], a saturated
  100 GbE connection hits it in ~22 s. On kernel 6.12 that is a forced close.
- **N3** — the RX conversion (`RecvMulti` → `RecvMsgMulti` on TCP, both
  backends) cannot be done without regressing the segmented-recv or
  direct-echo paths. [P-12]
- **N4** — the send-ordering hazard at the handshake→kTLS switch (see
  [Question 4](#4-what-breaks), "The switch is the sharp edge") has no clean
  answer within the per-connection send queue.
- **N5** — reinstating a per-connection short-send resubmit loop for kTLS
  connections (forced by [P-19]: kTLS refuses `MSG_WAITALL`) costs more than
  the copies kTLS removes. This is measurable in G2 and should be watched for
  specifically, because it is a regression against a shipped, argued decision
  ([`send-completion-design.md`](send-completion-design.md) §2).

**Explicitly not a GO criterion:** `TLS_HW`. It is unreachable on every host we
own [P-7] and cannot be a gate on work we could validate.

---

## Premise register

Every load-bearing claim, its verification status, and where to check it.
"Verified" means the primary source was opened and the cited lines read, or the
probe was run and its output is in the experiment log.

| # | Premise | Status | Source |
|---|---|---|---|
| P-1 | rustls' userspace encrypt costs **two** memcpy-class passes over the payload, on *both* engines | **Verified** | `rustls-0.23.41/src/common_state.rs` `write_fragments`; `src/crypto/ring/tls13.rs` `Tls13MessageEncrypter::encrypt`. Quoted in [`journal/2026-09-unbuffered-tls.md`](journal/2026-09-unbuffered-tls.md) "Evidence 2", and independently re-read here |
| P-2 | `tls_sw` **aliases** the plaintext scatterlist onto the ciphertext pages, so the ordinary kTLS send path is 1 copy + in-place AEAD | **Verified** | `net/tls/tls_sw.c:330-350` (`tls_clone_plaintext_msg`, `sk_msg_clone(sk, msg_pl, msg_en, skip, len)`), `:799` / `:806` (`sg_chain` of `sg_aead_in`→`msg_pl`, `sg_aead_out`→`msg_en`), `:1140` + `:1156` (the `tls_clone_plaintext_msg` call, then `sk_msg_memcopy_from_iter`), v6.12 |
| P-3 | `tls_sw` has a **faster** path that pins the caller's pages and never copies plaintext into the kernel at all | **Verified in source; not instrumented** | `net/tls/tls_sw.c:1103-1106` — `if (!is_kvec && (full_record \|\| eor) && !async_capable) sk_msg_zerocopy_from_iter(...)`; `net/core/skmsg.c:311-366` uses `iov_iter_get_pages2`. TX `async_capable` is zeroed by `kzalloc` (`tls_sw.c:2619`) and only ever set to 1 on an encryption *error* (`:829`) |
| P-4 | kTLS is all-or-nothing per connection | **Verified, and refined** | Both `dangerous_extract_secrets(self)` and its successor take `self`: `rustls-0.23.41/src/client/client_conn.rs:931`, `:944`; `src/server/server_conn.rs:961`, `:974`. **But** rustls ships a purpose-built kTLS API — see P-14 |
| P-5 | Linux **6.12 cannot rekey** a kTLS socket; 6.14 can, TLS 1.3 only | **Verified in source and measured on hv01** | v6.12 `net/tls/tls_main.c:636-638` — `/* Currently we don't support set crypto info more than one time */ ... return -EBUSY;`. v6.14 `net/tls/tls_main.c:639-653` adds the TLS 1.3 update path (absent in v6.13; present in v6.14-rc1). Probe on hv01: `second TLS_TX setsockopt -> Device or resource busy (errno 16)` |
| P-6 | `TLS_HW` (device offload) also copies plaintext into the kernel on the ordinary `sendmsg` path | **Verified** | `net/tls/tls_device.c:493-524` — `MSG_SPLICE_PAGES` branch does `iov_iter_extract_pages`; the `else if (copy)` branch does `tls_device_copy_data(...)`, which is `copy_from_iter` + `copy_from_iter_nocache` (`:393-416`) |
| P-7 | **No host we own supports `TLS_HW`** | **Verified in source and measured on hv01** | Only four in-tree `tlsdev_ops` implementations in v6.12: mlx5 (`en_accel/ktls.c:89`, TX+RX), Chelsio ch_ktls (`chcr_ktls.c:2132`, TX), Netronome nfp (`crypto/tls.c:465`, TX+RX), Fungible funeth (`funeth_ktls.c:126`, TX). Sweeping **every** `.c`/`.h` file in `drivers/net/ethernet/amazon/ena` (14 files), `intel/i40e` (38) and `intel/ice` (121) for `NETIF_F_HW_TLS`/`tlsdev_ops`/`tls_dev_add` yields **zero** hits. hv01 probe: **every** interface reports `tls-hw-tx-offload: off [fixed]` |
| P-8 | kTLS works end-to-end on hv01: ULP attach, key install, encrypt, decrypt | **Measured** | `experiments/ktls-premise-probe.toml`, experiment `01a07046-b5fa-7182-77b8-d0800cb0c828`: kernel `6.12.90+deb13.1-amd64`, `CONFIG_TLS=m`, `CONFIG_TLS_DEVICE=y`, `CONFIG_TLS_TOE` not set. 8/8 checks PASS |
| P-9 | AES-GCM's confidentiality limit is 2^24 records under rustls; ChaCha20-Poly1305 has none | **Verified** | `rustls-0.23.41/src/crypto/ring/tls13.rs:28` (`u64::MAX` for ChaCha20), `:48` / `:70` (`1 << 24` for AES-256-GCM / AES-128-GCM). rustls' `KernelConnection` explicitly does **not** track this: `src/conn/kernel.rs:37-52` |
| P-10 | A control record read with plain `recv()` fails with **EIO** | **Verified in source and measured** | `net/tls/tls_sw.c:1752-1772` — `tls_record_content_type` returns `-EIO` when the record is not `TLS_RECORD_TYPE_DATA` and `put_cmsg` failed or `MSG_CTRUNC` is set. hv01 probe: `PASS plain recv() of a control record fails with EIO` |
| P-11 | `IORING_OP_URING_CMD` / `SOCKET_URING_OP_SETSOCKOPT` accepts **any** level, so kTLS keys can be installed on a **fixed** descriptor | **Verified in source** | `io_uring/uring_cmd.c:313-329` (`io_uring_cmd_setsockopt`) has **no** level check, unlike `io_uring_cmd_getsockopt` at `:287-297` which is `SOL_SOCKET`-only. Exposed by the pinned `io-uring` 0.7.12 crate as `opcode::SetSockOpt`, whose `fd` is `impl sealed::UseFixed` (`opcode.rs:645-673`). `io_uring_cmd_sock` also requires `prot->ioctl` to be non-NULL (`uring_cmd.c:337-341`); the TLS ULP copies the base `tcp_prot` wholesale (`net/tls/tls_main.c:904`, `prot[TLS_BASE][TLS_BASE] = *base;`) and never overrides `.ioctl`, so it survives the ULP swap |
| P-12 | ringline's default TCP recv path is plain `RecvMulti` on io_uring and `Read::read` on mio, neither of which can carry cmsgs | **Verified** | `backend/uring/event_loop.rs:3617` `arm_recv` — the `timestamps` branch is `:3618-3634`, the unconditional fallthrough is `:3637` `submit_multishot_recv` → `backend/uring/ring.rs:199`; mio `backend/mio/event_loop.rs:534`, `:625`. The `RecvMsgMulti` TCP path exists only behind the default-off, CI-untested `timestamps` feature (`ringline/Cargo.toml:21`; handler `backend/uring/event_loop.rs:1453-1584`) |
| P-13 | ringline has **no** `splice`/`sendfile` path in either direction | **Verified** | `grep -rn 'splice\|sendfile' ringline/src/` returns nothing. `ConnCtx::forward_to` is `opcode::Send` from a provided recv buffer (`backend/uring/ring.rs:340-355`); the file sink is `opcode::Write` (`:364`); NVMe is `IORING_OP_URING_CMD` into user memory (`ring.rs:912`) |
| P-14 | rustls has a **dedicated kTLS API** that survives the handover and handles key updates and session tickets | **Verified** | `rustls-0.23.41/src/conn/kernel.rs` (whole module); entry points `UnbufferedClientConnection::dangerous_into_kernel_connection` (`src/client/client_conn.rs:944`) and the server twin (`src/server/server_conn.rs:974`). `dangerous_extract_secrets` is **`#[deprecated]`** in favour of it (`client_conn.rs:929-931`) |
| P-15 | `enable_secret_extraction` is required, defaults to `false`, and ringline never sets it | **Verified** | Gate at `rustls-0.23.41/src/conn.rs:1198-1203`. `grep -rn enable_secret_extraction` over the ringline tree: zero hits. ringline never *builds* a rustls config — it wraps a caller-supplied `Arc` (`ringline/src/config.rs:14`, `:28`) |
| P-16 | TLS 1.3 RX decrypt-straight-into-the-caller's-buffer needs `TLS_RX_EXPECT_NO_PAD` | **Verified in source and measured** | `net/tls/tls_sw.c:2606-2612` — `zc_capable = tls_ctx->rx_no_pad \|\| version != TLS_1_3_VERSION`; used at `:2031-2033`. hv01 probe: `PASS TLS_RX_EXPECT_NO_PAD accepted` |
| P-17 | The kernel's cipher set covers everything rustls can hand it | **Verified** | Kernel: `net/tls/tls_main.c:102-111` (AES-GCM-128/256, AES-CCM-128, ChaCha20-Poly1305, SM4-GCM/CCM, ARIA-GCM-128/256). rustls: `ConnectionTrafficSecrets` has exactly three variants — `Aes128Gcm`, `Aes256Gcm`, `Chacha20Poly1305` (`src/suites.rs:218-242`). All three are supported by the kernel |
| P-18 | The unbuffered engine's rustls connection is in a **consumable owned position** | **Verified** | `TlsTable.conns: Vec<Option<TlsConn>>` (`tls/mod.rs:244`) → `TlsConn.conn: TlsConnKind` (`:224`) → `TlsConnKind::Unbuffered(UnbufferedConn)` (`:123`) → `UnbufferedConn.kind: UnbufferedKind` (`unbuffered/mod.rs:129`) → the rustls type. No `Rc`/`Arc`/pin/slab indirection. Only the *accessors* are `&mut`-only |
| P-19 | **kTLS rejects `MSG_WAITALL` outright.** `tls_sw_sendmsg` and `tls_device_sendmsg` fail any flag outside a small allow-list with `-EOPNOTSUPP`, and ringline sets `MSG_WAITALL` on every stream send | **Verified in source and measured on hv01** | v6.12 `net/tls/tls_sw.c:1231-1234` — allow-list is `{MSG_MORE, MSG_DONTWAIT, MSG_NOSIGNAL, MSG_CMSG_COMPAT, MSG_SPLICE_PAGES, MSG_EOR, MSG_SENDPAGE_NOPOLICY}`; unchanged in v6.17 (`:1255-1258`). `tls_device.c:436-439` is narrower still. ringline: `STREAM_SEND_FLAGS = libc::MSG_WAITALL` (`ringline/src/completion.rs:1-10`), used by every stream send builder. Probe on hv01: `IORING_OP_SEND` with `MSG_WAITALL` on a kTLS fixed descriptor returned **-95 (`EOPNOTSUPP`)** |
| P-20 | kTLS keys can be installed on an io_uring **fixed** descriptor | **Measured on hv01** | Probe: `IORING_OP_URING_CMD/SOCKET_URING_OP_SETSOCKOPT set SOL_TCP/TCP_ULP="tls" on a FIXED descriptor (res=0)` and `SetSockOpt(SOL_TLS, TLS_TX, aes-128-gcm) accepted on a FIXED descriptor (res=0)`. Confirms P-11 end to end |
| U-1 | Whether io_uring's `IORING_OP_SEND` actually reaches `sk_msg_zerocopy_from_iter` in practice | **UNVERIFIED** | Source says it should — io_uring builds `ITER_UBUF`, so `is_kvec` is false, and `eor` is true without `MSG_MORE`. But `iov_iter_get_pages2` needs the issuing context's `mm`, and io-wq offload is a different context. Not instrumented. **This is the single premise G0 exists to settle.** |
| U-2 | Whether `IORING_OP_SEND_ZC` on a kTLS socket avoids the pool-slot copy | **UNVERIFIED, and suspected no** | `net/tls` contains no `MSG_ZEROCOPY`/`msg_ubuf` handling. The probe only records whether the CQEs are well-formed, which is not the same question |
| U-3 | The cost of a copy at 400/800 GbE | **UNVERIFIED, and out of reach** | Inherited verbatim from `tls-unbuffered-design.md:122-133`. The one number this project has is ~3.2 GiB/s/core on 200 GbE Graviton4. Do not extrapolate it into a core count |
| U-4 | Whether kTLS helps or hurts at ringline's *actual* payload sizes | **UNVERIFIED** | Every effect described here scales with payload. ringline's shipped benchmarks are dominated by 64 B–1 KiB ops, where per-syscall and per-record overhead dominate copies. G2 exists because of this |

---

## 1. What software kTLS actually buys

**Short answer: on the send path, source says it removes two of the three
memcpy-class passes over the payload — considerably more than the prior design
doc claimed. On the receive path it removes one. Neither has been measured, and
G0/N1 exist to settle the send half before anything is built.**

### Counting convention

The last effort's confusion was partly a counting problem, so state it. Count
**memcpy-class passes over the payload**, from ringline's caller buffer to the
point the NIC can DMA it, *excluding* the one unavoidable AEAD read→write. A
pass is a `memcpy`/`copy_from_iter`/`copy_from_slice` of the bulk data. Where
the AEAD reads one buffer and writes another, that is the crypto pass, not an
extra copy.

`docs/syscalls-and-copies.md` uses a narrower convention (userspace only, the
kernel's own `sendmsg` copy not counted). Both are given below so neither can
be quoted out of context.

### Send path

| | userspace passes | kernel passes | total (this doc) | `syscalls-and-copies.md` convention |
|---|---|---|---|---|
| **ringline today** (buffered *or* unbuffered engine) | 2 | 1 | **3** | 2 |
| **kTLS_SW, ordinary path** [P-2] | 1 (pool slot) | 1 | **2** | 1 |
| **kTLS_SW, pinned-pages path** [P-3] | 1 (pool slot) | 0 | **1** | 1 |
| **kTLS_HW** [P-6] | 1 (pool slot) | 1 | **2**, and no CPU crypto at all | 1 |
| kTLS + `MSG_SPLICE_PAGES` | 0 | 0 | **0** — but unreachable, see [Question 4](#4-what-breaks) | 0 |

Where each number comes from:

**ringline today = 3.** rustls 0.23.41 `WriteTraffic::encrypt` (and
`writer().write()` on the buffered engine, which reaches the same code) goes
`write_plaintext` → `CommonState::write_fragments`, which per fragment does
`let em = self.record_layer.encrypt_outgoing(m).encode();` then
`outgoing_tls[written..].copy_from_slice(&em)`. `Tls13MessageEncrypter::encrypt`
allocates a fresh `PrefixedPayload` per record, `extend_from_chunks` copies the
plaintext in (**pass 1**), seals in place, and the `copy_from_slice` above moves
the finished record into the pool slot (**pass 2**). `IORING_OP_SEND` then has
`tcp_sendmsg` copy the pool slot into skb pages (**pass 3**). [P-1]

**kTLS_SW ordinary = 2.** ringline still owns the SQE memory, so the caller's
bytes are copied into a `SendCopyPool` slot (**pass 1**) — Domain Invariant 1,
unchanged. Inside `tls_sw_sendmsg_locked`, `tls_alloc_encrypted_msg` allocates
the ciphertext pages, then `tls_clone_plaintext_msg` makes the *plaintext*
scatterlist reference **the same pages**, offset by `prepend_size`:

```c
/* net/tls/tls_sw.c:330-350 (v6.12) */
static int tls_clone_plaintext_msg(struct sock *sk, int required)
{
	...
	/* We add page references worth len bytes from encrypted sg
	 * at the end of plaintext sg. It is guaranteed that msg_en
	 * has enough required room (ensured by caller).
	 */
	len = required - msg_pl->sg.size;

	/* Skip initial bytes in msg_en's data to be able to use
	 * same offset of both plain and encrypted data.
	 */
	skip = prot->prepend_size + msg_pl->sg.size;

	return sk_msg_clone(sk, msg_pl, msg_en, skip, len);
}
```

`sk_msg_memcopy_from_iter` then copies the pool slot into those shared pages
(**pass 2**), and `tls_do_encryption` runs the AEAD with `sg_aead_in` chained to
`msg_pl` (`:799`) and `sg_aead_out` chained to `msg_en` (`:806`) — the same
pages — i.e. **in place**. That is one pass fewer than ringline pays today, and
it is the case the prior design doc described. It was right about this path and
wrong to think userspace matched it. [P-2]

**kTLS_SW pinned-pages = 1, and this is the interesting one.** Before it
reaches the copy path, `tls_sw_sendmsg_locked` tries:

```c
/* net/tls/tls_sw.c:1103-1106 (v6.12) */
if (!is_kvec && (full_record || eor) && !async_capable) {
	u32 first = msg_pl->sg.end;

	ret = sk_msg_zerocopy_from_iter(sk, &msg->msg_iter,
					msg_pl, try_to_copy);
```

`sk_msg_zerocopy_from_iter` (`net/core/skmsg.c:311-366`) calls
`iov_iter_get_pages2` and hangs the *caller's* pages off `msg_pl`. The AEAD
then reads those pages and writes the separately allocated `msg_en` pages — so
the plaintext is **never copied into the kernel**. The conditions are all
satisfied by an ordinary io_uring send:

- `is_kvec` is false — io_uring imports a user pointer, giving `ITER_UBUF`.
- `eor = !(msg_flags & MSG_MORE)` is true — ringline sets no `MSG_MORE`.
- `async_capable` for TX is zero out of `kzalloc` (`tls_sw.c:2619`) and is only
  ever set to 1 in the *error* arm of `tls_push_record` (`:829`). With a
  synchronous AEAD (AES-NI) it stays 0 for the life of the socket.

`num_zc` then forces `tls_encrypt_async_wait` before `sendmsg` returns
(`tls_sw.c:1206-1215`), so the pool slot is free the moment the CQE lands —
ringline's slot lifecycle is unaffected. Sub-conditions that fall back
gracefully to the 2-pass path: exceeding `MAX_MSG_FRAGS` scatter entries
(`= MAX_SKB_FRAGS`, `include/linux/skmsg.h:16`), and any `-EFAULT` from the
page pin (`goto fallback_to_reg_send` at `:1109`, label at `:1134`).

**This is the load-bearing unverified step.** [U-1] The source says it should
happen; nothing has confirmed io_uring actually lands there rather than in the
copy path, and io-wq offload runs in a different context than the submitting
task. **G0 exists precisely to test this, and N1 kills the effort if it fails.**
Do not build on it before then. That is the whole lesson of the previous
effort.

### Receive path

Today, per received record: kernel DMA into a `ProvidedBufRing` buffer (0
passes) → `CiphertextBuf::append` copies the ciphertext into a contiguous
buffer, because `process_tls_records` needs one (**pass 1**) → rustls decrypts
into an owned `Vec` (`ReadTraffic::next_record`; the crypto pass) → that
plaintext is copied into the `RecvAccumulator` (**pass 2**).

Under kTLS RX: `tls_sw_recvmsg` with `darg.zc` decrypts **directly into the
caller's iovec** — i.e. straight into ringline's provided buffer — and
`CiphertextBuf` disappears entirely (**pass 1**: provided buffer →
`RecvAccumulator`, which the plaintext path already pays). `zc` requires
`TLS_RX_EXPECT_NO_PAD` on TLS 1.3 (`tls_sw.c:2606-2612`), which hv01 accepts
[P-16]. Without it, the kernel decrypts into skb pages and copies to the user
buffer — still 2 passes, i.e. no worse than today.

So RX is a 2 → 1 improvement, conditional on the `NO_PAD` promise. Note the
promise is exactly that. If the peer *does* pad, `tls_decrypt_sw`
(`tls_sw.c:1651-1658`) sets `darg->zc = false`, bumps `TLSRXNOPADVIOL` and
`TLSDECRYPTRETRY`, and **decrypts the record a second time**:

```c
/* net/tls/tls_sw.c:1651-1658 (v6.12) */
/* If opportunistic TLS 1.3 ZC failed retry without ZC */
if (unlikely(darg->zc && prot->version == TLS_1_3_VERSION &&
             darg->tail != TLS_RECORD_TYPE_DATA)) {
        darg->zc = false;
        ...
        return tls_decrypt_sw(sk, tls_ctx, msg, darg);
}
```

So the downside is 2× AEAD cost per padded record, not a correctness bug — but
it is a cliff a hostile peer can trigger at will by padding every record, and
`/proc/net/tls_stat`'s `TlsRxNoPadViolation` is the counter that would show it.
Whether to take the `NO_PAD` promise at all is a real security-adjacent
trade-off, not a free knob.

### The honest caveat

**Every number above is structural, from source, and unmeasured.** The last
effort's failure was accepting exactly this kind of reasoning as settled. The
difference here is that the numbers are being stated as *predictions with a
named discriminator* (G0's allocation counter and cold-payload control) rather
than as results — and the criterion N1 says what happens if they are wrong.

Also unmeasured, and possibly decisive [U-4]: **whether any of this matters at
ringline's payload sizes.** At 64 B–1 KiB, copies are not the cost; syscalls,
records, and per-CQE work are. kTLS adds no syscalls but does not remove any
either. It is entirely possible that kTLS is a clear win at 256 KiB and inside
noise at 1 KiB, which is where ringline's benchmarks live.

---

## 2. Is `TLS_HW` reachable on hardware we own

**No. Not on any host, on any rig, today. This is the headline.**

### What exists in-tree

Four drivers implement `tlsdev_ops` in Linux 6.12 [P-7]:

| Driver | Path | Direction | Hardware |
|---|---|---|---|
| mlx5 | `drivers/net/ethernet/mellanox/mlx5/core/en_accel/ktls.c:89` | TX + RX (RX capability-gated, `:127`) | NVIDIA/Mellanox ConnectX-6 Dx and later |
| Chelsio ch_ktls | `drivers/net/ethernet/chelsio/inline_crypto/ch_ktls/chcr_ktls.c:2132` | TX only | Chelsio T6 |
| Netronome nfp | `drivers/net/ethernet/netronome/nfp/crypto/tls.c:465` | TX + RX (`:591-596`) | Netronome Agilio |
| Fungible funeth | `drivers/net/ethernet/fungible/funeth/funeth_ktls.c:126` | TX only | Fungible DPU — vendor defunct |

The **Direction** column is from the kernel source cited. The **Hardware**
column is vendor knowledge, *not* verified from kernel source — treat the
specific part numbers as indicative. What is verified is that these four
drivers, and only these four, install `tlsdev_ops`.

(`drivers/net/ethernet/chelsio/inline_crypto/chtls/` is a different, older
mechanism — TLS_TOE, full TCP offload. hv01's kernel has `CONFIG_TLS_TOE` **not
set** [P-8], and the kernel community treats it as legacy.)

### What we have

| Rig | NIC | Driver | `TLS_HW`? |
|---|---|---|---|
| **hv01** (`z2.baremetal`, the kTLS-capable host) | Intel X710 ×3, Aquantia AQC 10G, Intel i350, Intel VFs | `i40e`, `atlantic`, `igb`, `iavf` | **No** — every interface reports `tls-hw-tx-offload: off [fixed]`, `tls-hw-rx-offload: off [fixed]`, `tls-hw-record: off [fixed]` |
| **hv02** (`z1.baremetal`) | X710 (same fabric, per `experiments/tcp-fanin-audit-ab.toml:14-18`) | `i40e` | **No** — all 38 `.c`/`.h` files of `drivers/net/ethernet/intel/i40e` in v6.12 contain zero `NETIF_F_HW_TLS`/`tlsdev_ops`/`tls_dev_add` references. *Not probed — inferred from the driver, not measured on the host* |
| **AWS** (`z1.g.small`, `z2.g.medium`, and the Graviton4 c8gn rigs) | ENA | `ena` | **No** — all 14 `.c`/`.h` files of `drivers/net/ethernet/amazon/ena` in v6.12 contain zero such references. *Not probed — inferred from the driver, not measured on the host* |

`[fixed]` in `ethtool -k` output means the driver does not implement the
feature at all — it is not a configuration that can be turned on. Measured, not
inferred: experiment `01a07046-b5fa-7182-77b8-d0800cb0c828`.

### What follows

1. **The zero-copy endpoint the prior design doc pointed at does not exist for
   us**, and would not exist even with the right NIC, because `TLS_HW` is not
   zero-copy either [P-6]. It is *zero-CPU-crypto*, which is a different and
   arguably larger win — but one we cannot measure, cannot regression-test, and
   cannot ship a claim about.
2. **`TLS_HW` cannot be a GO criterion, a milestone, or a motivation.** Any
   argument of the shape "kTLS is the delivery mechanism for NIC offload" is
   an argument about hardware nobody in this project can put a job on. Buying a
   ConnectX-6 Dx is the prerequisite for even scoping it.
3. **What is left is software kTLS**, which stands or falls entirely on
   [Question 1](#1-what-software-ktls-actually-buys) — and on G0.

---

## 3. What is the real scope

Costed honestly. "Small/Medium/Large" is relative to the unbuffered engine
(4 PRs, ~2,600 lines including tests), which is the only comparable in-tree
reference point.

### 3.1 Prerequisites ringline does not have

| # | Item | Size | Note |
|---|---|---|---|
| 1 | `enable_secret_extraction` on the rustls config | **Small, but a public-API decision** | It gates everything [P-15], defaults false, and ringline never builds a rustls config — it wraps a caller-supplied `Arc<ServerConfig>` (`config.rs:14`). So either the caller must set it (documented requirement, silently breaks kTLS if forgotten) or ringline must own config construction (a real public-surface change, against the project's "wrap, don't build" stance). **Decide this before anything else** |
| 2 | By-value access to the rustls connection | **Small** | Two accessors: `TlsTable::take(conn_index) -> Option<TlsConn>` and `UnbufferedConn::into_kind(self)`. The ownership chain is already consumable [P-18] |
| 3 | TCP `RecvMsgMulti` on **both** backends | **Large** | See 3.3 |
| 4 | `setsockopt` on a fixed descriptor | **Small** | `opcode::SetSockOpt` exists in the pinned `io-uring` 0.7.12 and takes `impl UseFixed` [P-11]; the kernel imposes no level restriction on the setsockopt direction. Note the `optval` (which holds **key material**) must satisfy Domain Invariant 1 — live until the CQE — and be zeroized after |
| 5 | A second short-send strategy | **Medium, and unbudgeted anywhere** | kTLS refuses `MSG_WAITALL` [P-19], so kTLS connections need the userspace partial-resubmit loop that `MSG_WAITALL` was landed to remove. Two strategies coexisting in `submit_next_queued`/`handle_send`, keyed on connection type |
| 6 | A cmsg helper | **Small** | Two hardcoded single-option parsers duplicate the same walk today (`backend/uring/event_loop.rs:1589-1631`, `backend/udp_gro.rs:21-52`). Extract one iterator |

### 3.2 The unbuffered engine is genuinely the prerequisite — and better than we knew

The prior effort's *only* surviving justification for the unbuffered engine is
that `dangerous_extract_secrets` lives on `UnbufferedConnectionCommon`. That
holds, and it is stronger than recorded: **rustls 0.23.41 ships a purpose-built
kTLS API** [P-14] that the project's docs do not mention anywhere.

```
UnbufferedServerConnection::dangerous_into_kernel_connection(self)
    -> Result<(ExtractedSecrets, KernelConnection<ServerConnectionData>), Error>
```

`dangerous_extract_secrets` is **`#[deprecated]`** in favour of it, with the
reason spelled out in the attribute: *"does not support session tickets or key
updates"*. `KernelConnection` (`src/conn/kernel.rs`) survives the handover and
provides exactly two things:

- `update_tx_secret()` / `update_rx_secret()` → `(seq, ConnectionTrafficSecrets)`
  for a TLS 1.3 key update, with the sequence number reset to 0.
- `handle_new_session_ticket(payload)` (client only), so resumption still works.

Every ringline document that cites `dangerous_extract_secrets` as the kTLS hook
is out of date: `ringline/Cargo.toml:27`,
`ringline/src/tls/unbuffered/mod.rs:16`,
`tls-unbuffered-design.md:78`/`:592`/`:606`/`:609`, and
`journal/2026-09-unbuffered-tls.md:41`/`:50`/`:449`. (`CLAUDE.md:207` mentions
kTLS but not the API, so it needs no change.) Fixing those citations is a
docs-only follow-up worth doing regardless of whether kTLS proceeds.

The preconditions are strict and *shape the design*: the handshake must be
complete, `enable_secret_extraction` must be set, and **`sendable_tls` must be
empty** (`src/conn.rs:1209-1218`). Driving the unbuffered connection to
`WriteTraffic` satisfies the first and third naturally — rustls' own module
docs say so (`src/conn/kernel.rs:29-31`).

### 3.3 The RX conversion is the biggest single line item

kTLS control records (handshake, alert — including TLS 1.3 `key_update` and
`close_notify`) reach userspace only through a `TLS_GET_RECORD_TYPE` cmsg, and
a plain `recv()` that meets one **fails with `EIO`** [P-10] — verified in
source and measured. So the TCP recv path must become `recvmsg`-shaped.

What exists today is *not* infrastructure to build on:

- **Every default build's TCP recv is plain `RecvMulti`** (io_uring) or
  `Read::read` (mio) [P-12]. Neither can carry cmsgs.
- The `RecvMsgMulti` TCP handler exists only behind the **default-off,
  CI-untested** `timestamps` feature (`ringline/Cargo.toml:21`; handler at
  `backend/uring/event_loop.rs:1453-1584`), and it has three gaps a kTLS path
  must not inherit:
  1. **It never consults `tls_table`** — no counterpart to the TLS dispatch at
     `event_loop.rs:1120-1126`. A TLS connection with `timestamps` on today
     would feed raw ciphertext to the accumulator.
  2. **It ignores `recv_domain`/segmented delivery, `direct_echo`, and the
     zero-copy `pending_recv_bufs` hold.**
  3. **Close-path cancel is hardcoded to `OpTag::RecvMulti`**
     (`backend/uring/driver.rs:1180-1184`), so an armed `RecvMsgMultiTs` is
     never cancelled on close.
- The msghdr template is a **single per-worker `Box<libc::msghdr>`**
  (`backend/uring/driver.rs:330-333`, `:634-640`), so TLS and non-TLS
  connections on the same worker cannot have different control-region sizes.
  Sizing matters: `MSG_CTRUNC` on a control record is `EIO` [P-10].
- Two other io_uring TCP recv paths reach the accumulator and bypass the TLS
  dispatch entirely: `handle_recv_fallback` (`event_loop.rs:862`, appends at
  `:936`) and `flush_replenish_and_rearm` (`:729`). Both need the kTLS demux
  too.
- **mio has no `recvmsg` path at all** — it reads through
  `std::io::Read::read`. A kTLS mio backend needs a `libc::recvmsg` path built
  from nothing.

**Size: Large.** This is comparable to, or larger than, the whole unbuffered
engine, and it touches the recv hot path that segmented recv also wants to
change. N3 exists because of it.

### 3.4 Key updates and the confidentiality limit

This is the item most likely to be underestimated.

- **On kernel 6.12 — hv01, and therefore the only rig that can run this — the
  kernel refuses a second `TLS_TX`/`TLS_RX` with `EBUSY`** [P-5]. Source and
  measurement agree. TLS 1.3 `KeyUpdate` is therefore *unimplementable*; the
  only correct response is to tear the connection down.
- Rekey landed in **6.14** (absent in 6.13, present in v6.14-rc1), TLS 1.3
  only, same version and cipher required. So kTLS-with-rekey needs a kernel
  ringline cannot currently test on.
- rustls' `KernelConnection` **does not track the confidentiality limit** and
  says so explicitly (`src/conn/kernel.rs:37-52`): "It is the responsibility of
  the user of the API to track approximately how many messages have been sent."
  Under the userspace engines rustls does this for us. Under kTLS **ringline
  would have to count records**, per connection, per direction, and act at the
  limit.
- The limit for AES-GCM is **2^24 records** [P-9] — 256 GiB per direction at
  16 KiB records. Not academic for a runtime designed against 800 GbE.
  ChaCha20-Poly1305 has no practical limit (`u64::MAX`) but gives up AES-NI and
  rules out mlx5 offload; picking it to dodge the problem is a real option and
  a real trade.
- The peer can also *send* a `KeyUpdate` at any time. Detecting it requires the
  cmsg path (3.3), parsing the handshake record, and calling
  `update_rx_secret()` + a fresh `TLS_RX` setsockopt — again impossible on 6.12.

**Size: Medium on 6.14+, and a hard blocker on 6.12.** N2.

### 3.5 The two-backend split

- **io_uring:** the target. Everything above applies.
- **mio on Linux** (`force-mio`): kTLS would technically work — it is a socket
  feature, not an io_uring feature — but needs a `recvmsg` path built from
  scratch (3.3) and gains less, because mio's TLS sends already degrade to
  copies. `CLAUDE.md`'s stance is that mio must be *correct and
  non-pathological, not optimal*. **Recommendation: do not implement kTLS on
  mio.** Let mio always take the userspace engine, and make that an explicit
  documented asymmetry rather than an accident.
- **mio on macOS:** kTLS does not exist. Not applicable.

That asymmetry has a real cost, and it is the same one the unbuffered engine
paid: **the maintainer's dev machine cannot type-check, lint, or test the kTLS
path at all.** `lib.rs` applies `#[cfg_attr(not(has_io_uring), allow(dead_code))]`
to the whole `tls` module, so even dead-code lints are invisible on macOS
(journal, "Dead-code lints are invisible on macOS"). Every check would be
hv01/CI-only. Budget for that; the unbuffered effort lost a red branch to it
three separate times.

### 3.6 `ConnCtx` API surface

Smaller than expected. `ConnCtx`'s TLS-aware surface is `tls_info()`
(`runtime/io.rs:1599`), `eof_truncated()` (`:1617`), `connect_tls*`
(`:1629`, `:1646`), and `close()` (`:1689`); the rest is TLS-aware only
transitively through `DriverCtx::send`.

Under kTLS:

- `tls_info()` must still work. `KernelConnection` exposes
  `negotiated_cipher_suite()` and `protocol_version()`, so those survive; ALPN
  and SNI do **not** — they live on the consumed `CommonState`. **They must be
  snapshotted into a `TlsInfo` before the handover.** (Note SNI is already
  always `None` on the unbuffered engine — journal, "Plan 3" — so kTLS inherits
  that hole rather than creating it.)
- `eof_truncated()` must keep distinguishing a peer `close_notify` from a bare
  FIN. Under kTLS that signal arrives as a control record via cmsg, not from
  rustls. New plumbing, same contract.
- `send_parts().guard()`: today it is **not refused** under TLS — it silently
  copies the guard's bytes and drops the guard (`handler.rs:2893-2924`,
  contradicting `CLAUDE.md:243`). Under kTLS a guard could genuinely stay
  zero-copy, since the kernel takes plaintext. That is the most attractive
  downstream item here — and it depends on [U-1] and [U-2].
- **`send_chain` has no TLS check at all** (`handler.rs`, last `tls` reference
  at `:2917`). `ConnCtx::send_chain` builds `Send`/`SendMsgZc` SQEs straight
  from user memory with no encryption. On a TLS connection today that is a
  latent bug; under kTLS it would accidentally become *correct*. Worth an
  audit either way, and worth not "fixing" by accident.

**No new public config surface is proposed.** Following the unbuffered
engine's precedent: a default-off cargo feature, decided later.

### 3.7 Headline scope estimate

| Cluster | Size | Why |
|---|---|---|
| TCP `RecvMsgMulti` conversion, both backends | **Large** | 3.3. Touches the recv hot path, has no usable precedent (the `timestamps` path is TLS-, segment-, and cancel-unaware), and mio needs a `recvmsg` path written from nothing |
| Second short-send strategy | **Medium** | 3.1 item 5 / [P-19]. Undoes a shipped decision and adds a per-connection branch to the send path |
| Handshake → kTLS switch (ordering + drain preconditions) | **Medium** | [Question 4](#4-what-breaks). Small in lines, large in ways to get silently wrong — the class of bug the unbuffered engine hit three times |
| Key updates + confidentiality-limit accounting | **Medium on 6.14+, blocked on 6.12** | 3.4 / [P-5] / [P-9] |
| Control-record demux (`close_notify`, alerts, `key_update`) + `eof_truncated` plumbing | **Medium** | 3.3, 3.6 |
| Engine selection, fallbacks, prerequisites 1/2/4/6 | **Small** | 3.1 |

**Total: comparable to or larger than the unbuffered engine** (4 PRs,
~2,600 lines), on a path where **the maintainer's dev machine can neither
compile nor lint the code**, and where the only available rig **cannot exercise
the key-update half of the correctness envelope**.

**The single biggest risk is not any of the above.** It is that the whole thing
is built on [U-1] — an unmeasured claim about what the kernel does with an
io_uring send — and that ringline has just spent four PRs learning what happens
when a performance premise about a dependency is not checked against that
dependency before the design is written.

---

## 4. What breaks

Checked against `CLAUDE.md`'s *Domain Invariants* and
[`send-completion-design.md`](send-completion-design.md).

| Invariant | Under kTLS |
|---|---|
| **1. SQE memory outlives the operation** | **Survives, with a new instance.** Pool slots still hold the data until the CQE. But `SetSockOpt`'s `optval` is *key material* referenced by an SQE — it must live until the CQE and be zeroized after. The obvious mistake (a stack temporary) is a use-after-free with secrets in it. Treat the crypto-info block as a first-class pinned allocation |
| **2. io_uring does not order independent SQEs** | **Needs rethinking at exactly one point: the switch.** See below |
| **3. Stale CQEs are normal** | **Survives, extended.** The `SetSockOpt` CQE needs generation validation like any other, and the recv re-arm across the switch will produce in-flight `RecvMulti` CQEs for a connection that is now `RecvMsgMulti` |
| **4. No CQE-skip for pool-backed sends** | **Survives unchanged** |
| **5. Short sends happen** | **BROKEN — this is the sharpest finding in the document.** kTLS refuses `MSG_WAITALL` with `-EOPNOTSUPP` [P-19], measured, and ringline sets it on every stream send. See below |
| **6. `ENOBUFS` on multishot recv** | **Survives**, but the re-arm path must be reproduced for `RecvMsgMulti`, which today only exists behind `timestamps` |
| **7. `EINTR`/`EBUSY` on submit is backpressure** | **New hazard.** `EBUSY` from `SOCKET_URING_OP_SETSOCKOPT` is *not* backpressure — it is the rekey refusal [P-5]. Retrying it forever is a hang. The two must not be conflated |

### `MSG_WAITALL` is refused, and that undoes a shipped decision

`tls_sw_sendmsg` rejects any `msg_flags` outside a small allow-list:

```c
/* net/tls/tls_sw.c:1231-1234 (v6.12; identical at v6.17:1255-1258) */
if (msg->msg_flags & ~(MSG_MORE | MSG_DONTWAIT | MSG_NOSIGNAL |
                       MSG_CMSG_COMPAT | MSG_SPLICE_PAGES | MSG_EOR |
                       MSG_SENDPAGE_NOPOLICY))
        return -EOPNOTSUPP;
```

`MSG_WAITALL` is not in it. `tls_device_sendmsg` (`tls_device.c:436-439`) is
narrower still. ringline's `STREAM_SEND_FLAGS` **is** `MSG_WAITALL`
(`ringline/src/completion.rs:1-10`), set by every stream send builder — so
**every ringline send on a kTLS socket fails immediately with `EOPNOTSUPP`**.
Measured on hv01, not inferred: the io_uring probe's `IORING_OP_SEND` returned
`-95`.

This is not a small compatibility fix. `MSG_WAITALL` on stream sends is a
deliberate, argued, shipped decision —
[`send-completion-design.md`](send-completion-design.md) §2 landed it precisely
to collapse the CQE → userspace-resubmit → CQE round trips that short sends
otherwise cost, and `CLAUDE.md`'s Domain Invariant 5 records it as the
mechanism. Under kTLS, ringline must:

- drop `MSG_WAITALL` for kTLS connections, and
- reinstate the userspace partial-resubmit loop **for those connections only**,
  so the send path carries two short-send strategies keyed on connection type,
  and
- keep the resubmit ordered — a partial kTLS send leaves a *partial TLS record
  open* in the kernel (`pending_open_record_frags`), so anything that
  interleaves on that socket corrupts the record stream. Invariant 2's
  per-connection send queue already guarantees this, but the coupling is new
  and now load-bearing.

It also interacts with the coalescing interlock the unbuffered effort found by
accident (journal, "an accidental interlock"): `submit_next_queued` coalesces
consecutive pool-backed sends into one `SendMsgCoalesced` without inspecting
`OpTag`, and under kTLS a coalesced send is *one TLS record*, which changes
what a "logical send" means on the wire.

**This should be near the top of any implementation estimate.** It is what N5
measures, it was not anticipated by any prior document, and it was found by
running a probe rather than by reading — which is the argument for running G0
before designing anything.

### The switch is the sharp edge

Two orderings must hold, and neither is expressible in the current machinery.

**TX.** `dangerous_into_kernel_connection` requires `sendable_tls` empty, but
that is a rustls-side condition — it says nothing about ringline's own
per-connection send queue, which may still hold un-submitted handshake-record
SQEs. If `TLS_TX` is installed while those are queued, the kernel begins
encrypting from `ExtractedSecrets.tx` sequence number *N* while records
0..*N* are still on ringline's queue. Application data would reach the wire
ahead of the handshake tail: `bad_record_mac` at the peer, which is the exact
failure mode invariant 2 exists to prevent, and the exact failure mode the
unbuffered engine hit twice.

Two candidate answers, both needing design work: `IO_LINK` the `SetSockOpt`
behind the last handshake `Send` (cheap, but `IO_LINK` semantics under
`MSG_WAITALL` short sends need checking), or make the switch a queue element so
`submit_next_queued` drains past it in order (invasive, but it is where the
ordering guarantee already lives).

**RX.** `TLS_RX` installs a key at `ExtractedSecrets.rx` sequence *M*, meaning
"the next record on the wire is record *M*". That is only true if
`CiphertextBuf` is **empty** at the moment of the switch. If the peer coalesced
application data — or a `close_notify` — with its final handshake flight, those
bytes are already in ringline's buffer, and rustls is about to be consumed.
They cannot be handed to the kernel. So the switch must be conditional on
`CiphertextBuf` being drained; if it is not, either delay a round or decline
kTLS for that connection. This is a *checkable precondition*, and it should be
an assertion with a fallback, not a comment.

Note this is precisely the case the buffered engine gets wrong today (journal
"Backlog" item 7: `feed_tls_recv` returns `HandshakeJustCompleted` before the
`peer_has_closed()` check). kTLS would turn a latent bug into a data-loss bug.

### `sendfile` / NVMe-to-socket does not compose

The prior design doc listed it as a downstream benefit *"needs its own
investigation"*. Investigated:

1. **ringline has no splice/sendfile path at all** [P-13].
   `grep -rn 'splice\|sendfile' ringline/src/` returns nothing.
   `ConnCtx::forward_to` — the closest thing — is `opcode::Send` from a
   *provided recv buffer* (`backend/uring/ring.rs:340-355`), i.e. already
   user-memory-to-socket. There is no kernel-to-kernel path to preserve.
2. **NVMe passthrough reads into user memory** (`ring.rs:912`,
   `IORING_OP_URING_CMD`), so there is no page-cache page to splice.
3. `MSG_SPLICE_PAGES` — the flag both `tls_sw.c:1088` and `tls_device.c:493`
   honour for a genuine zero-copy send — is not settable from
   `IORING_OP_SEND`; it is what `sendfile`/`vmsplice` set internally.

**Conclusion: strike it.** It is not a kTLS benefit for ringline. It would be a
benefit for a *different* ringline that had a splice-based forward path, and
building that is a separate project with its own justification.

---

## Unverified, listed explicitly

Repeating the register's `U-` rows here so they are not lost in a table:

1. **[U-1] Whether io_uring's `IORING_OP_SEND` actually reaches
   `sk_msg_zerocopy_from_iter`.** Source says the conditions hold; the issuing
   context's `mm` under io-wq offload is the doubt. **This is the load-bearing
   premise of the entire effort.** G0/N1.
2. **[U-2] Whether `IORING_OP_SEND_ZC` avoids the pool-slot copy on a kTLS
   socket.** `net/tls` has no `MSG_ZEROCOPY`/`msg_ubuf` handling of its own;
   suspected no. This matters for the `send_parts().guard()` payoff. Note the
   probe's first attempt could not even reach this question, because
   `MSG_WAITALL` failed first [P-19]; a follow-up run
   (`experiments/ktls-uring-probe.toml`) tests plain `Send` and `SendZc`
   separately. **If that run's result is not recorded below, it had not
   completed when this document was written — do not assume it passed.**
3. **[U-3] The cost of a copy at 400/800 GbE.** Inherited unverified from the
   previous design doc and still unverified. One measured number exists
   (~3.2 GiB/s/core at 200 GbE); do not extrapolate a core count from it.
4. **[U-4] Whether kTLS helps at ringline's actual payload sizes.** Everything
   here scales with payload; ringline's benchmarks are small-op dominated.
5. **Not verified: whether any *other* in-tree driver implements TLS offload
   under a filename my search missed.** The four in the table were confirmed by
   reading their `tlsdev_ops` assignments; the file-name sweep that found them
   ran against a GitHub tree listing that reported `truncated: true`. The
   negative results for i40e/ice/ena are solid (their sources were fetched and
   grepped directly, plus the hv01 `ethtool` measurement); the *completeness*
   of the positive list is high-confidence, not certain.
6. **Not verified: kTLS behaviour under io_uring multishot `recvmsg`
   specifically.** The probe used blocking `recvmsg`. Multishot re-arm across
   an `EIO`, and `MSG_CTRUNC` sizing with a shared per-worker msghdr, are
   untested. Given that `MSG_WAITALL` turned out to be refused on the *send*
   side [P-19], assume nothing about flag acceptance on the recv side either —
   probe it before designing around it.
7. **Not verified: whether hv02 or the AWS rigs have `CONFIG_TLS` at all.**
   Only hv01 was probed. hv02 is the same Debian 13 family, so it is likely,
   but "likely" is what this document exists to avoid.

---

## Recommendation

**Do not start kTLS. Run G0 first, as a standalone measurement, and let the
number decide.**

The case for kTLS is stronger than the previous design doc believed on one
axis and weaker on the other, and the two do not cancel:

- **Stronger:** software kTLS plausibly removes 2 of 3 send-path passes and 1
  of 2 recv-path passes — not the "no change" the prior doc asserted. If [U-1]
  holds, the AEAD reads ringline's pool slot directly and the plaintext never
  enters the kernel. That is a real, structural, previously-unclaimed win.
- **Weaker:** the endpoint the effort was aimed at is gone. `TLS_HW` is not
  zero-copy [P-6] and is unreachable on every machine we own [P-7]. The
  `sendfile` benefit does not compose with ringline's architecture [P-13].
  What is left is a software win of unmeasured size at payload sizes we do not
  benchmark [U-4].

And the cost is high and front-loaded: **kTLS refuses `MSG_WAITALL`** [P-19],
so Domain Invariant 5's mechanism does not survive and the userspace
short-send resubmit loop has to come back for kTLS connections — undoing a
shipped, argued decision; a `RecvMsgMulti` conversion of the TCP recv hot path
on both backends (Large, and colliding with segmented recv); a send-ordering
hazard at the switch that invariant 2 does not currently cover; a
confidentiality-limit counter ringline would have to own; and — decisively for
the only rig we can run this on — **`KeyUpdate` is impossible on kernel 6.12**,
so hv01 can validate the fast path but cannot validate the correctness envelope
G3 demands.

The `MSG_WAITALL` refusal is worth dwelling on as a *method* result: it is a
hard blocker, it contradicts nothing in any prior document because no prior
document considered it, and it took one 150-line probe on hv01 to find. Three
documents' worth of reasoning about kTLS had been written without anyone
putting a kTLS socket on a rig.

So the sequencing is:

1. **Run G0.** It needs no new ringline code — the harness exists on the local
   `bench/tls-send-microbench` branch, and it needs one extra arm plus the
   `setsockopt` calls the probe already demonstrates. Report the allocation
   counter and the cold-payload control. Cost: hours, not weeks.
2. **If N1 fires, stop, and record the negative result** the way the unbuffered
   effort's was recorded. That is a good outcome: it costs a day and closes the
   question.
3. **If G1 passes**, the *next* question is not "build it" but **"is the win
   present at 1 KiB?"** [U-4]. If it is only visible at 256 KiB, kTLS is a
   feature for a workload ringline does not currently have, and it should wait
   for one.
4. **Only then** scope the implementation — and get a 6.14+ kernel onto a rig
   first, because without rekey the correctness envelope cannot be tested at
   all.

Two things worth doing **regardless** of the kTLS decision, both cheap:

- Correct every `dangerous_extract_secrets` citation to
  `dangerous_into_kernel_connection`: `ringline/Cargo.toml:27`,
  `ringline/src/tls/unbuffered/mod.rs:16`,
  `tls-unbuffered-design.md:78`/`:592`/`:606`/`:609`, and
  `journal/2026-09-unbuffered-tls.md:41`/`:50`/`:449`. The API we named as the
  kTLS hook is deprecated, and the one that replaced it solves two of the
  problems the journal listed as costs (key updates, session tickets).
- Correct `tls-unbuffered-design.md:120-121`'s copy table rows for kTLS. Both
  are wrong in the same direction as the row that was already retracted, and
  leaving them uncorrected is how the next person inherits a false premise.
