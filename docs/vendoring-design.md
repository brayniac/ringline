# Vendoring rustls and quinn-proto

Status: proposal (2026-10-27). Nothing here is built. The owner decided on
2026-10-26 to vendor rustls into a `ringline-tls` crate and quinn-proto into
ringline's QUIC crates, so that ringline can change the TLS record layer and
the QUIC packet path itself. This document says how, what it costs, and which
decisions are still open.

Related: `docs/ktls-design.md`, `docs/tls-unbuffered-design.md`,
`docs/syscalls-and-copies.md`.

## Goals

1. ringline can change rustls and quinn-proto code and release the change on
   its own schedule.
2. A caller's code compiles unchanged across ringline releases that patch the
   vendored code or move it to a new upstream version, unless the caller uses
   one of the two unstable re-exports (see "Public API").
3. An upstream security fix reaches ringline users as soon as ringline can
   release it, and `cargo audit` reports vendored code that predates the fix.

Not a goal: an abstraction over several TLS implementations. There is one
implementation, the vendored rustls.

## Why

rustls 0.23.46 with the ring provider makes two plain copies of each byte in
each direction besides the AEAD (measured with an `LD_PRELOAD` memcpy counter
on an in-memory unbuffered client and server, 64 records per size):

| Direction | Copy 1 | Copy 2 | Bytes copied per plaintext byte |
|---|---|---|---|
| Send | `PrefixedPayload` (`crypto/ring/tls13.rs:210,214`) | record into the pool slot (`common_state.rs:718`) | 2.021 at 1 KiB, 2.001 at 16 KiB |
| Receive | `into_vec` after in-place decrypt (`common_state.rs:557`, `msgs/base.rs:43`) | ringline's accumulator append (`unbuffered/mod.rs:436`) | 1.000 inside rustls, plus ringline's append |

Removing either rustls copy needs a change inside rustls. Upstream `main`
(0.24.0-dev, unreleased, breaking) removes both. It has no release date, and
its send path writes into a `&mut Vec<u8>`, which a pool slot is not. As of
2026-10-26 there is no 0.23.x release after 0.23.46. The 0.23 line has no in-place API, and
upstream issue rustls/rustls#3109 (open) proposes changing the encrypter
traits.

QUIC has no kernel offload: quinn-proto encrypts and decrypts every packet in
userspace. For QUIC, changing quinn-proto is the only way to change its copies.

kTLS is evaluated separately (`docs/ktls-design.md`). Vendoring does not
replace it. kTLS uses the upstream 0.23 API (`dangerous_into_kernel_connection`),
which the vendored copy keeps.

## Layout

### `ringline-tls`: the vendored rustls

- A published workspace crate whose contents are upstream's `rustls/`
  directory at a release tag. The package is renamed to `ringline-tls` and the
  library to `ringline_tls`.
- Its public API is rustls's API. It is not ringline's stable API (see
  "Public API").
- rustls refers to its own items through 977 `crate::` paths in 98 files.
  Those paths resolve only at a crate root. That is why the vendored code is a
  crate of its own and not a module of `ringline`.
- Its dependencies stay external crates:
  - `rustls-pki-types`. Callers load certificates and keys with it, so
    certificate loading keeps working with the rest of the ecosystem.
  - `rustls-webpki`, for certificate path validation. No change is planned.
  - `ring`, and `aws-lc-rs` if adopted.

  Advisories against these keep reaching users through `cargo audit` as they
  do today.

### quinn-proto

- The owner asked for quinn-proto inside the QUIC library. The vendored code
  has the same constraint as rustls: 131 `crate::` paths in 46 files.
- Two ways to do it:
  - A separate crate, `ringline-quic-proto`. The upstream tree is unchanged
    apart from the rename.
  - A module of `ringline-quic`. Every `crate::` path is rewritten to
    `crate::proto::` on every import, and the diff against upstream always
    contains those rewrites.
- Recommendation: the separate crate. Its diff against upstream then shows
  only ringline's patches.
- The vendored quinn-proto's `crypto::rustls` module depends on
  `ringline-tls`. A binary then has one TLS stack for TCP and QUIC.
- ringline enables only quinn-proto's `rustls-ring` feature, with default
  features off (workspace `Cargo.toml:63`). The vendored copy keeps
  `rustls-platform-verifier`, `aws-lc-rs`, `qlog` and `arbitrary` optional,
  or drops them.

### Unchanged

- `ringline-benchmarks` keeps upstream rustls, `tokio-rustls`, `quinn` and
  `h3-quinn` for its tokio comparison clients. Those clients measure
  upstream's code on purpose.
- ringline's TLS tests keep upstream rustls as the peer. For example,
  `ringline/tests/tls_echo.rs` drives `rustls::ClientConnection` over a std
  socket. Every such test then checks the vendored code against upstream's.

## Public API

### What exposes rustls and quinn-proto today

| Crate | Item | Exposes |
|---|---|---|
| ringline | `TlsConfig::new` (`config.rs:44`) | `Arc<rustls::ServerConfig>` |
| ringline | `TlsClientConfig::new` (`config.rs:58`) | `Arc<rustls::ClientConfig>` |
| ringline | `TlsInfo::protocol_version` (`tls/mod.rs:80`) | `rustls::ProtocolVersion` |
| ringline | `TlsInfo::cipher_suite` (`tls/mod.rs:85`) | `rustls::SupportedCipherSuite` |
| ringline-quic | `QuicConfig::server`, `server_config` (`config.rs:36,70`) | `Arc<quinn_proto::ServerConfig>` |
| ringline-quic | `QuicConfig::client`, `client_config` (`config.rs:49,76`) | `quinn_proto::ClientConfig` |
| ringline-quic | `QuicConfig::endpoint_config` (`config.rs:63`) | `Arc<quinn_proto::EndpointConfig>` |
| ringline-quic | re-exports (`lib.rs:46`) | `ConnectionStats`, `Dir`, `FrameStats`, `PathStats`, `ReadError`, `StreamId`, `UdpStats`, `VarInt`, `WriteError` |

Each row ties a caller's code to the version of the crate it exposes. Any
ringline release that changes the vendored code could break those callers.

### Stable configuration in ringline

ringline owns the configuration types. They take certificates and keys as
`rustls-pki-types` values, a 1.x crate:

```rust
let tls = ringline::TlsConfig::builder()
    .cert(chain, key)                 // Vec<CertificateDer<'static>>, PrivateKeyDer<'static>
    .sni_cert("api.example", chain2, key2)
    .alpn(&[b"h2", b"http/1.1"])
    .client_auth_roots(roots)         // optional mTLS
    .build()?;

let client = ringline::TlsClientConfig::builder()
    .roots(roots)                     // Vec<CertificateDer<'static>>
    .alpn(&[b"h2"])
    .client_cert(chain, key)          // optional
    .build()?;
```

- `TlsInfo` returns ringline enums, `TlsVersion` and `CipherSuite`, both
  `#[non_exhaustive]`.
- A benchmark client that skips certificate verification uses
  `TlsClientConfig::builder().dangerous_no_verify()`.
- The in-tree callers need only a certificate and ALPN:
  `ringline-bench/src/bin/server.rs:752` and
  `ringline-benchmarks/src/protocols/http2.rs:271`.
- ringline-quic builds its quinn-proto configs from the same two types:
  `QuicConfig::server(tls: TlsConfig)` and `QuicConfig::client(tls: TlsClientConfig)`.
  - QUIC requires TLS 1.3 and ALPN, and `QuicConfig` returns an error if
    either is missing.
  - Transport settings get ringline-quic builder methods, as
    `send_queue_capacity` and `allow_mtud` already are.
  - The stats types become ringline-quic structs.

### Unstable re-exports

- `ringline::tls::rustls` re-exports `ringline-tls`, and
  `TlsConfig::from_rustls(Arc<ringline::tls::rustls::ServerConfig>)` (with
  the client twin) accepts a config built from it.
- `ringline_quic::proto` re-exports the vendored quinn-proto, and
  `QuicConfig::from_proto` takes its configs.
- These cover custom certificate verifiers, certificate resolvers, crypto
  providers, and anything the builders do not offer yet.
- Code that uses them follows the vendored code. A ringline release that
  rebases or patches the vendored code may break it. The CHANGELOG entry for
  such a release names the upstream versions and the local patches.
- Every other item follows ringline's semver.

Consequence: crates that implement upstream rustls traits do not plug into the
vendored copy. Examples are `rustls-platform-verifier`, alternative crypto
providers, and a caller's own verifier. A caller's verifier ports with a change
of import path; the trait shapes are the same. A third-party crate that
implements upstream's traits does not port at all.

## Importing and tracking upstream

- Import from upstream's git release tag, not from crates.io. The rustls
  crates.io package excludes `tests/**` and `src/testdata`. The quinn-proto
  package includes its tests (`src/tests`).
- Each vendored crate has an `UPSTREAM` file. It records the repository, tag,
  commit and the list of local patches, one line each with its commit.
- A script, `tools/vendor/import.sh <crate> <tag>`, writes the upstream tree
  into the crate directory, applies the rename (package, library, and an
  `extern crate ringline_tls as rustls;` alias in each test root so
  upstream's tests compile unchanged), and commits that alone as
  `vendor: import rustls <tag> (<commit>)`. Each local patch is a separate
  commit on top.
- `git diff <import commit> HEAD -- ringline-tls/` is therefore always exactly
  the local patches.
- Moving to a new upstream release means running the import for the new tag
  on a branch and reapplying the patch commits. A patch that upstream has
  taken is dropped at this step.
- Patches that are general go upstream as well. Each accepted one makes the
  next import smaller.

## Security process

The workspace `Cargo.toml` (lines 52 to 85) records the advisories that set
today's version floors. Five are against rustls, fixed in 0.23.45 and 0.23.46:

- RUSTSEC-2026-0285
- RUSTSEC-2026-0346 to 0349

Eight are against quinn-proto, fixed by 0.11.19:

- RUSTSEC-2026-0338 to 0343
- two GHSA

Vendoring makes ringline responsible for shipping each of these.

- **Detection.** `cargo audit` matches advisories by crate name, so it does not
  see `ringline-tls`.
  - A CI step writes a lockfile listing `rustls` and `quinn-proto` at the
    versions in the `UPSTREAM` files, and runs `cargo audit` against that file.
    It runs daily, next to the existing audit job (`ci.yml:120-131`).
  - An advisory against an imported version fails the job.
  - I have not checked cargo-audit's option for auditing a lockfile at a given
    path; it needs confirming before this step is written.
- **Fix.**
  - Import the fixed upstream release, reapply the patches, and release
    `ringline-tls` as a semver-compatible patch version.
  - `cargo update` then picks up the fix without a ringline release.
- **Notice.** File a RustSec advisory against the affected `ringline-tls`
  versions, citing the upstream advisory, so downstream `cargo audit` runs
  report it.

## Testing

- CI runs the vendored crates' own tests: rustls's `tests/` and quinn-proto's
  `src/tests`.
- ringline's TLS tests run against an upstream rustls peer (see "Layout").
- A patch to the record layer also passes rustls's fuzz targets and its BoGo
  runner (the BoringSSL protocol test suite) before release. Both live in the
  upstream repository outside `rustls/`, and the import script brings them in
  too.
- A copy-count test asserts bytes copied per plaintext byte for send and
  receive. It is built from the measurement program the TLS5 evaluation used.
  It fails if a rebase brings a copy back.

## First patches

To rustls, on 0.23.46:

1. **Send.** Add `MessageEncrypter::encrypt_into(msg, seq, out: &mut [u8]) -> Result<usize>`.
   - The default falls back to today's path.
   - The ring and aws-lc-rs TLS 1.3 encrypters override it.
   - `write_fragments` calls it with the pool slot.
   - Removes `PrefixedPayload`. Estimated 80 to 150 lines.
2. **Receive.** Record the decrypted range in place of `into_vec`, and lend it
   from `ReadTraffic::next_record`.
   - ringline copies it into the accumulator, which leaves one copy.
   - In 0.23 each `ReadTraffic` carries one record, so the borrowed region
     stays valid until the next call. That is from reading the code and has
     not been tested.
   - Estimated 100 to 200 lines.
3. **AEAD only.** An out-of-place seal and open with aws-lc-rs ≥ 1.18.1:
   - `TlsRecordSealingKey::seal_out_of_place_scatter`, from caller memory into
     the slot;
   - `LessSafeKey::open_separate_gather`, from the ciphertext buffer into the
     accumulator.

   ring 0.17 has no out-of-place AEAD. This patch needs the provider decision
   below.

| | Send | Receive |
|---|---|---|
| 0.23.46 today | 2 copies + AEAD | 2 copies + AEAD |
| After 1 and 2 | 1 copy + AEAD | 1 copy + AEAD |
| After 3 (aws-lc-rs) | AEAD only | AEAD only |

These counts leave out the kernel's own socket copy, which only kTLS removes.

No quinn-proto patch is proposed until the QUIC path has been profiled.

Each patch is measured on its own: the TLS5 profile spec
(`experiments/tls5-profile.toml` on `exp/tls5-profile`) gives server CPU per
operation at 64 B, 4 KiB and 64 KiB values.

## Sequence

Each step is a separate PR.

1. ringline's stable TLS configuration, still over upstream rustls 0.23.46.
   It changes `TlsConfig::new`, `TlsClientConfig::new` and `TlsInfo`, so it
   lands before 0.7.0. Callers port once.
2. Import rustls 0.23.46 as `ringline-tls`, switch ringline to it, add the
   advisory check. The diff against upstream is the rename only, with no
   behaviour change.
3. rustls patches 1 and 2, each measured.
4. Import quinn-proto 0.11.19 against `ringline-tls`, and move
   ringline-quic's configuration to the stable types. This changes
   ringline-quic's public API, so it also belongs before 0.7.0 if 0.7.0 is to
   be the last break.
5. Patch 3, if the provider decision is aws-lc-rs.

Steps 2, 3 and 5 change no caller-visible item outside the unstable
re-exports. Goal 2 is what makes that possible.

## Costs

- **Size.** 49,208 lines of rustls source and 31,209 of quinn-proto, plus
  their tests, move into the repository. These are counts of `src/*.rs` in the
  crates.io packages.
- **Upstream releases.** Every upstream security release becomes a ringline
  release (see "Security process").
- **Duplicate rustls.** A binary that also uses upstream rustls, through
  `reqwest` or `tokio-rustls` for example, contains both copies.
- **Ecosystem crates.** Crates built on upstream's traits do not plug in (see
  "Public API").
- **Who reviews the patches.** Patches to the record layer are
  security-relevant. Upstream review no longer covers them unless they are
  upstreamed.

## Open decisions

1. **quinn-proto: separate crate or module.** Recommendation: a separate
   crate, `ringline-quic-proto` (see "Layout").
2. **rustls base: 0.23.46 or `main`.**
   - `main` already removes both copies.
   - It has no releases to import security fixes from, and following it means
     importing unreleased API changes.
   - Recommendation: 0.23.46 with patches 1 and 2. Revisit when 0.24.0 is
     released.
3. **Crypto provider: ring or aws-lc-rs.**
   - ring is what ringline uses today.
   - aws-lc-rs is needed for AEAD-only receive and send. It needs a C
     toolchain and CMake to build, and offers FIPS builds.
   - This can wait until patches 1 and 2 are measured.
4. **The escape hatches** (`from_rustls`, `from_proto`): keep them, or require
   every caller to use the builders. Recommendation: keep them, documented as
   unstable.
