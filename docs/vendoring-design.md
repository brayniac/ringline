# Vendoring rustls and quinn-proto

Status: proposal (2026-10-27). Nothing here is built. The owner decided on
2026-10-26 to vendor rustls and quinn-proto, so that ringline can change the
TLS record layer and the QUIC packet path itself, and on 2026-10-27 that each
lives in its own repository under `ringline-rs` as a maintained fork of
upstream. This document says how, and what it costs.

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

### The forks

| Repository | Fork of | Published crate | Library name | Used by |
|---|---|---|---|---|
| `ringline-rs/rustls` | `rustls/rustls` | `ringline-tls` | `rustls` | ringline, `ringline-quic-proto` |
| `ringline-rs/quinn` | `quinn-rs/quinn` | `ringline-quic-proto` | `quinn_proto` | `ringline-quic` |

- Each is a GitHub fork. It keeps upstream's history and can open pull
  requests against upstream directly.
- ringline's code lives on one branch per upstream line: `ringline/0.23` in
  the rustls fork and `ringline/0.11` in the quinn fork.
  - The branch starts at upstream's release tag.
  - The first commit on it renames the package.
  - Each further commit is one ringline patch.
  - `git diff <upstream tag> ringline/0.23` is always exactly ringline's
    changes.
- Only the package is renamed, not the library: `ringline-tls` builds a
  library still called `rustls`.
  - upstream's tests, examples, fuzz targets and BoGo runner build unchanged.
    The fork's other workspace members point at it with
    `rustls = { package = "ringline-tls", path = ... }`.
  - A crate that depends on both `ringline-tls` and upstream `rustls` must
    rename one of the two dependency keys. Without the rename, rustc stops
    with `E0464` (multiple candidates for `rustls`). This was checked with
    two toy packages, and renaming one key, for example
    `upstream_rustls = { package = "rustls", ... }`, builds.
  - ringline's own tests are such a crate, because they keep upstream rustls
    as the peer.
  - Callers who go through ringline's re-export never see the clash.
- The vendored quinn-proto depends on `rustls = { package = "ringline-tls" }`.
  Its `rustls::` paths stay as they are, and a binary has one TLS stack for
  TCP and QUIC.
- ringline enables only quinn-proto's `rustls-ring` feature, with default
  features off (workspace `Cargo.toml:63`). The fork keeps
  `rustls-platform-verifier`, `aws-lc-rs`, `qlog` and `arbitrary` optional.
- Dependencies that stay upstream crates:
  - `rustls-pki-types`. Callers load certificates and keys with it, so
    certificate loading keeps working with the rest of the ecosystem.
  - `rustls-webpki`, for certificate path validation. No change is planned.
  - `ring`, and `aws-lc-rs` if adopted.

  Advisories against these keep reaching users through `cargo audit` as they
  do today.

### Versions

- The forks have their own version numbers. `ringline-tls` 0.1.x is based on
  rustls 0.23.x, and `ringline-quic-proto` 0.1.x on quinn-proto 0.11.x.
- A merge of a newer upstream patch release, or a ringline patch that adds
  API, is a 0.1.x patch release.
- Moving to rustls 0.24 is `ringline-tls` 0.2.0.
- Reusing upstream's numbers would not work. A ringline-only fix released
  as 0.23.47 would collide with upstream's 0.23.47. crates.io refuses
  a version that differs from a published one only in build metadata
  (`0.23.46+ringline.1`).
- Each release's notes name the upstream tag it contains.

### Unchanged

- `ringline-benchmarks` keeps upstream rustls, `tokio-rustls`, `quinn` and
  `h3-quinn` for its tokio comparison clients. Those clients measure
  upstream's code on purpose.
- ringline's TLS tests keep upstream rustls as the peer. For example,
  `ringline/tests/tls_echo.rs` drives `rustls::ClientConnection` over a std
  socket. Every such test then checks the fork against upstream.
- quinn-proto stays the protocol crate with no I/O under `ringline-quic`, the
  same split as `resp-proto` under `ringline-redis`.

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
  updates or patches a fork may break it. The CHANGELOG entry for
  such a release names the upstream versions and the local patches.
- Every other item follows ringline's semver.

Consequence: crates that implement upstream rustls traits do not plug into the
vendored copy. Examples are `rustls-platform-verifier`, alternative crypto
providers, and a caller's own verifier. A caller's verifier ports with a change
of import path; the trait shapes are the same. A third-party crate that
implements upstream's traits does not port at all.

## Tracking upstream

- A new upstream release on the same line is merged into the ringline branch,
  for example tag 0.23.47 into `ringline/0.23`.
  - It is a merge, not a rebase, so the branch is never force-pushed and every
    update goes through a pull request.
  - The diff against the newest merged tag still shows only ringline's
    changes.
- When the release that contains a patch upstream has taken is merged, the
  merge resolves to upstream's version and the patch leaves the diff.
- General patches are also offered upstream, as pull requests from the fork.
  Each one upstream accepts shrinks the diff.

### Automated updates

A scheduled workflow in each fork runs daily:

1. It fetches upstream's tags and looks for a release on the branch's line
   newer than the last one merged.
   - It does not cross to a new line. 0.24 is a decision, not an update.
2. If it finds one, it merges the tag into a new branch off `ringline/0.23`
   and runs the fork's tests.
3. It opens a pull request naming the upstream release and any advisories it
   fixes. If the merge conflicts, it opens an issue naming the conflicting
   files instead.
4. If the advisory check (see "Security process") reports an advisory against
   the last merged release, the workflow runs at once, and the pull request is
   labelled as a security fix.

The pull request is reviewed and merged like any other, then the fork
releases a patch version. A pull request in ringline then moves `Cargo.lock`
to it; Dependabot or the same workflow opens it.

## Security process

The workspace `Cargo.toml` (lines 52 to 85) records the advisories that set
today's version floors. Five are against rustls, fixed in 0.23.45 and 0.23.46:

- RUSTSEC-2026-0285
- RUSTSEC-2026-0346 to 0349

Eight are against quinn-proto, fixed by 0.11.19:

- RUSTSEC-2026-0338 to 0343
- two GHSA

With the forks, ringline is responsible for shipping each of these.

- **Detection.** `cargo audit` matches advisories by crate name, so it does not
  see `ringline-tls`.
  - A daily job in each fork writes a lockfile listing `rustls` (or
    `quinn-proto`) at the last merged upstream release, and runs `cargo audit`
    against that file.
  - An advisory against that version fails the job and starts the update
    workflow.
  - I have not checked cargo-audit's option for auditing a lockfile at a given
    path; it needs confirming before this step is written.
- **Fix.**
  - Merge the fixed upstream release and release the fork as a
    semver-compatible patch version.
  - `cargo update` then picks up the fix without a ringline release.
- **Notice.** File a RustSec advisory against the affected `ringline-tls`
  versions, citing the upstream advisory, so downstream `cargo audit` runs
  report it.

## Testing

- The fork's CI runs upstream's test suites on the ringline branch. That
  covers rustls's `tests/` and quinn-proto's `src/tests`.
- A patch to the record layer also passes rustls's fuzz targets and its BoGo
  runner (the BoringSSL protocol test suite) before release. Both come with
  the fork.
- ringline's TLS tests run against an upstream rustls peer (see "Unchanged").
- A copy-count test in the fork asserts bytes copied per plaintext byte for
  send and receive. It is built from the measurement program the TLS5
  evaluation used, and fails if a merge brings a copy back.

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

   ring 0.17 has no out-of-place AEAD. This patch needs aws-lc-rs (see "Decisions").

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

Each step is a separate pull request in the repository it changes.

1. ringline's stable TLS configuration, still over upstream rustls 0.23.46.
   It changes `TlsConfig::new`, `TlsClientConfig::new` and `TlsInfo`, so it
   lands before 0.7.0. Callers port once.
2. Fork rustls as `ringline-rs/rustls`.
   - Branch `ringline/0.23` from the 0.23.46 tag and rename the package.
   - Add the advisory check and the update workflow.
   - Publish `ringline-tls` 0.1.0 and switch ringline to it.
   - The diff against upstream is the rename only, with no behaviour change.
3. rustls patches 1 and 2, each measured.
4. Fork quinn as `ringline-rs/quinn`, with `ringline-quic-proto` 0.1.0
   based on quinn-proto 0.11.19 and using `ringline-tls`. Then move
   ringline-quic's configuration to the stable types. This changes
   ringline-quic's public API, so it also belongs before 0.7.0 if 0.7.0 is to
   be the last break.
5. Patch 3, if the provider decision is aws-lc-rs.

Steps 2, 3 and 5 change no caller-visible item outside the unstable
re-exports. Goal 2 is what makes that possible.

## Costs

- **Two more repositories.** ringline maintains 49,208 lines of rustls source
  and 31,209 of quinn-proto, plus their tests. These are counts of `src/*.rs`
  in the crates.io packages.
- **Changes across repositories.** A ringline change that needs a fork patch
  takes a pull request in the fork, a fork release, and then the ringline pull
  request. Protocol changes to `resp-proto` already work this way.
- **Upstream releases.** Every upstream security release becomes a ringline
  release (see "Security process").
- **Duplicate rustls.** A binary that also uses upstream rustls, through
  `reqwest` or `tokio-rustls` for example, contains both copies.
- **Ecosystem crates.** Crates built on upstream's traits do not plug in (see
  "Public API").
- **Who reviews the patches.** Patches to the record layer are
  security-relevant. Upstream review no longer covers them unless they are
  upstreamed.

## Decisions

Owner, 2026-10-27:

1. quinn-proto is a separate crate, `ringline-quic-proto`, not a module of
   `ringline-quic`. As a module, its 131 `crate::` paths (in 46 files) would
   have to be rewritten on every update.
2. The rustls base is the released 0.23.46, with ringline's patches. Moving
   to 0.24 waits for its release. Updates within the line are automated (see
   "Automated updates").
3. The crypto provider stays ring until patches 1 and 2 are measured.
4. The escape hatches stay. ringline's builders are the configuration API
   users are expected to use; the re-exports cover everything else.
5. The forks live in their own repositories under `ringline-rs` (see
   "Layout"), like `resp-proto`.
