# Forking rustls and quinn-proto

Status: proposal (2026-10-27). Nothing here is built. The owner decided on
2026-10-26 that ringline maintains its own copies of rustls and quinn-proto,
so that it can change the TLS record layer and the QUIC packet path itself,
and on 2026-10-27 that each copy is a fork in its own repository under
`ringline-rs`. This document says how, and what it costs.

Related: `docs/ktls-design.md`, `docs/tls-unbuffered-design.md`,
`docs/syscalls-and-copies.md`.

## Goals

1. ringline can change rustls and quinn-proto code and release the change on
   its own schedule.
2. A caller's code compiles unchanged across ringline releases that patch a
   fork or move it to a newer upstream release on the same line. Code that
   uses the escape hatches (see "Escape hatches") is the exception.
3. An upstream security fix reaches ringline users as soon as the fork can
   release it, and `cargo audit` reports fork versions that predate the fix.

Not a goal: an abstraction over several TLS implementations. There is one
implementation, the rustls fork.

## Why

On send, rustls 0.23.46 with the ring provider makes two plain copies of each
byte besides the AEAD. On receive it makes one, and ringline makes a second
when it appends the plaintext to the accumulator. The rustls counts were
measured with an `LD_PRELOAD` shim that counts `memcpy`/`memmove` bytes,
around an in-memory unbuffered client and server, 64 records per size. That
program is not in this repository; step 3 adds it to the fork as the
copy-count test (see "Testing").

| Direction | Copy 1 | Copy 2 | Bytes copied per plaintext byte, inside rustls |
|---|---|---|---|
| Send | `PrefixedPayload` (`crypto/ring/tls13.rs:210,214`) | record into the pool slot (`common_state.rs:718`) | 2.021 at 1 KiB, 2.001 at 16 KiB |
| Receive | `into_vec` after in-place decrypt (`common_state.rs:557`, `msgs/base.rs:43`) | ringline's accumulator append (`unbuffered/mod.rs:436`) | 1.000 |

Removing either rustls copy needs a change inside rustls.

- Upstream `main` (0.24.0-dev, unreleased, breaking) removes both. It has no
  release date, and its send path writes into a `&mut Vec<u8>`, which a pool
  slot is not.
- As of 2026-10-26 there is no 0.23.x release after 0.23.46, and the 0.23
  line has no in-place API.
- Upstream issue rustls/rustls#3109 (open) proposes changing the encrypter
  traits.

ringline has no kernel QUIC path: quinn-proto encrypts and decrypts every
packet in userspace. For QUIC, changing quinn-proto is the only way to
change its copies.

kTLS is evaluated separately (`docs/ktls-design.md`). The forks do not
replace it. kTLS uses rustls 0.23's `dangerous_into_kernel_connection`, which
the fork keeps.

## Layout

### The forks

| Repository | Fork of | Published crate | Library name | Used by |
|---|---|---|---|---|
| `ringline-rs/rustls` | `rustls/rustls` | `ringline-tls` | `ringline_tls` | ringline, `ringline-tls-config`, `ringline-quic-proto` |
| `ringline-rs/quinn` | `quinn-rs/quinn` | `ringline-quic-proto` | `ringline_quic_proto` | `ringline-quic` |

- Each is a GitHub fork. It keeps upstream's history, and maintainers can
  open pull requests from it against upstream.
- ringline's code lives on one branch per upstream line: `ringline/0.23` in
  the rustls fork and `ringline/0.11` in the quinn fork. That branch is the
  fork's default branch, because GitHub runs scheduled workflows only from the
  default branch.
  - The branch starts at upstream's release tag.
  - The first commit on it renames the package and the library.
  - Each later commit is one ringline patch, a merge of an upstream release,
    or a release commit.
  - `git diff <newest merged upstream tag> ringline/0.23` is exactly ringline's
    changes.
- The library takes the package's name. A type error then names the crate
  each type came from: `ringline_tls::ServerConfig` against
  `rustls::ServerConfig`, where the two types would otherwise both print as
  `rustls::ServerConfig`. A crate can also depend on both the fork and
  upstream rustls without renaming either dependency.
- Published forks follow this convention. Each of these uses its package name
  as its library name (crates.io, 2026-10-27):
  - `iroh-quinn-proto`
  - `iroh-quinn`
  - `noq-proto`
  - `rustls-fork-shadow-tls`
  - `tokio-rustls-fork-shadow-tls`
  - `boring`
  - `serde_json_lenient`
- The rename changes the fork in three places:
  - The fork's other workspace members import it under a renamed key, so
    their `rustls::` paths stay as they are:
    `rustls = { package = "ringline-tls", path = ... }`. These members are
    the examples, benchmarks and fuzz targets in the rustls fork, and `quinn`
    and the tools in the quinn fork. `quinn` already imports quinn-proto
    under the key `proto`. `quinn-udp` depends on neither crate.

    Only `ringline-tls` and `ringline-quic-proto` are published.
  - The integration tests and benches inside the rustls package each get one
    alias line, `extern crate ringline_tls as rustls;`. The BoGo runner
    (`tests/bogo.rs`) is one of these tests.
  - Doctests that name `rustls::` get the same alias as a hidden line. 52
    doc-comment lines in 8 files under `src/` name `rustls::`. Links among
    them need no change, and the one in `src/manual/fips.rs` is
    `rust,ignore`.
- The names were checked against crates.io on 2026-10-27:
  - `ringline-tls` and `ringline-quic-proto` are unclaimed.
  - `quic-proto` belongs to an unrelated crate.
- The quinn fork depends on `rustls = { package = "ringline-tls" }`. Its
  `rustls::` paths stay as they are, and a binary has one TLS stack for TCP
  and QUIC.
- ringline enables only quinn-proto's `rustls-ring` feature, with default
  features off (workspace `Cargo.toml:63`).
  - The fork keeps `aws-lc-rs`, `qlog` and `arbitrary` optional.
  - It removes the `platform-verifier` feature: `rustls-platform-verifier`
    implements upstream rustls's traits, and quinn-proto's use of it
    (`src/crypto/rustls.rs:18,303`) does not compile against the fork.
    `quinn`'s `platform-verifier` feature, which is in its default set and
    enables quinn-proto's, is removed too.
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
- A fork patch release (0.1.x) never breaks the fork's API. It may merge a
  newer upstream release on the same line, or add a ringline patch that only
  adds API.
- A ringline patch that changes fork API is a fork minor release (0.2.0,
  0.3.0, ...), and so is moving to rustls 0.24. Only a ringline release
  adopts a fork minor release.
- Adopting a fork minor release is a breaking (0.x minor) release of
  `ringline-tls-config`, ringline and ringline-quic, released together,
  because the escape hatches name the fork's types (see "Escape hatches").
  Step 5 is the same kind of release.
  - A `ringline-tls` minor release also needs a `ringline-quic-proto` minor
    release, because quinn-proto's public API names rustls types
    (`QuicServerConfig`'s `TryFrom<Arc<rustls::ServerConfig>>`,
    `src/crypto/rustls.rs`).
  - ringline-quic takes no release of its own that changes its
    `ringline-tls-config` requirement. Otherwise a caller who runs
    `cargo update` can end up with two `ringline-tls-config` versions and a
    type mismatch between them.
- Upstream's numbers cannot be reused. A ringline-only fix released as
  `ringline-tls` 0.23.47 would leave no number for the later release that
  merges upstream 0.23.47. crates.io refuses a version that differs from a
  published one only in build metadata, so `0.23.46+ringline.2` cannot follow
  `0.23.46+ringline.1`.
- Each release's notes name the upstream tag it contains.

### Unchanged

- `ringline-benchmarks` keeps upstream rustls, `tokio-rustls`, `quinn` and
  `h3-quinn` for its tokio comparison clients. Those clients measure
  upstream's code on purpose.
- ringline's TLS tests keep upstream rustls as the peer. For example,
  `ringline/tests/tls_echo.rs` drives `rustls::ClientConnection` over a std
  socket. Every such test then checks the fork against upstream.
  ringline-quic's tests likewise use an upstream `quinn_proto::Endpoint` as
  the peer (`ringline-quic/tests/echo.rs:207`). Step 5 says how both crates
  name the fork and upstream side by side.
- quinn-proto stays the protocol crate with no I/O under `ringline-quic`, the
  same split as `resp-proto` under `ringline-redis`.

## Public API

### What exposes rustls and quinn-proto today

| Crate | Item | Exposes |
|---|---|---|
| ringline | `TlsConfig::new` (`config.rs:44`), also at `ringline::config::TlsConfig` (`lib.rs:228`) | `Arc<rustls::ServerConfig>` |
| ringline | `TlsClientConfig::new` (`config.rs:58`) | `Arc<rustls::ClientConfig>` |
| ringline | `TlsInfo::protocol_version` (`tls/mod.rs:80`) | `rustls::ProtocolVersion` |
| ringline | `TlsInfo::cipher_suite` (`tls/mod.rs:85`) | `rustls::SupportedCipherSuite` |
| ringline-quic | `QuicConfig::server`, `server_config` (`config.rs:36,70`) | `Arc<quinn_proto::ServerConfig>` |
| ringline-quic | `QuicConfig::client`, `client_config` (`config.rs:49,76`) | `quinn_proto::ClientConfig` |
| ringline-quic | `QuicConfig::endpoint_config` (`config.rs:63`) | `Arc<quinn_proto::EndpointConfig>` |
| ringline-quic | `Error` variants (`error.rs:4`) | `ConnectError`, `ConnectionError`, `ReadError`, `ReadableError`, `WriteError`, `VarInt` |
| ringline-quic | `QuicEvent` fields (`event.rs:4`) | `ConnectionError`, `Dir`, `StreamId`, `VarInt` |
| ringline-quic | `QuicEndpoint::open_bi`, `open_uni` (`endpoint.rs:569,621`) | `StreamId` |
| ringline-quic | `QuicEndpoint::stream_send`, `stream_send_chunks`, `stream_recv`, `stream_recv_into`, `stream_finish` (`endpoint.rs:342-640`) | `StreamId` |
| ringline-quic | `QuicEndpoint::stop_sending`, `reset_stream` | `StreamId`, `VarInt` |
| ringline-quic | `QuicEndpoint::connection_stats` | `ConnectionStats` |
| ringline-quic | re-exports (`lib.rs:46`) | `ConnectionStats`, `Dir`, `FrameStats`, `PathStats`, `ReadError`, `StreamId`, `UdpStats`, `VarInt`, `WriteError` |
| ringline-h3 | `H3Event` variants `Request`, `Response`, `Trailers`, `Data`, `StreamReset` (`connection.rs:155-195`) | `StreamId` |
| ringline-h3 | `send_request` returns it (`connection.rs:400`); `send_response`, `send_data`, `send_data_bytes`, `send_trailers` take it | `StreamId` |
| ringline-h3 | `has_pending_writes`, `has_request_stream` (`connection.rs:718,729`) | `StreamId` |

Each row makes a caller's code depend on the exposed crate's version. A
ringline release that changes the fork could break those callers.

### Stable configuration

The configuration types belong to ringline and live in a new crate,
`ringline-tls-config` (see "The configuration crate"). They take certificates
and keys as `rustls-pki-types` values, a 1.x crate.

```rust
let tls = TlsConfig::builder()
    .cert(chain, key)                 // Vec<CertificateDer<'static>>, PrivateKeyDer<'static>
    .sni_cert("api.example", chain2, key2)
    .alpn(&[b"h2", b"http/1.1"])
    .client_auth_roots(roots)         // optional mTLS
    .max_fragment_size(16384)         // optional
    .build()?;

let client = TlsClientConfig::builder()
    .roots(roots)                     // Vec<CertificateDer<'static>>
    .trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
    .alpn(&[b"h2"])
    .client_cert(chain, key)          // optional
    .build()?;
```

- `client_auth_roots` requires a client certificate signed by the roots.
  `client_auth_roots_optional` accepts a connection without one.
- The builders select the ring provider explicitly with
  `builder_with_provider`. `ServerConfig::builder()` panics when a caller's
  dependency tree enables both of rustls's `ring` and `aws_lc_rs` features
  (rustls `src/crypto/mod.rs:243-286`), and the builders must not.
- The builders accept TLS 1.2 and 1.3 by default, as rustls does.
  `.versions(&[TlsVersion::Tls13])` restricts them. `ringline-tls-config`
  enables rustls's `tls12` feature for this. Today ringline enables rustls
  without `tls12` (`ringline/Cargo.toml:41`), so a server negotiates TLS 1.2
  only when another crate in the build enables the feature.
- A config with TLS 1.2 enabled works for QUIC. On a QUIC connection rustls
  negotiates TLS 1.3 only and refuses a client that offers no TLS 1.3
  (`Tls13RequiredForQuic`, rustls `src/server/hs.rs:369-385`).
- `TlsInfo` returns ringline enums, `TlsVersion` and `CipherSuite`, both
  `#[non_exhaustive]`.
- A benchmark client that skips certificate verification uses
  `TlsClientConfig::builder().dangerous_no_verify()`.
- `max_fragment_size` exists because ringline's slot check reads it from the
  rustls configs (`ringline/src/config.rs:497-516`). That error message
  (`config.rs:515`) changes to name the builder method.
- The in-tree callers of `TlsConfig::new` and `TlsClientConfig::new` all port
  to the builders:

  | Callers | Files |
  |---|---|
  | ringline examples and tests | `ringline/examples/echo_tls_server.rs`, `ringline/tests/tls_echo.rs`, `recv_arrival.rs`, `recv_ready.rs`, `teardown_drops.rs` |
  | HTTP stack | `ringline-http/examples/http_get.rs`, `ringline-http/tests/public_servers.rs`, `ringline-h2/tests/round_trip.rs`, `ringline-grpc/tests/round_trip.rs` |
  | Benchmarks | `ringline-bench/src/bin/server.rs`, `ringline-benchmarks/src/protocols/http2.rs` |

  - The HTTP examples and public-server tests take their roots from
    `webpki_roots::TLS_SERVER_ROOTS`, which is
    `&[TrustAnchor<'static>]`; `trust_anchors` exists for them.
- ringline-quic builds its quinn-proto configs from the same two types. A
  caller can pass one value to ringline and ringline-quic.
  - ringline-quic clones the rustls config from `TlsConfig::rustls_config`
    and sets the QUIC-only fields on the clone. With
    `QuicConfig::zero_rtt(true)` those are `max_early_data_size = u32::MAX` on
    the server and `enable_early_data` on the client. QUIC accepts only 0 or
    `u32::MAX` for the server value (rustls `src/quic.rs:283`).
  - The TCP config keeps `max_early_data_size` at 0, because ringline's TCP
    engine treats early data as an error
    (`ringline/src/tls/unbuffered/mod.rs:497-502`).
  - `QuicConfig::alpn` overrides the ALPN list for QUIC, since H3 uses `h3`
    where TCP uses `h2` or `http/1.1`. ALPN stays optional, as it is in
    quinn-proto. Most of the QUIC and H3 tests set none.
  - `QuicConfig::server` and `QuicConfig::client` become fallible: quinn-proto
    refuses a rustls config without TLS 1.3 (`NoInitialCipherSuite`), which
    a config built with `.versions(&[TlsVersion::Tls12])` is.
  - Transport and endpoint settings get `QuicConfig` builder methods, as
    `send_queue_capacity` and `allow_mtud` already are. The in-tree tests need
    `max_idle_timeout`, `initial_mtu`, server `migration` (`peer.rs:1509`) and
    0-RTT. `QuicConfig::endpoint_config` is replaced by methods for the
    endpoint settings ringline-quic uses.
- ringline-quic owns every quinn-proto type in the ringline-quic and
  ringline-h3 rows, except the config types, which `QuicConfig` replaces.
  - `StreamId`, `VarInt` and `Dir` become ringline-quic types.
  - So do the connect, connection, read, readable and write error types, and
    the stats types.
  - `Error` and `QuicEvent` carry ringline-quic types only.
  - ringline-h3 uses ringline-quic's `StreamId`.
- `ringline::tls` is `pub(crate)` today (`ringline/src/lib.rs:190`). Step 1
  adds the public paths the escape hatches below need.
- ringline keeps exporting `TlsConfig` and `TlsClientConfig` at the paths it
  uses today, `ringline::TlsConfig` (`ringline/src/lib.rs:484-486`) and
  `ringline::config::TlsConfig`, now as re-exports. `TlsInfo` stays defined in
  ringline.
- The builders cover what the 11 in-tree TCP callers do today: a single
  certificate, a root store, ALPN, `max_fragment_size` and SNI. These need
  the escape hatches: a custom certificate verifier, a certificate resolver,
  a key log, client session resumption settings (`connect-bench.rs:124`
  disables resumption on a std client), server session storage and ticketers,
  and other crypto providers.

### The configuration crate

- `ringline-tls-config` is a new crate in this workspace. It holds
  `TlsConfig`, `TlsClientConfig`, their builders, and the `TlsVersion` and
  `CipherSuite` enums. `TlsInfo` stays in ringline.
- Its dependencies are rustls (upstream 0.23.46 until step 5, then
  `ringline-tls`) and `rustls-pki-types`. It has no dependency on the
  `ringline` crate, so ringline-quic, which depends on it, stays
  protocol-only. ringline-quic has
  `ringline` only as a dev-dependency today.
- ringline and ringline-quic both depend on it and re-export its types.
- A caller can pass one config value to both only if ringline and
  ringline-quic resolve the same `ringline-tls-config` version. Under 0.x,
  0.1 and 0.2 are different crates to the compiler, and the caller gets a type
  mismatch. Both crates therefore require the same minor version, and a
  coordinated release bumps all three together (see "Versions"). The list of
  inter-crate requirements in CLAUDE.md (Release Process) today names
  http→h2, grpc→h2 and h3→quic. Step 1 adds ringline→tls-config and
  step 2 adds quic→tls-config.
- ringline needs the built rustls configs to create connections and to run the
  slot check. ringline-quic needs them to build quinn-proto's
  `QuicServerConfig` and `QuicClientConfig`. `ringline-tls-config` provides
  them through public accessors, `TlsConfig::rustls_config` and
  `TlsClientConfig::rustls_config`. It also provides conversions from
  rustls's `ProtocolVersion` and `SupportedCipherSuite` to its enums. These
  name rustls types, so they are part of the escape hatches below.
- It starts at 0.1.0 and has its own version, like the other satellite crates.
- Because ringline-quic builds quinn-proto configs from it, its rustls and
  quinn-proto's rustls must be the same crate. It moves from upstream rustls
  to `ringline-tls` in the same change that moves ringline-quic to
  `ringline-quic-proto` (step 5).
- CLAUDE.md (API Design Principles) cites `TlsConfig::new` as its example of
  an opaque type. Step 1 updates that line.

### Escape hatches

- `ringline_tls_config::rustls` re-exports the rustls crate it is built on,
  and ringline re-exports that module as `ringline::tls::rustls`.
  `TlsConfig::from_rustls(Arc<ringline::tls::rustls::ServerConfig>)` and
  `TlsClientConfig::from_rustls` accept a config built from it. The
  accessors and conversions in "The configuration crate" belong to the same
  set.
- `ringline_quic::proto` re-exports `ringline-quic-proto`, and
  `QuicConfig::from_proto` takes its `ServerConfig`, `ClientConfig` and
  `EndpointConfig`.
- These cover custom certificate verifiers, certificate resolvers, crypto
  providers, and anything the builders do not offer yet.
- Code that uses them follows the fork's version. It can break when a
  ringline release adopts a new fork minor version (see "Versions"), and at
  step 5, where they move from upstream rustls and quinn-proto to the forks.
  Each such release takes a new `ringline-tls-config` minor version. The
  CHANGELOG entry for it names the upstream versions and the ringline
  patches.
- Every other item follows ringline's semver.

Crates that implement upstream rustls traits do not plug into the fork.
Examples are `rustls-platform-verifier`, alternative crypto providers, and a
caller's own verifier. A caller's verifier ports by changing its import path,
because the trait shapes are the same. A third-party crate that implements
upstream's traits does not port.

## Tracking upstream

- A new upstream release on the same line is merged into the ringline branch,
  for example tag 0.23.47 into `ringline/0.23`.
  - It is a merge, not a rebase, so the branch is never force-pushed and every
    update goes through a pull request.
  - Every merge conflicts in the package's `Cargo.toml` (and in `Cargo.lock`,
    if the repository commits one). Upstream changes the `version` line that
    the rename commit also changes. The conflict resolves by keeping the
    fork's name and version.
  - In the quinn fork, `quinn/Cargo.toml`'s
    `proto = { package = "quinn-proto", ..., version = "0.11.N" }` line also
    conflicts whenever upstream raises it, as it did in quinn 0.11.9, 0.11.11
    and 0.11.12. It resolves by keeping the fork's package name and version.
- Patches that apply generally are offered upstream, as pull requests from the
  fork.
  - If upstream releases the same change on the same line, the merge is clean
    and the patch leaves the diff.
  - If upstream's version differs, the merge conflicts and is resolved by
    hand to upstream's version.
  - A change upstream accepts only on `main` leaves the diff when the fork
    moves to that release line.

### Automated updates

A scheduled workflow on each fork's default branch runs daily:

1. It fetches upstream's tags and looks for a release on the branch's line
   newer than the last one merged. Moving to a new line is done by hand
   (decision 2).
2. If it finds one, it merges the tag into a new branch off the ringline
   branch.
   - It resolves the `Cargo.toml`, `Cargo.lock` and quinn `proto` line
     conflicts described above.
   - It runs the fork's tests.
3. It opens a pull request naming the upstream release and any advisories it
   fixes. If any other file conflicts, it opens an issue naming those files
   instead.
4. If the advisory check (see "Security process") reports an advisory against
   the last merged release, the workflow runs at once, and the pull request is
   labelled as a security fix.

The workflow authenticates as a GitHub App installed on the fork and on
ringline. A pull request opened with the default `GITHUB_TOKEN` does not
trigger `pull_request` workflows, so its checks would never run.

The pull request is reviewed and merged like any other, then the fork
releases a patch version. The same workflow then opens a pull request in
ringline that raises the fork's requirement in the workspace `Cargo.toml`
and updates `Cargo.lock` (see "Security process"). ringline has no
Dependabot configuration today.

- The workflow iterates over a configured list of fork branches. A fork
  minor version that a maintained ringline line (`release/X.Y.x`) still uses
  gets its own branch on that list.
- An advisory fixed only on a newer upstream line opens an issue for a
  manual backport.

## Security process

The workspace `Cargo.toml` (lines 52 to 85) records the advisories behind
today's version floors.

- Four set the rustls floor of 0.23.46: RUSTSEC-2026-0346 to 0349.
- The same comment also lists RUSTSEC-2026-0285 (fixed in 0.23.45) and two
  2024 advisories.
- Eight set the quinn-proto floor of 0.11.19: RUSTSEC-2026-0338 to 0343 and
  two GHSA advisories.

With the forks, ringline is responsible for shipping fixes like these.

- **Detection.** `cargo audit` matches advisories by crate name, so it does not
  see `ringline-tls`.
  - A daily job in each fork writes a lockfile listing `rustls` (or
    `quinn-proto`) at the last merged upstream release, with the crates.io
    `source` line, and runs `cargo audit` against that file.
  - The job also audits a second lockfile at a version with a known
    advisory, rustls 0.23.44 for example, and fails unless that one is
    reported.
  - An advisory against the merged version fails the job and starts the
    update workflow.
  - cargo-audit's flag for a lockfile path is unconfirmed. Confirm it before
    writing this job.
- **Fix.**
  - Merge the fixed upstream release and release the fork as a patch
    version. A downstream `cargo update` picks it up at once.
  - The ringline pull request raises the `ringline-tls` (or
    `ringline-quic-proto`) requirement in the workspace `Cargo.toml` to the
    fixed version, with the advisory in the comment, as the rustls and
    quinn-proto floors are recorded today. A ringline patch release carries
    the floor to downstream resolvers.
- **Notice.** File a RustSec advisory against the affected `ringline-tls`
  versions, citing the upstream advisory, so downstream `cargo audit` runs
  report it.

## Testing

- The fork's CI runs upstream's test suites on the ringline branch. That
  covers rustls's tests and quinn-proto's `src/tests`.
- A patch to the record layer also passes rustls's fuzz targets and its BoGo
  runner (the BoringSSL protocol test suite) before release. Both come with
  the fork.
- ringline's TLS tests run against an upstream rustls peer (see "Unchanged").
- A copy-count test in the fork asserts bytes copied per plaintext byte for
  send and receive. It is the counting program described under "Why", and it
  fails if a merge brings a copy back.

## First patches

To rustls, on 0.23.46:

1. **Send.** Add `MessageEncrypter::encrypt_into(msg, seq, out: &mut [u8]) -> Result<usize>`.
   - The default falls back to today's path.
   - The ring and aws-lc-rs TLS 1.3 encrypters override it.
   - `write_fragments` calls it with the pool slot.
   - This removes the `PrefixedPayload` allocation and copy from the TLS 1.3
     send path. `PrefixedPayload` itself stays: it is the payload type of
     `OutboundOpaqueMessage` and the default path uses it.
   - Estimated 80 to 150 lines.
2. **Receive.** Record the decrypted range in place of `into_vec`, and lend it
   from `ReadTraffic::next_record`.
   - ringline copies it into the accumulator, which leaves one copy.
   - In 0.23 each `ReadTraffic` carries one record, so the borrowed region
     stays valid until the next call. That is from reading the code and has
     not been tested.
   - Estimated 100 to 200 lines.
3. **AEAD only.** An out-of-place seal and open with aws-lc-rs ≥ 1.18.1:
   `LessSafeKey::seal_out_of_place_scatter` from caller memory into the slot,
   and `LessSafeKey::open_separate_gather` from the ciphertext buffer into the
   accumulator.
   - These are the non-FIPS path. The FIPS opening key
     (`TlsRecordOpeningKey`) has no out-of-place open.
   - `open_separate_gather` may write plaintext into its output before the
     tag check fails. Plaintext written to the accumulator must not be
     readable until the check passes.
   - The record's inner content type and padding arrive with the plaintext,
     so alerts and KeyUpdate records reach the accumulator before their type
     is known. The patch has to handle that.
   - ring 0.17 has no out-of-place AEAD. This patch needs aws-lc-rs (see
     decision 3).

For the default unbuffered engine on io_uring:

| | Send | Receive |
|---|---|---|
| 0.23.46 today | 2 copies + AEAD | 2 copies + AEAD |
| After 1 and 2 | 1 copy + AEAD | 1 copy + AEAD |
| After 3 (aws-lc-rs) | AEAD only | AEAD only |

- On mio, receive has one more copy: `unbuffered::feed` appends the
  ciphertext into `CiphertextBuf` (`unbuffered/mod.rs:586-603`).
- The `tls-buffered` engine receives through `reader()`, which patch 2 does
  not change.
- These counts leave out the kernel's own socket copy, which these patches do
  not change (see `docs/ktls-design.md`, "Send path").

No quinn-proto patch is proposed until the QUIC path has been profiled.

Each patch is measured on its own, as server CPU per operation at 64 B,
4 KiB and 64 KiB values, with and without TLS.

## Sequence

Each step is a separate pull request in the repository it changes.

1. `ringline-tls-config` and ringline's stable TLS configuration, over
   upstream rustls 0.23.46. It changes `TlsConfig::new`,
   `TlsClientConfig::new` and `TlsInfo`, so it lands before 0.7.0. Callers
   port once.
   - Add `publish_if_needed ringline-tls-config` to
     `.github/workflows/release.yml` ahead of `ringline`, which publishes
     first today.
   - The comment at workspace `Cargo.toml:82-84` names ringline and
     ringline-quic as the published crates carrying the `rustls-webpki`
     floor. `ringline-tls-config` uses rustls outside its dev-dependencies
     too, so it depends on `rustls-webpki` directly and the comment names
     it.
2. ringline-quic's configuration on the same types, and ringline-quic-owned
   stream, error, event and stats types, over upstream quinn-proto 0.11.19.
   This changes ringline-quic's and ringline-h3's public API, so it also lands
   before 0.7.0.
3. Fork rustls as `ringline-rs/rustls`.
   - Branch `ringline/0.23` from the 0.23.46 tag and rename the package and
     library.
   - Add the copy-count test, the advisory check and the update workflow.
   - Publish `ringline-tls` 0.1.0.
   - The diff against upstream is the rename, the alias lines and the new
     test, with no behaviour change.
4. Fork quinn as `ringline-rs/quinn`, with `ringline-quic-proto` 0.1.0 based
   on quinn-proto 0.11.19 and using `ringline-tls`.
5. Switch ringline, `ringline-tls-config` and ringline-quic to both forks in
   one pull request. They switch together because ringline-quic passes the
   rustls configs from `ringline-tls-config` to quinn-proto (see "The
   configuration crate").
   - ringline imports the fork under its library name, `ringline_tls`. That
     rewrites the `rustls` paths in `ringline/src`, 113 lines in 7 files. The
     tests keep the `rustls` key for upstream rustls.
   - Both cannot use the `rustls` key. Cargo rejects a manifest whose
     `[dependencies]` and `[dev-dependencies]` give one key two packages:
     "Dependency 'x' has different source paths depending on the build
     target."
   - ringline-quic does the same with `ringline_quic_proto`, 27 lines in 5
     files under `ringline-quic/src`. Its tests keep `quinn_proto` for the
     upstream peer.
   - This is a breaking release of ringline, ringline-quic and
     `ringline-tls-config` (see "Versions").
6. rustls patches 1 and 2, each measured.
7. Patch 3, if decision 3 moves to aws-lc-rs.

Steps 3 to 7 change no caller-visible item outside the escape hatches. Step 5
changes what the escape hatches name. Steps 1 and 2 make that possible.

## Costs

- **Two more repositories and one more crate.** ringline maintains 49,208
  lines of rustls source and 31,209 of quinn-proto, plus their tests. These
  are counts of `src/*.rs` in the crates.io packages. `ringline-tls-config`
  is one more crate to release with ringline and ringline-quic.
- **Changes across repositories.** A ringline change that needs a fork patch
  takes a pull request in the fork, a fork release, and then the ringline pull
  request. Protocol changes to `resp-proto` already work this way.
- **Upstream releases.** Every upstream security release becomes a
  `ringline-tls` or `ringline-quic-proto` release (see "Security process").
- **Duplicate rustls.** A binary that also uses upstream rustls, through
  `reqwest` or `tokio-rustls` for example, contains both copies.
- **Release coupling.** ringline-quic releases with ringline whenever
  `ringline-tls-config` changes minor version (see "Versions").
- **Ecosystem crates.** Crates built on upstream's traits do not plug in (see
  "Escape hatches").
- **Who reviews the patches.** Patches to the record layer are
  security-relevant. Upstream review does not cover them unless they are
  offered upstream.

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
   users are expected to use, and the escape hatches cover everything else.
5. The forks live in their own repositories under `ringline-rs` (see
   "Layout"), like `resp-proto`.
6. Each fork's library is named after its package: `ringline_tls` and
   `ringline_quic_proto`.
7. The configuration types live in a new crate, `ringline-tls-config`, which
   ringline and ringline-quic both re-export (see "The configuration crate").
   The alternatives were for ringline-quic to depend on ringline, which brings
   the whole runtime into the protocol-only crate, or to duplicate the
   builders in ringline-quic, which gives TCP and QUIC different config types.
8. TLS 1.2 is enabled by default on TCP, because it is still common on the
   servers ringline's clients connect to.
