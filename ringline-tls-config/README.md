# ringline-tls-config

TLS configuration for [ringline](https://crates.io/crates/ringline):
`TlsConfig` for servers, `TlsClientConfig` for clients, their builders, and
the `TlsVersion` and `CipherSuite` enums that `ringline::TlsInfo` reports.
ringline re-exports all of them, so a ringline user does not need to depend
on this crate directly.

```rust
use ringline_tls_config::{TlsClientConfig, TlsConfig};

let server = TlsConfig::builder()
    .cert(chain, key)
    .alpn(&[b"h2", b"http/1.1"])
    .build()?;

let client = TlsClientConfig::builder()
    .trust_anchors(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())
    .alpn(&[b"h2"])
    .build()?;
```

The builders use rustls's *ring* provider and accept TLS 1.2 and TLS 1.3.
`TlsConfig::from_rustls` and `TlsClientConfig::from_rustls` wrap a
configuration for what the builders do not cover, such as client
certificates or a custom verifier. Build it with
`ringline_tls_config::rustls`, which re-exports `ringline-tls`, ringline's
fork of rustls 0.23. Configurations from upstream `rustls`, and crates that
implement upstream rustls's traits such as `rustls-platform-verifier`, do
not fit. The `from_rustls` constructors, the `rustls_config` accessors,
`quic_resumption`, the enums' `from_rustls` conversions and the `rustls`
re-export follow `ringline-tls`'s version; the rest of the crate follows its
own semver.

The design is in
[docs/rustls-quinn-forks-design.md](https://github.com/ringline-rs/ringline/blob/main/docs/rustls-quinn-forks-design.md).
