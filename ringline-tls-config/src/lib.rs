//! TLS configuration for [ringline] and [ringline-quic].
//!
//! [`TlsConfig`] configures servers and [`TlsClientConfig`] configures
//! clients. Both are built with builders that take certificates and keys as
//! [`rustls_pki_types`] values:
//!
//! ```
//! # use ringline_tls_config::TlsConfig;
//! # let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
//! # let chain = vec![cert.cert.der().clone()];
//! # let key = rustls_pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
//! let tls = TlsConfig::builder()
//!     .cert(chain, key)
//!     .alpn(&[b"h2", b"http/1.1"])
//!     .build()?;
//! # Ok::<(), ringline_tls_config::Error>(())
//! ```
//!
//! The builders use rustls's *ring* crypto provider and enable TLS 1.2 and
//! TLS 1.3.
//!
//! # Escape hatches
//!
//! [`TlsConfig::from_rustls`], [`TlsClientConfig::from_rustls`],
//! [`TlsConfig::rustls_config`], [`TlsClientConfig::rustls_config`],
//! [`TlsClientConfig::quic_resumption`],
//! [`TlsVersion::from_rustls`], [`CipherSuite::from_rustls`] and the
//! [`rustls`] re-export name rustls types. They cover what the builders do
//! not: custom certificate verifiers and resolvers, client certificates,
//! session storage, key logs and other crypto providers. These items follow
//! the version of the rustls crate this one is built on, and a release that
//! moves to a new rustls version changes them. Every other item changes only
//! in a breaking release of this crate.
//!
//! [ringline]: https://docs.rs/ringline
//! [ringline-quic]: https://docs.rs/ringline-quic

use std::sync::Arc;

use rustls_pki_types::{CertificateDer, PrivateKeyDer, TrustAnchor};

/// The rustls crate the configurations are built on.
///
/// For use with [`TlsConfig::from_rustls`] and
/// [`TlsClientConfig::from_rustls`]. Items reached through it follow
/// rustls's version, not this crate's.
pub use rustls;

/// The largest `max_fragment_size` rustls accepts: 16384 bytes of plaintext
/// plus the 5-byte header length.
const MAX_FRAGMENT_SIZE: usize = 16384 + 5;

/// The smallest `max_fragment_size` rustls accepts.
const MIN_FRAGMENT_SIZE: usize = 32;

/// An error from [`TlsConfigBuilder::build`] or [`TlsClientConfigBuilder::build`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// [`TlsConfigBuilder::cert`] was not called, or was given an empty
    /// chain.
    #[error(
        "a server TLS config needs a certificate: call TlsConfigBuilder::cert with a non-empty chain"
    )]
    NoCertificate,
    /// [`TlsConfigBuilder::versions`] or [`TlsClientConfigBuilder::versions`]
    /// was given an empty list.
    #[error("no TLS versions are enabled")]
    NoVersions,
    /// The `max_fragment_size` is outside the range rustls accepts.
    #[error("max_fragment_size is {0}, outside the accepted range 32..=16389")]
    BadMaxFragmentSize(usize),
    /// A certificate passed to [`TlsClientConfigBuilder::root_certificates`]
    /// could not be parsed as a trust anchor.
    #[error("invalid root certificate: {0}")]
    InvalidRootCertificate(String),
    /// rustls rejected the configuration, for example a key that does not
    /// match the certificate. The text is rustls's message.
    #[error("TLS configuration rejected: {0}")]
    Tls(String),
}

/// A TLS protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TlsVersion {
    /// TLS 1.2.
    Tls12,
    /// TLS 1.3.
    Tls13,
}

impl TlsVersion {
    /// The `TlsVersion` for a rustls `ProtocolVersion`, or `None` for a
    /// version this enum does not name. Part of the escape hatches (see the
    /// crate docs).
    pub fn from_rustls(version: rustls::ProtocolVersion) -> Option<Self> {
        match version {
            rustls::ProtocolVersion::TLSv1_2 => Some(Self::Tls12),
            rustls::ProtocolVersion::TLSv1_3 => Some(Self::Tls13),
            _ => None,
        }
    }

    fn rustls_version(self) -> &'static rustls::SupportedProtocolVersion {
        match self {
            Self::Tls12 => &rustls::version::TLS12,
            Self::Tls13 => &rustls::version::TLS13,
        }
    }
}

/// A TLS cipher suite.
///
/// The named variants are the suites rustls's *ring* provider offers. A
/// suite whose IANA code is not one of these, for example one that only
/// another provider offers (see [`TlsConfig::from_rustls`]), is
/// [`Unknown`](Self::Unknown) with its code. To test for a specific suite,
/// compare [`iana_code`](Self::iana_code) rather than matching `Unknown`:
/// a later release may name a suite that is `Unknown` today.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[allow(non_camel_case_types)]
pub enum CipherSuite {
    /// `TLS13_AES_128_GCM_SHA256` (0x1301).
    TLS13_AES_128_GCM_SHA256,
    /// `TLS13_AES_256_GCM_SHA384` (0x1302).
    TLS13_AES_256_GCM_SHA384,
    /// `TLS13_CHACHA20_POLY1305_SHA256` (0x1303).
    TLS13_CHACHA20_POLY1305_SHA256,
    /// `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256` (0xc02b).
    TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
    /// `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384` (0xc02c).
    TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
    /// `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256` (0xc02f).
    TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
    /// `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384` (0xc030).
    TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    /// `TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256` (0xcca8).
    TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    /// `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` (0xcca9).
    TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
    /// A suite not named above, by its IANA code.
    Unknown(u16),
}

impl CipherSuite {
    /// The suite's IANA code.
    pub fn iana_code(self) -> u16 {
        match self {
            Self::TLS13_AES_128_GCM_SHA256 => 0x1301,
            Self::TLS13_AES_256_GCM_SHA384 => 0x1302,
            Self::TLS13_CHACHA20_POLY1305_SHA256 => 0x1303,
            Self::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 => 0xc02b,
            Self::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 => 0xc02c,
            Self::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 => 0xc02f,
            Self::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 => 0xc030,
            Self::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 => 0xcca8,
            Self::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 => 0xcca9,
            Self::Unknown(code) => code,
        }
    }

    /// The suite with the given IANA code.
    pub fn from_iana_code(code: u16) -> Self {
        match code {
            0x1301 => Self::TLS13_AES_128_GCM_SHA256,
            0x1302 => Self::TLS13_AES_256_GCM_SHA384,
            0x1303 => Self::TLS13_CHACHA20_POLY1305_SHA256,
            0xc02b => Self::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            0xc02c => Self::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            0xc02f => Self::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            0xc030 => Self::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            0xcca8 => Self::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
            0xcca9 => Self::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            other => Self::Unknown(other),
        }
    }

    /// The suite rustls names. Part of the escape hatches (see the crate
    /// docs).
    pub fn from_rustls(suite: rustls::CipherSuite) -> Self {
        Self::from_iana_code(u16::from(suite))
    }
}

/// Server TLS configuration.
///
/// Build one with [`TlsConfig::builder`]. Cloning is cheap: the rustls
/// configuration is shared.
#[derive(Clone)]
pub struct TlsConfig {
    inner: Arc<rustls::ServerConfig>,
}

impl TlsConfig {
    /// Start building a server configuration.
    pub fn builder() -> TlsConfigBuilder {
        TlsConfigBuilder {
            cert: None,
            alpn: Vec::new(),
            max_fragment_size: None,
            versions: default_versions(),
        }
    }

    /// Wrap a rustls server configuration built by the caller. Part of the
    /// escape hatches (see the crate docs).
    pub fn from_rustls(config: Arc<rustls::ServerConfig>) -> Self {
        Self { inner: config }
    }

    /// The rustls server configuration. Part of the escape hatches (see the
    /// crate docs).
    pub fn rustls_config(&self) -> &Arc<rustls::ServerConfig> {
        &self.inner
    }

    /// The configured `max_fragment_size` (see
    /// [`TlsConfigBuilder::max_fragment_size`]), or `None` for rustls's
    /// default of 16384 bytes of plaintext per record.
    pub fn max_fragment_size(&self) -> Option<usize> {
        self.inner.max_fragment_size
    }
}

/// Builder for [`TlsConfig`].
pub struct TlsConfigBuilder {
    cert: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    alpn: Vec<Vec<u8>>,
    max_fragment_size: Option<usize>,
    versions: Vec<TlsVersion>,
}

impl TlsConfigBuilder {
    /// The certificate chain, leaf first, and its private key. Required.
    pub fn cert(
        mut self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        self.cert = Some((chain, key));
        self
    }

    /// The ALPN protocols to offer, most preferred first. Defaults to none.
    pub fn alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = protocols.iter().map(|p| p.to_vec()).collect();
        self
    }

    /// The plaintext per TLS record plus 5 (the record header length), in
    /// `32..=16389`. A record then carries at most `size - 5` bytes of
    /// plaintext and is at most `size + 17` bytes on the wire with TLS 1.3,
    /// or `size + 24` with a TLS 1.2 GCM suite. Defaults to 16384 bytes of
    /// plaintext per record.
    pub fn max_fragment_size(mut self, size: usize) -> Self {
        self.max_fragment_size = Some(size);
        self
    }

    /// The TLS versions to accept. Defaults to TLS 1.2 and TLS 1.3.
    ///
    /// A TLS 1.2 connection that writes 2^24 records with an AES-GCM suite
    /// is closed, because TLS 1.2 cannot change its keys; at 100,000 records
    /// per second that is about 168 s. TLS 1.3 changes keys instead.
    /// `versions(&[TlsVersion::Tls13])` avoids the limit.
    pub fn versions(mut self, versions: &[TlsVersion]) -> Self {
        self.versions = versions.to_vec();
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Result<TlsConfig, Error> {
        let (chain, key) = self.cert.ok_or(Error::NoCertificate)?;
        if chain.is_empty() {
            return Err(Error::NoCertificate);
        }
        let versions = rustls_versions(&self.versions)?;
        check_fragment_size(self.max_fragment_size)?;
        let mut config = rustls::ServerConfig::builder_with_provider(provider())
            .with_protocol_versions(&versions)
            .map_err(tls_error)?
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .map_err(tls_error)?;
        config.alpn_protocols = self.alpn;
        config.max_fragment_size = self.max_fragment_size;
        Ok(TlsConfig {
            inner: Arc::new(config),
        })
    }
}

/// Client TLS configuration.
///
/// Build one with [`TlsClientConfig::builder`]. Cloning is cheap: the rustls
/// configuration is shared.
#[derive(Clone)]
pub struct TlsClientConfig {
    inner: Arc<rustls::ClientConfig>,
    /// The session store QUIC connections use, separate from TCP's; see
    /// [`TlsClientConfig::quic_resumption`]. `None` for a `from_rustls`
    /// config.
    quic_resumption: Option<rustls::client::Resumption>,
}

impl TlsClientConfig {
    /// Start building a client configuration.
    pub fn builder() -> TlsClientConfigBuilder {
        TlsClientConfigBuilder {
            root_certificates: Vec::new(),
            trust_anchors: Vec::new(),
            alpn: Vec::new(),
            max_fragment_size: None,
            versions: default_versions(),
        }
    }

    /// Wrap a rustls client configuration built by the caller. Part of the
    /// escape hatches (see the crate docs).
    pub fn from_rustls(config: Arc<rustls::ClientConfig>) -> Self {
        Self {
            inner: config,
            quic_resumption: None,
        }
    }

    /// The rustls client configuration. Part of the escape hatches (see the
    /// crate docs).
    pub fn rustls_config(&self) -> &Arc<rustls::ClientConfig> {
        &self.inner
    }

    /// The session-resumption store for QUIC connections made with this
    /// configuration, or `None` for a configuration from
    /// [`from_rustls`](Self::from_rustls). Part of the escape hatches (see
    /// the crate docs).
    ///
    /// rustls keys stored tickets by server name only, so TCP and QUIC
    /// connections sharing one store would take each other's tickets, and a
    /// QUIC connection holding a ticket from a TCP server sends no 0-RTT.
    /// ringline-quic gives QUIC connections this store instead. It is
    /// created when the configuration is built and is shared by its clones.
    /// A `from_rustls` configuration keeps the caller's `resumption` for
    /// both.
    pub fn quic_resumption(&self) -> Option<&rustls::client::Resumption> {
        self.quic_resumption.as_ref()
    }

    /// The configured `max_fragment_size` (see
    /// [`TlsClientConfigBuilder::max_fragment_size`]), or `None` for rustls's
    /// default of 16384 bytes of plaintext per record.
    pub fn max_fragment_size(&self) -> Option<usize> {
        self.inner.max_fragment_size
    }
}

/// Builder for [`TlsClientConfig`].
pub struct TlsClientConfigBuilder {
    root_certificates: Vec<CertificateDer<'static>>,
    trust_anchors: Vec<TrustAnchor<'static>>,
    alpn: Vec<Vec<u8>>,
    max_fragment_size: Option<usize>,
    versions: Vec<TlsVersion>,
}

impl TlsClientConfigBuilder {
    /// Trust servers whose chains end at these certificates. Adds to any
    /// roots given before.
    pub fn root_certificates(
        mut self,
        certs: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.root_certificates.extend(certs);
        self
    }

    /// Trust servers whose chains end at these trust anchors, for example
    /// `webpki_roots::TLS_SERVER_ROOTS`. Adds to any roots given before.
    pub fn trust_anchors(
        mut self,
        anchors: impl IntoIterator<Item = TrustAnchor<'static>>,
    ) -> Self {
        self.trust_anchors.extend(anchors);
        self
    }

    /// The ALPN protocols to offer, most preferred first. Defaults to none.
    pub fn alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = protocols.iter().map(|p| p.to_vec()).collect();
        self
    }

    /// The plaintext per TLS record plus 5 (the record header length), in
    /// `32..=16389`. A record then carries at most `size - 5` bytes of
    /// plaintext and is at most `size + 17` bytes on the wire with TLS 1.3,
    /// or `size + 24` with a TLS 1.2 GCM suite. Defaults to 16384 bytes of
    /// plaintext per record.
    pub fn max_fragment_size(mut self, size: usize) -> Self {
        self.max_fragment_size = Some(size);
        self
    }

    /// The TLS versions to offer. Defaults to TLS 1.2 and TLS 1.3. See
    /// [`TlsConfigBuilder::versions`] for the TLS 1.2 record limit.
    pub fn versions(mut self, versions: &[TlsVersion]) -> Self {
        self.versions = versions.to_vec();
        self
    }

    /// Build the configuration. With no roots, the client trusts no server.
    pub fn build(self) -> Result<TlsClientConfig, Error> {
        let versions = rustls_versions(&self.versions)?;
        check_fragment_size(self.max_fragment_size)?;
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(self.trust_anchors);
        for cert in self.root_certificates {
            roots
                .add(cert)
                .map_err(|e| Error::InvalidRootCertificate(e.to_string()))?;
        }
        let mut config = rustls::ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&versions)
            .map_err(tls_error)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        config.alpn_protocols = self.alpn;
        config.max_fragment_size = self.max_fragment_size;
        Ok(TlsClientConfig {
            inner: Arc::new(config),
            quic_resumption: Some(rustls::client::Resumption::default()),
        })
    }
}

fn default_versions() -> Vec<TlsVersion> {
    vec![TlsVersion::Tls13, TlsVersion::Tls12]
}

/// rustls's *ring* provider, named explicitly. `ServerConfig::builder()`
/// and the verifier builders panic when no process default provider is
/// installed and rustls's features do not name exactly one provider (both
/// `ring` and `aws_lc_rs`, or `custom-provider`).
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn rustls_versions(
    versions: &[TlsVersion],
) -> Result<Vec<&'static rustls::SupportedProtocolVersion>, Error> {
    if versions.is_empty() {
        return Err(Error::NoVersions);
    }
    Ok(versions.iter().map(|v| v.rustls_version()).collect())
}

fn check_fragment_size(size: Option<usize>) -> Result<(), Error> {
    match size {
        Some(s) if !(MIN_FRAGMENT_SIZE..=MAX_FRAGMENT_SIZE).contains(&s) => {
            Err(Error::BadMaxFragmentSize(s))
        }
        _ => Ok(()),
    }
}

fn tls_error(e: rustls::Error) -> Error {
    Error::Tls(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn self_signed() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
        (vec![cert.cert.der().clone()], key)
    }

    fn handshake(server: &TlsConfig, client: &TlsClientConfig) -> rustls::ClientConnection {
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut c = rustls::ClientConnection::new(client.rustls_config().clone(), name).unwrap();
        let mut s = rustls::ServerConnection::new(server.rustls_config().clone()).unwrap();
        while c.is_handshaking() || s.is_handshaking() {
            let mut buf = Vec::new();
            c.write_tls(&mut buf).unwrap();
            s.read_tls(&mut buf.as_slice()).unwrap();
            s.process_new_packets().unwrap();
            buf.clear();
            s.write_tls(&mut buf).unwrap();
            c.read_tls(&mut buf.as_slice()).unwrap();
            c.process_new_packets().unwrap();
        }
        c
    }

    fn pair(versions: &[TlsVersion]) -> (TlsConfig, TlsClientConfig) {
        let (chain, key) = self_signed();
        let server = TlsConfig::builder()
            .cert(chain.clone(), key)
            .alpn(&[b"h2", b"http/1.1"])
            .versions(versions)
            .build()
            .unwrap();
        let client = TlsClientConfig::builder()
            .root_certificates(chain)
            .alpn(&[b"http/1.1"])
            .versions(versions)
            .build()
            .unwrap();
        (server, client)
    }

    #[test]
    fn builders_handshake_with_tls13_by_default() {
        // No `.versions`: the builders' own defaults must include TLS 1.3.
        let (chain, key) = self_signed();
        let server = TlsConfig::builder()
            .cert(chain.clone(), key)
            .alpn(&[b"h2", b"http/1.1"])
            .build()
            .unwrap();
        let client = TlsClientConfig::builder()
            .root_certificates(chain)
            .alpn(&[b"http/1.1"])
            .build()
            .unwrap();
        let c = handshake(&server, &client);
        assert_eq!(
            c.protocol_version().and_then(TlsVersion::from_rustls),
            Some(TlsVersion::Tls13)
        );
        assert_eq!(c.alpn_protocol(), Some(&b"http/1.1"[..]));
    }

    #[test]
    fn tls12_is_enabled_by_default() {
        // Neither builder is given `.versions`; each side is paired with a
        // peer restricted to TLS 1.2, and the handshake must complete.
        let (chain, key) = self_signed();
        let server = TlsConfig::builder()
            .cert(chain.clone(), key.clone_key())
            .build()
            .unwrap();
        let client = TlsClientConfig::builder()
            .root_certificates(chain.clone())
            .build()
            .unwrap();
        let server12 = TlsConfig::builder()
            .cert(chain.clone(), key)
            .versions(&[TlsVersion::Tls12])
            .build()
            .unwrap();
        let client12 = TlsClientConfig::builder()
            .root_certificates(chain)
            .versions(&[TlsVersion::Tls12])
            .build()
            .unwrap();
        for (s, c) in [(&server, &client12), (&server12, &client)] {
            let conn = handshake(s, c);
            assert_eq!(
                conn.protocol_version().and_then(TlsVersion::from_rustls),
                Some(TlsVersion::Tls12)
            );
        }
    }

    #[test]
    fn versions_restricts_to_tls13() {
        let (chain, key) = self_signed();
        let server = TlsConfig::builder()
            .cert(chain, key)
            .versions(&[TlsVersion::Tls13])
            .build()
            .unwrap();
        let (_, client12) = pair(&[TlsVersion::Tls12]);
        let name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut c = rustls::ClientConnection::new(client12.rustls_config().clone(), name).unwrap();
        let mut s = rustls::ServerConnection::new(server.rustls_config().clone()).unwrap();
        let mut buf = Vec::new();
        c.write_tls(&mut buf).unwrap();
        s.read_tls(&mut buf.as_slice()).unwrap();
        assert!(s.process_new_packets().is_err());
    }

    #[test]
    fn cipher_suite_round_trips_through_rustls() {
        let (server, client) = pair(&default_versions());
        let c = handshake(&server, &client);
        let suite = CipherSuite::from_rustls(c.negotiated_cipher_suite().unwrap().suite());
        assert!(!matches!(suite, CipherSuite::Unknown(_)), "{suite:?}");
        assert_eq!(CipherSuite::from_iana_code(suite.iana_code()), suite);
        assert_eq!(
            CipherSuite::from_iana_code(0x00ff),
            CipherSuite::Unknown(0x00ff)
        );
    }

    #[test]
    fn every_ring_suite_is_named() {
        for suite in rustls::crypto::ring::default_provider().cipher_suites {
            let ours = CipherSuite::from_rustls(suite.suite());
            assert!(
                !matches!(ours, CipherSuite::Unknown(_)),
                "{:?}",
                suite.suite()
            );
            assert_eq!(ours.iana_code(), u16::from(suite.suite()));
        }
    }

    #[test]
    fn max_fragment_size_is_set_and_checked() {
        let (chain, key) = self_signed();
        let server = TlsConfig::builder()
            .cert(chain.clone(), key.clone_key())
            .max_fragment_size(2048)
            .build()
            .unwrap();
        assert_eq!(server.max_fragment_size(), Some(2048));
        for good in [32, 16389] {
            let server = TlsConfig::builder()
                .cert(chain.clone(), key.clone_key())
                .max_fragment_size(good)
                .build()
                .unwrap();
            assert_eq!(server.max_fragment_size(), Some(good));
        }
        for bad in [31, 16390] {
            let err = TlsConfig::builder()
                .cert(chain.clone(), key.clone_key())
                .max_fragment_size(bad)
                .build()
                .err()
                .unwrap();
            assert!(
                matches!(err, Error::BadMaxFragmentSize(s) if s == bad),
                "{err}"
            );
        }
        let client = TlsClientConfig::builder()
            .max_fragment_size(32)
            .build()
            .unwrap();
        assert_eq!(client.max_fragment_size(), Some(32));
    }

    #[test]
    fn build_errors() {
        assert!(matches!(
            TlsConfig::builder().build().err().unwrap(),
            Error::NoCertificate
        ));
        let (_, key) = self_signed();
        assert!(matches!(
            TlsConfig::builder()
                .cert(Vec::new(), key)
                .build()
                .err()
                .unwrap(),
            Error::NoCertificate
        ));
        let (chain, key) = self_signed();
        assert!(matches!(
            TlsConfig::builder()
                .cert(chain, key)
                .versions(&[])
                .build()
                .err()
                .unwrap(),
            Error::NoVersions
        ));
        let (chain, _) = self_signed();
        let (_, other_key) = self_signed();
        assert!(matches!(
            TlsConfig::builder()
                .cert(chain, other_key)
                .build()
                .err()
                .unwrap(),
            Error::Tls(_)
        ));
        assert!(matches!(
            TlsClientConfig::builder()
                .root_certificates([CertificateDer::from(vec![1, 2, 3])])
                .build()
                .err()
                .unwrap(),
            Error::InvalidRootCertificate(_)
        ));
    }

    #[test]
    fn from_rustls_keeps_the_config() {
        let (server, _) = pair(&default_versions());
        let wrapped = TlsConfig::from_rustls(server.rustls_config().clone());
        assert!(Arc::ptr_eq(wrapped.rustls_config(), server.rustls_config()));
    }
}
