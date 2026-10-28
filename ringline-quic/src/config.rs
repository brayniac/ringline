use std::sync::Arc;
use std::time::Duration;

use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig, TransportConfig};
use ringline_tls_config::{TlsClientConfig, TlsConfig, rustls};

use crate::types::VarInt;

/// Configuration for a [`QuicEndpoint`](crate::QuicEndpoint).
///
/// Build one with [`QuicConfig::builder`] from a ringline
/// [`TlsConfig`] (to accept connections), a [`TlsClientConfig`] (to make
/// them), or both. [`QuicConfig::from_proto`] takes complete quinn-proto
/// configurations instead.
///
/// `Clone` lets the same configuration drive several per-worker endpoints
/// (one `QuicEndpoint` per ringline worker, each bound to its own port
/// with `SO_REUSEPORT`). The quinn-proto configurations are shared, so a
/// clone is cheap.
#[derive(Clone)]
pub struct QuicConfig {
    /// Shared endpoint configuration (connection IDs, supported versions, etc.).
    pub(crate) endpoint_config: Arc<EndpointConfig>,
    /// Server-side TLS/QUIC config. `None` for client-only endpoints.
    pub(crate) server_config: Option<Arc<ServerConfig>>,
    /// Client-side TLS/QUIC config. `None` for server-only endpoints.
    pub(crate) client_config: Option<ClientConfig>,
    /// Most outgoing packets held for `poll_send`; see
    /// [`QuicConfig::send_queue_capacity`].
    pub(crate) send_queue_capacity: usize,
    /// Maximum number of datagrams quinn-proto may produce in a single
    /// `poll_transmit` call; see [`QuicConfig::max_transmit_datagrams`].
    pub(crate) max_transmit_datagrams: usize,
    /// Allow path MTU discovery. Default: true.
    pub(crate) allow_mtud: bool,
    /// Deterministic RNG seed for testing. Default: `None` (random).
    pub(crate) rng_seed: Option<[u8; 32]>,
}

/// An error from [`QuicConfigBuilder::build`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Neither [`QuicConfigBuilder::server`] nor
    /// [`QuicConfigBuilder::client`] was called.
    #[error("a QUIC config needs a server or client TLS config")]
    NoTlsConfig,
    /// The TLS configuration does not enable TLS 1.3, which QUIC requires.
    #[error("QUIC requires TLS 1.3, which the TLS config does not enable")]
    Tls13Required,
    /// The TLS configuration's provider has no TLS 1.3 suite QUIC can use
    /// for its Initial packets (`TLS13_AES_128_GCM_SHA256`).
    #[error("the TLS config has no cipher suite usable for QUIC Initial packets")]
    NoInitialCipherSuite,
    /// rustls rejected the configuration for QUIC. The text is rustls's
    /// message.
    #[error("TLS config rejected for QUIC: {0}")]
    Tls(String),
}

impl QuicConfig {
    /// Start building a configuration.
    pub fn builder() -> QuicConfigBuilder {
        QuicConfigBuilder::default()
    }

    /// A configuration from complete quinn-proto configurations, for what
    /// the builder does not cover (a non-default [`EndpointConfig`], other
    /// crypto, per-side transport settings, and other transport settings
    /// such as keep-alive and datagram buffers). Part of the escape hatches:
    /// its arguments are [`proto`](crate::proto) types, which follow
    /// quinn-proto's version.
    ///
    /// The configuration is used as given. A rustls configuration without
    /// TLS 1.3 inside `server` or `client` cannot be detected here and
    /// panics inside quinn-proto on the first connection.
    pub fn from_proto(
        endpoint: Arc<EndpointConfig>,
        server: Option<Arc<ServerConfig>>,
        client: Option<ClientConfig>,
    ) -> Self {
        Self {
            endpoint_config: endpoint,
            server_config: server,
            client_config: client,
            send_queue_capacity: 4096,
            max_transmit_datagrams: 10,
            allow_mtud: true,
            rng_seed: None,
        }
    }

    /// Set the most outgoing packets (each possibly a GSO buffer) held for
    /// `poll_send`; at the limit the endpoint stops pulling transmits from
    /// quinn-proto and drops stateless responses. Defaults to 4096.
    pub fn send_queue_capacity(mut self, send_queue_capacity: usize) -> Self {
        self.send_queue_capacity = send_queue_capacity;
        self
    }

    /// Set the most datagrams quinn-proto may produce in one
    /// `poll_transmit` call. Defaults to 10, the value quinn's high-level
    /// endpoint uses.
    ///
    /// Higher values amortise quinn-proto's per-call overhead across more
    /// datagrams. When it is more than 1, quinn may pack several datagrams
    /// into one buffer; [`QuicEndpoint::poll_send`](crate::QuicEndpoint::poll_send)
    /// then returns it as one [`OutgoingPacket`](crate::OutgoingPacket) with
    /// `segment_size` set, which a sender without GSO splits with
    /// [`OutgoingPacket::datagrams`](crate::OutgoingPacket::datagrams).
    pub fn max_transmit_datagrams(mut self, max_transmit_datagrams: usize) -> Self {
        self.max_transmit_datagrams = max_transmit_datagrams;
        self
    }

    /// Allow path MTU discovery. Defaults to `true`.
    pub fn allow_mtud(mut self, allow_mtud: bool) -> Self {
        self.allow_mtud = allow_mtud;
        self
    }

    /// Seed quinn-proto's RNG for deterministic tests. Defaults to a random
    /// seed.
    pub fn rng_seed(mut self, rng_seed: [u8; 32]) -> Self {
        self.rng_seed = Some(rng_seed);
        self
    }
}

/// Builder for [`QuicConfig`].
///
/// ```
/// # use ringline_quic::QuicConfig;
/// # use ringline_tls_config::TlsConfig;
/// # let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
/// # let chain = vec![cert.cert.der().clone()];
/// # let key = rustls_pki_types::PrivateKeyDer::Pkcs8(cert.key_pair.serialize_der().into());
/// let tls = TlsConfig::builder().cert(chain, key).build()?;
/// let quic = QuicConfig::builder()
///     .server(tls)
///     .alpn(&[b"h3"])
///     .max_concurrent_bidi_streams(1024)
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Default)]
pub struct QuicConfigBuilder {
    server: Option<TlsConfig>,
    client: Option<TlsClientConfig>,
    alpn: Option<Vec<Vec<u8>>>,
    zero_rtt: Option<bool>,
    max_concurrent_bidi_streams: Option<VarInt>,
    max_concurrent_uni_streams: Option<VarInt>,
    stream_receive_window: Option<VarInt>,
    receive_window: Option<VarInt>,
    max_idle_timeout: Option<Option<Duration>>,
    initial_rtt: Option<Duration>,
    migration: Option<bool>,
}

impl QuicConfigBuilder {
    /// Accept connections with this TLS configuration. A config that also
    /// serves TCP works: QUIC negotiates only TLS 1.3.
    pub fn server(mut self, tls: TlsConfig) -> Self {
        self.server = Some(tls);
        self
    }

    /// Make connections with this TLS configuration. Setting both
    /// [`server`](Self::server) and `client` gives an endpoint that accepts
    /// and connects.
    ///
    /// A `TlsClientConfig` from `TlsClientConfig::builder` gives QUIC
    /// connections a session store separate from TCP's, shared by every
    /// `QuicConfig` built from it. One from `from_rustls` uses its rustls
    /// `resumption` for TCP and QUIC alike; to keep them apart, pass two
    /// rustls configs, each with its own `Resumption`, to two `from_rustls`
    /// calls.
    pub fn client(mut self, tls: TlsClientConfig) -> Self {
        self.client = Some(tls);
        self
    }

    /// The ALPN protocols for QUIC connections, replacing the TLS
    /// configuration's list. HTTP/3 uses `h3` where TCP uses `h2` or
    /// `http/1.1`. Uncalled, QUIC uses the TLS configuration's list.
    pub fn alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = Some(protocols.iter().map(|p| p.to_vec()).collect());
        self
    }

    /// Whether to use 0-RTT. Uncalled, a server accepts 0-RTT, as
    /// quinn-proto's `ServerConfig::with_single_cert` does, and a client
    /// sends none. `true` lets a client send 0-RTT data on a resumed
    /// connection; `false` makes a server refuse it.
    pub fn zero_rtt(mut self, enabled: bool) -> Self {
        self.zero_rtt = Some(enabled);
        self
    }

    /// The most bidirectional streams the peer may have open at once.
    pub fn max_concurrent_bidi_streams(mut self, count: u32) -> Self {
        self.max_concurrent_bidi_streams = Some(VarInt::from_u32(count));
        self
    }

    /// The most unidirectional streams the peer may have open at once.
    pub fn max_concurrent_uni_streams(mut self, count: u32) -> Self {
        self.max_concurrent_uni_streams = Some(VarInt::from_u32(count));
        self
    }

    /// The most data, in bytes, the peer may send on one stream before it is
    /// read. A value of 2^62 or more is treated as 2^62 - 1.
    pub fn stream_receive_window(mut self, bytes: u64) -> Self {
        self.stream_receive_window = Some(VarInt::from_u64(bytes).unwrap_or(VarInt::MAX));
        self
    }

    /// The most data, in bytes, the peer may send across all streams before
    /// it is read. A value of 2^62 or more is treated as 2^62 - 1.
    pub fn receive_window(mut self, bytes: u64) -> Self {
        self.receive_window = Some(VarInt::from_u64(bytes).unwrap_or(VarInt::MAX));
        self
    }

    /// The idle timeout this endpoint advertises, or `None` to advertise
    /// none. A connection closes after the smaller of the two endpoints'
    /// timeouts and has none only when neither endpoint sets one. A duration
    /// under 1 ms, or of 2^62 ms (about 146 million years) or more,
    /// advertises none.
    pub fn max_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.max_idle_timeout = Some(timeout);
        self
    }

    /// The round-trip time assumed before one is measured. Defaults to
    /// quinn-proto's 333 ms.
    pub fn initial_rtt(mut self, rtt: Duration) -> Self {
        self.initial_rtt = Some(rtt);
        self
    }

    /// Whether a server lets clients migrate to a new address. Defaults to
    /// `true`. Has no effect on a client-only configuration.
    pub fn migration(mut self, allowed: bool) -> Self {
        self.migration = Some(allowed);
        self
    }

    /// Build the configuration. The transport settings apply to the server
    /// and the client sides.
    ///
    /// Fails if a TLS configuration does not enable TLS 1.3 or has no
    /// cipher suite QUIC can use. Without the TLS 1.3 check, quinn-proto
    /// accepts a TLS 1.2-only configuration and panics on the first
    /// connection.
    pub fn build(self) -> Result<QuicConfig, ConfigError> {
        if self.server.is_none() && self.client.is_none() {
            return Err(ConfigError::NoTlsConfig);
        }
        let server_config = match &self.server {
            Some(tls) => Some(Arc::new(self.server_config(tls)?)),
            None => None,
        };
        let client_config = match &self.client {
            Some(tls) => Some(self.client_config(tls)?),
            None => None,
        };
        Ok(QuicConfig::from_proto(
            Arc::new(EndpointConfig::default()),
            server_config,
            client_config,
        ))
    }

    fn server_config(&self, tls: &TlsConfig) -> Result<ServerConfig, ConfigError> {
        let mut rustls_config = rustls::ServerConfig::clone(tls.rustls_config());
        if let Some(alpn) = &self.alpn {
            rustls_config.alpn_protocols = alpn.clone();
        }
        // QUIC refuses any non-zero value other than u32::MAX.
        rustls_config.max_early_data_size = if self.zero_rtt.unwrap_or(true) {
            u32::MAX
        } else {
            0
        };
        let rustls_config = Arc::new(rustls_config);
        // quinn-proto's conversion checks only the provider's suites; the
        // per-connection TLS 1.3 check is unwrapped inside quinn-proto.
        // `supports_version` is private in rustls, so a throwaway
        // connection runs the check here.
        rustls::quic::ServerConnection::new(
            rustls_config.clone(),
            rustls::quic::Version::V1,
            Vec::new(),
        )
        .map_err(tls_error)?;
        let crypto = quinn_proto::crypto::rustls::QuicServerConfig::try_from(rustls_config)
            .map_err(|_| ConfigError::NoInitialCipherSuite)?;
        let mut config = ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(Arc::new(self.transport()));
        if let Some(migration) = self.migration {
            config.migration(migration);
        }
        Ok(config)
    }

    fn client_config(&self, tls: &TlsClientConfig) -> Result<ClientConfig, ConfigError> {
        let mut rustls_config = rustls::ClientConfig::clone(tls.rustls_config());
        if let Some(alpn) = &self.alpn {
            rustls_config.alpn_protocols = alpn.clone();
        }
        rustls_config.enable_early_data = self.zero_rtt.unwrap_or(false);
        if let Some(resumption) = tls.quic_resumption() {
            rustls_config.resumption = resumption.clone();
        }
        let rustls_config = Arc::new(rustls_config);
        // A placeholder name: the constructor starts the handshake and would
        // take a stored ticket for a real one.
        let placeholder =
            rustls::pki_types::ServerName::try_from("example.invalid").expect("a valid DNS name");
        rustls::quic::ClientConnection::new(
            rustls_config.clone(),
            rustls::quic::Version::V1,
            placeholder,
            Vec::new(),
        )
        .map_err(tls_error)?;
        let crypto = quinn_proto::crypto::rustls::QuicClientConfig::try_from(rustls_config)
            .map_err(|_| ConfigError::NoInitialCipherSuite)?;
        let mut config = ClientConfig::new(Arc::new(crypto));
        config.transport_config(Arc::new(self.transport()));
        Ok(config)
    }

    /// A transport configuration with the builder's settings. Built once per
    /// side, because quinn-proto's `TransportConfig` is not `Clone`.
    fn transport(&self) -> TransportConfig {
        let mut t = TransportConfig::default();
        if let Some(n) = self.max_concurrent_bidi_streams {
            t.max_concurrent_bidi_streams(n.to_proto());
        }
        if let Some(n) = self.max_concurrent_uni_streams {
            t.max_concurrent_uni_streams(n.to_proto());
        }
        if let Some(n) = self.stream_receive_window {
            t.stream_receive_window(n.to_proto());
        }
        if let Some(n) = self.receive_window {
            t.receive_window(n.to_proto());
        }
        if let Some(timeout) = self.max_idle_timeout {
            t.max_idle_timeout(timeout.and_then(|d| d.try_into().ok()));
        }
        if let Some(rtt) = self.initial_rtt {
            t.initial_rtt(rtt);
        }
        t
    }
}

fn tls_error(e: rustls::Error) -> ConfigError {
    match e {
        rustls::Error::General(msg) if msg.contains("TLS 1.3") => ConfigError::Tls13Required,
        other => ConfigError::Tls(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ringline_tls_config::TlsVersion;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    fn cert() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        (vec![CertificateDer::from(cert.cert)], key.into())
    }

    fn server_tls(versions: &[TlsVersion]) -> TlsConfig {
        let (chain, key) = cert();
        TlsConfig::builder()
            .cert(chain, key)
            .versions(versions)
            .build()
            .unwrap()
    }

    fn client_tls(versions: &[TlsVersion]) -> TlsClientConfig {
        TlsClientConfig::builder()
            .versions(versions)
            .build()
            .unwrap()
    }

    /// The builder sets one side or both; the ringline-quic settings keep
    /// their defaults and each setter writes its own field.
    #[test]
    fn builder_sets_sides_and_each_setter_its_field() {
        let s = QuicConfig::builder()
            .server(server_tls(&[TlsVersion::Tls13]))
            .build()
            .unwrap();
        assert!(s.server_config.is_some());
        assert!(s.client_config.is_none());
        let c = QuicConfig::builder()
            .client(client_tls(&[TlsVersion::Tls13]))
            .build()
            .unwrap();
        assert!(c.server_config.is_none());
        assert!(c.client_config.is_some());
        for d in [&s, &c] {
            assert_eq!(d.send_queue_capacity, 4096);
            assert_eq!(d.max_transmit_datagrams, 10);
            assert!(d.allow_mtud);
            assert_eq!(d.rng_seed, None);
        }

        let both = QuicConfig::builder()
            .server(server_tls(&[TlsVersion::Tls13]))
            .client(client_tls(&[TlsVersion::Tls13]))
            .build()
            .unwrap()
            .send_queue_capacity(7)
            .max_transmit_datagrams(3)
            .allow_mtud(false)
            .rng_seed([9; 32]);
        assert!(both.server_config.is_some());
        assert!(both.client_config.is_some());
        assert_eq!(both.send_queue_capacity, 7);
        assert_eq!(both.max_transmit_datagrams, 3);
        assert!(!both.allow_mtud);
        assert_eq!(both.rng_seed, Some([9; 32]));

        assert!(matches!(
            QuicConfig::builder().build().err().unwrap(),
            ConfigError::NoTlsConfig
        ));
    }

    /// A TLS 1.2-only config is refused at build time. quinn-proto's own
    /// conversion accepts it and panics on the first connection.
    #[test]
    fn tls12_only_is_refused() {
        let err = QuicConfig::builder()
            .server(server_tls(&[TlsVersion::Tls12]))
            .build()
            .err()
            .unwrap();
        assert!(matches!(err, ConfigError::Tls13Required), "{err}");
        let err = QuicConfig::builder()
            .client(client_tls(&[TlsVersion::Tls12]))
            .build()
            .err()
            .unwrap();
        assert!(matches!(err, ConfigError::Tls13Required), "{err}");
    }

    /// The TCP default, TLS 1.2 and 1.3, builds for QUIC.
    #[test]
    fn the_tcp_default_builds() {
        QuicConfig::builder()
            .server(server_tls(&[TlsVersion::Tls13, TlsVersion::Tls12]))
            .client(client_tls(&[TlsVersion::Tls13, TlsVersion::Tls12]))
            .build()
            .unwrap();
    }

    /// A provider whose only TLS 1.3 suite is ChaCha20 passes rustls's QUIC
    /// check and fails quinn-proto's Initial suite lookup.
    #[test]
    fn no_initial_suite_is_refused() {
        let mut provider = rustls::crypto::ring::default_provider();
        provider
            .cipher_suites
            .retain(|s| s.suite() == rustls::CipherSuite::TLS13_CHACHA20_POLY1305_SHA256);
        let (chain, key) = cert();
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        let err = QuicConfig::builder()
            .server(TlsConfig::from_rustls(Arc::new(config)))
            .build()
            .err()
            .unwrap();
        assert!(matches!(err, ConfigError::NoInitialCipherSuite), "{err}");
    }

    /// Windows of 2^62 bytes or more saturate at the largest QUIC integer.
    #[test]
    fn windows_saturate() {
        let b = QuicConfig::builder()
            .stream_receive_window(u64::MAX)
            .receive_window(1 << 20);
        assert_eq!(b.stream_receive_window, Some(VarInt::MAX));
        assert_eq!(b.receive_window, Some(VarInt::from_u32(1 << 20)));
    }

    /// QUIC gets its own ALPN and early-data settings on a copy; the TCP
    /// config is unchanged.
    #[test]
    fn quic_settings_go_on_a_copy() {
        let tls = server_tls(&[TlsVersion::Tls13]);
        QuicConfig::builder()
            .server(tls.clone())
            .alpn(&[b"h3"])
            .build()
            .unwrap();
        assert!(tls.rustls_config().alpn_protocols.is_empty());
        assert_eq!(tls.rustls_config().max_early_data_size, 0);
    }
}
