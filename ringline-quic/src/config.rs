use std::sync::Arc;

use quinn_proto::{ClientConfig, EndpointConfig, ServerConfig};

/// Configuration for a [`QuicEndpoint`](crate::QuicEndpoint).
///
/// `Clone` lets the same configuration drive multiple per-worker endpoints
/// (e.g. one `QuicEndpoint` per ringline worker, each bound to a different
/// port via `SO_REUSEPORT`). All inner state (`endpoint_config`,
/// `server_config`, `client_config`) is `Arc`-backed, so cloning is cheap.
///
/// Build it with [`server`](Self::server) or [`client`](Self::client) and
/// the builder methods.
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

impl QuicConfig {
    /// Create a server-only config with the given TLS server configuration.
    pub fn server(server_config: Arc<ServerConfig>) -> Self {
        Self {
            endpoint_config: Arc::new(EndpointConfig::default()),
            server_config: Some(server_config),
            client_config: None,
            send_queue_capacity: 4096,
            max_transmit_datagrams: 10,
            allow_mtud: true,
            rng_seed: None,
        }
    }

    /// Create a client-only config with the given TLS client configuration.
    pub fn client(client_config: ClientConfig) -> Self {
        Self {
            endpoint_config: Arc::new(EndpointConfig::default()),
            server_config: None,
            client_config: Some(client_config),
            send_queue_capacity: 4096,
            max_transmit_datagrams: 10,
            allow_mtud: true,
            rng_seed: None,
        }
    }

    /// Set the shared endpoint configuration (connection IDs, supported
    /// versions). Defaults to `EndpointConfig::default()`.
    pub fn endpoint_config(mut self, endpoint_config: Arc<EndpointConfig>) -> Self {
        self.endpoint_config = endpoint_config;
        self
    }

    /// Set the server-side TLS/QUIC config, so the endpoint accepts
    /// connections.
    pub fn server_config(mut self, server_config: Arc<ServerConfig>) -> Self {
        self.server_config = Some(server_config);
        self
    }

    /// Set the client-side TLS/QUIC config, so the endpoint can connect.
    pub fn client_config(mut self, client_config: ClientConfig) -> Self {
        self.client_config = Some(client_config);
        self
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
    /// crate uses.
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    fn server() -> Arc<ServerConfig> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
        let certs = vec![CertificateDer::from(cert.cert)];
        Arc::new(ServerConfig::with_single_cert(certs, PrivateKeyDer::from(key)).unwrap())
    }

    fn client() -> ClientConfig {
        let rustls_cfg = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        ClientConfig::new(Arc::new(
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(rustls_cfg).unwrap(),
        ))
    }

    /// The constructors set one side and the defaults; every setter writes
    /// its own field, and an endpoint can take both configs.
    #[test]
    fn constructors_set_the_defaults_and_each_setter_its_field() {
        let sc = server();
        let s = QuicConfig::server(sc.clone());
        assert!(
            s.server_config
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, &sc))
        );
        assert!(s.client_config.is_none());
        let c = QuicConfig::client(client());
        assert!(c.server_config.is_none());
        assert!(c.client_config.is_some());
        for d in [&s, &c] {
            assert_eq!(d.send_queue_capacity, 4096);
            assert_eq!(d.max_transmit_datagrams, 10);
            assert!(d.allow_mtud);
            assert_eq!(d.rng_seed, None);
        }

        let ec = Arc::new(EndpointConfig::default());
        let sc2 = server();
        let both = QuicConfig::client(client())
            .endpoint_config(ec.clone())
            .server_config(sc2.clone())
            .send_queue_capacity(7)
            .max_transmit_datagrams(3)
            .allow_mtud(false)
            .rng_seed([9; 32]);
        assert!(Arc::ptr_eq(&both.endpoint_config, &ec));
        assert!(
            both.server_config
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, &sc2))
        );
        assert!(both.client_config.is_some());
        assert_eq!(both.send_queue_capacity, 7);
        assert_eq!(both.max_transmit_datagrams, 3);
        assert!(!both.allow_mtud);
        assert_eq!(both.rng_seed, Some([9; 32]));
        let s_then_c = QuicConfig::server(server()).client_config(client());
        assert!(s_then_c.client_config.is_some());
        assert!(s_then_c.server_config.is_some());
    }
}
