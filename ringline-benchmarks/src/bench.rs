use std::fmt;
use std::time::Duration;

/// Which transport layer to use for the benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Transport {
    /// TCP
    #[default]
    Tcp,
    /// UDP
    Udp,
    /// QUIC
    Quic,
}

/// Which protocol layer to benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Protocol {
    /// Echo (send data, receive same data back)
    #[default]
    Echo,
    /// Request-response (send request, receive response)
    RequestResponse,
    /// Streaming (send data, receive data back)
    Streaming,
}

/// Whether a benchmark cell runs over TLS.
///
/// Consumed by [`crate::protocols::tls::run_tls_echo`], which selects a
/// TLS-terminating ringline server + a tokio/rustls client for
/// [`TlsConfig::Required`] and their plaintext equivalents for
/// [`TlsConfig::None`]. A definition built with [`BenchmarkDefinition::with_tls`]
/// yields both, so the plaintext cell acts as a control against which the TLS
/// cell's cost is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TlsConfig {
    #[default]
    None,
    Required,
}

/// Client runtime to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ClientRuntime {
    /// ringline client
    #[default]
    Ringline,
    /// tokio client
    Tokio,
}

/// Server runtime to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ServerRuntime {
    /// ringline server
    #[default]
    Ringline,
    /// tokio server
    Tokio,
}

/// A benchmark definition that composes all parameters.
#[derive(Clone, Debug)]
pub struct BenchmarkDefinition {
    pub transport: Transport,
    pub protocol: Protocol,
    pub client_runtime: ClientRuntime,
    pub server_runtime: ServerRuntime,
    pub sizes: Vec<usize>,
    pub concurrencies: Vec<usize>,
    pub tls: TlsConfig,
    pub duration: Duration,
    pub warmup: Duration,
}

impl BenchmarkDefinition {
    pub fn new() -> Self {
        Self {
            transport: Transport::Tcp,
            protocol: Protocol::Echo,
            client_runtime: ClientRuntime::Ringline,
            server_runtime: ServerRuntime::Ringline,
            sizes: vec![64, 512, 4096, 32768],
            concurrencies: vec![1, 10, 50, 200],
            tls: TlsConfig::None,
            duration: Duration::from_secs(5),
            warmup: Duration::from_secs(2),
        }
    }

    /// Add a message size to benchmark.
    pub fn with_size(mut self, size: usize) -> Self {
        self.sizes.push(size);
        self
    }

    /// Add a concurrency level to benchmark.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrencies.push(concurrency);
        self
    }

    /// Replace the message-size sweep.
    ///
    /// Distinct from [`with_size`](Self::with_size), which *appends* to the
    /// defaults — a CLI-supplied `--sizes` must replace them, or every run
    /// silently also sweeps `64,512,4096,32768`.
    pub fn with_sizes(mut self, sizes: Vec<usize>) -> Self {
        self.sizes = sizes;
        self
    }

    /// Replace the concurrency sweep. See [`with_sizes`](Self::with_sizes).
    pub fn with_concurrencies(mut self, concurrencies: Vec<usize>) -> Self {
        self.concurrencies = concurrencies;
        self
    }

    /// Enable TLS.
    pub fn with_tls(mut self) -> Self {
        self.tls = TlsConfig::Required;
        self
    }

    /// Use tokio client.
    pub fn with_tokio_client(mut self) -> Self {
        self.client_runtime = ClientRuntime::Tokio;
        self
    }

    /// Use tokio server.
    pub fn with_tokio_server(mut self) -> Self {
        self.server_runtime = ServerRuntime::Tokio;
        self
    }

    /// Use ringline client.
    pub fn with_ringline_client(mut self) -> Self {
        self.client_runtime = ClientRuntime::Ringline;
        self
    }

    /// Use ringline server.
    pub fn with_ringline_server(mut self) -> Self {
        self.server_runtime = ServerRuntime::Ringline;
        self
    }

    /// Set the transport layer.
    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = transport;
        self
    }

    /// Set the protocol.
    pub fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the duration and warmup.
    pub fn with_timing(mut self, warmup: Duration, duration: Duration) -> Self {
        self.warmup = warmup;
        self.duration = duration;
        self
    }

    /// Generate all benchmark combinations from this definition.
    pub fn combinations(&self) -> Vec<BenchmarkCombination> {
        let mut result = Vec::new();
        for &size in &self.sizes {
            for &concurrency in &self.concurrencies {
                for tls in [TlsConfig::None, TlsConfig::Required] {
                    // Skip TLS if not required
                    if tls == TlsConfig::Required && self.tls == TlsConfig::None {
                        continue;
                    }
                    result.push(BenchmarkCombination {
                        size,
                        concurrency,
                        tls,
                    });
                }
            }
        }
        result
    }
}

impl Default for BenchmarkDefinition {
    fn default() -> Self {
        Self::new()
    }
}

/// A single benchmark combination (size + concurrency + TLS).
#[derive(Clone, Copy, Debug)]
pub struct BenchmarkCombination {
    pub size: usize,
    pub concurrency: usize,
    pub tls: TlsConfig,
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transport::Tcp => write!(f, "tcp"),
            Transport::Udp => write!(f, "udp"),
            Transport::Quic => write!(f, "quic"),
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Protocol::Echo => write!(f, "echo"),
            Protocol::RequestResponse => write!(f, "request-response"),
            Protocol::Streaming => write!(f, "streaming"),
        }
    }
}

impl fmt::Display for ClientRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientRuntime::Ringline => write!(f, "ringline"),
            ClientRuntime::Tokio => write!(f, "tokio"),
        }
    }
}

impl fmt::Display for ServerRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerRuntime::Ringline => write!(f, "ringline"),
            ServerRuntime::Tokio => write!(f, "tokio"),
        }
    }
}

impl fmt::Display for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TlsConfig::None => write!(f, "none"),
            TlsConfig::Required => write!(f, "tls"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_tls_only_plaintext_cells() {
        let def = BenchmarkDefinition::new()
            .with_sizes(vec![64, 1024])
            .with_concurrencies(vec![4]);
        let combos = def.combinations();
        assert_eq!(combos.len(), 2);
        assert!(combos.iter().all(|c| c.tls == TlsConfig::None));
    }

    /// `.with_tls()` must yield the TLS cell *and* keep the plaintext one: the
    /// plaintext row is the control the TLS row is read against.
    #[test]
    fn with_tls_yields_control_and_tls_cells() {
        let def = BenchmarkDefinition::new()
            .with_sizes(vec![64, 1024])
            .with_concurrencies(vec![4])
            .with_tls();
        let combos = def.combinations();
        assert_eq!(combos.len(), 4);
        assert_eq!(
            combos
                .iter()
                .filter(|c| c.tls == TlsConfig::Required)
                .count(),
            2
        );
        assert_eq!(
            combos.iter().filter(|c| c.tls == TlsConfig::None).count(),
            2
        );
    }

    /// `with_sizes` replaces; `with_size` appends. Mixing them up silently
    /// sweeps the defaults too, which is why both exist explicitly.
    #[test]
    fn with_sizes_replaces_defaults() {
        let def = BenchmarkDefinition::new().with_sizes(vec![7]);
        assert_eq!(def.sizes, vec![7]);
        let def = BenchmarkDefinition::new().with_size(7);
        assert_eq!(def.sizes.last(), Some(&7));
        assert!(def.sizes.len() > 1);
    }
}
