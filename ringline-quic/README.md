# ringline-quic

**QUIC protocol support via quinn-proto.**

Wraps [quinn-proto](https://docs.rs/quinn-proto)'s sans-IO QUIC state machine
and exposes an event-based [`QuicEndpoint`] API for use with ringline's UDP
support.

## Quick Start

```rust,ignore
use ringline_quic::{QuicConfig, QuicEndpoint, QuicEvent, TlsConfig};

let tls = TlsConfig::builder().cert(chain, key).build()?;
let config = QuicConfig::builder()
    .server(tls)
    .alpn(&[b"h3"])
    .max_concurrent_bidi_streams(100u32)
    .build()?;
let mut quic = QuicEndpoint::new(config, local_addr);

// Read the clock once per event-loop iteration and pass it to every call.
let now = Instant::now();
// Feed incoming datagrams:
quic.handle_datagram(now, data, peer);
while let Some(event) = quic.poll_event() {
    match event {
        QuicEvent::NewConnection(conn) => { /* ... */ }
        QuicEvent::StreamReadable { conn, stream } => {
            let mut buf = [0u8; 4096];
            let (n, fin) = quic.stream_recv(now, conn, stream, &mut buf)?;
            quic.stream_send(conn, stream, &buf[..n])?;
        }
        _ => {}
    }
}
// Drain outgoing packets:
while let Some(pkt) = quic.poll_send() {
    for datagram in pkt.datagrams() {
        udp.send_to(pkt.destination, datagram)?;
    }
}
```

## Configuration

`QuicConfig::builder()` takes the same `TlsConfig` and `TlsClientConfig`
that ringline uses for TCP, so one TLS configuration serves both. QUIC
negotiates only TLS 1.3, and `build()` refuses a TLS configuration without
it. `alpn` and `zero_rtt` set QUIC's ALPN list and 0-RTT on a copy of the
TLS configuration, leaving the TCP one unchanged.

`QuicConfig::from_proto` takes complete quinn-proto configurations, reached
through `ringline_quic::proto`, for what the builder does not cover. Those
types follow quinn-proto's version.

## Features

- Event-driven API wrapping quinn-proto's sans-IO state machine
- Stream IDs, errors, events and statistics are ringline-quic types, so the
  API does not change when the quinn-proto version underneath does
- Connection and stream management
- Integrates with ringline's UDP support (`UdpCtx`)
