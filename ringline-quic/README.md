# ringline-quic

**QUIC protocol support via quinn-proto.**

Wraps [quinn-proto](https://docs.rs/quinn-proto)'s sans-IO QUIC state machine
and exposes an event-based [`QuicEndpoint`] API for use with ringline's UDP
support.

## Quick Start

```rust,ignore
use ringline_quic::{QuicConfig, QuicEndpoint, QuicEvent};

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
while let Some((dest, data)) = quic.poll_send() {
    udp.send_to(dest, &data)?;
}
```

## Features

- Event-driven API wrapping quinn-proto's sans-IO state machine
- Connection and stream management
- Integrates with ringline's UDP support (`UdpCtx`)
