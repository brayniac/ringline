/// Default cap on a single gRPC message size. Mirrors the gRPC standard
/// `grpc.max_receive_message_length` of 4 MiB. The length prefix is u32
/// (up to 4 GiB) so without a cap a misbehaving peer can request
/// arbitrarily large allocations by sending a fake length header.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Error returned when [`encode`] / [`encode_compressed`] receive a payload
/// that wouldn't fit in gRPC's 32-bit length prefix.
#[derive(Debug, PartialEq, Eq)]
pub struct EncodeTooLarge {
    pub len: usize,
}

impl std::fmt::Display for EncodeTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "gRPC message payload {} exceeds u32::MAX (4 GiB - 1)",
            self.len,
        )
    }
}

impl std::error::Error for EncodeTooLarge {}

/// The 32-bit length prefix for a payload of `len` bytes, or [`EncodeTooLarge`]
/// if it does not fit.
///
/// Split out from [`encode`] so the bound can be tested as the arithmetic it
/// is. The previous test allocated a 5 GiB buffer and then a 3 GiB one to
/// reach it, which made the workspace suite unrunnable under ~10 GiB of RAM
/// and never examined a single one of those bytes.
fn length_prefix(len: usize) -> Result<u32, EncodeTooLarge> {
    u32::try_from(len).map_err(|_| EncodeTooLarge { len })
}

/// Encode a gRPC length-prefixed message (uncompressed).
///
/// Format: 1 byte compress flag (0 = uncompressed) + 4 byte big-endian length + payload.
/// Returns `Err(EncodeTooLarge)` if the payload exceeds `u32::MAX` bytes —
/// without this guard the `as u32` cast truncates and produces a frame
/// with a wrong-length prefix.
pub fn encode(payload: &[u8], out: &mut Vec<u8>) -> Result<(), EncodeTooLarge> {
    let len = length_prefix(payload.len())?;
    out.push(0); // compress flag: uncompressed
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    Ok(())
}

/// Encode a gRPC length-prefixed message with compression.
///
/// The payload should already be compressed. Sets the compress flag to 1.
pub fn encode_compressed(
    compressed_payload: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), EncodeTooLarge> {
    let len = length_prefix(compressed_payload.len())?;
    out.push(1); // compress flag: compressed
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(compressed_payload);
    Ok(())
}

/// Result of attempting to decode a gRPC length-prefixed message from a buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodeResult {
    /// A complete message was decoded. Contains the payload, compress flag, and bytes consumed.
    Complete {
        payload: Vec<u8>,
        compressed: bool,
        consumed: usize,
    },
    /// Not enough data yet; need at least this many more bytes.
    Incomplete(usize),
    /// The length prefix is larger than `max_message_size`. Caller must
    /// fail the stream — there's no way to recover framing once we've
    /// decided to skip a length that big.
    TooLarge(usize),
}

/// Try to decode one gRPC length-prefixed message from the front of `buf`,
/// bounded at `max_size` bytes.
pub fn decode(buf: &[u8], max_size: usize) -> DecodeResult {
    if buf.len() < 5 {
        return DecodeResult::Incomplete(5 - buf.len());
    }

    let compressed = buf[0] != 0;
    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if length > max_size {
        return DecodeResult::TooLarge(length);
    }
    let total = 5 + length;

    if buf.len() < total {
        return DecodeResult::Incomplete(total - buf.len());
    }

    DecodeResult::Complete {
        payload: buf[5..total].to_vec(),
        compressed,
        consumed: total,
    }
}

/// Per-stream buffer for reassembling gRPC messages from DATA frame chunks.
///
/// The buffer is bounded — `push` returns an error once the accumulated
/// bytes would exceed the configured `max_message_size + 5` (header), so a
/// peer dribbling garbage into a never-decodable frame can't OOM us.
#[derive(Debug)]
pub struct MessageBuffer {
    buf: Vec<u8>,
    max_message_size: usize,
}

impl Default for MessageBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MESSAGE_SIZE)
    }
}

/// Returned by `MessageBuffer::try_decode` for callers that need to
/// distinguish a malformed frame (TooLarge) from a clean wait-for-more.
#[derive(Debug, PartialEq, Eq)]
pub enum BufferDecode {
    /// One complete message: `(payload, compressed_flag)`.
    Complete(Vec<u8>, bool),
    /// Not enough bytes for a complete message yet.
    Incomplete,
    /// The next message's length prefix exceeds `max_message_size`. The
    /// caller must fail the stream.
    TooLarge(usize),
}

impl MessageBuffer {
    pub fn new(max_message_size: usize) -> Self {
        Self {
            buf: Vec::new(),
            max_message_size,
        }
    }

    /// Append data from a DATA frame. Returns an error if accumulating
    /// would exceed `max_message_size + 5` (the header).
    pub fn push(&mut self, data: &[u8]) -> Result<(), crate::error::GrpcError> {
        if self.buf.len().saturating_add(data.len()) > self.max_message_size.saturating_add(5) {
            return Err(crate::error::GrpcError::MaxSizeExceeded(format!(
                "message reassembly buffer exceeds {} bytes",
                self.max_message_size + 5
            )));
        }
        self.buf.extend_from_slice(data);
        Ok(())
    }

    /// Try to drain one complete message.
    pub fn try_decode(&mut self) -> BufferDecode {
        match decode(&self.buf, self.max_message_size) {
            DecodeResult::Complete {
                payload,
                compressed,
                consumed,
            } => {
                self.buf.drain(..consumed);
                BufferDecode::Complete(payload, compressed)
            }
            DecodeResult::Incomplete(_) => BufferDecode::Incomplete,
            DecodeResult::TooLarge(n) => BufferDecode::TooLarge(n),
        }
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let payload = b"hello grpc";
        let mut buf = Vec::new();
        encode(payload, &mut buf).unwrap();

        assert_eq!(buf.len(), 5 + payload.len());
        assert_eq!(buf[0], 0); // no compression
        assert_eq!(
            u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]),
            payload.len() as u32
        );

        match decode(&buf, usize::MAX) {
            DecodeResult::Complete {
                payload: decoded,
                compressed,
                consumed,
            } => {
                assert_eq!(decoded, payload);
                assert!(!compressed);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn decode_incomplete_header() {
        assert_eq!(decode(&[], usize::MAX), DecodeResult::Incomplete(5));
        assert_eq!(decode(&[0, 0], usize::MAX), DecodeResult::Incomplete(3));
        assert_eq!(
            decode(&[0, 0, 0, 0], usize::MAX),
            DecodeResult::Incomplete(1)
        );
    }

    #[test]
    fn decode_incomplete_payload() {
        let mut buf = Vec::new();
        encode(b"hello", &mut buf).unwrap();
        // Truncate to just the header + 2 bytes of payload.
        buf.truncate(7);
        assert_eq!(decode(&buf, usize::MAX), DecodeResult::Incomplete(3));
    }

    #[test]
    fn encode_empty_message() {
        let mut buf = Vec::new();
        encode(b"", &mut buf).unwrap();
        assert_eq!(buf, &[0, 0, 0, 0, 0]);
        match decode(&buf, usize::MAX) {
            DecodeResult::Complete {
                payload, consumed, ..
            } => {
                assert!(payload.is_empty());
                assert_eq!(consumed, 5);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_oversize_length() {
        // 1 MiB claimed but cap is 1024.
        let header = [0u8, 0x00, 0x10, 0x00, 0x00]; // length = 0x100000 = 1 MiB
        assert!(matches!(decode(&header, 1024), DecodeResult::TooLarge(_)));
    }

    #[test]
    fn decode_u32_max_rejected() {
        // The pathological case the audit flagged.
        let mut buf = vec![0u8];
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        match decode(&buf, DEFAULT_MAX_MESSAGE_SIZE) {
            DecodeResult::TooLarge(_) => {}
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    /// The u32 length-prefix bound, tested at its edges rather than by
    /// allocating past it.
    ///
    /// This replaces a test that built a 5 GiB `Vec` and then a 3 GiB one to
    /// assert the same thing. Besides costing 8 GiB and the time to zero it,
    /// that test aborted on any machine with less RAM — and on a machine with
    /// slightly more it thrashed instead, which presents as a hang with no
    /// diagnostic rather than as a failure. Neither told you anything the
    /// arithmetic doesn't.
    #[test]
    fn length_prefix_rejects_payloads_past_u32_max() {
        assert_eq!(length_prefix(0), Ok(0));
        assert_eq!(length_prefix(4 * 1024 * 1024), Ok(4 * 1024 * 1024));
        // The largest payload that fits, and the first that does not.
        assert_eq!(length_prefix(u32::MAX as usize), Ok(u32::MAX));
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(
                length_prefix(u32::MAX as usize + 1),
                Err(EncodeTooLarge {
                    len: u32::MAX as usize + 1
                })
            );
            assert!(length_prefix(5 * 1024 * 1024 * 1024).is_err());
        }
    }

    /// The bound reaches `encode` and `encode_compressed`, not just the
    /// helper — at a size that costs nothing to allocate.
    #[test]
    fn encode_writes_the_length_prefix_it_computed() {
        let payload = vec![7u8; 1024];
        let mut out = Vec::new();
        encode(&payload, &mut out).expect("1 KiB encodes");
        assert_eq!(out[0], 0, "uncompressed flag");
        assert_eq!(&out[1..5], &1024u32.to_be_bytes(), "big-endian length");
        assert_eq!(&out[5..], &payload[..]);

        let mut out = Vec::new();
        encode_compressed(&payload, &mut out).expect("1 KiB encodes");
        assert_eq!(out[0], 1, "compressed flag");
        assert_eq!(&out[1..5], &1024u32.to_be_bytes());
    }

    #[test]
    fn message_buffer_reassembly() {
        let payload = b"reassembled message";
        let mut encoded = Vec::new();
        encode(payload, &mut encoded).unwrap();

        let mut mb = MessageBuffer::default();
        assert!(mb.is_empty());

        // Feed in chunks.
        mb.push(&encoded[..3]).unwrap();
        assert_eq!(mb.try_decode(), BufferDecode::Incomplete);

        mb.push(&encoded[3..8]).unwrap();
        assert_eq!(mb.try_decode(), BufferDecode::Incomplete);

        mb.push(&encoded[8..]).unwrap();
        match mb.try_decode() {
            BufferDecode::Complete(decoded, compressed) => {
                assert_eq!(decoded, payload);
                assert!(!compressed);
            }
            other => panic!("expected Complete, got {other:?}"),
        }
        assert!(mb.is_empty());
    }

    #[test]
    fn message_buffer_multiple_messages() {
        let mut encoded = Vec::new();
        encode(b"first", &mut encoded).unwrap();
        encode(b"second", &mut encoded).unwrap();

        let mut mb = MessageBuffer::default();
        mb.push(&encoded).unwrap();

        assert_eq!(
            mb.try_decode(),
            BufferDecode::Complete(b"first".to_vec(), false)
        );
        assert_eq!(
            mb.try_decode(),
            BufferDecode::Complete(b"second".to_vec(), false)
        );
        assert_eq!(mb.try_decode(), BufferDecode::Incomplete);
        assert!(mb.is_empty());
    }

    #[test]
    fn message_buffer_push_capped() {
        // Tiny cap; first push fits, second push overflows.
        let mut mb = MessageBuffer::new(10);
        mb.push(&[0u8; 15]).unwrap(); // 15 <= 10 + 5 header room
        let err = mb.push(&[0u8; 1]).err().unwrap();
        assert!(matches!(err, crate::error::GrpcError::MaxSizeExceeded(_)));
    }

    #[test]
    fn message_buffer_try_decode_too_large() {
        // Push a frame whose length prefix claims more than the cap.
        let mut mb = MessageBuffer::new(100);
        // Header: compressed=0, length=200 (over cap).
        mb.push(&[0, 0, 0, 0, 200]).unwrap();
        assert!(matches!(mb.try_decode(), BufferDecode::TooLarge(200)));
    }
}
