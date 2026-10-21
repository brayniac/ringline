//! HTTP/2 SETTINGS parameters (RFC 7540 Section 6.5.1).

use crate::error::H2Error;

// Settings identifiers.
const SETTINGS_HEADER_TABLE_SIZE: u16 = 0x1;
const SETTINGS_ENABLE_PUSH: u16 = 0x2;
const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 0x3;
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 0x4;
const SETTINGS_MAX_FRAME_SIZE: u16 = 0x5;
const SETTINGS_MAX_HEADER_LIST_SIZE: u16 = 0x6;

/// Hard ceiling on `header_table_size` we accept from a peer's SETTINGS,
/// even if the peer advertises a larger value. The HPACK dynamic table
/// uses ~32 bytes of accounting per entry plus the entry bytes themselves,
/// so unbounded growth here is a DoS vector. 64 KiB lets `4096`-sized
/// tables (the default) keep working while bounding worst case.
pub const MAX_PEER_HEADER_TABLE_SIZE: u32 = 65536;

/// Default value for `max_header_list_size` (RFC 7540 §6.5.2 leaves this
/// unbounded by default, but advertising a finite value is the only way to
/// stop a peer from amplifying small encoded frames into multi-megabyte
/// decoded header lists). 256 KiB matches the H3 default in ringline-h3.
pub const DEFAULT_MAX_HEADER_LIST_SIZE: u32 = 262_144;

/// HTTP/2 SETTINGS parameters.
///
/// Read each value with its getter and set it with the `with_` method of
/// the same name.
#[derive(Debug, Clone)]
pub struct Settings {
    /// SETTINGS_HEADER_TABLE_SIZE (0x1). Default 4096.
    pub(crate) header_table_size: u32,
    /// SETTINGS_ENABLE_PUSH (0x2). Default enabled.
    pub(crate) enable_push: bool,
    /// SETTINGS_MAX_CONCURRENT_STREAMS (0x3). Default unlimited.
    pub(crate) max_concurrent_streams: Option<u32>,
    /// SETTINGS_INITIAL_WINDOW_SIZE (0x4). Default 65535.
    pub(crate) initial_window_size: u32,
    /// SETTINGS_MAX_FRAME_SIZE (0x5). Default 16384.
    pub(crate) max_frame_size: u32,
    /// SETTINGS_MAX_HEADER_LIST_SIZE (0x6); `None` is unlimited. Default
    /// `Some(DEFAULT_MAX_HEADER_LIST_SIZE)`.
    pub(crate) max_header_list_size: Option<u32>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            header_table_size: 4096,
            enable_push: true,
            max_concurrent_streams: None,
            initial_window_size: 65535,
            max_frame_size: 16384,
            max_header_list_size: Some(DEFAULT_MAX_HEADER_LIST_SIZE),
        }
    }
}

impl Settings {
    /// SETTINGS_HEADER_TABLE_SIZE (0x1). Default 4096.
    pub fn header_table_size(&self) -> u32 {
        self.header_table_size
    }

    /// Set SETTINGS_HEADER_TABLE_SIZE (0x1). Default 4096.
    pub fn with_header_table_size(mut self, header_table_size: u32) -> Self {
        self.header_table_size = header_table_size;
        self
    }

    /// SETTINGS_ENABLE_PUSH (0x2). Default enabled.
    pub fn enable_push(&self) -> bool {
        self.enable_push
    }

    /// Set SETTINGS_ENABLE_PUSH (0x2). Default enabled.
    pub fn with_enable_push(mut self, enable_push: bool) -> Self {
        self.enable_push = enable_push;
        self
    }

    /// SETTINGS_MAX_CONCURRENT_STREAMS (0x3); `None` is unlimited, the default.
    pub fn max_concurrent_streams(&self) -> Option<u32> {
        self.max_concurrent_streams
    }

    /// Set SETTINGS_MAX_CONCURRENT_STREAMS (0x3); `None` is unlimited, the default.
    pub fn with_max_concurrent_streams(mut self, max_concurrent_streams: Option<u32>) -> Self {
        self.max_concurrent_streams = max_concurrent_streams;
        self
    }

    /// SETTINGS_INITIAL_WINDOW_SIZE (0x4). Default 65535.
    pub fn initial_window_size(&self) -> u32 {
        self.initial_window_size
    }

    /// Set SETTINGS_INITIAL_WINDOW_SIZE (0x4). Default 65535.
    pub fn with_initial_window_size(mut self, initial_window_size: u32) -> Self {
        self.initial_window_size = initial_window_size;
        self
    }

    /// SETTINGS_MAX_FRAME_SIZE (0x5). Default 16384.
    pub fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Set SETTINGS_MAX_FRAME_SIZE (0x5). Default 16384.
    pub fn with_max_frame_size(mut self, max_frame_size: u32) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// SETTINGS_MAX_HEADER_LIST_SIZE (0x6); `None` is unlimited. Default `Some(DEFAULT_MAX_HEADER_LIST_SIZE)`.
    pub fn max_header_list_size(&self) -> Option<u32> {
        self.max_header_list_size
    }

    /// Set SETTINGS_MAX_HEADER_LIST_SIZE (0x6); `None` is unlimited. Default `Some(DEFAULT_MAX_HEADER_LIST_SIZE)`.
    pub fn with_max_header_list_size(mut self, max_header_list_size: Option<u32>) -> Self {
        self.max_header_list_size = max_header_list_size;
        self
    }
}

impl Settings {
    /// Client defaults: push disabled (ENABLE_PUSH=0).
    pub fn client_default() -> Self {
        Self {
            enable_push: false,
            ..Default::default()
        }
    }

    /// Encode settings as a sequence of 6-byte (id: u16, value: u32) pairs.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf);
        buf
    }

    /// Encode settings into `buf`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_setting(buf, SETTINGS_HEADER_TABLE_SIZE, self.header_table_size);
        encode_setting(
            buf,
            SETTINGS_ENABLE_PUSH,
            if self.enable_push { 1 } else { 0 },
        );
        if let Some(v) = self.max_concurrent_streams {
            encode_setting(buf, SETTINGS_MAX_CONCURRENT_STREAMS, v);
        }
        encode_setting(buf, SETTINGS_INITIAL_WINDOW_SIZE, self.initial_window_size);
        encode_setting(buf, SETTINGS_MAX_FRAME_SIZE, self.max_frame_size);
        if let Some(v) = self.max_header_list_size {
            encode_setting(buf, SETTINGS_MAX_HEADER_LIST_SIZE, v);
        }
    }

    /// Decode settings from a byte buffer of 6-byte pairs.
    pub fn decode(buf: &[u8]) -> Result<Self, H2Error> {
        if !buf.len().is_multiple_of(6) {
            return Err(H2Error::FrameSizeError);
        }
        let mut settings = Settings::default();
        let mut pos = 0;
        while pos + 6 <= buf.len() {
            let id = (u16::from(buf[pos]) << 8) | u16::from(buf[pos + 1]);
            let value = (u32::from(buf[pos + 2]) << 24)
                | (u32::from(buf[pos + 3]) << 16)
                | (u32::from(buf[pos + 4]) << 8)
                | u32::from(buf[pos + 5]);
            pos += 6;
            match id {
                SETTINGS_HEADER_TABLE_SIZE => {
                    // Cap what we actually honour at MAX_PEER_HEADER_TABLE_SIZE,
                    // regardless of what the peer advertised. The peer can't
                    // force us to allocate an arbitrarily large HPACK table.
                    settings.header_table_size = value.min(MAX_PEER_HEADER_TABLE_SIZE);
                }
                SETTINGS_ENABLE_PUSH => {
                    if value > 1 {
                        return Err(H2Error::ProtocolError("ENABLE_PUSH must be 0 or 1".into()));
                    }
                    settings.enable_push = value == 1;
                }
                SETTINGS_MAX_CONCURRENT_STREAMS => {
                    settings.max_concurrent_streams = Some(value);
                }
                SETTINGS_INITIAL_WINDOW_SIZE => {
                    if value > 0x7fff_ffff {
                        return Err(H2Error::FlowControlError);
                    }
                    settings.initial_window_size = value;
                }
                SETTINGS_MAX_FRAME_SIZE => {
                    if !(16384..=16_777_215).contains(&value) {
                        return Err(H2Error::ProtocolError("MAX_FRAME_SIZE out of range".into()));
                    }
                    settings.max_frame_size = value;
                }
                SETTINGS_MAX_HEADER_LIST_SIZE => {
                    settings.max_header_list_size = Some(value);
                }
                // Unknown settings MUST be ignored (RFC 7540 Section 6.5.2).
                _ => {}
            }
        }
        Ok(settings)
    }
}

fn encode_setting(buf: &mut Vec<u8>, id: u16, value: u32) {
    buf.push((id >> 8) as u8);
    buf.push(id as u8);
    buf.push((value >> 24) as u8);
    buf.push((value >> 16) as u8);
    buf.push((value >> 8) as u8);
    buf.push(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_methods_set_what_the_getters_read() {
        let d = Settings::default();
        assert_eq!(d.header_table_size(), 4096);
        assert!(d.enable_push());
        assert_eq!(d.max_concurrent_streams(), None);
        assert_eq!(d.initial_window_size(), 65535);
        assert_eq!(d.max_frame_size(), 16384);
        assert_eq!(d.max_header_list_size(), Some(DEFAULT_MAX_HEADER_LIST_SIZE));
        let s = Settings::default()
            .with_header_table_size(1)
            .with_enable_push(false)
            .with_max_concurrent_streams(Some(2))
            .with_initial_window_size(3)
            .with_max_frame_size(20000)
            .with_max_header_list_size(None);
        assert_eq!(s.header_table_size(), 1);
        assert!(!s.enable_push());
        assert_eq!(s.max_concurrent_streams(), Some(2));
        assert_eq!(s.initial_window_size(), 3);
        assert_eq!(s.max_frame_size(), 20000);
        assert_eq!(s.max_header_list_size(), None);
    }

    #[test]
    fn default_settings_round_trip() {
        let settings = Settings::default();
        let encoded = settings.encode_to_vec();
        let decoded = Settings::decode(&encoded).unwrap();
        assert_eq!(decoded.header_table_size, 4096);
        assert!(decoded.enable_push);
        assert_eq!(decoded.initial_window_size, 65535);
        assert_eq!(decoded.max_frame_size, 16384);
    }

    #[test]
    fn client_settings_round_trip() {
        let settings = Settings::client_default();
        let encoded = settings.encode_to_vec();
        let decoded = Settings::decode(&encoded).unwrap();
        assert!(!decoded.enable_push);
    }

    #[test]
    fn custom_settings_round_trip() {
        let settings = Settings {
            header_table_size: 8192,
            enable_push: false,
            max_concurrent_streams: Some(100),
            initial_window_size: 1048576,
            max_frame_size: 32768,
            max_header_list_size: Some(65536),
        };
        let encoded = settings.encode_to_vec();
        let decoded = Settings::decode(&encoded).unwrap();
        assert_eq!(decoded.header_table_size, 8192);
        assert!(!decoded.enable_push);
        assert_eq!(decoded.max_concurrent_streams, Some(100));
        assert_eq!(decoded.initial_window_size, 1048576);
        assert_eq!(decoded.max_frame_size, 32768);
        assert_eq!(decoded.max_header_list_size, Some(65536));
    }

    #[test]
    fn invalid_enable_push_rejected() {
        let mut buf = Vec::new();
        encode_setting(&mut buf, 0x2, 2); // ENABLE_PUSH = 2 is invalid
        assert!(Settings::decode(&buf).is_err());
    }

    #[test]
    fn invalid_window_size_rejected() {
        let mut buf = Vec::new();
        encode_setting(&mut buf, 0x4, 0x8000_0000); // > 2^31 - 1
        assert!(Settings::decode(&buf).is_err());
    }

    #[test]
    fn invalid_max_frame_size_rejected() {
        let mut buf = Vec::new();
        encode_setting(&mut buf, 0x5, 100); // < 16384
        assert!(Settings::decode(&buf).is_err());
    }

    #[test]
    fn unknown_setting_ignored() {
        let mut buf = Vec::new();
        encode_setting(&mut buf, 0xff, 42);
        let decoded = Settings::decode(&buf).unwrap();
        assert_eq!(decoded.header_table_size, 4096); // still default
    }
}
