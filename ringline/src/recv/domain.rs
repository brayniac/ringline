//! Per-connection recv delivery domain (segmented-recv, see
//! `docs/segmented-recv-design.md`).
//!
//! Selects how a connection's received provided buffers are delivered:
//! - `CopyOrConsume` (the default) — the established path: bytes are copied into
//!   the `RecvAccumulator` (or held in the single-buffer zero-copy slot) and
//!   surfaced through `with_data`/`with_bytes`.
//! - `Segmented` — received bytes are held in `segment_hold` instead of the
//!   accumulator, for segment reads (Modes B and C) and `forward_to` /
//!   `forward_to_conn` sources (Mode A). Only a TLS connection enters it: its
//!   decrypted plaintext is held as owned entries. A plaintext connection
//!   receives directly into its accumulator and is refused with
//!   `io::ErrorKind::Unsupported`.
//!
//! This is a minimal per-connection selector; the holding machinery lives on the
//! `Driver` (`segment_hold`).

/// How received provided buffers are delivered for a connection.
///
/// Consumed only by the io_uring backend's recv path; under the mio backend the
/// enum is unused (there is no provided-buffer ring), hence the `dead_code`
/// allow — matching the "inert until a consumer exists" pattern in
/// `recv::occupancy`.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum RecvDomain {
    /// Copy into the accumulator / single-buffer zero-copy hold, surfaced via
    /// `with_data`/`with_bytes`. The default for every connection.
    #[default]
    CopyOrConsume,
    /// Hold received provided buffers in-place for segmented delivery (Mode B/C).
    /// Constructed only in tests until the segment reader lands (later increment).
    Segmented,
}
