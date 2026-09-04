//! Unbuffered TLS record layer, built on rustls' `UnbufferedConnectionCommon`.
//!
//! Encrypts directly from caller memory into a send-pool slot via
//! `WriteTraffic::encrypt`, removing the copy into rustls' internal plaintext
//! buffer that the buffered engine pays.
//!
//! Implemented in a follow-on plan; this module exists so the
//! `tls-unbuffered` feature builds and is exercised by CI from the start.
//!
//! Note the current `tls/mod.rs` split is by file size, not cleanly by engine:
//! `TlsConnKind`, `TlsConn`, `TlsTable` and `drain_tls_plaintext` are still
//! tied to the buffered API. Adding this engine requires threading an engine
//! dimension through them first. [`super::ciphertext::CiphertextBuf`] is the
//! incoming-ciphertext buffer this engine will drive.
