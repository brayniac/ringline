//! BENCH-ONLY SCAFFOLDING — investigation, not shipping code.
//!
//! In-process measurement of the io_uring TLS send entry point
//! (`crate::tls::encrypt_to_sends`) with no sockets, no io_uring submission and
//! no peer. The same symbol is measured under both engines; which one is
//! compiled is the `tls-unbuffered` cargo feature.
//!
//! Run with:
//! ```text
//! cargo test -p ringline --release --lib tls::send_microbench -- --nocapture --test-threads=1
//! ```

#![allow(clippy::print_stdout)]

use std::io::Write as _;
use std::sync::Arc;
use std::time::Instant;

use crate::accumulator::AccumulatorTable;
use crate::buffer::send_copy::SendCopyPool;
use crate::handler::BuiltSend;
use crate::tls::{PlaintextSink, TlsTable, encrypt_to_sends, feed_tls_recv};

// ── harness ─────────────────────────────────────────────────────────────

fn configs() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let key = rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
    let cert_der = rustls::pki_types::CertificateDer::from(cert.cert);
    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key.into())
            .unwrap(),
    );
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();
    let client_config: Arc<rustls::ClientConfig> = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
        .into();
    (server_config, client_config)
}

/// Drain the ciphertext out of the pool slots `sends` refers to and release
/// every slot, so the pool is whole again.
fn take_and_release(pool: &mut SendCopyPool, sends: Vec<BuiltSend>, out: &mut Vec<u8>) {
    for s in sends {
        let (ptr, len) = pool.current_ptr_remaining(s.pool_slot);
        // SAFETY: slot is filled and in use until we release it below.
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, len as usize) });
        pool.release(s.pool_slot);
    }
}

/// Release without reading — the measured loop's cleanup.
#[inline]
fn release_only(pool: &mut SendCopyPool, sends: Vec<BuiltSend>) -> (usize, u32) {
    let n = sends.len();
    let mut bytes = 0;
    for s in sends {
        bytes += s.total_len;
        pool.release(s.pool_slot);
    }
    (n, bytes)
}

/// A handshaked server connection at index 0 of a `TlsTable`, built with
/// whichever engine the feature selected, plus the peer client connection
/// (always rustls' buffered API — it is only the traffic generator).
struct Handshaked {
    table: TlsTable,
    client: rustls::ClientConnection,
    pool: SendCopyPool,
    accs: AccumulatorTable,
}

fn handshaked(pool_slots: u16, slot_size: u32) -> Handshaked {
    let (server_config, client_config) = configs();
    let mut table = TlsTable::new(4, Some(server_config), Some(client_config.clone()));
    table.create(0).expect("server conn");
    let name: rustls::pki_types::ServerName<'static> = "localhost".try_into().unwrap();
    let mut client = rustls::ClientConnection::new(client_config, name).unwrap();

    let mut pool = SendCopyPool::new(pool_slots, slot_size);
    let mut accs = AccumulatorTable::new(4, 64 * 1024);

    for _ in 0..40 {
        // client → server
        let mut ct = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut ct).unwrap();
        }
        let mut sends = Vec::new();
        if !ct.is_empty() || client.is_handshaking() {
            let sink = PlaintextSink::Accumulator(&mut accs);
            let _ = feed_tls_recv(&mut table, sink, &mut pool, 0, 0, &ct, &mut sends);
        }
        // server → client
        let mut back = Vec::new();
        take_and_release(&mut pool, sends, &mut back);
        if !back.is_empty() {
            let mut cursor = std::io::Cursor::new(&back[..]);
            while (cursor.position() as usize) < back.len() {
                if client.read_tls(&mut cursor).unwrap() == 0 {
                    break;
                }
                client.process_new_packets().unwrap();
            }
        }
        let server_handshaking = table.get_mut(0).unwrap().conn.is_handshaking();
        if !client.is_handshaking() && !server_handshaking {
            break;
        }
    }
    assert!(
        !client.is_handshaking(),
        "client handshake did not complete"
    );
    assert!(
        !table.get_mut(0).unwrap().conn.is_handshaking(),
        "server handshake did not complete"
    );
    // Match the runtime: a ConnCtx is only handed out post-handshake.
    table.get_mut(0).unwrap().handshake_complete = true;
    assert_eq!(pool.free_count(), pool_slots as usize, "pool leaked a slot");

    Handshaked {
        table,
        client,
        pool,
        accs,
    }
}

// ── measurement ─────────────────────────────────────────────────────────

const ENGINE: &str = if cfg!(feature = "tls-unbuffered") {
    "unbuffered"
} else {
    "buffered"
};

struct Row {
    label: String,
    size: usize,
    reps: Vec<f64>, // ns/op per rep
    sends_per_op: usize,
    ct_per_op: u32,
}

fn summarize(rows: &[Row]) {
    println!(
        "\n{:<26} {:>9} {:>6} {:>8} {:>12} {:>12} {:>12} {:>9} {:>8}",
        "case",
        "size",
        "sends",
        "ct/op",
        "ns/op min",
        "ns/op med",
        "ns/op max",
        "ns/byte",
        "spread"
    );
    for r in rows {
        let mut s = r.reps.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min = s[0];
        let med = s[s.len() / 2];
        let max = s[s.len() - 1];
        println!(
            "{:<26} {:>9} {:>6} {:>8} {:>12.1} {:>12.1} {:>12.1} {:>9.4} {:>7.2}%",
            r.label,
            r.size,
            r.sends_per_op,
            r.ct_per_op,
            min,
            med,
            max,
            med / r.size as f64,
            (max - min) / min * 100.0
        );
    }
}

/// Total plaintext bytes pushed per rep. Keeps every size at comparable work.
const BYTES_PER_REP: usize = 96 << 20;
const REPS: usize = 7;

/// One measurement case. `cold` cycles the plaintext over a working set far
/// larger than L3 so the source is never cache-resident.
fn measure(label: &str, size: usize, slot_size: u32, cold: bool, reps: usize) -> Row {
    // enough slots for the largest payload at this slot size, plus slack
    let need = (size / slot_size as usize + 4).max(8);
    let slots = u16::try_from(need.min(4000)).unwrap();
    let mut h = handshaked(slots, slot_size);

    // Working set: 1 buffer when hot, ~256 MiB worth of distinct buffers when
    // cold (bigger than any L3 on the box).
    let copies = if cold {
        ((256usize << 20) / size).clamp(2, 8192)
    } else {
        1
    };
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(copies);
    for c in 0..copies {
        payloads.push(
            (0..size)
                .map(|i| (i as u32).wrapping_mul(2654435761).wrapping_add(c as u32) as u8)
                .collect(),
        );
    }

    let iters = (BYTES_PER_REP / size).max(16);

    // warm up + capture shape
    let mut sends_per_op = 0;
    let mut ct_per_op = 0;
    for _ in 0..8 {
        let sends =
            encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &payloads[0]).expect("encrypt");
        let (n, b) = release_only(&mut h.pool, sends);
        sends_per_op = n;
        ct_per_op = b;
    }

    let mut out = Vec::with_capacity(reps);
    let mut cursor = 0usize;
    for _ in 0..reps {
        let t0 = Instant::now();
        for _ in 0..iters {
            let pt = &payloads[cursor % copies];
            cursor += 1;
            let sends = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, sends);
            std::hint::black_box(b);
        }
        let el = t0.elapsed();
        out.push(el.as_nanos() as f64 / iters as f64);
    }
    // keep the peer + accumulators alive so nothing is optimised out
    std::hint::black_box(&h.client);
    std::hint::black_box(&h.accs);
    assert_eq!(h.pool.free_count(), slots as usize, "pool leaked a slot");

    Row {
        label: label.to_string(),
        size,
        reps: out,
        sends_per_op,
        ct_per_op,
    }
}

/// Control: the same bytes copied into pool slots with no TLS at all. This is
/// the cost of exactly one pass over the payload plus the pool bookkeeping —
/// the thing the unbuffered engine claims to delete.
fn measure_plain_copy(label: &str, size: usize, slot_size: u32, cold: bool, reps: usize) -> Row {
    let need = (size / slot_size as usize + 4).max(8);
    let slots = u16::try_from(need.min(4000)).unwrap();
    let mut pool = SendCopyPool::new(slots, slot_size);
    let copies = if cold {
        ((256usize << 20) / size).clamp(2, 8192)
    } else {
        1
    };
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(copies);
    for c in 0..copies {
        payloads.push(
            (0..size)
                .map(|i| (i as u32).wrapping_mul(2654435761).wrapping_add(c as u32) as u8)
                .collect(),
        );
    }
    let iters = (BYTES_PER_REP / size).max(16);
    let mut out = Vec::with_capacity(reps);
    let mut cursor = 0usize;
    let mut staged: Vec<u16> = Vec::with_capacity(need);
    let mut sends_per_op = 0;
    for _ in 0..reps {
        let t0 = Instant::now();
        for _ in 0..iters {
            let pt = &payloads[cursor % copies];
            cursor += 1;
            staged.clear();
            for chunk in std::hint::black_box(pt).chunks(slot_size as usize) {
                let (slot, ptr, len) = pool.copy_in(chunk).expect("pool");
                std::hint::black_box((ptr, len));
                staged.push(slot);
            }
            sends_per_op = staged.len();
            for &s in &staged {
                pool.release(s);
            }
        }
        out.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    Row {
        label: label.to_string(),
        size,
        reps: out,
        sends_per_op,
        ct_per_op: size as u32,
    }
}

const SIZES: [usize; 5] = [1 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20];

#[test]
fn tls_send_microbench() {
    // Report what we actually negotiated — crypto choice dominates the
    // interpretation of every number below.
    {
        let h = handshaked(64, 16384);
        let info = h.table.get_info(0).expect("info");
        println!(
            "engine={ENGINE} version={:?} suite={:?}",
            info.protocol_version(),
            info.cipher_suite().map(|s| s.suite())
        );
    }
    let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("engine={ENGINE} loadavg_before={}", loadavg.trim());

    let mut rows = Vec::new();
    for &size in &SIZES {
        rows.push(measure(
            &format!("tls hot {ENGINE}"),
            size,
            16384,
            false,
            REPS,
        ));
    }
    for &size in &SIZES {
        rows.push(measure(
            &format!("tls cold {ENGINE}"),
            size,
            16384,
            true,
            REPS,
        ));
    }
    for &size in &SIZES {
        rows.push(measure_plain_copy(
            "copy-only hot",
            size,
            16384,
            false,
            REPS,
        ));
    }
    for &size in &SIZES {
        rows.push(measure_plain_copy(
            "copy-only cold",
            size,
            16384,
            true,
            REPS,
        ));
    }
    summarize(&rows);

    let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("engine={ENGINE} loadavg_after={}", loadavg.trim());
    std::io::stdout().flush().ok();
}

/// Slot size changes the number of `encrypt` calls per op for the unbuffered
/// engine and the number of `write_tls` drains for the buffered one, at a fixed
/// payload. If per-chunk overhead is what eats the saved copy, this moves it.
#[test]
fn tls_send_slot_size_sweep() {
    println!("engine={ENGINE} slot-size sweep @ 256 KiB");
    let mut rows = Vec::new();
    for &slot in &[4096u32, 8192, 16384, 32768, 65536] {
        rows.push(measure(
            &format!("tls hot slot={slot}"),
            256 << 10,
            slot,
            false,
            5,
        ));
    }
    summarize(&rows);
    std::io::stdout().flush().ok();
}

// ── allocation accounting ───────────────────────────────────────────────
//
// "Count the bytes actually moved" — a `PrefixedPayload` allocation per TLS
// record is the tell that rustls encrypted into its own scratch buffer and
// then copied the ciphertext out, rather than encrypting into `dst`.

mod counting_alloc {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

    pub static ON: AtomicBool = AtomicBool::new(false);
    pub static COUNT: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicU64 = AtomicU64::new(0);

    pub struct Counting;

    // SAFETY: every method forwards to `System` unchanged; the counters are
    // side effects only.
    unsafe impl GlobalAlloc for Counting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if ON.load(Relaxed) {
                COUNT.fetch_add(1, Relaxed);
                BYTES.fetch_add(layout.size() as u64, Relaxed);
            }
            unsafe { System.alloc(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            if ON.load(Relaxed) {
                COUNT.fetch_add(1, Relaxed);
                BYTES.fetch_add(new_size.saturating_sub(layout.size()) as u64, Relaxed);
            }
            unsafe { System.realloc(ptr, layout, new_size) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if ON.load(Relaxed) {
                COUNT.fetch_add(1, Relaxed);
                BYTES.fetch_add(layout.size() as u64, Relaxed);
            }
            unsafe { System.alloc_zeroed(layout) }
        }
    }
}

#[global_allocator]
static COUNTING_ALLOCATOR: counting_alloc::Counting = counting_alloc::Counting;

#[test]
fn tls_send_allocation_profile() {
    use std::sync::atomic::Ordering::Relaxed;
    println!("engine={ENGINE} allocation profile per encrypt_to_sends call");
    println!(
        "{:<12} {:>8} {:>10} {:>10} {:>12} {:>12}",
        "size", "sends", "ct/op", "records", "allocs/op", "allocbytes/op"
    );
    for &size in &SIZES {
        let slots = u16::try_from((size / 16384 + 4).max(8)).unwrap();
        let mut h = handshaked(slots, 16384);
        let pt: Vec<u8> = (0..size)
            .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
            .collect();
        // warm: let any lazily-built state settle so it is not charged below
        for _ in 0..4 {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).unwrap();
            release_only(&mut h.pool, s);
        }
        const N: u64 = 64;
        counting_alloc::COUNT.store(0, Relaxed);
        counting_alloc::BYTES.store(0, Relaxed);
        counting_alloc::ON.store(true, Relaxed);
        let mut sends = 0;
        let mut ct = 0;
        for _ in 0..N {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).unwrap();
            let (n, b) = release_only(&mut h.pool, s);
            sends = n;
            ct = b;
        }
        counting_alloc::ON.store(false, Relaxed);
        let allocs = counting_alloc::COUNT.load(Relaxed) as f64 / N as f64;
        let bytes = counting_alloc::BYTES.load(Relaxed) as f64 / N as f64;
        let records = (ct as usize - size) / 22;
        println!(
            "{:<12} {:>8} {:>10} {:>10} {:>12.2} {:>12.0}",
            size, sends, ct, records, allocs, bytes
        );
    }
    std::io::stdout().flush().ok();
}

// ── recv path (context for the end-to-end 16 KiB result) ────────────────

/// Measure `feed_tls_recv`: ciphertext in, plaintext into the accumulator.
/// The engines diverge here too, and the send-path numbers above cannot
/// explain an end-to-end win at 16 KiB, so this is where to look next.
fn measure_recv(label: &str, size: usize, reps: usize) -> Row {
    let mut h = handshaked(64, 16384);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();
    // Pre-encrypt one message with the peer; feed the same ciphertext every
    // iteration is NOT valid (sequence numbers), so build a batch up front.
    let batch = ((64usize << 20) / size).clamp(64, 512);
    // rustls caps `sendable_tls` at 64 KiB by default; a 256 KiB write_all
    // would fail with WriteZero before we ever drain it.
    h.client.set_buffer_limit(None);
    let mut msgs: Vec<Vec<u8>> = Vec::with_capacity(batch);
    for _ in 0..batch {
        let mut ct = Vec::new();
        h.client.writer().write_all(&pt).unwrap();
        while h.client.wants_write() {
            h.client.write_tls(&mut ct).unwrap();
        }
        msgs.push(ct);
    }
    let mut out = Vec::new();
    let mut iters = 0usize;
    let t_all = Instant::now();
    let mut reps_left = reps;
    let mut idx = 0usize;
    let mut per_rep = Vec::new();
    while reps_left > 0 && idx < batch {
        let chunk = (batch / reps).max(1);
        let t0 = Instant::now();
        let mut n = 0;
        while n < chunk && idx < batch {
            let mut sends = Vec::new();
            let sink = PlaintextSink::Accumulator(&mut h.accs);
            let r = feed_tls_recv(
                &mut h.table,
                sink,
                &mut h.pool,
                0,
                0,
                std::hint::black_box(&msgs[idx]),
                &mut sends,
            );
            assert!(matches!(r, crate::tls::TlsRecvResult::Ok), "recv failed");
            release_only(&mut h.pool, sends);
            let got = h.accs.data(0).len();
            assert_eq!(got, size, "one message per feed");
            h.accs.consume(0, got);
            idx += 1;
            n += 1;
            iters += 1;
        }
        per_rep.push(t0.elapsed().as_nanos() as f64 / n as f64);
        reps_left -= 1;
    }
    out.push(t_all.elapsed());
    std::hint::black_box(&out);
    assert!(iters > 0);
    Row {
        label: label.to_string(),
        size,
        reps: per_rep,
        sends_per_op: 0,
        ct_per_op: 0,
    }
}

#[test]
fn tls_recv_microbench() {
    println!("engine={ENGINE} recv path");
    let mut rows = Vec::new();
    for &size in &[1 << 10usize, 16 << 10, 64 << 10, 256 << 10] {
        rows.push(measure_recv(&format!("tls recv {ENGINE}"), size, 8));
    }
    summarize(&rows);
    std::io::stdout().flush().ok();
}

// ════════════════════════════════════════════════════════════════════════
// Round 2 — slot-size sweep and a tightened recv measurement.
//
// Everything below emits machine-readable lines so runs from the two feature
// builds can be interleaved at the process level and aggregated afterwards:
//
//   R<TAB>test<TAB>engine<TAB>size<TAB>param<TAB>rep<TAB>ns_per_op
//   S<TAB>test<TAB>engine<TAB>size<TAB>param<TAB>sends<TAB>ct<TAB>records<TAB>straddle<TAB>allocs<TAB>allocbytes
// ════════════════════════════════════════════════════════════════════════

/// Copy out the ciphertext the sends point at, and the per-slot lengths, then
/// release. Used for exact record accounting.
fn drain_shape(pool: &mut SendCopyPool, sends: Vec<BuiltSend>) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new();
    let mut lens = Vec::with_capacity(sends.len());
    for s in sends {
        let (ptr, len) = pool.current_ptr_remaining(s.pool_slot);
        // SAFETY: the slot is filled and still held until the release below.
        bytes.extend_from_slice(unsafe { std::slice::from_raw_parts(ptr, len as usize) });
        lens.push(len as usize);
        pool.release(s.pool_slot);
    }
    (bytes, lens)
}

/// Walk the TLS record headers in `ct` and report `(records, straddling)`,
/// where a record straddles if a slot boundary falls strictly inside it.
/// This is a direct count off the wire format, not `(ct - size) / overhead`.
fn count_records(ct: &[u8], slot_lens: &[usize]) -> (usize, usize) {
    let mut bounds = Vec::with_capacity(slot_lens.len());
    let mut acc = 0usize;
    for &l in slot_lens {
        acc += l;
        bounds.push(acc);
    }
    let mut off = 0usize;
    let (mut recs, mut strad) = (0usize, 0usize);
    while off + 5 <= ct.len() {
        let len = u16::from_be_bytes([ct[off + 3], ct[off + 4]]) as usize;
        let end = off + 5 + len;
        if end > ct.len() {
            break;
        }
        recs += 1;
        if bounds.iter().any(|&b| b > off && b < end) {
            strad += 1;
        }
        off = end;
    }
    (recs, strad)
}

/// Slots to provision so any engine can encrypt `size` at `slot_size`.
/// Generous on purpose: pool exhaustion would show up as an error, not a
/// slow number, but there is no reason to run close to the edge.
fn slots_for(size: usize, slot_size: u32) -> u16 {
    let per_slot = (slot_size as usize).min(16384).saturating_sub(64).max(1);
    u16::try_from((size / per_slot + 8).clamp(8, 4000)).unwrap()
}

/// Send-path timing at an explicit slot size. `bytes_per_rep` fixes the work
/// per rep so every payload size is comparable.
fn measure_send2(size: usize, slot_size: u32, bytes_per_rep: usize, reps: usize) -> Vec<f64> {
    let slots = slots_for(size, slot_size);
    let mut h = handshaked(slots, slot_size);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();
    let iters = (bytes_per_rep / size).max(16);

    for _ in 0..8 {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
        release_only(&mut h.pool, s);
    }

    let mut out = Vec::with_capacity(reps);
    for _ in 0..reps {
        let t0 = Instant::now();
        for _ in 0..iters {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(&pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, s);
            std::hint::black_box(b);
        }
        out.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    assert_eq!(h.pool.free_count(), slots as usize, "pool leaked a slot");
    std::hint::black_box(&h.client);
    out
}

/// Send-path shape at an explicit slot size: slots, ciphertext bytes, exact
/// record count, straddling records, allocations.
#[allow(clippy::type_complexity)]
fn measure_send_shape(size: usize, slot_size: u32) -> (usize, u32, usize, usize, f64, f64) {
    use std::sync::atomic::Ordering::Relaxed;
    let slots = slots_for(size, slot_size);
    let mut h = handshaked(slots, slot_size);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();
    for _ in 0..8 {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
        release_only(&mut h.pool, s);
    }
    // Shape pass (not timed, allocator off).
    let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
    let nsends = s.len();
    let (ct, lens) = drain_shape(&mut h.pool, s);
    let ctlen = ct.len() as u32;
    let (records, straddle) = count_records(&ct, &lens);

    const N: u64 = 64;
    counting_alloc::COUNT.store(0, Relaxed);
    counting_alloc::BYTES.store(0, Relaxed);
    counting_alloc::ON.store(true, Relaxed);
    for _ in 0..N {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
        release_only(&mut h.pool, s);
    }
    counting_alloc::ON.store(false, Relaxed);
    let allocs = counting_alloc::COUNT.load(Relaxed) as f64 / N as f64;
    let abytes = counting_alloc::BYTES.load(Relaxed) as f64 / N as f64;
    assert_eq!(h.pool.free_count(), slots as usize, "pool leaked a slot");
    (nsends, ctlen, records, straddle, allocs, abytes)
}

/// Slot sizes bracketing the one-max-record boundary.
///
/// A full TLS 1.3 AES-GCM record on the wire is 5 (header) + 16384 (max
/// plaintext, `fragmenter::MAX_FRAGMENT_LEN`) + 17 (content-type byte + tag,
/// `encrypted_payload_len`) = **16406**. The default slot is 16384, so it
/// misses a whole record by 22 bytes and `encrypt_chunk` must shrink. 16645 =
/// 5 + 2^14 + 256 is RFC 8446's suite-independent `TLSCiphertext` ceiling.
/// Sizes above that fit *several* whole records per slot — a different lever
/// (per-`encrypt`-call amortisation), not the record-count fix.
const SLOTS_SHAPE: [u32; 10] = [
    16384, 16400, 16406, 16512, 16645, 17408, 32768, 32812, 65536, 65624,
];
const SLOTS_TIME: [u32; 8] = [16384, 16406, 16645, 17408, 32768, 32812, 65536, 65624];

const SEND_SIZES: [usize; 5] = [1 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20];

/// Payloads that bracket every "exact multiple" boundary in play, so the
/// relocation question can be answered from record counts rather than a story:
/// multiples of 16384 (the max record plaintext), of 16362 (what fits when the
/// slot is 16384 and `encrypt_chunk` has shrunk), and one-off neighbours.
const SHAPE_SIZES: [usize; 15] = [
    1024, 16362, 16383, 16384, 16385, 16406, 32724, 32767, 32768, 32769, 65536, 262143, 262144,
    262145, 1048576,
];

/// Straddle payloads, timed, at the two slot sizes the decision hinges on.
const STRADDLE_SIZES: [usize; 9] = [
    16383, 16384, 16385, 32767, 32768, 32769, 262143, 262144, 262145,
];
const STRADDLE_SLOTS: [u32; 3] = [16384, 16406, 16645];

const SEND_BYTES_PER_REP: usize = 32 << 20;
const SEND_REPS: usize = 5;

#[test]
fn slot_sweep_timing() {
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "# slot_sweep_timing engine={ENGINE} loadavg_before={}",
        lp.trim()
    );
    for &slot in &SLOTS_TIME {
        for &size in &SEND_SIZES {
            let reps = measure_send2(size, slot, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\tslot_sweep\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "# slot_sweep_timing engine={ENGINE} loadavg_after={}",
        lp.trim()
    );
    std::io::stdout().flush().ok();
}

#[test]
fn slot_multiple_timing() {
    println!("# slot_multiple_timing engine={ENGINE}");
    for &slot in &STRADDLE_SLOTS {
        for &size in &STRADDLE_SIZES {
            let reps = measure_send2(size, slot, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\tstraddle\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    std::io::stdout().flush().ok();
}

#[test]
fn slot_sweep_shape() {
    println!("# slot_sweep_shape engine={ENGINE}");
    for &slot in &SLOTS_SHAPE {
        for &size in &SHAPE_SIZES {
            let (sends, ct, recs, strad, allocs, abytes) = measure_send_shape(size, slot);
            println!(
                "S\tslot_sweep\t{ENGINE}\t{size}\t{slot}\t{sends}\t{ct}\t{recs}\t{strad}\t{allocs:.2}\t{abytes:.0}"
            );
        }
    }
    std::io::stdout().flush().ok();
}

// ── recv, tightened ─────────────────────────────────────────────────────

/// Pre-encrypted application-data messages from the peer, each the ciphertext
/// for one `size`-byte plaintext write. Distinct messages are mandatory: TLS
/// sequence numbers make replay a decrypt failure, so the batch is built up
/// front and consumed once.
struct RecvBatch {
    h: Handshaked,
    msgs: Vec<Vec<u8>>,
}

fn build_recv_batch(size: usize, budget_bytes: usize, max_msgs: usize) -> RecvBatch {
    let mut h = handshaked(64, 16384);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();
    let batch = (budget_bytes / size).clamp(64, max_msgs);
    // rustls caps `sendable_tls` at 64 KiB by default; a 256 KiB write_all
    // would fail with WriteZero before we ever drain it.
    h.client.set_buffer_limit(None);
    let mut msgs = Vec::with_capacity(batch);
    for _ in 0..batch {
        let mut ct = Vec::new();
        h.client.writer().write_all(&pt).unwrap();
        while h.client.wants_write() {
            h.client.write_tls(&mut ct).unwrap();
        }
        msgs.push(ct);
    }
    RecvBatch { h, msgs }
}

/// Feed one message's ciphertext, in `chunk` sized pieces (0 = whole), and
/// assert the whole plaintext landed. Returns nothing; the caller times it.
#[inline]
fn feed_one(h: &mut Handshaked, msg: &[u8], chunk: usize, size: usize) {
    let step = if chunk == 0 { msg.len() } else { chunk };
    let mut fed = 0usize;
    while fed < msg.len() {
        let end = (fed + step).min(msg.len());
        let mut sends = Vec::new();
        let sink = PlaintextSink::Accumulator(&mut h.accs);
        let r = feed_tls_recv(
            &mut h.table,
            sink,
            &mut h.pool,
            0,
            0,
            std::hint::black_box(&msg[fed..end]),
            &mut sends,
        );
        assert!(matches!(r, crate::tls::TlsRecvResult::Ok), "recv failed");
        release_only(&mut h.pool, sends);
        fed = end;
    }
    let got = h.accs.data(0).len();
    debug_assert_eq!(got, size, "one message per feed");
    h.accs.consume(0, got);
}

/// `chunk` models the runtime's recv buffer size: the io_uring recv path hands
/// `feed_tls_recv` one provided buffer at a time, so a 16 KiB recv buffer
/// splits a 256 KiB message across 16 calls. 0 means "whole message at once".
fn measure_recv2(size: usize, chunk: usize, reps: usize) -> Vec<f64> {
    const BUDGET: usize = 128 << 20;
    const MAX_MSGS: usize = 8192;
    let mut b = build_recv_batch(size, BUDGET, MAX_MSGS);
    let n = b.msgs.len();
    let per = (n / (reps + 1)).max(1); // one slice reserved for warm-up
    let mut idx = 0usize;
    // warm-up slice, not recorded
    for _ in 0..per.min(n) {
        let msg = std::mem::take(&mut b.msgs[idx]);
        feed_one(&mut b.h, &msg, chunk, size);
        idx += 1;
    }
    let mut out = Vec::with_capacity(reps);
    for _ in 0..reps {
        if idx >= n {
            break;
        }
        let take = per.min(n - idx);
        let t0 = Instant::now();
        for _ in 0..take {
            let msg = std::mem::take(&mut b.msgs[idx]);
            feed_one(&mut b.h, &msg, chunk, size);
            idx += 1;
        }
        out.push(t0.elapsed().as_nanos() as f64 / take as f64);
    }
    out
}

/// Allocation accounting for the recv path — the mechanism probe. rustls'
/// buffered `read_tls` grows its deframer buffer 4 KiB at a time
/// (`DeframerVecBuffer::prepare_read`, `READ_SIZE = 4096`) and shrinks it back
/// when the buffer empties, so a large message should show many allocations
/// and many resized bytes on the buffered engine and few on the unbuffered
/// one, which appends into a fixed `CiphertextBuf`.
fn measure_recv_allocs(size: usize, chunk: usize) -> (f64, f64) {
    use std::sync::atomic::Ordering::Relaxed;
    const N: usize = 48;
    let mut b = build_recv_batch(size, 32 << 20, 4096);
    let n = b.msgs.len();
    assert!(n > N + 8, "batch too small for alloc probe");
    let mut idx = 0usize;
    for _ in 0..8 {
        let msg = std::mem::take(&mut b.msgs[idx]);
        feed_one(&mut b.h, &msg, chunk, size);
        idx += 1;
    }
    counting_alloc::COUNT.store(0, Relaxed);
    counting_alloc::BYTES.store(0, Relaxed);
    counting_alloc::ON.store(true, Relaxed);
    for _ in 0..N {
        let msg = std::mem::take(&mut b.msgs[idx]);
        feed_one(&mut b.h, &msg, chunk, size);
        idx += 1;
    }
    counting_alloc::ON.store(false, Relaxed);
    (
        counting_alloc::COUNT.load(Relaxed) as f64 / N as f64,
        counting_alloc::BYTES.load(Relaxed) as f64 / N as f64,
    )
}

const RECV_SIZES: [usize; 4] = [1 << 10, 16 << 10, 64 << 10, 256 << 10];
/// Recv chunk = the runtime's `recv_buffer.buffer_size`. 16384 is the default.
const RECV_CHUNKS: [usize; 4] = [4096, 16384, 65536, 0];
const RECV_REPS: usize = 10;

#[test]
fn recv_timing() {
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("# recv_timing engine={ENGINE} loadavg_before={}", lp.trim());
    for &chunk in &RECV_CHUNKS {
        for &size in &RECV_SIZES {
            let reps = measure_recv2(size, chunk, RECV_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\trecv\t{ENGINE}\t{size}\t{chunk}\t{i}\t{ns:.2}");
            }
        }
    }
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("# recv_timing engine={ENGINE} loadavg_after={}", lp.trim());
    std::io::stdout().flush().ok();
}

#[test]
fn recv_shape() {
    println!("# recv_shape engine={ENGINE}");
    for &chunk in &RECV_CHUNKS {
        for &size in &RECV_SIZES {
            let (allocs, abytes) = measure_recv_allocs(size, chunk);
            println!("S\trecv\t{ENGINE}\t{size}\t{chunk}\t0\t0\t0\t0\t{allocs:.2}\t{abytes:.0}");
        }
    }
    std::io::stdout().flush().ok();
}

/// Engine-independent probe of the buffered recv loop's shape, driving plain
/// rustls directly with exactly the loop `feed_tls_recv_buffered` runs:
/// `while cursor.position() < len { read_tls; process_new_packets; drain }`.
/// It compiles and reports identically in both feature builds, so it isolates
/// "how many times does rustls' deframer get re-driven" from any cross-build
/// noise.
#[test]
fn buffered_deframer_probe() {
    use std::io::Read as _;
    println!("# buffered_deframer_probe engine={ENGINE}");
    println!(
        "{:<10} {:>7} {:>10} {:>12} {:>14} {:>12} {:>14}",
        "size", "chunk", "read_tls", "process_new", "max_read", "allocs/op", "allocbytes/op"
    );
    for &size in &RECV_SIZES {
        for &chunk in &RECV_CHUNKS {
            let (server_config, client_config) = configs();
            let name: rustls::pki_types::ServerName<'static> = "localhost".try_into().unwrap();
            let mut client = rustls::ClientConnection::new(client_config, name).unwrap();
            let mut server = rustls::ServerConnection::new(server_config).unwrap();
            // handshake the pair
            for _ in 0..16 {
                let mut a = Vec::new();
                while client.wants_write() {
                    client.write_tls(&mut a).unwrap();
                }
                if !a.is_empty() {
                    let mut c = std::io::Cursor::new(&a[..]);
                    while (c.position() as usize) < a.len() {
                        if server.read_tls(&mut c).unwrap() == 0 {
                            break;
                        }
                        server.process_new_packets().unwrap();
                    }
                }
                let mut b = Vec::new();
                while server.wants_write() {
                    server.write_tls(&mut b).unwrap();
                }
                if !b.is_empty() {
                    let mut c = std::io::Cursor::new(&b[..]);
                    while (c.position() as usize) < b.len() {
                        if client.read_tls(&mut c).unwrap() == 0 {
                            break;
                        }
                        client.process_new_packets().unwrap();
                    }
                }
                if !client.is_handshaking() && !server.is_handshaking() {
                    break;
                }
            }
            assert!(!client.is_handshaking() && !server.is_handshaking());
            client.set_buffer_limit(None);

            let pt: Vec<u8> = (0..size)
                .map(|i| (i as u32).wrapping_mul(7) as u8)
                .collect();
            const N: usize = 24;
            let mut msgs = Vec::with_capacity(N + 4);
            for _ in 0..N + 4 {
                let mut ct = Vec::new();
                client.writer().write_all(&pt).unwrap();
                while client.wants_write() {
                    client.write_tls(&mut ct).unwrap();
                }
                msgs.push(ct);
            }
            let mut scratch = vec![0u8; size + 4096];
            let step =
                |m: &[u8], chunk: usize| -> usize { if chunk == 0 { m.len() } else { chunk } };
            // warm
            for m in msgs.iter().take(4) {
                let s = step(m, chunk);
                let mut fed = 0;
                while fed < m.len() {
                    let end = (fed + s).min(m.len());
                    let mut c = std::io::Cursor::new(&m[fed..end]);
                    while (c.position() as usize) < end - fed {
                        if server.read_tls(&mut c).unwrap() == 0 {
                            break;
                        }
                        let st = server.process_new_packets().unwrap();
                        if st.plaintext_bytes_to_read() > 0 {
                            let _ = server.reader().read(&mut scratch).unwrap();
                        }
                    }
                    fed = end;
                }
            }
            let mut reads = 0usize;
            let mut procs = 0usize;
            let mut max_read = 0usize;
            use std::sync::atomic::Ordering::Relaxed;
            counting_alloc::COUNT.store(0, Relaxed);
            counting_alloc::BYTES.store(0, Relaxed);
            counting_alloc::ON.store(true, Relaxed);
            for m in msgs.iter().skip(4) {
                let s = step(m, chunk);
                let mut fed = 0;
                while fed < m.len() {
                    let end = (fed + s).min(m.len());
                    let mut c = std::io::Cursor::new(&m[fed..end]);
                    while (c.position() as usize) < end - fed {
                        let n = server.read_tls(&mut c).unwrap();
                        reads += 1;
                        max_read = max_read.max(n);
                        if n == 0 {
                            break;
                        }
                        let st = server.process_new_packets().unwrap();
                        procs += 1;
                        if st.plaintext_bytes_to_read() > 0 {
                            let _ = server.reader().read(&mut scratch).unwrap();
                        }
                    }
                    fed = end;
                }
            }
            counting_alloc::ON.store(false, Relaxed);
            let allocs = counting_alloc::COUNT.load(Relaxed) as f64 / N as f64;
            let abytes = counting_alloc::BYTES.load(Relaxed) as f64 / N as f64;
            println!(
                "{:<10} {:>7} {:>10.2} {:>12.2} {:>14} {:>12.2} {:>14.0}",
                size,
                chunk,
                reads as f64 / N as f64,
                procs as f64 / N as f64,
                max_read,
                allocs,
                abytes
            );
        }
    }
    std::io::stdout().flush().ok();
}

/// Cold-payload variant of [`measure_send2`]: the plaintext is cycled over a
/// working set far larger than L3, so the source is never cache-resident —
/// which is the situation a real server is in. A slot-size recommendation that
/// only holds hot is not a recommendation.
fn measure_send2_cold(size: usize, slot_size: u32, bytes_per_rep: usize, reps: usize) -> Vec<f64> {
    let slots = slots_for(size, slot_size);
    let mut h = handshaked(slots, slot_size);
    let copies = ((256usize << 20) / size).clamp(2, 8192);
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(copies);
    for c in 0..copies {
        payloads.push(
            (0..size)
                .map(|i| (i as u32).wrapping_mul(2654435761).wrapping_add(c as u32) as u8)
                .collect(),
        );
    }
    let iters = (bytes_per_rep / size).max(16);
    for _ in 0..8 {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &payloads[0]).expect("encrypt");
        release_only(&mut h.pool, s);
    }
    let mut out = Vec::with_capacity(reps);
    let mut cursor = 0usize;
    for _ in 0..reps {
        let t0 = Instant::now();
        for _ in 0..iters {
            let pt = &payloads[cursor % copies];
            cursor += 1;
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, s);
            std::hint::black_box(b);
        }
        out.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    assert_eq!(h.pool.free_count(), slots as usize, "pool leaked a slot");
    out
}

#[test]
fn slot_sweep_cold_timing() {
    println!("# slot_sweep_cold_timing engine={ENGINE}");
    for &slot in &[16384u32, 16406, 32768, 65536, 65624] {
        for &size in &[1 << 10usize, 16 << 10, 64 << 10, 262144, 1 << 20] {
            let reps = measure_send2_cold(size, slot, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\tslot_cold\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    std::io::stdout().flush().ok();
}

/// Same as [`measure_send2`] but with an explicit slot *count*, so the pool's
/// total footprint can be held at the production default (1024 slots) instead
/// of the minimum the payload needs. The minimum-sized pool is fully
/// cache-resident for small payloads, which flatters small slot sizes for a
/// reason that does not exist in a real worker (default pool = 1024 * 16384 =
/// 16 MiB, far past any L2).
fn measure_send2_pool(
    size: usize,
    slot_size: u32,
    slot_count: u16,
    bytes_per_rep: usize,
    reps: usize,
) -> Vec<f64> {
    let mut h = handshaked(slot_count, slot_size);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();
    let iters = (bytes_per_rep / size).max(16);
    for _ in 0..8 {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
        release_only(&mut h.pool, s);
    }
    let mut out = Vec::with_capacity(reps);
    for _ in 0..reps {
        let t0 = Instant::now();
        for _ in 0..iters {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(&pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, s);
            std::hint::black_box(b);
        }
        out.push(t0.elapsed().as_nanos() as f64 / iters as f64);
    }
    assert_eq!(
        h.pool.free_count(),
        slot_count as usize,
        "pool leaked a slot"
    );
    out
}

#[test]
fn slot_pool_realistic() {
    println!("# slot_pool_realistic engine={ENGINE} (pool held at the 1024-slot default)");
    for &slot in &[16384u32, 16406, 32768, 65536] {
        for &size in &[1 << 10usize, 4 << 10, 16 << 10, 64 << 10] {
            let reps = measure_send2_pool(size, slot, 1024, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\tpool1024\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    std::io::stdout().flush().ok();
}

/// Small payloads only, across slot sizes chosen to separate two candidate
/// causes of the small-send penalty seen when the slot grows past 16384:
/// **alignment** (16406 and 16428 are not multiples of 64, so every slot after
/// the first starts mid-cache-line) versus **footprint** (a 1024-slot pool is
/// 16 MiB at 16384 and 64 MiB at 65536). 16448 and 20480 are 64-byte aligned
/// and 20480 is page aligned, so if those recover the 16384 number the cause
/// is alignment, not size.
#[test]
fn small_send_alignment() {
    println!("# small_send_alignment engine={ENGINE} (pool = 1024 slots)");
    for &slot in &[16384u32, 16406, 16428, 16448, 20480, 32768, 65536] {
        for &size in &[512usize, 1 << 10, 4 << 10, 8 << 10] {
            let reps = measure_send2_pool(size, slot, 1024, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\talign\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    std::io::stdout().flush().ok();
}

/// Record counts for the alignment candidates at a large payload — the
/// `encrypt_chunk` shrink loop converges differently either side of 16428, so
/// a slot chosen for alignment can silently double the record count.
#[test]
fn alignment_candidates_shape() {
    println!("# alignment_candidates_shape engine={ENGINE}");
    for &slot in &[
        16384u32, 16406, 16420, 16427, 16428, 16429, 16448, 20480, 32768,
    ] {
        for &size in &[16384usize, 262144] {
            let (sends, ct, recs, strad, allocs, abytes) = measure_send_shape(size, slot);
            println!(
                "S\talign\t{ENGINE}\t{size}\t{slot}\t{sends}\t{ct}\t{recs}\t{strad}\t{allocs:.2}\t{abytes:.0}"
            );
        }
    }
    std::io::stdout().flush().ok();
}

/// Characterise the ~25 ns/op penalty that every slot size other than 16384
/// pays at 1 KiB. Alignment is already ruled out (16448/20480/32768/65536 are
/// all 64-byte aligned and pay it; 20480/32768/65536 are page aligned too).
/// This walks smaller slot sizes as well, to see whether 16384 is special or
/// whether the penalty is simply "slot larger than N".
#[test]
fn small_send_slot_shape() {
    println!("# small_send_slot_shape engine={ENGINE} (pool = 1024 slots, 1 KiB payload)");
    for &slot in &[
        2048u32, 4096, 8192, 12288, 16383, 16384, 16385, 24576, 32768,
    ] {
        for &size in &[1 << 10usize] {
            let reps = measure_send2_pool(size, slot, 1024, SEND_BYTES_PER_REP, SEND_REPS);
            for (i, ns) in reps.iter().enumerate() {
                println!("R\tslotshape\t{ENGINE}\t{size}\t{slot}\t{i}\t{ns:.2}");
            }
        }
    }
    std::io::stdout().flush().ok();
}

// ── INVESTIGATION: slot-size cliff ──────────────────────────────────────
//
// Scaffolding for the "any slot size > 16384 costs ~25 ns/op on small sends"
// question. Everything below is driven from the environment so `perf stat` can
// be pointed at a process whose runtime *is* the measured loop, and so a
// setup-only arm can be subtracted from it.

/// Per-process THP suppression, for the causal half of the huge-page question.
/// `prctl(PR_SET_THP_DISABLE)` is inherited by the whole process and touches
/// nothing outside it — hv01 is shared, so the system-wide
/// `/sys/kernel/mm/transparent_hugepage/enabled` knob is off limits.
fn disable_thp_for_this_process() -> bool {
    const PR_SET_THP_DISABLE: libc::c_int = 41;
    // SAFETY: plain prctl with the documented argument shape; no pointers.
    let rc = unsafe { libc::prctl(PR_SET_THP_DISABLE, 1_u64, 0_u64, 0_u64, 0_u64) };
    rc == 0
}

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}

/// Read `AnonHugePages` (KiB) for the VMA containing `addr`, plus that VMA's
/// start/end/size, out of this process's own smaps.
fn smaps_for(addr: usize) -> Option<(usize, usize, usize, usize)> {
    let s = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut cur: Option<(usize, usize)> = None;
    let mut size_kb = 0usize;
    for line in s.lines() {
        if let Some((range, _)) = line.split_once(' ')
            && let Some((a, b)) = range.split_once('-')
            && let (Ok(a), Ok(b)) = (usize::from_str_radix(a, 16), usize::from_str_radix(b, 16))
        {
            cur = Some((a, b));
            size_kb = 0;
            continue;
        }
        let Some((lo, hi)) = cur else { continue };
        if let Some(v) = line.strip_prefix("Size:") {
            size_kb = v.trim().trim_end_matches(" kB").trim().parse().unwrap_or(0);
        }
        if let Some(v) = line.strip_prefix("AnonHugePages:")
            && addr >= lo
            && addr < hi
        {
            let ahp: usize = v.trim().trim_end_matches(" kB").trim().parse().unwrap_or(0);
            return Some((lo, hi, size_kb, ahp));
        }
    }
    None
}

/// One (slot_size, payload) cell and nothing else, so the process runtime is
/// the measured loop. `RL_SETUP_ONLY=1` runs the identical setup and skips the
/// loop, giving a baseline to subtract from `perf stat` totals.
///
/// Env: RL_SLOT, RL_SIZE, RL_SLOTS, RL_ITERS, RL_SETUP_ONLY, RL_THP.
#[test]
fn perf_single_cell() {
    let slot = env_usize("RL_SLOT", 16384) as u32;
    let size = env_usize("RL_SIZE", 1024);
    let slot_count = env_usize("RL_SLOTS", 1024) as u16;
    let iters = env_usize("RL_ITERS", 20_000_000);
    let setup_only = env_usize("RL_SETUP_ONLY", 0) == 1;
    let thp = env_usize("RL_THP", 0) == 1;
    let nothp = env_usize("RL_NOTHP", 0) == 1;
    if nothp {
        let ok = disable_thp_for_this_process();
        println!("# PR_SET_THP_DISABLE applied={ok}");
    }

    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "# perf_single_cell engine={ENGINE} slot={slot} size={size} slots={slot_count} iters={iters} setup_only={setup_only} loadavg_before={}",
        lp.trim()
    );

    let mut h = handshaked(slot_count, slot);
    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();

    // Warm up exactly as the sweep does.
    for _ in 0..8 {
        let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
        release_only(&mut h.pool, s);
    }

    if thp {
        // Base address of slot 0 == base of the pool's backing allocation.
        let (idx, ptr, _) = h.pool.copy_in(&[0u8; 1]).expect("slot");
        let base = ptr as usize;
        h.pool.release(idx);
        let total = slot_count as usize * slot as usize;
        match smaps_for(base) {
            Some((lo, hi, size_kb, ahp)) => println!(
                "THP\t{ENGINE}\t{slot}\t{slot_count}\tbase=0x{base:x}\tbase_off_2M={}\tpool_bytes={total}\tvma=0x{lo:x}-0x{hi:x}\tvma_kb={size_kb}\tAnonHugePages_kB={ahp}",
                base % (2 << 20)
            ),
            None => println!("THP\t{ENGINE}\t{slot}\t{slot_count}\tbase=0x{base:x}\tNO_VMA_FOUND"),
        }
    }

    if !setup_only {
        let t0 = Instant::now();
        for _ in 0..iters {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(&pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, s);
            std::hint::black_box(b);
        }
        let el = t0.elapsed();
        println!(
            "P\t{ENGINE}\t{slot}\t{size}\t{iters}\t{:.3}",
            el.as_nanos() as f64 / iters as f64
        );
    }
    assert_eq!(
        h.pool.free_count(),
        slot_count as usize,
        "pool leaked a slot"
    );
    std::hint::black_box(&h.client);
    std::io::stdout().flush().ok();
}

/// THP accounting alone, both slot sizes, in one process — no timing, so it can
/// run without a quiet box.
#[test]
fn thp_accounting() {
    let nothp = env_usize("RL_NOTHP", 0) == 1;
    if nothp {
        let ok = disable_thp_for_this_process();
        println!("# PR_SET_THP_DISABLE applied={ok}");
    }
    println!("# thp_accounting engine={ENGINE} nothp={nothp}");
    for &slot in &[16384u32, 16385, 16406, 16448, 20480, 32768, 65536] {
        let slot_count = 1024u16;
        let mut h = handshaked(slot_count, slot);
        let pt: Vec<u8> = (0..1024)
            .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
            .collect();
        for _ in 0..100_000 {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
            release_only(&mut h.pool, s);
        }
        let (idx, ptr, _) = h.pool.copy_in(&[0u8; 1]).expect("slot");
        let base = ptr as usize;
        h.pool.release(idx);
        let total = slot_count as usize * slot as usize;
        match smaps_for(base) {
            Some((lo, hi, size_kb, ahp)) => println!(
                "THP\t{ENGINE}\t{slot}\t{slot_count}\tbase=0x{base:x}\tbase_off_2M={}\tpool_bytes={total}\tvma=0x{lo:x}-0x{hi:x}\tvma_kb={size_kb}\tAnonHugePages_kB={ahp}",
                base % (2 << 20)
            ),
            None => println!("THP\t{ENGINE}\t{slot}\t1024\tbase=0x{base:x}\tNO_VMA_FOUND"),
        }
        std::hint::black_box(&h.client);
    }
    std::io::stdout().flush().ok();
}

/// Separate "slot size" from "pool allocation size", which the original sweep
/// confounds: it held the slot *count* at 1024, so growing the slot also grew
/// the backing `Vec`.
///
/// `16384 * 1028` and `16448 * 1024` are both **exactly** 16,842,752 bytes, so:
///
/// | cell | slot | count | backing bytes |
/// |---|---|---|---|
/// | A | 16384 | 1024 | 16,777,216 (fast in the original sweep) |
/// | B | 16448 | 1024 | 16,842,752 (slow in the original sweep) |
/// | C | 16384 | 1028 | 16,842,752 — B's footprint, A's slot size |
/// | D | 16448 | 1020 | 16,776,960 — A's footprint (−256 B), B's slot size |
///
/// If C is slow and D is fast, the lever is the allocation, not the slot size.
///
/// `RL_MODE=inter` rebuilds every cell each round (re-rolling heap layout);
/// `RL_MODE=fixed` builds each cell once and runs its rounds back to back,
/// which is what the original sweep did.
#[test]
fn alloc_vs_slot() {
    let mode = std::env::var("RL_MODE").unwrap_or_else(|_| "inter".to_string());
    if env_usize("RL_NOTHP", 0) == 1 {
        let ok = disable_thp_for_this_process();
        println!("# PR_SET_THP_DISABLE applied={ok}");
    }
    let rounds = env_usize("RL_ROUNDS", 7);
    let size = env_usize("RL_SIZE", 1024);
    let iters = env_usize("RL_ITERS", 200_000);
    let cells: [(&str, u32, u16); 4] = [
        ("A_16384x1024", 16384, 1024),
        ("B_16448x1024", 16448, 1024),
        ("C_16384x1028", 16384, 1028),
        ("D_16448x1020", 16448, 1020),
    ];
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "# alloc_vs_slot engine={ENGINE} mode={mode} size={size} iters={iters} rounds={rounds} loadavg_before={}",
        lp.trim()
    );

    let pt: Vec<u8> = (0..size)
        .map(|i| (i as u32).wrapping_mul(2654435761) as u8)
        .collect();

    let run = |h: &mut Handshaked, pt: &[u8]| -> f64 {
        let t0 = Instant::now();
        for _ in 0..iters {
            let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, std::hint::black_box(pt))
                .expect("encrypt");
            let (_n, b) = release_only(&mut h.pool, s);
            std::hint::black_box(b);
        }
        t0.elapsed().as_nanos() as f64 / iters as f64
    };

    if mode == "fixed" {
        for (label, slot, count) in cells {
            let mut h = handshaked(count, slot);
            for _ in 0..8 {
                let s = encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
                release_only(&mut h.pool, s);
            }
            for r in 0..rounds {
                let ns = run(&mut h, &pt);
                println!("A\t{ENGINE}\tfixed\t{label}\t{slot}\t{count}\t{r}\t{ns:.3}");
            }
            std::hint::black_box(&h.client);
        }
    } else {
        for r in 0..rounds {
            for (label, slot, count) in cells {
                let mut h = handshaked(count, slot);
                for _ in 0..8 {
                    let s =
                        encrypt_to_sends(&mut h.table, &mut h.pool, 0, 0, &pt).expect("encrypt");
                    release_only(&mut h.pool, s);
                }
                let ns = run(&mut h, &pt);
                println!("A\t{ENGINE}\tinter\t{label}\t{slot}\t{count}\t{r}\t{ns:.3}");
                std::hint::black_box(&h.client);
            }
        }
    }
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("# alloc_vs_slot loadavg_after={}", lp.trim());
    std::io::stdout().flush().ok();
}

/// Is the cliff a property of the slot size, or of *when in the process* a cell
/// is measured?
///
/// [`small_send_alignment`] runs every slot size inside one process, in
/// ascending order, each cell allocating and freeing its own ~16 MiB pool. So
/// 16384 — the baseline every other column is divided by — is always the
/// **first** cell measured, on a fresh heap, and every other column is measured
/// after N previous 16 MiB alloc/free cycles have churned the allocator.
///
/// Each list here measures one slot size **twice, first and last**. If the same
/// slot size differs between the two ends of the same process by about the size
/// of the reported cliff, then position is the effect and slot size is not —
/// and reversing the list moves the penalty onto whichever size now sits last.
///
/// `RL_PREHEAT=1` allocates and frees one pool before the first cell, so
/// position 0 starts from the same allocator state as the others. If that alone
/// flattens the sweep, the confound is allocator state specifically rather than
/// anything about the slot.
#[test]
fn slot_order_control() {
    let order = std::env::var("RL_ORDER").unwrap_or_else(|_| "fwd".to_string());
    let size = env_usize("RL_SIZE", 1024);
    let reps = env_usize("RL_REPS", SEND_REPS);
    let bpr = env_usize("RL_BPR", SEND_BYTES_PER_REP);
    let preheat = env_usize("RL_PREHEAT", 0) == 1;

    // Same sizes as the original sweep; the first size repeats at the end.
    let fwd: [u32; 8] = [16384, 16406, 16428, 16448, 20480, 32768, 65536, 16384];
    let rev: [u32; 8] = [65536, 32768, 20480, 16448, 16428, 16406, 16384, 65536];
    let list = if order == "rev" { rev } else { fwd };

    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!(
        "# slot_order_control engine={ENGINE} order={order} size={size} reps={reps} bytes_per_rep={bpr} preheat={preheat} loadavg_before={}",
        lp.trim()
    );

    if preheat {
        // One alloc/free cycle of the same shape the first cell would see, so
        // position 0 no longer measures a pristine heap.
        let h = handshaked(1024, 16384);
        drop(h);
    }

    for (pos, &slot) in list.iter().enumerate() {
        let r = measure_send2_pool(size, slot, 1024, bpr, reps);
        for (i, ns) in r.iter().enumerate() {
            println!("O\t{ENGINE}\t{order}\t{preheat}\t{pos}\t{slot}\t{i}\t{ns:.3}");
        }
    }
    let lp = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    println!("# slot_order_control loadavg_after={}", lp.trim());
    std::io::stdout().flush().ok();
}
