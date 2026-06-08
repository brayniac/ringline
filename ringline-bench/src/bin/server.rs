//! Standalone echo server for distributed benchmarking.
//!
//! Usage:
//!   bench-server --runtime ringline --addr 0.0.0.0:7878 --workers 4 --msg-size 64
//!   bench-server --runtime tokio --addr 0.0.0.0:7878 --workers 4

use std::net::SocketAddr;

use clap::Parser;

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Runtime {
    Ringline,
    Tokio,
}

#[derive(Parser)]
#[command(
    name = "bench-server",
    about = "Echo server for distributed benchmarking"
)]
struct Args {
    /// Server runtime
    #[arg(long)]
    runtime: Runtime,

    /// Listen address
    #[arg(long, default_value = "0.0.0.0:7878")]
    addr: SocketAddr,

    /// Number of worker threads (0 = available parallelism)
    #[arg(long, default_value_t = 0)]
    workers: usize,

    /// Message size hint for buffer tuning (bytes)
    #[arg(long, default_value_t = 4096)]
    msg_size: usize,

    /// (ringline only) Echo via the multi-buffer zero-copy recv-forward path
    /// (`enable_recv_forward` + `forward_held`): held provided recv buffers are
    /// scatter-gathered into one `sendmsg` with no accumulator copy.
    #[arg(long, default_value_t = false)]
    recv_forward: bool,

    /// (ringline only) Connections assigned to each worker before moving to the next.
    /// 1 = classic round-robin. Higher values pack connections onto fewer workers
    /// at low connection counts, keeping per-worker CQE density high for batching.
    #[arg(long, default_value_t = 1)]
    conn_chunk_size: usize,

    /// Restrict the whole process to these logical CPUs, e.g. `0-7,16-23` or
    /// `12,13,14,15` (the "taskset the task" model). When set, the process
    /// affinity mask is applied before launch and ringline's per-worker core
    /// pinning is disabled (so it doesn't pin to cores outside the mask).
    /// Pass `--workers N` to match the number of physical cores in the list.
    #[arg(long)]
    cpu_list: Option<String>,

    /// (ringline only) Override the recv provided-buffer size in bytes (how many
    /// bytes — i.e. how many pipelined messages — one multishot-recv completion
    /// can carry). 0 = msg_size rounded up, min 4096. Bigger amortizes per-op
    /// cost across more messages (the knob that matches a bulk read()).
    #[arg(long, default_value_t = 0)]
    recv_buf_size: usize,

    /// (ringline only) Number of recv provided buffers in the ring.
    #[arg(long, default_value_t = 256)]
    recv_ring_size: u16,

    /// (ringline only) io_uring submission-queue entries.
    #[arg(long, default_value_t = 256)]
    sq_entries: u32,

    /// Server protocol: `echo` (send received bytes back — zero-copy forward
    /// possible), `respond` (per `msg_size`-byte request, send a server-owned
    /// constant `msg_size`-byte response — realistic request/response, e.g.
    /// PING→PONG at msg_size=6), or `cache` (per request, do a HashMap GET with
    /// a rotating key and return the stored `msg_size`-byte value — a realistic
    /// read-heavy cache hot path: protocol framing + hash lookup + value copy).
    #[arg(long, default_value = "echo")]
    protocol: String,

    /// (cache protocol) number of pre-populated keys.
    #[arg(long, default_value_t = 65536)]
    cache_keys: u64,

    /// (segcache protocol, ringline io_uring) drive connections through the
    /// `run_direct_respond` fast path — the responder (cache lookup + value
    /// copy) runs in the event loop with NO task wakeup. Tests whether the
    /// async task path is the small-value overhead vs tokio.
    #[arg(long, default_value_t = false)]
    direct_respond: bool,

    /// (segcache protocol, ringline) value size (bytes) at/above which to send
    /// zero-copy via a SendGuard; below it, copy the value into the send pool
    /// (plain send). Zero-copy send carries per-send notification/pinning
    /// overhead and the kernel copies small payloads anyway, so copying small
    /// values is faster. Set very high to force copy everywhere.
    #[arg(long, default_value_t = 16384)]
    zc_threshold: usize,
}

/// Fixed server-owned response, set before launch when `--protocol respond`:
/// (request/response unit size, a buffer of that unit repeated). The handler
/// sends `frames * unit` of this per recv, never touching the recv bytes.
static RESPOND: std::sync::OnceLock<(usize, Vec<u8>)> = std::sync::OnceLock::new();

/// Pre-populated cache for `--protocol cache`: (num_keys, request/value unit
/// size, key→value map). Each value is `unit` bytes. The server does a real
/// hash lookup per request (rotating key) and returns the stored value.
static CACHE: std::sync::OnceLock<(u64, usize, std::collections::HashMap<u64, Vec<u8>>)> =
    std::sync::OnceLock::new();

/// Real Segcache (vendored from brayniac/crucible) for `--protocol segcache`:
/// (num_keys, value unit size, cache). GET returns a zero-copy `ValueRef` that
/// borrows segment memory — ringline sends it zero-copy (SendGuard, no copy);
/// tokio must `write` it (one copy). This is the realistic cache hot path where
/// ringline's zero-copy send composes with a borrow-capable cache.
static SEGCACHE: std::sync::OnceLock<(u64, usize, usize, segcache::SegCache)> =
    std::sync::OnceLock::new();

/// When true, the segcache server drives connections via ringline's
/// `run_direct_respond` fast path (responder runs in the event loop, no task
/// wakeup) instead of the `with_data` task loop.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
static DIRECT_RESPOND: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Rotating key state for the direct-respond responder (a plain fn has no
/// per-connection state, so the rotation is global — fine for the benchmark).
#[cfg_attr(not(has_io_uring), allow(dead_code))]
static RESP_CTR: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0x9E3779B97F4A7C15);

/// Direct-respond responder: per `unit`-byte request, look up a rotating key in
/// the segcache and append the stored value to `out`. Runs synchronously in the
/// event loop (no task wakeup); `out` is then copy-sent by the core.
#[cfg_attr(not(has_io_uring), allow(dead_code))]
fn segcache_responder(req: &[u8], out: &mut Vec<u8>) {
    use std::sync::atomic::Ordering;
    let (k, unit, _zc, cache) = match SEGCACHE.get() {
        Some(x) => x,
        None => return,
    };
    let frames = req.len() / unit;
    let mut c = RESP_CTR.load(Ordering::Relaxed);
    for _ in 0..frames {
        c = c.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = (c >> 16) % k;
        if let Some(vref) = cache.get_value_ref(&key.to_le_bytes()) {
            out.extend_from_slice(vref.as_slice());
        }
    }
    RESP_CTR.store(c, Ordering::Relaxed);
}

/// Wraps a Segcache `ValueRef` (Send + 'static, ref-counts the segment) as a
/// ringline `SendGuard` so the value can be sent zero-copy directly from
/// segment memory; the guard keeps the segment alive until the ZC notification.
struct ValueRefGuard(segcache::ValueRef);
impl ringline::SendGuard for ValueRefGuard {
    fn as_ptr_len(&self) -> (*const u8, u32) {
        let s = self.0.as_slice();
        (s.as_ptr(), s.len() as u32)
    }
    fn region(&self) -> ringline::RegionId {
        ringline::RegionId::UNREGISTERED
    }
}

/// Parse a cpu-list spec (`0-7,16-23` / `12,13,14,15`) into logical CPU ids.
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = part.split_once('-') {
            let lo: usize = lo.trim().parse().expect("invalid cpu-list range start");
            let hi: usize = hi.trim().parse().expect("invalid cpu-list range end");
            cpus.extend(lo..=hi);
        } else {
            cpus.push(part.parse().expect("invalid cpu-list entry"));
        }
    }
    cpus
}

/// Pin the current process to `cpus` via `sched_setaffinity` (taskset-equivalent,
/// in-process). Worker threads spawned afterwards inherit this mask.
#[cfg(target_os = "linux")]
fn apply_cpu_affinity(cpus: &[usize]) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if ret != 0 {
            panic!(
                "sched_setaffinity({cpus:?}) failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

/// Pin the *current thread* to a single `core` via `sched_setaffinity`. Used to
/// pin tokio worker threads one-per-core (the runtime does not pin them itself,
/// and on an `isolcpus=...,domain` host the scheduler will not load-balance
/// unpinned threads off the first core — they all pile onto core 0).
#[cfg(target_os = "linux")]
fn pin_thread_to_core(core: usize) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

/// Per-thread CPU affinity is not supported on this platform (no-op).
#[cfg(not(target_os = "linux"))]
fn pin_thread_to_core(_core: usize) {}

/// Process CPU affinity is not supported on this platform (no-op).
#[cfg(not(target_os = "linux"))]
fn apply_cpu_affinity(_cpus: &[usize]) {
    eprintln!("bench-server: --cpu-list ignored (CPU affinity unsupported on this platform)");
}

fn main() {
    let args = Args::parse();

    // Apply process CPU affinity before launch so worker threads inherit it.
    // Disables ringline's own per-worker pinning (see run_ringline) to avoid
    // pinning workers to cores outside the requested mask.
    let pin_to_core = match &args.cpu_list {
        Some(spec) => {
            let cpus = parse_cpu_list(spec);
            assert!(!cpus.is_empty(), "--cpu-list parsed to an empty set");
            apply_cpu_affinity(&cpus);
            eprintln!("bench-server: pinned process to CPUs {cpus:?}");
            false
        }
        None => true,
    };

    // Realistic request/response mode: build the server-owned constant response
    // (PING→PONG at msg_size=6, else a 0xCD-filled unit) and stash it for the
    // handlers. Repeated to 256 KiB so one recv's worth of frames always fits.
    if args.protocol == "respond" {
        let unit = args.msg_size.max(1);
        let pattern: Vec<u8> = if unit == 6 {
            b"PONG\r\n".to_vec()
        } else {
            vec![0xCDu8; unit]
        };
        let reps = (262_144 / unit).max(1);
        let mut buf = Vec::with_capacity(reps * unit);
        for _ in 0..reps {
            buf.extend_from_slice(&pattern);
        }
        RESPOND.set((unit, buf)).ok();
        eprintln!("bench-server: protocol=respond unit={unit}B");
    } else if args.protocol == "cache" {
        let unit = args.msg_size.max(1);
        let k = args.cache_keys.max(1);
        let mut map = std::collections::HashMap::with_capacity(k as usize);
        for key in 0..k {
            // Value = unit bytes seeded by key so values differ across keys.
            map.insert(key, vec![(key & 0xff) as u8; unit]);
        }
        CACHE.set((k, unit, map)).ok();
        eprintln!("bench-server: protocol=cache keys={k} value={unit}B");
    } else if args.protocol == "segcache" {
        let unit = args.msg_size.max(1);
        let k = args.cache_keys.max(1);
        // Heap sized to hold all values comfortably (avoid eviction): ~4x data.
        let heap = ((k as usize * (unit + 96)) * 4).max(256 << 20);
        let cache = segcache::SegCache::builder()
            .heap_size(heap)
            .segment_size(4 << 20)
            .build()
            .expect("segcache build");
        let val = vec![0xCDu8; unit];
        for key in 0..k {
            cache
                .set(
                    &key.to_le_bytes(),
                    &val,
                    std::time::Duration::from_secs(3600),
                )
                .ok();
        }
        SEGCACHE.set((k, unit, args.zc_threshold, cache)).ok();
        DIRECT_RESPOND.store(args.direct_respond, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "bench-server: protocol=segcache keys={k} value={unit}B heap={heap} zc_threshold={} direct_respond={}",
            args.zc_threshold, args.direct_respond
        );
    } else if args.protocol != "echo" {
        panic!("--protocol must be 'echo', 'respond', 'cache', or 'segcache'");
    }

    let workers = if args.workers == 0 {
        ringline::physical_core_count()
    } else {
        args.workers
    };

    let runtime_name = match args.runtime {
        Runtime::Ringline => "ringline",
        Runtime::Tokio => "tokio",
    };

    eprintln!(
        "bench-server: {} runtime, {} workers, listening on {}",
        runtime_name, workers, args.addr,
    );

    match args.runtime {
        Runtime::Ringline => run_ringline(
            args.addr,
            workers,
            args.msg_size,
            args.recv_forward,
            args.conn_chunk_size,
            pin_to_core,
            args.recv_buf_size,
            args.recv_ring_size,
            args.sq_entries,
        ),
        Runtime::Tokio => run_tokio(args.addr, workers, args.msg_size),
    }
}

#[allow(clippy::manual_async_fn)]
#[allow(clippy::too_many_arguments)]
fn run_ringline(
    addr: SocketAddr,
    workers: usize,
    msg_size: usize,
    recv_forward: bool,
    conn_chunk_size: usize,
    pin_to_core: bool,
    recv_buf_size: usize,
    recv_ring_size: u16,
    sq_entries: u32,
) {
    use ringline::{AsyncEventHandler, Config, ConnCtx, ParseResult, RinglineBuilder};

    // Direct-echo path (default): no task wakeup per message — echo SQEs are
    // submitted directly from handle_recv_multi, bypassing collect_wakeups and
    // poll_ready_tasks entirely. Falls back to the forward_recv_buf loop on the
    // mio backend (macOS / non-io_uring builds).
    struct EchoHandler;
    impl AsyncEventHandler for EchoHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                #[cfg(has_io_uring)]
                {
                    conn.run_direct_echo().await;
                    return;
                }
                #[cfg(not(has_io_uring))]
                loop {
                    let n = conn
                        .with_data(|data| {
                            if let Err(e) = conn.forward_recv_buf(data) {
                                eprintln!("echo: forward_recv_buf failed: {e}");
                                return ParseResult::NeedMore;
                            }
                            ParseResult::Consumed(data.len())
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            EchoHandler
        }
    }

    // Multi-buffer zero-copy recv-forward path: hold provided recv buffers and
    // scatter-gather them back in one sendmsg — no accumulator copy at all.
    struct RecvForwardEchoHandler;
    impl AsyncEventHandler for RecvForwardEchoHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                conn.enable_recv_forward();
                loop {
                    conn.recv_ready().await;
                    let n = match conn.forward_held() {
                        Ok(f) => f.await.unwrap_or(0),
                        Err(_) => break,
                    };
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            RecvForwardEchoHandler
        }
    }

    // Realistic request/response path: for every `unit`-byte request, send a
    // server-owned constant `unit`-byte response (never the recv bytes — so
    // recv-forward / zero-copy echo does NOT apply; this is the normal
    // copy-into-send-pool path real protocols use). PING→PONG at unit=6.
    struct RespondHandler;
    impl AsyncEventHandler for RespondHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let (unit, resp) = RESPOND.get().expect("RESPOND set");
                let max_frames = resp.len() / unit;
                loop {
                    let n = conn
                        .with_data(|data| {
                            let frames = (data.len() / unit).min(max_frames);
                            if frames == 0 {
                                return ParseResult::NeedMore;
                            }
                            if let Err(e) = conn.send_nowait(&resp[..frames * unit]) {
                                eprintln!("respond: send failed: {e}");
                                return ParseResult::NeedMore;
                            }
                            ParseResult::Consumed(frames * unit)
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            RespondHandler
        }
    }

    // Realistic read-heavy cache hot path: per `unit`-byte request, do a HashMap
    // GET with a rotating (pseudo-random) key and return the stored value. Real
    // application work (framing + hash lookup + value gather) — not echo, not a
    // constant. Responses for a recv's frames are gathered into one send.
    struct CacheHandler;
    impl AsyncEventHandler for CacheHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let (k, unit, map) = CACHE.get().expect("CACHE set");
                let mut ctr: u64 = (conn.index() as u64).wrapping_mul(0x9E3779B97F4A7C15) | 1;
                let mut respbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
                loop {
                    let n = conn
                        .with_data(|data| {
                            let frames = data.len() / unit;
                            if frames == 0 {
                                return ParseResult::NeedMore;
                            }
                            respbuf.clear();
                            for _ in 0..frames {
                                // LCG step → pseudo-random key (realistic cache
                                // access pattern / cache-line spread).
                                ctr = ctr.wrapping_mul(6364136223846793005).wrapping_add(1);
                                let key = (ctr >> 16) % k;
                                if let Some(v) = map.get(&key) {
                                    respbuf.extend_from_slice(v);
                                }
                            }
                            if let Err(e) = conn.send_nowait(&respbuf) {
                                eprintln!("cache: send failed: {e}");
                                return ParseResult::NeedMore;
                            }
                            ParseResult::Consumed(frames * unit)
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            CacheHandler
        }
    }

    // Zero-copy Segcache GET: per request, look up a rotating key, get a
    // `ValueRef` borrowing segment memory, and send it ZERO-COPY via a
    // SendGuard (the value never leaves segment memory until the kernel's ZC
    // notification). tokio must copy the value into the kernel on write.
    use ringline::GuardBox;
    struct SegcacheHandler;
    impl AsyncEventHandler for SegcacheHandler {
        fn on_accept(&self, conn: ConnCtx) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                // Fast path: run the responder in the event loop with no task
                // wakeup (io_uring only). Bypasses the with_data task loop below.
                #[cfg(has_io_uring)]
                if DIRECT_RESPOND.load(std::sync::atomic::Ordering::Relaxed) {
                    conn.run_direct_respond(segcache_responder).await;
                    return;
                }
                let (k, unit, zc_threshold, cache) = SEGCACHE.get().expect("SEGCACHE set");
                let mut ctr: u64 = (conn.index() as u64).wrapping_mul(0x9E3779B97F4A7C15) | 1;
                // Send-copy slot size = how many gathered values fit in one
                // batched copy send (matches the recv-buffer / slot sizing).
                let slot = unit.next_power_of_two().max(4096);
                let cap_frames = (slot / unit).max(1);
                let mut respbuf: Vec<u8> = Vec::with_capacity(slot);
                loop {
                    let n = conn
                        .with_data(|data| {
                            if *zc_threshold <= *unit {
                                // Large value: one zero-copy send per request
                                // (typically ~1 frame per recv at this size).
                                let frames = data.len() / unit;
                                if frames == 0 {
                                    return ParseResult::NeedMore;
                                }
                                for _ in 0..frames {
                                    ctr = ctr.wrapping_mul(6364136223846793005).wrapping_add(1);
                                    let key = (ctr >> 16) % k;
                                    if let Some(vref) = cache.get_value_ref(&key.to_le_bytes()) {
                                        let g = ValueRefGuard(vref);
                                        let _ = conn
                                            .send_parts()
                                            .build(move |b| b.guard(GuardBox::new(g)).submit());
                                    }
                                }
                                ParseResult::Consumed(frames * unit)
                            } else {
                                // Small value: GATHER a recv's values into one
                                // copy send (matches tokio's gathered write),
                                // amortizing per-send overhead. Cap to one slot.
                                let frames = (data.len() / unit).min(cap_frames);
                                if frames == 0 {
                                    return ParseResult::NeedMore;
                                }
                                respbuf.clear();
                                for _ in 0..frames {
                                    ctr = ctr.wrapping_mul(6364136223846793005).wrapping_add(1);
                                    let key = (ctr >> 16) % k;
                                    if let Some(vref) = cache.get_value_ref(&key.to_le_bytes()) {
                                        respbuf.extend_from_slice(vref.as_slice());
                                    }
                                }
                                if !respbuf.is_empty()
                                    && let Err(e) = conn.send_nowait(&respbuf)
                                {
                                    eprintln!("segcache: batched copy send failed: {e}");
                                }
                                ParseResult::Consumed(frames * unit)
                            }
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            SegcacheHandler
        }
    }

    let mut config = Config::default();
    config.worker.threads = workers;
    // When --cpu-list set a process affinity mask, leave the OS to schedule
    // workers within it; otherwise pin each worker to its own core (0..N).
    config.worker.pin_to_core = pin_to_core;
    config.sq_entries = sq_entries;
    config.recv_buffer.ring_size = recv_ring_size;
    let recv_buf = if recv_buf_size > 0 {
        recv_buf_size
    } else {
        msg_size.next_power_of_two().max(4096)
    };
    config.recv_buffer.buffer_size = recv_buf as u32;
    config.max_connections = 16384;
    // Send-copy pool must hold all in-flight copy sends. Small values produce
    // many sends per recv (e.g. 16 at 256 B into a 4 KiB recv buffer) across
    // hundreds of connections, so 512 slots exhaust and copy sends fail. Size
    // generously for the copy path (respond/cache/segcache-small).
    config.send_copy_count = 16384;
    config.send_copy_slot_size = msg_size.next_power_of_two().max(4096) as u32;
    config.conn_chunk_size = conn_chunk_size;

    let builder = RinglineBuilder::new(config).bind(addr);
    let respond = RESPOND.get().is_some();
    let cache = CACHE.get().is_some();
    let segcache = SEGCACHE.get().is_some();
    let (shutdown, handles) = if segcache {
        builder.launch::<SegcacheHandler>()
    } else if cache {
        builder.launch::<CacheHandler>()
    } else if respond {
        builder.launch::<RespondHandler>()
    } else if recv_forward {
        builder.launch::<RecvForwardEchoHandler>()
    } else {
        builder.launch::<EchoHandler>()
    }
    .expect("failed to launch ringline server");

    eprintln!("bench-server: ready (recv_forward={recv_forward} respond={respond})");

    // Block until SIGINT/SIGTERM, then trigger graceful shutdown so each
    // worker's event loop runs its shutdown path — including the
    // `[ringline diag]`/`[ringline stall]` counter dump. (A SIGKILL at
    // teardown skips that, hiding the server-side loop diagnostics.)
    shutdown.wait_on_signal();

    for h in handles {
        h.join().ok();
    }
}

fn run_tokio(addr: SocketAddr, workers: usize, msg_size: usize) {
    // Pin each tokio worker thread to its own core (worker i -> core i). tokio's
    // multi_thread runtime does not pin threads; without this, on an
    // `isolcpus=...,domain` host the unpinned threads all collapse onto core 0
    // (the scheduler does not balance across the isolated domain). This mirrors
    // ringline's per-worker pinning so both runtimes get the same core budget.
    let next_core = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let nc = next_core.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .on_thread_start(move || {
            let id = nc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            pin_thread_to_core(id % workers);
        })
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(async move {
        let socket = tokio::net::TcpSocket::new_v4().expect("failed to create socket");
        socket.set_reuseaddr(true).expect("failed to set reuseaddr");
        socket.bind(addr).expect("failed to bind");
        let listener = socket.listen(1024).expect("failed to listen");

        eprintln!("bench-server: ready");

        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => continue,
            };
            stream.set_nodelay(true).ok();

            // `cache`: HashMap GET per request, return stored value (gathered).
            // `respond`: per request, write a server-owned constant response.
            // else bulk byte-echo. All three are the fair tokio counterparts to
            // ringline's handlers.
            let respond = RESPOND.get();
            let cache = CACHE.get();
            let segcache = SEGCACHE.get();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let cap = msg_size.next_power_of_two().max(65536);
                let mut buf = vec![0u8; cap];
                let mut respbuf: Vec<u8> = Vec::with_capacity(64 * 1024);
                let mut ctr: u64 = 0x9E3779B97F4A7C15;
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    let ok = if let Some((k, unit, _zc, sc)) = segcache {
                        // Same zero-copy GET (ValueRef borrow), but tokio must
                        // copy the bytes into the kernel on write.
                        let frames = n / unit;
                        respbuf.clear();
                        for _ in 0..frames {
                            ctr = ctr.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let key = (ctr >> 16) % k;
                            if let Some(vref) = sc.get_value_ref(&key.to_le_bytes()) {
                                respbuf.extend_from_slice(vref.as_slice());
                            }
                        }
                        stream.write_all(&respbuf).await.is_ok()
                    } else if let Some((k, unit, map)) = cache {
                        let frames = n / unit;
                        respbuf.clear();
                        for _ in 0..frames {
                            ctr = ctr.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let key = (ctr >> 16) % k;
                            if let Some(v) = map.get(&key) {
                                respbuf.extend_from_slice(v);
                            }
                        }
                        stream.write_all(&respbuf).await.is_ok()
                    } else if let Some((unit, resp)) = respond {
                        let frames = (n / unit).min(resp.len() / unit);
                        stream.write_all(&resp[..frames * unit]).await.is_ok()
                    } else {
                        stream.write_all(&buf[..n]).await.is_ok()
                    };
                    if !ok {
                        break;
                    }
                }
            });
        }
    });
}
