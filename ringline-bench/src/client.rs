use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::stats::{BenchResult, ConnCounts, LatencyHistogram, LatencyStats, process_cpu_time_ns};

/// `send_nowait` calls refused because the ringline client's send pool was
/// full, since the process started. In open loop a refused request is sent
/// later, behind schedule, and the delay is in the measured latency; in
/// closed loop a refused send ends that connection.
static SEND_POOL_FULL: AtomicU64 = AtomicU64::new(0);

/// Counts `e` in [`SEND_POOL_FULL`] if the pool refused the send. A closed
/// connection or an oversized send is not a full pool.
fn count_refused_send(e: &std::io::Error) {
    if e.kind() == std::io::ErrorKind::Other {
        SEND_POOL_FULL.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn send_pool_full() -> u64 {
    SEND_POOL_FULL.load(Ordering::Relaxed)
}

/// Shared state of an open-loop run whose offered rate changes while its
/// connections stay open ([`LiveOpenLoop`]). Each connection reads `rate`
/// again when `generation` changes, and records every response's
/// coordinated-omission-corrected time into `hist`.
pub struct LiveCtl {
    rate: AtomicU64,
    /// Incremented on every rate change. A connection that sees a new value
    /// stops sending until its in-flight requests have been answered.
    generation: AtomicU64,
    /// The generation whose schedule connections may start sending on.
    go: AtomicU64,
    /// Connections whose in-flight requests have all been answered since
    /// the last rate change: the generation in the high 32 bits, the count
    /// in the low 32.
    drained: AtomicU64,
    hist: histogram::AtomicHistogram,
    /// Connections currently open.
    conns: AtomicU64,
}

const DRAINED_COUNT: u64 = 0xffff_ffff;

impl LiveCtl {
    fn new(rate: u64) -> Self {
        Self {
            rate: AtomicU64::new(rate),
            generation: AtomicU64::new(0),
            go: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            // 2^-7 relative bucket width (under 1% error), values to 2^64 ns.
            hist: histogram::AtomicHistogram::new(7, 64).expect("valid histogram config"),
            conns: AtomicU64::new(0),
        }
    }

    /// Undoes [`mark_drained`](Self::mark_drained) for a connection that
    /// closed. Has no effect once the controller has moved past `generation`.
    fn unmark_drained(&self, generation: u64) {
        let _ = self
            .drained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v >> 32 == generation & DRAINED_COUNT && v & DRAINED_COUNT > 0).then(|| v - 1)
            });
    }

    /// Counts one connection as drained for `generation`. A connection that
    /// reports for a generation the controller has moved past is not counted.
    fn mark_drained(&self, generation: u64) {
        let _ = self
            .drained
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                (v >> 32 == generation & DRAINED_COUNT).then_some(v + 1)
            });
    }
}

/// Open-loop load configuration.
///
/// `rate` is the aggregate offered requests/sec across all connections (split
/// evenly per connection). `max_inflight` bounds outstanding requests per
/// connection: when it is reached the sender stalls and falls behind its fixed
/// schedule, so overload manifests as climbing latency — measured from the
/// *scheduled* send time (coordinated-omission-free) — rather than a hang.
#[derive(Clone, Copy)]
pub struct OpenLoop {
    pub rate: u64,
    pub max_inflight: usize,
}

// ── Async (tokio) client — closed loop ──────────────────────────────

async fn run_tokio_client(
    addr: String,
    msg_size: usize,
    depth: usize,
    stop: Arc<AtomicBool>,
    ops_counter: Arc<AtomicU64>,
) -> LatencyHistogram {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let depth = depth.max(1);
    let batch = vec![0xABu8; msg_size * depth];
    let mut recv_buf = vec![0u8; msg_size * depth];
    let mut histogram = LatencyHistogram::new();

    let mut stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  client connect failed: {e}");
            return histogram;
        }
    };
    stream.set_nodelay(true).ok();

    let mut local_ops: u64 = 0;

    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();

        if stream.write_all(&batch).await.is_err() {
            break;
        }

        // Responses arrive in order, so response `i` is complete once
        // `(i+1) * msg_size` bytes have been read. Each is recorded against the
        // single send time, because all `depth` requests were in flight from
        // that moment.
        let mut total_read = 0;
        let mut recorded = 0usize;
        while total_read < batch.len() {
            match stream.read(&mut recv_buf[total_read..]).await {
                Ok(0) => return histogram,
                Ok(n) => total_read += n,
                Err(_) => return histogram,
            }
            while recorded < depth && (recorded + 1) * msg_size <= total_read {
                histogram.record(t0.elapsed().as_nanos() as u64);
                recorded += 1;
            }
        }

        local_ops += depth as u64;
        if local_ops >= 256 {
            ops_counter.fetch_add(local_ops & !0xFF, Ordering::Relaxed);
            local_ops &= 0xFF;
        }
    }

    ops_counter.fetch_add(local_ops & 0xFF, Ordering::Relaxed);
    histogram
}

// ── Async (tokio) client — open loop ────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn run_tokio_open_client(
    addr: String,
    msg_size: usize,
    per_conn_rate: f64,
    max_inflight: usize,
    measure: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    ops_counter: Arc<AtomicU64>,
    svc_out: Arc<Mutex<Vec<Vec<u64>>>>,
    lag_out: Arc<Mutex<Vec<Vec<u64>>>>,
) -> LatencyHistogram {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  client connect failed: {e}");
            return LatencyHistogram::new();
        }
    };
    stream.set_nodelay(true).ok();
    let (mut rd, mut wr) = stream.into_split();

    let interval = 1.0 / per_conn_rate; // seconds between sends on this connection
    // Each in-flight request carries (scheduled time, actual send time) so the
    // reader can decompose the response time into offer-lag (send − scheduled,
    // the coordinated-omission/back-pressure component) and service time
    // (recv − actual send, the true wire+server latency).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(Instant, Instant)>();
    let received = Arc::new(AtomicU64::new(0));

    // Reader task: drains responses, matches each to its scheduled send time
    // (FIFO), and records coordinated-omission-free latency while measuring.
    let reader = {
        let measure = measure.clone();
        let ops_counter = ops_counter.clone();
        let received = received.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut histogram = LatencyHistogram::new();
            let mut svc_local: Vec<u64> = Vec::new();
            let mut lag_local: Vec<u64> = Vec::new();
            let mut buf = vec![0u8; msg_size];
            let mut local_ops: u64 = 0;
            loop {
                // Read one full response, but bail promptly once stop is set
                // (the measurement window has ended; no need to drain in-flight).
                let read_one = async {
                    let mut total_read = 0;
                    while total_read < msg_size {
                        match rd.read(&mut buf[total_read..]).await {
                            Ok(0) => return false,
                            Ok(n) => total_read += n,
                            Err(_) => return false,
                        }
                    }
                    true
                };
                tokio::select! {
                    biased;
                    ok = read_one => { if !ok { break; } }
                    _ = async {
                        while !stop.load(Ordering::Relaxed) {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    } => { break; }
                }
                let (sched, sent_at) = match rx.recv().await {
                    Some(s) => s,
                    None => break,
                };
                received.fetch_add(1, Ordering::Relaxed);
                if measure.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    // CO-corrected response time (recv − scheduled).
                    histogram.record(now.saturating_duration_since(sched).as_nanos() as u64);
                    // True service time (recv − actual send).
                    svc_local.push(now.saturating_duration_since(sent_at).as_nanos() as u64);
                    // Offer lag (actual send − scheduled): the CO/back-pressure part.
                    lag_local.push(sent_at.saturating_duration_since(sched).as_nanos() as u64);
                    local_ops += 1;
                    if local_ops & 0xFF == 0 {
                        ops_counter.fetch_add(256, Ordering::Relaxed);
                    }
                }
            }
            svc_out.lock().unwrap().push(svc_local);
            lag_out.lock().unwrap().push(lag_local);
            histogram
        })
    };

    // Sender: pace to each request's scheduled time, coalescing all currently-due
    // requests into ONE write to match ringline's send coalescing (so the two
    // clients are syscall-for-syscall comparable). When keeping up we sleep until
    // the next request is due; when behind we send a full batch to catch up,
    // bounded by in-flight (overload → fall behind → climbing
    // coordinated-omission-free latency). Messages are a constant byte, so one
    // preallocated buffer serves any batch.
    const TOKIO_SEND_BATCH: usize = 32;
    let src_msg = vec![0xABu8; msg_size];
    let mut batch_buf = vec![0u8; msg_size * TOKIO_SEND_BATCH];
    let start = Instant::now();
    let mut sent: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let elapsed = start.elapsed().as_secs_f64();
        // +1 so the first request is due immediately at T=0 (sent=0 → target=start),
        // matching the ringline client which also fires the first request at T=0.
        // Without this, due=0 at T=0 so the first send is delayed one full interval,
        // inflating all latency measurements by exactly 1/per_conn_rate seconds.
        let due = (elapsed / interval) as u64 + 1;
        let inflight = sent.saturating_sub(received.load(Ordering::Relaxed));
        let room = max_inflight.saturating_sub(inflight as usize) as u64;
        let want = due
            .saturating_sub(sent)
            .min(room)
            .min(TOKIO_SEND_BATCH as u64) as usize;
        if want == 0 {
            if (inflight as usize) >= max_inflight {
                tokio::time::sleep(Duration::from_micros(50)).await; // in-flight full
            } else {
                let target = start + Duration::from_secs_f64(interval * sent as f64);
                let now = Instant::now();
                if now < target {
                    tokio::time::sleep(target - now).await;
                } else {
                    tokio::time::sleep(Duration::from_micros(10)).await; // spin guard
                }
            }
            continue;
        }
        // Copy each message into the send buffer before writing — fair vs the
        // ringline client, whose send_nowait copies each message into the send
        // pool. (Measured: this does not move throughput at any payload size, so
        // the copy is not what differentiates the runtimes — but it removes the
        // confound so the comparison is defensible.)
        for k in 0..want {
            batch_buf[k * msg_size..(k + 1) * msg_size].copy_from_slice(&src_msg);
        }
        if wr.write_all(&batch_buf[..want * msg_size]).await.is_err() {
            stop.store(true, Ordering::Relaxed);
            break;
        }
        // Timestamp the actual completion of the write — shared by the whole
        // batch. This is the "send" point for service-time accounting; the gap
        // from each request's scheduled time is its offer lag.
        let sent_at = Instant::now();
        let mut failed = false;
        for k in 0..want as u64 {
            let sched = start + Duration::from_secs_f64(interval * (sent + k) as f64);
            if tx.send((sched, sent_at)).is_err() {
                stop.store(true, Ordering::Relaxed);
                failed = true;
                break;
            }
        }
        if failed {
            break;
        }
        sent += want as u64;
    }
    drop(tx);
    reader.await.unwrap_or_else(|_| LatencyHistogram::new())
}

// ── Async (ringline) client ─────────────────────────────────────────

mod ringline_client {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use ringline::{
        AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder, Runtime,
        connect, sleep, spawn,
    };

    use super::{LiveCtl, OpenLoop};

    /// A fraction in [0, 1) that differs per call: successive multiples of
    /// the golden-ratio conjugate, which stay evenly spread for any count.
    fn schedule_phase() -> f64 {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let k = NEXT.fetch_add(1, Ordering::Relaxed);
        (k as f64 * 0.618_033_988_749_894_9).fract()
    }

    /// Shared state passed into ringline worker tasks.
    pub struct ClientState {
        pub target: std::net::SocketAddr,
        pub msg_size: usize,
        /// Total client connections (used to split the aggregate open-loop rate).
        pub num_clients: usize,
        /// Closed-loop pipeline depth: requests sent per batch, each recorded
        /// against the batch's send time. 1 is send-one-wait-one.
        pub depth: usize,
        /// Number of ringline worker threads. Used by each worker's `on_start`
        /// to compute its chunk-based slice of connections.
        pub num_workers: usize,
        /// Connections assigned to each worker before moving to the next.
        /// Mirrors `Config::conn_chunk_size` on the server side.
        pub conn_chunk_size: usize,
        pub stop: Arc<AtomicBool>,
        /// Set once warmup ends and cleared when the measured window ends;
        /// responses are recorded only while it is set, in both modes.
        pub measure: Arc<AtomicBool>,
        pub ops: Arc<AtomicU64>,
        pub histograms: Arc<Mutex<Vec<Vec<u64>>>>,
        /// `Some` => open-loop (rate-controlled); `None` => closed-loop.
        pub open: Option<OpenLoop>,
        /// Open-loop decomposition (per-connection sample vecs, ns). `svc` =
        /// service time (recv - actual send_nowait); `lag` = offer lag
        /// (actual send - scheduled), the coordinated-omission/back-pressure
        /// component. Empty for closed-loop. The existing `histograms` keeps the
        /// CO-corrected response time (recv - scheduled).
        pub svc: Arc<Mutex<Vec<Vec<u64>>>>,
        pub lag: Arc<Mutex<Vec<Vec<u64>>>>,
        /// `Some` => open loop whose rate changes in place ([`super::LiveOpenLoop`]);
        /// responses go to its histogram and the sample vecs stay empty.
        pub live: Option<Arc<super::LiveCtl>>,
    }

    struct ClientHandler {
        state: Arc<ClientState>,
        /// Index of the worker thread owning this handler.
        /// Used to compute this worker's slice of connections in `on_start`.
        worker_id: usize,
    }

    #[allow(clippy::manual_async_fn)]
    impl AsyncEventHandler for ClientHandler {
        fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
            async {}
        }

        fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
            let state = self.state.clone();
            let worker_id = self.worker_id;
            Some(Box::pin(async move {
                // Chunk-based assignment: this worker owns connections in slices
                // of `conn_chunk_size` starting at `worker_id * chunk_size`,
                // then wrapping every `num_workers * chunk_size` indices.
                //
                // With chunk_size=1 this is identical to the previous round-robin
                // (worker_id, worker_id+N, worker_id+2N, ...). With chunk_size=32
                // and 8 workers: worker 0 owns 0–31, worker 1 owns 32–63, etc.,
                // keeping each active worker's connection density high enough for
                // io_uring CQE batching to pay off at low total connection counts.
                let num_workers = state.num_workers;
                let num_clients = state.num_clients;
                let chunk = state.conn_chunk_size.max(1);
                let mut chunk_start = worker_id * chunk;
                while chunk_start < num_clients {
                    for k in 0..chunk {
                        let idx = chunk_start + k;
                        if idx >= num_clients {
                            break;
                        }
                        let s = state.clone();
                        match s.open {
                            Some(ol) => {
                                if let Err(e) = spawn(async move {
                                    run_ringline_open_client(s, ol).await;
                                }) {
                                    eprintln!("  ringline client spawn failed: {e}");
                                }
                            }
                            None => {
                                if let Err(e) = spawn(async move {
                                    run_ringline_client(s).await;
                                }) {
                                    eprintln!("  ringline client spawn failed: {e}");
                                }
                            }
                        }
                    }
                    chunk_start += num_workers * chunk;
                }
            }))
        }

        fn create_for_worker(worker_id: usize) -> Self {
            let state = GLOBAL_STATE
                .lock()
                .unwrap()
                .as_ref()
                .expect("client state not set")
                .clone();
            ClientHandler { state, worker_id }
        }
    }

    static GLOBAL_STATE: Mutex<Option<Arc<ClientState>>> = Mutex::new(None);

    /// Send pool slots per client worker. A connection holds a slot for each
    /// send the kernel has not completed; when the pool is full a send is
    /// refused and counted in [`super::send_pool_full`].
    const SEND_POOL_SLOTS: u16 = 8192;

    async fn run_ringline_client(state: Arc<ClientState>) {
        let msg_size = state.msg_size;

        let conn = match connect(state.target).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  ringline client connect failed: {e}");
                return;
            }
        };
        // Reading an outbound connection goes through its read half.
        let (mut conn_tx, mut conn_rx) = conn.split();

        let mut rec = ConnRecord::new(state.clone());

        // Pipeline depth: `depth` requests go out in one send, then their
        // responses are matched in order. At depth 1 this is byte-identical to
        // the previous send-one-wait-one loop. Above 1 it is what makes the
        // syscall-amortization claim measurable in closed loop — `--open`'s
        // `max_inflight` only covers the rate-controlled mode.
        let depth = state.depth.max(1);
        let batch = vec![0xABu8; msg_size * depth];

        while !state.stop.load(Ordering::Relaxed) {
            let t0 = Instant::now();

            if let Err(e) = conn_tx.send_nowait(&batch) {
                super::count_refused_send(&e);
                break;
            }

            // Responses come back in order, so response `i` is complete once
            // `(i+1) * msg_size` bytes have arrived. Each is recorded against
            // the single send time — all `depth` requests genuinely were in
            // flight from that moment.
            let mut got = 0usize;
            let mut recorded = 0usize;
            while got < batch.len() {
                let remaining = batch.len() - got;
                let consumed = conn_rx
                    .with_data(|data| {
                        let take = data.len().min(remaining);
                        ParseResult::Consumed(take)
                    })
                    .await;
                if consumed == 0 {
                    return; // EOF; `rec` reports on drop
                }
                got += consumed;
                // Only responses completed after warmup are recorded, so
                // `total_ops` and the latency samples cover the same window.
                let measuring = state.measure.load(Ordering::Relaxed);
                while recorded < depth && (recorded + 1) * msg_size <= got {
                    if measuring {
                        rec.record(t0.elapsed().as_nanos() as u64);
                    }
                    recorded += 1;
                }
            }
            debug_assert_eq!(recorded, depth);
        }
        // `rec` reports on drop.
    }

    /// Open-loop ringline client. The connecting task is the *reader* (recv
    /// wake targets the owning task), and a separate spawned task is the
    /// *sender* (fire-and-forget `send_nowait`, never awaits recv).
    async fn run_ringline_open_client(state: Arc<ClientState>, ol: OpenLoop) {
        let conn = match ringline::connect(state.target).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("  ringline client connect failed: {e}");
                return;
            }
        };
        // Reading an outbound connection goes through its read half.
        let (mut conn_tx, mut conn_rx) = conn.split();
        let live: Option<&LiveCtl> = state.live.as_deref();
        if let Some(l) = live {
            l.conns.fetch_add(1, Ordering::Relaxed);
        }
        let mut rec = ConnRecord::new(state.clone());
        rec.svc.reserve(1_000_000);
        rec.lag.reserve(1_000_000);

        let msg_size = state.msg_size;
        let msg = vec![0xABu8; msg_size];
        let num_clients = state.num_clients.max(1) as f64;
        // Below 1/s per connection is allowed: 1 MiB echo at 1000 connections
        // saturates at a few hundred requests/s in total.
        // With a live controller the first pass of the loop sees a changed
        // generation, reports this connection drained and waits for `go`, so
        // a connection opened during a drain does not hold it up.
        let mut generation = u64::MAX;
        // Between a rate change and `go`: send nothing, receive what is in
        // flight.
        let mut waiting = false;
        let rate = live.map_or(ol.rate, |l| l.rate.load(Ordering::Relaxed));
        let mut interval = num_clients / rate.max(1) as f64;

        // Single interleaved task: send a bounded burst of due requests, then do
        // one recv (the await is the yield point that lets other connections on
        // this single-threaded worker run). A separate sender task would starve
        // the reader on the same worker, so we keep both in one task. `fifo`
        // holds scheduled send-instants for in-flight requests (FIFO = echo
        // response order).
        const SEND_BURST: usize = 256;
        // Spread the connections' schedules across one interval. Without this
        // every connection's first request is due when it connects, and at
        // rates below a few per second per connection they all send together.
        let phase = schedule_phase();
        let mut start = Instant::now() + Duration::from_secs_f64(interval * phase);
        // Requests sent since `start`; the next is due at
        // `start + interval * sent`.
        let mut sent: u64 = 0;
        let mut fifo: VecDeque<(Instant, Instant)> = VecDeque::new();
        // Bytes received toward the next not-yet-complete response, carried
        // across recv wakes (responses are a fixed msg_size byte stream).
        let mut partial: usize = 0;

        'outer: loop {
            // Exit promptly once stopped — the measurement window is over, so
            // there's no value in draining in-flight responses, and waiting to
            // drain risks the task being force-dropped at shutdown before it can
            // flush its recorded samples.
            if state.stop.load(Ordering::Relaxed) {
                break;
            }

            // A rate change stops sending until this connection's in-flight
            // requests are answered, so they do not queue ahead of the next
            // step's. Once every connection has drained (`go`), a new schedule
            // starts from now, at the same phase within the new interval; the
            // backlog behind the old schedule is dropped.
            if let Some(l) = live {
                let g = l.generation.load(Ordering::Acquire);
                if g != generation {
                    generation = g;
                    waiting = true;
                }
                if waiting && fifo.is_empty() {
                    if rec.drained_for != Some(generation) {
                        l.mark_drained(generation);
                        rec.drained_for = Some(generation);
                    }
                    if l.go.load(Ordering::Acquire) < generation {
                        sleep(Duration::from_millis(1)).await;
                        continue;
                    }
                    waiting = false;
                    interval = num_clients / l.rate.load(Ordering::Relaxed).max(1) as f64;
                    start = Instant::now() + Duration::from_secs_f64(interval * phase);
                    sent = 0;
                }
            }

            // Send all currently-due requests, bounded by in-flight and a burst
            // cap so we return to recv frequently.
            let mut burst = 0;
            let mut pool_full = false;
            while !waiting && fifo.len() < ol.max_inflight && burst < SEND_BURST {
                let target = start + Duration::from_secs_f64(interval * sent as f64);
                if Instant::now() < target {
                    break; // next request not due yet
                }
                if let Err(e) = conn_tx.send_nowait(&msg) {
                    if e.kind() == std::io::ErrorKind::NotConnected {
                        break 'outer; // the connection is closing
                    }
                    super::count_refused_send(&e);
                    pool_full = true;
                    break; // send pool full; drain responses first
                }
                fifo.push_back((target, Instant::now()));
                sent += 1;
                burst += 1;
            }

            if fifo.is_empty() {
                if pool_full {
                    // Behind but the send pool is exhausted with nothing of ours
                    // in flight to recv. Yield so in-flight sends (other conns)
                    // complete and free the pool — without this the loop
                    // busy-spins on failed sends and starves completion
                    // processing, so the pool never drains (overload collapse).
                    sleep(Duration::from_micros(50)).await;
                } else {
                    // Caught up: sleep until the next request is due, at most
                    // 100 ms at a time, inside the 200 ms the runner waits
                    // after `stop`. A task still asleep at shutdown is dropped
                    // with its samples unrecorded, which at low
                    // per-connection rates loses most of the run. A shorter
                    // cap costs a timer wake per connection per cap: 2M/s at
                    // 10,000 connections and 5 ms.
                    let target = start + Duration::from_secs_f64(interval * sent as f64);
                    let now = Instant::now();
                    if now < target {
                        sleep((target - now).min(Duration::from_millis(100))).await;
                    } else {
                        sleep(Duration::from_micros(10)).await; // spin guard
                    }
                }
                continue;
            }

            // Drain every complete response available in one wake (blocks for at
            // least one byte; the await is this task's yield point). Consuming
            // the whole accumulator per wake — instead of one response per loop
            // iteration — is what lets the generator keep up at high rates. With
            // a fixed msg_size, the number of newly-complete responses is
            // (partial + consumed) / msg_size; the leftover is carried in
            // `partial`.
            let consumed = conn_rx
                .with_data(|data| ParseResult::Consumed(data.len()))
                .await;
            if consumed == 0 {
                break 'outer; // EOF
            }
            partial += consumed;
            let completed = partial / msg_size;
            partial -= completed * msg_size;
            let measuring = state.measure.load(Ordering::Relaxed);
            for _ in 0..completed {
                let Some((sched, sent_at)) = fifo.pop_front() else {
                    continue;
                };
                if let Some(l) = live {
                    let ns = sched.elapsed().as_nanos() as u64;
                    l.hist.increment(ns).ok();
                }
                if measuring {
                    let now = Instant::now();
                    rec.svc
                        .push(now.saturating_duration_since(sent_at).as_nanos() as u64);
                    rec.lag
                        .push(sent_at.saturating_duration_since(sched).as_nanos() as u64);
                    rec.record(now.saturating_duration_since(sched).as_nanos() as u64);
                }
            }
        }
        // `rec` reports on drop.
    }

    /// One connection's measurements. They are reported when this is
    /// dropped, so a connection whose task is dropped at shutdown while it
    /// waits for a response still counts the responses it did get.
    struct ConnRecord {
        state: Arc<ClientState>,
        samples: Vec<u64>,
        svc: Vec<u64>,
        lag: Vec<u64>,
        local_ops: u64,
        /// The live generation this connection has been counted drained
        /// for, if any.
        drained_for: Option<u64>,
    }

    impl ConnRecord {
        fn new(state: Arc<ClientState>) -> Self {
            Self {
                state,
                samples: Vec::with_capacity(1_000_000),
                svc: Vec::new(),
                lag: Vec::new(),
                local_ops: 0,
                drained_for: None,
            }
        }

        /// Record one response's latency and count it in `state.ops`, in
        /// steps of 256 to limit contention on the shared counter.
        fn record(&mut self, ns: u64) {
            self.samples.push(ns);
            self.local_ops += 1;
            if self.local_ops & 0xFF == 0 {
                self.state.ops.fetch_add(256, Ordering::Relaxed);
            }
        }
    }

    impl Drop for ConnRecord {
        fn drop(&mut self) {
            let state = &self.state;
            if let Some(l) = state.live.as_deref() {
                // A connection that closes mid-drain leaves the drained count
                // before the open count, so the controller never counts it
                // drained but not open and releases while another
                // connection still has requests in flight.
                if let Some(g) = self.drained_for {
                    l.unmark_drained(g);
                }
                l.conns.fetch_sub(1, Ordering::Release);
            }
            state
                .ops
                .fetch_add(self.local_ops & 0xFF, Ordering::Relaxed);
            state
                .histograms
                .lock()
                .unwrap()
                .push(std::mem::take(&mut self.samples));
            state
                .svc
                .lock()
                .unwrap()
                .push(std::mem::take(&mut self.svc));
            state
                .lag
                .lock()
                .unwrap()
                .push(std::mem::take(&mut self.lag));
        }
    }

    pub struct RinglineClientRuntime {
        pub shutdown: Runtime,
        pub handles: Vec<std::thread::JoinHandle<Result<(), ringline::Error>>>,
    }

    impl RinglineClientRuntime {
        pub fn start(state: Arc<ClientState>, workers: usize) -> Result<Self, ringline::Error> {
            let msg_size = state.msg_size;
            // Read before `state` is moved into GLOBAL_STATE below.
            let clients = state.num_clients;

            *GLOBAL_STATE.lock().unwrap() = Some(state);

            // Room for every requested connection, and at least 4096.
            let max_conns = (clients as u32).next_power_of_two().max(4096);

            let config = ConfigBuilder::new()
                .workers(workers)
                .pin_to_core(false)
                .sq_entries(4096)
                .recv_buffer(4096, msg_size.next_power_of_two().max(4096) as u32)
                .max_connections(max_conns)
                .send_pool(
                    SEND_POOL_SLOTS,
                    msg_size.next_power_of_two().max(256) as u32,
                )
                // Each connection is a spawned task, and an open-loop one holds
                // a timer slot while it sleeps between requests. Both pools
                // default to 256 per worker, and a spawn past that failed:
                // a worker ran at most 256 connections. The `on_start` task
                // that spawns them holds one more task slot.
                .standalone_task_capacity((clients as u32 + 1).clamp(256, 65535))
                .timer_slots((clients as u32).clamp(256, 65535))
                .build()
                .expect("valid config");

            // Client-only mode: no bind address.
            let (shutdown, handles) = RinglineBuilder::new(config).launch::<ClientHandler>()?;

            Ok(RinglineClientRuntime { shutdown, handles })
        }

        pub fn stop(self) {
            self.shutdown.shutdown();
            for h in self.handles {
                h.join().ok();
            }
        }
    }
}

// ── Shared ─────────────────────────────────────────────────────────

fn wait_for_server(addr: &str) {
    for _ in 0..100 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on {addr}");
}

/// Run a benchmark against a server already listening on `addr`.
///
/// `open` selects open-loop (rate-controlled) mode; `None` is the closed-loop
/// request/response loop. `conn_chunk_size` controls how many connections are
/// assigned to each ringline worker before moving to the next (1 = round-robin).
#[allow(clippy::too_many_arguments)]
pub fn run_bench(
    addr: &str,
    num_clients: usize,
    msg_size: usize,
    depth: usize,
    warmup: Duration,
    duration: Duration,
    ringline_client: bool,
    workers: usize,
    open: Option<OpenLoop>,
    conn_chunk_size: usize,
) -> BenchResult {
    let stop = Arc::new(AtomicBool::new(false));
    let ops = Arc::new(AtomicU64::new(0));

    wait_for_server(addr);

    if ringline_client {
        run_bench_ringline(
            addr,
            num_clients,
            msg_size,
            depth,
            warmup,
            duration,
            stop,
            ops,
            workers,
            open,
            conn_chunk_size,
        )
    } else {
        run_bench_tokio(
            addr,
            num_clients,
            msg_size,
            depth,
            warmup,
            duration,
            stop,
            ops,
            workers,
            open,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn run_bench_tokio(
    addr: &str,
    num_clients: usize,
    msg_size: usize,
    depth: usize,
    warmup: Duration,
    duration: Duration,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    workers: usize,
    open: Option<OpenLoop>,
) -> BenchResult {
    let client_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers.max(1))
        .enable_all()
        .build()
        .expect("failed to build client runtime");

    let measure = Arc::new(AtomicBool::new(false));
    let per_conn_rate = open.map(|o| o.rate as f64 / num_clients.max(1) as f64);

    // Open-loop decomposition accumulators (per-connection ns sample vecs).
    let svc = Arc::new(Mutex::new(Vec::new()));
    let lag = Arc::new(Mutex::new(Vec::new()));

    let mut task_handles = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        let addr = addr.to_string();
        let stop = stop.clone();
        let ops = ops.clone();
        if let Some(o) = open {
            let measure = measure.clone();
            let rate = per_conn_rate.unwrap().max(1.0);
            task_handles.push(client_rt.spawn(run_tokio_open_client(
                addr,
                msg_size,
                rate,
                o.max_inflight,
                measure,
                stop,
                ops,
                svc.clone(),
                lag.clone(),
            )));
        } else {
            task_handles.push(client_rt.spawn(run_tokio_client(addr, msg_size, depth, stop, ops)));
        }
    }

    std::thread::sleep(warmup);
    ops.store(0, Ordering::Relaxed);
    measure.store(true, Ordering::Relaxed);

    let cpu_before = process_cpu_time_ns();
    let start = Instant::now();
    std::thread::sleep(duration);
    // Recording stops with the window `elapsed` measures. Only the open
    // loop reads `measure`: `run_tokio_client` records warmup and
    // wind-down responses too.
    measure.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();
    let cpu_after = process_cpu_time_ns();
    stop.store(true, Ordering::Relaxed);

    let mut merged = LatencyHistogram::new();
    client_rt.block_on(async {
        for handle in task_handles {
            match tokio::time::timeout(Duration::from_secs(2), handle).await {
                Ok(Ok(histogram)) => {
                    for &sample in histogram.samples() {
                        merged.record(sample);
                    }
                }
                Ok(Err(_join_err)) => {}
                Err(_timeout) => {}
            }
        }
    });

    client_rt.shutdown_timeout(Duration::from_secs(1));

    if open.is_some() {
        summarize_decomp("service recv-send", &svc.lock().unwrap());
        summarize_decomp("offer_lag send-sched", &lag.lock().unwrap());
    }

    // For open-loop, achieved throughput = measured responses; derive it from
    // the sample count so it stays consistent with the latency histogram.
    let total_ops = if open.is_some() {
        merged.samples().len() as u64
    } else {
        ops.load(Ordering::Relaxed)
    };
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    BenchResult {
        ops_per_sec,
        total_ops,
        latency: merged.finalize(),
        cpu_ns: cpu_after.saturating_sub(cpu_before),
        per_conn: None,
    }
}

/// Print a percentile summary (µs) of flattened per-connection ns samples to
/// stderr — used for the open-loop service-time / offer-lag decomposition.
fn summarize_decomp(label: &str, per_conn: &[Vec<u64>]) {
    let mut all: Vec<u64> = per_conn.iter().flatten().copied().collect();
    if all.is_empty() {
        return;
    }
    all.sort_unstable();
    let n = all.len();
    let q = |num: usize, den: usize| all[(n * num / den).min(n - 1)] / 1000;
    eprintln!(
        "[openloop-decomp] {label}: n={n} p50={}us p90={}us p99={}us p999={}us max={}us",
        q(50, 100),
        q(90, 100),
        q(99, 100),
        q(999, 1000),
        all[n - 1] / 1000,
    );
}

#[allow(clippy::too_many_arguments)]
fn run_bench_ringline(
    addr: &str,
    num_clients: usize,
    msg_size: usize,
    depth: usize,
    warmup: Duration,
    duration: Duration,
    stop: Arc<AtomicBool>,
    ops: Arc<AtomicU64>,
    workers: usize,
    open: Option<OpenLoop>,
    conn_chunk_size: usize,
) -> BenchResult {
    let histograms = Arc::new(Mutex::new(Vec::new()));
    let svc = Arc::new(Mutex::new(Vec::new()));
    let lag = Arc::new(Mutex::new(Vec::new()));
    let measure = Arc::new(AtomicBool::new(false));

    let state = Arc::new(ringline_client::ClientState {
        target: addr.parse().expect("invalid addr for ringline client"),
        msg_size,
        num_clients,
        depth,
        num_workers: workers.max(1),
        conn_chunk_size,
        stop: stop.clone(),
        measure: measure.clone(),
        ops: ops.clone(),
        histograms: histograms.clone(),
        open,
        svc: svc.clone(),
        lag: lag.clone(),
        live: None,
    });

    let client_rt = match ringline_client::RinglineClientRuntime::start(state, workers.max(1)) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("  ringline client failed to start: {e}");
            return BenchResult {
                ops_per_sec: 0.0,
                total_ops: 0,
                latency: LatencyHistogram::new().finalize(),
                cpu_ns: 0,
                per_conn: None,
            };
        }
    };

    std::thread::sleep(warmup);
    ops.store(0, Ordering::Relaxed);
    measure.store(true, Ordering::Relaxed);

    let cpu_before = process_cpu_time_ns();
    let start = Instant::now();
    std::thread::sleep(duration);
    // Responses that complete while the tasks wind down are outside the
    // window `elapsed` measures, so recording stops with it.
    measure.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();
    let cpu_after = process_cpu_time_ns();
    stop.store(true, Ordering::Relaxed);

    // Give tasks a moment to finish and push their histograms.
    std::thread::sleep(Duration::from_millis(200));
    client_rt.stop();

    let mut merged = LatencyHistogram::new();
    let per_conn;
    {
        let collected = histograms.lock().unwrap();
        per_conn = ConnCounts::from_counts(collected.iter().map(|s| s.len() as u64).collect());
        for samples in collected.iter() {
            for &sample in samples {
                merged.record(sample);
            }
        }
    }

    // Open-loop decomposition: corrected response time (above) = service time
    // (recv-actual_send) + offer lag (actual_send-scheduled). offer_lag ~ 0 ==
    // clean open-loop; growing offer_lag == back-pressure/coordinated-omission
    // contamination (past capacity).
    if open.is_some() {
        summarize_decomp("service recv-send", &svc.lock().unwrap());
        summarize_decomp("offer_lag send-sched", &lag.lock().unwrap());
    }

    // For open-loop, achieved throughput = measured responses; derive it from
    // the sample count so it stays consistent with the latency histogram.
    let total_ops = if open.is_some() {
        merged.samples().len() as u64
    } else {
        ops.load(Ordering::Relaxed)
    };
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    BenchResult {
        ops_per_sec,
        total_ops,
        latency: merged.finalize(),
        cpu_ns: cpu_after.saturating_sub(cpu_before),
        per_conn,
    }
}

// ── Open loop with in-place rate changes ───────────────────────────

/// A point-in-time copy of a [`LiveOpenLoop`]'s response histogram.
pub struct LiveSnapshot {
    hist: histogram::Histogram,
    at: Instant,
}

/// An open-loop ringline client whose connections stay open while
/// [`set_rate`](Self::set_rate) changes the offered rate, for an SLO search
/// that measures every step on the same connections. A step is measured by
/// a [`snapshot`](Self::snapshot) at its start and end and
/// [`LiveOpenLoop::step`] on the two.
pub struct LiveOpenLoop {
    ctl: Arc<LiveCtl>,
    stop: Arc<AtomicBool>,
    rt: ringline_client::RinglineClientRuntime,
}

impl LiveOpenLoop {
    pub fn start(
        addr: &str,
        num_clients: usize,
        msg_size: usize,
        workers: usize,
        max_inflight: usize,
        conn_chunk_size: usize,
        rate: u64,
    ) -> Result<Self, String> {
        wait_for_server(addr);
        let ctl = Arc::new(LiveCtl::new(rate));
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(ringline_client::ClientState {
            target: addr
                .parse()
                .map_err(|e| format!("invalid addr {addr}: {e}"))?,
            msg_size,
            num_clients,
            depth: 1,
            num_workers: workers.max(1),
            conn_chunk_size,
            stop: stop.clone(),
            measure: Arc::new(AtomicBool::new(false)),
            ops: Arc::new(AtomicU64::new(0)),
            histograms: Arc::new(Mutex::new(Vec::new())),
            open: Some(OpenLoop { rate, max_inflight }),
            svc: Arc::new(Mutex::new(Vec::new())),
            lag: Arc::new(Mutex::new(Vec::new())),
            live: Some(ctl.clone()),
        });
        let rt = ringline_client::RinglineClientRuntime::start(state, workers.max(1))
            .map_err(|e| format!("ringline client failed to start: {e}"))?;
        Ok(Self { ctl, stop, rt })
    }

    /// Offer `rate` requests/s in total. Every connection stops sending
    /// until the requests it has in flight are answered; once all open
    /// connections have drained, or `timeout` has passed, each starts a new
    /// schedule at `rate`. Returns how long the drain took, or `None` if it
    /// timed out.
    pub fn set_rate(&self, rate: u64, timeout: Duration) -> Option<Duration> {
        let ctl = &self.ctl;
        let next = ctl.generation.load(Ordering::Relaxed) + 1;
        ctl.drained
            .store((next & DRAINED_COUNT) << 32, Ordering::Release);
        ctl.rate.store(rate, Ordering::Relaxed);
        ctl.generation.store(next, Ordering::Release);
        let began = Instant::now();
        let drained = loop {
            // `conns` first: a closing connection unmarks itself before it
            // leaves `conns`.
            let open = ctl.conns.load(Ordering::Acquire);
            let n = ctl.drained.load(Ordering::Acquire) & DRAINED_COUNT;
            if n >= open {
                break Some(began.elapsed());
            }
            if began.elapsed() >= timeout {
                break None;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        ctl.go.store(next, Ordering::Release);
        drained
    }

    /// Connections open now.
    pub fn connections(&self) -> u64 {
        self.ctl.conns.load(Ordering::Relaxed)
    }

    pub fn snapshot(&self) -> LiveSnapshot {
        LiveSnapshot {
            hist: self.ctl.hist.load(),
            at: Instant::now(),
        }
    }

    /// Achieved responses/s and latency between two snapshots.
    pub fn step(before: &LiveSnapshot, after: &LiveSnapshot) -> (f64, LatencyStats) {
        let delta = after
            .hist
            .wrapping_sub(&before.hist)
            .expect("snapshots of one histogram share a config");
        let count: u64 = delta.iter().map(|b| b.count()).sum();
        let secs = after.at.saturating_duration_since(before.at).as_secs_f64();
        let pct = |q: f64| -> u64 {
            delta
                .quantile(q)
                .ok()
                .flatten()
                .and_then(|r| r.entries().values().next().map(|b| b.end()))
                .unwrap_or(0)
        };
        // The histogram has no exact sum; the mean is over bucket midpoints.
        let mean_ns = if count == 0 {
            0
        } else {
            (delta
                .iter()
                .map(|b| (b.start() as u128 + b.end() as u128) / 2 * b.count() as u128)
                .sum::<u128>()
                / count as u128) as u64
        };
        let stats = LatencyStats {
            mean_ns,
            p50_ns: pct(0.5),
            p90_ns: pct(0.9),
            p99_ns: pct(0.99),
            p999_ns: pct(0.999),
            p9999_ns: pct(0.9999),
            max_ns: pct(1.0),
            count,
        };
        (if secs > 0.0 { count as f64 / secs } else { 0.0 }, stats)
    }

    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(200));
        self.rt.stop();
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;

    #[test]
    fn step_reports_only_the_window_between_snapshots() {
        let ctl = LiveCtl::new(1000);
        // Recorded before the window: must not appear in the step.
        for _ in 0..1000 {
            ctl.hist.increment(1_000_000_000).unwrap();
        }
        let before = LiveSnapshot {
            hist: ctl.hist.load(),
            at: Instant::now(),
        };
        for us in 1..=1000u64 {
            ctl.hist.increment(us * 1000).unwrap();
        }
        let after = LiveSnapshot {
            hist: ctl.hist.load(),
            at: before.at + Duration::from_secs(2),
        };
        let (rate, lat) = LiveOpenLoop::step(&before, &after);
        assert_eq!(lat.count, 1000);
        assert_eq!(rate, 500.0);
        // Bucket ends are within 2^-7 of the value.
        let near = |got: u64, want: u64| (got as f64 - want as f64).abs() / want as f64 <= 0.01;
        assert!(near(lat.p50_ns, 500_000), "p50 {}", lat.p50_ns);
        assert!(near(lat.p99_ns, 990_000), "p99 {}", lat.p99_ns);
        assert!(near(lat.max_ns, 1_000_000), "max {}", lat.max_ns);
        assert!(near(lat.mean_ns, 500_500), "mean {}", lat.mean_ns);
    }
}
