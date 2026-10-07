//! TCP receive-strategy benchmark for `docs/recv-into-accumulator-design.md`.
//!
//! `server --strategy NAME` accepts `--conns` connections and receives
//! length-prefixed messages on one io_uring, acking each with one byte
//! (unless `--no-ack`). Strategies (`src/strategies/`):
//!
//! - `shared`: ringline's model today. One provided-buffer ring per worker,
//!   lend-in-place when nothing is buffered, copy otherwise, fallback recv.
//! - `shared_inc`: the same ring with `IOU_PBUF_RING_INC`.
//! - `ring`: a one-entry INC ring per connection over its region; moves
//!   rewrite the posted entry between enters.
//! - `ring_norewrite`: the same, never touching a posted entry.
//! - `oneshot`: one `RECV` at a time into the region.
//!
//! Regions start at `--region` (4 KiB) and grow to `--region-max` when a
//! message needs it, as the design specifies.
//!
//! `client` drives the load: `--depth` messages outstanding per connection
//! (1 = request/response), or `--stream` (no acks, continuous). `--mix
//! SIZE:WEIGHT,...` draws message sizes; otherwise `--msg-size`. It
//! reports throughput and request latency percentiles over the window
//! after `--warmup`.
//!
//! `bytes-bench` times a `Bytes::from_owner` view against today's
//! `BytesMut::split_to().freeze()`. `inc-probe` and `ring-limit` report the
//! kernel facts the design relies on (`src/probe.rs`).

mod common;
mod probe;
mod server;
mod strategies;

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: Option<T>) -> T {
    match args.iter().position(|a| a == name) {
        Some(i) => args
            .get(i + 1)
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("bad value for {name}")),
        None => default.unwrap_or_else(|| panic!("missing {name}")),
    }
}

pub fn flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => server::run(&args),
        Some("client") => client(&args),
        Some("bytes-bench") => bytes_bench(&args),
        Some("inc-probe") => probe::inc_probe(),
        Some("ring-limit") => probe::ring_limit(),
        _ => {
            eprintln!("usage: recv-strategies server|client|bytes-bench|inc-probe|ring-limit ...");
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------- client ---

/// Latency histogram: power-of-two buckets of nanoseconds, each split into
/// 16 linear sub-buckets.
struct Hist {
    counts: Vec<u64>,
}

impl Hist {
    fn new() -> Self {
        Hist { counts: vec![0; 64 * 16] }
    }
    fn index(ns: u64) -> usize {
        let ns = ns.max(1);
        let p = 63 - ns.leading_zeros() as usize;
        let sub = if p >= 4 { ((ns >> (p - 4)) & 15) as usize } else { 0 };
        p * 16 + sub
    }
    fn value(i: usize) -> u64 {
        let p = i / 16;
        let sub = (i % 16) as u64;
        if p >= 4 { (16 + sub) << (p - 4) } else { 1 << p }
    }
    fn record(&mut self, ns: u64) {
        self.counts[Self::index(ns)] += 1;
    }
    fn merge(&mut self, o: &Hist) {
        for (a, b) in self.counts.iter_mut().zip(&o.counts) {
            *a += b;
        }
    }
    fn percentile(&self, q: f64) -> u64 {
        let total: u64 = self.counts.iter().sum();
        if total == 0 {
            return 0;
        }
        let want = (total as f64 * q).ceil() as u64;
        let mut seen = 0;
        for (i, &c) in self.counts.iter().enumerate() {
            seen += c;
            if seen >= want {
                return Self::value(i);
            }
        }
        0
    }
}

struct ClientCfg {
    depth: usize,
    stream: bool,
    /// Stamp each message with the connection's sequence number and a
    /// payload derived from it, for the server's `--verify`.
    verify: bool,
    /// Pre-built messages (header + payload) and their cumulative weights.
    msgs: Vec<Vec<u8>>,
    weights: Vec<u32>,
    measure_from: Instant,
}

fn client(args: &[String]) {
    let addr: String = arg(args, "--addr", None);
    let nconns: usize = arg(args, "--conns", None);
    let threads: usize = arg(args, "--threads", Some(4));
    let seconds: u64 = arg(args, "--seconds", None);
    let warmup: u64 = arg(args, "--warmup", Some(2));
    let delay_ms: u64 = arg(args, "--start-delay-ms", Some(1000));
    let stream = flag(args, "--stream");
    let verify = flag(args, "--verify");
    let depth: usize = arg(args, "--depth", Some(1));
    // Open loop: send at this many messages per second in total, each
    // connection on its own fixed schedule, and measure latency from the
    // scheduled send time. 0 keeps the closed loop.
    let rate: f64 = arg(args, "--rate", Some(0.0));
    let mix: String = if flag(args, "--mix") {
        arg(args, "--mix", None)
    } else {
        format!("{}:1", arg::<usize>(args, "--msg-size", None))
    };
    let mut msgs = Vec::new();
    let mut weights = Vec::new();
    let mut acc = 0;
    for part in mix.split(',') {
        let (size, w) = part.split_once(':').expect("--mix SIZE:WEIGHT,...");
        let size: usize = size.parse().unwrap();
        acc += w.parse::<u32>().unwrap();
        // The size is the whole message, header included.
        let len = size.saturating_sub(4);
        let mut m = (len as u32).to_le_bytes().to_vec();
        m.extend(std::iter::repeat_n(b'm', len));
        msgs.push(m);
        weights.push(acc);
    }

    let mut streams: Vec<TcpStream> = Vec::with_capacity(nconns);
    // The server may not be listening yet (two-machine runs start the next
    // server when the previous run ends): retry the first connect.
    let deadline = Instant::now() + Duration::from_secs(30);
    for _ in 0..nconns {
        let s = loop {
            match TcpStream::connect(&addr) {
                Ok(s) => break s,
                Err(e) if streams.is_empty() && Instant::now() < deadline => {
                    let _ = e;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("connect {addr}: {e}"),
            }
        };
        s.set_nodelay(true).ok();
        streams.push(s);
    }
    std::thread::sleep(Duration::from_millis(delay_ms));

    let cfg = Arc::new(ClientCfg {
        depth,
        stream,
        verify,
        msgs,
        weights,
        measure_from: Instant::now() + Duration::from_secs(warmup),
    });
    let done = Arc::new(AtomicBool::new(false));
    let per = nconns.div_ceil(threads);
    let mut handles = Vec::new();
    let mut streams = streams.into_iter();
    for t in 0..threads {
        let mine: Vec<TcpStream> = streams.by_ref().take(per).collect();
        if mine.is_empty() {
            break;
        }
        let (cfg, done) = (cfg.clone(), done.clone());
        let per_conn_interval = if rate > 0.0 {
            Some(Duration::from_secs_f64(nconns as f64 / rate))
        } else {
            None
        };
        handles.push(std::thread::spawn(move || match per_conn_interval {
            Some(interval) => client_rate_thread(t as u64, mine, cfg, done, interval),
            None => client_thread(t as u64, mine, cfg, done),
        }));
    }
    std::thread::sleep(Duration::from_secs(seconds));
    done.store(true, Ordering::Relaxed);
    let mut hist = Hist::new();
    let mut acked = 0u64;
    for h in handles {
        let (n, hh) = h.join().unwrap();
        acked += n;
        hist.merge(&hh);
    }
    let window = seconds.saturating_sub(warmup).max(1) as f64;
    println!(
        "CLIENT acked_per_sec={:.0} p50_us={:.1} p99_us={:.1} p999_us={:.1}",
        acked as f64 / window,
        hist.percentile(0.5) as f64 / 1e3,
        hist.percentile(0.99) as f64 / 1e3,
        hist.percentile(0.999) as f64 / 1e3,
    );
}

struct ConnState {
    sock: mio::net::TcpStream,
    /// The message being written (index into `msgs`) and bytes written.
    cur: Option<(usize, usize)>,
    /// When the message being written started: latency runs from its first
    /// byte, so a receiver slow enough to block the write is counted.
    started: Instant,
    /// Start times of messages written and not yet acked.
    sent: VecDeque<Instant>,
    /// `--verify`: the message being written, stamped, and the next
    /// sequence number.
    stamped: Vec<u8>,
    seq: u32,
}

/// Write messages while fewer than `depth` are outstanding (always, when
/// streaming), until the socket would block.
fn fill(cfg: &ClientCfg, c: &mut ConnState, pick: &mut dyn FnMut() -> usize) {
    loop {
        if !cfg.stream && c.cur.is_none() && c.sent.len() >= cfg.depth {
            return;
        }
        let (i, off) = match c.cur {
            Some(cur) => cur,
            None => {
                let i = pick();
                if cfg.verify {
                    c.stamped.clear();
                    c.stamped.extend_from_slice(&cfg.msgs[i]);
                    stamp(&mut c.stamped, c.seq);
                    c.seq = c.seq.wrapping_add(1);
                }
                c.cur = Some((i, 0));
                c.started = Instant::now();
                (i, 0)
            }
        };
        let msg: &[u8] = if cfg.verify { &c.stamped } else { &cfg.msgs[i] };
        match c.sock.write(&msg[off..]) {
            Ok(n) => {
                if off + n == msg.len() {
                    c.cur = None;
                    if !cfg.stream {
                        c.sent.push_back(c.started);
                    }
                } else {
                    c.cur = Some((i, off + n));
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return,
        }
    }
}

/// Write `seq` and the payload derived from it into message `m` (header
/// already set). The server's `--verify` checks the same pattern.
fn stamp(m: &mut [u8], seq: u32) {
    let payload = &mut m[4..];
    if payload.len() >= 4 {
        payload[..4].copy_from_slice(&seq.to_le_bytes());
        for (i, b) in payload[4..].iter_mut().enumerate() {
            *b = common::pattern(seq, i);
        }
    }
}

/// One connection in the open-loop client.
struct RateConn {
    sock: mio::net::TcpStream,
    /// When the next message is due.
    next_due: Instant,
    /// Messages due and not yet fully written: (scheduled time, message).
    pending: VecDeque<(Instant, usize)>,
    /// Bytes of the front pending message already written.
    off: usize,
    /// Scheduled times of messages written and not yet acked.
    sent: VecDeque<Instant>,
}

/// Open-loop client: each connection sends one message every `interval`,
/// on schedule whether or not earlier ones are acked; latency runs from the
/// scheduled time, so a server that falls behind is charged for the
/// queueing it causes.
fn client_rate_thread(
    seed: u64,
    streams: Vec<TcpStream>,
    cfg: Arc<ClientCfg>,
    done: Arc<AtomicBool>,
    interval: Duration,
) -> (u64, Hist) {
    use mio::{Events, Interest, Poll, Token};
    let mut poll = Poll::new().unwrap();
    let n = streams.len().max(1);
    let start = Instant::now();
    let mut conns: Vec<RateConn> = streams
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            s.set_nonblocking(true).unwrap();
            let mut sock = mio::net::TcpStream::from_std(s);
            poll.registry().register(&mut sock, Token(i), Interest::READABLE | Interest::WRITABLE).unwrap();
            // Stagger the connections across one interval.
            let offset = interval.mul_f64((i as f64 + (seed as f64 % 1.0)) / n as f64);
            RateConn { sock, next_due: start + offset, pending: VecDeque::new(), off: 0, sent: VecDeque::new() }
        })
        .collect();
    let mut rng = (seed + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let weights = cfg.weights.clone();
    let total_w = *weights.last().unwrap();
    let mut pick = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let r = (rng % total_w as u64) as u32;
        weights.iter().position(|&w| r < w).unwrap()
    };
    let mut hist = Hist::new();
    let mut acked = 0u64;
    let mut buf = [0u8; 4096];
    let mut events = Events::with_capacity(1024);

    fn write_pending(cfg: &ClientCfg, c: &mut RateConn) {
        while let Some(&(due, i)) = c.pending.front() {
            let msg = &cfg.msgs[i];
            match c.sock.write(&msg[c.off..]) {
                Ok(w) => {
                    c.off += w;
                    if c.off == msg.len() {
                        c.off = 0;
                        c.pending.pop_front();
                        c.sent.push_back(due);
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => return,
            }
        }
    }

    while !done.load(Ordering::Relaxed) {
        let now = Instant::now();
        let mut earliest = now + Duration::from_millis(50);
        for c in conns.iter_mut() {
            let mut added = false;
            while c.next_due <= now {
                c.pending.push_back((c.next_due, pick()));
                c.next_due += interval;
                added = true;
            }
            if added {
                write_pending(&cfg, c);
            }
            earliest = earliest.min(c.next_due);
        }
        let timeout = earliest.saturating_duration_since(Instant::now());
        poll.poll(&mut events, Some(timeout)).unwrap();
        for ev in events.iter() {
            let c = &mut conns[ev.token().0];
            if ev.is_readable() {
                loop {
                    match c.sock.read(&mut buf) {
                        Ok(0) => break,
                        Ok(got) => {
                            let at = Instant::now();
                            for _ in 0..got {
                                if let Some(t) = c.sent.pop_front() {
                                    if t >= cfg.measure_from {
                                        hist.record(at.duration_since(t).as_nanos() as u64);
                                        acked += 1;
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            }
            if ev.is_writable() {
                write_pending(&cfg, c);
            }
        }
    }
    (acked, hist)
}

fn client_thread(seed: u64, streams: Vec<TcpStream>, cfg: Arc<ClientCfg>, done: Arc<AtomicBool>) -> (u64, Hist) {
    use mio::{Events, Interest, Poll, Token};
    let mut poll = Poll::new().unwrap();
    let mut conns: Vec<ConnState> = streams
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            s.set_nonblocking(true).unwrap();
            let mut sock = mio::net::TcpStream::from_std(s);
            poll.registry().register(&mut sock, Token(i), Interest::READABLE | Interest::WRITABLE).unwrap();
            ConnState { sock, cur: None, started: Instant::now(), sent: VecDeque::new(), stamped: Vec::new(), seq: 0 }
        })
        .collect();
    let mut rng = (seed + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let weights = cfg.weights.clone();
    let total_w = *weights.last().unwrap();
    let mut pick = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let r = (rng % total_w as u64) as u32;
        weights.iter().position(|&w| r < w).unwrap()
    };
    let mut hist = Hist::new();
    let mut acked = 0u64;
    let mut buf = [0u8; 4096];
    let mut events = Events::with_capacity(1024);

    for c in conns.iter_mut() {
        fill(&cfg, c, &mut pick);
    }
    while !done.load(Ordering::Relaxed) {
        poll.poll(&mut events, Some(Duration::from_millis(50))).unwrap();
        for ev in events.iter() {
            let c = &mut conns[ev.token().0];
            if ev.is_readable() {
                loop {
                    match c.sock.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            let now = Instant::now();
                            for _ in 0..n {
                                if let Some(t) = c.sent.pop_front() {
                                    if t >= cfg.measure_from {
                                        hist.record(now.duration_since(t).as_nanos() as u64);
                                        acked += 1;
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            }
            fill(&cfg, c, &mut pick);
        }
    }
    (acked, hist)
}

// ----------------------------------------------------------- bytes-bench ---

fn bytes_bench(args: &[String]) {
    use bytes::{Bytes, BytesMut};
    let iters: usize = arg(args, "--iters", Some(10_000_000));
    let len: usize = arg(args, "--len", Some(256));
    let mut sink = 0usize;

    // Today: the accumulator freezes its unread prefix into a `Bytes`.
    let mut acc = BytesMut::with_capacity(1 << 20);
    let chunk = vec![7u8; len];
    let t = Instant::now();
    for _ in 0..iters {
        if acc.capacity() - acc.len() < len {
            acc = BytesMut::with_capacity(1 << 20);
        }
        acc.extend_from_slice(&chunk);
        let frozen = acc.split_to(len).freeze();
        sink += frozen.len();
        drop(frozen);
    }
    let today = t.elapsed();

    // The design: a view of a shared region allocation.
    struct View {
        region: Arc<Vec<u8>>,
        off: usize,
        len: usize,
    }
    impl AsRef<[u8]> for View {
        fn as_ref(&self) -> &[u8] {
            &self.region[self.off..self.off + self.len]
        }
    }
    let region = Arc::new(vec![7u8; 1 << 20]);
    let t = Instant::now();
    for i in 0..iters {
        let off = (i * len) % ((1 << 20) - len);
        let v = Bytes::from_owner(View { region: region.clone(), off, len });
        sink += v.len();
        drop(v);
    }
    let owner = t.elapsed();

    // A view sliced from one cached owner `Bytes` (no allocation per view).
    let cached = Bytes::from_owner(View { region: region.clone(), off: 0, len: 1 << 20 });
    let t = Instant::now();
    for i in 0..iters {
        let off = (i * len) % ((1 << 20) - len);
        let v = cached.slice(off..off + len);
        sink += v.len();
        drop(v);
    }
    let sliced = t.elapsed();
    println!(
        "BYTES len={} split_freeze_ns={:.1} from_owner_ns={:.1} cached_slice_ns={:.1} sink={}",
        len,
        today.as_nanos() as f64 / iters as f64,
        owner.as_nanos() as f64 / iters as f64,
        sliced.as_nanos() as f64 / iters as f64,
        sink % 10
    );
}
