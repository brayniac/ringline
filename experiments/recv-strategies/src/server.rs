//! The server: accepts `--conns` connections, then runs one io_uring event
//! loop that a `Strategy` plugs its receive model into.

use crate::common::{Parsed, parse};
use crate::{arg, flag};
use io_uring::{IoUring, opcode, squeue, types};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant};

pub const TAG_RECV: u64 = 1;
pub const TAG_SEND: u64 = 2;
/// A strategy's second recv kind (the shared ring's fallback recv).
pub const TAG_RECV_AUX: u64 = 3;

/// Encode a CQE's user_data: tag, a 24-bit strategy value, connection.
pub fn ud(tag: u64, extra: u32, conn: usize) -> u64 {
    (tag << 56) | ((extra as u64 & 0xff_ffff) << 32) | conn as u64
}

static ACKS: [u8; 256] = [b'k'; 256];

/// What every strategy shares: the ring, the sockets and the application.
pub struct Ctx {
    pub uring: IoUring,
    pub fds: Vec<RawFd>,
    /// Ack each message with one byte (request/response and pipelined
    /// workloads); off for streaming.
    pub ack: bool,
    pub msgs: u64,
    pub bytes: u64,
    pub touch: u64,
    pub sends_failed: u64,
    /// Pushes that found the SQ full and submitted inline.
    pub sq_full: u64,
}

impl Ctx {
    pub fn push(&mut self, sqe: squeue::Entry) {
        while unsafe { self.uring.submission().push(&sqe) }.is_err() {
            self.sq_full += 1;
            self.uring.submit().expect("submit");
        }
    }

    /// The application: consume whole messages from `data` (connection
    /// `c`'s unread bytes) and ack them.
    pub fn deliver(&mut self, c: usize, data: &[u8]) -> Parsed {
        let p = parse(data, &mut self.touch);
        self.msgs += p.msgs as u64;
        self.bytes += p.consumed as u64;
        if self.ack {
            let mut left = p.msgs as usize;
            while left > 0 {
                let n = left.min(ACKS.len());
                let sqe = opcode::Send::new(types::Fd(self.fds[c]), ACKS.as_ptr(), n as u32)
                    .build()
                    .user_data(ud(TAG_SEND, 0, c));
                self.push(sqe);
                left -= n;
            }
        }
        p
    }
}

/// A receive model. The loop calls `on_recv` for each recv CQE in a batch,
/// then `settle` for each connection the batch touched, after the CQ is
/// drained and before the next enter.
pub trait Strategy {
    fn name(&self) -> String;
    /// Register what the strategy needs and arm every connection.
    fn start(&mut self, cx: &mut Ctx);
    fn on_recv(&mut self, cx: &mut Ctx, c: usize, tag: u64, extra: u32, res: i32, flags: u32);
    fn settle(&mut self, cx: &mut Ctx, c: usize);
    fn dead(&self, c: usize) -> bool;
    /// Called once per loop iteration; returns when it next needs a call.
    fn tick(&mut self, _cx: &mut Ctx) -> Option<Instant> {
        None
    }
    /// Strategy counters for the RESULT line.
    fn report(&self) -> String;
}

fn status_kb(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with(field))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn cpu(who: i32) -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(who, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

pub fn run(args: &[String]) {
    let addr: String = arg(args, "--addr", None);
    let nconns: usize = arg(args, "--conns", None);
    let warmup: u64 = arg(args, "--warmup", Some(2));
    let duration: u64 = arg(args, "--duration", Some(8));
    let sqpoll = flag(args, "--sqpoll");
    let strategy_name: String = arg(args, "--strategy", None);
    let mut strategy = crate::strategies::build(&strategy_name, args, nconns, sqpoll);

    let listener = TcpListener::bind(&addr).expect("bind");
    // Give up if the client never connects, so a failed run cannot stall a
    // two-machine sequence.
    listener.set_nonblocking(true).expect("nonblocking listener");
    let accept_deadline = Instant::now() + Duration::from_secs(60);
    let mut streams = Vec::with_capacity(nconns);
    while streams.len() < nconns {
        match listener.accept() {
            Ok((s, _)) => {
                s.set_nonblocking(false).ok();
                s.set_nodelay(true).ok();
                streams.push(s);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < accept_deadline, "accept timeout: {} of {nconns}", streams.len());
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(e) => panic!("accept: {e}"),
        }
    }

    let mut b = IoUring::builder();
    // IORING_MAX_CQ_ENTRIES is 65536; a larger request fails with EINVAL.
    b.setup_cqsize((8 * nconns as u32 + 4096).next_power_of_two().min(65536));
    if sqpoll {
        b.setup_sqpoll(1000);
        if args.iter().any(|a| a == "--sqpoll-cpu") {
            b.setup_sqpoll_cpu(arg(args, "--sqpoll-cpu", None));
        }
    } else {
        b.setup_single_issuer().setup_defer_taskrun();
    }
    let sq_entries: u32 = arg(args, "--sq-entries", Some(4096));
    let uring = b.build(sq_entries).expect("io_uring setup");
    let mut cx = Ctx {
        uring,
        fds: streams.iter().map(|s| s.as_raw_fd()).collect(),
        ack: !flag(args, "--no-ack"),
        msgs: 0,
        bytes: 0,
        touch: 0,
        sends_failed: 0,
        sq_full: 0,
    };
    strategy.start(&mut cx);
    cx.uring.submit().expect("submit arms");
    let idle_rss = status_kb("VmRSS:");

    let start = Instant::now();
    let measure_from = start + Duration::from_secs(warmup);
    let measure_to = measure_from + Duration::from_secs(duration);
    let mut snap: Option<(u64, u64, Duration, Duration, Instant)> = None;
    let mut seen = vec![false; nconns];
    let mut touched = Vec::with_capacity(nconns);
    let mut batch: Vec<(u64, i32, u32)> = Vec::with_capacity(16384);
    let tick = types::Timespec::new().nsec(100_000_000);
    let mut next_tick: Option<Instant> = None;

    loop {
        let wait = match next_tick {
            Some(at) => {
                let d = at.saturating_duration_since(Instant::now()).min(Duration::from_millis(100));
                types::Timespec::new().sec(d.as_secs()).nsec(d.subsec_nanos())
            }
            None => tick,
        };
        let _ = cx.uring.submitter().submit_with_args(1, &types::SubmitArgs::new().timespec(&wait));
        batch.clear();
        batch.extend(cx.uring.completion().map(|c| (c.user_data(), c.result(), c.flags())));
        for &(u, res, flags) in &batch {
            let c = (u & 0xffff_ffff) as usize;
            let tag = u >> 56;
            match tag {
                TAG_SEND => {
                    if res < 0 {
                        cx.sends_failed += 1;
                    }
                }
                TAG_RECV | TAG_RECV_AUX => {
                    strategy.on_recv(&mut cx, c, tag, ((u >> 32) & 0xff_ffff) as u32, res, flags);
                    if !seen[c] {
                        seen[c] = true;
                        touched.push(c);
                    }
                }
                _ => {}
            }
        }
        for &c in &touched {
            strategy.settle(&mut cx, c);
            seen[c] = false;
        }
        touched.clear();
        next_tick = strategy.tick(&mut cx);

        let now = Instant::now();
        if snap.is_none() && now >= measure_from {
            snap = Some((
                cx.msgs,
                cx.bytes,
                cpu(libc::RUSAGE_SELF),
                cpu(libc::RUSAGE_THREAD),
                now,
            ));
        }
        if now >= measure_to {
            break;
        }
        if (0..nconns).all(|c| strategy.dead(c)) {
            eprintln!("all connections closed before the window ended");
            break;
        }
    }

    let (m0, b0, p0, t0, at) = snap.expect("the run ended before its warmup");
    let secs = at.elapsed().as_secs_f64();
    let msgs = cx.msgs - m0;
    let bytes = cx.bytes - b0;
    let proc_cpu = (cpu(libc::RUSAGE_SELF) - p0).as_secs_f64();
    let main_cpu = (cpu(libc::RUSAGE_THREAD) - t0).as_secs_f64();
    let dead = (0..nconns).filter(|&c| strategy.dead(c)).count();
    println!(
        "RESULT strategy={} sqpoll={} conns={} msgs_per_sec={:.0} mbyte_per_sec={:.1} \
         cpu_ns_per_msg={:.0} cpu_ns_per_kib={:.0} main_util={:.3} proc_util={:.3} \
         idle_rss_kb={} rss_kb={} sq_full={} sends_failed={} dead={} touch={} {}",
        strategy.name(),
        sqpoll,
        nconns,
        msgs as f64 / secs,
        bytes as f64 / secs / 1e6,
        proc_cpu * 1e9 / msgs.max(1) as f64,
        proc_cpu * 1e9 / (bytes.max(1) as f64 / 1024.0),
        main_cpu / secs,
        proc_cpu / secs,
        idle_rss,
        status_kb("VmRSS:"),
        cx.sq_full,
        cx.sends_failed,
        dead,
        cx.touch % 10,
        strategy.report(),
    );
    drop(streams);
}
