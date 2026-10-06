//! TCP receive-strategy benchmark for `docs/recv-into-accumulator-design.md`.
//!
//! `server` accepts `--conns` connections, then receives fixed-size messages
//! on one io_uring and answers each with a one-byte ack. `--strategy` picks
//! how bytes reach the connection's buffer:
//!
//! - `shared`: one provided-buffer ring per worker (`--shared-bufs` × 16 KiB;
//!   256 is ringline's default), multishot recv, a copy into a per-connection
//!   accumulator. On `-ENOBUFS` it re-arms after the batch has replenished,
//!   as ringline does; it has no fallback recv.
//! - `ring`: a one-entry `IOU_PBUF_RING_INC` ring per connection whose buffer
//!   is the connection's region; multishot recv appends into it.
//! - `oneshot`: a one-shot `RECV` into the connection's region.
//!
//! `--sqpoll` sets the ring up with SQPOLL instead of SINGLE_ISSUER +
//! DEFER_TASKRUN, with the kernel thread on `--sqpoll-cpu` if given (`ring` is
//! refused there: it moves posted entries in place, which is only sound under
//! DEFER_TASKRUN).
//!
//! The server measures over `--duration` seconds after `--warmup` and prints
//! one `RESULT` line. `client` drives the load: closed loop, one message
//! outstanding per connection.

use io_uring::{cqueue, opcode, squeue, types, IoUring};
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const TAG_RECV: u64 = 1;
const TAG_SEND: u64 = 2;
const SHARED_BUF: usize = 16 * 1024;
const PAGE: usize = 4096;
const INC: u16 = 2; // IOU_PBUF_RING_INC
static ACK: [u8; 1] = [b'k'];

#[derive(Clone, Copy, PartialEq, Debug)]
enum Strategy {
    Shared,
    Ring,
    Oneshot,
}

fn arg<T: std::str::FromStr>(args: &[String], name: &str, default: Option<T>) -> T {
    match args.iter().position(|a| a == name) {
        Some(i) => args
            .get(i + 1)
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("bad value for {name}")),
        None => default.unwrap_or_else(|| panic!("missing {name}")),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("server") => server(&args),
        Some("client") => client(&args),
        _ => {
            eprintln!("usage: recv-strategies server|client ...");
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------- memory ---

fn mmap_anon(bytes: usize) -> *mut u8 {
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            bytes.max(PAGE),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    assert_ne!(p, libc::MAP_FAILED, "mmap {bytes} bytes");
    p as *mut u8
}

/// One `io_uring_buf` entry.
#[repr(C)]
struct BufEntry {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

/// A provided-buffer ring in userspace memory: `entries` entries at `base`,
/// the tail at offset 14.
struct BufRing {
    base: *mut u8,
    mask: u16,
    tail: u16,
}

impl BufRing {
    fn entry(&self, i: u16) -> *mut BufEntry {
        unsafe { (self.base as *mut BufEntry).add((i & self.mask) as usize) }
    }
    /// Post one buffer and publish it.
    fn push(&mut self, addr: u64, len: u32, bid: u16) {
        unsafe {
            let e = self.entry(self.tail);
            (*e).addr = addr;
            (*e).len = len;
            (*e).bid = bid;
        }
        self.tail = self.tail.wrapping_add(1);
        unsafe { (*(self.base.add(14) as *const AtomicU16)).store(self.tail, Ordering::Release) };
    }
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

/// CPU time of the whole process (all threads, the SQPOLL thread included).
fn process_cpu() -> Duration {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| Duration::new(t.tv_sec as u64, t.tv_usec as u32 * 1000);
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

/// CPU time of the SQPOLL thread alone, from /proc (clock ticks).
fn sqpoll_cpu() -> Duration {
    let tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    let mut ticks = 0u64;
    if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
        for t in dir.flatten() {
            let comm = std::fs::read_to_string(t.path().join("comm")).unwrap_or_default();
            if !comm.starts_with("iou-sqp") {
                continue;
            }
            let stat = std::fs::read_to_string(t.path().join("stat")).unwrap_or_default();
            let after = stat.rsplit(')').next().unwrap_or("");
            let f: Vec<&str> = after.split_whitespace().collect();
            ticks += f.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
                + f.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
        }
    }
    Duration::from_nanos(ticks * 1_000_000_000 / tck)
}

// ---------------------------------------------------------------- server ---

struct Conn {
    fd: RawFd,
    /// Unread bytes are `[head, tail)` of the region (or of `acc`).
    head: usize,
    tail: usize,
    /// `shared`: the accumulator the provided buffers are copied into.
    acc: Vec<u8>,
    /// `ring`: the connection's one-entry ring.
    ring: Option<BufRing>,
    /// `ring`: the region has a range posted to the kernel.
    posted: bool,
    /// The recv arm has ended and must be re-armed after this batch.
    rearm: bool,
    /// `ring`: the entry was used up (F_BUF_MORE clear) or needs moving.
    repost: bool,
    dead: bool,
}

struct Server {
    strategy: Strategy,
    uring: IoUring,
    conns: Vec<Conn>,
    msg: usize,
    region: usize,
    regions: *mut u8,
    shared: Option<(BufRing, *mut u8)>,
    msgs: u64,
    /// `ring`: times the posted entry did not sit where the reaped bytes end.
    entry_mismatch: u64,
    enobufs: u64,
    rearms: u64,
    compactions: u64,
    sends_failed: u64,
}

impl Server {
    fn region_base(&self, c: usize) -> *mut u8 {
        unsafe { self.regions.add(c * self.region) }
    }

    fn push(&mut self, sqe: squeue::Entry) {
        loop {
            match unsafe { self.uring.submission().push(&sqe) } {
                Ok(()) => return,
                Err(_) => {
                    // SQ full: a plain submit (no GETEVENTS) runs no deferred
                    // task work for armed recvs.
                    self.uring.submit().expect("submit");
                }
            }
        }
    }

    fn arm(&mut self, c: usize) {
        let fd = types::Fd(self.conns[c].fd);
        let ud = (TAG_RECV << 56) | c as u64;
        let sqe = match self.strategy {
            Strategy::Shared => opcode::RecvMulti::new(fd, 0).build(),
            Strategy::Ring => opcode::RecvMulti::new(fd, 1 + c as u16).build(),
            Strategy::Oneshot => {
                let conn = &self.conns[c];
                let ptr = unsafe { self.region_base(c).add(conn.tail) };
                opcode::Recv::new(fd, ptr, (self.region - conn.tail) as u32).build()
            }
        };
        self.push(sqe.user_data(ud));
        self.rearms += 1;
    }

    fn ack(&mut self, c: usize) {
        let sqe = opcode::Send::new(types::Fd(self.conns[c].fd), ACK.as_ptr(), 1)
            .build()
            .user_data((TAG_SEND << 56) | c as u64);
        self.push(sqe);
    }

    /// Consume whole messages from `[head, tail)` and ack each one.
    fn consume(&mut self, c: usize) {
        let msg = self.msg;
        let mut n = 0;
        {
            let conn = &mut self.conns[c];
            let len = |conn: &Conn| {
                if conn.ring.is_none() && conn.acc.capacity() > 0 {
                    conn.acc.len() - conn.head
                } else {
                    conn.tail - conn.head
                }
            };
            while len(conn) >= msg {
                conn.head += msg;
                n += 1;
            }
            if n > 0 && self_is_shared(conn) && conn.head == conn.acc.len() {
                conn.acc.clear();
                conn.head = 0;
            }
        }
        for _ in 0..n {
            self.msgs += 1;
            self.ack(c);
        }
    }

    fn on_recv(&mut self, c: usize, res: i32, flags: u32) {
        let more = cqueue::more(flags);
        if res > 0 {
            match self.strategy {
                Strategy::Shared => {
                    let bid = cqueue::buffer_select(flags).expect("shared recv without a buffer");
                    let (ring, backing) = self.shared.as_mut().unwrap();
                    let src = unsafe {
                        std::slice::from_raw_parts(backing.add(bid as usize * SHARED_BUF), res as usize)
                    };
                    self.conns[c].acc.extend_from_slice(src);
                    let addr = unsafe { backing.add(bid as usize * SHARED_BUF) } as u64;
                    ring.push(addr, SHARED_BUF as u32, bid);
                }
                Strategy::Ring => {
                    let conn = &mut self.conns[c];
                    conn.tail += res as usize;
                    if !cqueue::buffer_more(flags) {
                        conn.posted = false;
                        conn.repost = true;
                    }
                }
                Strategy::Oneshot => {
                    self.conns[c].tail += res as usize;
                }
            }
            self.consume(c);
            if self.strategy == Strategy::Oneshot {
                self.conns[c].rearm = true;
            }
            if self.strategy != Strategy::Oneshot && !more {
                self.conns[c].rearm = true;
            }
            return;
        }
        if res == -libc::ENOBUFS {
            self.enobufs += 1;
            self.conns[c].rearm = true;
            if self.strategy == Strategy::Ring {
                self.conns[c].posted = false;
                self.conns[c].repost = true;
            }
            return;
        }
        // EOF or error: the connection is done.
        self.conns[c].dead = true;
    }

    /// After a drained batch, with no enter since (the CQ is settled): move
    /// regions, re-post entries and re-arm.
    fn settle(&mut self, touched: &[usize]) {
        for &c in touched {
            if self.conns[c].dead {
                continue;
            }
            match self.strategy {
                Strategy::Shared => {}
                Strategy::Ring => self.settle_ring(c),
                Strategy::Oneshot => self.settle_oneshot(c),
            }
            if self.conns[c].rearm {
                self.conns[c].rearm = false;
                self.arm(c);
            }
        }
    }

    fn settle_oneshot(&mut self, c: usize) {
        let region = self.region;
        let base = self.region_base(c);
        let conn = &mut self.conns[c];
        if !conn.rearm {
            return; // a recv is still in flight into the region
        }
        if conn.head == conn.tail {
            conn.head = 0;
            conn.tail = 0;
        } else if conn.tail == region {
            let unread = conn.tail - conn.head;
            unsafe { std::ptr::copy(base.add(conn.head), base, unread) };
            conn.head = 0;
            conn.tail = unread;
            self.compactions += 1;
        }
    }

    fn settle_ring(&mut self, c: usize) {
        let region = self.region;
        let base = self.region_base(c);
        let conn = &mut self.conns[c];
        let empty = conn.head == conn.tail;
        if conn.posted && empty && conn.tail > 0 {
            // Move the posted entry back to the front of the region, in place.
            let ring = conn.ring.as_mut().unwrap();
            let e = ring.entry(ring.tail.wrapping_sub(1));
            unsafe {
                if (*e).addr != base as u64 + conn.tail as u64 {
                    self.entry_mismatch += 1;
                }
                (*e).addr = base as u64;
                (*e).len = region as u32;
            }
            conn.head = 0;
            conn.tail = 0;
            return;
        }
        if conn.repost || !conn.posted {
            if empty {
                conn.head = 0;
                conn.tail = 0;
            } else if conn.tail == region {
                let unread = conn.tail - conn.head;
                unsafe { std::ptr::copy(base.add(conn.head), base, unread) };
                conn.head = 0;
                conn.tail = unread;
                self.compactions += 1;
            }
            if conn.tail < region {
                let addr = unsafe { base.add(conn.tail) } as u64;
                conn.ring.as_mut().unwrap().push(addr, (region - conn.tail) as u32, 0);
                conn.posted = true;
            }
            conn.repost = false;
        }
    }
}

fn self_is_shared(conn: &Conn) -> bool {
    conn.ring.is_none() && conn.acc.capacity() > 0
}

fn server(args: &[String]) {
    let addr: String = arg(args, "--addr", None);
    let nconns: usize = arg(args, "--conns", None);
    let msg: usize = arg(args, "--msg-size", None);
    let strategy = match arg::<String>(args, "--strategy", None).as_str() {
        "shared" => Strategy::Shared,
        "ring" => Strategy::Ring,
        "oneshot" => Strategy::Oneshot,
        s => panic!("unknown strategy {s}"),
    };
    let sqpoll = args.iter().any(|a| a == "--sqpoll");
    let shared_bufs: usize = arg(args, "--shared-bufs", Some(256));
    assert!(shared_bufs.is_power_of_two() && shared_bufs <= 32768, "--shared-bufs");
    let warmup: u64 = arg(args, "--warmup", Some(3));
    let duration: u64 = arg(args, "--duration", Some(10));
    let region: usize = arg(args, "--region", Some(msg.max(PAGE).next_multiple_of(PAGE)));
    assert!(region >= msg, "--region must hold one message");
    assert!(!(sqpoll && strategy == Strategy::Ring), "ring is DEFER_TASKRUN only");
    assert!(nconns < 65535, "one bgid per connection");

    let listener = TcpListener::bind(&addr).expect("bind");
    let mut streams = Vec::with_capacity(nconns);
    while streams.len() < nconns {
        let (s, _) = listener.accept().expect("accept");
        s.set_nodelay(true).ok();
        streams.push(s);
    }

    let mut b = IoUring::builder();
    b.setup_cqsize((4 * nconns as u32 + 1024).next_power_of_two().max(4096));
    if sqpoll {
        b.setup_sqpoll(1000);
        if let Some(cpu) = args.iter().position(|a| a == "--sqpoll-cpu") {
            b.setup_sqpoll_cpu(args[cpu + 1].parse().expect("--sqpoll-cpu"));
        }
    } else {
        b.setup_single_issuer().setup_defer_taskrun();
    }
    let uring = b.build(4096).expect("io_uring setup");

    let regions = if strategy == Strategy::Shared { std::ptr::null_mut() } else { mmap_anon(nconns * region) };
    let mut conns: Vec<Conn> = streams
        .iter()
        .map(|s| Conn {
            fd: s.as_raw_fd(),
            head: 0,
            tail: 0,
            acc: if strategy == Strategy::Shared { Vec::with_capacity(PAGE) } else { Vec::new() },
            ring: None,
            posted: false,
            rearm: false,
            repost: false,
            dead: false,
        })
        .collect();

    let mut shared = None;
    match strategy {
        Strategy::Shared => {
            let ring_mem = mmap_anon(shared_bufs * 16);
            let backing = mmap_anon(shared_bufs * SHARED_BUF);
            unsafe {
                uring
                    .submitter()
                    .register_buf_ring_with_flags(ring_mem as u64, shared_bufs as u16, 0, 0)
                    .expect("register shared ring");
            }
            let mut ring = BufRing { base: ring_mem, mask: (shared_bufs - 1) as u16, tail: 0 };
            for i in 0..shared_bufs {
                ring.push(unsafe { backing.add(i * SHARED_BUF) } as u64, SHARED_BUF as u32, i as u16);
            }
            shared = Some((ring, backing));
        }
        Strategy::Ring => {
            let pages = mmap_anon(nconns * PAGE);
            for (i, conn) in conns.iter_mut().enumerate() {
                let ring_mem = unsafe { pages.add(i * PAGE) };
                unsafe {
                    uring
                        .submitter()
                        .register_buf_ring_with_flags(ring_mem as u64, 1, 1 + i as u16, INC)
                        .expect("register connection ring");
                }
                let mut ring = BufRing { base: ring_mem, mask: 0, tail: 0 };
                ring.push(unsafe { regions.add(i * region) } as u64, region as u32, 0);
                conn.ring = Some(ring);
                conn.posted = true;
            }
        }
        Strategy::Oneshot => {}
    }

    let mut srv = Server {
        strategy,
        uring,
        conns,
        msg,
        region,
        regions,
        shared,
        msgs: 0,
        entry_mismatch: 0,
        enobufs: 0,
        rearms: 0,
        compactions: 0,
        sends_failed: 0,
    };
    for c in 0..nconns {
        srv.arm(c);
    }
    srv.uring.submit().expect("submit arms");
    let idle_rss = status_kb("VmRSS:");
    let idle_lck = status_kb("VmLck:");
    let idle_pin = status_kb("VmPin:");

    let start = Instant::now();
    let measure_from = start + Duration::from_secs(warmup);
    let measure_to = measure_from + Duration::from_secs(duration);
    let mut snap: Option<(u64, Duration, Duration, Instant)> = None;
    let mut touched = Vec::with_capacity(nconns);
    let mut seen = vec![false; nconns];
    let mut batch: Vec<(u64, i32, u32)> = Vec::with_capacity(8192);

    loop {
        srv.uring
            .submitter()
            .submit_with_args(1, &types::SubmitArgs::new().timespec(&types::Timespec::new().nsec(100_000_000)))
            .ok();
        batch.clear();
        batch.extend(srv.uring.completion().map(|c| (c.user_data(), c.result(), c.flags())));
        for &(ud, res, flags) in &batch {
            let c = (ud & 0xffff_ffff) as usize;
            match ud >> 56 {
                TAG_RECV => {
                    srv.on_recv(c, res, flags);
                    if !seen[c] {
                        seen[c] = true;
                        touched.push(c);
                    }
                }
                TAG_SEND => {
                    if res < 0 {
                        srv.sends_failed += 1;
                    }
                }
                _ => {}
            }
        }
        srv.settle(&touched);
        for &c in &touched {
            seen[c] = false;
        }
        touched.clear();

        let now = Instant::now();
        if snap.is_none() && now >= measure_from {
            snap = Some((srv.msgs, process_cpu(), sqpoll_cpu(), now));
        }
        if now >= measure_to {
            break;
        }
        if srv.conns.iter().all(|c| c.dead) {
            eprintln!("all connections closed before the window ended");
            break;
        }
    }

    let (m0, cpu0, sq0, t0) = snap.expect("the run ended before the warmup did");
    let secs = t0.elapsed().as_secs_f64();
    let msgs = srv.msgs - m0;
    let cpu = process_cpu() - cpu0;
    let sq = sqpoll_cpu() - sq0;
    println!(
        "RESULT strategy={:?} sqpoll={} shared_bufs={} conns={} msg_size={} region={} msgs_per_sec={:.0} \
         cpu_ns_per_msg={:.0} sqpoll_cpu_ns_per_msg={:.0} gbit_per_sec={:.3} idle_rss_kb={} \
         idle_lck_kb={} idle_pin_kb={} rss_kb={} lck_kb={} pin_kb={} enobufs={} rearms={} \
         compactions={} entry_mismatch={} sends_failed={} dead={}",
        strategy,
        sqpoll,
        shared_bufs,
        nconns,
        msg,
        region,
        msgs as f64 / secs,
        cpu.as_nanos() as f64 / msgs.max(1) as f64,
        sq.as_nanos() as f64 / msgs.max(1) as f64,
        msgs as f64 * msg as f64 * 8.0 / secs / 1e9,
        idle_rss,
        idle_lck,
        idle_pin,
        status_kb("VmRSS:"),
        status_kb("VmLck:"),
        status_kb("VmPin:"),
        srv.enobufs,
        srv.rearms,
        srv.compactions,
        srv.entry_mismatch,
        srv.sends_failed,
        srv.conns.iter().filter(|c| c.dead).count(),
    );
    drop(streams);
}

// ---------------------------------------------------------------- client ---

fn client(args: &[String]) {
    let addr: String = arg(args, "--addr", None);
    let nconns: usize = arg(args, "--conns", None);
    let threads: usize = arg(args, "--threads", Some(4));
    let msg: usize = arg(args, "--msg-size", None);
    let seconds: u64 = arg(args, "--seconds", None);
    let delay_ms: u64 = arg(args, "--start-delay-ms", Some(1000));

    // Connect everything first, so the server's accept loop finishes before
    // load starts and its idle memory snapshot sees no traffic.
    let mut streams: Vec<TcpStream> = Vec::with_capacity(nconns);
    for _ in 0..nconns {
        let s = TcpStream::connect(&addr).expect("connect");
        s.set_nodelay(true).ok();
        streams.push(s);
    }
    std::thread::sleep(Duration::from_millis(delay_ms));

    let done = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let per = nconns.div_ceil(threads);
    let mut handles = Vec::new();
    let mut streams = streams.into_iter();
    for _ in 0..threads {
        let mine: Vec<TcpStream> = streams.by_ref().take(per).collect();
        if mine.is_empty() {
            break;
        }
        let done = done.clone();
        let total = total.clone();
        handles.push(std::thread::spawn(move || client_thread(mine, msg, done, total)));
    }
    std::thread::sleep(Duration::from_secs(seconds));
    done.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    println!(
        "CLIENT msgs={} msgs_per_sec={:.0}",
        total.load(Ordering::Relaxed),
        total.load(Ordering::Relaxed) as f64 / seconds as f64
    );
}

fn client_thread(streams: Vec<TcpStream>, msg: usize, done: Arc<AtomicBool>, total: Arc<AtomicU64>) {
    use mio::{Events, Interest, Poll, Token};
    let payload = vec![b'm'; msg];
    let mut poll = Poll::new().unwrap();
    let mut conns: Vec<(mio::net::TcpStream, usize, bool)> = streams
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            s.set_nonblocking(true).unwrap();
            let mut s = mio::net::TcpStream::from_std(s);
            poll.registry()
                .register(&mut s, Token(i), Interest::READABLE | Interest::WRITABLE)
                .unwrap();
            (s, 0usize, false) // (stream, bytes of the current message sent, awaiting ack)
        })
        .collect();
    let mut events = Events::with_capacity(1024);
    let mut buf = [0u8; 64];
    let mut done_msgs = 0u64;

    // Write as much of the current message as the socket takes.
    let send = |c: &mut (mio::net::TcpStream, usize, bool)| loop {
        if c.2 {
            return;
        }
        match c.0.write(&payload[c.1..]) {
            Ok(n) => {
                c.1 += n;
                if c.1 == payload.len() {
                    c.2 = true;
                    return;
                }
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return,
        }
    };
    for c in conns.iter_mut() {
        send(c);
    }
    while !done.load(Ordering::Relaxed) {
        poll.poll(&mut events, Some(Duration::from_millis(50))).unwrap();
        for ev in events.iter() {
            let c = &mut conns[ev.token().0];
            if ev.is_readable() {
                loop {
                    match c.0.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            done_msgs += n as u64;
                            c.1 = 0;
                            c.2 = false;
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
            }
            send(c);
        }
    }
    total.fetch_add(done_msgs, Ordering::Relaxed);
}
