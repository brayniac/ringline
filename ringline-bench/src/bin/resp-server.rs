//! RESP GET/SET server for measurement 9 of `docs/io-api-redesign.md`: one
//! parser and store (`ringline_bench::resp`), served by three runtimes.
//!
//!   resp-server --runtime ringline     --addr 0.0.0.0:6379 --workers 8
//!   resp-server --runtime ringline     --addr 0.0.0.0:6379 --workers 8 --recv-on-demand
//!   resp-server --runtime tokio-pc     --addr 0.0.0.0:6379 --workers 8
//!   resp-server --runtime tokio-uring  --addr 0.0.0.0:6379 --workers 8   (feature tokio-uring-arm)
//!
//! Every arm: one worker per core, pinned, its own store; parse every
//! complete command that has arrived, append every response to one output
//! buffer, and write it once per batch.
#![allow(clippy::manual_async_fn)]

use std::net::SocketAddr;

use clap::{Parser, ValueEnum};
use ringline_bench::resp;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum, Debug)]
enum Runtime {
    /// ringline's current API: `with_data` (lend path) and `send_nowait`.
    Ringline,
    /// One tokio `current_thread` runtime per core, `SO_REUSEPORT`
    /// listeners, `read_buf` into a `BytesMut`, `write_all`.
    TokioPc,
    /// tokio-uring per core: owned-buffer `read` into the accumulator's
    /// spare capacity, `write_all`.
    TokioUring,
}

#[derive(Parser)]
#[command(
    name = "resp-server",
    about = "RESP GET/SET server, one per runtime arm"
)]
struct Args {
    #[arg(long, value_enum)]
    runtime: Runtime,
    #[arg(long, default_value = "0.0.0.0:6379")]
    addr: SocketAddr,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// ringline only: arm one-shot receives on every connection
    /// (`ConfigBuilder::recv_on_demand`).
    #[arg(long)]
    recv_on_demand: bool,
    /// ringline only: incremental provided-buffer ring.
    #[arg(long)]
    recv_incremental: bool,
    /// ringline only: provided receive buffers per worker (16 KiB each);
    /// 0 keeps the default.
    #[arg(long, default_value_t = 0)]
    recv_buffers: u16,
    /// Send GET values of at least this many bytes without copying them
    /// (ringline: `send_parts` guard; tokio: `write_vectored`). 0 = copy all.
    #[arg(long, default_value_t = 0)]
    zc_get: usize,
    /// Receive SET values of at least this many bytes straight into the value
    /// buffer (ringline: `set_recv_sink`; tokio: `read_exact`). 0 = parse in
    /// place from the receive buffer.
    #[arg(long, default_value_t = 0)]
    stream_sets: usize,
}

impl Args {
    fn opts(&self) -> resp::Opts {
        resp::Opts {
            zc_min: self.zc_get,
            stream_min: self.stream_sets,
        }
    }
}

fn main() {
    let args = Args::parse();
    eprintln!(
        "resp-server: runtime={:?} workers={} on_demand={} incremental={} zc_get={} stream_sets={} addr={}",
        args.runtime,
        args.workers,
        args.recv_on_demand,
        args.recv_incremental,
        args.zc_get,
        args.stream_sets,
        args.addr
    );
    diag::spawn(matches!(args.runtime, Runtime::Ringline));
    match args.runtime {
        Runtime::Ringline => ringline_arm::run(&args),
        Runtime::TokioPc => tokio_pc::run(args.addr, args.workers, args.opts()),
        Runtime::TokioUring => {
            #[cfg(all(target_os = "linux", feature = "tokio-uring-arm"))]
            tokio_uring_arm::run(args.addr, args.workers);
            #[cfg(not(all(target_os = "linux", feature = "tokio-uring-arm")))]
            panic!("built without --features tokio-uring-arm");
        }
    }
}

fn pin(core: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = core;
}

/// A `std` listener with `SO_REUSEADDR` and `SO_REUSEPORT`, nonblocking.
fn reuseport_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.set_reuseport(true)?;
    socket.bind(addr)?;
    // `TcpSocket::listen` needs a tokio reactor; go through std instead.
    use std::os::fd::{FromRawFd, IntoRawFd};
    let fd = socket.into_raw_fd();
    if unsafe { libc::listen(fd, 4096) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let l = unsafe { std::net::TcpListener::from_raw_fd(fd) };
    l.set_nonblocking(true)?;
    Ok(l)
}

/// Fairness and backpressure diagnostics, logged every 10 s.
mod diag {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use std::sync::{Arc, Mutex};

    pub static CONNS: Mutex<Vec<Arc<AtomicU64>>> = Mutex::new(Vec::new());
    pub static BACKPRESSURED: AtomicU64 = AtomicU64::new(0);
    pub static BP_MAX_US: AtomicU64 = AtomicU64::new(0);

    pub fn register() -> Arc<AtomicU64> {
        let c = Arc::new(AtomicU64::new(0));
        CONNS.lock().unwrap().push(c.clone());
        c
    }

    pub fn note_wait(us: u64) {
        BACKPRESSURED.fetch_add(1, Relaxed);
        BP_MAX_US.fetch_max(us, Relaxed);
    }

    pub fn spawn(ringline: bool) {
        std::thread::spawn(move || {
            // Keyed by counter address. A connection whose task has ended
            // dropped its handle, so only the registry holds the counter;
            // it is counted for the interval it closed in and then removed.
            let mut prev: std::collections::HashMap<usize, u64> = Default::default();
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                let mut next = std::collections::HashMap::new();
                let mut d: Vec<u64> = Vec::new();
                CONNS.lock().unwrap().retain(|c| {
                    let key = Arc::as_ptr(c) as usize;
                    let v = c.load(Relaxed);
                    d.push(v - prev.get(&key).copied().unwrap_or(0));
                    let open = Arc::strong_count(c) > 1;
                    if open {
                        next.insert(key, v);
                    }
                    open
                });
                prev = next;
                d.sort_unstable();
                let q = |f: f64| {
                    d.get(((d.len() as f64 - 1.0) * f) as usize)
                        .copied()
                        .unwrap_or(0)
                };
                let zero = d.iter().filter(|&&v| v == 0).count();
                let mut line = format!(
                    "diag: conns={} ops/conn/10s min={} p1={} p10={} p50={} max={} zero={} backpressured={} bp_max_us={}",
                    d.len(),
                    q(0.0),
                    q(0.01),
                    q(0.10),
                    q(0.50),
                    q(1.0),
                    zero,
                    BACKPRESSURED.swap(0, Relaxed),
                    BP_MAX_US.swap(0, Relaxed)
                );
                if ringline {
                    use ringline::metrics::{POOL, pool};
                    let v = |i| POOL.value(i).unwrap_or(0);
                    line += &format!(
                        " ring_empty={} recv_parked={} recv_fallback={} send_exhausted={}",
                        v(pool::BUFFER_RING_EMPTY),
                        v(pool::RECV_PARKED),
                        v(pool::RECV_FALLBACK),
                        v(pool::SEND_EXHAUSTED)
                    );
                }
                eprintln!("{line}");
            }
        });
    }
}

mod ringline_arm {
    use super::*;
    use ringline::{
        AsyncEventHandler, ConfigBuilder, Connection, GuardBox, ParseResult, RegionId,
        RinglineBuilder, SendGuard, SendPart,
    };
    use std::sync::OnceLock;

    static OPTS: OnceLock<resp::Opts> = OnceLock::new();

    struct BytesGuard(bytes::Bytes);
    impl SendGuard for BytesGuard {
        fn as_ptr_len(&self) -> (*const u8, u32) {
            (self.0.as_ptr(), self.0.len() as u32)
        }
        fn region(&self) -> RegionId {
            RegionId::UNREGISTERED
        }
    }

    /// Send `out`: copy parts and zero-copy values, in order, in batches of
    /// at most 8 guards and 32 parts. Falls back to one copied send if a
    /// batch is refused.
    async fn flush(tx: &mut ringline::SendHalf, out: &mut resp::Out) -> bool {
        if out.vals.is_empty() {
            let ok = tx.send_nowait(&out.buf).is_ok() || {
                let t = std::time::Instant::now();
                let r = tx.send_backpressured(&out.buf).await.is_ok();
                crate::diag::note_wait(t.elapsed().as_micros() as u64);
                r
            };
            out.clear();
            return ok;
        }
        let mut parts: Vec<SendPart<'_>> = Vec::new();
        let mut guards = 0;
        let mut at = 0;
        let mut ok = true;
        for (off, v) in out.vals.iter() {
            if *off > at {
                parts.push(SendPart::Copy(&out.buf[at..*off]));
            }
            parts.push(SendPart::Guard(GuardBox::new(BytesGuard(v.clone()))));
            guards += 1;
            at = *off;
            if guards == 8 || parts.len() >= 30 {
                if tx
                    .send_parts()
                    .submit_batch(std::mem::take(&mut parts))
                    .is_err()
                {
                    ok = false;
                    break;
                }
                guards = 0;
            }
        }
        if ok && at < out.buf.len() {
            parts.push(SendPart::Copy(&out.buf[at..]));
        }
        if ok && !parts.is_empty() && tx.send_parts().submit_batch(parts).is_err() {
            ok = false;
        }
        out.clear();
        ok
    }

    struct Handler;

    impl AsyncEventHandler for Handler {
        fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let opts = *OPTS.get().expect("opts");
                let ctx = conn.as_conn();
                let (mut tx, mut rx) = conn.split();
                let mut out = resp::Out::default();
                let served = crate::diag::register();
                loop {
                    let mut bad = false;
                    let mut body: Option<(Vec<u8>, usize)> = None;
                    let n = rx
                        .with_data(|d| match resp::process_ext(d, &mut out, &opts) {
                            Ok(resp::Step::Done(0)) => ParseResult::NeedMore,
                            Ok(resp::Step::Done(n)) => ParseResult::Consumed(n),
                            Ok(resp::Step::Body { consumed, key, len }) => {
                                body = Some((key, len));
                                ParseResult::Consumed(consumed)
                            }
                            Err(()) => {
                                bad = true;
                                ParseResult::Consumed(d.len().max(1))
                            }
                        })
                        .await;
                    if n == 0 || bad {
                        break;
                    }
                    if let Some((key, len)) = body {
                        // value + CRLF
                        let need = len + 2;
                        let mut val: Vec<u8> = Vec::with_capacity(need);
                        // Bytes that arrived with the header.
                        rx.try_with_data(|d| {
                            let take = d.len().min(need);
                            val.extend_from_slice(&d[..take]);
                            ParseResult::Consumed(take)
                        });
                        while val.len() < need {
                            let have = val.len();
                            unsafe {
                                ctx.set_recv_sink(val.as_mut_ptr().add(have), need - have);
                            }
                            rx.recv_ready().await;
                            let got = rx.take_recv_sink();
                            unsafe { val.set_len(have + got) };
                            if got == 0 {
                                let mut moved = 0;
                                let r = rx.try_with_data(|d| {
                                    let take = d.len().min(need - val.len());
                                    val.extend_from_slice(&d[..take]);
                                    moved = take;
                                    ParseResult::Consumed(take)
                                });
                                if moved == 0 && r.is_none() {
                                    return;
                                }
                            }
                        }
                        if &val[len..] != b"\r\n" {
                            return;
                        }
                        val.truncate(len);
                        resp::store_set(key, bytes::Bytes::from(val), &mut out);
                    }
                    if !out.is_empty() {
                        served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if !flush(&mut tx, &mut out).await {
                            break;
                        }
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            Handler
        }
    }

    pub fn run(args: &Args) {
        OPTS.set(args.opts()).ok();
        let mut builder = ConfigBuilder::new()
            .workers(args.workers)
            .pin_to_core(true)
            .sq_entries(4096)
            .max_connections(8192)
            .send_pool(16384, 16384)
            .recv_incremental(args.recv_incremental)
            .recv_on_demand(args.recv_on_demand);
        if args.recv_buffers != 0 {
            builder = builder.recv_buffer(args.recv_buffers, 16384);
        }
        let config = builder.build().expect("valid config");
        let (_shutdown, handles) = RinglineBuilder::new(config)
            .bind(args.addr)
            .launch::<Handler>()
            .expect("launch");
        eprintln!("resp-server: ready (ringline x{})", args.workers);
        for h in handles {
            let _ = h.join();
        }
    }
}

mod tokio_pc {
    use super::*;
    use bytes::{Buf, BytesMut};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub fn run(addr: SocketAddr, workers: usize, opts: resp::Opts) {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let mut handles = Vec::new();
        for core in 0..workers {
            let ready_tx = ready_tx.clone();
            handles.push(std::thread::spawn(move || {
                pin(core);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("runtime");
                rt.block_on(async move {
                    let std_l = reuseport_listener(addr).expect("bind");
                    let listener = tokio::net::TcpListener::from_std(std_l).expect("listener");
                    ready_tx.send(()).ok();
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            continue;
                        };
                        let _ = stream.set_nodelay(true);
                        tokio::spawn(serve(stream, opts));
                    }
                });
            }));
        }
        for _ in 0..workers {
            ready_rx.recv().expect("worker died before binding");
        }
        eprintln!("resp-server: ready (tokio per-core x{workers})");
        for h in handles {
            let _ = h.join();
        }
    }

    /// Write `out`: one `write_all` when every value was copied in, otherwise
    /// vectored writes that send the stored values without a copy.
    async fn flush(stream: &mut tokio::net::TcpStream, out: &mut resp::Out) -> bool {
        if out.vals.is_empty() {
            let ok = stream.write_all(&out.buf).await.is_ok();
            out.clear();
            return ok;
        }
        let mut segs: Vec<&[u8]> = Vec::with_capacity(out.vals.len() * 2 + 1);
        let mut at = 0;
        for (off, v) in out.vals.iter() {
            if *off > at {
                segs.push(&out.buf[at..*off]);
            }
            segs.push(&v[..]);
            at = *off;
        }
        if at < out.buf.len() {
            segs.push(&out.buf[at..]);
        }
        let mut i = 0;
        let mut skip = 0;
        let mut ok = true;
        while i < segs.len() {
            let mut iov: Vec<std::io::IoSlice<'_>> = Vec::with_capacity(64);
            iov.push(std::io::IoSlice::new(&segs[i][skip..]));
            for s in segs.iter().skip(i + 1).take(63) {
                iov.push(std::io::IoSlice::new(s));
            }
            match stream.write_vectored(&iov).await {
                Ok(0) | Err(_) => {
                    ok = false;
                    break;
                }
                Ok(mut n) => {
                    while n > 0 && i < segs.len() {
                        let left = segs[i].len() - skip;
                        if n >= left {
                            n -= left;
                            i += 1;
                            skip = 0;
                        } else {
                            skip += n;
                            n = 0;
                        }
                    }
                }
            }
        }
        out.clear();
        ok
    }

    async fn serve(mut stream: tokio::net::TcpStream, opts: resp::Opts) {
        let mut inbuf = BytesMut::with_capacity(64 << 10);
        let mut out = resp::Out::default();
        let served = crate::diag::register();
        loop {
            match stream.read_buf(&mut inbuf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            loop {
                match resp::process_ext(&inbuf, &mut out, &opts) {
                    Ok(resp::Step::Done(n)) => {
                        inbuf.advance(n);
                        break;
                    }
                    Ok(resp::Step::Body { consumed, key, len }) => {
                        inbuf.advance(consumed);
                        // value + CRLF, straight into the value's buffer
                        let need = len + 2;
                        let mut val = vec![0u8; need];
                        let have = inbuf.len().min(need);
                        val[..have].copy_from_slice(&inbuf[..have]);
                        inbuf.advance(have);
                        if have < need && stream.read_exact(&mut val[have..]).await.is_err() {
                            return;
                        }
                        if &val[len..] != b"\r\n" {
                            return;
                        }
                        val.truncate(len);
                        resp::store_set(key, bytes::Bytes::from(val), &mut out);
                    }
                    Err(()) => return,
                }
            }
            if !out.is_empty() {
                served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !flush(&mut stream, &mut out).await {
                    return;
                }
            }
            if inbuf.capacity() - inbuf.len() < 16 << 10 {
                inbuf.reserve(64 << 10);
            }
        }
    }
}

#[cfg(all(target_os = "linux", feature = "tokio-uring-arm"))]
mod tokio_uring_arm {
    use super::*;
    use tokio_uring::buf::BoundedBuf;

    pub fn run(addr: SocketAddr, workers: usize) {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let mut handles = Vec::new();
        for core in 0..workers {
            let ready_tx = ready_tx.clone();
            handles.push(std::thread::spawn(move || {
                pin(core);
                let std_l = reuseport_listener(addr).expect("bind");
                std_l.set_nonblocking(false).ok();
                tokio_uring::start(async move {
                    let listener = tokio_uring::net::TcpListener::from_std(std_l);
                    ready_tx.send(()).ok();
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            continue;
                        };
                        set_nodelay(&stream);
                        tokio_uring::spawn(serve(stream));
                    }
                });
            }));
        }
        for _ in 0..workers {
            ready_rx.recv().expect("worker died before binding");
        }
        eprintln!("resp-server: ready (tokio-uring x{workers})");
        for h in handles {
            let _ = h.join();
        }
    }

    fn set_nodelay(stream: &tokio_uring::net::TcpStream) {
        use std::os::fd::AsRawFd;
        let on: libc::c_int = 1;
        unsafe {
            libc::setsockopt(
                stream.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &on as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    /// The accumulator is one `Vec`: each read fills its spare capacity, so
    /// received bytes land where the parser reads them, with no copy.
    async fn serve(stream: tokio_uring::net::TcpStream) {
        let mut acc: Vec<u8> = Vec::with_capacity(128 << 10);
        let mut pos = 0;
        let mut out: Vec<u8> = Vec::with_capacity(64 << 10);
        loop {
            if acc.capacity() - acc.len() < 16 << 10 {
                // Move the unparsed tail to the front before growing, as
                // `BytesMut::reserve` does for the tokio arm.
                acc.drain(..pos);
                pos = 0;
                acc.reserve(64 << 10);
            }
            let start = acc.len();
            let cap = acc.capacity();
            let (res, slice) = stream.read(acc.slice(start..cap)).await;
            acc = slice.into_inner();
            match res {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            match resp::process(&acc[pos..], &mut out) {
                Ok(n) => {
                    pos += n;
                    if pos == acc.len() {
                        acc.clear();
                        pos = 0;
                    }
                }
                Err(()) => return,
            }
            if !out.is_empty() {
                let (res, b) = stream.write_all(out).await;
                out = b;
                if res.is_err() {
                    return;
                }
                out.clear();
            }
        }
    }
}
