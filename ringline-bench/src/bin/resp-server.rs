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
#[command(name = "resp-server", about = "RESP GET/SET server, one per runtime arm")]
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
}

fn main() {
    let args = Args::parse();
    eprintln!(
        "resp-server: runtime={:?} workers={} on_demand={} incremental={} addr={}",
        args.runtime, args.workers, args.recv_on_demand, args.recv_incremental, args.addr
    );
    match args.runtime {
        Runtime::Ringline => ringline_arm::run(&args),
        Runtime::TokioPc => tokio_pc::run(args.addr, args.workers),
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

mod ringline_arm {
    use super::*;
    use ringline::{AsyncEventHandler, ConfigBuilder, Connection, ParseResult, RinglineBuilder};

    struct Handler;

    impl AsyncEventHandler for Handler {
        fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
            async move {
                let (mut tx, mut rx) = conn.split();
                let mut out: Vec<u8> = Vec::with_capacity(64 << 10);
                loop {
                    let mut bad = false;
                    let n = rx
                        .with_data(|d| match resp::process(d, &mut out) {
                            Ok(0) => ParseResult::NeedMore,
                            Ok(n) => ParseResult::Consumed(n),
                            Err(()) => {
                                bad = true;
                                ParseResult::Consumed(d.len().max(1))
                            }
                        })
                        .await;
                    if n == 0 || bad {
                        break;
                    }
                    if !out.is_empty() {
                        // Copy into the send pool; wait for room if it is full.
                        if tx.send_nowait(&out).is_err() && tx.send_backpressured(&out).await.is_err() {
                            break;
                        }
                        out.clear();
                    }
                }
            }
        }
        fn create_for_worker(_id: usize) -> Self {
            Handler
        }
    }

    pub fn run(args: &Args) {
        let config = ConfigBuilder::new()
            .workers(args.workers)
            .pin_to_core(true)
            .sq_entries(4096)
            .max_connections(8192)
            .send_pool(16384, 16384)
            .recv_incremental(args.recv_incremental)
            .recv_on_demand(args.recv_on_demand)
            .build()
            .expect("valid config");
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

    pub fn run(addr: SocketAddr, workers: usize) {
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
                        tokio::spawn(serve(stream));
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

    async fn serve(mut stream: tokio::net::TcpStream) {
        let mut inbuf = BytesMut::with_capacity(64 << 10);
        let mut out: Vec<u8> = Vec::with_capacity(64 << 10);
        loop {
            match stream.read_buf(&mut inbuf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            match resp::process(&inbuf, &mut out) {
                Ok(n) => inbuf.advance(n),
                Err(()) => return,
            }
            if !out.is_empty() {
                if stream.write_all(&out).await.is_err() {
                    return;
                }
                out.clear();
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
