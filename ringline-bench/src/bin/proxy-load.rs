//! One-way load generator for the proxy forwarding A/B.
//!
//! Writes continuously and never reads, because the workload under test is
//! one-way relay: client -> proxy -> drain. The proxy's forward path is the
//! only thing between the two, which is the point — a request/response client
//! would put a response path in the measurement that `forward_to` has no part
//! in.
//!
//! Throughput is bytes accepted by the kernel over the measurement window.
//! Blocking writes provide the backpressure: when the proxy stops forwarding,
//! the socket buffer fills and `write_all` blocks, so a stalled proxy shows up
//! as a lower rate rather than as unbounded client-side buffering.

use std::io::Write;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;

#[derive(Parser)]
#[command(about = "One-way load generator for the proxy forwarding A/B")]
struct Args {
    #[arg(long)]
    addr: String,
    #[arg(long, default_value_t = 64)]
    clients: usize,
    #[arg(long, default_value_t = 16384)]
    msg_size: usize,
    #[arg(long, default_value_t = 20)]
    duration: u64,
    #[arg(long, default_value_t = 5)]
    warmup: u64,
    #[arg(long, default_value_t = 16)]
    threads: usize,
}

fn main() {
    let args = Args::parse();
    let stop = Arc::new(AtomicBool::new(false));
    // Counted separately so warmup bytes are excluded rather than subtracted:
    // the rate during warmup is not the rate being reported.
    let measuring = Arc::new(AtomicBool::new(false));
    let bytes = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));

    let per_thread = args.clients.div_ceil(args.threads.max(1));
    let mut handles = Vec::new();
    for t in 0..args.threads {
        let first = t * per_thread;
        let n = per_thread.min(args.clients.saturating_sub(first));
        if n == 0 {
            break;
        }
        let (addr, stop, measuring, bytes, writes) = (
            args.addr.clone(),
            stop.clone(),
            measuring.clone(),
            bytes.clone(),
            writes.clone(),
        );
        let msg_size = args.msg_size;
        handles.push(std::thread::spawn(move || {
            let mut conns: Vec<TcpStream> = (0..n)
                .filter_map(|_| {
                    let s = TcpStream::connect(&addr).ok()?;
                    s.set_nodelay(true).ok();
                    Some(s)
                })
                .collect();
            let payload = vec![0xA5u8; msg_size];
            while !stop.load(Ordering::Relaxed) {
                for c in conns.iter_mut() {
                    if c.write_all(&payload).is_err() {
                        stop.store(true, Ordering::Relaxed);
                        return;
                    }
                    if measuring.load(Ordering::Relaxed) {
                        bytes.fetch_add(msg_size as u64, Ordering::Relaxed);
                        writes.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    std::thread::sleep(Duration::from_secs(args.warmup));
    measuring.store(true, Ordering::Relaxed);
    let t0 = Instant::now();
    std::thread::sleep(Duration::from_secs(args.duration));
    let elapsed = t0.elapsed().as_secs_f64();
    measuring.store(false, Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);

    let b = bytes.load(Ordering::Relaxed);
    let w = writes.load(Ordering::Relaxed);
    println!(
        "{}",
        serde_json::json!({
            "clients": args.clients,
            "msg_size": args.msg_size,
            "duration_secs": elapsed,
            "bytes": b,
            "writes": w,
            "bytes_per_sec": b as f64 / elapsed,
            "gbit_per_sec": (b as f64 * 8.0 / elapsed) / 1e9,
            "writes_per_sec": w as f64 / elapsed,
        })
    );
    for h in handles {
        let _ = h.join();
    }
}
