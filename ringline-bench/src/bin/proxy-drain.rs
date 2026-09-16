//! Sink for the proxy A/B: accept, read, discard.
//!
//! Deliberately dumb — blocking reads on a thread per connection. It exists to
//! not be the bottleneck while the proxy under test is measured, so it does
//! the least work that can absorb a stream.

use std::io::Read;
use std::net::{SocketAddr, TcpListener};

use clap::Parser;

#[derive(Parser)]
#[command(about = "Drain sink for the proxy forwarding A/B")]
struct Args {
    #[arg(long, default_value = "0.0.0.0:7900")]
    addr: SocketAddr,
    /// Read buffer size. Larger than the proxy's forward chunk so a read is
    /// never the thing that splits a chunk.
    #[arg(long, default_value_t = 256 * 1024)]
    buf: usize,
}

fn main() {
    let args = Args::parse();
    let listener = TcpListener::bind(args.addr).expect("bind");
    eprintln!("proxy-drain: ready on {}", args.addr);
    for stream in listener.incoming() {
        let Ok(mut s) = stream else { continue };
        let buf_size = args.buf;
        std::thread::spawn(move || {
            s.set_nodelay(true).ok();
            let mut buf = vec![0u8; buf_size];
            let mut total: u64 = 0;
            loop {
                match s.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => total += n as u64,
                }
            }
            let _ = total;
        });
    }
}
