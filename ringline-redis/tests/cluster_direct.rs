//! `ClusterConfig::recv_direct`: a cluster client opens its node connections
//! with `TcpConnect::recv_direct`.
//!
//! A std-thread stub answers `CLUSTER SLOTS` with one slot range on itself
//! and every `GET` with `hello`. The client reads a value through the cluster
//! client, then checks that a segment read on the node's connection is
//! refused, which happens only on a direct connection. io_uring only: the
//! segment read does not exist on mio.
#![cfg(has_io_uring)]
#![allow(clippy::manual_async_fn)]

mod common;

use std::future::Future;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::Mutex;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};
use ringline_redis::{ClusterClient, ClusterConfig, Error};

static SERVER: Mutex<Option<SocketAddr>> = Mutex::new(None);
static RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

/// Read one RESP array of bulk strings, or `None` at EOF.
fn read_command(r: &mut BufReader<TcpStream>) -> Option<Vec<Vec<u8>>> {
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let n: usize = line.trim_end().strip_prefix('*')?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        line.clear();
        r.read_line(&mut line).ok()?;
        let len: usize = line.trim_end().strip_prefix('$')?.parse().ok()?;
        let mut arg = vec![0u8; len + 2];
        r.read_exact(&mut arg).ok()?;
        arg.truncate(len);
        args.push(arg);
    }
    Some(args)
}

/// Serve one connection until it closes.
fn serve_conn(stream: TcpStream, port: u16) {
    let mut w = stream.try_clone().unwrap();
    let mut r = BufReader::new(stream);
    while let Some(args) = read_command(&mut r) {
        let reply = match args.first().map(|a| a.to_ascii_uppercase()) {
            Some(cmd) if cmd == b"CLUSTER" => {
                format!("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{port}\r\n")
                    .into_bytes()
            }
            Some(cmd) if cmd == b"GET" => b"$5\r\nhello\r\n".to_vec(),
            _ => b"-ERR unknown command\r\n".to_vec(),
        };
        if w.write_all(&reply).is_err() {
            return;
        }
    }
}

struct ClusterDirect;

impl AsyncEventHandler for ClusterDirect {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let mut cluster =
                    ClusterClient::new(ClusterConfig::new(vec![addr]).recv_direct(true));
                cluster
                    .connect()
                    .await
                    .map_err(|e| format!("cluster connect: {e}"))?;
                let v = cluster
                    .get("k")
                    .await
                    .map_err(|e| format!("cluster get: {e}"))?
                    .ok_or("cluster get: None")?;
                if v.as_ref() != b"hello" {
                    return Err(format!("cluster get read {v:?}"));
                }
                let client = cluster
                    .node_client(&addr.to_string())
                    .await
                    .map_err(|e| format!("node_client: {e}"))?;
                match client.get_stream(b"k").await {
                    Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::Unsupported => {}
                    Err(e) => return Err(format!("get_stream: {e}")),
                    Ok(_) => return Err("get_stream accepted, so not direct".into()),
                }
                cluster.close_all();
                Ok(())
            }
            .await;
            *RESULT.lock().unwrap() = Some(r);
            let _ = ringline::request_shutdown();
        }))
    }
    fn on_accept(&self, _conn: Connection) -> impl Future<Output = ()> + 'static {
        async {}
    }
    fn create_for_worker(_id: usize) -> Self {
        ClusterDirect
    }
}

#[test]
fn a_cluster_client_with_recv_direct_opens_direct_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    *SERVER.lock().unwrap() = Some(addr);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            std::thread::spawn(move || serve_conn(stream, addr.port()));
        }
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<ClusterDirect>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    assert_eq!(
        RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(())
    );
}
