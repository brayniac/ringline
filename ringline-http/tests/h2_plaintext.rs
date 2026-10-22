//! HTTP/2 over a plaintext connection: `H2AsyncConn::from_conn` on a
//! connection from `ringline::connect`, which on io_uring receives directly
//! into its accumulator. A std-thread stub answers the SETTINGS exchange.
#![allow(clippy::manual_async_fn)]

mod common;

use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder};
use ringline_http::h2_conn::H2AsyncConn;

static SERVER: Mutex<Option<SocketAddr>> = Mutex::new(None);
static RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

/// The client connection preface: the magic string, then a SETTINGS frame.
const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
/// An empty SETTINGS frame: length 0, type 4, no flags, stream 0.
const SETTINGS: [u8; 9] = [0, 0, 0, 4, 0, 0, 0, 0, 0];
/// A SETTINGS frame with the ACK flag.
const SETTINGS_ACK: [u8; 9] = [0, 0, 0, 4, 1, 0, 0, 0, 0];

struct H2Plain;

impl AsyncEventHandler for H2Plain {
    fn on_start(&self) -> Option<Pin<Box<dyn Future<Output = ()> + 'static>>> {
        Some(Box::pin(async {
            let addr = SERVER.lock().unwrap().unwrap();
            let r = async {
                let conn = ringline::connect(addr)
                    .await
                    .map_err(|e| format!("connect: {e}"))?;
                let mut h2 =
                    ringline::timeout(Duration::from_secs(5), H2AsyncConn::from_conn(conn))
                        .await
                        .map_err(|_| "from_conn did not resolve".to_string())?
                        .map_err(|e| format!("from_conn: {e}"))?;
                h2.close();
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
        H2Plain
    }
}

#[test]
fn h2_completes_the_settings_exchange_over_plaintext() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    *SERVER.lock().unwrap() = Some(listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        // The preface and the client's SETTINGS frame header.
        let mut got = Vec::new();
        let mut buf = [0u8; 1024];
        while got.len() < PREFACE.len() + 9 {
            match s.read(&mut buf) {
                Ok(0) | Err(_) => return,
                Ok(n) => got.extend_from_slice(&buf[..n]),
            }
        }
        assert_eq!(&got[..PREFACE.len()], PREFACE);
        // A failed write means the client gave up; its result says why.
        if s.write_all(&SETTINGS).is_err() || s.write_all(&SETTINGS_ACK).is_err() {
            return;
        }
        // Hold the connection until the client closes it.
        let _ = s.read(&mut buf);
    });
    let config = ConfigBuilder::new()
        .workers(1)
        .pin_to_core(false)
        .max_connections(16)
        .build()
        .expect("config");
    let (runtime, handles) = RinglineBuilder::new(config)
        .launch::<H2Plain>()
        .expect("launch");
    common::join_workers(&runtime, handles);
    server.join().unwrap();
    assert_eq!(
        RESULT
            .lock()
            .unwrap()
            .take()
            .expect("client did not finish"),
        Ok(())
    );
}
