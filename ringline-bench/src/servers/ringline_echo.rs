#![allow(clippy::manual_async_fn)]

use std::net::SocketAddr;
use std::thread::JoinHandle;

use ringline::{AsyncEventHandler, ConfigBuilder, Connection, RinglineBuilder, ShutdownHandle};
// ParseResult is only needed in the non-io_uring fallback path.
#[cfg(not(has_io_uring))]
use ringline::ParseResult;

struct EchoHandler;

impl AsyncEventHandler for EchoHandler {
    fn on_accept(&self, conn: Connection) -> impl std::future::Future<Output = ()> + 'static {
        async move {
            // Direct-echo path: on io_uring, echo SQEs are submitted directly
            // from the CQE handler without waking this task — eliminating
            // the collect_wakeups → poll_ready_tasks roundtrip per message.
            // Falls back to the forward_recv_buf loop on non-io_uring builds.
            //
            // Only the fallback needs the halves: `run_direct_echo` is a
            // whole-connection operation that belongs to neither, so the
            // split lives in the branch that actually uses it.
            #[cfg(has_io_uring)]
            {
                // No `return` needed: the fallback below is cfg'd out whenever
                // this arm is compiled in. (Never linted before #402, because
                // this block was dead on every platform.)
                conn.as_conn().run_direct_echo().await;
            }
            #[cfg(not(has_io_uring))]
            {
                let (mut tx, mut rx) = conn.split();
                loop {
                    let n = rx
                        .with_data(|data| {
                            if let Err(e) = tx.forward_recv_buf(data) {
                                eprintln!("echo: forward_recv_buf failed: {e}");
                                return ParseResult::NeedMore;
                            }
                            ParseResult::Consumed(data.len())
                        })
                        .await;
                    if n == 0 {
                        break;
                    }
                }
            }
        }
    }

    fn create_for_worker(_worker_id: usize) -> Self {
        EchoHandler
    }
}

pub struct RinglineServer {
    shutdown: ShutdownHandle,
    handles: Vec<JoinHandle<Result<(), ringline::Error>>>,
}

impl RinglineServer {
    pub fn start(
        addr: SocketAddr,
        workers: usize,
        msg_size: usize,
    ) -> Result<Self, ringline::Error> {
        let config = ConfigBuilder::new()
            .workers(workers)
            .pin_to_core(false)
            .sq_entries(4096)
            .recv_buffer(4096, msg_size.next_power_of_two().max(4096) as u32)
            .max_connections(4096)
            .send_pool(4096, msg_size.next_power_of_two().max(4096) as u32)
            .build()
            .expect("valid config");

        let (shutdown, handles) = RinglineBuilder::new(config)
            .bind(addr)
            .launch::<EchoHandler>()?;

        Ok(RinglineServer { shutdown, handles })
    }

    pub fn stop(self) {
        self.shutdown.shutdown();
        for h in self.handles {
            h.join().ok();
        }
    }
}
