//! Connection pool for ringline-redis.
//!
//! `Pool` manages a fixed set of backend connections with round-robin dispatch
//! and lazy reconnection. It is single-threaded (no Arc, no Mutex) — designed
//! for use within a single ringline async worker.
//!
//! # Usage
//!
//! ```no_run
//! # use std::net::SocketAddr;
//! # use ringline_redis::{Pool, PoolConfig};
//! # async fn example() -> Result<(), ringline_redis::Error> {
//! let config = PoolConfig::new("127.0.0.1:6379".parse().unwrap(), 4);
//! let mut pool = Pool::new(config);
//! pool.connect_all().await?;
//! pool.client().await?.set("k", "v").await?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;

use ringline::ConnCtx;

#[cfg(has_io_uring)]
use crate::ValueStream;
use crate::{Client, Error};

/// Configuration for a connection pool.
pub struct PoolConfig {
    /// Backend address to connect to.
    pub(crate) addr: SocketAddr,
    /// Number of connections in the pool.
    pub(crate) pool_size: usize,
    /// Connect timeout in milliseconds. 0 means no timeout.
    pub(crate) connect_timeout_ms: u64,
    /// TLS server name (SNI) for outbound connections. `None` means plain TCP.
    pub(crate) tls_server_name: Option<String>,
    /// Password for AUTH after connect. `None` skips authentication.
    pub(crate) password: Option<String>,
    /// Username for ACL-based AUTH (Redis 6.0+). Only used when `password` is set.
    pub(crate) username: Option<String>,
}

impl PoolConfig {
    /// Create a pool config for `pool_size` connections to `addr`.
    /// Defaults: no connect timeout, plain TCP (no TLS).
    pub fn new(addr: SocketAddr, pool_size: usize) -> Self {
        Self {
            addr,
            pool_size,
            connect_timeout_ms: 0,
            tls_server_name: None,
            password: None,
            username: None,
        }
    }

    /// Set the connect timeout in milliseconds. `0` (the default) disables it.
    pub fn connect_timeout_ms(mut self, ms: u64) -> Self {
        self.connect_timeout_ms = ms;
        self
    }

    /// Set the TLS server name (SNI) for outbound connections; enables TLS.
    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.tls_server_name = Some(name.into());
        self
    }

    /// Set the password for AUTH after connect.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set the username for ACL-based AUTH (Redis 6.0+). Only used when a
    /// password is set.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }
}

enum Slot {
    /// A pooled connection *is* a client.
    ///
    /// The slot used to hold a bare `ConnCtx` and mint a throwaway `Client` per
    /// checkout, which once the halves became claims meant a `split()` — two
    /// driver round trips and two claim writes — on every call. `ShardedClient`
    /// and `ClusterClient` moved to owning the client for exactly that reason
    /// (#439, #440); `connect` resolving to a non-`Copy` `Connection` (#528)
    /// finishes the job here. One split per *connection*, not per checkout.
    ///
    /// Boxed so the enum is a pointer rather than a whole `Client` per slot,
    /// and so the client sits at a stable address — which is what lets
    /// `get_stream` hand out a `ValueStream` borrowing it, with no separate
    /// parking field.
    Connected(Box<Client>),
    Disconnected,
}

/// A fixed-size connection pool with round-robin dispatch.
///
/// All slots start disconnected. Call [`connect_all()`](Pool::connect_all) for
/// eager startup, or let [`client()`](Pool::client) lazily reconnect on demand.
pub struct Pool {
    addr: SocketAddr,
    slots: Vec<Slot>,
    next: usize,
    connect_timeout_ms: u64,
    tls_server_name: Option<String>,
    password: Option<String>,
    username: Option<String>,
    /// Index of the slot whose connection is/was lent to a streaming
    /// `get_stream`, pending a health re-check on the next checkout. See
    /// [`reconcile_stream_slot`](Pool::reconcile_stream_slot).
    #[cfg(has_io_uring)]
    stream_slot: Option<usize>,
}

impl Pool {
    /// Create a new pool. All slots start disconnected.
    pub fn new(config: PoolConfig) -> Self {
        let mut slots = Vec::with_capacity(config.pool_size);
        for _ in 0..config.pool_size {
            slots.push(Slot::Disconnected);
        }
        Pool {
            addr: config.addr,
            slots,
            next: 0,
            connect_timeout_ms: config.connect_timeout_ms,
            tls_server_name: config.tls_server_name,
            password: config.password,
            username: config.username,
            #[cfg(has_io_uring)]
            stream_slot: None,
        }
    }

    /// Eagerly connect all slots. Returns an error if any connection fails.
    pub async fn connect_all(&mut self) -> Result<(), Error> {
        for i in 0..self.slots.len() {
            let client = self.do_connect().await?;
            self.slots[i] = Slot::Connected(Box::new(client));
        }
        Ok(())
    }

    /// Get a [`Client`] bound to the next healthy connection.
    ///
    /// Advances the round-robin cursor and returns a client for a connected
    /// slot. Disconnected slots are lazily reconnected. If all slots fail,
    /// returns [`Error::AllConnectionsFailed`].
    pub async fn client(&mut self) -> Result<&mut Client, Error> {
        self.reconcile_stream_slot();
        let idx = self.checkout().await?;
        match &mut self.slots[idx] {
            Slot::Connected(client) => Ok(client),
            // `checkout` only returns the index of a slot it left `Connected`.
            Slot::Disconnected => Err(Error::AllConnectionsFailed),
        }
    }

    /// Select the next healthy slot (round-robin, lazily reconnecting a
    /// disconnected one) and return its index.
    ///
    /// Returns the index rather than the client because the caller needs to
    /// borrow it out of `self.slots`, which it cannot do while this method holds
    /// `&mut self`. Both callers — [`client`](Pool::client) and
    /// [`get_stream`](Pool::get_stream) — want the index anyway; `get_stream`
    /// records it so the next checkout can re-check that connection's health.
    async fn checkout(&mut self) -> Result<usize, Error> {
        let size = self.slots.len();
        for _ in 0..size {
            let idx = self.next;
            self.next = (self.next + 1) % size;

            match &self.slots[idx] {
                Slot::Connected(_) => return Ok(idx),
                Slot::Disconnected => {
                    if let Ok(client) = self.do_connect().await {
                        self.slots[idx] = Slot::Connected(Box::new(client));
                        return Ok(idx);
                    }
                }
            }
        }
        Err(Error::AllConnectionsFailed)
    }

    /// Re-check the health of a slot that was lent to a streaming
    /// [`get_stream`](Pool::get_stream) and evict it if the stream poisoned it.
    ///
    /// A [`ValueStream`] dropped mid-value calls `close()` on its connection
    /// (poison), which flips the slot's `recv_mode` to `Closed` synchronously —
    /// so [`ConnCtx::is_alive`](ringline::ConnCtx::is_alive) reports it dead on
    /// the very next turn, before the Close CQE has even bumped the generation.
    /// A cleanly drained stream (`collect`/`discard`/`next_segment`-to-end, or a
    /// nil reply) restores the default read path and leaves the connection
    /// alive.
    ///
    /// Called at the top of every checkout path so a poisoned connection is
    /// marked [`Disconnected`](Slot::Disconnected) — forcing a reconnect on its
    /// next use — and is **never handed back out desynced**. On a healthy stream
    /// the slot stays `Connected` and is reused with no reconnect.
    #[cfg(has_io_uring)]
    fn reconcile_stream_slot(&mut self) {
        let Some(idx) = self.stream_slot.take() else {
            return;
        };
        if let Slot::Connected(client) = &self.slots[idx]
            && !client.is_alive()
        {
            // Poisoned by an undrained-stream drop — evict so the next checkout
            // reconnects rather than reusing a closed/desynced connection. The
            // stream's own `close()` already tore the connection down; do not
            // close again here.
            self.slots[idx] = Slot::Disconnected;
        }
    }

    #[cfg(not(has_io_uring))]
    #[inline]
    fn reconcile_stream_slot(&mut self) {}

    /// Get a [`Client`] on the next healthy connection, ready to pipeline.
    ///
    /// This used to return the [`Pipeline`](crate::Pipeline) directly, which is no longer
    /// possible: a pipeline borrows its client's connection halves for its
    /// lifetime, so it cannot outlive a client created inside this method.
    /// Hold the client and call [`Client::pipeline`](crate::Client::pipeline) on it:
    ///
    /// ```ignore
    /// let mut client = pool.client().await?;
    /// let results = client.pipeline().get(b"a").get(b"b").execute().await?;
    /// ```
    ///
    /// The borrow is the point, not an inconvenience: pipeline responses are
    /// positional, so a command issued on the side mid-batch would consume one
    /// of them and desync the rest.
    #[deprecated(note = "call `Client::pipeline` on a client from `Pool::client`")]
    pub async fn pipeline(&mut self) -> Result<&mut Client, Error> {
        self.client().await
    }

    /// Streaming GET on the next healthy pooled connection (io_uring only).
    ///
    /// Like [`Client::get_stream`], but routed onto a pooled connection. The
    /// returned [`ValueStream`] borrows `&mut self`, so the pool is held
    /// **exclusively** for the stream's whole lifetime — no other pool operation
    /// can run concurrently (a compile error), which is what keeps the streamed
    /// connection from being handed to a second caller mid-read.
    ///
    /// Returns `Ok(None)` for a missing key.
    ///
    /// # Poison eviction (why this is sound to pool)
    ///
    /// Dropping the [`ValueStream`] before its value is fully drained poisons
    /// the underlying connection (`close()`), exactly as on a single-connection
    /// [`Client`]. Because the borrow ties the stream to `&mut self`, the pool
    /// re-checks that connection's health on the **next** checkout
    /// ([`reconcile_stream_slot`](Pool::reconcile_stream_slot)): a poisoned
    /// connection is evicted and lazily reconnected, so a desynced connection is
    /// never returned to the pool for reuse. A fully drained stream
    /// (`collect`/`discard`/`next_segment`-to-end) leaves the connection healthy
    /// and it is reused with no reconnect.
    ///
    /// # Scope
    ///
    /// v1 offers pooled streaming on [`Pool`] only. `ShardedClient` streaming is
    /// a documented follow-up (the same borrow/eviction shape, per shard);
    /// `ClusterClient` streaming stays out of scope — a MOVED/ASK redirect
    /// requires re-issuing the read on another node mid-stream, which the
    /// length-bounded single-connection stream cannot express.
    #[cfg(has_io_uring)]
    pub async fn get_stream(
        &mut self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<ValueStream<'_>>, Error> {
        self.reconcile_stream_slot();
        let idx = self.checkout().await?;
        // The slot's client is already at a stable address (it is boxed), so the
        // returned `ValueStream` can borrow it directly — the separate parking
        // field this used to need is gone. Record the slot so the next checkout
        // re-checks this connection's health and evicts it if the stream
        // poisoned it.
        self.stream_slot = Some(idx);
        match &mut self.slots[idx] {
            Slot::Connected(client) => client.get_stream(key).await,
            Slot::Disconnected => Err(Error::AllConnectionsFailed),
        }
    }

    /// Mark a connection as dead after a `ConnectionClosed` error.
    ///
    /// Matches by [`ConnCtx::token()`] and sets the slot to disconnected.
    /// The next [`client()`](Pool::client) call will lazily reconnect.
    pub fn mark_disconnected(&mut self, conn: ConnCtx) {
        let token = conn.token();
        for slot in &mut self.slots {
            if let Slot::Connected(conn) = slot
                && conn.token() == token
            {
                conn.close();
                *slot = Slot::Disconnected;
                return;
            }
        }
    }

    /// Close all connections and reset slots to disconnected.
    pub fn close_all(&mut self) {
        for slot in &mut self.slots {
            if let Slot::Connected(conn) = slot {
                conn.close();
            }
            *slot = Slot::Disconnected;
        }
    }

    /// Number of currently connected slots.
    pub fn connected_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|s| matches!(s, Slot::Connected(_)))
            .count()
    }

    /// Total number of slots in the pool.
    pub fn pool_size(&self) -> usize {
        self.slots.len()
    }

    async fn do_connect(&self) -> Result<Client, Error> {
        // Options compose on one builder instead of branching over four
        // entry points (#528).
        let mut connect = ringline::connect(self.addr);
        if let Some(sni) = &self.tls_server_name {
            connect = connect.tls(sni.as_str());
        }
        if self.connect_timeout_ms > 0 {
            connect = connect.timeout(std::time::Duration::from_millis(self.connect_timeout_ms));
        }
        let conn = connect.await?;

        // The authed client itself, not the handle: with a non-`Copy`
        // `Connection` there is no way to hand back a second handle to the same
        // slot, and every caller was building a client from it anyway.
        let mut client = Client::new(conn);
        client
            .maybe_auth(self.password.as_deref(), self.username.as_deref())
            .await?;
        Ok(client)
    }
}
