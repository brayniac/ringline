//! Connection pool for ringline-memcache.
//!
//! `Pool` manages a fixed set of backend connections with round-robin dispatch
//! and lazy reconnection. It is single-threaded (no Arc, no Mutex) — designed
//! for use within a single ringline async worker.
//!
//! # Usage
//!
//! ```no_run
//! # use std::net::SocketAddr;
//! # use ringline_memcache::{Pool, PoolConfig};
//! # async fn example() -> Result<(), ringline_memcache::Error> {
//! let config = PoolConfig::new("127.0.0.1:11211".parse().unwrap(), 4);
//! let mut pool = Pool::new(config);
//! pool.connect_all().await?;
//! pool.client().await?.set("k", "v").await?;
//! # Ok(())
//! # }
//! ```

use std::net::SocketAddr;

use ringline::ConnCtx;

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
}

enum Slot {
    /// A pooled connection *is* a client.
    ///
    /// The slot held a bare `ConnCtx` and minted a throwaway `Client` per
    /// checkout, which once the halves became claims meant a `split()` — two
    /// driver round trips and two claim writes — every call. `ShardedClient`
    /// moved to owning the client for that reason; `connect` resolving to a
    /// non-`Copy` `Connection` (#528) finishes it here. One split per
    /// *connection*, not per checkout.
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
    ///
    /// # One live client per slot
    ///
    /// The pool owns each slot's client and lends it out, so two clients on one
    /// slot is not representable — it used to be refused at run time with
    /// `EBUSY`. Two clients reading one connection interleave their responses
    /// and desynchronise the protocol, so this was never sound; now the borrow
    /// checker says so instead of the driver (#528).
    pub async fn client(&mut self) -> Result<&mut Client, Error> {
        let size = self.slots.len();
        for _ in 0..size {
            let idx = self.next;
            self.next = (self.next + 1) % size;

            match &self.slots[idx] {
                Slot::Connected(_) => {
                    // Re-index rather than returning the borrow from the match:
                    // the `Disconnected` arm below needs `&mut self`.
                    let Slot::Connected(client) = &mut self.slots[idx] else {
                        unreachable!("just matched Connected")
                    };
                    return Ok(client);
                }
                Slot::Disconnected => {
                    if let Ok(client) = self.do_connect().await {
                        self.slots[idx] = Slot::Connected(Box::new(client));
                        let Slot::Connected(client) = &mut self.slots[idx] else {
                            unreachable!("just stored Connected")
                        };
                        return Ok(client);
                    }
                }
            }
        }
        Err(Error::AllConnectionsFailed)
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
        Ok(Client::new(conn))
    }
}
