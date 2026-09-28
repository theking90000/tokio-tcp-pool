use crate::{
    AcquireError, ConfigError, ConnectError, Connection, Route, factory::ConnectionFactory,
    transport::ConnectionInner,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

/// Connection limits and lazy expiration policy.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Maximum idle, leased, and connecting connections combined. Default: 8.
    pub max_open: usize,
    /// Total DNS, TCP, proxy and TLS establishment deadline. Default: 10 seconds.
    pub connect_timeout: Duration,
    /// Maximum time since release before reuse. Default: 60 seconds.
    pub idle_timeout: Duration,
    /// Maximum age since establishment started. Default: no age limit.
    pub max_lifetime: Option<Duration>,
}
impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_open: 8,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            max_lifetime: None,
        }
    }
}

/// A cheaply cloned pool for exactly one immutable route.
///
/// Connections are created on demand. Dropping the last pool handle closes idle
/// connections; leased connections remain usable, but can no longer be recycled.
#[derive(Clone)]
pub struct Pool {
    pub(crate) shared: Arc<PoolShared>,
}

/// Builds a pool without performing DNS or network I/O.
pub struct PoolBuilder {
    route: Route,
    config: PoolConfig,
    #[cfg(feature = "tls")]
    tls: Option<crate::TlsConfig>,
}
impl Pool {
    /// Starts a builder with default limits and plain TCP transport.
    pub fn builder(route: Route) -> PoolBuilder {
        PoolBuilder {
            route,
            config: PoolConfig::default(),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
    /// Returns the pool's immutable route.
    pub fn route(&self) -> &Route {
        &self.shared.factory.route
    }
    /// Returns the pool's configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.shared.config
    }
    /// Waits for an idle connection or a slot to establish a new one.
    ///
    /// Cancellation releases any reserved slot and destroys a partial transport.
    /// Waiting for capacity has no deadline; `connect_timeout` starts only when
    /// a new connection is needed. Wrap this future in Tokio's `timeout` to bound
    /// the entire acquisition. No strict waiter ordering is promised.
    pub async fn acquire(&self) -> Result<Connection, AcquireError> {
        loop {
            // Register BEFORE inspecting state. notify_waiters wakes every registered
            // waiter, so cancellation of one cannot swallow another waiter's wakeup.
            let available = self.shared.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            let action = {
                let mut state = self.shared.state.lock().expect("pool mutex poisoned");
                if let Some(idle) = state.idle.pop_front() {
                    Action::Reuse(idle)
                } else if state.open < self.shared.config.max_open {
                    state.open += 1;
                    Action::Connect(OpenSlot {
                        pool: Arc::downgrade(&self.shared),
                    })
                } else {
                    Action::Wait
                }
            };
            match action {
                Action::Reuse(idle) => {
                    if idle.idle_since.elapsed() >= self.shared.config.idle_timeout
                        || self.shared.expired(idle.live.created_at)
                        || idle.live.inner.known_dead()
                    {
                        // Destruction releases the slot outside the state mutex.
                        drop(idle);
                        continue;
                    }
                    return Ok(Connection::new(idle.live));
                }
                Action::Connect(slot) => {
                    let created_at = Instant::now();
                    let inner = tokio::time::timeout(
                        self.shared.config.connect_timeout,
                        self.shared.factory.connect(),
                    )
                    .await
                    .map_err(|_| AcquireError::Connect(ConnectError::Timeout))?
                    .map_err(AcquireError::Connect)?;
                    return Ok(Connection::new(LiveConnection {
                        inner,
                        created_at,
                        slot,
                    }));
                }
                Action::Wait => available.await,
            }
        }
    }
}
impl PoolBuilder {
    /// Replaces all connection limits and expiration settings.
    pub fn config(mut self, config: PoolConfig) -> Self {
        self.config = config;
        self
    }
    /// Sets the total connection limit. Zero is rejected by `build`.
    pub fn max_open(mut self, max_open: usize) -> Self {
        self.config.max_open = max_open;
        self
    }
    /// Sets the deadline covering all establishment stages.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }
    /// Sets the maximum idle time. Zero disables reuse.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.idle_timeout = timeout;
        self
    }
    /// Sets the maximum age. Active operations are never interrupted by this limit.
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.config.max_lifetime = Some(lifetime);
        self
    }
    /// Enables TLS after connecting to the route's destination.
    #[cfg(feature = "tls")]
    pub fn tls(mut self, tls: impl Into<crate::TlsConfig>) -> Self {
        self.tls = Some(tls.into());
        self
    }
    /// Validates the configuration and creates an empty pool.
    pub fn build(self) -> Result<Pool, ConfigError> {
        if self.config.max_open == 0 {
            return Err(ConfigError::ZeroMaxOpen);
        }
        #[cfg(feature = "tls")]
        let tls = self
            .tls
            .map(|tls| {
                let name = tls
                    .server_name
                    .clone()
                    .unwrap_or_else(|| self.route.target().host().to_string());
                let name = tokio_rustls::rustls::pki_types::ServerName::try_from(name)
                    .map_err(|_| ConfigError::InvalidServerName)?;
                Ok((tls, name))
            })
            .transpose()?;
        Ok(Pool {
            shared: Arc::new(PoolShared {
                factory: ConnectionFactory {
                    route: self.route,
                    #[cfg(feature = "tls")]
                    tls,
                },
                config: self.config,
                state: Mutex::new(PoolState {
                    idle: VecDeque::new(),
                    open: 0,
                }),
                available: Notify::new(),
            }),
        })
    }
}

enum Action {
    Reuse(IdleConnection),
    Connect(OpenSlot),
    Wait,
}
pub(crate) struct PoolShared {
    factory: ConnectionFactory,
    pub config: PoolConfig,
    state: Mutex<PoolState>,
    available: Notify,
}
struct PoolState {
    idle: VecDeque<IdleConnection>,
    open: usize,
}
struct IdleConnection {
    live: LiveConnection,
    idle_since: Instant,
}
pub(crate) struct LiveConnection {
    // Drop the socket BEFORE returning capacity to a waiter.
    pub inner: ConnectionInner,
    pub created_at: Instant,
    pub slot: OpenSlot,
}
/// One RAII owner spans establishment, leasing and idle storage.
pub(crate) struct OpenSlot {
    pub pool: Weak<PoolShared>,
}
impl Drop for OpenSlot {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            {
                pool.state.lock().expect("pool mutex poisoned").open -= 1;
            }
            pool.available.notify_waiters();
        }
    }
}
impl PoolShared {
    pub fn expired(&self, created_at: Instant) -> bool {
        self.config
            .max_lifetime
            .is_some_and(|max| created_at.elapsed() >= max)
    }
    pub fn recycle(&self, live: LiveConnection) {
        if self.expired(live.created_at) || self.config.idle_timeout.is_zero() {
            return;
        }
        {
            self.state
                .lock()
                .expect("pool mutex poisoned")
                .idle
                .push_back(IdleConnection {
                    live,
                    idle_since: Instant::now(),
                });
        }
        self.available.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn cancelling_tcp_connect_returns_reserved_slot() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = Pool::builder(Route::Direct {
            target: listener.local_addr().unwrap().into(),
        })
        .max_open(1)
        .build()
        .unwrap();
        let mut acquire = Box::pin(pool.acquire());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(acquire.as_mut().poll(cx).is_pending())).await;
        assert!(
            pending,
            "first TCP connect poll should wait for socket readiness"
        );
        assert_eq!(pool.shared.state.lock().unwrap().open, 1);
        drop(acquire);
        assert_eq!(pool.shared.state.lock().unwrap().open, 0);
        let connection = pool.acquire().await.unwrap();
        assert_eq!(pool.shared.state.lock().unwrap().open, 1);
        drop(connection);
        assert_eq!(pool.shared.state.lock().unwrap().open, 0);
    }

    #[tokio::test]
    async fn fin_peek_is_non_destructive_and_removes_idle_entry() {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = Pool::builder(Route::Direct {
            target: listener.local_addr().unwrap().into(),
        })
        .build()
        .unwrap();
        let connection = pool.acquire().await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        connection.release();
        peer.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pool
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .idle
                    .front()
                    .unwrap()
                    .live
                    .inner
                    .known_dead()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _connection = pool.acquire().await.unwrap();
        assert!(pool.shared.state.lock().unwrap().idle.is_empty());
        assert_eq!(pool.shared.state.lock().unwrap().open, 1);
        listener.accept().await.unwrap();
    }
}
