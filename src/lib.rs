//! Async pooled TCP/TLS connections over a single immutable route.
//!
//! [`Pool::acquire`] returns an opaque Tokio stream. Only [`Connection::release`]
//! authorizes reuse; dropping a connection discards it. No background task or
//! implicit application I/O timeout is created.

mod connection;
mod endpoint;
mod error;
mod factory;
#[cfg(feature = "http-connect")]
mod http_connect;
mod pool;
mod route;
#[cfg(feature = "socks5")]
mod socks5;
#[cfg(feature = "tls")]
mod tls;
mod transport;

pub use connection::Connection;
pub use endpoint::{Endpoint, EndpointError, Host};
#[cfg(feature = "http-connect")]
pub use error::HttpConnectStatusError;
pub use error::{AcquireError, ConfigError, ConnectError};
pub use pool::{Pool, PoolBuilder, PoolConfig};
pub use route::Route;
#[cfg(feature = "tls")]
pub use tls::TlsConfig;
/// The rustls version used by this crate, for configuring TLS trust and protocols.
#[cfg(feature = "tls")]
pub use tokio_rustls::rustls;
#[cfg(feature = "socks5")]
pub use {error::Socks5ReplyError, route::Socks5Dns};
