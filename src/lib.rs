#![doc = include_str!("../README.md")]

mod connection;
mod endpoint;
mod error;
mod factory;
#[cfg(feature = "http-connect")]
mod http_connect;
mod pool;
#[cfg(any(feature = "socks5", feature = "http-connect"))]
mod proxy_auth;
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
#[cfg(any(feature = "socks5", feature = "http-connect"))]
pub use proxy_auth::ProxyAuthError;
#[cfg(feature = "http-connect")]
pub use proxy_auth::ProxyAuthorization;
#[cfg(feature = "socks5")]
pub use proxy_auth::Socks5Credentials;
pub use route::Route;
#[cfg(feature = "tls")]
pub use tls::TlsConfig;
/// The rustls version used by this crate, for configuring TLS trust and protocols.
#[cfg(feature = "tls")]
pub use tokio_rustls::rustls;
#[cfg(feature = "socks5")]
pub use {error::Socks5ReplyError, route::Socks5Dns};
