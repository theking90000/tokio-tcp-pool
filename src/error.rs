use std::{fmt, io};

/// The stage at which connection establishment failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum ConnectError {
    /// System DNS resolution failed or returned no addresses.
    Resolve(io::Error),
    /// TCP connection establishment failed.
    Tcp(io::Error),
    /// SOCKS5 negotiation failed. The source retains I/O errors or reply codes.
    #[cfg(feature = "socks5")]
    Socks5(io::Error),
    /// HTTP CONNECT negotiation failed. The source retains I/O or parse errors.
    #[cfg(feature = "http-connect")]
    HttpConnect(io::Error),
    /// TLS negotiation or certificate validation failed.
    #[cfg(feature = "tls")]
    Tls(io::Error),
    /// The total establishment deadline elapsed.
    Timeout,
}
impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve(e) => write!(f, "DNS resolution failed: {e}"),
            Self::Tcp(e) => write!(f, "TCP connect failed: {e}"),
            #[cfg(feature = "socks5")]
            Self::Socks5(e) => write!(f, "SOCKS5 handshake failed: {e}"),
            #[cfg(feature = "http-connect")]
            Self::HttpConnect(e) => write!(f, "HTTP CONNECT handshake failed: {e}"),
            #[cfg(feature = "tls")]
            Self::Tls(e) => write!(f, "TLS handshake failed: {e}"),
            Self::Timeout => f.write_str("connection establishment timed out"),
        }
    }
}
impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resolve(e) | Self::Tcp(e) => Some(e),
            #[cfg(feature = "socks5")]
            Self::Socks5(e) => Some(e),
            #[cfg(feature = "http-connect")]
            Self::HttpConnect(e) => Some(e),
            #[cfg(feature = "tls")]
            Self::Tls(e) => Some(e),
            Self::Timeout => None,
        }
    }
}

/// Failure to acquire a usable connection.
#[derive(Debug)]
#[non_exhaustive]
pub enum AcquireError {
    /// Establishing a new connection failed.
    Connect(ConnectError),
}
impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for AcquireError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(e) => Some(e),
        }
    }
}

/// A rejected pool configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    /// At least one connection slot is required.
    ZeroMaxOpen,
    /// The TLS server name is invalid.
    #[cfg(feature = "tls")]
    InvalidServerName,
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroMaxOpen => "max_open must be greater than zero",
            #[cfg(feature = "tls")]
            Self::InvalidServerName => "invalid TLS server name",
        })
    }
}
impl std::error::Error for ConfigError {}

/// A SOCKS5 proxy refused the CONNECT request.
#[cfg(feature = "socks5")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Socks5ReplyError(pub u8);
#[cfg(feature = "socks5")]
impl fmt::Display for Socks5ReplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SOCKS5 reply code {}", self.0)
    }
}
#[cfg(feature = "socks5")]
impl std::error::Error for Socks5ReplyError {}

/// An HTTP proxy returned a non-success status.
#[cfg(feature = "http-connect")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpConnectStatusError(pub u16);
#[cfg(feature = "http-connect")]
impl fmt::Display for HttpConnectStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HTTP CONNECT status {}", self.0)
    }
}
#[cfg(feature = "http-connect")]
impl std::error::Error for HttpConnectStatusError {}
