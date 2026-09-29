use crate::Endpoint;
#[cfg(feature = "http-connect")]
use crate::ProxyAuthorization;
#[cfg(feature = "socks5")]
use crate::Socks5Credentials;

/// Where a SOCKS5 target hostname is resolved.
#[cfg(feature = "socks5")]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Socks5Dns {
    /// Resolve locally for each new connection and send an IP to the proxy.
    Local,
    /// Send the hostname to the proxy without resolving it locally.
    Proxy,
}

/// One immutable path to one destination.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Route {
    /// Connect directly to the target.
    Direct {
        /// Logical destination.
        target: Endpoint,
    },
    /// Use an unauthenticated SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    Socks5 {
        /// TCP proxy endpoint.
        proxy: Endpoint,
        /// Logical destination.
        target: Endpoint,
        /// Explicit DNS policy for the target.
        dns: Socks5Dns,
    },
    /// Use a SOCKS5 proxy with username/password authentication.
    #[cfg(feature = "socks5")]
    Socks5Auth {
        /// TCP proxy endpoint.
        proxy: Endpoint,
        /// Logical destination.
        target: Endpoint,
        /// Explicit DNS policy for the target.
        dns: Socks5Dns,
        /// Credentials required by the proxy.
        credentials: Socks5Credentials,
    },
    /// Use an unauthenticated HTTP CONNECT proxy over plain TCP.
    #[cfg(feature = "http-connect")]
    HttpConnect {
        /// TCP proxy endpoint.
        proxy: Endpoint,
        /// Logical destination.
        target: Endpoint,
    },
    /// Use an HTTP CONNECT proxy with a `Proxy-Authorization` header.
    #[cfg(feature = "http-connect")]
    HttpConnectAuth {
        /// TCP proxy endpoint.
        proxy: Endpoint,
        /// Logical destination.
        target: Endpoint,
        /// Authorization sent on every new CONNECT request.
        authorization: ProxyAuthorization,
    },
}
impl Route {
    /// Returns the logical destination, also used for TLS identity validation.
    pub fn target(&self) -> &Endpoint {
        match self {
            Self::Direct { target } => target,
            #[cfg(feature = "socks5")]
            Self::Socks5 { target, .. } => target,
            #[cfg(feature = "socks5")]
            Self::Socks5Auth { target, .. } => target,
            #[cfg(feature = "http-connect")]
            Self::HttpConnect { target, .. } => target,
            #[cfg(feature = "http-connect")]
            Self::HttpConnectAuth { target, .. } => target,
        }
    }
}
