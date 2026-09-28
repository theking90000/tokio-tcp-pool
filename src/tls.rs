use std::sync::Arc;
use tokio_rustls::rustls::ClientConfig;

/// Caller-supplied TLS trust, protocol, and optional identity configuration.
///
/// Certificate verification uses the route target unless explicitly overridden.
/// No root certificates or ALPN protocols are installed by the pool.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) server_name: Option<String>,
}
impl TlsConfig {
    /// Wraps a rustls client configuration.
    pub fn new(client: Arc<ClientConfig>) -> Self {
        Self {
            client,
            server_name: None,
        }
    }
    /// Overrides the name used for certificate validation and DNS-name SNI.
    ///
    /// Useful when routing to a literal IP while validating a DNS identity.
    /// The builder validates the name when constructing the pool.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }
}
impl From<Arc<ClientConfig>> for TlsConfig {
    fn from(client: Arc<ClientConfig>) -> Self {
        Self::new(client)
    }
}
