use std::fmt;

/// Invalid proxy credentials or authorization header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyAuthError {
    /// SOCKS5 usernames and passwords must each contain 1 to 255 bytes.
    #[cfg(feature = "socks5")]
    InvalidSocks5Length,
    /// An HTTP authorization value must contain printable ASCII only.
    #[cfg(feature = "http-connect")]
    InvalidHeaderValue,
    /// A Basic username cannot contain a colon or control character.
    #[cfg(feature = "http-connect")]
    InvalidBasicCredentials,
}

impl fmt::Display for ProxyAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            #[cfg(feature = "socks5")]
            Self::InvalidSocks5Length => "SOCKS5 username and password must be 1 to 255 bytes",
            #[cfg(feature = "http-connect")]
            Self::InvalidHeaderValue => "invalid Proxy-Authorization header value",
            #[cfg(feature = "http-connect")]
            Self::InvalidBasicCredentials => "invalid HTTP Basic proxy credentials",
        })
    }
}
impl std::error::Error for ProxyAuthError {}

/// SOCKS5 username/password credentials (RFC 1929).
#[cfg(feature = "socks5")]
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Socks5Credentials {
    pub(crate) username: String,
    pub(crate) password: String,
}

#[cfg(feature = "socks5")]
impl Socks5Credentials {
    /// Creates credentials with a 1 to 255 byte username and password.
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, ProxyAuthError> {
        let username = username.into();
        let password = password.into();
        if !(1..=255).contains(&username.len()) || !(1..=255).contains(&password.len()) {
            return Err(ProxyAuthError::InvalidSocks5Length);
        }
        Ok(Self { username, password })
    }
}

#[cfg(feature = "socks5")]
impl fmt::Debug for Socks5Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Socks5Credentials([redacted])")
    }
}

/// A validated value for the HTTP CONNECT `Proxy-Authorization` header.
#[cfg(feature = "http-connect")]
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ProxyAuthorization(String);

#[cfg(feature = "http-connect")]
impl ProxyAuthorization {
    /// Accepts a complete header value, such as `Bearer token`.
    pub fn new(value: impl Into<String>) -> Result<Self, ProxyAuthError> {
        let value = value.into();
        if value.is_empty() || !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
            return Err(ProxyAuthError::InvalidHeaderValue);
        }
        Ok(Self(value))
    }

    /// Creates an HTTP Basic header value from UTF-8 credentials.
    pub fn basic(username: &str, password: &str) -> Result<Self, ProxyAuthError> {
        if username.contains(':')
            || username.chars().any(char::is_control)
            || password.chars().any(char::is_control)
        {
            return Err(ProxyAuthError::InvalidBasicCredentials);
        }
        use base64::Engine;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        Ok(Self(format!("Basic {encoded}")))
    }

    pub(crate) fn value(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "http-connect")]
impl fmt::Debug for ProxyAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProxyAuthorization([redacted])")
    }
}
