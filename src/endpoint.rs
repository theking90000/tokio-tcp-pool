use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

/// An IP address or an ASCII DNS hostname.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Host {
    /// A literal IP address; no DNS lookup is needed.
    Ip(IpAddr),
    /// An ASCII DNS hostname.
    Name(String),
}

impl fmt::Display for Host {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => ip.fmt(f),
            Self::Name(name) => name.fmt(f),
        }
    }
}

/// A validated hostname or IP address and a TCP port.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Endpoint {
    host: Host,
    port: u16,
}

/// An invalid endpoint, hostname, or port syntax.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointError;

impl fmt::Display for EndpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("expected an IP address or ASCII DNS hostname and a port")
    }
}
impl std::error::Error for EndpointError {}

impl Endpoint {
    /// Creates an endpoint. IPv6 addresses here are unbracketed.
    ///
    /// International domain names must first be converted to ASCII (IDNA).
    pub fn new(host: impl AsRef<str>, port: u16) -> Result<Self, EndpointError> {
        let host = host.as_ref();
        let host = if let Ok(ip) = host.parse() {
            Host::Ip(ip)
        } else {
            let name = host.strip_suffix('.').unwrap_or(host);
            if name.is_empty()
                || name.len() > 253
                || !name.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && label.as_bytes()[0].is_ascii_alphanumeric()
                        && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                        && label
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
            {
                return Err(EndpointError);
            }
            Host::Name(host.to_owned())
        };
        Ok(Self { host, port })
    }
    /// Returns the hostname or literal address.
    pub fn host(&self) -> &Host {
        &self.host
    }
    /// Returns the TCP port.
    pub fn port(&self) -> u16 {
        self.port
    }
}
impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self {
            host: Host::Ip(addr.ip()),
            port: addr.port(),
        }
    }
}
impl FromStr for Endpoint {
    type Err = EndpointError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(addr.into());
        }
        let (host, port) = s.rsplit_once(':').ok_or(EndpointError)?;
        if host.contains(':') {
            return Err(EndpointError);
        }
        Self::new(host, port.parse().map_err(|_| EndpointError)?)
    }
}
impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.host {
            Host::Ip(IpAddr::V6(ip)) => write!(f, "[{ip}]:{}", self.port),
            host => write!(f, "{host}:{}", self.port),
        }
    }
}
