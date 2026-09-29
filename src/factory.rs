use crate::{ConnectError, Endpoint, Host, Route, transport::ConnectionInner};
use std::{io, net::SocketAddr};
use tokio::net::{TcpStream, lookup_host};

pub(crate) struct ConnectionFactory {
    pub route: Route,
    #[cfg(feature = "tls")]
    pub tls: Option<(
        crate::TlsConfig,
        tokio_rustls::rustls::pki_types::ServerName<'static>,
    )>,
}
impl ConnectionFactory {
    pub async fn connect(&self) -> Result<ConnectionInner, ConnectError> {
        let socket = match &self.route {
            Route::Direct { target } => connect_tcp(target).await?,
            #[cfg(feature = "socks5")]
            Route::Socks5 { proxy, target, dns } => {
                crate::socks5::connect(proxy, target, *dns, None).await?
            }
            #[cfg(feature = "socks5")]
            Route::Socks5Auth {
                proxy,
                target,
                dns,
                credentials,
            } => crate::socks5::connect(proxy, target, *dns, Some(credentials)).await?,
            #[cfg(feature = "http-connect")]
            Route::HttpConnect { proxy, target } => {
                let mut socket = connect_tcp(proxy).await?;
                crate::http_connect::handshake(&mut socket, target, None)
                    .await
                    .map_err(ConnectError::HttpConnect)?;
                socket
            }
            #[cfg(feature = "http-connect")]
            Route::HttpConnectAuth {
                proxy,
                target,
                authorization,
            } => {
                let mut socket = connect_tcp(proxy).await?;
                crate::http_connect::handshake(&mut socket, target, Some(authorization))
                    .await
                    .map_err(ConnectError::HttpConnect)?;
                socket
            }
        };
        #[cfg(feature = "tls")]
        if let Some((tls, name)) = &self.tls {
            let stream = tokio_rustls::TlsConnector::from(tls.client.clone())
                .connect(name.clone(), socket)
                .await
                .map_err(ConnectError::Tls)?;
            return Ok(ConnectionInner::Tls(Box::new(stream)));
        }
        Ok(ConnectionInner::Plain(socket))
    }
}

pub(crate) async fn resolve(endpoint: &Endpoint) -> Result<Vec<SocketAddr>, ConnectError> {
    let addresses = match endpoint.host() {
        Host::Ip(ip) => vec![SocketAddr::new(*ip, endpoint.port())],
        Host::Name(name) => lookup_host((name.as_str(), endpoint.port()))
            .await
            .map_err(ConnectError::Resolve)?
            .collect(),
    };
    if addresses.is_empty() {
        return Err(ConnectError::Resolve(io::Error::new(
            io::ErrorKind::NotFound,
            "DNS returned no addresses",
        )));
    }
    Ok(addresses)
}
pub(crate) async fn connect_tcp(endpoint: &Endpoint) -> Result<TcpStream, ConnectError> {
    let addresses = resolve(endpoint).await?;
    TcpStream::connect(addresses.as_slice())
        .await
        .map_err(ConnectError::Tcp)
}
