use crate::{
    ConnectError, Endpoint, Host, Socks5Credentials, Socks5Dns, Socks5ReplyError,
    factory::{connect_tcp, resolve},
};
use std::{
    io::{self, IoSlice},
    net::IpAddr,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub(crate) async fn connect(
    proxy: &Endpoint,
    target: &Endpoint,
    dns: Socks5Dns,
    credentials: Option<&Socks5Credentials>,
) -> Result<TcpStream, ConnectError> {
    let hosts = match (target.host(), dns) {
        (Host::Name(_), Socks5Dns::Local) => resolve(target)
            .await?
            .into_iter()
            .map(|a| Host::Ip(a.ip()))
            .collect(),
        (host, _) => vec![host.clone()],
    };
    let mut last = None;
    for host in hosts {
        let mut socket = connect_tcp(proxy).await?;
        match handshake(&mut socket, &host, target.port(), credentials).await {
            Ok(()) => return Ok(socket),
            Err(error) => last = Some(error),
        }
    }
    Err(ConnectError::Socks5(
        last.expect("at least one target address"),
    ))
}
async fn handshake(
    socket: &mut TcpStream,
    host: &Host,
    port: u16,
    credentials: Option<&Socks5Credentials>,
) -> io::Result<()> {
    let method = if credentials.is_some() { 2 } else { 0 };
    socket.write_all(&[5, 1, method]).await?;
    let mut choice = [0; 2];
    socket.read_exact(&mut choice).await?;
    if choice != [5, method] {
        return Err(invalid("proxy did not select the offered SOCKS5 method"));
    }
    if let Some(credentials) = credentials {
        let header = [1, credentials.username.len() as u8];
        let password_len = [credentials.password.len() as u8];
        write_all_vectored(
            socket,
            &mut [
                IoSlice::new(&header),
                IoSlice::new(credentials.username.as_bytes()),
                IoSlice::new(&password_len),
                IoSlice::new(credentials.password.as_bytes()),
            ],
        )
        .await?;
        let mut result = [0; 2];
        socket.read_exact(&mut result).await?;
        if result != [1, 0] {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "SOCKS5 authentication failed",
            ));
        }
    }
    let port = port.to_be_bytes();
    match host {
        Host::Ip(IpAddr::V4(ip)) => {
            let header = [5, 1, 0, 1];
            let address = ip.octets();
            write_all_vectored(
                socket,
                &mut [
                    IoSlice::new(&header),
                    IoSlice::new(&address),
                    IoSlice::new(&port),
                ],
            )
            .await?;
        }
        Host::Ip(IpAddr::V6(ip)) => {
            let header = [5, 1, 0, 4];
            let address = ip.octets();
            write_all_vectored(
                socket,
                &mut [
                    IoSlice::new(&header),
                    IoSlice::new(&address),
                    IoSlice::new(&port),
                ],
            )
            .await?;
        }
        Host::Name(name) => {
            let header = [5, 1, 0, 3, name.len() as u8];
            write_all_vectored(
                socket,
                &mut [
                    IoSlice::new(&header),
                    IoSlice::new(name.as_bytes()),
                    IoSlice::new(&port),
                ],
            )
            .await?;
        }
    }
    let mut reply = [0; 4];
    socket.read_exact(&mut reply).await?;
    if reply[0] != 5 || reply[2] != 0 {
        return Err(invalid("invalid SOCKS5 reply"));
    }
    if reply[1] != 0 {
        return Err(io::Error::other(Socks5ReplyError(reply[1])));
    }
    let length = match reply[3] {
        1 => 4,
        4 => 16,
        3 => {
            let length = socket.read_u8().await? as usize;
            if length == 0 {
                return Err(invalid("empty SOCKS5 bound hostname"));
            }
            length
        }
        _ => return Err(invalid("invalid SOCKS5 address type")),
    };
    let mut bound = [0; 257];
    socket.read_exact(&mut bound[..length + 2]).await?;
    Ok(())
}
async fn write_all_vectored(socket: &mut TcpStream, mut bufs: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !bufs.is_empty() {
        let written = socket.write_vectored(bufs).await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
