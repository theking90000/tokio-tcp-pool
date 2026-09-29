use crate::{Endpoint, HttpConnectStatusError, ProxyAuthorization};
use std::io;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// One byte at a time deliberately avoids consuming bytes belonging to the tunnel.
// A CONNECT response is small and occurs only during connection establishment.
const MAX_HEADERS: usize = 16 * 1024;
pub(crate) async fn handshake(
    socket: &mut TcpStream,
    target: &Endpoint,
    authorization: Option<&ProxyAuthorization>,
) -> io::Result<()> {
    let mut request = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
    if let Some(authorization) = authorization {
        request.push_str("Proxy-Authorization: ");
        request.push_str(authorization.value());
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    socket.write_all(request.as_bytes()).await?;
    let mut bytes = Vec::with_capacity(256);
    loop {
        if bytes.len() == MAX_HEADERS {
            return Err(invalid("CONNECT headers exceed 16 KiB"));
        }
        bytes.push(socket.read_u8().await?);
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let mut headers = [httparse::EMPTY_HEADER; 128];
    let mut response = httparse::Response::new(&mut headers);
    let status = response
        .parse(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if !matches!(status, httparse::Status::Complete(n) if n == bytes.len()) {
        return Err(invalid("incomplete CONNECT response"));
    }
    let code = response
        .code
        .ok_or_else(|| invalid("missing CONNECT status"))?;
    if !(200..300).contains(&code) {
        return Err(io::Error::other(HttpConnectStatusError(code)));
    }
    Ok(())
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
