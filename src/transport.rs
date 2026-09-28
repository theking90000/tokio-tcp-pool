use std::{
    io::{self, IoSlice},
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

// After negotiation, both proxy protocols yield a plain TCP tunnel.
pub(crate) enum ConnectionInner {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}
impl ConnectionInner {
    pub fn known_dead(&self) -> bool {
        let socket = match self {
            Self::Plain(socket) => socket,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.get_ref().0,
        };
        let socket = socket2::SockRef::from(socket);
        match socket.take_error() {
            Ok(None) => {}
            _ => return true,
        }
        match socket.peek(&mut [MaybeUninit::uninit(); 1]) {
            Ok(0) => true,
            Ok(_) => false,
            Err(e) => !matches!(
                e.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ),
        }
    }
    fn stream(&mut self) -> Pin<&mut (dyn AsyncStream + Send + Unpin)> {
        match self {
            Self::Plain(stream) => Pin::new(stream),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => Pin::new(stream.as_mut()),
        }
    }
}
trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncStream for T {}
impl AsyncRead for ConnectionInner {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.stream().poll_read(cx, buf)
    }
}
impl AsyncWrite for ConnectionInner {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stream().poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream().poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.stream().poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            #[cfg(feature = "tls")]
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }
}
