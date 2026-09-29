use crate::pool::LiveConnection;
use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

/// An opaque pooled byte stream implementing Tokio I/O and futures I/O writing.
///
/// Dropping this value discards its transport. Only an explicit [`Self::release`]
/// authorizes reuse. I/O errors, read EOF, zero-length nonempty writes, and write
/// shutdown make the connection ineligible for reuse. No I/O deadline is added.
pub struct Connection {
    live: LiveConnection,
    broken: bool,
}
impl Connection {
    pub(crate) fn new(live: LiveConnection) -> Self {
        Self {
            live,
            broken: false,
        }
    }
    /// Returns a reusable connection to its pool, consuming this lease.
    ///
    /// The caller must have consumed the complete application response and restored
    /// protocol synchronization. Buffered outgoing data must be flushed first.
    /// Broken, expired, or orphaned connections are destroyed instead.
    pub fn release(self) {
        if !self.broken {
            if let Some(pool) = self.live.slot.pool.upgrade() {
                pool.recycle(self.live);
            }
        }
    }
    /// Immediately destroys the connection and releases its slot.
    pub fn discard(self) {
        drop(self);
    }
    /// Sends TLS close_notify when applicable and shuts down the write transport.
    ///
    /// The socket is then dropped. Failure or cancellation also discards it.
    /// This operation has no implicit deadline.
    pub async fn close(mut self) -> io::Result<()> {
        self.shutdown().await
    }
}
impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let room = buf.remaining();
        let result = Pin::new(&mut this.live.inner).poll_read(cx, buf);
        if matches!(&result, Poll::Ready(Err(_)))
            || (matches!(&result, Poll::Ready(Ok(()))) && room != 0 && buf.filled().len() == before)
        {
            this.broken = true;
        }
        result
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.live.inner).poll_write(cx, buf);
        if matches!(&result, Poll::Ready(Err(_)))
            || (!buf.is_empty() && matches!(&result, Poll::Ready(Ok(0))))
        {
            this.broken = true;
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.live.inner).poll_flush(cx);
        if matches!(&result, Poll::Ready(Err(_))) {
            this.broken = true;
        }
        result
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.broken = true;
        Pin::new(&mut this.live.inner).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.live.inner).poll_write_vectored(cx, bufs);
        if matches!(&result, Poll::Ready(Err(_)))
            || (bufs.iter().any(|b| !b.is_empty()) && matches!(&result, Poll::Ready(Ok(0))))
        {
            this.broken = true;
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.live.inner.is_write_vectored()
    }
}

impl futures_io::AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <Self as AsyncWrite>::poll_write(self, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <Self as AsyncWrite>::poll_flush(self, cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <Self as AsyncWrite>::poll_shutdown(self, cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        <Self as AsyncWrite>::poll_write_vectored(self, cx, bufs)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Pool, Route};
    use std::time::Duration;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[tokio::test]
    async fn read_error_marks_connection_broken() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = Pool::builder(Route::Direct {
            target: listener.local_addr().unwrap().into(),
        })
        .max_open(1)
        .build()
        .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        let (peer, _) = listener.accept().await.unwrap();
        socket2::SockRef::from(&peer)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(peer);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), connection.read_u8())
                .await
                .unwrap()
                .is_err()
        );
        assert!(connection.broken);
        connection.release();
        let _new = pool.acquire().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
    }
}
