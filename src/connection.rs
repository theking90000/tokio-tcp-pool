use crate::pool::LiveConnection;
use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

/// An opaque pooled byte stream implementing Tokio I/O and futures I/O.
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

impl futures_io::AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut buf = ReadBuf::new(buf);
        <Self as AsyncRead>::poll_read(self, cx, &mut buf).map_ok(|()| buf.filled().len())
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
    use std::{future::poll_fn, pin::Pin, task::Poll, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[tokio::test]
    async fn futures_pending_read_wakes_and_preserves_reuse() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let pool = Pool::builder(Route::Direct {
                target: listener.local_addr().unwrap().into(),
            })
            .max_open(1)
            .build()
            .unwrap();
            let mut connection = pool.acquire().await.unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut buf = [0; 1];
            poll_fn(|cx| {
                assert!(
                    futures_io::AsyncRead::poll_read(Pin::new(&mut connection), cx, &mut buf)
                        .is_pending()
                );
                Poll::Ready(())
            })
            .await;
            assert!(!connection.broken);
            let read = tokio::spawn(async move {
                let count = poll_fn(|cx| {
                    futures_io::AsyncRead::poll_read(Pin::new(&mut connection), cx, &mut buf)
                })
                .await
                .unwrap();
                assert_eq!(count, 1);
                assert_eq!(&buf, b"x");
                assert!(!connection.broken);
                connection.release();
            });
            // Let the reader register its waker before making data available.
            tokio::task::yield_now().await;
            assert!(!read.is_finished());
            peer.write_all(b"x").await.unwrap();
            read.await.unwrap();
            let mut connection = pool.acquire().await.unwrap();
            connection.write_all(b"y").await.unwrap();
            assert_eq!(peer.read_u8().await.unwrap(), b'y');
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn futures_empty_read_is_not_eof_but_nonempty_eof_prevents_reuse() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let pool = Pool::builder(Route::Direct {
                target: listener.local_addr().unwrap().into(),
            })
            .max_open(1)
            .build()
            .unwrap();
            let mut connection = pool.acquire().await.unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            peer.shutdown().await.unwrap();
            assert_eq!(
                poll_fn(|cx| {
                    futures_io::AsyncRead::poll_read(Pin::new(&mut connection), cx, &mut [])
                })
                .await
                .unwrap(),
                0
            );
            assert!(!connection.broken);
            assert_eq!(
                poll_fn(|cx| {
                    futures_io::AsyncRead::poll_read(Pin::new(&mut connection), cx, &mut [0; 1])
                })
                .await
                .unwrap(),
                0
            );
            assert!(connection.broken);
            connection.release();
            let _new = pool.acquire().await.unwrap();
            listener.accept().await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn futures_read_error_marks_connection_broken() {
        tokio::time::timeout(Duration::from_secs(2), async {
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
                poll_fn(|cx| {
                    futures_io::AsyncRead::poll_read(Pin::new(&mut connection), cx, &mut [0; 1])
                })
                .await
                .is_err()
            );
            assert!(connection.broken);
            connection.release();
            let _new = pool.acquire().await.unwrap();
            listener.accept().await.unwrap();
        })
        .await
        .unwrap();
    }

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
