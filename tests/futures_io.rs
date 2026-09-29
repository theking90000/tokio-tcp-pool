//! Futures I/O reads preserve bytes and connection reuse over TCP and TLS.
mod common;
use common::*;
use std::{future::poll_fn, pin::Pin, sync::atomic::Ordering};
use tokio::io::AsyncWriteExt;
#[cfg(feature = "tls")]
use tokio_tcp_pool::Route;
use tokio_tcp_pool::{Connection, Pool};

async fn exchange_with_futures_read(connection: &mut Connection) -> u64 {
    connection.write_all(b"x").await.unwrap();
    connection.flush().await.unwrap();
    let mut response = [0; 9];
    let mut filled = 0;
    while filled < response.len() {
        // A small buffer requires multiple reads for one response.
        let mut buf = [0xaa; 2];
        let room = buf.len().min(response.len() - filled);
        let count = poll_fn(|cx| {
            futures_io::AsyncRead::poll_read(Pin::new(&mut *connection), cx, &mut buf[..room])
        })
        .await
        .unwrap();
        assert!(count > 0 && count <= room);
        assert!(buf[count..].iter().all(|&byte| byte == 0xaa));
        response[filled..filled + count].copy_from_slice(&buf[..count]);
        filled += count;
    }
    assert_eq!(response[8], b'x');
    u64::from_be_bytes(response[..8].try_into().unwrap())
}

async fn assert_futures_read_reuses(pool: Pool, server: &Server) {
    let mut connection = pool.acquire().await.unwrap();
    let id = exchange_with_futures_read(&mut connection).await;
    connection.release();
    let mut connection = pool.acquire().await.unwrap();
    assert_eq!(exchange_with_futures_read(&mut connection).await, id);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn futures_read_tcp_preserves_bytes_and_reuses() {
    bounded(async {
        let server = echo().await;
        assert_futures_read_reuses(pool(server.addr), &server).await;
    })
    .await;
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn futures_read_tls_preserves_bytes_and_reuses() {
    bounded(async {
        let server = tls_echo().await;
        let (tls, _) = tls_configs();
        let pool = Pool::builder(Route::Direct {
            target: server.addr.into(),
        })
        .tls(tls.server_name("localhost"))
        .build()
        .unwrap();
        assert_futures_read_reuses(pool, &server).await;
    })
    .await;
}
