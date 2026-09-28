//! Local TLS validation, proxy composition, deadlines and cancellation.
#![cfg(feature = "tls")]
mod common;
use tokio_tcp_pool::{AcquireError, ConnectError, Endpoint, Pool, Route, TlsConfig, rustls};
use common::*;
use std::{sync::Arc, time::Duration};
#[cfg(feature = "http-connect")]
use tokio::io::AsyncWriteExt;
use tokio::{io::AsyncReadExt, net::TcpListener};

#[tokio::test]
async fn direct_and_proxy_tls_validate_the_target_and_reuse() {
    bounded(async {
        let server = tls_echo().await;
        let (tls, _) = tls_configs();
        let target = Endpoint::new("localhost", server.addr.port()).unwrap();
        #[allow(unused_mut)]
        let mut routes = vec![Route::Direct {
            target: target.clone(),
        }];
        #[allow(unused_mut)]
        let mut proxies = Vec::new();
        #[cfg(feature = "socks5")]
        {
            let proxy = tunnel(ProxyKind::Socks, server.addr).await;
            routes.push(Route::Socks5 {
                proxy: proxy.addr.into(),
                target: target.clone(),
                dns: tokio_tcp_pool::Socks5Dns::Proxy,
            });
            proxies.push(proxy);
        }
        #[cfg(feature = "http-connect")]
        {
            let proxy = tunnel(ProxyKind::Http, server.addr).await;
            routes.push(Route::HttpConnect {
                proxy: proxy.addr.into(),
                target: target.clone(),
            });
            proxies.push(proxy);
        }
        // Explicit type keeps this test valid when neither proxy feature is enabled.
        let _proxies: Vec<Server> = proxies;
        for route in routes {
            let pool = Pool::builder(route).tls(tls.clone()).build().unwrap();
            let mut a = pool.acquire().await.unwrap();
            let id = exchange(&mut a).await;
            a.release();
            let mut b = pool.acquire().await.unwrap();
            assert_eq!(exchange(&mut b).await, id);
            b.close().await.unwrap();
            let mut c = pool.acquire().await.unwrap();
            assert_ne!(exchange(&mut c).await, id);
        }
    })
    .await;
}

#[tokio::test]
async fn wrong_certificate_or_hostname_fails_and_override_is_explicit() {
    bounded(async {
        let server = tls_echo().await;
        let (tls, _) = tls_configs();
        let empty_trust = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        let configs = [
            TlsConfig::new(Arc::new(empty_trust)).server_name("localhost"),
            tls.clone(),
            tls.clone().server_name("wrong.invalid"),
        ];
        for config in configs {
            let pool = Pool::builder(Route::Direct {
                target: server.addr.into(),
            })
            .tls(config)
            .max_open(1)
            .build()
            .unwrap();
            for _ in 0..2 {
                assert!(matches!(
                    pool.acquire().await,
                    Err(AcquireError::Connect(ConnectError::Tls(_)))
                ));
            }
        }
        let pool = Pool::builder(Route::Direct {
            target: server.addr.into(),
        })
        .tls(tls.clone().server_name("localhost"))
        .build()
        .unwrap();
        exchange(&mut pool.acquire().await.unwrap()).await;
        assert!(
            Pool::builder(Route::Direct {
                target: server.addr.into()
            })
            .tls(tls.server_name("bad name"))
            .build()
            .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn tls_cancellation_timeout_and_interrupted_handshake_release_capacity() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap();
        let (tls, _) = tls_configs();
        let pool = Pool::builder(Route::Direct {
            target: target.into(),
        })
        .tls(tls.server_name("localhost"))
        .max_open(1)
        .connect_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
        let p = pool.clone();
        let first = tokio::spawn(async move { p.acquire().await });
        let (mut peer, _) = listener.accept().await.unwrap();
        assert!(peer.read(&mut [0; 4096]).await.unwrap() > 0);
        let p = pool.clone();
        let waiter = tokio::spawn(async move { p.acquire().await });
        tokio::task::yield_now().await;
        first.abort();
        assert!(matches!(first.await, Err(error) if error.is_cancelled()));
        let (_second_peer, _) = listener.accept().await.unwrap();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(AcquireError::Connect(ConnectError::Timeout))
        ));
        let p = pool.clone();
        let next = tokio::spawn(async move { p.acquire().await });
        let (mut third_peer, _) = listener.accept().await.unwrap();
        assert!(third_peer.read(&mut [0; 4096]).await.unwrap() > 0);
        drop(third_peer);
        assert!(matches!(
            next.await.unwrap(),
            Err(AcquireError::Connect(ConnectError::Tls(_)))
        ));
    })
    .await;
}

#[tokio::test]
async fn close_sends_close_notify_and_cancelled_close_discards() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap();
        let (tls, acceptor) = tls_configs();
        let pool = Pool::builder(Route::Direct {
            target: target.into(),
        })
        .tls(tls.server_name("localhost"))
        .max_open(1)
        .build()
        .unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            // Rustls returns UnexpectedEof on an unclean TLS EOF. Ok(0) proves close_notify.
            assert_eq!(tls.read(&mut [0; 1]).await.unwrap(), 0);
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(socket).await.unwrap();
            assert!(tls.read(&mut [0; 1]).await.is_err());
        });
        pool.acquire().await.unwrap().close().await.unwrap();
        let connection = pool.acquire().await.unwrap();
        let close = connection.close();
        drop(close); // Cancellation before first poll must still discard the owned lease.
        server.await.unwrap();
    })
    .await;
}

#[cfg(feature = "http-connect")]
#[tokio::test]
async fn one_deadline_covers_proxy_and_tls_and_io_has_no_deadline() {
    tokio::time::timeout(Duration::from_secs(300), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap();
        let (tls, acceptor) = tls_configs();
        let pool = Pool::builder(Route::HttpConnect {
            proxy: proxy.into(),
            target: "localhost:443".parse().unwrap(),
        })
        .tls(tls)
        .max_open(1)
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap();
        let p = pool.clone();
        let acquire = tokio::spawn(async move { p.acquire().await });
        let (mut socket, _) = listener.accept().await.unwrap();
        http_request(&mut socket).await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::time::resume();
        socket.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.unwrap();
        assert!(socket.read(&mut [0; 4096]).await.unwrap() > 0); // TLS has begun.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::time::resume();
        assert!(matches!(
            acquire.await.unwrap(),
            Err(AcquireError::Connect(ConnectError::Timeout))
        ));
        drop(socket);
        let p = pool.clone();
        let acquire = tokio::spawn(async move { p.acquire().await });
        let (mut socket, _) = listener.accept().await.unwrap();
        http_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.unwrap();
        let mut peer = acceptor.accept(socket).await.unwrap();
        let mut connection = acquire.await.unwrap().unwrap();
        let read = tokio::spawn(async move { connection.read_u8().await.unwrap() });
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(100)).await;
        tokio::time::resume();
        assert!(!read.is_finished());
        peer.write_all(b"x").await.unwrap();
        peer.flush().await.unwrap();
        assert_eq!(read.await.unwrap(), b'x');
    })
    .await
    .unwrap();
}
