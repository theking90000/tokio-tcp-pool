//! Local proxy protocol success, failure and cancellation tests.
#![cfg(any(feature = "socks5", feature = "http-connect"))]
mod common;
use common::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_tcp_pool::{AcquireError, ConnectError, Pool, Route};

#[cfg(feature = "socks5")]
#[tokio::test]
async fn socks_local_proxy_dns_ipv4_and_ipv6() {
    use tokio_tcp_pool::Socks5Dns;
    bounded(async {
        for (name, dns, expected_type) in [
            ("localhost", Socks5Dns::Local, 0),
            ("never-resolve.invalid", Socks5Dns::Proxy, 3),
            ("127.0.0.1", Socks5Dns::Proxy, 1),
            ("::1", Socks5Dns::Proxy, 4),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (kind, host, port) = socks_request(&mut socket).await;
                assert_eq!(port, 1234);
                if expected_type == 0 {
                    assert!(kind == 1 || kind == 4);
                } else {
                    assert_eq!(kind, expected_type);
                }
                if kind == 3 {
                    assert_eq!(host, b"never-resolve.invalid");
                }
                // Exercise each valid bound-address form, with payload coalesced.
                let mut response = match kind {
                    3 => vec![5, 0, 0, 3, 1, b'x', 0, 0],
                    4 => {
                        let mut r = vec![5, 0, 0, 4];
                        r.extend_from_slice(&[0; 18]);
                        r
                    }
                    _ => vec![5, 0, 0, 1, 127, 0, 0, 1, 0, 0],
                };
                response.extend_from_slice(b"ready");
                socket.write_all(&response).await.unwrap();
                serve(socket, 1).await;
            });
            let pool = Pool::builder(Route::Socks5 {
                proxy: proxy.into(),
                target: tokio_tcp_pool::Endpoint::new(name, 1234).unwrap(),
                dns,
            })
            .build()
            .unwrap();
            let mut connection = pool.acquire().await.unwrap();
            let mut ready = [0; 5];
            connection.read_exact(&mut ready).await.unwrap();
            assert_eq!(&ready, b"ready");
            assert_eq!(exchange(&mut connection).await, 1);
            drop(connection);
            server.await.unwrap();
        }
    })
    .await;
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn socks_rejects_failure_and_malformed_replies() {
    bounded(async {
        for reply in [
            vec![5, 5, 0, 1],
            vec![4, 0, 0, 1],
            vec![5, 0, 1, 1],
            vec![5, 0, 0, 9],
            vec![5, 0, 0, 3, 0],
            vec![5, 0],
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socks_request(&mut socket).await;
                socket.write_all(&reply).await.unwrap();
            });
            let pool = Pool::builder(Route::Socks5 {
                proxy: proxy.into(),
                target: "remote.invalid:80".parse().unwrap(),
                dns: tokio_tcp_pool::Socks5Dns::Proxy,
            })
            .build()
            .unwrap();
            assert!(matches!(
                pool.acquire().await,
                Err(AcquireError::Connect(ConnectError::Socks5(_)))
            ));
            server.await.unwrap();
        }
    })
    .await;
}

#[cfg(feature = "http-connect")]
#[tokio::test]
async fn http_success_preserves_tunnel_bytes_and_formats_ipv6() {
    bounded(async {
        for (target, code) in [("remote.invalid:443", 200), ("[::1]:443", 299)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    http_request(&mut socket).await,
                    format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n")
                );
                socket
                    .write_all(
                        format!("HTTP/1.1 {code} Tunnel\r\nX-Test: yes\r\n\r\nready").as_bytes(),
                    )
                    .await
                    .unwrap();
                serve(socket, 42).await;
            });
            let pool = Pool::builder(Route::HttpConnect {
                proxy: proxy.into(),
                target: target.parse().unwrap(),
            })
            .build()
            .unwrap();
            let mut connection = pool.acquire().await.unwrap();
            let mut ready = [0; 5];
            connection.read_exact(&mut ready).await.unwrap();
            assert_eq!(&ready, b"ready");
            assert_eq!(exchange(&mut connection).await, 42);
            drop(connection);
            server.await.unwrap();
        }
    })
    .await;
}

#[cfg(feature = "http-connect")]
#[tokio::test]
async fn http_rejects_status_invalid_oversized_and_truncated_headers() {
    bounded(async {
        let responses = [
            b"HTTP/1.1 403 Forbidden\r\n\r\n".to_vec(),
            b"HTTP/1.1 407 Auth\r\n\r\n".to_vec(),
            b"HTTP/1.1 500 Error\r\n\r\n".to_vec(),
            b"NOT HTTP\r\n\r\n".to_vec(),
            b"HTTP/1.1 200 OK\r\ninvalid header\r\n\r\n".to_vec(),
            format!("HTTP/1.1 200 OK\r\nX-Large: {}\r\n\r\n", "x".repeat(16384)).into_bytes(),
            format!("HTTP/1.1 200 OK\r\n{}\r\n", "X: y\r\n".repeat(129)).into_bytes(),
            b"HTTP/1.1 200 OK\r\n".to_vec(),
            Vec::new(),
        ];
        for response in responses {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                http_request(&mut socket).await;
                let _ = socket.write_all(&response).await;
            });
            let pool = Pool::builder(Route::HttpConnect {
                proxy: proxy.into(),
                target: "remote.invalid:443".parse().unwrap(),
            })
            .build()
            .unwrap();
            assert!(matches!(
                pool.acquire().await,
                Err(AcquireError::Connect(ConnectError::HttpConnect(_)))
            ));
            server.await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn proxy_cancellation_and_timeout_wake_waiters() {
    bounded(async {
        let kinds = [
            #[cfg(feature = "socks5")]
            ProxyKind::Socks,
            #[cfg(feature = "http-connect")]
            ProxyKind::Http,
        ];
        for kind in kinds {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy = listener.local_addr().unwrap();
            let route = match kind {
                #[cfg(feature = "socks5")]
                ProxyKind::Socks => Route::Socks5 {
                    proxy: proxy.into(),
                    target: "remote.invalid:443".parse().unwrap(),
                    dns: tokio_tcp_pool::Socks5Dns::Proxy,
                },
                #[cfg(feature = "http-connect")]
                ProxyKind::Http => Route::HttpConnect {
                    proxy: proxy.into(),
                    target: "remote.invalid:443".parse().unwrap(),
                },
            };
            let pool = Pool::builder(route)
                .max_open(1)
                .connect_timeout(Duration::from_millis(100))
                .build()
                .unwrap();
            let p = pool.clone();
            let first = tokio::spawn(async move { p.acquire().await });
            let (mut peer, _) = listener.accept().await.unwrap();
            assert!(peer.read(&mut [0; 1024]).await.unwrap() > 0);
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
            let (_third_peer, _) = listener.accept().await.unwrap();
            assert!(matches!(
                next.await.unwrap(),
                Err(AcquireError::Connect(ConnectError::Timeout))
            ));
        }
    })
    .await;
}
