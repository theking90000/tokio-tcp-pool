//! Pool lifecycle, concurrency and endpoint integration tests.
mod common;
use async_tls_pool::{AcquireError, ConfigError, ConnectError, Connection, Endpoint, Pool, Route};
use common::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[test]
fn endpoint_and_config_validation() {
    for text in ["127.0.0.1:8080", "[::1]:443", "example.com:443"] {
        assert_eq!(text.parse::<Endpoint>().unwrap().to_string(), text);
    }
    for text in [
        "::1:443",
        "host",
        "host:65536",
        "host:bad",
        "bad\r\nInjected:80",
        "-bad:80",
        "a..b:80",
        ":80",
    ] {
        assert!(text.parse::<Endpoint>().is_err(), "{text}");
    }
    assert!(Endpoint::new("::1", 80).is_ok());
    assert!(Endpoint::new("localhost.", 80).is_ok());
    assert!(Endpoint::new("a".repeat(256), 80).is_err());
    let result = Pool::builder(Route::Direct {
        target: "localhost:1".parse().unwrap(),
    })
    .max_open(0)
    .build();
    assert!(matches!(result, Err(ConfigError::ZeroMaxOpen)));
    fn shared<T: Clone + Send + Sync>() {}
    fn sent<T: Send>() {}
    shared::<Pool>();
    sent::<Connection>();
}

#[tokio::test]
async fn release_reuses_discard_drop_and_close_do_not() {
    bounded(async {
        let server = echo().await;
        let pool = pool(server.addr);
        assert_eq!(server.accepted.load(Ordering::SeqCst), 0);
        let mut a = pool.acquire().await.unwrap();
        let id = exchange(&mut a).await;
        a.release();
        let mut b = pool.clone().acquire().await.unwrap();
        assert_eq!(exchange(&mut b).await, id);
        b.discard();
        let mut c = pool.acquire().await.unwrap();
        assert_ne!(exchange(&mut c).await, id);
        drop(c);
        let mut d = pool.acquire().await.unwrap();
        let id = exchange(&mut d).await;
        d.close().await.unwrap();
        let mut e = pool.acquire().await.unwrap();
        assert_ne!(exchange(&mut e).await, id);
        assert_eq!(server.accepted.load(Ordering::SeqCst), 4);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_open_under_contention_and_waiter_cancellation() {
    bounded(async {
        let server = echo().await;
        let pool = pool(server.addr);
        let active = Arc::new(AtomicUsize::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let pool = pool.clone();
            let active = active.clone();
            tasks.spawn(async move {
                let mut conn = pool.acquire().await.unwrap();
                assert!(active.fetch_add(1, Ordering::SeqCst) < 2);
                exchange(&mut conn).await;
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
                conn.release();
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert!(server.accepted.load(Ordering::SeqCst) <= 2);
        let a = pool.acquire().await.unwrap();
        let b = pool.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pool.acquire())
                .await
                .is_err()
        );
        let p = pool.clone();
        let waiter = tokio::spawn(async move { p.acquire().await.unwrap() });
        tokio::task::yield_now().await;
        a.discard();
        let c = waiter.await.unwrap();
        drop((b, c));
    })
    .await;
}

#[tokio::test]
async fn failures_do_not_leak_capacity() {
    // Windows can take about a second to report each refused TCP connect.
    // Eight serialized attempts need a larger test guard than successful I/O.
    tokio::time::timeout(Duration::from_secs(30), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let pool = Pool::builder(Route::Direct {
            target: addr.into(),
        })
        .max_open(1)
        .build()
        .unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let pool = pool.clone();
            tasks.spawn(async move {
                assert!(matches!(
                    pool.acquire().await,
                    Err(AcquireError::Connect(ConnectError::Tcp(_)))
                ));
            });
        }
        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_expiration_is_lazy() {
    let server = echo().await;
    let pool = Pool::builder(Route::Direct {
        target: server.addr.into(),
    })
    .idle_timeout(Duration::from_secs(2))
    .build()
    .unwrap();
    let mut a = pool.acquire().await.unwrap();
    let id = exchange(&mut a).await;
    a.release();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::time::resume();
    assert_eq!(server.accepted.load(Ordering::SeqCst), 1);
    let mut b = pool.acquire().await.unwrap();
    assert_ne!(exchange(&mut b).await, id);
}

#[tokio::test]
async fn lifetime_does_not_interrupt_leases_and_prevents_recycling() {
    let server = echo().await;
    let pool = Pool::builder(Route::Direct {
        target: server.addr.into(),
    })
    .max_lifetime(Duration::from_secs(5))
    .build()
    .unwrap();
    let mut a = pool.acquire().await.unwrap();
    let id = exchange(&mut a).await;
    a.release();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::time::resume();
    let mut a = pool.acquire().await.unwrap();
    assert_eq!(exchange(&mut a).await, id);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::time::resume();
    assert_eq!(exchange(&mut a).await, id);
    a.release();
    let mut b = pool.acquire().await.unwrap();
    assert_ne!(exchange(&mut b).await, id);
    b.release();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::time::resume();
    let mut c = pool.acquire().await.unwrap();
    assert_ne!(exchange(&mut c).await, id + 1);
}

#[tokio::test]
async fn last_pool_drop_closes_idle_but_preserves_leased_streams() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = pool(listener.local_addr().unwrap());
        let a = pool.acquire().await.unwrap();
        let (mut peer_a, _) = listener.accept().await.unwrap();
        let mut b = pool.acquire().await.unwrap();
        let (mut peer_b, _) = listener.accept().await.unwrap();
        a.release();
        drop(pool);
        assert_eq!(
            peer_a.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        b.write_all(b"x").await.unwrap();
        assert_eq!(peer_b.read_u8().await.unwrap(), b'x');
        b.release();
        assert_eq!(
            peer_b.read_u8().await.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
    })
    .await;
}

#[tokio::test]
async fn health_check_preserves_pending_application_bytes() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = pool(listener.local_addr().unwrap());
        let a = pool.acquire().await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        a.release();
        peer.write_all(b"hello").await.unwrap();
        let mut b = pool.acquire().await.unwrap();
        let mut buf = [0; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
        b.release();
    })
    .await;
}

#[tokio::test]
async fn observed_eof_and_write_shutdown_prevent_reuse() {
    bounded(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pool = pool(listener.local_addr().unwrap());
        let mut a = pool.acquire().await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        peer.shutdown().await.unwrap();
        assert_eq!(a.read(&mut [0; 1]).await.unwrap(), 0);
        a.release();
        let mut b = pool.acquire().await.unwrap();
        let _peer = listener.accept().await.unwrap();
        b.shutdown().await.unwrap();
        b.release();
        let _c = pool.acquire().await.unwrap();
        let _peer = listener.accept().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn io_error_prevents_reuse_and_copy_works() {
    bounded(async {
        let server = echo().await;
        let pool = pool(server.addr);
        let mut a = pool.acquire().await.unwrap();
        let mut source: &[u8] = b"x";
        assert_eq!(tokio::io::copy(&mut source, &mut a).await.unwrap(), 1);
        let id = a.read_u64().await.unwrap();
        assert_eq!(a.read_u8().await.unwrap(), b'x');
        a.shutdown().await.unwrap();
        assert!(a.write_all(b"x").await.is_err());
        a.release();
        let mut b = pool.acquire().await.unwrap();
        assert_ne!(exchange(&mut b).await, id);
    })
    .await;
}
