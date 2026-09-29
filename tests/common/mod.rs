#![allow(dead_code)]
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};
use tokio_tcp_pool::{Connection, Endpoint, Pool, Route};

pub struct Server {
    pub addr: SocketAddr,
    pub accepted: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
pub async fn echo() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, _) = result.unwrap();
                    let id = count.fetch_add(1, Ordering::SeqCst) + 1;
                    tasks.spawn(serve(stream, id as u64));
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    Server {
        addr,
        accepted,
        task,
    }
}
pub async fn serve(mut stream: impl AsyncRead + AsyncWrite + Unpin, id: u64) {
    while let Ok(byte) = stream.read_u8().await {
        if stream.write_u64(id).await.is_err()
            || stream.write_u8(byte).await.is_err()
            || stream.flush().await.is_err()
        {
            break;
        }
    }
}
pub fn pool(addr: SocketAddr) -> Pool {
    Pool::builder(Route::Direct {
        target: addr.into(),
    })
    .max_open(2)
    .build()
    .unwrap()
}
pub async fn exchange(conn: &mut Connection) -> u64 {
    conn.write_all(b"x").await.unwrap();
    conn.flush().await.unwrap();
    let id = conn.read_u64().await.unwrap();
    assert_eq!(conn.read_u8().await.unwrap(), b'x');
    id
}
pub async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("test deadline")
}
pub fn endpoint(addr: SocketAddr) -> Endpoint {
    addr.into()
}

#[cfg(any(feature = "socks5", feature = "http-connect"))]
#[derive(Clone, Copy)]
pub enum ProxyKind {
    #[cfg(feature = "socks5")]
    Socks,
    #[cfg(feature = "http-connect")]
    Http,
}
#[cfg(any(feature = "socks5", feature = "http-connect"))]
pub async fn tunnel(kind: ProxyKind, upstream: SocketAddr) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (mut downstream, _) = result.unwrap();
                    count.fetch_add(1, Ordering::SeqCst);
                    tasks.spawn(async move {
                        match kind {
                            #[cfg(feature = "socks5")]
                            ProxyKind::Socks => { socks_request(&mut downstream).await; downstream.write_all(&[5,0,0,1,127,0,0,1,0,0]).await.unwrap(); }
                            #[cfg(feature = "http-connect")]
                            ProxyKind::Http => { http_request(&mut downstream).await; downstream.write_all(b"HTTP/1.1 200 OK\r\n\r\n").await.unwrap(); }
                        }
                        let mut upstream = tokio::net::TcpStream::connect(upstream).await.unwrap();
                        let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    Server {
        addr,
        accepted,
        task,
    }
}
#[cfg(feature = "socks5")]
pub async fn socks_request(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>, u16) {
    let mut greeting = [0; 3];
    stream.read_exact(&mut greeting).await.unwrap();
    assert_eq!(greeting, [5, 1, 0]);
    stream.write_all(&[5, 0]).await.unwrap();
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..3], &[5, 1, 0]);
    let len = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream.read_u8().await.unwrap() as usize,
        _ => panic!("address type"),
    };
    let mut host = vec![0; len];
    stream.read_exact(&mut host).await.unwrap();
    let port = stream.read_u16().await.unwrap();
    (header[3], host, port)
}
#[cfg(feature = "http-connect")]
pub async fn http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        assert!(request.len() < 16384);
        request.push(stream.read_u8().await.unwrap());
    }
    String::from_utf8(request).unwrap()
}

#[cfg(feature = "tls")]
pub fn tls_configs() -> (tokio_tcp_pool::TlsConfig, tokio_rustls::TlsAcceptor) {
    use tokio_tcp_pool::rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    };
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(CertificateDer::from(
            include_bytes!("../fixtures/ca.der").to_vec(),
        ))
        .unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(
                include_bytes!("../fixtures/server.der").to_vec(),
            )],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                include_bytes!("../fixtures/server-key.der").to_vec(),
            )),
        )
        .unwrap();
    (
        tokio_tcp_pool::TlsConfig::new(Arc::new(client)),
        tokio_rustls::TlsAcceptor::from(Arc::new(server)),
    )
}
#[cfg(feature = "tls")]
pub async fn tls_echo() -> Server {
    let (_, acceptor) = tls_configs();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, _) = result.unwrap(); let acceptor = acceptor.clone();
                    let id = count.fetch_add(1, Ordering::SeqCst) + 1;
                    tasks.spawn(async move { if let Ok(stream) = acceptor.accept(stream).await { serve(stream, id as u64).await; } });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    Server {
        addr,
        accepted,
        task,
    }
}
