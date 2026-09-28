#![allow(dead_code)]
use async_tls_pool::{Connection, Endpoint, Pool, Route};
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
