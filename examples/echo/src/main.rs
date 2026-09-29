use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_tcp_pool::{Pool, Route};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let pool = Pool::builder(Route::Direct {
        target: listener.local_addr()?.into(),
    })
    .max_open(1)
    .build()?;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let (mut reader, mut writer) = stream.split();
        tokio::io::copy(&mut reader, &mut writer).await
    });
    for message in [b"ping", b"pong"] {
        let mut connection = pool.acquire().await?;
        connection.write_all(message).await?;
        connection.flush().await?;
        let mut response = [0; 4];
        connection.read_exact(&mut response).await?;
        assert_eq!(&response, message);
        println!("{}", std::str::from_utf8(&response)?);
        connection.release();
    }
    drop(pool);
    server.await??;
    Ok(())
}
