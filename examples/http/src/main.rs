use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinSet,
};
use tokio_tcp_pool::{
    Endpoint, Pool, Route, TlsConfig,
    rustls::{ClientConfig, RootCertStore, crypto::ring::default_provider},
};

async fn get(pool: Pool, path: String) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = pool.acquire().await?;

    println!("Connection :: {:p}, Req={}", &connection, &path);

    connection
        .write_all(format!("GET {} HTTP/1.1\r\n", path).as_bytes())
        .await?;
    connection.write_all(b"Host: theking90000.be\r\n").await?;
    connection
        .write_all(b"User-Agent: async-tls-pool/0.1\r\n")
        .await?;
    connection
        .write_all(b"Connection: keep-alive\r\n\r\n")
        .await?;

    let mut response = vec![0; 1024];

    loop {
        let n = connection.read(&mut response).await?;

        println!(
            "Received {} bytes {:?}",
            n,
            str::from_utf8(&response[..n]).unwrap_or("Invalid UTF-8")
        );
        // Check if HTTP/1.1 200 OK is present in the response
        if response
            .windows(15)
            .any(|window| window == b"HTTP/1.1 200 OK\r\n")
        {
            println!("Received HTTP/1.1 200 OK");
        }
        if response
            .windows(24)
            .any(|window| window == b"HTTP/1.1 404 Not Found\r\n")
        {
            println!("Received HTTP/1.1 404 Not Found");
        }

        // normally : should do parsing of content length header.
        if n < 1024 {
            break;
        }
    }

    println!("Finished reading response for path: {}", path);
    // if error :
    // connection.discard();
    connection.release();

    Ok(())
}

async fn get_panic(pool: Pool, path: String) -> () {
    get(pool, path).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    default_provider()
        .install_default()
        .expect("Échec de l'installation du fournisseur crypto ring");

    let mut root_store = RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().unwrap() {
        root_store.add(cert)?;
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let config_arc = Arc::new(client_config);

    let pool = Pool::builder(Route::Direct {
        target: Endpoint::new("theking90000.be", 443)?,
    })
    .tls(TlsConfig::new(config_arc))
    .max_open(2)
    .build()?;

    let mut j = JoinSet::new();

    for i in 0..10 {
        let path = format!("/{}", i).to_string();
        j.spawn(get_panic(pool.clone(), path));
    }
    j.spawn(get_panic(pool.clone(), "/404".to_string()));
    j.spawn(get_panic(pool.clone(), "/404-2".to_string()));
    j.spawn(get_panic(pool.clone(), "/404-3".to_string()));

    while let Some(res) = j.join_next().await {}

    drop(pool);
    Ok(())
}
