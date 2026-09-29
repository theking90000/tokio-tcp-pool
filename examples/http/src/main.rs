use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
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

    let mut buf = vec![0; 4096];
    let mut read = 0;
    let mut body;
    let mut content_length: usize = 0;
    let mut remaining = 0;

    loop {
        match connection.read(&mut buf[read..(read + 32)]).await {
            Ok(n) => {
                read += n;
                println!("Connexion :: {:p}, Read read={}", &connection, read);
                let mut headers = [httparse::EMPTY_HEADER; 100];
                let mut resp = httparse::Response::new(&mut headers);
                // println!("Connexion :: {:p}, data data={:?}",&connection, str::from_utf8(&buf[..read]));
                match resp.parse(&buf[..read]) {
                    Ok(httparse::Status::Partial) => {}
                    Ok(httparse::Status::Complete(n)) => {
                        //println!("Connexion :: {:p}, Headers={:?}", &connection, headers.len());
                        for h in headers {
                            if h.name.to_lowercase() == "content-length" {
                                content_length =
                                    str::from_utf8(h.value).unwrap().parse::<usize>().unwrap();
                                remaining = content_length - (read - n);
                                println!(
                                    "Connexion :: {:p}, content_length={:?}, remaining_read={}",
                                    &connection, &content_length, remaining
                                );
                            }
                        }

                        body = n;
                        break;
                    }
                    Err(e) => {
                        println!("Connexion :: {:p}, Error! {:?}", &connection, e);
                        connection.discard();
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                println!("Connexion :: {:p}, Error! {:?}", &connection, e);
                connection.discard();
                return Ok(());
            }
        };
    }

    // data remaining "body..read"
    println!(
        "Connexion :: {:p}, Read~> {:?}",
        &connection,
        str::from_utf8(&buf[body..read])
    );
    while remaining != 0 {
        match connection.read(&mut buf[..40.min(remaining)]).await {
            Ok(n) => {
                println!(
                    "Connexion :: {:p}, Read~> {:?}",
                    &connection,
                    str::from_utf8(&buf[..n])
                );
                remaining -= n;
            }
            Err(e) => {
                println!("Connexion :: {:p}, Error! {:?}", &connection, e);
                connection.discard();
                return Ok(());
            }
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

    while let Some(_) = j.join_next().await {}

    get_panic(pool.clone(), "/".to_string()).await;

    drop(pool);
    Ok(())
}
