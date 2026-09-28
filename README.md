# async-tls-pool

Reuse TCP/TLS connections to one destination over a direct connection, SOCKS5,
or HTTP CONNECT. Each acquired connection implements Tokio's `AsyncRead` and
`AsyncWrite`, so ordinary Tokio I/O works without a protocol adapter.

A connection returns to the pool only through `release()`. Dropping it discards
it. This lets a protocol implementation reuse a fully consumed response stream
without accidentally recycling a failed or partially consumed exchange.

The pool creates connections on demand and starts no background tasks.

## Installation

Requires Rust 1.85 or newer and a Tokio runtime with I/O and time enabled.
Until a crates.io release, use the Git repository with access to the private repo:

```toml
[dependencies]
async-tls-pool = { git = "https://github.com/theking90000/async-tls-pool" }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "io-util", "time"] }
```

## Acquiring and reusing a stream

Each pool has one immutable route. Cloning a pool shares its connections and
limits. Separate destinations require separate pools.

```rust,no_run
use async_tls_pool::{Pool, Route};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let pool = Pool::builder(Route::Direct {
    target: "127.0.0.1:9000".parse()?,
})
.max_open(8)
.connect_timeout(Duration::from_secs(5))
.idle_timeout(Duration::from_secs(60))
.max_lifetime(Duration::from_secs(3600))
.build()?;

let mut connection = pool.acquire().await?;
connection.write_all(b"ping").await?;
connection.flush().await?;
let mut response = [0; 4];
connection.read_exact(&mut response).await?;

// Release only when the application protocol permits another exchange.
connection.release();
# Ok(())
# }
```

`discard()` destroys the connection immediately. `close().await` attempts an
orderly shutdown, including TLS `close_notify`, and then destroys it. Dropping a
connection, including through `?` or cancellation, has the same effect as
`discard()`. Observed I/O errors, read EOF and write shutdown prevent reuse even
if `release()` is called afterward.

## Proxies and TLS

The default features enable TLS, SOCKS5 and HTTP CONNECT. TLS is applied to the
destination after establishing the transport. The caller supplies the rustls
configuration, including trusted roots and any ALPN protocols.

```rust,no_run
# #[cfg(all(feature = "tls", feature = "socks5"))]
# fn example(client: std::sync::Arc<async_tls_pool::rustls::ClientConfig>)
#     -> Result<async_tls_pool::Pool, Box<dyn std::error::Error>> {
use async_tls_pool::{Pool, Route, Socks5Dns};

let pool = Pool::builder(Route::Socks5 {
    proxy: "127.0.0.1:1080".parse()?,
    target: "storage.example.com:443".parse()?,
    dns: Socks5Dns::Proxy,
})
.tls(client)
.build()?;
# Ok(pool)
# }
```

`Socks5Dns::Proxy` sends the hostname to the proxy without resolving it locally.
`Socks5Dns::Local` resolves it for each new connection. Direct connections and
proxy endpoints use the system resolver through Tokio. No DNS cache is added.

`Route::HttpConnect { proxy, target }` establishes an unauthenticated CONNECT
tunnel over plain TCP. TLS always validates the destination identity, never the
proxy identity. `TlsConfig::server_name` provides an explicit override when
connecting to an IP address that serves a DNS certificate.

| Feature | Effect |
| --- | --- |
| `tls` | rustls through tokio-rustls, with the ring provider and TLS 1.2/1.3 |
| `socks5` | SOCKS5 without proxy authentication |
| `http-connect` | HTTP CONNECT, with httparse for response validation |

For only TCP and pooling, set `default-features = false`. This removes TLS and
HTTP parser dependencies.

## Limits and deadlines

`max_open` counts idle connections, active leases and connections being
established. Acquisition waits when every slot is occupied. The default is 8.

`connect_timeout` covers the complete DNS, TCP, proxy and TLS establishment, with
a default of 10 seconds. It starts after acquiring capacity. There is no implicit
acquisition or read/write timeout. An application can wrap any operation in
`tokio::time::timeout`.

Idle connections expire lazily, by default after 60 seconds. `max_lifetime` is
optional and limits reuse without interrupting an active lease. Dropping the last
pool handle closes idle connections; existing leases can finish but cannot return
to the pool.

A nonblocking socket peek detects some closed idle connections without consuming
application data. It cannot prove that a peer is alive, and the peer can fail
immediately after the check. The next I/O operation can still fail.

See the [guide](docs/guide.md) for configuration and error semantics, the
[local echo example](examples/README.md), and the
[acceptance coverage](docs/testing.md). No benchmarks are included in this version.

## License

MIT.
