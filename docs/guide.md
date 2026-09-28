# Connection and pool semantics

## Configuration

`Pool::builder(route).build()` creates an empty pool without DNS or network I/O.
All clones share that route, the configuration, idle connections and capacity.
`PoolConfig` can replace all limits through `.config(config)`; later builder
methods override individual fields.

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_open` | 8 | Idle, leased and establishing connections combined; zero is invalid |
| `connect_timeout` | 10 s | One deadline for the complete establishment |
| `idle_timeout` | 60 s | Time since the last release; zero disables reuse |
| `max_lifetime` | `None` | Age since establishment started; zero disables reuse |

Expiration uses Tokio's monotonic clock. Expired idle sockets may remain allocated
until acquisition or pool destruction. No maintenance task, warmup or minimum
idle count exists. An active lease is never interrupted by expiration.

## Endpoint and DNS handling

`Endpoint::new(host, port)` validates an IP address or ASCII DNS hostname.
`FromStr` accepts `127.0.0.1:80`, `[::1]:80` and `example.com:80`. IPv6 passed to
`new` is unbracketed. Convert international domain names to IDNA ASCII first.
Whitespace, control characters and HTTP header delimiters are rejected.

Resolution happens during each new connection. Direct TCP and proxy TCP try the
resolved addresses sequentially within the one establishment deadline. SOCKS5
local DNS similarly attempts the resolved target addresses, using a fresh proxy
connection for each target attempt. No Happy Eyeballs algorithm is implemented.
SOCKS5 proxy DNS and CONNECT send the destination hostname to the proxy.

The pool does not retry application operations. It cannot know whether bytes
already sent caused a remote side effect.

## TLS

The `tls` feature re-exports its rustls version. Supply an `Arc<ClientConfig>` or
`TlsConfig` to the builder. Configure trust roots, client authentication and ALPN
in rustls. The pool does not install a root store or disable verification.

By default, the route target supplies the certificate identity and DNS-name SNI.
A literal IP is validated as an IP identity and sends no DNS-name SNI. An explicit
`TlsConfig::new(client).server_name("service.example.com")` override separates
network addressing from TLS identity.

TLS is established after TCP or proxy negotiation. TLS to the proxy itself is not
supported. Proxy authentication is also outside version 0.1.

## Recycling and shutdown

`release(self)` means the upper protocol permits a new exchange. The caller must
consume the complete response, handle framing, and flush outgoing data before
release. The pool cannot recognize an application protocol error or distinguish
expected bytes from a partial response.

`discard(self)` and ordinary drop close the socket and return its capacity.
`close(self).await` calls asynchronous shutdown, sends TLS `close_notify` when
applicable and drops the socket. It does not wait for the peer's TLS close_notify.
A pending close still occupies a slot. Cancellation or an error destroys the
connection and frees that slot. Apply a caller-chosen timeout if needed.

Read, write, vectored write, flush and shutdown delegate to the transport without
I/O deadlines. Errors mark the connection broken. Nonempty reads returning EOF,
nonempty writes returning zero, and any shutdown attempt also prevent reuse.
Zero-capacity reads do not mark a connection broken.

The idle check inspects socket errors and peeks at one byte without consuming it.
EOF or a known socket error prevents reuse. WouldBlock and pending data do not
prove failure. It cannot detect a silent network partition or interpret TLS
records such as close_notify without reading them. A healthy-looking connection
may therefore fail on its next operation.

## Concurrency, cancellation and ownership

Each connection slot has one RAII owner throughout establishment, leasing and
idle storage. A failed or cancelled establishment drops that owner, restores
capacity and wakes waiters. Idle entries are removed under a short mutex, but
network operations and connection destruction take place outside it.

Waiters register before inspecting pool state. Capacity changes notify all
registered waiters, so cancelling one waiter cannot consume the only wakeup.
Strict FIFO fairness is not promised.

Leased and idle connections hold only a weak reference to the pool. Dropping the
last `Pool` closes its idle sockets even if leased connections still exist. Those
leases continue functioning, and their eventual release destroys them. There is
no global pool cache or explicit global shutdown operation in version 0.1.

## Errors

`build()` returns `ConfigError` for a zero capacity or invalid TLS identity.
Endpoint parsing returns `EndpointError`.

`acquire()` returns `AcquireError::Connect(ConnectError)`. Connection errors
identify DNS resolution, TCP, SOCKS5, HTTP CONNECT, TLS or the total timeout.
I/O and parser errors remain available through `Error::source`. Proxy refusals
retain typed `Socks5ReplyError` or `HttpConnectStatusError` values inside the
stage's `io::Error`; these can be inspected with `io::Error::get_ref` and
`downcast_ref`. Ordinary stream operations return `std::io::Error`.

No `Closed` acquisition state is needed because global pool shutdown is not part
of this version. A borrowed pool stays alive for the duration of acquisition.

CONNECT responses allow 2xx status codes, up to 16 KiB and 128 header fields.
Malformed, truncated, oversized and non-success responses fail establishment.
Headers are read exactly through their terminating CRLF pair, preserving any
following tunnel bytes. Proxy credentials and HTTPS proxies are unsupported.
