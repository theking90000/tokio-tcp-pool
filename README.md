# async-tls-pool

Reuse TCP/TLS connections to one destination over a direct connection, SOCKS5,
or HTTP CONNECT. Connections implement Tokio's `AsyncRead` and `AsyncWrite`.

Connections return to the pool only through an explicit `release()`.
Dropping a connection discards it. The pool starts no background tasks.
