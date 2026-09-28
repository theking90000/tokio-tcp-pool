# Examples

Each example is an independent crate that depends on the library through a local
path. The echo example starts a loopback server, performs two exchanges through
one pool, and releases the connection after each complete response.

```sh
cargo run --locked --manifest-path examples/echo/Cargo.toml
```
