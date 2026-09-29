# Acceptance coverage

Tests use loopback listeners and a test-only CA. They need no public DNS or
Internet service. `localhost` exercises the system resolver; `.invalid` targets
verify that proxy DNS does not trigger local destination resolution.

| Contract | Coverage |
| --- | --- |
| New connection, reuse, discard, drop and close | `tests/pool.rs` lifecycle test, identified echo connections |
| One shared pool and at most two open connections | 64 concurrent tasks using cloned handles |
| Failed connects and waiter progress | Concurrent connection refusals at capacity one |
| Cancelled TCP establishment | Unit test polls TCP connect once, verifies the reserved count, cancels and reacquires |
| Cancelled proxy and TLS handshakes | Local peers stall negotiation while another task waits for capacity |
| Total establishment deadline | Paused clock spans CONNECT and TLS stages; combined duration exceeds the limit |
| Lazy idle and lifetime expiration | Clock advances between network exchanges, including a lease held beyond its lifetime |
| Last pool destruction | Idle peer observes EOF while an outstanding lease remains usable |
| Non-destructive health check | Pending application bytes survive reuse; private test waits until FIN is observable |
| Broken connections | TCP reset produces a read error and sets the private broken flag; EOF and write shutdown also prevent reuse |
| Tokio I/O | Read/write, flush, shutdown and `tokio::io::copy` |
| SOCKS5 | Local/proxy DNS, IPv4/IPv6/domain addresses, username/password authentication, reply failures and malformed/truncated replies |
| CONNECT | 2xx, IPv6 authority, Basic and custom `Proxy-Authorization`, coalesced tunnel bytes, 403/407/500, malformed/oversized/truncated headers |
| TLS | Direct, SOCKS5 and CONNECT, incorrect trust or hostname, explicit identity override, interrupted handshake |
| Graceful TLS close | Peer reads clean EOF after close_notify; cancelling an unpolled close yields unclean EOF |
| No implicit read timeout | A TLS read remains pending beyond the establishment timeout |
| Cargo features | Full suite, TCP only, and each optional feature in isolation |

The fixture private key is intentionally public test data. It has no use outside
these tests. `tests/fixtures/generate.sh` replaces the CA, localhost certificate
and key using OpenSSL. The checked-in certificates last 100 years from generation.
Do not use these certificates or keys for an application.

Run `cargo test --locked --all-features` for the complete local suite.
CI also checks Rust 1.85, release mode, formatting, Clippy, rustdoc and packaging.
Portability jobs run on Linux, macOS and Windows.
