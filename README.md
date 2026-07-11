# Nacelle

Nacelle is an experimental Tokio-based Rust library for building typed streaming
services across TCP, Unix sockets, HTTP/1, and TLS-enabled listeners.

```rust
context.respond(transport_response).await
```

Each transport owns its request, response, and completion types. Handlers receive
a typed context with a streaming `NacelleBody`, connection metadata, and concrete
connection state, so services can process chunks without forcing full buffering.

## Status

Nacelle is currently `0.2.x`. It is ready for experiments and prototype
integrations, but the public API is still allowed to change before `1.0`.

The typed pipeline contracts, runtime limits, host/app builders, and telemetry
sink are the most stable parts of the API. Transport metadata, listener options,
stress-tool configuration, optional OpenSSL TLS detection, and OpenTelemetry
integration are still moving.

Authentication and compression are not implemented in Nacelle. Keep those in
your application, protocol layer, or edge proxy.

## Quick Start

Until you pin a released crate version, depend on this repository directly:

```toml
[dependencies]
nacelle = { git = "https://github.com/microsoft/nacelle" }
nacelle-reference-protocol = { git = "https://github.com/microsoft/nacelle" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The unpublished `nacelle-reference-protocol` package is an example fixture from
this repository, not part of Nacelle's library API. Minimal TCP service using
that fixture:

```rust
use nacelle::core::pipeline::handler_fn;
use nacelle::tcp::{TcpRequestContext, TcpResponse};
use nacelle::{NacelleApp, NacelleError, NacelleProtocols, NacelleTelemetry};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let handler = handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
        while let Some(chunk) = context.request_mut().body.next_chunk().await {
            let _ = chunk?;
        }

        context.respond(TcpResponse::bytes("ok")).await
    });

    let addr = "127.0.0.1:8080".parse().map_err(NacelleError::protocol)?;
    let protocols = NacelleProtocols::new()
        .tcp("echo", addr, LengthDelimitedProtocol);

    NacelleApp::new(handler)
        .with_telemetry(NacelleTelemetry::default())
        .with_ctrl_c_shutdown()
        .serve(protocols)
        .await
}
```

## Examples

Run the checked-in examples from a local checkout:

```bash
# TCP echo with the reference protocol
cargo run -p nacelle-examples --bin echo -- 127.0.0.1:8080

# One app core served through two TCP protocol adapters
cargo run -p nacelle-examples --bin app_core -- 127.0.0.1:8080 127.0.0.1:8081

# HTTP echo
cargo run -p nacelle-examples --no-default-features --features http --bin http_echo -- 127.0.0.1:8080

# HTTP memory budget guard demo
cargo run -p nacelle-examples --no-default-features --features http --bin memory_guard

# TCP memory budget guard demo with the reference protocol
cargo run -p nacelle-examples --bin tcp_memory_guard

# HTTPS echo with an ephemeral self-signed certificate
cargo run -p nacelle-examples --no-default-features --features http,tls-self-signed --bin tls_http_echo -- 127.0.0.1:8443

# TCP echo with an ephemeral self-signed certificate
cargo run -p nacelle-examples --features tls-self-signed --bin tls_echo -- 127.0.0.1:8443

# TCP and HTTP listeners sharing app state and one host
cargo run -p nacelle-examples --features http --bin dual_echo -- 127.0.0.1:8080 127.0.0.1:8081
```

## What Nacelle Provides

- Transport-owned typed request, response, and completion contracts.
- Static handler and middleware dispatch without boxed hot-path futures.
- App-core serving with swappable protocol adapters.
- Streaming request and response bodies.
- Custom TCP protocol support over TCP and Unix domain sockets.
- HTTP/1 serving through Hyper.
- Rustls TLS for HTTP and TCP.
- OpenSSL TLS for TCP, including optional plain/TLS detection on one listener.
- Shared runtime limits, backpressure, graceful shutdown, and telemetry hooks.
- A stress server and stress client for local performance validation.

## Feature Flags

Choose the smallest feature set that matches the transports you actually run:

```toml
# TCP with a custom protocol (enabled by default)
nacelle = { version = "0.2" }

# HTTP only
nacelle = { version = "0.2", default-features = false, features = ["http"] }

# TCP + HTTP + OpenTelemetry metrics
nacelle = { version = "0.2", features = ["http", "otel"] }

# Include setup hints in NacelleError Display output
nacelle = { version = "0.2", features = ["error-hints"] }

# Local self-signed TLS for tests
nacelle = { version = "0.2", features = ["tls-self-signed"] }

# TCP with OpenSSL, without Rustls
nacelle = { version = "0.2", default-features = false, features = ["tcp", "openssl"] }
```

| Feature | Purpose |
| --- | --- |
| `tcp` | Custom TCP protocol transport over TCP and Unix sockets. Enabled by default. |
| `error-hints` | Include actionable setup hints in `NacelleError` display output. |
| `http` | Hyper HTTP/1 server transport. |
| `tls` | Provider-neutral TLS capability. |
| `rustls` | Rustls-backed TLS for HTTP and TCP. |
| `openssl` | OpenSSL-backed TLS for TCP. |
| `openssl-vendored` | Build OpenSSL from source when native OpenSSL is unavailable. |
| `tls-self-signed` | Generate ephemeral Rustls self-signed certificates for local tests. |
| `otel` | OpenTelemetry metrics API integration through `NacelleTelemetry`. |
| `tokio-util` | Bridge `tokio_util::sync::CancellationToken` into Nacelle shutdown. |

OpenSSL builds need native OpenSSL development files unless you enable
`openssl-vendored`. Vendored OpenSSL also needs Perl on Windows.

## Workspace Layout

- `nacelle-core` contains shared typed pipeline, body, resource limit,
    lifecycle, telemetry, and TLS primitives.
- `nacelle-tcp` contains the TCP transport, protocol runtime, and TCP limits.
- `nacelle-http` contains the Hyper HTTP/1 transport, HTTP limits, and HTTP edge
    policy.
- `nacelle` is the convenience crate with `core`, `codec`, `tcp`, `http`, and
    `runtime` capability namespaces.
- `examples/nacelle-examples` owns unpublished runnable examples and benchmarks.
- `examples/nacelle-reference-protocol` is an unpublished protocol fixture used
    by examples, tests, benchmarks, and stress tools.
- `examples/nacelle-stress-*` contains the stress harness.

## Production Notes

Use explicit `NacelleLimits` plus transport-specific `NacelleTcpLimits` or
`NacelleHttpLimits` for production services. For internet-facing deployments,
the recommended shape is:

```text
client -> proxy/load balancer/TLS -> Nacelle service
```

The proxy should own public TLS automation, coarse traffic filtering, and
external idle timeouts. Nacelle should own application limits, protocol handling,
body limits, telemetry, and graceful shutdown.

For direct HTTP exposure, configure `NacelleHttpPolicy` deliberately: trusted
proxy IPs, Host/method/URI/header limits, access logging, and per-peer caps. For
high connection counts, tune TCP read and response buffer capacities before
raising `max_connections`.

Nacelle does not enforce a runtime memory cap by default. Set
`NacelleLimits::with_max_memory_bytes(...)` only when you want to opt into
Nacelle's memory allocation budget for a measured deployment or test profile.

Self-signed certificates are intended for local tests and auto-deploy flows, not
as a public-edge certificate strategy.

## Stress Harness

Run a short plain TCP smoke profile:

```bash
cargo run --release --package nacelle-stress-server -- \
    --config examples/nacelle-stress-server/configs/tcp.toml

# In another shell:
cargo run --release --package nacelle-stress-test -- \
    --connections 32 \
    --pipeline 16 \
    --duration-secs 15
```

The root [config.toml](config.toml) is loaded automatically when the stress server
is run from the repository root. It enables self-signed TCP TLS for local runs,
so the stress client needs `--tls-insecure` with that default config.

For repeatable local profiles, use the helper scripts:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml
```

```powershell
.\examples\run-stress-test.ps1 -Config examples/nacelle-stress-server/configs/tcp.toml
```

## Development

Verify the workspace before submitting changes:

```bash
cargo fmt --all
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features
```

Build release binaries:

```bash
cargo build --release
```

Build just the stress binaries and copy them to `./artifacts/`:

```bash
./build-all.sh
```

## Documentation

- [Getting started](docs/tutorials/getting-started.md)
- [Architecture](docs/topics/architecture.md)
- [Runtime limits and backpressure](docs/topics/runtime-limits.md)
- [Operations](docs/topics/operations.md)
- [Production configuration](docs/how-to/configure-production.md)
- [HTTP hardening](docs/how-to/harden-http.md)
- [Stress testing](docs/how-to/run-stress-tests.md)
- [Performance tuning](docs/how-to/compare-performance.md)
- [Security scanning](docs/how-to/security-scanning.md)
- [Reference protocol](docs/reference/protocol.md)
- [API stability](docs/reference/api-stability.md)
- [Rust API reference](docs/reference/rust-api.md)

Build the mdBook site:

```bash
mdbook build
```

Generate Rust API docs:

```bash
cargo doc --workspace --all-features --no-deps
```

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute to this project.

## License

This project is licensed under the [MIT License](LICENSE.txt).

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.