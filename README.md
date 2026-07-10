# Nacelle

Nacelle is an experimental Tokio-based Rust library for building streaming
services with one handler shape across TCP, Unix sockets, HTTP/1, and TLS-enabled
listeners.

```rust
async fn handle(request: NacelleRequest) -> Result<NacelleResponse, NacelleError>
```

Handlers receive a streaming `NacelleBody`, so services can process request and
response chunks without forcing full buffering.

## Status

Nacelle is currently `0.2.x`. It is ready for experiments and prototype
integrations, but the public API is still allowed to change before `1.0`.

The core request/response model, handler adapter, runtime limits, host/app
builders, and telemetry sink are the most stable parts of the API. Transport
metadata, listener options, stress-tool configuration, optional OpenSSL TLS
detection, and some `tower`/`otel` feature combinations are still moving.

Authentication and compression are not implemented in Nacelle. Keep those in
your application, protocol layer, or edge proxy.

## Quick Start

Until you pin a released crate version, depend on this repository directly:

```toml
[dependencies]
nacelle = { git = "https://github.com/microsoft/nacelle", features = ["reference_protocol"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Minimal TCP service using the reference length-delimited protocol:

```rust
use nacelle::prelude::*;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let handler = handler_fn(|mut request: NacelleRequest| async move {
        while let Some(chunk) = request.body.next_chunk().await {
            let _ = chunk?;
        }

        Ok(NacelleResponse::tcp_bytes("ok"))
    });

    let addr = "127.0.0.1:8080".parse().map_err(NacelleError::protocol)?;
    let protocols = NacelleProtocols::new()
        .tcp::<FrameRequest, _>("echo", addr, LengthDelimitedProtocol);

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
cargo run --features reference_protocol --example echo -- 127.0.0.1:8080

# One app core served through two TCP protocol adapters
cargo run --features reference_protocol --example app_core -- 127.0.0.1:8080 127.0.0.1:8081

# HTTP echo
cargo run --no-default-features --features http --example http_echo -- 127.0.0.1:8080

# HTTP memory budget guard demo
cargo run --no-default-features --features http --example memory_guard

# TCP memory budget guard demo with the reference protocol
cargo run --features reference_protocol --example tcp_memory_guard

# HTTPS echo with an ephemeral self-signed certificate
cargo run --no-default-features --features http,tls-self-signed --example tls_http_echo -- 127.0.0.1:8443

# TCP echo with an ephemeral self-signed certificate
cargo run --features reference_protocol,tls-self-signed --example tls_echo -- 127.0.0.1:8443

# TCP and HTTP listeners sharing one handler and host
cargo run --features reference_protocol,http --example dual_echo -- 127.0.0.1:8080 127.0.0.1:8081
```

## What Nacelle Provides

- One app-facing handler model for multiple transports.
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
# TCP with the built-in reference protocol
nacelle = { version = "0.2", features = ["reference_protocol"] }

# HTTP only
nacelle = { version = "0.2", default-features = false, features = ["http"] }

# TCP + HTTP + OpenTelemetry metrics
nacelle = { version = "0.2", features = ["reference_protocol", "http", "otel"] }

# Include setup hints in NacelleError Display output
nacelle = { version = "0.2", features = ["reference_protocol", "error-hints"] }

# Local self-signed TLS for tests
nacelle = { version = "0.2", features = ["reference_protocol", "tls-self-signed"] }

# TCP with OpenSSL, without Rustls
nacelle = { version = "0.2", default-features = false, features = ["tcp", "openssl"] }
```

| Feature | Purpose |
| --- | --- |
| `tcp` | Custom TCP protocol transport over TCP and Unix sockets. Enabled by default. |
| `reference_protocol` | Optional length-delimited example protocol. |
| `error-hints` | Include actionable setup hints in `NacelleError` display output. |
| `http` | Hyper HTTP/1 server transport. |
| `tls` | Provider-neutral TLS capability. |
| `rustls` | Rustls-backed TLS for HTTP and TCP. |
| `openssl` | OpenSSL-backed TLS for TCP. |
| `openssl-vendored` | Build OpenSSL from source when native OpenSSL is unavailable. |
| `tls-self-signed` | Generate ephemeral Rustls self-signed certificates for local tests. |
| `otel` | OpenTelemetry metrics API integration through `NacelleTelemetry`. |
| `tokio-util` | Bridge `tokio_util::sync::CancellationToken` into Nacelle shutdown. |
| `tower` | Adapt `tower::Service<NacelleRequest>` into a Nacelle handler. |

OpenSSL builds need native OpenSSL development files unless you enable
`openssl-vendored`. Vendored OpenSSL also needs Perl on Windows.

## Workspace Layout

- `nacelle-core` contains shared request, response, body, resource limits,
    lifecycle, core telemetry, and TLS primitives.
- `nacelle-tcp` contains the TCP transport, protocol runtime, and TCP limits.
- `nacelle-http` contains the Hyper HTTP/1 transport, HTTP limits, and HTTP edge
    policy.
- `nacelle` is the convenience crate with re-exports and the reference protocol.
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