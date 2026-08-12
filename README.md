# Nacelle

Nacelle is an experimental Tokio-based Rust library for building typed streaming
services across TCP, Unix sockets, HTTP/1, and TLS-enabled listeners.

```rust
context.respond(transport_response).await
```

Each transport owns its request, response, and completion types. Handlers receive
a typed context with a streaming `NacelleBody`, connection metadata, and concrete
application and connection state, so services can process chunks without forcing
full buffering.

## Status

Nacelle is currently `0.3.x`. It is ready for experiments and prototype
integrations, but the public API is still allowed to change before `1.0`.

The typed pipeline contracts, runtime limits, host/app builders, and telemetry
observer contract are the most stable parts of the API. Transport metadata and
listener options are retained as non-exhaustive APIs. Stress-tool configuration,
experimental OpenSSL detection, experimental memory accounting, and
experimental thread-per-core execution are still moving.

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
use nacelle::core::{NacelleError, NacelleTelemetry};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle::NacelleApp;
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

    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler)
        .build()?;
    let addr = "127.0.0.1:8080".parse().map_err(NacelleError::protocol)?;

    NacelleApp::with_telemetry(NacelleTelemetry::default())
        .with_ctrl_c_shutdown()
        .tcp("echo", addr, server)
        .run()
        .await
}
```

Applications with shared dependencies use `NacelleApp::with_state(...)` or
`NacelleApp::with_state_and_telemetry(...)` and declare that state as the final
request-context type parameter. Handlers borrow it with `context.app_state()`.
Nacelle keeps one internal `Arc` allocation and shares it across every listener;
it does not expose mutable access or replace the dependency root at runtime.
Reloadable configuration can live behind that root and return one owned snapshot
per request before the handler awaits.

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
cargo run -p nacelle-examples --no-default-features --features http,experimental-memory --bin memory_guard

# TCP memory budget guard demo with the reference protocol
cargo run -p nacelle-examples --features experimental-memory --bin tcp_memory_guard

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
- One typed application dependency root shared across registered listeners.
- Streaming request and response bodies.
- Custom TCP protocol support over TCP and Unix domain sockets.
- Serial plain TCP, OpenSSL, experimental optional OpenSSL, and Unix socket handlers with
    exclusive mutable connection state and no async mutex on the connection
    path.
- Explicit bounded TCP response coalescing for already-buffered request bursts;
    immediate delivery remains the default.
- HTTP/1 serving through Hyper.
- Rustls TLS for HTTP and TCP.
- OpenSSL TLS for TCP, with experimental plain/TLS detection on one listener.
- Shared runtime limits, backpressure, graceful shutdown, and telemetry hooks.
- A stress server and stress client for local performance validation.

## Feature Flags

Choose the smallest feature set that matches the transports you actually run:

```toml
# TCP with a custom protocol (enabled by default)
nacelle = { version = "0.3" }

# HTTP only
nacelle = { version = "0.3", default-features = false, features = ["http"] }

# TCP + HTTP; backend-neutral metrics are always available
nacelle = { version = "0.3", features = ["http"] }

# TCP diagnostic phase histograms; still requires runtime activation
nacelle = { version = "0.3", features = ["phase-timing"] }

# Experimental runtime memory accounting and admission
nacelle = { version = "0.3", features = ["experimental-memory"] }

# Experimental Linux thread-per-core runtime with TCP
nacelle = { version = "0.3", features = ["tcp", "experimental-thread-per-core"] }

# Experimental plaintext/OpenSSL detection; implies TCP and OpenSSL
nacelle = { version = "0.3", default-features = false, features = ["experimental-openssl-detection"] }

# Expose structured setup hints through NacelleError::hint()
nacelle = { version = "0.3", features = ["error-hints"] }

# Local self-signed TLS for tests
nacelle = { version = "0.3", features = ["tls-self-signed"] }

# TCP with OpenSSL, without Rustls
nacelle = { version = "0.3", default-features = false, features = ["tcp", "openssl"] }
```

| Feature | Purpose |
| --- | --- |
| `tcp` | Custom TCP protocol transport over TCP and Unix sockets. Enabled by default. |
| `error-hints` | Expose actionable operator guidance through `NacelleError::hint()` without changing `Display`. |
| `http` | Hyper HTTP/1 server transport. |
| `rustls` | Rustls-backed TLS for HTTP and TCP. |
| `openssl` | OpenSSL-backed TLS for TCP. |
| `openssl-vendored` | Build OpenSSL from source when native OpenSSL is unavailable. |
| `tls-self-signed` | Generate ephemeral Rustls self-signed certificates for local tests. |
| `phase-timing` | Compile TCP read, decode, handler, encode, and write phase timers. Disabled by default. |
| `experimental-memory` | Compile runtime memory accounting, admission, ownership tracking, and related telemetry. Disabled by default. |
| `experimental-thread-per-core` | Compile the explicit Linux thread-per-core runtime and worker-local listener APIs. Disabled by default. |
| `experimental-openssl-detection` | Compile plaintext/OpenSSL detection and mixed-mode listener APIs. Implies `tcp` and `openssl`; disabled by default. |

Features prefixed with `experimental-` are use-at-your-own-risk APIs outside
the supported `0.3` contract. They remain opt-in and may change or be removed in
a future minor release. `phase-timing` and `error-hints` are supported opt-ins;
the exact text returned by `NacelleError::hint()` is advisory and must not be
parsed or used as a programmatic error code.

Match resource and timeout failures through `NacelleResourceLimitReason` and
`NacelleTimeoutReason`, not through `Display` or hint text. Their `as_str()`
methods expose the stable low-cardinality labels used by Nacelle telemetry.
Applications can use the explicit `Other(&'static str)` variants for their own
static, bounded reason vocabulary.

`rustls` and `openssl` are mutually exclusive compile-time choices. Select
exactly one when TLS is required; Nacelle has no runtime provider selector.
HTTP TLS requires `rustls`, while TCP supports either backend.
Root-level Cargo commands use the Rustls workspace lane by default; validate
OpenSSL explicitly with `-p nacelle-openssl` or the `openssl` facade feature.

Nacelle emits metrics through the [`metrics`](https://crates.io/crates/metrics)
facade and does not select an exporter. Install the recorder chosen by your
application before constructing Nacelle runtime state, telemetry, or servers so
their cached metric handles bind to that recorder. Without a recorder, those
handles are inexpensive no-ops. Request-duration and TCP phase histograms remain
runtime opt-ins because they add timers to request and transport paths.
Applications can disable all Nacelle metric emission locally with
`NacelleTelemetry::default().with_metrics(false)` without replacing the process
recorder or disabling telemetry observers. Connection, request, runtime, error,
and phase-duration domains also have independent configuration switches.

OpenSSL builds need native OpenSSL development files unless you enable
`openssl-vendored`. Vendored OpenSSL also needs Perl on Windows.

## Workspace Layout

- `nacelle-core` contains shared typed pipeline, body, resource limit,
    lifecycle, telemetry, and negotiated TLS connection metadata.
- `nacelle-openssl` contains reloadable OpenSSL configuration and negotiated
    connection metadata extraction.
- `nacelle-rustls` contains reloadable Rustls configuration, certificate
    parsing, SNI policy, self-signed test support, and connection metadata extraction.
- `nacelle-tcp` contains the TCP transport, protocol runtime, and TCP limits.
- `nacelle-http` contains the Hyper HTTP/1 transport, HTTP limits, and HTTP edge
    policy.
- `nacelle` is the convenience crate with `core`, `codec`, `tcp`, `http`,
    `openssl`, `rustls`, and `runtime` capability namespaces.
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

Nacelle does not compile runtime memory accounting by default. Enable the
non-default `experimental-memory` feature and set
`NacelleLimits::with_max_memory_bytes(...)` only for a measured deployment or
test profile. Keep a process or container limit as the hard memory boundary.

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

The root [config.toml](https://github.com/microsoft/nacelle/blob/main/config.toml) is loaded automatically when the stress server
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
./scripts/validate-all.sh
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

- [Getting started](https://github.com/microsoft/nacelle/blob/main/docs/tutorials/getting-started.md)
- [Architecture](https://github.com/microsoft/nacelle/blob/main/docs/topics/architecture.md)
- [Runtime limits and backpressure](https://github.com/microsoft/nacelle/blob/main/docs/topics/runtime-limits.md)
- [Operations](https://github.com/microsoft/nacelle/blob/main/docs/topics/operations.md)
- [Production configuration](https://github.com/microsoft/nacelle/blob/main/docs/how-to/configure-production.md)
- [HTTP hardening](https://github.com/microsoft/nacelle/blob/main/docs/how-to/harden-http.md)
- [Stress testing](https://github.com/microsoft/nacelle/blob/main/docs/how-to/run-stress-tests.md)
- [Performance tuning](https://github.com/microsoft/nacelle/blob/main/docs/how-to/compare-performance.md)
- [Security scanning](https://github.com/microsoft/nacelle/blob/main/docs/how-to/security-scanning.md)
- [Reference protocol](https://github.com/microsoft/nacelle/blob/main/docs/reference/protocol.md)
- [API stability](https://github.com/microsoft/nacelle/blob/main/docs/reference/api-stability.md)
- [Rust API reference](https://github.com/microsoft/nacelle/blob/main/docs/reference/rust-api.md)

Build the mdBook site:

```bash
mdbook build
```

Generate Rust API docs:

```bash
cargo doc -p nacelle --no-default-features --features buffer-rotation,error-hints,experimental-memory,experimental-thread-per-core,http,phase-timing,rustls,tcp,tls-self-signed --no-deps
cargo doc -p nacelle-openssl --all-features --no-deps
```

## Contributing

Open issues and pull requests in the
[Nacelle repository](https://github.com/microsoft/nacelle). Follow the
[Code of Conduct](https://github.com/microsoft/nacelle/blob/main/CODE_OF_CONDUCT.md)
when participating.

## License

This project is licensed under the
[MIT License](https://github.com/microsoft/nacelle/blob/main/LICENSE).

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.