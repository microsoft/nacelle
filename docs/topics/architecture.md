# Architecture

Nacelle is organized as a small core plus protocol-specific transport crates.

## Crate Layout

- `nacelle-core`: shared handler, request/response body, limits, lifecycle, telemetry, and TLS primitives.
- `nacelle-tcp`: TCP/Unix socket server, protocol trait, connection loop, and listener runtime.
- `nacelle-http`: Hyper HTTP/1 server, HTTP request policy, and HTTP TLS listener integration.
- `nacelle`: convenience crate with `core`, `codec`, `tcp`, `http`, and
  `runtime` capability namespaces.
- `examples/nacelle-reference-protocol`: unpublished length-delimited protocol
  fixture used by examples, tests, benchmarks, and stress tools.
- `examples/nacelle-examples`: unpublished runnable examples and benchmarks.

The reference protocol intentionally stays out of `nacelle-core` and
`nacelle-tcp`. It demonstrates the public protocol and codec contracts without
becoming part of the published library API.

## App Core And Protocol Adapters

Nacelle is organized so application behavior lives behind the `Handler`
boundary. A handler receives a transport-neutral `NacelleRequest` and returns a
`NacelleResponse`.

TCP `Protocol` implementations are adapters: they decode a wire format into
request metadata and encode responses back into frames. Swapping protocols
should not require rewriting the app core. The app-first serving path wires
those pieces together with `NacelleApp`, `NacelleProtocols`, and
`NacelleApp::serve(...)`; lower-level `TcpServer` and `NacelleHost` APIs remain
available for services that need direct listener control.

TLS lives in `nacelle-core` because the configuration and provider metadata are
shared. `tls` is provider-neutral. `rustls` enables the Rustls provider used by
HTTP and TCP. `openssl` enables the OpenSSL provider for TCP without
selecting Rustls. Both providers feed `NacelleTlsProvider` and per-connection
TLS metadata.

## Request Flow

```text
listener
  -> connection limit
  -> connection task
  -> protocol/HTTP decode
  -> request limit
  -> handler
  -> response body encode/stream
```

TCP and Unix socket listeners use the `nacelle-tcp` `Protocol` trait and its
associated `Request` type to
decode request heads and encode response frames. HTTP uses `nacelle-http` with
Hyper HTTP/1 and maps requests into the same `NacelleRequest` /
`NacelleResponse` shape.

`NacelleRequest::connection` carries transport, a stable connection id, peer
address, local address, local Unix socket path, effective peer IP, TLS metadata,
and an optional typed extension. Raw protocol servers can populate that
extension with `connection_extension_factory(...)` for auth/session state
derived at accept or handshake time. Apps using `serve(protocols, app)` can set
the same extension factory on `NacelleApp`.

HTTP-specific edge policy remains in `nacelle-http`: Host, method, URI/header
shape checks, per-peer request rate limits, access logging, and security header
injection. TCP keeps protocol semantics in the protocol implementation and
shared lifecycle/limit enforcement in core.

## Runtime State

`NacelleRuntimeState` owns shared budgets and counters. Connection, request, and
streaming-task limits are non-blocking atomic bounded counters. Memory uses a
checked allocation guard that releases on drop.

This keeps the common request path allocation-light while still enforcing
bounded defaults.

## Bodies

`NacelleBody` has three internal shapes:

- empty/single chunk for fast small responses
- buffered chunks for decoded TCP bodies already in memory
- streaming channel for request/response bodies that move asynchronously

TCP large request bodies reserve their declared length while streaming. HTTP
request bodies reserve `Content-Length` when Hyper exposes a bounded size hint.
TCP protocols can override `RequestMetadata::max_body_bytes(...)` to choose a
phase-aware body limit immediately after head decoding and before body buffering
or streaming begins.

## Shutdown

Listeners own a `JoinSet` of accepted connection tasks. Shutdown proceeds in
stages:

1. signal shutdown
2. stop accepting
3. drain active connection tasks
4. abort remaining tasks after the drain deadline
5. emit shutdown telemetry

Task tracking is at the connection boundary, not the per-request hot path.

## Observability

Telemetry is deliberately low-cardinality. Reasons are static strings such as
`connections`, `request_body_bytes`, or `http_body_read`.

With `otel`, runtime gauges are observable instruments backed by runtime-state
atomics, so collection reads current values without per-request metric writes.
`NacelleTelemetry` owns lifecycle, request, phase, error, and byte metrics for
all transports. Transports that can provide extra low-cardinality detail attach
a `NacelleMetricsContext` with listener, protocol, transport, and TLS labels.

Request metric switches live under `NacelleTelemetryConfig::request_metrics`.
Started/completed counters and byte counters are on by default; in-flight
counters, duration histograms, and phase histograms are disabled by default.
Enable them deliberately with `NacelleTelemetry::default()` builder methods on
the server or app when you need diagnostic detail and can afford the extra
per-request metric writes. Core/HTTP request paths do not start a request timer
unless duration metrics or HTTP access logging are enabled.
