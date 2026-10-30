# Architecture

Nacelle is organized as a small core plus protocol-specific transport crates.

## Crate Layout

- `nacelle-core`: shared handler and admission contracts, request/response body,
  limits, lifecycle, telemetry, and negotiated TLS connection metadata.
- `nacelle-openssl`: OpenSSL configuration reload and negotiated metadata extraction.
- `nacelle-rustls`: Rustls configuration reload, certificate parsing, SNI policy, and negotiated metadata extraction.
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

Nacelle is organized so application behavior lives behind statically dispatched
handler boundaries. TCP handlers receive `TcpRequestContext<P, AppState>` and
complete requests with the response type associated with `P`, or explicitly
finish silently with `context.complete()`. Both TCP outcomes share one decoder,
handler, body-processing, and admission pipeline. Protocols map eligible
framework failures through `Protocol::error_response`; any returned typed
response uses the same encoder and bounded delivery path as handler responses.
HTTP handlers
receive `HttpRequestContext<ConnectionState, AppState>` and complete through
`HttpResponse`. Both state parameters default to `()`.

TCP `Protocol` implementations are adapters: they decode a wire format into
request metadata and encode responses back into frames. Swapping protocols
should not require rewriting the app core. The app-first serving path wires
concrete typed servers together with
`NacelleApp::new().tcp(...).http(...).run()`. Applications that need shared
dependencies start with `NacelleApp::with_state(...)`. The app owns one stable
`Arc<AppState>` shared by every registered listener, plus runtime state,
telemetry, shutdown, and supervision. Handlers receive only `&AppState` through
`RequestContext::app_state()`; Nacelle provides no mutable accessor or runtime
replacement for the whole dependency root. `nacelle::runtime::NacelleHost`
remains available for services that need manual listener control.

Reloadable configuration belongs behind the application root rather than in
Nacelle. A configuration service may use `ArcSwap` or another snapshot mechanism
internally. A handler should acquire one owned snapshot for a request and avoid
holding a reload guard across `.await`.

Per-connection TLS metadata lives in `nacelle-core`. Concrete configuration,
certificate handling, reload policy, and negotiated metadata extraction live
in `nacelle-rustls` and `nacelle-openssl`. Transport crates retain listener
lifecycle and async I/O adaptation so provider crates do not depend back on TCP
or HTTP. The `nacelle` facade exposes mutually exclusive `rustls` and `openssl`
features plus the Rustls-based `tls-self-signed` helper. The backend is fixed at
compile time; there is no runtime provider abstraction or selection path. The
workspace defaults to its Rustls members, while validation runs OpenSSL through
explicit package and feature lanes.

## Request Flow

```text
listener
  -> connection limit
  -> connection task
  -> protocol/HTTP decode
  -> request limit
  -> optional typed admission
  -> handler
  -> response body encode/stream
```

TCP and Unix socket listeners use the `nacelle-tcp` `Protocol` trait and its
associated request, response, and connection-state types to decode request
heads and encode bounded response frames. HTTP uses `nacelle-http` with Hyper
HTTP/1 and a transport-owned typed request/response pipeline.

Connection metadata carries the transport, a stable connection id, listener
label, peer and local addresses, local Unix socket path, effective peer IP, and
TLS metadata. The typed TCP pipeline presents this immutable metadata to
handlers as `ConnectionInfo` through `RequestContext`.

The pipeline model for connection-local application state is
`ConnectionContext<State>`. TCP protocols construct `Protocol::ConnectionState`
once per accepted connection. `TcpServer` and `LocalTcpServer` expose shared
state as `Arc<P::ConnectionState>`. `SerialTcpServer` and
`LocalSerialTcpServer` instead lend `&mut ConnectionContext<P::ConnectionState>`
to exactly one awaited handler at a time, avoiding an async mutex for state
confined to one serial connection loop. No dynamic extension map participates
in either request path.

The shared multi-thread Tokio runtime remains the default. Experimental
thread-per-core execution requires `experimental-thread-per-core` and currently
supports TCP, HTTP, Rustls TCP/HTTPS, required OpenSSL TCP, and optional
plaintext/OpenSSL TCP on Linux. Each selected worker owns a current-thread Tokio
runtime, `LocalSet`, reuse-port listener, protocol, and `LocalHandler` pipeline.
Accepted streams, handshakes, and connection tasks remain on the accepting
worker. Unsupported platforms fail configuration; Nacelle does not silently
switch runtime topology.

Serial mutable-state listeners support plain TCP, required OpenSSL, optional
OpenSSL detection, and Unix sockets in the shared runtime. Worker-local serial
listeners support plain TCP, required OpenSSL, and optional OpenSSL detection.
Rustls serial and worker-local Unix socket serial variants are not exposed.
Optional plaintext/OpenSSL detection requires
`experimental-openssl-detection`; required OpenSSL remains available with the
ordinary `openssl` feature.

`ThreadPerCoreConfig::with_max_threads(...)` caps the selected worker set after
automatic or explicit selection and before any worker thread is created. It
does not configure the caller-owned shared Tokio runtime.

Thread-per-core resource accounting is selected statically at startup. Global
mode shares all existing counters. Worker mode partitions finite connection,
request, streaming, and per-peer capacities in configured worker order.
Worker factories execute once per worker. Process-wide client pools, backend
limits, and other external resource budgets must be shared explicitly when they
must not scale with worker count. `LocalTcpRuntimeConfig::with_state(...)` and
`LocalHttpRuntimeConfig::with_state(...)` share one `Arc<AppState>` across
workers while protocol, handler, and connection state remain worker-local.

HTTP-specific edge policy remains in `nacelle-http`: Host, method, URI/header
shape checks, per-peer request rate limits, access logging, and security header
injection. TCP keeps protocol semantics in the protocol implementation and
shared lifecycle/limit enforcement in core.

### Typed request admission

`Admission<Context>` returns an owned associated `Permit`, or a bounded
`AdmissionRejection`. `LocalAdmission` permits worker-local `!Send` state,
futures, and permits. Policies borrow the decoded head, connection metadata
and state, and the same typed application root as the handler. There is one
concrete policy, not a chain or a dynamically dispatched future.

TCP owns bounded head reads, prior-output delivery, and protocol error
responses. HTTP owns Hyper parser configuration, effective peer resolution,
status mapping, and the head-to-body transition. Core owns neither protocol's
scheduling. Existing handler traits remain the application interface; internal
service adapters carry admission and handling together through every server mode.

```mermaid
flowchart LR
    Head["Decoded head"] --> Cheap["Size, policy and runtime capacity checks"]
    Cheap --> Admit["One concrete admission future"]
    Admit -->|"Pending"| Wait["Finite waiter count and deadline; no body work"]
    Wait -->|"Allow"| Work["Body resolution and handler"]
    Admit -->|"Ready allow"| Work
    Admit -->|"Reject"| Reject["Transport error response; close unread body"]
    Wait -->|"Deadline or reject"| Reject
    Work --> Produce["Response production owns permit"]
    Produce --> Release["Drop application permit"]
    Release -.-> Deliver["Transport output and flush may remain"]
```

Only a pending policy starts an admission timer. TCP also flushes prior output
before awaiting that same pinned future, including buffers inside `AsyncWrite`.
Ready policies do not force a flush. HTTP creates no body bridge before
allowance and does not trigger `100 Continue` by polling an unadmitted body.
Hyper may already have read body bytes with the head; this is not TCP's H/B
read allowance.

The application permit covers body resolution, handler work, and response
production, not remote receipt. HTTP cancels or completes its body pump before
transferring the permit into its concrete response body. TCP releases it after
encoding, before any completion-callback flush. Error, cancellation, and drop
release owned capacity. Runtime request gauges and response-byte metrics keep
their separate existing completion boundaries.

## Runtime State

`NacelleRuntimeState` owns shared budgets and counters. Connection, request, and
streaming-task limits are non-blocking atomic bounded counters.

`NacelleLimits` stores startup-only configuration. `NacelleRuntimeControl`
publishes total connection and per-peer connection-open rate thresholds through
atomics on those same states. Default request handling does not read the live
connection fields. Administrative updates neither reset counters nor revoke
permits; rate-table initialization is lazy, bounded, and completed before enable.
Optional per-peer concurrency retains its sharded locks.

App, host, and direct-server handles remain attached to installed state.
Replacing shared builder state is a configuration error. Global worker
accounting shares one handle; partitioned accounting exposes aggregate totals
and deterministic shares, including zero. Fan-out is administrative, not an
instantaneous cross-worker cutover. Startup rate-table capacity is divided
before first enable rather than allocated independently from the process
budget on each worker. See [runtime control](runtime-limits.md#live-connection-control)
for update and installation contracts.

Optional TCP/Unix/HTTP connection tracking adds a bounded core registry of metadata
and administrative signals. The transport owns setup, activity classification, and
I/O teardown; existing listener `JoinSet`s still own tasks. The registry never
owns sockets or worker-local state. Partitioned states share duplicate-ID
detection while enforcing worker-local capacities and reclaim targets.
TCP arbitrates input against idle reclaim. HTTP cannot prove live parser/output
idleness and reports idle reclaim as unsupported; its explicit graceful
retirement delegates to Hyper with a registration-owned cancellation deadline.
The driver selects tracked/untracked activity outside the request loop; registry
locks occur at registration, removal, and administration, not request dispatch.
See [tracked connections](runtime-limits.md#tracked-connections).

## Bodies

`NacelleBody` has three internal shapes:

- empty/single chunk for fast small responses
- buffered chunks for decoded TCP bodies already in memory
- streaming channel for request/response bodies that move asynchronously

TCP protocols can override `Protocol::max_request_body_bytes(...)` to choose a
phase-aware body limit from the decoded head, immutable connection metadata,
and concrete connection state immediately after head decoding and before
additional body reads.

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

Nacelle emits through the backend-neutral `metrics` facade and does not own an
exporter. `NacelleTelemetry` owns lifecycle, request, phase, error, and byte
metrics for all transports. Transports that can
provide extra low-cardinality detail attach a `NacelleMetricsContext` with
listener, protocol, transport, and TLS labels. Applications must install their
recorder before constructing these values so cached handles bind to it; without
a recorder the handles are no-ops.

`NacelleTelemetryConfig` separates connection, request, runtime, error, and TCP
phase-duration metric domains. All domains except phase durations are enabled by
default for compatibility. Request sub-switches live under `request_metrics`:
started/completed counters and byte counters are on by default; in-flight
counters and duration histograms are disabled by default. `with_metrics(false)`
gates every domain without replacing those individual settings, so re-enabling
metrics restores the configured domain policy. Telemetry observers are
independent and continue receiving events while metrics are disabled.

TCP phase histograms additionally require the non-default `phase-timing` Cargo
feature. Enable them deliberately with `NacelleTelemetry::default()` builder
methods on the server or app when you need diagnostic detail and can afford the
extra timers and metric writes. Without `phase-timing`, TCP phase timer storage
and `Instant` calls are not compiled. Core/HTTP request paths do not start a
request timer unless duration metrics or HTTP access logging are enabled.
