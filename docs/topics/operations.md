# Operations model

## Deployment Shape

Recommended internet-facing shape:

```text
client -> proxy/load balancer/TLS -> Nacelle service
```

The proxy should own TLS, coarse connection filtering, and external idle
timeouts. Nacelle owns application limits, protocol handling, body limits, and
graceful shutdown.

## Startup

Use explicit limits and print the effective config for stress or benchmark
services. For production services, record:

- process version and git SHA
- configured limits
- listener addresses
- feature flags
- allocator settings

Thread-per-core mode requires `experimental-thread-per-core`, is experimental,
and is Linux-only. Select workers explicitly, record logical CPU ids and
affinity settings, and treat any bind, affinity, or worker initialization
failure as a whole-runtime startup failure. TCP, HTTP, Rustls TCP/HTTPS,
required OpenSSL TCP, and optional plaintext/OpenSSL TCP have worker-local
stacks; optional OpenSSL detection additionally requires
`experimental-openssl-detection`. Performance qualification remains under
implementation. Use `ThreadPerCoreConfig::with_max_threads(...)` to cap any
selected worker set; configure the caller-owned Tokio builder separately for
shared-runtime thread limits.

## Shutdown

Use `NacelleApp::with_ctrl_c_shutdown()` for the standard signal path, or pass a
shared `NacelleShutdown` through `NacelleApp::with_shutdown(...)`. Configure the
drain deadline with `with_shutdown_drain_timeout(...)`. Advanced manual hosts
can use `nacelle::runtime::NacelleHost::shutdown_and_wait_timeout(...)`. Short
deadlines protect deploy velocity but can abort in-flight work.

Expected shutdown telemetry:

- shutdown requested
- listener stopped accepting
- drain started
- drain completed or timed out
- active connections aborted

## Metrics To Watch

- `nacelle.connection.active`
- `nacelle.request.active`
- `nacelle.streaming_task.active`
- `nacelle.memory.usage`
- `nacelle.connection.accepted`
- `nacelle.connection.closed`
- `nacelle.request.started`
- `nacelle.request.completed`
- `nacelle.connection.rejected`
- `nacelle.request.rejected`
- `nacelle.request.timed_out`
- `nacelle.timeouts`
- `nacelle.request.failed`
- `nacelle.request.body.size`
- `nacelle.response.body.size`

Alerts should focus on sustained saturation, rising rejections, timeout spikes,
and memory approaching the configured budget.

## Benchmarking

Nacelle emits metrics through the `metrics` facade according to
`NacelleTelemetryConfig`. Connection, runtime, and error domains are on by
default. Request metrics are grouped under `request_metrics`: `started`,
`completed`, and `byte_counts` are on by default, while `in_flight` and
`duration_ms` are opt-in. TCP phase histograms require the non-default
`phase-timing` Cargo feature and explicit runtime activation.

Use `NacelleTelemetry::default().with_metrics(false)` to suppress all Nacelle
metric domains while retaining any application recorder. This global gate does
not erase individual domain settings, and telemetry observers remain active.
Use `with_connection_metrics`, `with_request_metrics`, `with_runtime_metrics`,
`with_error_metrics`, and `with_phase_duration_metrics` for independent policy.
A shared `NacelleRuntimeState` has one runtime-metric policy; configure servers
sharing that state consistently before serving traffic.

The stress server installs a debugging recorder and prints a compact console
snapshot every 5 seconds. Production applications should install their chosen
recorder before constructing Nacelle runtime state, telemetry, or servers. If no
recorder is installed, facade handles are no-ops.

Request duration metrics remain opt-in through `NacelleTelemetryConfig`. With
the default config, core/HTTP request paths avoid request timer work unless HTTP
access logging is enabled.

Compile and activate TCP phase timing only for a diagnostic build:

```toml
[dependencies]
nacelle = { version = "0.3.0-rc.1", features = ["phase-timing"] }
```

```rust
let telemetry = NacelleTelemetry::default()
	.with_phase_duration_metrics(true);
```

The `nacelle.phase.duration_ms` histogram uses a low-cardinality `phase` label:

| Phase | Boundary |
| --- | --- |
| `socket_read` | One completed transport read, including asynchronous wait but excluding decode. |
| `decode` | One protocol decoder invocation; a request may require more than one invocation. |
| `request_body_read` | Request-body assembly or remaining streaming-body drain. May include `socket_read` operations. |
| `handler` | The awaited application handler, including application body consumption and response construction. |
| `response_encode` | One synchronous protocol response-frame encoder invocation. |
| `socket_write` | One response write batch or explicit transport flush, including asynchronous wait. |

These are operation histograms, not a per-request trace. Do not add their
percentiles to infer round-trip latency: pipelining can decode several requests
from one read, streaming overlaps body reads with the handler, and response
coalescing can write several completed requests in one batch. Use
`nacelle.request.duration` for server request processing and client-side
latency for actual round-trip time.

The server cannot measure TCP handshake duration because the kernel completes
it before `accept()` returns. Connection accepted, active, and closed metrics
remain available; TLS handshake timing is not currently emitted as a phase.

Canonical metric names are resource-first. Instrument type is documented here
rather than embedded in the metric name:

| Metric | Type | Notes |
| --- | --- | --- |
| `nacelle.connection.active` | Gauge | Current active connections. Listener-labeled series provide transport-level detail; unlabeled series represent runtime permit usage. |
| `nacelle.request.active` | Gauge | Current active requests. Protocol-labeled series provide request-level detail; unlabeled series represent runtime permit usage. |
| `nacelle.streaming_task.active` | Gauge | Current runtime streaming body tasks. |
| `nacelle.memory.usage` | Gauge (`By`) | Current bytes allocated by runtime memory accounting; emitted only with `experimental-memory`. |
| `nacelle.connection.accepted` | Counter | Accepted connections, labeled by listener/transport/TLS where available. |
| `nacelle.connection.closed` | Counter | Closed connections, labeled with close reason where available. |
| `nacelle.connection.rejected` | Counter | Connections rejected before acceptance. |
| `nacelle.request.started` | Counter | Requests started. |
| `nacelle.request.completed` | Counter | Requests completed, labeled by status where available. |
| `nacelle.request.rejected` | Counter | Requests rejected before handler execution. |
| `nacelle.request.timed_out` | Counter | Request failures caused by a timeout, labeled by operation. |
| `nacelle.request.failed` | Counter | Requests failed before normal completion. |
| `nacelle.request.body.size` | Histogram (`By`) | Request body size per completed request. |
| `nacelle.response.body.size` | Histogram (`By`) | Response body size per completed response. |
| `nacelle.request.duration` | Histogram (`s`) | Request duration, opt-in. |
| `nacelle.phase.duration_ms` | Histogram | TCP operation duration; requires compile-time and runtime opt-in. |

Run microbenchmarks before and after hot-path changes:

```bash
cargo bench -p nacelle-examples --features "bench tcp experimental-memory"
```
