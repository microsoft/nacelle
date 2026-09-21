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

- `server.runtime.workers`
- `server.connection.active`
- `server.request.active`
- `server.streaming_task.active`
- `server.memory.usage`
- `server.connection.accepted`
- `server.connection.closed`
- `server.request.started`
- `server.request.completed`
- `server.connection.rejected`
- `server.request.rejected`
- `server.request.timed_out`
- `server.timeouts`
- `server.request.failed`
- `server.request.body.size`
- `server.response.body.size`

Alerts should focus on sustained saturation, rising rejections, timeout spikes,
and memory approaching the configured budget.

## Runtime Topology

Nacelle parallelizes connection handling by spawning onto the ambient Tokio
runtime, so a listener started on a current-thread runtime is confined to a
single core regardless of how many cores the host has. The first listener to
start — on any transport — reports the topology it observed:

```text
INFO nacelle: listener started transport="http" runtime="multi_thread" workers=12
```

A single-worker topology additionally emits a warning, and the worker count is
published as the `server.runtime.workers` gauge. The gauge belongs to the
runtime metric domain, so `with_metrics(false)` or `with_runtime_metrics(false)`
suppresses it; the log lines are not metrics and are always emitted.

Thread-per-core hosts give each worker its own current-thread runtime, so the
ambient handle reports one worker per runtime. `run_thread_per_core` declares
the process-wide worker count before starting workers, so these deployments
report `runtime="thread_per_core"` with the real worker count rather than a
false single-core warning. Hosts that build their own per-worker runtimes
should call `nacelle_core::runtime::declare_worker_topology` to get the same
behavior.

Alert on `server.runtime.workers == 1` where the process is expected to span
several cores; it is the clearest signal that the process is leaving throughput
on the table.

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
nacelle = { version = "0.3.0", features = ["phase-timing"] }
```

```rust
let telemetry = NacelleTelemetry::default()
	.with_phase_duration_metrics(true);
```

The `server.phase.duration_ms` histogram uses a low-cardinality `phase` label:

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
`server.request.duration` for server request processing and client-side
latency for actual round-trip time.

The server cannot measure TCP handshake duration because the kernel completes
it before `accept()` returns. Connection accepted, active, and closed metrics
remain available; TLS handshake timing is not currently emitted as a phase.

Canonical metric names are resource-first. Instrument type is documented here
rather than embedded in the metric name:

| Metric | Type | Notes |
| --- | --- | --- |
| `server.runtime.workers` | Gauge | Worker threads serving the process, reported once by the first listener to start. A value of `1` means throughput is capped to one core. Thread-per-core hosts report their declared process-wide worker count. |
| `server.connection.active` | Gauge | Current active connections. Listener-labeled series provide transport-level detail; unlabeled series represent runtime permit usage. |
| `server.request.active` | Gauge | Current active requests. Protocol-labeled series provide request-level detail; unlabeled series represent runtime permit usage. |
| `server.streaming_task.active` | Gauge | Current runtime streaming body tasks. |
| `server.memory.usage` | Gauge (`By`) | Current bytes allocated by runtime memory accounting; emitted only with `experimental-memory`. |
| `server.connection.accepted` | Counter | Accepted connections, labeled by listener/transport/TLS where available. |
| `server.connection.closed` | Counter | Closed connections, labeled with close reason where available. |
| `server.connection.rejected` | Counter | Connections rejected before acceptance. |
| `server.request.started` | Counter | Requests started. |
| `server.request.completed` | Counter | Requests completed, labeled by status where available. |
| `server.request.rejected` | Counter | Requests rejected before handler execution. |
| `server.request.timed_out` | Counter | Request failures caused by a timeout, labeled by operation. |
| `server.request.failed` | Counter | Requests failed before normal completion. |
| `server.request.body.size` | Histogram (`By`) | Request body size per completed request. |
| `server.response.body.size` | Histogram (`By`) | Response body size per completed response. |
| `server.request.duration` | Histogram (`s`) | Request duration, opt-in. |
| `server.phase.duration_ms` | Histogram | TCP operation duration; requires compile-time and runtime opt-in. |

Run microbenchmarks before and after hot-path changes:

```bash
cargo bench -p nacelle-examples --features "bench tcp experimental-memory"
```
