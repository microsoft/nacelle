# Performance model

nacelle's high-throughput TCP path is sensitive to small per-request costs.
When comparing runs, keep these variables fixed:

- commit
- Linux kernel and CPU governor
- allocator configuration
- server threads
- connection count
- pipeline depth
- payload size
- TLS versus plain TCP
- stress client version

Use the performance how-to for repeatable command lines.

Do not use a repository-wide RPS number as a baseline. Compare commits on the
same host with the same kernel, governor, allocator, features, transport/TLS
mode, configuration, workload, and client revision.

Suggested local benchmark:

```bash
cargo bench -p nacelle-examples --features "bench tcp"
```

The `runtime_limits` benchmark group covers connection/request permit
acquire/drop and memory allocation overhead. Watch it closely after changes to
`NacelleRuntimeState`.

## Successful-path ownership and dispatch

The default successful TCP and HTTP pipelines use concrete protocol, handler,
responder, body, and telemetry observer types. Nacelle does not box handler
futures or dynamically dispatch those contracts.

Retained costs are scoped by ownership:

- TCP connection buffers and decoder state are created per connection and reused
	across requests. Oversized response frames and optional input-buffer rotation
	can replace those buffers deliberately.
- A non-empty HTTP request uses one bounded Tokio channel for the request-body
	bridge; an exact zero-length body skips the channel and producer work, while
	the small scoped future still completes immediately.
- HTTP response bodies remain concrete `StreamBody` values rather than boxed
	body trait objects.
- Request/handler/read/write timeouts use concrete futures. The HTTP response
	write deadline retains a boxed `Sleep` only after connection-level
	backpressure because Hyper requires its I/O wrapper to remain `Unpin`.
- Memory-wait queue allocation and its boxed timeout occur only under memory
  contention; the available-capacity path is atomic and allocation-free.
- Enabled per-peer request and connection-open rate limits use fixed-capacity,
  lock-free tables. Admission probes a bounded number of atomic slots and
  rejects a newly observed peer when the configured table is full; it does not
  take a per-request mutex or sweep every tracked peer.
- Type-erased protocol/handler errors are constructed only on error paths.
- TCP response coalescing is opt-in. It queues only complete bounded frames,
	retains overflow memory guards until flush, and restores socket backpressure
	at thresholds, streaming waits, and socket-read boundaries. Overflow grows
	geometrically through an old-plus-replacement memory-accounted transaction.
- App listener installers and worker thread closures erase startup-only closure
	types; they are not involved in request dispatch.
- Optional tracing, Hyper, Tokio, TLS providers, allocators, and OpenTelemetry
	retain their own external indirection.

TCP computes effective telemetry modes once per connection. When metrics are
disabled it skips `NacelleMetricsContext` and OTel attribute construction while
retaining connection/request permits, memory accounting, and configured limits.

Enable the `buffer-rotation` feature for long-lived TCP connections that may
occasionally receive large requests. Once an oversized cumulative input buffer
is empty, Nacelle replaces it with a buffer sized to `read_buffer_capacity`.
Leave the feature disabled when retaining peak buffer capacity is preferable to
allocating again after traffic spikes.

Suggested RPS comparison:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --server-threads 48 --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

The default stress server build also prints a compact OTel console snapshot every
5 seconds. Request metrics are grouped under the generic telemetry
`request_metrics` config; started/completed counters and byte counters are on by
default, while in-flight and duration metrics remain opt-in. Request duration
metrics are opt-in as well, which avoids request `Instant` work on core/HTTP
paths unless duration metrics or HTTP access logs are enabled. Use
`--no-byte-metrics` when comparing the cost of byte accounting, and use
`--no-default-features` with the plain TCP config for a metrics-free baseline.

The checked-in root `config.toml` enables self-signed TCP TLS for local
stress runs. For the plain TCP throughput baseline, use
`examples/nacelle-stress-server/configs/tcp.toml`. Compare TLS and non-TLS runs
separately:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp-low-memory.toml
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp-tls.toml
```

The `examples/run-stress-test.sh` and `examples/run-stress-test.ps1` helpers
apply root `config.toml` first, then the selected profile, and choose the
matching client mode automatically.

Guardrails:

- keep shutdown task tracking at the connection/listener boundary
- avoid per-request locks in the TCP hot path
- keep telemetry observers optional; default operation uses `NoopObserver`
- preserve single-chunk body fast paths
- tune TCP buffer sizes for the connection count instead of relying on large defaults
