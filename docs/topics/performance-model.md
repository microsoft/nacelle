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
acquire/drop overhead. Watch it closely after changes to `NacelleRuntimeState`.

## Concurrent requests without pipelining

Use pipeline depth 1 when evaluating request/response services. A connection
then waits for each response before sending another request; additional
connections provide concurrency without batching requests on one socket.
Keep a pipelined case as a separate control, not as a substitute for this workload.

Measure server CPU per completed request alongside throughput and p50/p99
latency. Separate user CPU from kernel CPU: small loopback messages can spend
most server CPU in socket I/O, hiding improvements in the Rust request path.
Use disjoint physical cores for server and client and record their NUMA placement.
Compare same-node and cross-node single-connection placement separately, keeping
worker counts and CPU masks identical between binaries. A node-local process mask
still permits worker migration and SMT sharing; it is not individual-core pinning.
A client-bound run cannot establish server capacity.

For the one-connection case, use one persistent socket, pipeline depth 1, one
listener, and one client worker. Immediate delivery prevents batching from
confounding this measurement. The stress fixture's serial handler lends
connection state rather than cloning a shared context per request, and plaintext
serial I/O uses specialized borrowed TCP halves. An explicit one-worker
`--worker-cpus` list selects a dedicated current-thread listener; omitting it
uses the supervision runtime: current-thread for one unpinned immediate listener,
and one-worker multi-thread for coalescing or dedicated listeners. These are
distinct configurations, not interchangeable baselines.

The `request_permit_contended` benchmark compares owned and transport-scoped
borrowed permits on one shared runtime state with 1, 4, and 8 worker threads.
Each worker acquires/releases 1,024 permits per iteration. Thread startup is
excluded; barrier synchronization is included. No metrics recorder is installed.
This isolates accounting contention, not socket throughput.

## Successful-path ownership and dispatch

### Connection tracking costs

Tracking is disabled by default. Enabling it adds connection-time registry
allocation/locking, one cancellation waiter per connection, and atomic activity
transitions at input/idle boundaries. It does not add per-request registry locks,
timers, or metrics. Bounded registry map storage retains its high-water
allocation; TLS metadata, snapshots retained by callers, sockets, and transport
buffers must be budgeted separately.

Run the explicit lifecycle profiles:

```bash
cargo bench -p nacelle-examples --features bench,tcp --bench connection_lifecycle
```

`registry_churn` compares registration/drop with tracking off/on.
`tracking_tcp` compares one and 64 requests per direct-I/O connection, with
immediate delivery and metrics disabled. These are in-memory driver cases,
not network throughput. The 256-entry idle/reclaim profiles isolate registry
administration; the four-worker profile also includes channel dispatch/replies,
but excludes worker thread startup. Registry-only entries contain no TCP tasks.

On Linux, the same benchmark binary accepts
`NACELLE_TRACKING_MEMORY_ENTRIES=100000`. Run that mode in a fresh process.
It touches guard storage before observing RSS, registers plain metadata without
peer/TLS strings, then observes live and retired RSS. The difference estimates
registry/signal/allocator bytes per live entry, not total connection memory or
exact allocated bytes. Page rounding and allocator retention affect the result.
Keep allocator, entry count, toolchain, and platform fixed between comparisons.

Compare tracking-off network throughput with the retained baseline and frozen
stress client as well; the absence of registry operations does not prove that
future layout or code generation has no cost.

### Request ownership

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
- Nacelle's handler, TCP I/O, and HTTP body-read timeout wrappers use concrete
  futures. Ready operations do not construct a Tokio timer. Pending operations
  retain the deadline computed
  before their first poll, including partial I/O; cooperative budget exhaustion
  cannot suppress the timeout. The HTTP response
	write deadline retains a boxed `Sleep` only after connection-level
	backpressure because Hyper requires its I/O wrapper to remain `Unpin`.
- Request and body-pump permits borrow the runtime state already owned by their
  connection or request future. This avoids shared reference-count updates per
  operation while retaining exact global limits, active counts, gauge updates,
  and release on cancellation. Public owned permits remain available for work
  that must outlive a borrow.
- The scoped HTTP request-body pump borrows runtime state and telemetry and
  moves received chunks into the body channel. HTTP response bodies retain the
  immutable response-byte limit rather than another runtime-state handle.
- Enabled per-peer request and connection-open rate limits use fixed-capacity,
  lock-free tables. Admission probes a bounded number of atomic slots and
  rejects a newly observed peer when the configured table is full; it does not
  take a per-request mutex or sweep every tracked peer.
- Type-erased protocol/handler errors are constructed only on error paths.
- TCP response coalescing is opt-in. It queues only complete bounded frames and
	restores socket backpressure at thresholds, streaming waits, and socket-read
	boundaries.
- App listener installers and worker thread closures erase startup-only closure
	types; they are not involved in request dispatch.
- Optional tracing, Hyper, Tokio, TLS providers, allocators, and metrics
	recorders retain their own external indirection.

TCP computes effective telemetry modes once per connection and constructs one
`NacelleMetricsContext` containing cached facade handles. Without an installed
recorder, metric writes through those handles are no-ops. Connection/request
permits update cached runtime gauges at their existing state transitions.
Diagnostic TCP phase timers are compiled only with
the non-default `phase-timing` feature and remain runtime-disabled until
explicitly enabled on `NacelleTelemetry`.

Enable the `buffer-rotation` feature for long-lived TCP connections that may
occasionally receive large requests. Once an oversized cumulative input buffer
is empty, Nacelle replaces it with a buffer sized to `read_buffer_capacity`.
Leave the feature disabled when retaining peak buffer capacity is preferable to
allocating again after traffic spikes.

Suggested RPS comparison:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --server-threads 48 --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

The stress server installs a console recorder backed by `metrics-util` and prints
a snapshot every 5 seconds. It aggregates request/response body-size samples
into fixed, cache-line-isolated byte-total shards across labels rather than
retaining raw byte samples. Cached histogram handles select one of 64 shards
at registration, so recording adds no thread-local lookup, mutex, or allocation.
Zero-byte contributions skip the atomic update. The console drains and sums
the shards off the connection path. Snapshots have approximate window boundaries;
successive drains conserve totals unless their `u64` sum saturates.
Library histogram names and types are unchanged. Other histograms,
including opt-in phase diagnostics, retain the debugging recorder's sample
storage. Request metrics are grouped under the generic telemetry
`request_metrics` config; started/completed counters and byte counters are on by
default, while in-flight and duration metrics remain opt-in. Request duration
metrics are opt-in as well, which avoids request `Instant` work on core/HTTP
paths unless duration metrics or HTTP access logs are enabled. Use
`--no-byte-metrics` when comparing the cost of byte accounting. Use the
`telemetry_paths` benchmark for a no-recorder facade baseline; the stress server
intentionally installs its console recorder in every feature set.

With multiple stress-server workers, each listener owns a current-thread Tokio
runtime. A separate one-worker Tokio runtime supervises listeners and exports
metrics; increasing `--server-threads` does not create another equally sized
supervisor pool. Process affinity alone does not pin each listener thread to a
specific physical core.
Its plaintext shared-handler path uses borrowed TCP socket halves rather than
the generic split allocation and mutex. Serial handlers and TLS retain their
generic I/O path; compare those modes separately.

The fixture's shared 64-byte success template is read-only static storage, so
its `Bytes` clones/drops do not update a shared reference count. Custom sizes
and explicit worker-local templates retain owned `Bytes`. This optimization
preserves the fixed payload and does not apply to dynamically generated
application responses.

The stress fixture's optional `--worker-local-response` allocates one immutable
response template per listener. This separates the template's reference-count
traffic between workers; it does not remove per-response ownership updates or
make transport buffers worker-local. Runtime admission counters, limits, live
controls, and rate tables remain process-wide. Optional Linux `--worker-cpus`
pins each listener before its template initialization. Both options preserve
the default shared-template behavior when omitted. Compare affinity alone and
worker-local ownership independently, and budget template replication and
per-worker allocator retention; first-touch placement is not a NUMA guarantee.
See [stress-test options](../how-to/run-stress-tests.md) for validation rules.

The checked-in root `config.toml` enables self-signed TCP TLS for local
stress runs. For the plain TCP throughput baseline, use
`examples/nacelle-stress-server/configs/tcp.toml`. Compare TLS and non-TLS runs
separately:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml
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
