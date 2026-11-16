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

The client normally uses a separate Tokio worker pool. Its opt-in
`--current-thread` mode places the same connection tasks and I/O on the main
thread. Measure client CPU/request, RSS, and thread count as well as server
CPU and end-to-end throughput. A client-runtime gain on an unchanged server
does not establish higher server capacity. Keep the multi-thread client
default for multi-core coalescing load and hold its runtime fixed in
server-only comparisons.

The stress client's opt-in `--blocking-io` profile assigns a blocking I/O worker
to each persistent pipeline-1 connection. This changes socket wait/scheduling
costs without adding requests in flight or changing the negotiated TLS
connection. It also adds threads and per-worker allocator/runtime memory.
Treat any improvement as an end-to-end driver profile result, not server-code
capacity evidence; keep this option off unless the measured trade-off fits
the workload.

For the sequential TLS target, keep exactly one request outstanding on each
persistent connection. Compare TLS and plaintext using identical 256-byte
requests, responses, and immediate delivery. Warm up those same connections,
then report measured mean RTT and p50/p95/p99 with client/server CPU separately.
Current stress-client RTT includes request preparation and TLS write; older
after-write measurements omit that portion of the round trip.

Profile the complete lifecycle before changing buffering or synchronization.
Socket-read wall time includes peer and scheduler waits; overlapping client and
server phase durations cannot be added to reconstruct RTT. Allocation-counting
and phase-timing stress-tool features are diagnostic-only and default off.
Allocation counters observe Rust global-allocator calls, not native C internals.
Do not infer TLS mutex contention from a generic split symbol alone, or propose
a logging queue when formatting already runs outside request processing.
See [sequential TCP/TLS comparisons](../how-to/compare-performance.md#sequential-tcp-and-tls-round-trips).

The `request_permit_contended` benchmark compares owned and transport-scoped
borrowed permits on one shared runtime state with 1, 4, and 8 worker threads.
Each worker acquires/releases 1,024 permits per iteration. Thread startup is
excluded; barrier synchronization is included. No metrics recorder is installed.
This isolates accounting contention, not socket throughput.

## Successful-path ownership and dispatch

### Connection tracking costs

Tracking is disabled by default. Enabling it adds connection-time registry
allocation/locking and a cancellation waiter per connection. TCP uses atomic
activity transitions at input/idle boundaries. HTTP also waits for graceful
retirement but adds no request activity hooks; it cannot prove safe idle
eligibility. Only a selected retirement creates an administrative timer.
Tracking does not add per-request registry locks,
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

For HTTP, keep the unchanged `critical_paths` HTTP round-trip cases as the
tracking-off baseline and run the separate opt-in profiles:

```bash
cargo bench -p nacelle-examples --features bench,http --bench http_connection_lifecycle
NACELLE_HTTP_TRACKING=0 NACELLE_HTTP_MEMORY_CONNECTIONS=2048 cargo bench -p nacelle-examples --features bench,http --bench http_connection_lifecycle
NACELLE_HTTP_TRACKING=1 NACELLE_HTTP_MEMORY_CONNECTIONS=2048 cargo bench -p nacelle-examples --features bench,http --bench http_connection_lifecycle
NACELLE_HTTP_TRACKING=0 NACELLE_HTTP_LATENCY_REQUESTS=50000 cargo bench -p nacelle-examples --features bench,http --bench http_connection_lifecycle
NACELLE_HTTP_TRACKING=1 NACELLE_HTTP_LATENCY_REQUESTS=50000 cargo bench -p nacelle-examples --features bench,http --bench http_connection_lifecycle
```

`http_tracking` compares one and 64 sequential requests per loopback connection
using identical 32-byte fixed-length responses, a two-worker Tokio runtime, and
disabled metrics. It includes client parsing/allocation, network I/O, and
connection churn, not just registry work. `http_retirement` measures selection
through client-observed EOF and capacity release for established keep-alive
connections; setup and its warm-up request are excluded. Pending production,
flush, and deadline cancellation are deterministic tests, not latency claims.

The Linux memory mode holds warmed client/server pairs in one process. Its RSS
delta includes both endpoints, runtime tasks, buffers, registry, and allocator
retention, not isolated server allocations or kernel socket memory. Run each
mode in a fresh process with the same live count. The latency mode reports
client-observed p50/p99 on one warmed persistent connection; these distributions
are separate from Criterion batch means. Alternate tracking-off/on repetitions
without concurrent builds, keep CPU affinity and feature flags fixed, and do
not generalize these plain HTTP profiles to HTTPS or worker-local topology.

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

The default-off `compact-connections` feature reduces response-buffer storage
for immediate TCP/Unix responses. Connections start without output storage,
allocate for their first encoded frame, and retain small buffers across
requests. After a buffer exceeds `response_buffer_capacity`, draining it
releases the allocation without eagerly replacing it. Coalescing policies keep
their configured initial and replacement capacity.

This profile leaves input buffers, request-future placement, limits, deadlines,
backpressure, and TLS unchanged. It introduces no buffer pool or per-request
boxing. It targets memory use, not a guaranteed CPU or single-connection
throughput improvement. Compare warmed idle populations and active traffic
separately; unused connections have not yet exercised lazy output allocation.

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
storage. Request metrics use the generic telemetry `request_metrics` mode; the
default `counters-and-bytes` mode preserves lifecycle counters and byte
histograms, while `counters` omits byte writes and `off` suppresses both.
Diagnostics remain independent and opt-in, which avoids request `Instant` work
on core/HTTP paths unless request duration metrics or HTTP access logs are
enabled. Use `--request-metrics counters` when comparing the cost of byte
accounting. Use the
`telemetry_paths` benchmark for a no-recorder facade baseline; the stress server
intentionally installs its console recorder in every feature set.

With multiple stress-server workers, each listener owns a current-thread Tokio
runtime. A separate one-worker Tokio runtime supervises listeners and exports
metrics; increasing `--server-threads` does not create another equally sized
supervisor pool. Process affinity alone does not pin each listener thread to a
specific physical core.
Its plaintext shared and serial handler paths use borrowed TCP socket halves
rather than the generic split allocation and mutex. TLS retains the generic I/O
path; compare handler and transport modes separately.

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
