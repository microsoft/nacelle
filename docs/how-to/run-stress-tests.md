# Run stress tests

Run the server:

```bash
cargo run --release --package nacelle-stress-server -- --config examples/nacelle-stress-server/configs/tcp.toml
```

Run a bounded client smoke test:

```bash
cargo run --release --package nacelle-stress-test -- --connections 32 --pipeline 16 --duration-secs 15
```

The `examples/run-stress-test.sh` and `examples/run-stress-test.ps1` helpers
accept `--config`/`-Config` and pass `--tls-insecure` to the stress client only
when the effective `tls_self_signed` value is true.

The Bash helper builds both binaries before starting its listener-readiness
wait. It rejects an occupied listen port, waits up to 10 seconds for its own
server to listen, and stops only the server/client processes it launched.
It does not terminate other stress servers by name.

The stress client enables its Rustls support by default so `--tls-insecure`
works with the local self-signed server. For a Rustls-free plain TCP build, run
both stress binaries with `--no-default-features` and use
`examples/nacelle-stress-server/configs/tcp.toml`.

Repeatable profiles:

- `examples/nacelle-stress-server/configs/tcp.toml`: plain TCP baseline.
- `examples/nacelle-stress-server/configs/tcp-tls.toml`: TCP wrapped in
  self-signed TLS.

Server TOML separates initial live thresholds from startup-only limits:

```toml
[connection_limits]
max_connections = 16384
# max_connection_opens_per_peer_per_second = 100

[limits]
connection_rate_limit_table_capacity = 16384
```

Connection totals and rates no longer belong in `[limits]`; old fields are
rejected rather than ignored. Zero rejects new admissions. An overlay can use
`unlimited_connections = true` or `disable_connection_open_rate = true` inside
`[connection_limits]` to clear an earlier finite value. A finite value and its
true clearing flag in the same file are an error; omitted or false flags leave
the prior value unchanged. These files configure startup thresholds, not a
remote administrative endpoint.

To measure opt-in bounded head reads, add this table to a copy of the plain TCP
profile; leave the baseline profile unchanged:

```toml
[head_read_limits]
max_head_bytes = 24
max_undecided_bytes = 4096
```

Both fields are required together, and invalid bounds reject the file before
applying its settings. Omitting the table preserves earlier configuration; code
defaults leave head-read limits disabled. The effective configuration prints the
selected limits. Report enabled-budget results separately from the default
baseline; `B` changes read-ahead and can change read counts. See
[bounded head reads](../reference/protocol.md#bounded-head-reads) for the
plaintext scope and body/EOF contracts.

Linux example:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --server-threads 48 --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

PowerShell example:

```powershell
.\examples\run-stress-test.ps1 -Config examples/nacelle-stress-server/configs/tcp.toml -ServerThreads 48 -Connections 256 -Pipeline 8 -DurationSecs 30 -PayloadBytes 256
```

The stress server installs a console recorder backed by `metrics-util` and prints
a compact metrics snapshot every 5 seconds. It enables request
started/completed counters plus request/response byte counters by default. The
generic telemetry API groups those switches under `request_metrics`; the stress
server exposes byte accounting as `byte_metrics = true`.
Request/response body-size histogram samples are aggregated into constant-space
byte totals across labels for this console; raw byte distributions are not
retained. Cached handles collect into 64 cache-line-isolated shards, and the
console drains them off the connection path. Window boundaries are approximate;
totals are conserved across successive snapshots unless they saturate.
This does not change the library's emitted metric names or types.
Use `--no-byte-metrics` for a lower-overhead recorder run. Use
`--no-default-features` with the plain TCP config for a system-allocator,
Rustls-free diagnostic; metrics collection remains active. Add
`--features mimalloc-allocator` when the baseline must keep mimalloc while
disabling TLS.

On Linux, multiple server workers each own a current-thread Tokio runtime.
One unpinned listener with immediate responses uses the main thread's
current-thread runtime for I/O, metrics, and supervision. Coalescing and dedicated
listener configurations retain a one-worker multi-thread supervision runtime.
Process affinity does not assign individual listener threads to fixed cores.
Plaintext shared and serial handlers use specialized borrowed TCP socket halves.
TLS keeps generic I/O. Keep handler and transport modes fixed in comparisons.

The client defaults to a multi-thread Tokio runtime, sized by
`TOKIO_WORKER_THREADS` when set. Its explicit `--current-thread` option runs
the same connection tasks and I/O on the main thread without another worker
pool. It does not change connection count, pipeline depth, duration, or TLS
verification. Compare it separately for a one-connection workload; keep the
multi-thread default when generating load across multiple cores.

The explicit `--blocking-io` client mode requires `--pipeline 1` and persistent
connections; it rejects connection churn. Each connection gets one blocking I/O
worker while the selected Tokio runtime coordinates the run. TCP/TLS setup is
unchanged, and each worker writes one request, flushes, then receives the complete
response before sending another. TLS uses the existing negotiated Rustls
connection and cipher; it does not reconnect or change verification.

This mode trades additional threads and allocator/runtime memory for different
socket wakeup costs. It does not busy-wait. The blocking pool is sized to the
explicit connection count so all workers can reach the phase barriers.
Cancellation shuts down the shared socket and wakes phase waits. Keep the ordinary
asynchronous mode as the default and compare CPU, RTT, RSS, and thread count
before selecting blocking I/O for a workload.

Use `--warmup-secs 5` to warm up the same established connections before
measuring. All warmup responses drain before the measured phase. The client
flushes `measurement_start` and `measurement_end` markers outside the request
loop; connect/handshake work, warmup, and final shutdown are excluded from its
request statistics. Server console windows include all served requests,
including warmup.

Mean RTT includes request frame preparation, write/flush, and receipt and
validation of the complete response. p50, p95, and p99 use every completion in
a bounded histogram with less than 0.4% upper-bound quantization error. The
histogram uses approximately 114 KiB per connection, independent of run duration.
Earlier client revisions timed from after the request write and sampled
percentiles; keep the same client revision in comparisons.

For allocation diagnostics, build either stress tool with
`--features allocation-counting`. It wraps that tool's unchanged global
allocator and counts successful allocation, zeroed-allocation, and reallocation
calls, requested bytes, and frees. Client deltas exclude warmup and shutdown;
server deltas use its periodic/final console windows. Counts are process-wide,
include console work, and have non-atomic snapshot boundaries. Native C
allocations are not counted. Keep these instrumented results separate from
CPU/latency baselines; the feature adds atomic writes to allocation operations
and is disabled by default.

The client's separate `phase-timing` feature reports request preparation/
write/flush and response read/validation wall times. Read time includes peer
wait; these are diagnostic timings, not isolated TLS or socket CPU costs.
Rustls's synchronous stream can drive TLS I/O while servicing a caller operation,
so blocking-mode phase labels follow those operations rather than TLS internals.
Both extra timing and allocation collection compile out of ordinary builds.

The fixture's default 64-byte success payload uses read-only static `Bytes`.
This avoids template reference-count updates and payload allocation without
replicating templates. Other response sizes keep owned storage. This is specific
to the fixture's fixed `0x5A` payload, not an optimization of application-generated
responses. Explicit worker-local mode keeps owned templates at every size.

For an explicit Linux listener-placement experiment, choose allowed logical CPU
IDs, preferably one per physical core:

```bash
cargo run --release -p nacelle-stress-server -- \
  --config examples/nacelle-stress-server/configs/tcp.toml \
  --server-threads 2 --worker-cpus 0,2 --worker-local-response
```

Replace `0,2` with CPUs in the process's affinity mask. `--worker-cpus` requires
one unique CPU per effective listener worker. Invalid lists, unavailable CPUs,
and affinity failures reject startup; explicit affinity requires Linux.
The supervisor and metrics threads retain the process mask.

`--worker-local-response` is off by default. It builds one response template and
handler per listener instead of sharing them across workers; all listeners still
share one runtime state, admission limits, live controls, and rate history.
Template initialization happens after optional worker pinning. Allocator page
reuse means this is not a guarantee of NUMA-local memory. Template storage scales
with `effective_workers * response_bytes`, with overflow rejected at startup;
worker-local allocator retention can also increase RSS. Measure CPU, throughput,
and memory together. Use `--worker-cpus` without `--worker-local-response` to
isolate affinity from template ownership. Neither option changes the protocol
or response-write policy.

TCP phase timing is excluded from the default build. Compile and activate it
only for a diagnostic run:

```bash
cargo run --release -p nacelle-stress-server --features phase-timing -- \
  --config examples/nacelle-stress-server/configs/tcp.toml \
  --phase-duration-metrics
```

The five-second metrics snapshot then prints count, mean, minimum, and maximum for
each observed phase. Use an external metrics backend for percentiles and longer
retention. The phase timers and histogram writes affect the measured hot path,
so do not compare this run directly with a phase-timing-free throughput
baseline.

The Tokio stress server default build includes `tls-self-signed` support. The
checked-in root `config.toml` enables `tls_self_signed = true`, so the local
stress client should use `--tls-insecure` with that default config. Use
`--no-default-features` with `examples/nacelle-stress-server/configs/tcp.toml` when
you need a Rustls-free plain TCP baseline.

CI-friendly scenarios should stay short and deterministic:

- baseline echo throughput
- max connection cap
- max request cap
- slow reader
- slow writer
- graceful shutdown under load

For plain-TCP connection churn, cap each connection at one request and use
pipeline depth 1:

```bash
cargo run --release --package nacelle-stress-test -- \
  --connections 64 \
  --pipeline 1 \
  --requests-per-connection 1 \
  --duration-secs 30
```

The summary reports `completed_connections` and `connection_rate`. Churn mode
does not currently support TLS; use the persistent TLS profile for transport
throughput and latency evidence.

Heavy RPS and soak tests should run manually or nightly on dedicated Linux hosts.

For response-delivery comparisons, the stress server accepts
`--response-write-mode immediate` (the default) or
`--response-write-mode coalesce-buffered`. The same setting can be placed in a
server config as `response_write_mode`. Coalescing drains complete requests
already present in the socket read buffer and flushes before awaiting more
input, so it does not leave a single response waiting for a later request. It
is intended for measured pipelined workloads; keep immediate delivery for
latency-first or unmeasured workloads.

The Linux profiling helper records this setting and also exposes shared versus
serial handler dispatch for controlled diagnostics:

```bash
./scripts/profile-linux.sh \
  --tool baseline \
  --handler-mode shared \
  --response-write-mode coalesce-buffered \
  --pipeline 8 \
  --runs 3
```

Add `--feature-set default` for mimalloc. To measure the
self-signed Rustls config, also pass
`--config examples/nacelle-stress-server/configs/tcp-tls.toml` and
`--tls-insecure`. The latter disables certificate verification and is only for
the local generated certificate. The stress client flushes each populated
request window before reading responses so buffered TLS records cannot strand
deeply pipelined workers.
