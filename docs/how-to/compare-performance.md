# Compare performance profiles

Use separate profiles for each transport mode:

- plain TCP
- TCP with low-memory allocator behavior
- TCP with TLS
- HTTP

Do not compare TLS and non-TLS runs as if they measure the same path. Likewise,
do not compare two runs if the stress client version changed.

Recommended plain TCP baseline config:

```text
examples/nacelle-stress-server/configs/tcp.toml
```

Then run:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

Record the tested commit, kernel, CPU model/topology, CPU governor, Rust toolchain,
allocator, features, configuration, connection count, pipeline depth, payload,
TLS provider, and client revision with every result. A throughput number without
that context is not a comparable baseline.

Suggested local benchmark:

```bash
cargo bench -p nacelle-examples --features "bench tcp exp-memory-limits"
```

For the codec/TCP integration specifically, run both Criterion targets:

```bash
cargo bench -p nacelle-codec --bench framed_comparison --all-features
cargo bench -p nacelle-examples --bench critical_paths --features "bench tcp exp-memory-limits"
```

The telemetry group can be run independently:

```bash
cargo bench -p nacelle-examples --bench critical_paths --features "bench tcp exp-memory-limits" -- telemetry --noplot
```

The TCP crate also measures one complete connection plus one request through the
metrics facade, with independent phase-timing coverage:

```bash
cargo bench -p nacelle-tcp --bench telemetry_paths --all-features -- --noplot
```

Response delivery policies have a separate end-to-end benchmark:

```bash
cargo bench -p nacelle-tcp --bench response_delivery -- --noplot
```

## Compare a tag or commit

The PowerShell comparison scripts keep data under
`target/performance-comparisons`, resolve tags to immutable commit hashes, and
use detached temporary worktrees so the current checkout is not modified. Each
revision receives an isolated Cargo target directory; only Criterion baseline
data is copied into the candidate target before comparison, preventing build
artifacts from being reused across worktrees.

Capture all Criterion suites for the current commit, a release tag, or another
commit:

```powershell
./scripts/capture-performance-baseline.ps1
./scripts/capture-performance-baseline.ps1 -Reference v0.3.0
./scripts/capture-performance-baseline.ps1 -Reference 00747f3
```

With no parameters, `HEAD` is captured in a detached worktree. Uncommitted
working-tree changes are intentionally excluded from that baseline.

Compare the current working tree with a captured baseline:

```powershell
./scripts/compare-performance.ps1 -BaselineReference v0.3.0
```

Compare two committed revisions without checking either one out:

```powershell
./scripts/compare-performance.ps1 `
	-BaselineReference v0.3.0 `
	-CandidateReference 0123456789abcdef
```

Use `-Suite` to limit both capture and comparison to one or more matching
suites: `codec`, `critical-paths`, `telemetry`, or `response-delivery`. For
example:

```powershell
./scripts/capture-performance-baseline.ps1 `
	-Reference v0.3.0 `
	-Suite critical-paths,telemetry
./scripts/compare-performance.ps1 `
	-BaselineReference v0.3.0 `
	-Suite critical-paths,telemetry
```

The default capture runs every suite available at the selected commit. The
`critical-paths` suite follows its historical move from `nacelle` to
`nacelle-examples`. The telemetry and response-delivery suites are omitted for
commits that predate those benchmark targets. When comparison omits `-Suite`,
it uses exactly the suites recorded by the baseline, so newer-only targets do
not invalidate an older baseline. Within a shared suite, Criterion compares
matching benchmark IDs and measures newer IDs without a delta when no baseline
exists for them.

Each capture records the resolved commit, Rust toolchain, operating system,
architecture, CPU information, selected suites, and full benchmark log. Each
comparison records the same candidate metadata and Criterion's percentage and
confidence-interval output. Use `-Force` to replace an existing baseline for
the same resolved commit.

## Run native host workloads

On a dedicated Linux host, the native workload harness detects physical cores,
SMT siblings, sockets, NUMA nodes, memory, and CPU governors. It reserves one
physical core per NUMA node for the operating system and uses one logical CPU
per remaining physical core. Review the generated plan without building or
running workloads:

```powershell
./scripts/run-native-performance.ps1 -PlanOnly
```

Run the default capacity matrix with a warm-up before every measured sample:

```powershell
./scripts/run-native-performance.ps1 `
	-Runs 3 `
	-WarmupSecs 15 `
	-DurationSecs 30
```

The harness defaults to three 30-second measured runs per workload. The default
capacity matrix uses 50 persistent connections, pipeline depth 1, worker counts
of 1, 2, 4, 8, 16, 32, and 36, and response bodies of 0, 1, 10, and 100 KiB.
Worker counts larger than the isolated server core set detected on the host are
omitted. Requests use a zero-byte body while retaining the protocol's fixed
frame overhead; sample banners report request and response body sizes
separately.

Run plain TCP and TLS as separate result sets:

```powershell
./scripts/run-native-performance.ps1 `
	-Config examples/nacelle-stress-server/configs/tcp.toml `
	-OutputDirectory target/native-performance/plain
./scripts/run-native-performance.ps1 `
	-Config examples/nacelle-stress-server/configs/tcp-tls.toml `
	-OutputDirectory target/native-performance/tls
```

The earlier diagnostic profiles remain available explicitly. For example:

```powershell
./scripts/run-native-performance.ps1 -Profile min-rtt,pooled
```

Use `-Profile all` to run both the capacity matrix and the diagnostic
minimum-RTT, pooled, pool-saturation, and pipelined-throughput profiles. Override
the capacity dimensions with `-CapacityWorkerCounts` or
`-CapacityResponseKiB`.

The harness builds native release binaries once, starts a fresh server for each
sample, applies process and memory binding with `numactl`, and writes the host
snapshot, exact workload plan, raw server/client logs, per-run parsed results,
and median summary JSON under `target/native-performance`. It warns when it
detects virtualization or a non-performance CPU governor and refuses to reuse
an occupied bind address. Use `-SkipBuild` only when the release binaries
already match the current source.

The codec target measures length-delimited encoding, compares direct decoding
with `MessageReader::decode_buffered`, measures incomplete-header calls, and
separates the no-op buffer-rotation check from replacing an empty 256 KiB
buffer. The TCP target measures per-connection decoder construction and
compares direct reference-protocol decoding with the buffered 64-request
head/body drain used by the connection loop. Treat the rotation replacement
result as allocation cost, not per-request overhead.

The `runtime_limits` benchmark group covers connection/request permit
acquire/drop and memory allocation overhead. Watch it closely after changes to
`NacelleRuntimeState`.

## Profile Linux CPU and allocations

Use the Linux profiling helper for repeatable plain-TCP diagnostics:

```bash
./scripts/profile-linux.sh --tool baseline
./scripts/profile-linux.sh --tool perf
./scripts/profile-linux.sh --tool heaptrack
```

The helper builds the `profiling` Cargo profile with optimization, debug
information, and frame pointers. Its default `--feature-set minimal` uses
`--no-default-features`, which disables TLS and mimalloc. The stress server's
downstream console metrics recorder remains active. The
system allocator is required because Heaptrack cannot intercept calls made
directly to mimalloc. Treat this as a diagnostic profile, not as a matched
comparison with the default mimalloc build.

Use `--feature-set default` to profile the default mimalloc and Rustls-capable
binaries. Self-signed Rustls workloads also require a TLS config
and the explicit local-test trust flag:

```bash
./scripts/profile-linux.sh \
	--tool perf \
	--feature-set default \
	--config examples/nacelle-stress-server/configs/tcp-tls.toml \
	--tls-insecure
```

The helper rejects Heaptrack with the default feature set because direct
mimalloc calls are invisible to Heaptrack. It also rejects `--tls-insecure`
with the minimal feature set because that client omits Rustls.

Pass disjoint CPU lists after reviewing the native harness plan:

```bash
pwsh -NoProfile -File ./scripts/run-native-performance.ps1 -PlanOnly
./scripts/profile-linux.sh \
	--tool perf \
	--server-cpus 2,4,6,8,10,12,14,16 \
	--client-cpus 3,5,7,9,11,13,15,17,19,21,23
```

For user-space profiling of processes owned by the current user, Linux must
permit performance counters. A value of `2` keeps kernel profiling disabled:

```bash
sudo sysctl -w kernel.perf_event_paranoid=2
```

Each run writes metadata, raw logs, and text reports under
`target/linux-profiles`. The `perf` mode warms the server before attaching and
records `cycles:u` at 999 Hz with frame-pointer call graphs. Heaptrack launches
the server directly so allocator interception survives process startup, then
applies any requested CPU affinity to all server threads.

The helper can compare handler ownership and response delivery without
changing their safe defaults:

```bash
./scripts/profile-linux.sh --tool perf --handler-mode serial
./scripts/profile-linux.sh \
	--tool baseline \
	--response-write-mode coalesce-buffered \
	--pipeline 8 \
	--runs 3
```

### July 2026 Linux response-delivery diagnostic

The following results are local confidence checks from an Intel Xeon Silver
4214 host with 24 physical cores, Linux 6.8, and Rust 1.95.0. The profiling
build used the system allocator, no TLS, and the former metrics configuration,
eight pinned server
workers, 256 persistent connections, 256-byte request bodies, 64-byte response
bodies, and all timeout defaults enabled. Server and client CPU sets were
disjoint. Each matrix cell used a five-second warm-up and three measured
ten-second runs; pipeline depths 1 and 8 also received ABBA interleaved checks.

| Pipeline | Immediate median | Coalesced median | Local delta |
| ---: | ---: | ---: | ---: |
| 1 | 614,661 req/s | 624,258 req/s | inconclusive; ABBA was -1.0% |
| 8 | 763,646 req/s | 1,081,277 req/s | +41.6% |
| 32 | 724,094 req/s | 1,256,126 req/s | +73.5% |

All clients completed without reported failures. The pipeline-8 ABBA check
measured 764,854-774,894 req/s for immediate delivery and
1,072,662-1,075,516 req/s for coalesced delivery. Matched perf captures lost no
samples; coalescing reduced `write_all_tracked_with_timeout` self share from
4.33% to 1.39%, `ResponseDelivery::write_pending` from 4.14% to 1.68%, and TCP
write polling from 1.59% to 0.46%. The connection loop always flushes after it
drains the requests already decoded from the current read buffer and before it
awaits another socket read.

These loopback saturation results support coalescing as an explicit option for
highly pipelined workloads, not as a universal default. Pipeline-1 performance
showed no repeatable benefit, and target-network latency, response sizes,
backpressure, TLS, and telemetry require separate measurements.

### Default-feature plain TCP and Rustls diagnostic

A follow-up on the same host used the then-default stress-server feature set:
mimalloc, the former OpenTelemetry recorder with byte metrics, and self-signed
Rustls support. The
workload, CPU sets, timeout defaults, warm-up, sample duration, and three-run
cell size matched the preceding matrix. Plain TCP and Rustls were measured as
separate transport profiles.

| Transport | Pipeline | Immediate median | Coalesced median | Local delta |
| --- | ---: | ---: | ---: | ---: |
| Plain TCP | 1 | 537,566 req/s | 536,274 req/s | -0.2% |
| Plain TCP | 8 | 619,749 req/s | 1,168,411 req/s | +88.5% |
| Plain TCP | 32 | 608,519 req/s | 1,329,735 req/s | +118.5% |
| Rustls | 1 | 478,062 req/s | 471,512 req/s | -1.4% |
| Rustls | 8 | 524,887 req/s | 529,998 req/s | +1.0% |
| Rustls | 32 | 547,103 req/s | 546,342 req/s | -0.1% |

All 36 measured client runs completed without reported failures; all cells had
at most 1.1% min-to-max spread. At pipeline depth 8, matched perf captures lost
no samples. Plain coalescing reduced tracked write-loop self share from 2.10%
to 0.47%, pending-write self share from 1.78% to 0.42%, `send` from 0.62% to
0.12%, and TCP write polling from 0.53% to 0.09%. The equivalent Rustls pair
was throughput-neutral: tracked write-loop self share remained approximately
1.01%, while encryption, TLS buffering, and the former metrics aggregation remained
material costs.

Deep-pipeline Rustls testing also exposed two stress-path correctness gaps.
The client now flushes buffered TLS requests before reading responses, and the
TCP connection driver flushes the underlying transport before another request
read and performs a write-timeout-bounded shutdown. This delivers terminal TLS
records promptly and emits `close_notify`. Final pipeline-32 Rustls samples all
completed in approximately 10.02 seconds instead of waiting for the 30-second
read timeout.

These results support coalescing for measured plain-TCP pipelined workloads on
this host. They do not support enabling it for Rustls or pipeline-1 workloads.

## Historical disabled-policy specialization

A matched local comparison used the `telemetry_paths` benchmark at checkpoint
`0bce7f0` and after caching the effective TCP telemetry plan once per connection.
Both builds used the same WSL2 host/toolchain and all TCP features:

| Path | Before | After | Local delta |
| --- | ---: | ---: | ---: |
| Metrics disabled | 5.72-5.76 us | 5.03-5.07 us | approximately 12% lower |
| Metrics enabled | 7.00-7.04 us | 7.02-7.08 us | no material change |

The optimized path did not construct `NacelleMetricsContext` or metric attribute
arrays when metrics are disabled, and request/phase mode checks are cached in a
copyable per-connection plan. Connection/request permits and memory accounting
remain active because they enforce runtime limits and expose operational state;
disabling telemetry does not disable safety policy.

On the same local WSL2 host, 64 already-buffered one-byte requests producing
32-byte responses measured. The first two rows use a 2 KiB base response
buffer; the threshold rows use a 1 KiB base buffer:

| Policy | Time | Recorded writes |
| --- | ---: | ---: |
| Immediate | 27.14-27.55 us | 64 |
| CoalesceBuffered | 20.18-20.40 us | 1 |
| FlushAtBytes(1024) | 20.22-20.45 us | 2 |
| FlushAtBytes(2048), grows from 1024 | 20.40-20.63 us | 1 |

This is a synthetic in-memory writer benchmark and demonstrates dispatch/write
amortization plus one transactional buffer-growth case, not network throughput.
Keep immediate delivery for latency-first workloads unless a matched workload
shows a benefit.

### Response coalescing depth and persistent-pool controls

The `response_delivery` benchmark also provides two policy-matched workload
families at pipeline depths 1, 8, and 32:

- `same_socket`: one persistent in-memory connection and one request window
- `pool`: eight persistent in-memory connections, each processing eight request
	windows; the next window becomes readable only after the previous response
	window is delivered

Both families compare `Immediate` with `CoalesceBuffered` on the same server,
decoder, handler, response shape, runtime, and transport. Every response is 32
bytes. The benchmark asserts total write count and largest write size on every
iteration. Response-buffer capacity is 2 KiB, so these cases remain inside the
base connection allocation and do not exercise overflow growth.

Run them with:

```bash
cargo bench -p nacelle-tcp --bench response_delivery --all-features
```

A local confidence run based on commit `9be59a0` with a modified worktree used
Rust 1.95.0 on Linux 6.6.87.2 WSL2 and an Intel Xeon Platinum 8370C virtualized
topology (one socket, eight visible cores, two threads per core). Times are
Criterion 95% confidence intervals from that one host.

| Same-socket depth | Policy | Time | Writes | Largest batch | Median delta |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Immediate | 5.569-5.699 us | 1 | 32 B | baseline |
| 1 | CoalesceBuffered | 5.536-5.641 us | 1 | 32 B | 1.1% lower |
| 8 | Immediate | 8.497-8.577 us | 8 | 32 B | baseline |
| 8 | CoalesceBuffered | 7.593-7.758 us | 1 | 256 B | 10.2% lower |
| 32 | Immediate | 17.834-17.944 us | 32 | 32 B | baseline |
| 32 | CoalesceBuffered | 14.344-14.436 us | 1 | 1,024 B | 19.6% lower |

| Pool depth | Policy | Time | Total writes | Largest batch | Median delta |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Immediate | 51.695-52.949 us | 64 | 32 B | baseline |
| 1 | CoalesceBuffered | 50.578-50.891 us | 64 | 32 B | 2.8% lower |
| 8 | Immediate | 229.82-230.90 us | 512 | 32 B | baseline |
| 8 | CoalesceBuffered | 176.78-177.72 us | 64 | 256 B | 23.1% lower |
| 32 | Immediate | 837.09-851.92 us | 2,048 | 32 B | baseline |
| 32 | CoalesceBuffered | 608.71-614.00 us | 64 | 1,024 B | 27.6% lower |

Depth 1 remains the latency control: both policies perform one write per
window, and their intervals are close. At depths 8 and 32, coalescing reduces
one response window to one write and lowers elapsed time in this synthetic
adapter workload. These results demonstrate write-path amortization, not socket
syscall cost, network throughput, allocator counts, or production latency.

Suggested RPS comparison:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --server-threads 48 --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

The stress server installs a `metrics-util` recorder and prints a compact metrics
snapshot every 5 seconds. Request metrics are grouped under the generic telemetry
`request_metrics` config; started/completed counters and byte counters are on by
default, while in-flight and duration metrics remain opt-in. Request duration
metrics are opt-in as well, which avoids request `Instant` work on core/HTTP
paths unless duration metrics or HTTP access logs are enabled. Use
`--no-byte-metrics` when comparing the cost of byte accounting. Use the
`telemetry_paths` Criterion target for a no-recorder facade baseline; every
stress-server feature set installs the console recorder.

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
- keep telemetry observers optional; the default `NoopObserver` must remain allocation-free
- preserve single-chunk body fast paths
- tune TCP buffer sizes for the connection count instead of relying on large defaults
