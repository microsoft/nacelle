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
cargo bench -p nacelle-examples --features "bench tcp"
```

For the codec/TCP integration specifically, run both Criterion targets:

```bash
cargo bench -p nacelle-codec --bench framed_comparison --all-features
cargo bench -p nacelle-examples --bench critical_paths --features "bench tcp"
```

The telemetry group can be run independently:

```bash
cargo bench -p nacelle-examples --bench critical_paths --features "bench tcp" -- telemetry --noplot
```

The TCP crate also measures one complete connection plus one request with OTel
compiled in:

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

## Phase 7 local confidence checks

The following measurements are local confidence checks, not portable release
claims. They were collected from source checkpoint `9e42adb` under WSL2 kernel
`6.6.87.2-microsoft-standard-WSL2` on an Intel Xeon Platinum 8370C host
(8 cores / 16 logical CPUs) with `rustc 1.95.0`, Criterion defaults, the system
allocator, and features `bench,tcp`. WSL2 did not expose a CPU governor:

- disabled connection telemetry: approximately `0.58-0.62 ns`
- disabled request-completed telemetry: approximately `1.89-1.91 ns`
- disabled timeout telemetry: approximately `0.78-0.79 ns`
- concrete in-memory observation: approximately `60-89 ns` across repeated
	local runs, dominated by its mutex and event-vector write

Generated-code inspection used monomorphized release modules with ThinLTO
disabled for readability:

```bash
cargo rustc -p nacelle-examples --bin echo --release --features tcp -- \
	-C lto=no -C codegen-units=1 --emit=llvm-ir
cargo rustc -p nacelle-examples --bin http_echo --release --features http -- \
	-C lto=no -C codegen-units=1 --emit=llvm-ir
```

The complete TCP and HTTP modules contained 98 and 221 indirect calls,
respectively, from the full dependency graph. Inspection found no Nacelle
dynamic-observer symbols or boxed handler-future symbols. The modules retain
Nacelle's startup-only listener-installer indirection plus calls from Tokio,
Hyper, tracing, and allocator internals. These feature sets did not enable TLS
or OpenTelemetry, so provider/backend indirection requires separate builds.
No module-wide count should be presented as request-path Nacelle dispatch
without symbol-level attribution.

`perf`/flamegraph profiling was unavailable for the WSL2 kernel used for these
checks. Run profile-level validation on the target Linux kernel before making
throughput, latency, or production-readiness claims.

Matched 30-second saturation profiles were also collected from source
checkpoint `7eb400f` with 8 server threads, 256 connections, pipeline depth 8,
256-byte requests, 64-byte responses, byte metrics enabled, and the default
OpenTelemetry console observer:

| Profile | Requests | Effective RPS | p50 | p95 | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain TCP | 23,521,395 | 783,584 | 2.45 ms | 4.32 ms | 5.75 ms | 24.19 ms |
| Low-memory TCP | 23,389,425 | 779,230 | 2.48 ms | 4.52 ms | 6.34 ms | 28.30 ms |
| Self-signed Rustls TCP | 18,868,139 | 628,688 | 3.04 ms | 5.36 ms | 7.36 ms | 37.08 ms |

All three clients completed without reported failures. The low-memory server
reported 20 MiB of accounted connection memory while 256 connections were
active and returned to zero after they closed. These are saturation results;
use pipeline depth 1 for established-connection RTT measurements. They are not
a substitute for a matched pre-migration baseline or target-hardware load test.

## Disabled-policy specialization

A matched local comparison used the `telemetry_paths` benchmark at checkpoint
`0bce7f0` and after caching the effective TCP telemetry plan once per connection.
Both builds used the same WSL2 host/toolchain and all TCP features:

| Path | Before | After | Local delta |
| --- | ---: | ---: | ---: |
| Metrics disabled | 5.72-5.76 us | 5.03-5.07 us | approximately 12% lower |
| Metrics enabled | 7.00-7.04 us | 7.02-7.08 us | no material change |

The optimized path does not construct `NacelleMetricsContext` or OTel attribute
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
- keep telemetry observers optional; the default `NoopObserver` must remain allocation-free
- preserve single-chunk body fast paths
- tune TCP buffer sizes for the connection count instead of relying on large defaults
