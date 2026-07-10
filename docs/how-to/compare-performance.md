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

More background:

The current `main` branch has been observed around 1.9M RPS on Linux for the TCP benchmark path. This branch should be compared against that baseline on the same host, kernel, CPU governor, allocator settings, and command line.

Suggested local benchmark:

```bash
cargo bench -p nacelle --features bench,reference_protocol
```

For the codec/TCP integration specifically, run both Criterion targets:

```bash
cargo bench -p nacelle-codec --bench framed_comparison --all-features
cargo bench -p nacelle --bench critical_paths --features bench,reference_protocol
```

The codec target compares direct decoding with `MessageReader::decode_buffered`,
measures incomplete-header calls, and separates the no-op buffer-rotation check
from replacing an empty 256 KiB buffer. The TCP target measures per-connection
decoder construction and compares direct reference-protocol decoding with the
buffered 64-request head/body drain used by the connection loop. Treat the
rotation replacement result as allocation cost, not per-request overhead.

The `runtime_limits` benchmark group covers connection/request permit
acquire/drop and memory allocation overhead. Watch it closely after changes to
`NacelleRuntimeState`.

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
- keep telemetry sinks optional; default operation should not push into in-memory sinks
- preserve single-chunk body fast paths
- tune TCP buffer sizes for the connection count instead of relying on large defaults
