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

The stress client enables its Rustls support by default so `--tls-insecure`
works with the local self-signed server. For a Rustls-free plain TCP build, run
both stress binaries with `--no-default-features` and use
`examples/nacelle-stress-server/configs/tcp.toml`.

Repeatable profiles:

- `examples/nacelle-stress-server/configs/tcp.toml`: plain TCP baseline.
- `examples/nacelle-stress-server/configs/tcp-low-memory.toml`: plain TCP with
  mimalloc low-memory behavior enabled.
- `examples/nacelle-stress-server/configs/tcp-tls.toml`: TCP wrapped in
  self-signed TLS.

Linux example:

```bash
./examples/run-stress-test.sh --config examples/nacelle-stress-server/configs/tcp.toml --server-threads 48 --connections 256 --pipeline 8 --duration-secs 30 --payload-bytes 256
```

PowerShell example:

```powershell
.\examples\run-stress-test.ps1 -Config examples/nacelle-stress-server/configs/tcp.toml -ServerThreads 48 -Connections 256 -Pipeline 8 -DurationSecs 30 -PayloadBytes 256
```

OpenTelemetry metrics are enabled in the default stress server build. That build
prints a compact OTel console snapshot every 5 seconds and enables request
started/completed counters plus request/response byte counters by default. The
generic telemetry API groups those switches under `request_metrics`; the stress
server exposes byte accounting as `byte_metrics = true`.
Use `--no-byte-metrics` for a lower-overhead OTel run, or use
`--no-default-features` with the plain TCP config when you intentionally want a
metrics-free, system-allocator diagnostic. Add `--features mimalloc-allocator`
when the baseline must keep mimalloc while disabling TLS and OpenTelemetry.

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
