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

The stress server installs a `metrics-util` recorder and prints a compact
metrics snapshot every 5 seconds. It enables request
started/completed counters plus request/response byte counters by default. The
generic telemetry API groups those switches under `request_metrics`; the stress
server exposes byte accounting as `byte_metrics = true`.
Use `--no-byte-metrics` for a lower-overhead recorder run. Use
`--no-default-features` with the plain TCP config for a system-allocator,
Rustls-free diagnostic; metrics collection remains active. Add
`--features mimalloc-allocator` when the baseline must keep mimalloc while
disabling TLS.

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
