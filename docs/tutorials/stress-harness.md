# Run the stress harness

The stress harness has two binaries:

- `nacelle-stress-server`, from `nacelle-stress-server`
- `nacelle-stress-test`, from `nacelle-stress-test`

Run the convenience script:

```bash
./examples/run-stress-test.sh
```

The script reads root `config.toml` by default. Pass `--config` to select a
repeatable benchmark profile. If the effective `tls_self_signed` value is true,
it passes `--tls-insecure` to the client so the server and client speak the
same transport.

For a plain TCP baseline, use
`examples/nacelle-stress-server/configs/tcp.toml`.

For full details, see the how-to guide:

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

The stress server installs a `metrics-util` recorder and prints a compact
metrics snapshot every 5 seconds. It enables request
started/completed counters plus request/response byte counters by default. The
generic telemetry API groups those switches under `request_metrics`; the stress
server exposes byte accounting as `byte_metrics = true`.
Use `--no-byte-metrics` for a lower-overhead recorder run. Use
`--no-default-features` with the plain TCP config for a Rustls-free,
system-allocator baseline; metrics collection remains active.

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
