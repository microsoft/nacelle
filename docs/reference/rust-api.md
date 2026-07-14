# Rust API reference

Generate the Rust API reference with:

```bash
cargo doc --workspace --all-features --no-deps
```

On Windows:

```powershell
.\scripts\build-rustdoc.ps1
```

The generated index is:

```text
target/doc/nacelle/index.html
```

Start with these public entry points:

- `nacelle::prelude::*` for common application imports.
- `nacelle::core`, `nacelle::codec`, `nacelle::tcp`, `nacelle::http`,
  `nacelle::openssl`, `nacelle::rustls`, and `nacelle::runtime` for
  capability-oriented imports.
- `nacelle::openssl::NacelleOpenSslConfig` and
  `nacelle::rustls::NacelleTlsConfig` for concrete provider configuration.
- `nacelle::advanced::runtime` for raw executor and transport listener helpers
  when app/host composition is not sufficient.
- `nacelle::NacelleApp` listener registration and `NacelleApp::run(...)` for the
  app-first serving path across TCP, Unix sockets, HTTP, and TLS.
- `nacelle::core::pipeline::Handler` for typed shared-runtime handlers.
- `nacelle::runtime::{ThreadPerCoreConfig, WorkerSet}` and
  the `run_local_*_thread_per_core(...)` functions for experimental Linux-only
  worker-local TCP, HTTP, Rustls, required OpenSSL, and optional OpenSSL execution. This mode
  requires explicit selection and does not silently fall back to the shared
  runtime.
- `ThreadPerCoreConfig::with_max_threads(...)` to cap the worker threads selected by
  `WorkerSet::all()`, `WorkerSet::first(...)`, or `WorkerSet::explicit(...)` while preserving
  selection order. The shared runtime is caller-owned; configure its Tokio thread count on the
  runtime builder instead.
- `nacelle::runtime::ThreadPerCoreLimits::Global` for exact process-wide counters, or
  `ThreadPerCoreLimits::Worker` for partitioned worker-local counters. Worker
  mode still enforces one shared hard memory ceiling across all workers.
- `nacelle::runtime::WorkerContext::offload_blocking(...)` for explicit blocking work whose
  completion is awaited back on the originating local worker.
- `nacelle::tcp::Protocol` for TCP wire-format adapters.
- `nacelle::tcp::{TcpServer, LocalTcpServer}` for `Arc`-backed connection
  state, or `SerialTcpServer` / `LocalSerialTcpServer` for exclusive mutable
  state lent to one serial handler at a time.
- `NacelleApp` and `NacelleHost` serial listener methods for plain TCP,
  required OpenSSL, optional OpenSSL, and Unix sockets.
- `nacelle::runtime::run_local_serial_tcp_thread_per_core(...)` and
  `run_local_serial_tcp_openssl_thread_per_core(...)` for worker-local serial
  plain TCP and required OpenSSL. Use
  `run_local_serial_tcp_optional_openssl_thread_per_core(...)` when plaintext and OpenSSL must
  share one worker-local listener. Worker factories run once per worker, so
  externally bounded pools should be shared deliberately rather than
  constructed per worker.
- `nacelle::core::{NacelleTelemetry, NacelleTelemetryConfig}` for metrics and telemetry.
- `nacelle::core::{NacelleMemoryBudget, NacelleMemoryAllocation}` and
  `NacelleRuntimeState::memory_budget()` for shared application/transport
  memory budget allocations. Owned allocation guards can release retained
  capacity with `NacelleMemoryAllocation::shrink_to(...)`.
- `nacelle::tcp::TcpServer`, `nacelle::http::HyperServer`, `nacelle::runtime::NacelleHost`, and
  `nacelle::advanced::runtime` when a service needs lower-level listener control.
