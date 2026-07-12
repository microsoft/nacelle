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
- `nacelle::core`, `nacelle::codec`, `nacelle::tcp`, `nacelle::http`, and
  `nacelle::runtime` for capability-oriented imports.
- `nacelle::advanced::runtime` for raw executor and transport listener helpers
  when app/host composition is not sufficient.
- `nacelle::NacelleApp` listener registration and `NacelleApp::run(...)` for the
  app-first serving path across TCP, Unix sockets, HTTP, and TLS.
- `nacelle::core::pipeline::Handler` for typed shared-runtime handlers.
- `nacelle::runtime::{ThreadPerCoreConfig, WorkerSet}` and
  the `run_local_*_thread_per_core(...)` functions for experimental Linux-only
  worker-local TCP, HTTP, Rustls, and required OpenSSL execution. This mode
  requires explicit selection and does not silently fall back to the shared
  runtime.
- `nacelle::runtime::ThreadPerCoreLimits::Global` for exact process-wide counters, or
  `ThreadPerCoreLimits::Worker` for partitioned worker-local counters. Worker
  mode still enforces one shared hard memory ceiling across all workers.
- `nacelle::runtime::WorkerContext::offload_blocking(...)` for explicit blocking work whose
  completion is awaited back on the originating local worker.
- `nacelle::tcp::Protocol` for TCP wire-format adapters.
- `nacelle::tcp::{TcpServer, LocalTcpServer}` for `Arc`-backed connection
  state, or `SerialTcpServer` / `LocalSerialTcpServer` for exclusive mutable
  state lent to one serial handler at a time.
- `nacelle::runtime::run_local_serial_tcp_thread_per_core(...)` for worker-local
  serial TCP. Worker factories run once per worker, so externally bounded pools
  should be shared deliberately rather than constructed per worker.
- `nacelle::core::{NacelleTelemetry, NacelleTelemetryConfig}` for metrics and telemetry.
- `nacelle::core::{NacelleMemoryBudget, NacelleMemoryAllocation}` and
  `NacelleRuntimeState::memory_budget()` for shared application/transport
  memory budget allocations. Owned allocation guards can release retained
  capacity with `NacelleMemoryAllocation::shrink_to(...)`.
- `nacelle::tcp::TcpServer`, `nacelle::http::HyperServer`, `nacelle::runtime::NacelleHost`, and
  `nacelle::advanced::runtime` when a service needs lower-level listener control.
