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
- `NacelleApp` listener registration and `NacelleApp::run(...)` for the
  app-first serving path across TCP, Unix sockets, HTTP, and TLS.
- `nacelle::core::pipeline::Handler` for typed shared-runtime handlers.
- `Protocol` for TCP wire-format adapters.
- `NacelleTelemetry` and `NacelleTelemetryConfig` for metrics and telemetry.
- `NacelleMemoryBudget`, `NacelleMemoryAllocation`, and
  `NacelleRuntimeState::memory_budget()` for shared application/transport
  memory budget allocations.
- `TcpServer`, `HyperServer`, `nacelle::runtime::NacelleHost`, and
  `nacelle::advanced::runtime` when a service needs lower-level listener control.
