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
- `NacelleApp`, `NacelleProtocols`, and `NacelleApp::serve(...)` for the
  app-first serving path.
- `Handler` for the app-core boundary.
- `Protocol` for TCP wire-format adapters.
- `NacelleTelemetry` and `NacelleTelemetryConfig` for metrics and telemetry.
- `NacelleMemoryBudget`, `NacelleMemoryAllocation`, and
  `NacelleRuntimeState::memory_budget()` for shared application/transport
  memory budget allocations.
- `TcpServer`, `NacelleHost`, and transport runtime helpers when a service
  needs lower-level listener control.
