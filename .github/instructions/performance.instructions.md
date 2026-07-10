---
name: Performance
description: "Use when changing or reviewing throughput, latency, memory use, allocation behavior, buffering, telemetry overhead, benchmarks, or the Nacelle stress harness."
---

# Performance Guidelines

- Establish the existing baseline before making throughput, latency, memory, or binary-size claims.
- Compare the same platform, toolchain, feature flags, configuration, workload, dataset, cache state, and security mode.
- Keep counters, tracing, logging, allocation-heavy helpers, timers, and per-request locks out of hot paths unless measurement justifies them.
- Preserve bounded buffers, backpressure, and opt-in diagnostic overhead. Add feature gates or explicit configuration when new work could add steady-state cost.
- Use the existing Criterion benchmarks or `examples/nacelle-stress-*` harness, and record enough command and configuration detail to reproduce a comparison.
- Prefer explicit benchmark or stress profiles over changing root defaults. Treat local measurements as confidence checks rather than release-grade proof.
- Update `docs/topics/performance-model.md` or `docs/how-to/compare-performance.md` when assumptions, methodology, or operator guidance changes.