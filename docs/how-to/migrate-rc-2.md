# Migrate from 0.3.0-rc.1 to 0.3.0-rc.2

Pin the release candidate exactly while evaluating it:

```toml
nacelle = { version = "=0.3.0-rc.2" }
```

A broad `0.3` requirement does not select Cargo prereleases. Keep the same
feature set that was validated with RC.1.

RC.2 preserves the Rust API, limits, timeout defaults, and TLS feature
relationships from RC.1. It adds compiler-pressure and correctness regressions,
changes the emitted metrics schema, and rejects oversized declared HTTP request
bodies with `413 Payload Too Large` before handler dispatch. Applications that
do not instantiate affected serial connection futures, consume Nacelle metrics,
or rely on handling those oversized requests need no source migration.

## Raise the recursion limit for serial connection futures

RC.2 increases the type and layout depth of serial connection futures. A crate
that instantiates one of these futures can exceed rustc's default query-depth
limit during monomorphization, including under strict Clippy builds. This is a
compile-time limit and does not indicate runtime recursion or a runtime failure.

Add the following inner attribute to the root of each affected binary or library
crate (`main.rs` or `lib.rs`):

```rust
#![recursion_limit = "256"]
```

The attribute applies to the crate that compiles the concrete application
future; setting it in a dependency does not propagate to consumers.

## Update metric names

Metric names are now singular and resource-first:

| RC.1 | RC.2 |
| --- | --- |
| `nacelle.connections.opened` | `nacelle.connection.opened` |
| `nacelle.connections.accepted` | `nacelle.connection.accepted` |
| `nacelle.connections.active` | `nacelle.connection.active` |
| `nacelle.connections.in_flight` | `nacelle.connection.active` |
| `nacelle.connections.closed` | `nacelle.connection.closed` |
| `nacelle.requests.started` | `nacelle.request.started` |
| `nacelle.requests.in_flight` | `nacelle.request.active` |
| `nacelle.requests.completed` | `nacelle.request.completed` |
| `nacelle.requests.failed` | `nacelle.request.failed` |
| `nacelle.streaming_tasks.active` | `nacelle.streaming_task.active` |
| `nacelle.memory.used_bytes` | `nacelle.memory.usage` |
| `nacelle.request.duration_ms` | `nacelle.request.duration` |

RC.2 also separates rejected connections, rejected requests, and timed-out
requests into `nacelle.connection.rejected`, `nacelle.request.rejected`, and
`nacelle.request.timed_out`.

## Update metric types and units

Request and response body measurements changed from cumulative byte counters to
per-request histograms:

| RC.1 | RC.2 |
| --- | --- |
| `nacelle.request.bytes` counter | `nacelle.request.body.size` histogram in bytes |
| `nacelle.response.bytes` counter | `nacelle.response.body.size` histogram in bytes |
| `nacelle.request.duration_ms` histogram in milliseconds | `nacelle.request.duration` histogram in seconds |

Update dashboards, recording rules, alerts, and exporters together. Do not sum
the new body-size histograms as if they were the former cumulative counters.
During a rolling deployment, query RC.1 and RC.2 series separately or use an
explicit compatibility recording rule; the runtime does not emit both schemas.

The complete RC.2 schema and label guidance are in
[Operations model](../topics/operations.md#metric-schema).
