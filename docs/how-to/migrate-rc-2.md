# Migrate from 0.3.0-rc.1 to 0.3.0-rc.2

Pin the release candidate exactly while evaluating it:

```toml
nacelle = { version = "=0.3.0-rc.2" }
```

A broad `0.3` requirement does not select Cargo prereleases. Keep the same
feature set that was validated with RC.1.

RC.2 preserves the Rust API, wire behavior, limits, timeout defaults, and TLS
feature relationships from RC.1. It adds compiler-pressure and correctness
regressions and changes the emitted metrics schema. Applications that do not
consume Nacelle metrics need no source migration.

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
