# API stability

Nacelle is `0.2.x`, so public APIs are still experimental.

Stable enough for prototype integrations:

- `NacelleRequest`, `NacelleResponse`, and `NacelleBody`
- `Handler` and `handler_fn`
- `NacelleLimits` and `NacelleRuntimeState`
- `NacelleHost`
- `NacelleApp`, `NacelleProtocols`, `NacelleApp::serve(...)`, and `serve(...)`
- `nacelle::prelude::*` for common application imports
- `NacelleTelemetry` and `NacelleTelemetryConfig`
- `NacelleTelemetrySink` for application telemetry bridges

Experimental:

- transport-specific metadata
- transport listener option structs
- optional OpenSSL TLS detection on shared TCP listeners
- telemetry sink details
- stress tooling config
- feature combinations involving `tower`, `otel`, and `error-hints`

New application code should prefer the app-first path:
`NacelleApp::new(handler).serve(protocols).await`. Lower-level server and host
APIs remain available when a service needs direct listener/runtime control.
Telemetry docs should teach the generic `NacelleTelemetry` API.

Before `1.0`, minor releases may change defaults or builder methods when production safety requires it. After `1.0`, public API changes should follow semver, with migration notes for config/default changes.
