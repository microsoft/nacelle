# API stability

Nacelle is `0.2.x`, so public APIs are still experimental.

Stable enough for prototype integrations:

- `nacelle::core::pipeline` typed context, responder, and handler contracts
- `nacelle::tcp` and `nacelle::http` transport-owned request/response contracts
- `NacelleBody`
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
- feature combinations involving `otel` and `error-hints`

TCP application code may use the app-first path:
`NacelleApp::new(handler).serve(protocols).await`. HTTP and mixed-transport
services use `HyperServer` and `NacelleHost`. Lower-level server APIs remain
available when a service needs direct listener/runtime control. Telemetry docs
teach the generic `NacelleTelemetry` API.

The former detached `NacelleRequest`/`NacelleResponse` handler and Tower adapter
were removed. Transport pipelines now remain strongly typed through completion;
there is no compatibility adapter.

Before `1.0`, minor releases may change defaults or builder methods when production safety requires it. After `1.0`, public API changes should follow semver, with migration notes for config/default changes.

## Reference protocol migration

The former `reference_protocol` feature and its facade/prelude exports have
moved to the unpublished `examples/nacelle-reference-protocol` workspace
package. Repository examples depend on that package directly. Application code
should implement `nacelle::tcp::Protocol` or maintain its protocol in a separate
application crate rather than depending on a protocol implementation from the
Nacelle facade.
