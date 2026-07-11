# API stability

Nacelle is `0.3.x`, so public APIs are still experimental.

Stable enough for prototype integrations:

- `nacelle::core::pipeline` typed context, responder, and handler contracts
- `nacelle::tcp` and `nacelle::http` transport-owned request/response contracts
- `nacelle::core::NacelleBody`
- `nacelle::core::{NacelleLimits, NacelleRuntimeState}`
- `nacelle::NacelleApp` listener registration and `NacelleApp::run(...)`
- `nacelle::prelude::*` for common application imports
- `nacelle::core::{NacelleTelemetry, NacelleTelemetryConfig}`
- `nacelle::core::NacelleTelemetryObserver` for statically dispatched application telemetry

Experimental:

- transport-specific metadata
- transport listener option structs
- optional OpenSSL TLS detection on shared TCP listeners
- telemetry observer event details
- stress tooling config
- feature combinations involving `otel` and `error-hints`

Application code should use the app-first path:
`NacelleApp::new().tcp(...).http(...).run().await`. The app owns shared runtime
state, telemetry, shutdown, and listener supervision. Concrete transport
servers retain transport-specific limits and policy. `nacelle::runtime::NacelleHost`
and lower-level server APIs remain available for advanced manual supervision.

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
