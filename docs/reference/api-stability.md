# API stability

Nacelle is pre-`1.0`, but the `0.3` line distinguishes supported opt-in APIs
from explicitly experimental features.

Stable enough for prototype integrations:

- `nacelle::core::pipeline` typed context, responder, and handler contracts
- `nacelle::tcp` and `nacelle::http` transport-owned request/response contracts
- `nacelle::core::NacelleBody`
- `nacelle::core::{NacelleLimits, NacelleRuntimeState}`
- `nacelle::NacelleApp` listener registration and `NacelleApp::run(...)`
- `nacelle::prelude::*` for common application imports
- `nacelle::core::{NacelleTelemetry, NacelleTelemetryConfig}`
- `nacelle::core::NacelleTelemetryObserver` for statically dispatched application telemetry
- the `phase-timing` feature and its documented low-cardinality phase schema
- the `error-hints` feature and `NacelleError::hint()` method

Experimental:

- runtime memory accounting behind `experimental-memory`
- Linux thread-per-core execution behind `experimental-thread-per-core`
- plaintext/OpenSSL detection behind `experimental-openssl-detection`
- stress tooling config

Features prefixed with `experimental-` are default-off and use at your own
risk. They are not part of the supported `0.3` contract and may change or be
removed in a future minor release. `NacelleError::hint()` is supported, but its
returned text is advisory operator guidance: do not parse it or treat it as a
stable error identifier. Match `NacelleError::ResourceLimit` with
`NacelleResourceLimitReason`, or `NacelleError::Timeout` with
`NacelleTimeoutReason`, instead. Both reason enums are non-exhaustive; include a
wildcard arm. Their `as_str()` methods return stable low-cardinality telemetry
labels. Applications may use `Other(&'static str)` for their own static, bounded
reason vocabulary.

Application code should use the app-first path:
`NacelleApp::new().tcp(...).http(...).run().await`. The app owns shared runtime
state, telemetry, shutdown, and listener supervision. Concrete transport
servers retain transport-specific limits and policy. `nacelle::runtime::NacelleHost`
and lower-level server APIs remain available for advanced manual supervision.

Use `NacelleApp::with_state(...)` when handlers need application dependencies.
The app shares one typed root internally through `Arc`, while handlers borrow
`&AppState` from `RequestContext::app_state()`. Mutable access, dynamic type
maps, and runtime replacement of the whole root are outside the contract.

Growth-prone connection metadata, `ConnectionInfo`, telemetry events and event
kinds, and TCP/Unix listener option types are
non-exhaustive. Consumers must include wildcard enum match arms and construct
supported option values through `new`, `Default`, conversions, and `with_*` or
`without_*` builders. `NacelleTcpConfig` and transport limit types follow the
same builder-first rule so settings introduced by later releases retain their
defaults.

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
