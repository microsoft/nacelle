# Rust API reference

Generate the Rust API reference with:

```bash
cargo doc -p nacelle --no-default-features --features buffer-rotation,error-hints,experimental-memory,experimental-thread-per-core,http,phase-timing,rustls,tcp,tls-self-signed --no-deps
cargo doc -p nacelle-openssl --all-features --no-deps
```

On Windows:

```powershell
.\scripts\build-rustdoc.ps1
```

The facade documentation uses the Rustls backend. OpenSSL configuration is
documented separately from `nacelle-openssl` because both backend features
cannot be enabled in one build.

The generated index is:

```text
target/doc/nacelle/index.html
```

## Serving contract

The following contract applies to every supported app, host, direct TCP,
TCP/Unix/TLS listener, and HTTP serving entry point. Overloads only select
ownership of the listener, socket options, shutdown source, or drain timeout;
they do not change request semantics.

- **Purpose and ownership:** `NacelleApp` is the primary composition root and
  owns registered listener configurations until `run`. `NacelleHost` starts
  manually registered listeners immediately and owns their tasks until `wait`
  or `shutdown_and_wait`. Lower-level listener functions consume an `Arc`-backed
  server and own accepted connection tasks. Direct TCP methods borrow the
  server and own the supplied I/O value for the duration of the returned
  future. Dropping a serving future cancels that future; it does not provide a
  graceful drain guarantee.
- **Cancellation and shutdown:** entry points without a shutdown argument run
  until listener failure or external future cancellation. Token-aware entry
  points stop accepting when the token changes, wait for active connections,
  and abort tasks still active at the drain deadline. `NacelleApp::run` also
  requests process-wide shutdown when one listener fails. Configure Ctrl-C
  handling explicitly with `with_ctrl_c_shutdown()`.
- **Errors:** serving futures return `NacelleError` for bind, accept, socket,
  protocol, TLS, timeout, resource-limit, listener-task, and shutdown-drain
  failures. Match stable categories and reason enums rather than parsing
  `Display`. Connection-local failures, including HTTP connection-task panics,
  are observed through telemetry and do not normally stop the listener;
  listener setup/accept failure and top-level listener-task failure do.
- **Panics:** shared-runtime serving methods do not intentionally panic for
  runtime or peer input. They must be called while a Tokio runtime is entered;
  Tokio may panic otherwise. Worker-local methods additionally require the
  documented `LocalSet`/thread-per-core context. Panics from application
  handlers are task failures and may trigger host/app supervision; panic-abort
  builds terminate instead of unwinding.
- **Limits:** `NacelleRuntimeState` supplies process-wide connection, per-peer,
  request, streaming-task, body-size, and optional memory limits. TCP and HTTP
  server configurations add transport timeouts, frame/header policy, and edge
  limits. Listener overloads do not bypass these limits. Functions whose names
  contain `without_connection_limit` are advanced direct-I/O building blocks
  and require the caller to hold the connection permit.
- **Features:** plain TCP and Unix serving require `tcp`; HTTP/1 requires
  `http`; Rustls listeners require `rustls`; required OpenSSL listeners require
  `openssl`. `experimental-openssl-detection`, `experimental-memory`, and
  `experimental-thread-per-core` remain outside the supported `0.3` contract.
  Unix-domain listeners are available only on Unix targets.

Runnable examples exercise the same contracts:

```bash
cargo run -p nacelle-examples --bin echo
cargo run -p nacelle-examples --bin http_echo --no-default-features --features http
cargo run -p nacelle-examples --bin tls_echo --features tls-self-signed
cargo run -p nacelle-examples --bin tls_http_echo --no-default-features --features http,tls-self-signed
cargo run -p nacelle-examples --bin listener_tcp
cargo run -p nacelle-examples --bin unix_echo
cargo run -p nacelle-examples --bin openssl_echo --no-default-features --features openssl -- cert.pem key.pem
```

See [Runtime limits](../topics/runtime-limits.md) for default values and
[Operations model](../topics/operations.md#shutdown-and-draining) for the
listener drain sequence.

Start with these public entry points:

- `nacelle::prelude::*` for common application imports.
- `nacelle::core`, `nacelle::codec`, `nacelle::tcp`, `nacelle::http`,
  `nacelle::openssl`, `nacelle::rustls`, and `nacelle::runtime` for
  capability-oriented imports.
- `nacelle::openssl::NacelleOpenSslConfig` and
  `nacelle::rustls::NacelleTlsConfig` for concrete provider configuration.
- `nacelle::advanced::runtime` for raw executor and transport listener helpers
  when app/host composition is not sufficient.
- `nacelle::NacelleApp` listener registration and `NacelleApp::run(...)` for the
  app-first serving path across TCP, Unix sockets, HTTP, and TLS.
- `NacelleApp::with_state(...)` or `with_state_and_telemetry(...)` for one typed
  dependency root shared across listeners. Declare it in
  `TcpRequestContext<P, AppState>` or
  `HttpRequestContext<ConnectionState, AppState>` and borrow it through
  `RequestContext::app_state()`.
- `nacelle::core::pipeline::Handler` for typed shared-runtime handlers.
- `nacelle::tcp::{NacelleTcpConfig, NacelleTcpLimits}` for TCP buffering,
  framing, and timeout policy. These structs are non-exhaustive; construct them
  with `Default` and apply `with_*` builders so future fields retain their
  defaults.
- `nacelle::runtime::{ThreadPerCoreConfig, WorkerSet}` and
  the `run_local_*_thread_per_core(...)` functions for experimental Linux-only
  worker-local TCP, HTTP, Rustls, required OpenSSL, and optional OpenSSL
  execution. These APIs require `experimental-thread-per-core`; this mode does
  not silently fall back to the shared runtime.
- `LocalTcpRuntimeConfig::with_state(...)` and
  `LocalHttpRuntimeConfig::with_state(...)` to share the same typed dependency
  root across worker-local listeners.
- `ThreadPerCoreConfig::with_max_threads(...)` to cap the worker threads selected by
  `WorkerSet::all()`, `WorkerSet::first(...)`, or `WorkerSet::explicit(...)` while preserving
  selection order. The shared runtime is caller-owned; configure its Tokio thread count on the
  runtime builder instead.
- `nacelle::runtime::ThreadPerCoreLimits::Global` for exact process-wide counters, or
  `ThreadPerCoreLimits::Worker` for partitioned worker-local counters. Worker
  mode enforces one shared hard memory ceiling across all workers when
  `experimental-memory` is enabled.
- `nacelle::runtime::WorkerContext::offload_blocking(...)` for explicit blocking work whose
  completion is awaited back on the originating local worker.
- `nacelle::tcp::Protocol` for TCP wire-format adapters.
- `nacelle::tcp::{TcpServer, LocalTcpServer}` for `Arc`-backed connection
  state, or `SerialTcpServer` / `LocalSerialTcpServer` for exclusive mutable
  state lent to one serial handler at a time.
- With `experimental-memory`, `nacelle::tcp::TcpStreamingBodyMemoryPolicy` to
  retain declared-length admission or account only live streaming chunks.
- `NacelleApp` and `NacelleHost` serial listener methods for plain TCP,
  required OpenSSL, optional OpenSSL, and Unix sockets. Optional plaintext/
  OpenSSL methods require `experimental-openssl-detection`, which implies the
  `tcp` and `openssl` features.
- `nacelle::runtime::run_local_serial_tcp_thread_per_core(...)` and
  `run_local_serial_tcp_openssl_thread_per_core(...)` for worker-local serial
  plain TCP and required OpenSSL. Use
  `run_local_serial_tcp_optional_openssl_thread_per_core(...)` when plaintext and OpenSSL must
  share one worker-local listener; it requires both experimental features.
  Worker factories run once per worker, so
  externally bounded pools should be shared deliberately rather than
  constructed per worker.
- Use `without_handler_timeout()`, the four `NacelleTcpLimits::without_*_timeout()`
  builders, and the HTTP `without_*_timeout()` / `without_max_connection_age()`
  builders when an explicitly unbounded policy is required.
- `nacelle::core::{NacelleTelemetry, NacelleTelemetryConfig}` for metrics and telemetry.
- `nacelle::core::NacelleError::hint()` with the `error-hints` feature for
  optional operator guidance. `NacelleError::Display` remains stable across
  feature combinations; applications append hints deliberately where suitable.
  Hint text is advisory and must not be parsed as a stable identifier.
- Match `NacelleError::ResourceLimit(NacelleResourceLimitReason::...)` and
  `NacelleError::Timeout(NacelleTimeoutReason::...)` for programmatic handling.
  The reason enums are non-exhaustive and their `as_str()` methods expose stable
  low-cardinality labels. Use `Other(&'static str)` only for application-owned
  static reason vocabularies.
- With `experimental-memory`,
  `nacelle::core::{NacelleMemoryBudget, NacelleMemoryAllocation}` and
  `NacelleRuntimeState::memory_budget()` for shared application/transport
  memory budget allocations. Owned allocation guards can release retained
  capacity with `NacelleMemoryAllocation::shrink_to(...)`.
- `nacelle::tcp::TcpServer`, `nacelle::http::HyperServer`, `nacelle::runtime::NacelleHost`, and
  `nacelle::advanced::runtime` when a service needs lower-level listener control.

Connection metadata, `ConnectionInfo`, telemetry event types, and TCP/Unix
listener options are non-exhaustive. Observe them with
wildcard enum matches and construct option values through their documented
constructors, defaults, conversions, and builders.
