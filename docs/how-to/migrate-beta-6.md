# Migrate from beta.5 to beta.6

The beta.6 stabilization track deliberately changes several pre-release APIs.
Wire behavior and bounded defaults remain unchanged. Update feature selection,
application state, error matching, and direct construction of extensible types
before upgrading.

## Select one TLS backend

TLS is now a graph-wide compile-time choice. Enable exactly one backend:

```toml
# HTTP or TCP with Rustls
nacelle = { version = "0.3", default-features = false, features = ["tcp", "http", "rustls"] }

# TCP with OpenSSL
nacelle = { version = "0.3", default-features = false, features = ["tcp", "openssl"] }
```

Remove the former `tls` umbrella feature, `NacelleTlsProvider`, and calls to
`NacelleTlsConfig::provider()` or `NacelleOpenSslConfig::provider()`. Backend
selection comes from Cargo features and the concrete configuration type. A
dependency graph that enables both `rustls` and `openssl` is rejected at compile
time. HTTP TLS requires Rustls; TCP TLS supports either backend.

## Enable experimental APIs explicitly

Linux thread-per-core APIs now require `experimental-thread-per-core`:

```toml
nacelle = { version = "0.3", features = ["tcp", "experimental-thread-per-core"] }
```

Plaintext/OpenSSL detection and optional-OpenSSL listener APIs now require
`experimental-openssl-detection`, which enables TCP and OpenSSL:

```toml
nacelle = { version = "0.3", default-features = false, features = ["experimental-openssl-detection"] }
```

These gates make the experimental boundary explicit; the gated APIs remain
available when their feature is enabled.

## Move dependencies into typed application state

Code that captured shared dependencies in each handler can move them into one
application-owned root. Add the root as the final request-context type parameter
and borrow it with `app_state()`:

```rust
use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};

struct AppState {
    response_prefix: &'static [u8],
}

let handler = handler_fn(
    |context: TcpRequestContext<MyProtocol, AppState>| async move {
        let prefix = context.app_state().response_prefix;
        context.respond(TcpResponse::bytes(prefix)).await
    },
);
let server = TcpServer::<MyProtocol>::builder()
    .protocol(MyProtocol)
    .handler(handler)
    .build()?;

NacelleApp::with_state(AppState {
    response_prefix: b"service: ",
})
.tcp("service", address, server)
.run()
.await?;
# Ok::<(), nacelle::core::NacelleError>(())
```

`NacelleApp`, `NacelleHost`, TCP handler/context types, and HTTP handler/context
types retain `()` as their default state, so applications without dependencies
need no state-related changes. `NacelleApp` shares one stable root through
`Arc`; there is no mutable accessor or runtime replacement of the root. Put
reloadable configuration behind an application-owned service in that root.

Low-level code that calls `RequestContext::new(...)` directly must now pass an
`Arc<AppState>` instead of an inline state value. Because access may dereference
an `Arc`, `RequestContext::app_state()` is no longer a `const fn`.

## Match structured failure reasons

`NacelleError::ResourceLimit` and `NacelleError::Timeout` no longer contain raw
strings. Match the corresponding non-exhaustive reason enum:

```rust
use nacelle::core::{NacelleError, NacelleTimeoutReason};

match error {
    NacelleError::Timeout(NacelleTimeoutReason::Handler) => {
        // Apply handler-timeout policy.
    }
    NacelleError::Timeout(reason) => {
        tracing::warn!(reason = reason.as_str(), "operation timed out");
    }
    _ => {}
}
```

Use `NacelleResourceLimitReason::Other("application_reason")` or
`NacelleTimeoutReason::Other("application_reason")` for application-defined
static reasons. Keep these values bounded and low-cardinality. Use `as_str()`
for stable telemetry or log labels; do not parse `Display` or `hint()` text.

## Use builders for extensible values

Connection metadata, `ConnectionInfo`, telemetry events and event kinds, and
TCP/Unix listener option structs are now non-exhaustive. Replace external struct
literals with `new`, `Default`, conversions, and `with_*` builders. Add wildcard
arms when matching non-exhaustive enums:

```rust
match event.kind {
    KnownKind => handle_known(),
    _ => handle_other(),
}
```

## Disable timeouts explicitly

Timeout defaults remain bounded. Applications that intentionally require no
deadline can use the new consuming builders:

- `NacelleLimits::without_handler_timeout()`
- `NacelleTcpLimits::without_read_timeout()`
- `NacelleTcpLimits::without_write_timeout()`
- `NacelleTcpLimits::without_shutdown_timeout()`
- `NacelleTcpLimits::without_idle_timeout()`
- `NacelleHttpLimits::without_header_read_timeout()`
- `NacelleHttpLimits::without_request_body_read_timeout()`
- `NacelleHttpLimits::without_response_write_timeout()`
- `NacelleHttpLimits::without_max_connection_age()`

Disabling an internet-facing deadline weakens resource protection. Keep an
equivalent upstream or application deadline where appropriate.

## Compatibility review

The stabilization review compared the public API of all seven published crates
against the final beta.5 implementation checkpoint with `cargo-public-api`.
Rustls and OpenSSL surfaces were reviewed separately, and the newly gated
experimental surfaces were snapshotted with their features enabled.

- `nacelle-codec` has no public API changes.
- `nacelle-rustls` and `nacelle-openssl` only remove runtime provider accessors.
- Core and transport changes are limited to structured reasons, application
  state, non-exhaustive types, and additive timeout-disable builders.
- Facade removals in ordinary feature lanes are the newly gated experimental
  APIs; those APIs remain present in their explicit feature lanes.

No unclassified public API removal was found. Re-run the application test suite
under its selected TLS backend and explicit experimental features before
deploying the upgrade.