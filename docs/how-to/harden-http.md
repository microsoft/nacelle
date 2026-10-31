# Harden HTTP listeners

Nacelle's HTTP transport is Hyper HTTP/1. Configure HTTP timeout and keep-alive behavior through `NacelleHttpLimits`, shared body-size budgets through `NacelleLimits`, and request-shape policy through `NacelleHttpPolicy`.

Defaults:

- `NacelleHttpLimits::header_read_timeout`: 30 seconds, enforced with Hyper's HTTP/1 header timeout and `TokioTimer`.
- `NacelleHttpLimits::request_body_read_timeout`: 30 seconds, enforced while reading body frames.
- `NacelleHttpLimits::response_write_timeout`: 30 seconds, enforced at Hyper's I/O write boundary.
- `NacelleHttpLimits::keep_alive`: enabled.
- `NacelleHttpLimits::max_connection_age`: disabled by default.
- request and response body size limits: 16 MiB each.

Configure parser bounds separately from post-parse header policy:

```rust
use nacelle::http::{HttpParserLimits, NacelleHttpLimits};

let limits = NacelleHttpLimits::default()
    .with_parser_limits(HttpParserLimits::new(16 * 1024, None)?);
```

The minimum buffer setting is 8192 bytes. It controls Hyper's parser strategy,
not an exact read allowance: complete heads may include body read-ahead beyond
that threshold. `None` keeps the default 100-header parser. A configured count
selects Hyper's configurable-count path; in the resolved implementation, counts
through 100 remain inline and larger counts allocate the header arrays.

Use typed admission when requests must obtain application capacity before body
work. The [HTTP admission example](https://github.com/microsoft/nacelle/blob/main/examples/nacelle-examples/src/bin/http_admission.rs)
shares an owned semaphore permit through `AppState` and accepts `/work`:

```sh
cargo run -p nacelle-examples --features http --bin http_admission -- 127.0.0.1:8080
curl -H 'Expect: 100-continue' --data 'work' http://127.0.0.1:8080/work
```

Install one policy with `.with_admission(policy, AdmissionConfig::new(timeout,
max_pending)?)`. Size waiter capacity across independently constructed workers
and listeners, and share the backend semaphore explicitly when its budget must
be process-wide. Pending admission does not poll the body or send `100 Continue`.
Rejection/deadline with an unread body closes HTTP/1, even when a following
request is already buffered. See the [status mapping and permit
lifetime](../reference/http-hardening.md#application-admission).

`NacelleHttpPolicy` can reject requests before the handler runs:

- allowed Host headers
- allowed HTTP methods
- maximum URI length
- maximum header count
- maximum aggregate header bytes
- optional per-peer request rate limits through `with_max_requests_per_peer_per_second`
- bounded lock-free per-peer request-rate state through
  `with_peer_rate_limit_table_capacity` (16,384 peers by default when enabled)
- optional trusted proxy forwarded address handling through `with_trusted_proxy_ips`
- optional security headers through `with_security_header(...)` or `with_default_security_headers()`
- optional per-peer connection caps through `NacelleLimits::with_max_connections_per_peer`
- optional live per-peer connection-open rate caps through `NacelleRuntimeControl::set_connection_open_rate`

Rejected requests receive deterministic HTTP responses where the request parser has already accepted the request: `405`, `414`, `421`, `429`, or `431`. Rejections emit low-cardinality telemetry reasons such as `host`, `method_not_allowed`, `uri_too_long`, `header_count`, `header_bytes`, `peer_rate`, and `peer_rate_table_full`.

Per-peer request and connection-open rate limiters use fixed-capacity,
lock-free tables. They retain active peer entries for 60 seconds and do not
allocate, lock, or scan every tracked peer during admission. Size the HTTP
table with `NacelleHttpPolicy::with_peer_rate_limit_table_capacity(...)` and
the TCP/connection table with
`NacelleLimits::with_connection_rate_limit_table_capacity(...)`. If a table is
full or no inactive entry is found within its fixed probe budget, a newly
observed peer is rejected. Choose capacity from the deployment's expected
active-peer cardinality rather than silently accepting unbounded state.

Enable the `rustls` feature to terminate HTTP over TLS. `NacelleTlsConfig` loads PEM certificate/key pairs, accepts explicit Rustls `ServerConfig` values, supports reloads for future handshakes, and enforces a TLS handshake timeout. Enable `tls-self-signed` only when local load tests or auto-deploying applications need to generate a self-signed certificate immediately; it implies `rustls`.

For direct edge HTTPS, build TLS config with
`NacelleTlsConfig::from_pem_with_allowed_server_names(...)` or
`NacelleTlsConfig::from_der_with_allowed_server_names(...)`. When an SNI
allowlist is configured, clients that omit SNI or send a name outside the list
fail during the TLS handshake. HTTP Host policy is enforced after the handshake,
so configure the SNI allowlist and `NacelleHttpPolicy::with_allowed_hosts(...)`
with the same service names unless you intentionally need a narrower Host
policy.

`NacelleTlsConfig` is the Rustls config shared with TCP TLS. HTTP TLS requires
the compile-time `rustls` feature. TCP can instead select `openssl`, but the two
backend features cannot be enabled together and cannot be swapped at runtime.

Enable `HyperServer::with_access_log(true)` when direct edge deployments need
structured request logs. Access events are emitted with target
`nacelle::access` and include transport, method, URI, status, request bytes,
elapsed microseconds, and rejection reason. Declared and streamed body-size
failures both use `request_body_bytes`; body-read deadlines use
`http_body_read`.

Forwarded peer identity is disabled by default. List only known proxy IPs with
`with_trusted_proxy_ips(...)` and choose the header your proxy controls:

```rust
use nacelle::http::{NacelleForwardedHeader, NacelleHttpPolicy};

let policy = NacelleHttpPolicy::new()
  .with_trusted_proxy_ips(["10.0.0.1".parse::<std::net::IpAddr>()?])
  .with_forwarded_header(NacelleForwardedHeader::Forwarded);
```

The default is `X-Forwarded-For`; `Forwarded` now requires explicit selection.
There is no fallback between header families. Every trusted proxy must sanitize
or append to the selected header, never pass it through unchanged. Nacelle uses
the rightmost untrusted IP across repeated header fields, or the leftmost IP
when all hops are trusted. Missing or malformed selected headers use the socket
peer identity. See the [HTTP hardening reference](../reference/http-hardening.md)
for parsing and migration details.

Internal handler errors return a generic `500` response. Construct a sanitized
`HttpResponse` explicitly for client-facing application errors. Protect
server-side diagnostics and avoid secrets in query strings logged by access logs.

Allow enough memory for handlers that aggregate entire bodies, and leave
separate headroom for Hyper read-ahead and TLS/socket buffers. See [runtime
limits](../topics/runtime-limits.md) for sizing guidance.

For internet-facing deployments, a reverse proxy or load balancer can still own coarse traffic filtering and certificate automation. Nacelle now also enforces application-level body, request, connection, per-peer connection/request/connection-open-rate, timeout, TLS handshake, security header, and optional Host/header/method/URI limits in-process.

Slowloris-style clients are closed by `NacelleHttpLimits::header_read_timeout`.
The body timeout bounds each frame wait; keep the handler deadline or an upstream
total upload deadline to bound continuous trickle uploads. Slow response readers
are closed by `NacelleHttpLimits::response_write_timeout` when writes stall.
