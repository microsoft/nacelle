# HTTP hardening reference

Nacelle's HTTP transport is Hyper HTTP/1. Configure HTTP timeout and keep-alive behavior through `NacelleHttpLimits`, shared body-size budgets through `NacelleLimits`, and request-shape policy through `NacelleHttpPolicy`.

Defaults:

- `NacelleHttpLimits::header_read_timeout`: 30 seconds, enforced with Hyper's HTTP/1 header timeout and `TokioTimer`.
- `NacelleHttpLimits::request_body_read_timeout`: 30 seconds, enforced while reading body frames.
- `NacelleHttpLimits::response_write_timeout`: 30 seconds, enforced at Hyper's I/O write boundary.
- `NacelleHttpLimits::keep_alive`: enabled.
- `NacelleHttpLimits::max_connection_age`: disabled by default.
- request and response body size limits: 16 MiB each.

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
- optional per-peer connection-open rate caps through `NacelleLimits::with_max_connection_opens_per_peer_per_second`

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

Enable `HyperServer::with_access_log(true)` when direct edge deployments need structured request logs. Access events are emitted with target `nacelle::access` and include transport, method, URI, status, request bytes, elapsed microseconds, and rejection reason.

Forwarded peer identity is disabled by default. The immediate socket peer must
be listed in `NacelleHttpPolicy::with_trusted_proxy_ips(...)`. When trusted,
Nacelle reads only `X-Forwarded-For` by default. Proxies using the standardized
header must explicitly select
`with_forwarded_header(NacelleForwardedHeader::Forwarded)`. There is no fallback
between header families. This replaces the previous automatic preference for
`Forwarded`; update proxy-aware applications when upgrading.

Across all instances of the selected header, Nacelle selects the rightmost
untrusted IP, skipping trusted proxy hops to its right. If every address is
trusted, the leftmost address is used. Missing, malformed, or ambiguous selected
headers fall back to the socket peer; numeric IP addresses are required, and
duplicate `for` parameters are invalid. Configure every trusted proxy to
sanitize or append to the selected header on every request. Never trust a proxy
that passes that header through unchanged. Request rate limits, request metadata,
and access logs all use this effective identity.

Handler failures produce a generic `500` body (`internal server error`), not
the wrapped error's diagnostic text. Return an explicit sanitized `HttpResponse`
for application errors intended for clients. Failure telemetry still receives
the original error; protect diagnostic logs and observers accordingly.

With `experimental-memory` and a finite memory budget, known-length bodies
reserve their declared length. Unknown-length/chunked bodies reserve each data
chunk before enqueueing it for the handler. Charges follow queued and retained
`Bytes` clones until the last clone drops. Admission waits honor
`memory_allocation_timeout` and are cancelled when the request is dropped.
Leave headroom for Hyper's read-ahead, shared backing allocations, and TLS/socket
buffers: the body budget is not a process-RSS ceiling. Handlers aggregating a
whole chunked body need enough budget to retain that body without blocking their
own subsequent chunks.

Rustls certificate-only reloads preserve the configured SNI allowlist, including
concurrent reloads. Invalid replacement material leaves the previous snapshot
active. Explicit `replace_server_config*` calls instead install the supplied
configuration's policy and clear the stored allowlist for subsequent reloads.

For internet-facing deployments, a reverse proxy or load balancer can still own coarse traffic filtering and certificate automation. Nacelle now also enforces application-level body, request, connection, per-peer connection/request/connection-open-rate, timeout, TLS handshake, security header, and optional Host/header/method/URI limits in-process.

Slowloris-style clients are closed by `NacelleHttpLimits::header_read_timeout`.
The request-body timeout bounds each frame wait, not total upload duration;
keep the default handler deadline or enforce a total deadline upstream to bound
continuous trickle uploads. Slow response readers are closed by
`NacelleHttpLimits::response_write_timeout` when socket writes stop making progress.
