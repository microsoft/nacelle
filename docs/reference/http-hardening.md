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

`NacelleTlsConfig` is the Rustls config shared with TCP TLS.
`NacelleTlsProvider` reports `Rustls` for that config and `OpenSsl` for the TCP
OpenSSL backend. HTTP TLS currently uses Rustls.

Enable `HyperServer::with_access_log(true)` when direct edge deployments need structured request logs. Access events are emitted with target `nacelle::access` and include transport, method, URI, status, request bytes, elapsed microseconds, and rejection reason.

Forwarded peer identity is disabled by default. `Forwarded` and
`X-Forwarded-For` are considered only when the immediate socket peer is listed
in `NacelleHttpPolicy::with_trusted_proxy_ips(...)`; otherwise rate limits,
request metadata, and access logs use the socket peer address.

For internet-facing deployments, a reverse proxy or load balancer can still own coarse traffic filtering and certificate automation. Nacelle now also enforces application-level body, request, connection, per-peer connection/request/connection-open-rate, timeout, TLS handshake, security header, and optional Host/header/method/URI limits in-process.

Slowloris-style clients are closed by `NacelleHttpLimits::header_read_timeout`. Trickle request bodies are closed by `NacelleHttpLimits::request_body_read_timeout`. Slow response readers are closed by `NacelleHttpLimits::response_write_timeout` when socket writes stop making progress.
