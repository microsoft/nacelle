# HTTP hardening reference

Nacelle's HTTP transport is Hyper HTTP/1. Configure HTTP timeout and keep-alive behavior through `NacelleHttpLimits`, shared body-size budgets through `NacelleLimits`, and request-shape policy through `NacelleHttpPolicy`.

Defaults:

- `NacelleHttpLimits::header_read_timeout`: 30 seconds, enforced with Hyper's HTTP/1 header timeout and `TokioTimer`.
- `NacelleHttpLimits::request_body_read_timeout`: 30 seconds, enforced while reading body frames.
- `NacelleHttpLimits::response_write_timeout`: 30 seconds, enforced at Hyper's I/O write boundary.
- `NacelleHttpLimits::keep_alive`: enabled.
- `NacelleHttpLimits::max_connection_age`: disabled by default.
- `NacelleHttpLimits::parser`: Hyper's default bounded parser buffer strategy
  and stack-backed 100-header parser.
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
- optional live per-peer connection-open rate caps through `NacelleRuntimeControl::set_connection_open_rate`

Rejected requests receive deterministic HTTP responses where the request parser has already accepted the request: `405`, `414`, `421`, `429`, or `431`. Rejections emit low-cardinality telemetry reasons such as `host`, `method_not_allowed`, `uri_too_long`, `header_count`, `header_bytes`, `peer_rate`, and `peer_rate_table_full`.

## Parser and body boundaries

Host, method, URI, and header policies run after Hyper parses the request head,
before Nacelle starts its body bridge or calls the handler. Header-policy
limits therefore do not bound how many bytes Hyper has already read or
allocated. A pending service future can retain body bytes read with the head
even when no body chunks have been delivered to Nacelle.

In the resolved Hyper 1.10.1 dependency, the low-level HTTP/1 builder's
`max_buf_size` accepts a minimum of 8192 bytes. It controls parser buffering,
not a hard per-request plaintext read allowance: a complete head and read-ahead
can exceed that setting. Explicitly setting Hyper's `max_headers` selects its
configurable-count parser path.
Nacelle exposes these separately through validated
`HttpParserLimits::new(max_buffer_bytes, max_headers)` and
`NacelleHttpLimits::with_parser_limits(...)`. `None` or `Some(100)` retains
the default parser path. In the resolved implementation, positive counts
through 100 use inline header arrays; larger counts allocate those arrays.
Buffer values below 8192 are rejected before serving. Keep post-parse byte/count
checks as a separate application policy.

For `Expect: 100-continue`, Hyper waits for the body consumer to request data
before sending `100 Continue`. A head-policy rejection does not start that
consumer or send the interim response. Nacelle sets `Connection: close` on
rejections with an unread body, including a fully buffered body followed by a
pipelined request. It does not drain that body or dispatch the following request.
Rejections of requests whose bodies are already empty may retain keep-alive.

These observations concern plaintext reads and HTTP parsing. They do not bound
TLS records, kernel socket buffers, or total process memory.

## Application admission

Shared servers accept `.with_admission(policy, AdmissionConfig)` where the
policy implements `Admission<HttpAdmissionContext<'_, State, AppState>>`.
Local servers also accept `LocalAdmission<LocalHttpAdmissionContext<...>>`.
The borrowed `HttpRequestHead` contains method, URI, headers, HTTP version, and
effective peer IP. Headers are not cloned for admission. Typed application
state is the same root available to the handler.

Host/method/header policy, effective-peer resolution, rate checks, declared
body-size checks, and runtime request capacity precede custom admission.
A separate finite waiter slot is acquired before policy invocation. Only a
pending policy starts the admission timer; the original future remains pinned.
Before allowance Nacelle neither creates its body channel nor polls `Incoming`
or constructs the handler future. This also applies to chunked bodies.

| Admission outcome | HTTP status | Stable reason |
| --- | --- | --- |
| `Forbidden` | 403 | `admission_forbidden` |
| `RateLimited` | 429 | `admission_rate_limited` |
| `Unavailable` | 503 | `admission_unavailable` |
| Framework waiter exhaustion | 503 | `admission_waiters` |
| Admission deadline | 503 | `admission` |

Responses contain only bounded reasons, retain configured security headers and
response-write deadlines, and close when a body remains unread. The admission
deadline is separate from the handler/body deadlines. Server clones share waiter
accounting; separate listeners/workers do not unless they share the same server.
See [runtime limits](../topics/runtime-limits.md#application-admission-budgets)
for process-wide capacity planning.

Body-producer failures now return directly to the driver, which cancels the
handler rather than awaiting an error send into a full channel. Streaming
request-size violations produce a sanitized 413 response; other body failures
use the generic 500 response. Do not depend on observing producer errors through
`next_body_chunk()` to construct a custom response.
On handler/body failure, Nacelle conservatively closes connections whose
requests initially had a nonempty body, since the pump may have been cancelled.

## Completion and connection retirement

HTTP request-completion telemetry and the runtime request permit finish before
the response body necessarily finishes production. Response-body byte telemetry
is emitted separately when that body completes; cancellation does not record a
partial successful body sample. Neither event acknowledges remote receipt.

An application admission permit has a longer lifetime than the runtime request
gauge: it transfers into the concrete response body after request pumping ends
or is cancelled. It releases on production EOF, response error, or body drop.
Hyper's HEAD/empty/final-content-length paths can drop the body without another
EOF poll; these release the permit as well. Cancellation and write failure drop
that ownership. It is not retained for a later socket flush, and this adds no
boxed callback or dynamically dispatched body.

Hyper's public graceful-shutdown operation disables keep-alive and still needs
the connection to be polled through response production and underlying I/O
flush. It does not expose a public live-idle query covering partial heads,
pipelined input, and pending output. Request counters alone cannot establish
that an HTTP connection is safe to reclaim as idle. Nacelle does not expose
HTTP idle reclamation.

## Peer limits and deployment policy

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
trusted, the leftmost address is used. Malformed or ambiguous elements are
ignored without discarding valid addresses elsewhere in the selected header.
If no valid address remains, resolution falls back to the socket peer. Numeric
IP addresses are required, and duplicate `for` parameters are invalid. Access
logs set `peer_resolution_reason` to `forwarded_malformed` whenever malformed
elements are skipped. Configure every trusted proxy to
sanitize or append to the selected header on every request. Never trust a proxy
that passes that header through unchanged. Request rate limits, request metadata,
and access logs all use this effective identity.

Handler failures produce a generic `500` body (`internal server error`), not
the wrapped error's diagnostic text. Return an explicit sanitized `HttpResponse`
for application errors intended for clients. Failure telemetry still receives
the original error; protect diagnostic logs and observers accordingly.

Leave headroom for Hyper's read-ahead, shared backing allocations, and
TLS/socket buffers. Handlers aggregating a whole chunked body need enough
process or container memory to retain that body.

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
