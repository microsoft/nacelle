# Runtime limits and backpressure

Runtime limits are enforced through `NacelleRuntimeState`. They are intended to
make overload predictable rather than perfectly invisible.

Applications can additionally opt into
[TCP lifecycle policies](../reference/tcp-policies.md) to own admission and
capacity waits at explicit TCP boundaries. This first migration area does not
disable or replace existing budgets, deadlines, or HTTP policy; disabling the
new policy feature leaves the controls documented here active.

Key budgets include:

- active connections
- in-flight requests
- streaming body tasks
- optional per-peer connections
- request and response body size
- core handler timeout
- TCP read, write, final shutdown, and idle timeouts through `NacelleTcpLimits`
- HTTP header, body, write, keep-alive, and connection-age limits through `NacelleHttpLimits`
- TLS handshake timeouts through the TLS config types

The important production habit is to size limits together. A high connection
count with large read and response buffers is a memory budget decision, not just
a concurrency decision.

Call `NacelleLimits::without_max_connections()` to disable only the process-wide
connection ceiling. Connection accounting and configured per-peer limits remain
active.

For configuration details:

Start from `NacelleLimits::default()` and tune shared resource budgets for the
deployment. Use `NacelleTcpLimits` for TCP socket timeouts and
`NacelleHttpLimits` for HTTP edge timeouts and keep-alive behavior. Active
connections, in-flight requests, streaming tasks, body sizes, handler timeouts,
and transport timeouts are bounded by default.

Optional deadlines can be disabled without direct field mutation. Use
`NacelleLimits::without_handler_timeout()`, the TCP
`without_read_timeout()`, `without_write_timeout()`,
`without_shutdown_timeout()`, and `without_idle_timeout()` builders, and the
corresponding HTTP `without_*_timeout()` or
`without_max_connection_age()` builders. Keep bounded defaults for public-edge
listeners unless another layer enforces an equivalent deadline.

TCP idle and read deadlines are independent. The default 120-second idle deadline
bounds waiting for the first byte of a message with an empty input buffer. Once
bytes are available, the 30-second read deadline bounds decoding that message
without resetting on additional bytes. Each subsequent request-body read also
uses the read deadline. Idle time does not include active messages, bodies,
handlers, or response delivery.

In 0.3.2, this replaces the earlier read-or-idle fallback: silent connections now
use the idle deadline rather than the read deadline. Set
`with_idle_timeout(Duration::from_secs(30))` to retain a 30-second silent-connection
limit. `without_idle_timeout()` now permits indefinite waiting for the next
message while still bounding active reads. `without_read_timeout()` leaves idle
waiting bounded but makes active message and body reads unbounded; the idle
deadline no longer protects those reads.

Recommended presets:

- Internal service: keep defaults, set body limits to the largest expected payload, and run behind process supervision.
- Internet-facing behind proxy: cap connections and requests to the container budget, keep 30 second transport timeouts, and let the proxy own coarse traffic filtering or certificate automation when desired.
- Proxy-aware HTTP: trust only known proxy addresses and select the authoritative forwarding header; see [HTTP hardening](../reference/http-hardening.md). `X-Forwarded-For` is the default; `Forwarded` requires explicit selection.
- Direct HTTPS listener: enable `http,rustls`, load certificate/key material through `NacelleTlsConfig`, configure an SNI allowlist with `from_pem_with_allowed_server_names` or `from_der_with_allowed_server_names`, set a short TLS handshake timeout, configure `max_connections_per_peer` and `max_connection_opens_per_peer_per_second`, enable HTTP access logs, and attach `NacelleHttpPolicy` with Host, method, URI, header, security-header, and per-peer request-rate limits.
- Direct TCP Rustls listener: enable `tcp,rustls`, load certificate/key material through `NacelleTlsConfig`, register it with `NacelleApp::tcp_tls(...)`, and keep protocol-level authentication/authorization in the application protocol.
- Direct TCP OpenSSL listener: enable `tcp,openssl`, load certificate/key material through `NacelleOpenSslConfig`, register it with `NacelleApp::tcp_openssl(...)`, and configure the `SslAcceptor` yourself when you need OpenSSL-specific policy.
- Local load-test/autodeploy HTTPS: enable `tls-self-signed` and call `NacelleTlsConfig::self_signed(...)`; do not treat generated certificates as a public trust or rotation strategy.
- High concurrency: reduce TCP buffer capacities before raising `max_connections`, and tune `NacelleTcpLimits` separately from shared resource budgets.

Memory sizing:

This sizing formula requires a finite effective connection ceiling. Use the
configured `max_connections` when it is nonzero. When `max_connections` is the
zero sentinel for unlimited connections, substitute the finite connection
boundary enforced by the proxy, process supervisor, container, or other
external layer.

```text
connection_budget =
  effective_connection_ceiling * (read_buffer_capacity + response_buffer_capacity)
body_budget =
  concurrent_buffered_or_streaming_bodies * max_request_body_bytes
total_budget =
  connection_budget + body_budget + handler/backend/runtime headroom
```

Nacelle does not attempt to account for total process memory. Keep a process or
container memory limit in place and use the sizing formula above when selecting
connection, body, and concurrency limits.

TCP processes requests sequentially per connection.
`request_body_channel_capacity` controls the queued streaming chunks between
the socket reader and handler. HTTP uses Hyper's internal buffers plus Nacelle's
body queue, so leave extra headroom when enabling large request bodies.

For TCP protocols, `NacelleLimits::max_request_body_bytes` is the default body
limit. Override
`Protocol::max_request_body_bytes(request, connection, state, default_limit)` to
choose a per-request limit from the decoded head, immutable connection metadata,
and concrete connection state before body-specific allocation or additional
body reads. There is no dynamically typed connection extension.

With `experimental-thread-per-core`, server factories execute once per
configured worker. Nacelle's global or partitioned runtime counters do not
partition external client pools or backend resources automatically; pass
explicitly shared resources into worker factories when process-wide budgets
must remain global.

Cap Nacelle-owned worker threads after any worker-selection strategy with
`ThreadPerCoreConfig::with_max_threads(...)`. The effective capped worker count
must also be used when constructing `ThreadPerCoreLimits::worker(...)`. Shared
runtime threads belong to the caller's Tokio runtime and are configured there.

Dangerous configurations:

- unbounded connections with large per-connection buffers
- large body limits without a process/container memory limit
- disabled timeouts on internet-facing listeners
- direct internet-facing HTTP without Host/header/method/URI policy
- direct internet-facing TLS without an SNI allowlist
- direct internet-facing listeners without per-peer connection caps
- direct internet-facing listeners without per-peer connection-open rate caps
- direct internet-facing HTTP without per-peer request caps and access logs
- trusting forwarded peer headers without an explicit trusted proxy list
- generated self-signed certificates used as a long-lived public-edge certificate strategy
- high keep-alive connection counts without proxy-level idle limits

TLS certificate rotation:

```rust
let tls = NacelleTlsConfig::from_pem_files("cert.pem", "key.pem")?;
tls.reload_from_pem_files("next-cert.pem", "next-key.pem")?;
```

Reloads affect new TLS handshakes. Existing connections continue with the
configuration negotiated when they connected.

Rustls certificate-only reloads serialize with other reloads/replacements and
preserve the current SNI allowlist. Invalid material leaves the prior snapshot
intact. Explicit `replace_server_config*` APIs install the supplied policy and
clear the stored allowlist for future certificate-only reloads.

`NacelleOpenSslConfig::from_pem_files` uses Mozilla's v5 intermediate profile
and requires TLS 1.2 or newer. TLS 1.3 is enabled when supported by the linked
OpenSSL-compatible library. Legacy protocol policy requires an explicitly
configured acceptor through `from_acceptor`.
