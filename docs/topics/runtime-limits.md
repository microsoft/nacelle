# Runtime limits and backpressure

Runtime limits are enforced through `NacelleRuntimeState`. They are intended to
make overload predictable rather than perfectly invisible.

Key budgets include:

- active connections
- in-flight requests
- streaming body tasks
- optional per-peer connections
- memory budget allocations
- request and response body size
- core handler timeout
- TCP read, write, and idle timeouts through `NacelleTcpLimits`
- HTTP header, body, write, keep-alive, and connection-age limits through `NacelleHttpLimits`
- TLS handshake timeouts through the TLS config types

The important production habit is to size limits together. A high connection
count with large read and response buffers is a memory budget decision, not just
a concurrency decision.

For configuration details:

Start from `NacelleLimits::default()` and tune shared resource budgets for the
deployment. Use `NacelleTcpLimits` for TCP socket timeouts and
`NacelleHttpLimits` for HTTP edge timeouts and keep-alive behavior. Active
connections, in-flight requests, streaming tasks, body sizes, handler timeouts,
and transport timeouts are bounded by default. Memory allocation budgeting is opt-in: the default
`max_memory_bytes` is `usize::MAX`, which disables Nacelle memory-budget
enforcement until you set an explicit byte limit.

Recommended presets:

- Internal service: keep defaults, set body limits to the largest expected payload, and run behind process supervision.
- Internet-facing behind proxy: cap connections and requests to the container budget, keep 30 second transport timeouts, and let the proxy own coarse traffic filtering or certificate automation when desired.
- Proxy-aware HTTP: configure `NacelleHttpPolicy::with_trusted_proxy_ips(...)` only with known proxy addresses before allowing `Forwarded` or `X-Forwarded-For` to affect per-peer request limits or request metadata.
- Direct HTTPS listener: enable `http,tls`, load certificate/key material through `NacelleTlsConfig`, configure an SNI allowlist with `from_pem_with_allowed_server_names` or `from_der_with_allowed_server_names`, set a short TLS handshake timeout, configure `max_connections_per_peer` and `max_connection_opens_per_peer_per_second`, enable HTTP access logs, and attach `NacelleHttpPolicy` with Host, method, URI, header, security-header, and per-peer request-rate limits.
- Direct TCP Rustls listener: enable `tcp,tls`, load certificate/key material through `NacelleTlsConfig`, register it with `NacelleApp::tcp_tls(...)`, and keep protocol-level authentication/authorization in the application protocol.
- Direct TCP OpenSSL listener: enable `tcp,openssl`, load certificate/key material through `NacelleOpenSslConfig`, register it with `NacelleApp::tcp_openssl(...)`, and configure the `SslAcceptor` yourself when you need OpenSSL-specific policy.
- Local load-test/autodeploy HTTPS: enable `tls-self-signed` and call `NacelleTlsConfig::self_signed(...)`; do not treat generated certificates as a public trust or rotation strategy.
- High concurrency: reduce TCP buffer capacities before raising `max_connections`, and tune `NacelleTcpLimits` separately from shared resource budgets.

Memory budget:

```text
connection_budget =
  max_connections * (read_buffer_capacity + response_buffer_capacity)
body_budget =
  concurrent_buffered_or_streaming_bodies * max_request_body_bytes
total_budget =
  connection_budget + body_budget + handler/backend/runtime headroom
```

When memory limiting is enabled with `NacelleLimits::with_max_memory_bytes(...)`,
Nacelle allocates from that budget for connection buffers and buffered or
streaming request bodies. The limiter accounts for Nacelle-managed allocations,
not total process RSS, so keep process or container memory limits in place.
Request body allocations wait in FIFO order when the budget is full. The default
wait limit is `NacelleLimits::memory_allocation_timeout == Some(5s)`, and can
be tuned with `with_memory_allocation_timeout(...)` or disabled with
`without_memory_allocation_timeout()`. A timed-out waiter returns
`NacelleError::Timeout("memory_allocation")`.

The memory budget is an accounting guard, not a buffer allocator: it grants a
`NacelleMemoryAllocation` that tracks bytes the transport or application intends to
hold elsewhere, and releases those bytes when the guard is dropped.
Applications can allocate from the same budget through
`NacelleRuntimeState::memory_budget()`. Use `try_allocate(...)` for immediate
admission, `allocate(...)` for FIFO waiting, or
`allocate_with_timeout_and_shutdown(...)` when app work should stop waiting
during shutdown.

TCP processes requests sequentially per connection. `request_body_channel_capacity` controls the queued streaming chunks between the socket reader and handler. HTTP uses Hyper's internal buffers plus Nacelle's body queue, so leave extra headroom when enabling large request bodies.

For TCP protocols, `NacelleLimits::max_request_body_bytes` is the default body
limit. Override
`Protocol::max_request_body_bytes(request, connection, state, default_limit)` to
choose a per-request limit from the decoded head, immutable connection metadata,
and concrete connection state before body-specific allocation or additional
body reads. There is no dynamically typed connection extension.

Thread-per-core server factories execute once per configured worker. Nacelle's
global or partitioned runtime counters do not partition external client pools or
backend resources automatically; pass explicitly shared resources into worker
factories when process-wide budgets must remain global.

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
