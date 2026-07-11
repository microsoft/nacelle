# Production configuration reference

Start from `NacelleLimits::default()` and tune shared resource budgets for the
deployment. Use `NacelleTcpLimits` for TCP socket timeouts and
`NacelleHttpLimits` for HTTP edge timeouts and keep-alive behavior. Active
connections, in-flight requests, streaming tasks, body sizes, handler timeouts,
and transport timeouts are bounded by default. Memory allocation budgeting is opt-in: set
`max_memory_bytes` only after measuring that the limiter behaves correctly for
your service.

Recommended presets:

- Internal service: keep defaults, set body limits to the largest expected payload, and run behind process supervision.
- Internet-facing behind proxy: cap connections and requests to the container budget, keep 30 second transport timeouts, and let the proxy own coarse traffic filtering or certificate automation when desired.
- Proxy-aware HTTP: configure `NacelleHttpPolicy::with_trusted_proxy_ips(...)` only with known proxy addresses before allowing `Forwarded` or `X-Forwarded-For` to affect per-peer request limits or request metadata.
- Direct HTTPS listener: enable `http,tls`, load certificate/key material through `NacelleTlsConfig`, configure an SNI allowlist with `from_pem_with_allowed_server_names` or `from_der_with_allowed_server_names`, set a short TLS handshake timeout, configure `max_connections_per_peer` and `max_connection_opens_per_peer_per_second`, enable HTTP access logs, and attach `NacelleHttpPolicy` with Host, method, URI, header, security-header, and per-peer request-rate limits.
- Direct TCP Rustls listener: enable `tcp,tls`, load certificate/key material through `NacelleTlsConfig`, register it with `NacelleApp::tcp_tls(...)`, and keep protocol-level authentication/authorization in the application protocol.
- Direct TCP OpenSSL listener: enable `tcp,openssl`, load certificate/key material through `NacelleOpenSslConfig`, register it with `NacelleApp::tcp_openssl(...)`, and configure the `SslAcceptor` yourself when you need OpenSSL-specific policy.
- Optional TCP OpenSSL listener: enable `tcp,openssl`, use `serve_tcp_optional_openssl` or the matching host/app builder method, and keep `NacelleTlsDetectionOptions::timeout` short enough for your accepted-connection budget.
- IPv4 plus IPv6 TCP bind: use the `NacelleApp::*_dual_stack(...)` helpers to register separate IPv4 and IPv6 listeners while forcing the IPv6 listener to v6-only mode.
- Unix socket listener: enable `tcp` on Unix and use `NacelleUnixSocketOptions` only when this process owns stale-path cleanup or socket-file permissions.
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

Set `NacelleLimits::with_max_memory_bytes(...)` when you want Nacelle to enforce
the calculated budget. Without an explicit memory limit, Nacelle still enforces
connection/request/body limits and transport-owned timeouts but leaves total memory governance to the
application, runtime, process supervisor, or container.
When the memory budget is full, request body allocations wait in FIFO order and
time out after `NacelleLimits::memory_allocation_timeout` (`5s` by default).
Tune this with `with_memory_allocation_timeout(...)`, or call
`NacelleRuntimeState::memory_budget()` when application code needs to allocate from
the same budget as the transports.

TCP processes requests sequentially per connection. `request_body_channel_capacity` controls the queued streaming chunks between the socket reader and handler. HTTP uses Hyper's internal buffers plus Nacelle's body queue, so leave extra headroom when enabling large request bodies.

For TCP protocols, `NacelleLimits::max_request_body_bytes` is the default body
limit. Override `Protocol::max_request_body_bytes(request, connection, default_limit)`
when the decoded request head or immutable connection metadata should choose a
stricter phase-specific cap before Nacelle buffers or streams the body. Keep
mutable protocol state in the concrete `Protocol::ConnectionState` exposed to handlers.

`NacelleTcpOptions` controls accepted TCP stream behavior. Defaults preserve the
existing behavior: `TCP_NODELAY` enabled and TCP keepalive disabled.
`NacelleTcpBindOptions` adds listener bind controls such as IPv6-only mode for
APIs that need explicit family behavior.

`NacelleTcpLimits` controls TCP socket read, socket write, and idle timeouts.
`NacelleHttpLimits` controls HTTP header read, request body read, response
write, keep-alive, and max connection age behavior on `HyperServer`.

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
- long TLS detection timeouts on optional TLS listeners
- Unix stale-path cleanup for a socket path not exclusively owned by this process

TLS certificate rotation:

```rust
let tls = NacelleTlsConfig::from_pem_files("cert.pem", "key.pem")?;
tls.reload_from_pem_files("next-cert.pem", "next-key.pem")?;
```

Reloads affect new TLS handshakes. Existing connections continue with the
configuration negotiated when they connected.

OpenSSL builds need native OpenSSL development files. The `openssl-vendored`
feature can build OpenSSL from source, but that build requires Perl on Windows.
