# Configure production limits

Start from `NacelleLimits::default()` and tune shared resource budgets for the
deployment. Use `NacelleTcpLimits` for TCP socket timeouts and
`NacelleHttpLimits` for HTTP edge timeouts and keep-alive behavior. Active
connections, in-flight requests, streaming tasks, body sizes, handler timeouts,
and transport timeouts are bounded by default.

`state.control().set_max_connections(None)?` disables that state's connection
admission ceiling. Active connections are still counted for telemetry, and any
configured per-peer connection or connection-open rate limits still apply. Use
this only when another layer enforces an appropriate connection and memory
boundary.

Set finite live thresholds with `set_max_connections(Some(n))` and
`set_connection_open_rate(Some(n))`. Zero rejects new admissions without evicting
existing connections. Obtain the app/host handle before serving and retain it
for updates; see [runtime control](../topics/runtime-limits.md#live-connection-control)
for startup configuration, update errors, and aggregate worker scope.

For explicit TCP/Unix retirement, opt into
`NacelleLimits::with_connection_tracking(NacelleConnectionTracking::new(n)?)`
before installation. Set a registry capacity appropriate to the deployment,
retain the installed control, and prefer `reclaim_idle(budget)` over forced
close when active work must be preserved. Tracking is off by default and does
not manage HTTP connections. See [tracking configuration and outcomes](../topics/runtime-limits.md#tracked-tcpunix-connections).

Recommended presets:

- Internal service: keep defaults, set body limits to the largest expected payload, and run behind process supervision.
- Internet-facing behind proxy: cap connections and requests to the container budget, keep 30 second transport timeouts, and let the proxy own coarse traffic filtering or certificate automation when desired.
- Proxy-aware HTTP: trust only known proxy addresses and select the authoritative forwarding header; see [HTTP hardening](harden-http.md). `X-Forwarded-For` is the default; `Forwarded` requires explicit selection.
- Direct HTTPS listener: enable `http,rustls`, load certificate/key material through `NacelleTlsConfig`, configure an SNI allowlist with `from_pem_with_allowed_server_names` or `from_der_with_allowed_server_names`, set a short TLS handshake timeout, configure `max_connections_per_peer` and `max_connection_opens_per_peer_per_second`, enable HTTP access logs, and attach `NacelleHttpPolicy` with Host, method, URI, header, security-header, and per-peer request-rate limits.
- Direct TCP Rustls listener: enable `tcp,rustls`, load certificate/key material through `NacelleTlsConfig`, register it with `NacelleApp::tcp_tls(...)`, and keep protocol-level authentication/authorization in the application protocol.
- Direct TCP OpenSSL listener: enable `tcp,openssl`, load certificate/key material through `NacelleOpenSslConfig`, register it with `NacelleApp::tcp_openssl(...)`, and configure the `SslAcceptor` yourself when you need OpenSSL-specific policy.
- Optional TCP OpenSSL listener: enable `experimental-openssl-detection` (which implies `tcp,openssl`) and use `serve_tcp_optional_openssl(...)` or the matching host/app builder method when one listener must accept both plain and TLS clients; keep `NacelleTlsDetectionOptions::timeout` short enough to avoid tying up idle accepted connections.
- IPv4 plus IPv6 TCP bind: use the `NacelleApp::*_dual_stack(...)` helpers to register separate IPv4 and IPv6 listeners while forcing the IPv6 listener to v6-only mode.
- Unix socket listener: enable `tcp` on Unix and call `NacelleApp::unix_socket(...)`; use `NacelleUnixSocketOptions` only when this process owns stale-path cleanup or socket-file permissions.
- Local load-test/autodeploy HTTPS: enable `tls-self-signed` and call `NacelleTlsConfig::self_signed(...)`; do not treat generated certificates as a public trust or rotation strategy.
- High concurrency: reduce TCP buffer capacities before raising `max_connections`, and tune `NacelleTcpLimits` separately from shared resource budgets.

Memory sizing:

This sizing formula requires a finite effective connection ceiling. Use the
finite cap from `control.snapshot().max_connections`; for `None`, substitute
an externally enforced finite boundary. Lowering a live cap does not evict
existing connections, so also budget for retained occupancy.

```text
connection_budget =
  effective_connection_ceiling * (read_buffer_capacity + response_buffer_capacity)
body_budget =
  concurrent_buffered_or_streaming_bodies * max_request_body_bytes
total_budget =
  connection_budget + body_budget + handler/backend/runtime headroom
```

Nacelle leaves memory governance to the application, runtime, process
supervisor, or container. Use the calculated budget to select those external
limits and to tune connection, body, and concurrency limits together.

TCP processes requests sequentially per connection. `request_body_channel_capacity` controls the queued streaming chunks between the socket reader and handler. HTTP uses Hyper's internal buffers plus Nacelle's body queue, so leave extra headroom when enabling large request bodies.

For TCP protocols, `NacelleLimits::max_request_body_bytes` is the default body
limit. Override
`Protocol::max_request_body_bytes(request, connection, state, default_limit)`
when the decoded request head, immutable connection metadata, or concrete
`Protocol::ConnectionState` should choose a stricter phase-specific cap. The
hook runs before body-specific allocation or additional body reads; normal
decoder read-ahead may already have placed bytes in the connection buffer.

Use `NacelleTcpOptions` for accepted TCP stream behavior. Defaults preserve the
existing behavior: `TCP_NODELAY` enabled and TCP keepalive disabled. Enable
keepalive deliberately per deployment target because OS defaults and supported
fields vary. `NacelleTcpBindOptions` adds listener bind controls such as
IPv6-only mode for APIs that need explicit family behavior.

`NacelleTcpConfig::response_write_policy` defaults to
`ResponseWritePolicy::Immediate`. Select `CoalesceBuffered` or
`FlushAtBytes(n)` only for measured workloads with already-buffered request
bursts. Coalescing preserves complete-frame order and flushes before waiting for
more socket input; streaming responses flush before awaiting the next body
chunk. Larger thresholds can delay earlier responses until a threshold or batch
boundary and apply the write timeout to the complete queued batch. Size the base
buffer near a measured batch size when using larger thresholds.

Use `NacelleTcpLimits` for TCP socket read, socket write, final writer shutdown,
and idle timeouts. `idle_timeout` (120 seconds by default) bounds waiting for the
first byte of a message when the input buffer is empty. Once input is available,
`read_timeout` (30 seconds by default) bounds completion of the decoded message;
it also bounds each subsequent request-body read. Additional message bytes do
not restart the message deadline. These limits apply independently, including
when one is disabled. Set `shutdown_timeout` independently when finalization
needs a shorter deadline than ordinary response delivery. Disable these only through
the corresponding `without_*_timeout()` builders when an explicitly unbounded
policy is required.
Use `NacelleHttpLimits` on `HyperServer` for HTTP header read, request body
read, response write, keep-alive, and max connection age behavior. Use its
`without_*_timeout()` and `without_max_connection_age()` builders to disable
optional deadlines, and `NacelleLimits::without_handler_timeout()` for handler
execution.

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
