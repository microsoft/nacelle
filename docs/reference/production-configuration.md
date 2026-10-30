# Production configuration reference

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

Connection thresholds are live: `set_max_connections(Some(n))` sets the total,
and `set_connection_open_rate(Some(n))` sets opens per peer per second.
Zero rejects new admissions; `None` disables the respective ceiling.
Defaults remain 16,384 connections and rate limiting disabled. Startup-only
limits stay in `NacelleLimits`. See [runtime control](../topics/runtime-limits.md#live-connection-control)
for installed handles, retained rate history, update errors, and worker scope.

Recommended presets:

- Internal service: keep defaults, set body limits to the largest expected payload, and run behind process supervision.
- Internet-facing behind proxy: cap connections and requests to the container budget, keep 30 second transport timeouts, and let the proxy own coarse traffic filtering or certificate automation when desired.
- Proxy-aware HTTP: trust only known proxy addresses and select the authoritative forwarding header; see [HTTP hardening](http-hardening.md). `X-Forwarded-For` is the default; `Forwarded` requires explicit selection.
- Direct HTTPS listener: enable `http,rustls`, load certificate/key material through `NacelleTlsConfig`, configure an SNI allowlist with `from_pem_with_allowed_server_names` or `from_der_with_allowed_server_names`, set a short TLS handshake timeout, configure `max_connections_per_peer` and `max_connection_opens_per_peer_per_second`, enable HTTP access logs, and attach `NacelleHttpPolicy` with Host, method, URI, header, security-header, and per-peer request-rate limits.
- Direct TCP Rustls listener: enable `tcp,rustls`, load certificate/key material through `NacelleTlsConfig`, register it with `NacelleApp::tcp_tls(...)`, and keep protocol-level authentication/authorization in the application protocol.
- Direct TCP OpenSSL listener: enable `tcp,openssl`, load certificate/key material through `NacelleOpenSslConfig`, register it with `NacelleApp::tcp_openssl(...)`, and configure the `SslAcceptor` yourself when you need OpenSSL-specific policy.
- Optional TCP OpenSSL listener: enable `experimental-openssl-detection` (which implies `tcp,openssl`), use `serve_tcp_optional_openssl` or the matching host/app builder method, and keep `NacelleTlsDetectionOptions::timeout` short enough for your accepted-connection budget.
- IPv4 plus IPv6 TCP bind: use the `NacelleApp::*_dual_stack(...)` helpers to register separate IPv4 and IPv6 listeners while forcing the IPv6 listener to v6-only mode.
- Unix socket listener: enable `tcp` on Unix and use `NacelleUnixSocketOptions` only when this process owns stale-path cleanup or socket-file permissions.
- Local load-test/autodeploy HTTPS: enable `tls-self-signed` and call `NacelleTlsConfig::self_signed(...)`; do not treat generated certificates as a public trust or rotation strategy.
- High concurrency: reduce TCP buffer capacities before raising `max_connections`, and tune `NacelleTcpLimits` separately from shared resource budgets.

The OpenSSL PEM helper uses Mozilla's v5 intermediate profile with a TLS 1.2
minimum. TLS 1.3 is enabled when supported by the linked OpenSSL-compatible
library. Custom acceptors own their own protocol policy.

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

TCP processes requests sequentially per connection.
`request_body_channel_capacity` controls the queued streaming chunks between
the socket reader and handler. HTTP uses Hyper's internal buffers plus Nacelle's
body queue, so leave extra headroom when enabling large request bodies. See
[runtime limits](../topics/runtime-limits.md) for memory sizing and TLS reload
semantics.

For TCP protocols, `NacelleLimits::max_request_body_bytes` is the default body
limit. Override
`Protocol::max_request_body_bytes(request, connection, state, default_limit)`
when the decoded request head, immutable connection metadata, or concrete
`Protocol::ConnectionState` should choose a stricter phase-specific cap. The
hook runs before body-specific allocation or additional body reads; normal
decoder read-ahead may already have placed bytes in the connection buffer.

Set `NacelleTcpConfig::with_head_read_limits(TcpHeadReadLimits::new(H, B)?)`
when that pre-check read-ahead must be bounded. `H` caps encoded head bytes and
`B` caps cumulative undecided plaintext, including inherited body/next-request
prefixes; require `0 < H <= B <= isize::MAX`. The reference protocol uses `H = 24`.
The default is `None`, retaining existing batching. Choose `B` for the workload:
smaller values reduce plaintext prefetch but can require more reads.

This is not a process memory cap: buffer capacity, shared decoded allocations,
decoder-owned memory, TLS transport buffers (including decrypted record data),
and kernel buffers are excluded. Existing
body/frame limits still govern admitted bodies. See
[bounded head reads](protocol.md#bounded-head-reads) for EOF behavior, stable
rejection reasons, and the decoder allocation contract.

`NacelleTcpOptions` controls accepted TCP stream behavior. Defaults preserve the
existing behavior: `TCP_NODELAY` enabled and TCP keepalive disabled.
`NacelleTcpBindOptions` adds listener bind controls such as IPv6-only mode for
APIs that need explicit family behavior.

TCP response delivery defaults to `ResponseWritePolicy::Immediate`.
`CoalesceBuffered` uses `response_buffer_capacity` as its threshold, while the
configuration builder normalizes `FlushAtBytes(0)` to `FlushAtBytes(1)`. Only
complete frames enter the pending batch; the runtime flushes before another
socket read, before awaiting another streaming body chunk, and when the
threshold is reached or crossed. Read-boundary flushes include the underlying
`AsyncWrite`, and clean connection teardown performs a writer shutdown bounded
by `NacelleTcpLimits::shutdown_timeout`; terminal error paths make the same
bounded attempt. This delivers buffered TLS records and permits a TLS
`close_notify`.

`NacelleTcpLimits` controls TCP socket read, socket write, final writer shutdown,
and idle timeouts. `idle_timeout` defaults to 120 seconds and bounds waiting for
the first byte of a message with an empty input buffer, including the first
message on a connection. `read_timeout` defaults to 30 seconds and bounds
completion of a decoded message once bytes are available. Buffered partial
messages enter the read phase immediately; additional bytes do not restart its
deadline. Subsequent request-body reads each use `read_timeout`, never
`idle_timeout`. Idle expiry reports `NacelleTimeoutReason::Idle`; message and
body read expiry report `TcpRead` and `RequestBodyRead`, respectively.

Neither timeout is a fallback for the other. Idle time excludes handler execution
and response delivery. Shutdown uses its own deadline so finalization policy can
be tuned independently of ordinary response delivery. The corresponding
`without_*_timeout()` builders make an explicitly unbounded policy possible.
`NacelleHttpLimits` controls HTTP header read, request body read, response
write, keep-alive, and max connection age behavior on `HyperServer`. Its
`without_*_timeout()` and `without_max_connection_age()` builders disable those
optional deadlines; `NacelleLimits::without_handler_timeout()` disables the
core handler deadline.

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
