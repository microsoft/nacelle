# Nacelle TCP Proxy Example

This example demonstrates application composition around a Nacelle Rustls TCP
listener. It accepts Nacelle's unpublished length-delimited reference protocol,
forwards each request to a plain TCP backend, and returns the correlated
response. It is intentionally a small teaching proxy, not a transparent or
production-ready gateway.

## Run The Example

From the repository root, start a plain TCP backend:

```bash
cargo run -p nacelle-examples --bin echo -- 127.0.0.1:8080
```

Start the TLS proxy in another terminal:

```bash
cargo run -p nacelle-proxy
```

The default feature creates an ephemeral self-signed certificate for local
development. Send traffic through the proxy with the existing stress client:

```bash
cargo run -p nacelle-stress-test -- \
	--addr 127.0.0.1:8443 \
	--connections 1 \
	--pipeline 1 \
	--duration-secs 1 \
	--payload-bytes 16 \
	--tls-insecure
```

`--tls-insecure` disables certificate verification and is appropriate only for
this local self-signed workflow. To run without self-signed support, provide
both PEM paths in a configuration file and use:

```bash
cargo run -p nacelle-proxy \
	--no-default-features --features tls -- path/to/proxy.toml
```

When no path argument is supplied, the binary uses the example configuration
next to this README, independent of the current working directory.

## Reload Model

The watcher checks the TOML file and referenced certificate files once per
second. Invalid or partially deployed changes leave the last known-good state
active, and identical rejection messages are suppressed until the observed
condition changes.

These settings apply to requests that start after a successful reload:

- backend address
- connect timeout
- complete upstream I/O deadline
- maximum upstream response size

Listener, handler, connection, concurrency, Nacelle memory, and request/response
hard limits are startup-only because changing them requires rebuilding runtime
state. `ProxyService` snapshots runtime settings at request start, so in-flight
requests continue with their original values.

TLS identity replacement is validated before runtime settings are swapped, but
the two updates are sequential rather than one atomic transaction. New TLS
handshakes use the replacement identity; established TLS connections are
unchanged. Deploy TOML, certificate, and key files by writing temporary files
and atomically renaming each into place. A certificate/key pair cannot be
replaced atomically as a group, so a mixed pair may be rejected for one polling
cycle before the complete pair becomes active.

## Resource And Timeout Model

The checked-in profile sets explicit total connections, per-peer connections,
in-flight requests, body sizes, handler timeout, and Nacelle-managed memory.
`max_memory_bytes` accounts for Nacelle transport allocations; it is not a
total RSS limit and does not include every allocation made by this example.
Use a process or container memory limit for a hard deployment boundary.

This proxy fully buffers each inbound request and complete upstream response.
The connection and in-flight limits therefore need to be sized together with
both body bounds. `connect_timeout_ms` covers only TCP connection establishment.
`upstream_io_timeout_ms` is one deadline for writing the request and reading all
response frames. `handler_timeout_ms` is the outer Nacelle handler deadline.

## Benchmarks

Run the loopback upstream RTT benchmark with:

```bash
cargo bench -p nacelle-proxy --bench simple_rtt
```

`simple_rtt` measures one 64-byte `ReferenceProtocolClient` exchange, including
TCP connection establishment, request framing, socket writes and reads, and
correlated response parsing. It intentionally reflects the example's one
upstream connection per request design. It does not include the inbound Rustls
listener or Nacelle handler; use the `--pipeline 1` stress-client workflow above
when measuring the complete running application.

Run the socket-free protocol benchmark with:

```bash
cargo bench -p nacelle-proxy --bench protocol_efficiency
```

`protocol_efficiency` encodes and decodes batches of 64 frames with 256-byte
payloads. Criterion reports useful payload throughput, while each frame also
carries the protocol's 24-byte header. The benchmark includes the current
per-frame allocation and body extraction behavior but excludes socket and task
scheduling costs.

Compare results only when the machine, Rust toolchain, feature set, benchmark
profile, and workload are the same. Treat local results as regression signals,
not deployment capacity claims.

## Trust Boundaries And Omissions

- Client-to-proxy traffic uses Rustls; proxy-to-backend traffic is plaintext.
- The example provides no authentication, authorization, or backend identity verification.
- It opens one upstream connection per request and has no bounded connection pool.
- It has no retries, load balancing, health checks, circuit breaking, or graceful backend draining.
- It rewrites requests into one start/end frame and does not preserve inbound frame boundaries.
- It buffers payloads rather than streaming them with end-to-end backpressure.
- It uses console output rather than structured, low-cardinality operational telemetry.
- `nacelle-reference-protocol` is an example fixture, not a published general-purpose proxy protocol.

These omissions are deliberate so the example stays focused on application
boundaries, owned errors, bounded inputs, lifecycle supervision, runtime
configuration, and certificate rotation.

## Module Boundaries

- `main.rs` contains only the executable entry point.
- `app.rs` owns lifecycle and Nacelle assembly.
- `config.rs` owns the file schema, validation, path resolution, and TLS snapshots.
- `service.rs` owns `ProxyService` and its reloadable application state.
- `reload.rs` owns file watching, startup-only policy, and last-known-good updates.
- `wire.rs` owns protocol selection, framing, upstream socket I/O, and wire tests.
- `error.rs` owns the public proxy error boundary.
- `lib.rs` is the crate facade and contains no implementation behavior.

## Error Boundary

Public application and service APIs return `ProxyError`, not `NacelleError`.
Callers can branch on stable `ProxyErrorKind` categories while the concrete I/O,
TLS, task, and Nacelle runtime errors remain private chained sources:

```rust
match ProxyApp::from_env().await?.run().await {
    Ok(()) => {}
    Err(error) if error.kind() == ProxyErrorKind::Configuration => {
        eprintln!("proxy configuration rejected: {error}");
    }
    Err(error) => return Err(error),
}
```

The private Nacelle handler adapter converts `ProxyError` into the transport's
required handler error only after application handling crosses back into the
framework.
