# Runtime limits and backpressure

Runtime limits are enforced through `NacelleRuntimeState`. They are intended to
make overload predictable rather than perfectly invisible.

Key budgets include:

- active connections
- in-flight requests
- streaming body tasks
- optional per-peer connections
- request and response body size
- core handler timeout
- optional application-admission deadline and finite policy waiter capacity
- TCP read, write, final shutdown, and idle timeouts through `NacelleTcpLimits`
- HTTP header, body, write, keep-alive, and connection-age limits through `NacelleHttpLimits`
- TLS handshake timeouts through the TLS config types

The important production habit is to size limits together. A high connection
count with large read and response buffers is a memory budget decision, not just
a concurrency decision.

## Live connection control

`NacelleLimits` contains startup-only request, streaming, body, handler-timeout,
per-peer concurrency, and rate-table capacity settings.
`NacelleRuntimeControl` changes two connection thresholds on the installed
state without restarting listeners or resetting counters.

```rust
use nacelle::core::{NacelleLimits, NacelleRuntimeState};
use nacelle::NacelleApp;

let state = NacelleRuntimeState::new(
    NacelleLimits::default().with_connection_rate_limit_table_capacity(16_384),
);
let app = NacelleApp::new().with_runtime_state(state)?;
let control = app.runtime_control(); // Retain before moving the app into run().
control.set_max_connections(Some(8_000))?;
control.set_connection_open_rate(Some(100))?;
// Register listeners on app, then run it. The same handle remains live.
control.set_max_connections(Some(0))?; // Pause new connections, not existing work.
control.set_max_connections(None)?;   // Unlimited total connections.
control.set_connection_open_rate(None)?; // Disable new rate accounting.
let observed = control.snapshot();
```

The default total is `Some(16_384)` and the default rate is `None`.
`Some(0)` rejects all new connections for a total cap, or all IP-based
admissions for a rate cap. `None` means unlimited total connections or disabled
rate accounting, respectively. Unlimited connections still consume permits and
update the existing gauges. Unix sockets have no invented peer IP: they consume
total capacity but bypass IP-based concurrency and rate checks.

Lowering below occupancy does not evict connections. Admissions already racing
an update may finish with the threshold observed by that check; later checks
read the new value. The order remains total capacity, optional peer concurrency,
then optional peer rate. Earlier rejection does not consume rate budget, and
rate rejection releases previously acquired capacity.

First rate enable allocates the bounded table on the administrative path before
publishing the rate. Changing or disabling the rate retains entries and their
fixed-window clocks. Opens while disabled are not counted retroactively.
Table capacity cannot change at runtime. Zero startup capacity prevents rate
enable. Finite total caps must be below `usize::MAX`; finite rates must also fit
`u32`. Invalid updates return `NacelleError::Configuration` without changing the
threshold.

`state.control()`, app/host `runtime_control()`, and direct-server
`runtime_control()` address their installed state. Use the app/host handle when
that object owns the listeners. A transport's own handle cannot be silently
rebound to the app/host state: installation returns a configuration error
instead. Configure startup state before exporting handles or cloning servers.
Fallible `with_runtime_state(...)` calls permit reinstalling the same state but
reject replacing one still shared by a handle, listener, server clone, or permit.

`startup_limits()` borrows only immutable configuration; `control.snapshot()`
returns owned live thresholds. This replaces the removed connection fields and
setters on `NacelleLimits`, the old `limits()` accessor, and facade `with_limits`
setters. Host `enable_*` methods and runtime-state installation now return
`Result`; propagate configuration failures with `?`.
App `*_dual_stack*` registration also returns `Result`: it installs app runtime
state before cloning the server for IPv4 and IPv6. Select startup state before
registering those listeners.

### Worker accounting

`control.scope()` distinguishes `Shared` from `Partitioned { partitions }`.
`ThreadPerCoreLimits::global(state)` shares one state across workers.
`ThreadPerCoreLimits::worker(startup_limits, count)` returns independent counters
with an aggregate `control()`. Pass that policy into
`LocalTcpRuntimeConfig::new(...)` or `LocalHttpRuntimeConfig::new(...)`; their
fallible constructors check the effective worker count. Obtain the installed
aggregate handle through the resulting config's `runtime_control()`.

Aggregate finite values are process totals, divided by quotient and remainder
in configured worker order. A total below the worker count leaves zero shares,
which reject rather than becoming unlimited. Spare capacity is not borrowed
between workers. Rate-table capacity is also divided at startup, even while
rates are disabled; enabling requires at least one reserved entry per worker.
No table is allocated until enable, and the process budget is not multiplied by
worker count.

Administrative fan-out and aggregate snapshots are serialized; admission on
different workers does not switch instantaneously. Use the aggregate handle,
not independent updates through `state_for(...).control()`. Each worker keeps
its own rate window and counter history. A shared scope identifies one state,
not necessarily the entire process if the caller constructed separate states.

## Startup configuration

### Tracked TCP/Unix connections

Tracking is off by default. Enable a finite registry before installing runtime
state, then obtain the control handle from the app, host, or direct server that
owns that state:

```rust
use nacelle::core::{NacelleConnectionTracking, NacelleLimits, NacelleRuntimeState};
use nacelle::NacelleApp;

let state = NacelleRuntimeState::new(NacelleLimits::default()
    .with_connection_tracking(NacelleConnectionTracking::new(8_000)?));
let app = NacelleApp::new().with_runtime_state(state)?;
let control = app.runtime_control();
control.set_max_connections(Some(8_000))?;
// Register listeners and move app into run(); retain control for administration.
let snapshots = control.connections(64); // None if disabled; otherwise owned metadata.
control.set_max_connections(Some(4_000))?;
let reclaim = control.reclaim_idle(128); // Explicit, best-effort idle selection.
if let Some(connection) = snapshots.and_then(|mut entries| entries.pop()) {
    let lookup = control.connection(connection.connection.connection_id);
    let close = control.close_connection(connection.connection.connection_id);
}
```

The registry capacity is independent of the live connection ceiling and cannot
be resized or enabled retroactively. Zero and unrepresentable capacities are
rejected. Setting the live ceiling to `None` does not bypass registry capacity.
Registration follows the existing connection/peer/rate admission checks;
registry rejection can therefore consume an allowed connection-open rate slot.
Full storage and duplicate live custom IDs report `ConnectionRegistry` and
`DuplicateConnectionId`. No protocol or request work starts after rejection.

TCP, Unix, and their existing shared/local/serial TLS variants register before
controlled setup. Setup, TLS detection/handshake, partial heads, queued input,
admission, bodies, handlers, callbacks, and unflushed output are not idle.
Only the driver establishes idle after the underlying writer has flushed and
no accumulated input or work remains. New input and reclaim compete atomically:
if reclaim wins, that input is not dispatched. Bytes not yet observed by Nacelle
can still be discarded by idle reclaim.

`connection(id)` distinguishes `TrackingDisabled`, `Unknown`, and `Found`.
Snapshots are owned, unordered observations, not live task handles or a stable
pagination mechanism. `Unknown` includes finished IDs and IDs outside a worker
handle's scope. Custom IDs must remain unique across the tracked group and
must not be reused while administrative callers can hold stale IDs.

`close_connection(id)` distinguishes disabled/unknown, `Selected`, and
`AlreadyClosing`. Forced close can cancel setup, active requests, response
delivery, and pending normal shutdown. Selection is not teardown confirmation.
It returns `NacelleError::ConnectionTerminated(Forced)` from direct serving,
does not invoke `Protocol::error_response`, and does not retry writes, flush,
or shutdown. Outstanding completion callbacks resolve as cancelled, not
delivered; already resolved callbacks are not repeated. Peers may have received
partial or complete output. Borrowed serial I/O remains with the caller, who
must not resume the cancelled protocol.

`reclaim_idle(budget)` selects at most `budget` new idle closes toward each
partition's current target. `None` has no gap. Existing closing reservations
are subtracted before selecting more, including across concurrent scans.
Reports include observed occupancy, target, already-closing count, new
selections, and the unfilled gap. Insufficient idle entries or budget leave a
nonzero gap. Lowering a target alone never scans or cancels work.

Connection capacity remains occupied until work/I/O teardown, then the entry
is removed. Setup failure, EOF, error, panic unwinding, cancellation, and task
abort use the same registration owner. Listener `JoinSet` supervision and
stop/drain/abort shutdown remain separate from administrative selection.
HTTP connections do not participate in this registry yet; shared occupancy
can include HTTP connections that cannot be selected.

Worker accounting divides registry capacity at startup, including zero shares
that reject rather than disabling tracking. All partitions share duplicate-ID
detection but retain their own capacity and target gaps. Use the aggregate
handle; spare capacity is not borrowed across workers.

Storage grows on registration, retains its bounded map high-water allocation,
and holds only metadata/signals, not sockets, application state, or tasks.
There is no registry allocation/lock or administrative waiter on tracking-off
connections. Tracked request activity uses atomics, not registry locks or new
request timers/metrics. Snapshot/reclaim scans can inspect the entire allocated
registry even with a small result/selection budget; registration and removal
wait for that lock. Choose capacity and scan frequency together.
See [lifecycle measurements](performance-model.md#connection-tracking-costs).

### Application admission budgets

Use `AdmissionConfig::new(timeout, max_pending)` with one concrete application
policy. Both values must be positive; timeouts must be representable, and
capacity must be below `usize::MAX`. Configure TCP head-read limits before
enabling TCP admission. HTTP uses Hyper's parser bounds, not TCP's exact H/B
allowance; see [HTTP hardening](../reference/http-hardening.md#parser-and-body-boundaries).

The framework acquires request capacity and then a finite waiter slot before
invoking the policy. Waiter accounting covers the policy invocation, including
an immediately ready result, and ends when admission resolves or is cancelled.
Server clones share this count. Independently constructed listeners and worker
factories have independent counts: size their combined waiting-head budget
explicitly. A process-wide backend budget belongs in shared application state,
for example an `Arc<Semaphore>` returning an owned permit.

The admission deadline begins on the first pending poll. On TCP it also covers
flushing prior output. The handler deadline starts only after allowance; body
read and response-write deadlines retain their transport responsibilities.
A blocking policy cannot be preempted by an async timeout. Queue discipline
belongs to the policy: Tokio semaphore FIFO is preserved by awaiting the same
future, but Nacelle does not promise fairness across listeners or workers.

Application permits remain held through body resolution and response
production. They do not extend runtime request metrics to remote delivery.
Silent TCP completion releases after body resolution; HTTP response bodies own
their permits after the service future returns. EOF, response error, early
drop, task cancellation, and suppressed HEAD/empty bodies release ownership.
No application permit is retained just to flush queued output.

For admission-enabled TCP failures, the admission timeout supplies a finite
fallback for error-response writes and closing flush/shutdown when those TCP
timeouts were disabled, including malformed or oversized heads rejected before
request dispatch. Explicit TCP timeouts take precedence. Normal admitted
responses retain the selected transport deadlines. Cancellation during a
prior-response flush abandons that delivery and closes without retrying
partially written bytes or generating another error response.

### Transport and body settings

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
finite cap from `control.snapshot().max_connections`. For `None`, substitute a
finite boundary enforced externally. A lower live cap does not reduce existing
occupancy immediately; size for retained connections as well as future
admissions. Zero pauses admission, not memory use by existing connections.

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
