# Static admission policies

Static policies let applications make admission decisions at transport-owned
checkpoints without changing handler types. Enable `policies` on the facade
alongside the transports you use, or on `nacelle-tcp` / `nacelle-http` directly.
The facade feature does not select a transport. Existing limits, HTTP edge
hardening, and [legacy TCP lifecycle policies](tcp-policies.md) remain active.

Initialize policy dependencies before starting listeners. Register concrete
policies in `TcpPolicies` or `HttpPolicies`, then attach the completed set to
the server. Repeated checkpoint registrations compose in registration order,
not replacement order. The server invokes hooks automatically; handlers do not
initialize, invoke, or carry policy types.

The checked-in `examples/nacelle-examples/src/bin/policy_echo.rs` shares one
peer-rate budget and one bounded backend-admission policy between TCP and HTTP.
Its TCP opcode predicate runs before rate admission; backend leases cover handler
execution only:

```sh
cargo run -p nacelle-examples --features http,policies --bin policy_echo -- 127.0.0.1:8080 127.0.0.1:8081
```

Attach TCP sets with builder `.policies(set)` or server `.with_policies(set)`;
attach HTTP sets with `.with_policies(set)`. These methods are available only
before a set is attached, preventing silent replacement. TCP shared and serial
head inputs expose their respective connection-state storage (`Arc<State>` or
`State`). TCP accepted/transport-ready policies receive `TcpTransport`, with
read-only connection metadata. HTTP uses `HttpRequestHead<State>`, including its
resolved `peer_ip()` and read-only `state()`. `HttpHandlerInput<State>` also
exposes connection state. Worker-local HTTP policies use `LocalHttpRequestHead`
and `LocalHttpHandlerInput`; register local hooks with `.on_local_*` or
`.around_local_handler`.

HTTP bootstrap helpers can return `impl HttpPolicySet<State>` or
`impl LocalHttpPolicySet<State>` directly. TCP helpers use
`SharedTcpPolicySet<Request, Storage>` or `LocalTcpPolicySet<Request, Storage>`
from `nacelle::tcp::policies`. Callers do not need to name the checkpoint chains
or response-lease adapter types.

| Registration | Checkpoint |
| --- | --- |
| `.on_socket(...)` | Synchronous admission before spawning a connection task. |
| `.on_accepted(...)` | Admission before TLS negotiation. |
| `.on_transport_ready(...)` | Admission after transport negotiation. |
| `.before_request(...)` | TCP-only admission before reading or decoding the next head. |
| `.on_request_head(...)` | Validated head admission before additional Nacelle body work. |
| `.around_handler(...)` | Admission immediately before handler invocation. |

TCP `serve_io*` callers own acceptance and TLS, so those entry points do not
visit socket, accepted, or transport-ready checkpoints. Hyper owns HTTP parsing;
HTTP has no pre-decode `.before_request(...)` hook.

## Custom decisions and shared algorithms

`nacelle::core::policy::Admission<Input>` returns a concrete `Send` future with
either a lease or `Rejection`. `LocalAdmission` allows worker-local state and
non-`Send` futures. Use `Shared(policy)` to adapt a shared policy to a local
checkpoint, `check_fn` / `local_check_fn` for synchronous predicates, and
`map_input` / `local_map_input` to extract owned facts for reusable algorithms.
Input views expose read access; their exclusive reborrowing permits sequential
composition without requiring every request or serial connection state to be
`Sync`.

A policy may inspect arbitrary typed request fields and application dependencies.
It must not block an executor thread, bypass mandatory transport bounds, or retain
borrowed request inputs in its lease. Consumer-defined async policies implement
the trait directly; no macro, boxed future, registry, or extension map is needed.

`RequestRate` adapts the existing bounded fixed-window peer limiter. Initialize
one `Arc<NacellePeerRateLimiter>` and use the same table and limit in TCP and
HTTP adapters for one combined budget. HTTP keys must use the resolved effective
peer, not an unchecked forwarding header. The supplied policy rejects a missing
peer; applications may implement another explicit identity rule for Unix sockets.
Cloning a policy shares its budget; recreating it inside every worker factory
creates independent budgets instead.

`Chain` evaluates policies sequentially and stops at the first rejection.
Later projections and hooks do not run after rejection. A rate charge counts an
admission attempt and is not refunded when a later policy rejects.

## Leases, waiting, and cancellation

Return `()` for a decision-only policy. Return `Hold::new(guard)` to retain a
custom owned resource guard; its `Drop` must release capacity synchronously.
Composite leases drop in reverse acquisition order, including on failure or
cancellation.

Lease lifetimes belong to the checkpoint, not the guard implementation:
connection admission covers the connection, handler admission covers handler
execution, and head admission covers request processing and the transport-owned
response lifetime. TCP response-scoped resource leases require a local flush
before release and can reduce response coalescing. Ready decision-only policies
do not require that delivery barrier. HTTP response leases end on body
EOF/error/drop; EOF is not proof of socket flush or peer receipt.

A pending admission future applies backpressure at its checkpoint. The transport
retains and repolls that future rather than charging again after each wakeup or
fragment. Use `bounded_wait(policy, max_wait, max_waiters)` to bound both waiting
time and waiter count. The decorator first polls the policy; immediately ready
checks do not create a deadline. Saturated waiter capacity rejects immediately.
Existing finite connection/request bounds must also cover waiting work.

Policies run before the corresponding I/O or handler operation, so those
operation deadlines are not policy deadlines. Acquire resources in a consistent
order; do not await capacity whose release depends on resources already held by
the same policy chain.

`Cancellation` is an explicit opt-in group handle for ejecting active work.
Attach it with `.with_cancellation(handle.clone())` on the policy set; application
monitoring calls `handle.cancel()` when the group must be ejected. Use separate
handles or children for independently cancellable groups. Cancellation closes
transport-owned work and releases its leases; it does not undo application side
effects, cancel detached application tasks, or reclaim retained `Bytes`. No
cancellation handle, timer, or policy task is installed for an empty set.

`RejectionKind` separates rate rejection, overload, denied predicates, closed
admission, and timeout. Reasons are static diagnostic labels and must not contain
secrets or peer-controlled values. HTTP maps categories to rejection status codes;
TCP uses its existing protocol error-response boundary. Socket and early
connection rejection close the connection without a protocol response. After
response commitment or during eviction, the transport closes rather than sending
a second response.

## Performance and body boundaries

Dispatch, futures, and lease storage are concrete. The design avoids policy
registry lookups, framework boxing, and per-hook tasks. Empty sets and ready
predicates require no policy heap allocation. Enabled policies still pay for
their own comparisons, shared accounting, clock reads, waits, and guard drops.
Static composition can increase future size and generated code size. Use the
[performance comparison guide](../how-to/compare-performance.md) rather than
assuming that static dispatch proves an overhead number.

These policies do not provide deferrable bodies or a resident-memory bound.
Decoded-head admission precedes additional Nacelle body work, but TCP decoders,
Hyper, TLS, and the kernel can already hold read-ahead. Current body drivers
still pump input. Future reservation-before-read/growth checkpoints must track
buffer ownership, unknown HTTP lengths, queued chunks, and allocations retained
by cloned `Bytes`. Request leases alone do not account for that memory.
