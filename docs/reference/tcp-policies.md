# TCP lifecycle policies

TCP lifecycle policies let an application admit, reject, or wait for its own
capacity at explicit transport and request boundaries. Enable the non-default
`tcp-policies` facade feature, or `policies` when depending directly on
`nacelle-tcp`.

Attach a concrete `TcpLifecyclePolicy<Request, ConnectionState>` with
`protocol.with_policy(policy)`, then use the resulting `WithTcpPolicy` as the
server's protocol type. The decoder, request head, response type, connection
state, body modes, and response/error encoder remain those of the wrapped
protocol. Handler contexts refer to the wrapped protocol type. Wrapping a
protocol again replaces its lifecycle policy rather than composing both
policies; implement composition in the application policy when needed.

The policy trait, wrapper, hook calls, and policy permit storage are compiled
only with this feature. Dispatch and futures are concrete; Nacelle does not box
policy futures or allocate a policy registry. Enabling the feature without
attaching a policy uses the unit no-op implementation.

## Checkpoints

| Hook | Boundary | Permit lifetime |
| --- | --- | --- |
| `accepted` | Accepted socket, before TLS detection or handshake | Connection task |
| `transport_ready` | Ready transport, before connection buffers and protocol state | Connection serving future |
| `before_request` | Before reading/decoding the next head, including buffered input | Request and response delivery, or EOF/error |
| `request_head` | Decoded head, before body-specific allocation or further reads | Request and response delivery |
| `body_complete` | Successful completion of body input | Rest of request and response delivery |

All listener-owned sockets visit `accepted`, including shared, serial,
worker-local, required TLS, optional plaintext/OpenSSL, and Unix listeners.
It executes inside the connection task so waiting in one application policy
does not block the accept loop. Existing connection admission occurs before
that task is spawned. Listener labels and socket metadata are available;
negotiated TLS metadata is not.

`transport_ready` runs after TLS negotiation when applicable. Plaintext and Unix
connections also visit it, without TLS metadata. Direct I/O serving entry
points start here: their caller owns acceptance and any TLS handshake, so
Nacelle cannot enforce a pre-handshake policy for an externally supplied
transport.

Requests remain sequential per connection; no per-request task is introduced.
`before_request` may run before an idle wait, incomplete head, or clean EOF.
It cannot assume that a request exists yet. Once acquired, its permit is
retained across fragmented head reads rather than reacquired for each fragment.
`request_head` receives the typed head, declared body length, immutable
connection metadata, and current connection state.

`body_complete` runs once for successfully read bodies, including empty bodies
and bodies already in read-ahead. It does not run after body-read failure. For
buffered bodies it precedes the handler. For streaming bodies it follows
successful pumping, but the handler may already have processed chunks, and
queued chunks need not have been consumed. The hook receives metadata and
declared length, not a copy of the body or head. Whole-body application
validation belongs in the handler or an application staging layer.

## Admission and permits

Each hook returns `Result<Option<Policy::Permit>, NacelleError>`.

- Return `Ok(Some(permit))` to proceed and retain an application-owned resource,
  or `Ok(None)` to proceed without a lease.
- Await an application semaphore, budget, or other capacity source to apply
  backpressure before proceeding.
- Return `Err(...)` to reject the connection or terminate the request.

`Permit` must be owned, `Send`, and `'static`. Use `()` for decision-only
policies, or an owned application lease such as `OwnedSemaphorePermit`.
Default hooks return `Ok(None)`. Different resource kinds can be represented
by an application enum or struct. Release resources in `Drop`; Nacelle retains
the returned values and drops them on completion, failure, or cancellation.
Policies are `Send + Sync` and return `Send` futures. Serial and worker-local
connection state still need not be `Sync`; snapshot any needed non-`Sync` state
synchronously before constructing an asynchronous policy wait.

Acquired request permits create a response-delivery barrier: pending response
bytes are flushed before releasing request permits. This can reduce response
coalescing compared with the no-op policy. Even a policy without permits may
wait before the next pipelined request; pending responses are flushed before
waiting so they cannot be stranded behind admission for the next request.

Policy waits are not covered by the socket-read, TLS-handshake, or handler
deadlines: those operations have not started at their corresponding admission
points. Applications must explicitly bound policy waits, for example with
`tokio::time::timeout`, and map failures to stable errors. Graceful listener
shutdown stops accepting and eventually aborts remaining connection tasks at
the configured drain deadline, cancelling waits and dropping acquired permits.
Panicking application policies have the same connection-task failure boundary
as panicking handlers.

## Example: application-owned connection admission

This uses the repository's unpublished reference protocol fixture:

```rust
use std::sync::Arc;
use std::time::Duration;

use nacelle::core::{NacelleError, NacelleResourceLimitReason, NacelleTimeoutReason};
use nacelle::core::request::NacelleConnectionMeta;
use nacelle::tcp::{Protocol, TcpLifecyclePolicy};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct Admission {
    connections: Arc<Semaphore>,
}

impl TcpLifecyclePolicy<FrameRequest, ()> for Admission {
    type Permit = OwnedSemaphorePermit;

    async fn accepted(
        &self,
        _connection: &NacelleConnectionMeta,
    ) -> Result<Option<Self::Permit>, NacelleError> {
        let acquire = self.connections.clone().acquire_owned();
        let permit = tokio::time::timeout(Duration::from_secs(1), acquire)
            .await
            .map_err(|_| NacelleError::Timeout(NacelleTimeoutReason::Other("admission")))?
            .map_err(|_| {
                NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::Other("admission_closed"),
                )
            })?;
        Ok(Some(permit))
    }
}

let protocol = LengthDelimitedProtocol.with_policy(Admission {
    connections: Arc::new(Semaphore::new(128)),
});
// Pass `protocol` to the server builder and use its wrapped protocol type
// in the handler's TcpRequestContext.
```

Applications may share the same policy capacity across listeners and workers,
or deliberately partition it. Nacelle does not implicitly multiply or
partition application-owned budgets. Keep rejection reasons static and
low-cardinality; do not return secrets or peer-controlled strings in responses.

## Rejection and observability

Connection-stage rejection closes the connection without a protocol response.
`before_request` rejection may use `Protocol::error_response` without request
error context. Head/body rejection uses the context captured from the decoded
head and the normal bounded response encoder, then terminates the connection.
Response-delivery failures are not recursively offered as new policy failures.

Policy errors use stable operation stages: `policy_accepted`,
`policy_transport_ready`, `policy_before_request`, `policy_request_head`, and
`policy_body_complete`. Connection-stage rejections also emit the stable
connection-rejection reason `policy`. Existing request failure and connection
task reporting remain in place. Applications own higher-level decision logs.

## Staged migration and non-guarantees

This is the first migration area, not replacement of existing limits.
`NacelleRuntimeState`, per-peer admission, TCP limits, body and frame bounds,
handler deadlines, streaming/memory accounting, and HTTP policy remain active.
New lifecycle policies are opt-in and add no default application limits.
Do not assume that disabling `tcp-policies` disables the legacy controls.
Migration of built-in resource controls into opt-in application policies is a
separate step; framing validation and memory-safe I/O remain invariants.

This area does **not** change body consumption:

- Head decoding can read ahead into body bytes or subsequent requests.
- Buffered mode still reads the complete body before invoking the handler.
- Streaming mode still eagerly pumps into a bounded channel and drains unread
  input before a successful response.
- Permits do not follow application-retained `Bytes` clones and are not an
  accounting guarantee for resident memory, TLS buffers, or kernel buffers.

Strict header-only reads, demand-driven per-chunk admission, and explicit
unread-body disposition are the next deferred-body feature area. Lifecycle
hooks alone must not be advertised as those guarantees.
