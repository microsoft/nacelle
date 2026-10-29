# Reference protocol

This document describes the `LengthDelimitedProtocol` fixture in the unpublished
`examples/nacelle-reference-protocol` workspace package. It is used by examples,
tests, benchmarks, and stress tools, but it is not part of Nacelle's published
library API. Applications can use it from a repository checkout or implement
`Protocol` with an associated `Request` type for TCP or Unix domain sockets.

## Frame Layout

All integer fields are little-endian.

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 4 | `frame_len` |
| 4 | 8 | `request_id` |
| 12 | 8 | `opcode` |
| 20 | 4 | `flags` |
| 24 | `frame_len - 20` | body bytes |

`frame_len` counts the fixed fields after itself plus the body. The minimum
valid value is `20`.

## Flags

| Flag | Value | Meaning |
| --- | ---: | --- |
| `FRAME_FLAG_START` | `0b0001` | First response frame for a request |
| `FRAME_FLAG_END` | `0b0010` | Last response frame for a request |
| `FRAME_FLAG_ERROR` | `0b0100` | Response body contains an error message |

Request flags are decoded and preserved in `FrameRequest`, but the built-in
server does not currently interpret request flags.

## Requests

Each request frame contains one complete request body. The server decodes only
the frame head before dispatch, then exposes the body to the handler as a
`NacelleBody`. Small bodies are served from the connection read buffer. Larger
bodies are streamed to the handler in configured chunks.

Custom protocols provide a per-connection `MessageDecoder` through
`Protocol::decoder`. Decoders follow the `nacelle-codec` progress contract:
returning a request consumes at least one head byte, while requesting more input
leaves the cumulative buffer unchanged.

A decoder may wait for a fixed header plus a small protocol prefix before
returning a `DecodedRequest<Protocol::Request>`. If the prefix is incomplete, return `Ok(None)` and
leave every byte untouched. Once classification is possible, consume only the
header/prefix and report the unconsumed body length in `DecodedRequest`. This
preserves early body-limit checks and streaming. The request head can carry an
opcode or flag that the handler uses to choose a response or silent completion.

`opcode` is request metadata. The application handler decides whether to use it
for routing, reject it, or ignore it. If the handler rejects an opcode after
draining the body and returns an error, the server encodes that error as a
response frame.

Handlers receive `TcpRequestContext<P, AppState>`, where application state
defaults to `()`. `context.app_state()` borrows the dependency root shared by
the app across listeners. Its request contains the associated
protocol head under `request().head` and the bounded body under
`request_mut().body`. Its connection context contains a stable connection id,
peer/local addresses, listener label, TLS metadata, and
`Arc<P::ConnectionState>`. The runtime constructs that state once with
`Protocol::connection_state` and shares it across requests on the connection.

State confined to the serial connection loop can use `SerialTcpServer` or
`LocalSerialTcpServer`. Their handlers implement `SerialTcpHandler` or
`LocalSerialTcpHandler` and receive `SerialTcpRequestContext<'_, P>`, which
lends exclusive mutable access to directly owned connection state. The loop
awaits completion before decoding the next message, so one connection cannot
overlap serial handler calls. The same serial context permits either a response
or explicit silent completion.

## Responses

Handlers call `context.respond(P::Response).await` or return
`Ok(context.complete())` and return the resulting typed completion. A protocol can accept its own response type; the reference protocol
uses `TcpResponse`, whose body may be empty, one chunk, or streaming. Returning
an HTTP response from a TCP handler does not compile.

Every decoded message uses `TcpRequestContext<P>` and the same concrete handler.
`TcpHandlerCompletion<P>` wraps an optional response; explicit silent completion
does not call the encoder or emit any bytes. This differs from responding with
`TcpResponse::empty()`, which still produces protocol framing.

Silent completion is application policy, not a compile-time prohibition on
responses. Framework failures may still produce an error response. Protocols
requiring strict silence for a particular message must encode that policy in
their handler and `error_response` hook.

The TCP runtime encodes and writes each streaming response chunk before polling
the next one, so socket backpressure bounds response production. It stages only
one bounded frame at a time and writes an explicit end frame after a streaming
body reaches EOF.

`ResponseWritePolicy::Immediate` writes each completed frame immediately.
`CoalesceBuffered` and `FlushAtBytes` may queue multiple completed frames from
already-decoded requests, preserving order and rolling back only the current
frame on encoder failure. The queue drains before reading more request-head bytes
from the socket and before awaiting another streaming response chunk. At that
boundary, the runtime also flushes the underlying `AsyncWrite`, which is required
for buffered transports such as TLS. When the connection ends cleanly, the runtime performs a
`shutdown_timeout`-bounded writer shutdown so TLS transports can emit
`close_notify`. Terminal error paths normally make the same bounded shutdown
attempt; an earlier connection failure remains the returned error if shutdown
also fails. If pending response delivery is cancelled, the connection is dropped
without another flush or shutdown attempt, because retrying could replay a
partially written response.
Request telemetry records encoded response bytes when a request completes; a
later batch write, transport flush, or shutdown failure is reported as a
connection operation error.

The protocol guarantees:

- the first response frame has `FRAME_FLAG_START`
- the last response frame has `FRAME_FLAG_END`
- a handler that returns an empty body still emits a start/end response frame
- a handler error emits a start/end/error frame through the fixture's
  `error_response` implementation

Responses are written in request-processing order for a single connection. The
prototype does not yet provide concurrent per-connection response interleaving.

## Error Handling

`Protocol::error_response(context, error)` returns either
`Some(TcpCompletion::new(response, response_context))` or `None`. The default is
`None`, meaning no error response. A returned response passes through
`apply_response`, `response_body`, and the ordinary `encode_response_*` methods:
there is no separate error-frame encoder. The protocol chooses a peer-safe
payload and any response metadata or error flag. Avoid exposing arbitrary
internal error text to untrusted peers.

Malformed heads, oversized frames, framing-progress violations, incomplete-head
EOF, idle timeout, and message-read timeout are offered without a request error
context. Once a head is decoded, body-limit rejection, request/streaming
admission failures, body-read failures, handler errors, and handler timeouts are
offered with the context captured by `error_context`.
This includes failures while draining the body after a handler completes
silently or prepares a response. A streaming body-read failure cancels a still
running handler.
If that cancellation interrupts delivery of an earlier coalesced response,
Nacelle closes the connection without invoking `error_response` or retrying
delivery.

Responses retain the configured body limits, write policy, and write
deadlines. They are best-effort: resource exhaustion can prevent delivery.
Connection admission and TLS handshake failures occur before this hook is
available. Initial socket read I/O failures and failures during response
encoding, streaming, writing, flushing, or shutdown do not initiate a new error
response. In particular, a partially delivered response is never followed by
an automatic replacement response, and an
error-response delivery failure never recursively invokes the hook.

The connection closes after a terminal failure, even if its custom response is
successfully sent, so unread body bytes cannot become another request. The
original error is returned unless response delivery fails; sending an error
payload does not turn the request into success. Unknown opcode handling remains
application policy.

### Migrating from separate one-way handlers

- Return `DecodedRequest<Request>` directly from the decoder, without
  `DecodedMessage::Request` or `DecodedMessage::OneWay`.
- Combine request head types into one `Protocol::Request` (an enum is suitable
  when message shapes differ). Remove `OneWayRequest`, `one_way_wire_bytes`,
  and `max_one_way_body_bytes`; apply the ordinary request hooks to every head.
- Move `.one_way_handler(...)` logic into `.handler(...)` and return
  `Ok(context.complete())` for the silent branch. Use the ordinary serial or
  local handler for those execution modes.
- Replace `encode_error` with `error_response` and represent error-specific
  framing in the typed response or response context.
- `TcpHandlerCompletion` is now an `OptionalCompletion`; use `into_response()`
  when inspecting it. Its `map(...)` helper transforms only actual responses,
  retaining delivery completion items without running on silent completions.
- TCP server type parameters no longer include a separate one-way handler.

These are source-breaking TCP API changes. HTTP required-response contexts and
the core required/no-response capability primitives are unchanged.

## Limits

The opt-in [TCP lifecycle policy hooks](tcp-policies.md) allow
application-owned admission before TLS, before request-head decoding, after
head decoding, and after body input completes. They retain the same typed
handler pathway and do not yet replace the limits below or change body reading.

The server enforces `NacelleTcpConfig::max_frame_len` against `frame_len`.
Buffer sizes and request-body chunking are configured through
`NacelleTcpConfig`.
Runtime budgets, timeouts, and active counters are configured through
`NacelleLimits` / `NacelleRuntimeState`.

TCP protocols can apply phase-aware request body limits by overriding
`Protocol::max_request_body_bytes(request, connection, state, default_limit)`.
The TCP
runtime calls this after decoding the request head and before buffering or
streaming the body. Implementations can use concrete protocol configuration,
the decoded head, immutable `ConnectionInfo`, and the same concrete
`Protocol::ConnectionState` exposed to handlers. Rejection occurs before
body-specific allocation or additional body reads, although decoder read-ahead
may already have buffered bytes.
Messages that the handler completes silently use these same hooks and
early-rejection boundary.

TCP request handling is sequential per connection. Pipelined frames can sit
in the socket/read buffer, but Nacelle does not run multiple handlers
concurrently for one TCP connection. Streaming request bodies use
`request_body_channel_capacity` for backpressure between socket reads and the
handler.
For streaming request bodies, earlier response output flushes concurrently with
handler admission, the admitted handler, and the body pump. This includes output
retained by the underlying writer after Nacelle's frame queue has drained.
Blocked response writes therefore do not prevent an admitted handler from
draining the bounded body channel. This behavior is independent of the `policies`
feature.

`SharedProtocol` marks protocols whose connection state is `Send + Sync` and is
required by the existing `Arc`-backed shared server. Shared serial servers
require state and handler futures to be `Send`, but not `Sync`, because each
connection owns its state. Worker-local serial servers may use `!Send` state and
futures.
