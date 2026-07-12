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
classifying a message as `DecodedMessage::Request` or
`DecodedMessage::OneWay`. If the prefix is incomplete, return `Ok(None)` and
leave every byte untouched. Once classification is possible, consume only the
header/prefix and report the unconsumed body length in `DecodedRequest`. This
preserves early body-limit checks and streaming while allowing one-way flags to
live immediately after the fixed header.

`opcode` is request metadata. The application handler decides whether to use it
for routing, reject it, or ignore it. If the handler rejects an opcode after
draining the body and returns an error, the server encodes that error as a
response frame.

Handlers receive `TcpRequestContext<P>`. Its request contains the associated
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
overlap serial handler calls. Serial one-way contexts expose the same mutable
state and still provide no response capability.

## Responses

Handlers call `context.respond(P::Response)` and return the resulting typed
completion. A protocol can accept its own response type; the reference protocol
uses `TcpResponse`, whose body may be empty, one chunk, or streaming. Returning
an HTTP response from a TCP handler does not compile.

Protocols classify decoded messages as `DecodedMessage::Request` or
`DecodedMessage::OneWay`. Required requests use `TcpRequestContext<P>` and must
respond. One-way messages use `TcpOneWayContext<P>` with `NoResponse`, so no
`respond` method exists. Servers supporting one-way messages install a separate
concrete handler with `TcpServer::<YourProtocol>::builder().one_way_handler(...)`. Request-only
protocols use `Infallible` as their one-way request type.

The TCP runtime encodes and writes each streaming response chunk before polling
the next one, so socket backpressure bounds response production. It stages only
one bounded frame at a time, accounts staging growth against the runtime memory
budget, and writes an explicit end frame after a streaming body reaches EOF.

`ResponseWritePolicy::Immediate` writes each completed frame immediately.
`CoalesceBuffered` and `FlushAtBytes` may queue multiple completed frames from
already-decoded requests, preserving order and rolling back only the current
frame on encoder failure. The queue drains before another socket read and before
awaiting another streaming response chunk. Request telemetry records encoded
response bytes when a request completes; a later batch write failure is reported
as a connection operation error.

The protocol guarantees:

- the first response frame has `FRAME_FLAG_START`
- the last response frame has `FRAME_FLAG_END`
- a handler that returns an empty body still emits a start/end response frame
- a handler error emits a start/end/error frame

Responses are written in request-processing order for a single connection. The
prototype does not yet provide concurrent per-connection response interleaving.

## Error Handling

Malformed frame heads, oversized frames, and EOF before a complete frame cause
the connection to fail. Streaming request read failure cancels the handler
future. Handler errors and timeouts are encoded as error frames when enough
request context is available, then the connection closes so unread body bytes
cannot be interpreted as another frame. Unknown opcode handling is application
policy.

## Limits

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
One-way messages use the equivalent
`Protocol::max_one_way_body_bytes(request, connection, state, default_limit)`
hook and the same early-rejection boundary.

TCP request handling is sequential per connection. Pipelined frames can sit
in the socket/read buffer, but Nacelle does not run multiple handlers
concurrently for one TCP connection. Streaming request bodies use
`request_body_channel_capacity` for backpressure between socket reads and the
handler, and declared streaming body bytes are allocated against the memory
budget until the streaming request finishes.

`SharedProtocol` marks protocols whose connection state is `Send + Sync` and is
required by the existing `Arc`-backed shared server. Shared serial servers
require state and handler futures to be `Send`, but not `Sync`, because each
connection owns its state. Worker-local serial servers may use `!Send` state and
futures.
