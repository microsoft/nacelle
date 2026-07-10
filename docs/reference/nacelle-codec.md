# Codec primitives

`nacelle-codec` provides ordered byte/message I/O over `AsyncRead` and
`AsyncWrite` transports.

## Components

| Component | Purpose |
| --- | --- |
| `MessageDecoder` | Decodes messages from cumulative input |
| `MessageReader` | Reads, decodes, validates progress, and handles EOF |
| `MessageEncoder` | Appends encoded messages to an output buffer |
| `MessageWriter` | Queues encoded bytes and writes them to a transport |
| `LengthDelimitedDecoder` | Decodes four-byte length-prefixed payloads |
| `LengthDelimitedEncoder` | Encodes four-byte length-prefixed payloads |
| `RotatingMessageReader` | Reclaims empty large input buffers when enabled |

## Decoder contract

Implement `MessageDecoder` for protocol parsing. `decode` receives the
cumulative `BytesMut` directly. Return a message only after consuming input,
and return `Ok(None)` without consuming bytes when more input is required.
`MessageReader` reports either progress-contract violation explicitly.

The built-in length-delimited decoder returns a split `BytesMut` without
copying. Callers can retain it, freeze it, copy it, pool it, or parse it into a
protocol-specific value.

## Writer contract

Implement `MessageEncoder<M>` by appending directly to `BytesMut`.
`MessageWriter` checkpoints the buffer before each encoder call and rolls back
newly appended bytes when encoding fails.

`feed` only encodes. `flush` writes all queued bytes and flushes the transport.
`send` performs both operations.

## Buffer behavior

`MessageReader` and `MessageWriter` each hold one cumulative `BytesMut`, because
framing partial reads and writes requires storage across transport operations.
Use `with_buffer` to supply storage and `into_parts` to reclaim it.

A decoded message may share its backing allocation with the reader's input
buffer. `freeze()` converts it to `Bytes` without copying. Use
`Bytes::copy_from_slice` or `BytesMut::from` when the decoded message needs an
independent allocation.

## Limits

The length-delimited codecs reject frames larger than their configured maximum.
Custom decoders are responsible for rejecting oversized input before the
cumulative `MessageReader` buffer grows indefinitely.

`MessageWriter::feed` queues encoded bytes. `flush`, `send`, and `shutdown`
write queued bytes to the transport.

## Long-lived connections

The optional `buffer-rotation` feature provides
`RotatingMessageReader`. Configure a threshold and replacement capacity to
replace an empty input buffer after a large decoded message. The decoded
`BytesMut` remains zero-copy and retains its original allocation until it is
dropped. If coalesced input follows the large message, replacement waits until
that input has been consumed.

## Stability

The `0.2` API is experimental.
