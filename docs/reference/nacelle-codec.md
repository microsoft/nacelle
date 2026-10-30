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

`MessageReader::read_more()` appends one transport read without decoding and
returns the byte count, or zero at EOF. Once EOF is observed, later calls return
zero without polling the transport. `read_message()` still handles final
decoding and incomplete-EOF errors. Cancelling a pending `read_more()` does not
consume bytes. This primitive supports separate first-byte and message-completion
deadlines without changing buffer ownership. Callers must decode between reads
to enforce input limits; repeated `read_more()` calls alone do not bound growth.

A decoded message may share its backing allocation with the reader's input
buffer. `freeze()` converts it to `Bytes` without copying. Use
`Bytes::copy_from_slice` or `BytesMut::from` when the decoded message needs an
independent allocation.

## Bounded reads

`read_message_bounded(B)` decodes buffered input before attempting another read.
`read_more_bounded(B)` performs one read without decoding. Both cap the supplied
destination to `B - buffer.len()`, even when the allocation has much more spare
capacity. The allowance is cumulative, not renewed on each syscall. Following
message/body prefixes already buffered count toward `B`.

An initial buffer larger than `B` returns
`MessageReadError::ReadBudgetExceeded { limit, buffered }` before decoding or
polling, preserving its bytes. At exactly `B`, a complete message succeeds;
needing another read returns that error without polling a zero-length
destination. A zero budget cannot discover EOF. Exhaustion does not mark EOF:
the caller can resume with a larger budget or explicitly consume admitted bytes.

Normal decoding, progress checks, cancellation, and shared-slice ownership
remain unchanged. Once a transport read observes EOF, `decode_eof` runs as
usual, including incomplete-input errors. An EOF-delimited message that fills
the entire budget cannot discover EOF and needs spare allowance. Known EOF does
not poll the transport again.

The bound covers plaintext returned into the cumulative buffer. Allocation
capacity, retained shared messages, decoder-owned allocations, TLS transport
buffers (including decrypted record data), and kernel buffers have separate
lifetimes and limits. Mixing bounded calls with unbounded reads or
direct buffer mutation is the caller's responsibility; the next bounded call
rejects an oversized buffer rather than trimming it.

## Limits

The length-delimited codecs reject frames larger than their configured maximum.
Custom decoders are responsible for rejecting oversized input before the
cumulative `MessageReader` buffer grows indefinitely.
They must also bound their own allocations independently of reader budgets.

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

The `0.3` API is experimental.
