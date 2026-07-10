# nacelle-codec

`nacelle-codec` provides byte-to-message decoding and message-to-byte encoding
for asynchronous Rust I/O.

## Core API

- `MessageDecoder` decodes directly from a cumulative `BytesMut`.
- `MessageReader` drives an `AsyncRead`, decoder, and input `BytesMut`.
- `MessageEncoder` encodes directly into a cumulative `BytesMut`.
- `MessageWriter` drives an `AsyncWrite`, encoder, and output `BytesMut`.
- `RotatingMessageReader` is available with the optional `buffer-rotation`
  feature for long-lived connections.

The built-in length-delimited codec uses a four-byte length prefix and returns
each decoded payload as `BytesMut`:

```rust,no_run
use nacelle_codec::{
    LengthDelimitedDecoder, LengthDelimitedEncoder, MessageReader, MessageWriter,
};
use tokio::net::TcpStream;

# async fn example(stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
let (read_half, write_half) = stream.into_split();
let mut reader = MessageReader::new(read_half, LengthDelimitedDecoder::new(1024 * 1024));
let mut writer = MessageWriter::new(write_half, LengthDelimitedEncoder::new(1024 * 1024));

while let Some(message) = reader.read_message().await? {
    writer.send(message).await?;
}
writer.shutdown().await?;
# Ok(())
# }
```

`feed` appends a message synchronously. `flush` writes all queued bytes, and
`send` combines `feed` and `flush`.

## Buffers

Reader and writer each hold exactly one `BytesMut`. The default constructors
start them at 8 KiB. `with_capacity` changes that initial capacity, while
`with_buffer` accepts caller-provided storage. The buffers remain accessible
through `buffer` and `buffer_mut` and are returned by `into_parts`.

Decoded messages can be:

- `freeze()` a message into immutable `Bytes`;
- copy it into detached request storage;
- return it to a pool;
- stream or parse it into a protocol-specific value.

A decoded message may share its backing allocation with the cumulative input
buffer. `freeze()` preserves that allocation without copying. Use
`Bytes::copy_from_slice` or `BytesMut::from` when an independent allocation is
required.

## Limits

`LengthDelimitedDecoder` and `LengthDelimitedEncoder` reject frames larger than
their configured maximum. Custom decoders are responsible for rejecting
oversized input while it accumulates in `MessageReader`. `MessageWriter` queues
encoded bytes until `flush`, `send`, or `shutdown` writes them.

## Long-lived connections

Enable the `buffer-rotation` feature and wrap a `MessageReader` with
`RotatingMessageReader::new(reader, rotation_threshold)`. After a decoded
message exceeds the threshold and the input is drained, the reader installs a
fresh replacement buffer. The decoded message remains zero-copy and keeps its
original allocation until it is dropped.

The initial `0.1` API is experimental.
