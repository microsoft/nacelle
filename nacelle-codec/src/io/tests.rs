use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use super::*;
use crate::{LengthDelimitedDecoder, LengthDelimitedEncoder, MessageDecoder, MessageEncoder};

#[tokio::test]
async fn reader_handles_fragmented_and_coalesced_messages() {
    let (mut client, server) = tokio::io::duplex(64);
    client.write_all(&[0, 0, 0]).await.expect("fragment");
    client
        .write_all(&[
            5, b'h', b'e', b'l', b'l', b'o', 0, 0, 0, 5, b'w', b'o', b'r', b'l', b'd',
        ])
        .await
        .expect("coalesced frames");
    client.shutdown().await.expect("client shutdown");

    let mut reader = MessageReader::new(server, LengthDelimitedDecoder::new(32));
    assert_eq!(
        &reader.read_message().await.expect("read").expect("first")[..],
        b"hello"
    );
    assert_eq!(
        &reader.read_message().await.expect("read").expect("second")[..],
        b"world"
    );
    assert!(reader.read_message().await.expect("clean EOF").is_none());
}

#[tokio::test]
async fn reader_reports_incomplete_eof() {
    let (mut client, server) = tokio::io::duplex(16);
    client
        .write_all(&[0, 0, 0, 5, b'h'])
        .await
        .expect("partial frame");
    client.shutdown().await.expect("client shutdown");

    let mut reader = MessageReader::new(server, LengthDelimitedDecoder::new(32));
    let error = reader.read_message().await.expect_err("partial EOF");
    assert!(matches!(
        error,
        MessageReadError::UnexpectedEof { remaining: 5 }
    ));
}

#[tokio::test]
async fn reader_read_more_preserves_buffer_and_eof() {
    let (mut client, server) = tokio::io::duplex(16);
    let mut reader = MessageReader::new(server, LengthDelimitedDecoder::new(32));
    client.write_all(&[0, 0, 0, 1, b'x']).await.expect("frame");

    assert_eq!(reader.read_more().await.expect("prefetch"), 5);
    assert_eq!(&reader.buffer()[..], &[0, 0, 0, 1, b'x']);
    assert_eq!(
        &reader.read_message().await.expect("read").expect("message")[..],
        b"x"
    );

    client.write_all(&[0, 0]).await.expect("partial frame");
    assert_eq!(reader.read_more().await.expect("partial prefetch"), 2);
    client.shutdown().await.expect("shutdown");
    assert_eq!(reader.read_more().await.expect("EOF"), 0);
    assert_eq!(reader.read_more().await.expect("repeated EOF"), 0);
    assert!(matches!(
        reader.read_message().await,
        Err(MessageReadError::UnexpectedEof { remaining: 2 })
    ));
}

#[tokio::test]
async fn reader_read_more_cancellation_preserves_input() {
    let (mut client, server) = tokio::io::duplex(16);
    let mut reader = MessageReader::new(server, LengthDelimitedDecoder::new(32));
    {
        let pending_read = reader.read_more();
        tokio::pin!(pending_read);
        std::future::poll_fn(|context| {
            assert!(pending_read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(reader.buffer().is_empty());
    client.write_all(&[0, 0, 0, 1, b'x']).await.expect("frame");
    assert_eq!(
        &reader.read_message().await.expect("read").expect("message")[..],
        b"x"
    );
    client.shutdown().await.expect("shutdown");
    assert_eq!(reader.read_more().await.expect("EOF"), 0);
    assert!(reader.read_message().await.expect("clean EOF").is_none());
}

#[derive(Debug, Default)]
struct NoProgressDecoder;

impl MessageDecoder for NoProgressDecoder {
    type Message = ();
    type Error = Infallible;

    fn decode(&mut self, _input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        Ok(Some(()))
    }
}

#[tokio::test]
async fn reader_rejects_message_without_progress() {
    let (_client, server) = tokio::io::duplex(8);
    let mut reader = MessageReader::new(server, NoProgressDecoder);

    let error = reader.read_message().await.expect_err("progress violation");
    assert!(matches!(error, MessageReadError::MessageWithoutProgress));
}

#[derive(Debug, Default)]
struct ConsumingWaitDecoder;

impl MessageDecoder for ConsumingWaitDecoder {
    type Message = ();
    type Error = Infallible;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if !input.is_empty() {
            input.advance(1);
        }
        Ok(None)
    }
}

#[tokio::test]
async fn reader_rejects_consumption_before_need_more() {
    let (mut client, server) = tokio::io::duplex(8);
    client.write_all(&[1, 2]).await.expect("input");
    let mut reader = MessageReader::new(server, ConsumingWaitDecoder);

    let error = reader.read_message().await.expect_err("progress violation");
    assert!(matches!(
        error,
        MessageReadError::ConsumedOnNeedMore {
            before: 2,
            after: 1
        }
    ));
}

#[test]
fn reader_accepts_caller_buffer() {
    let (_client, server) = tokio::io::duplex(8);
    let buffer = BytesMut::from(&[0, 0, 0, 0][..]);
    let reader = MessageReader::with_buffer(server, LengthDelimitedDecoder::new(0), buffer);

    assert_eq!(reader.buffer().len(), 4);
}

#[test]
fn reader_decodes_buffered_messages_without_reading() {
    let (_client, server) = tokio::io::duplex(64);
    let buffer = BytesMut::from(
        &[
            0, 0, 0, 5, b'f', b'i', b'r', b's', b't', 0, 0, 0, 6, b's', b'e', b'c', b'o', b'n',
            b'd',
        ][..],
    );
    let mut reader = MessageReader::with_buffer(server, LengthDelimitedDecoder::new(32), buffer);

    let first = reader
        .decode_buffered()
        .expect("first buffered decode")
        .expect("first buffered message");
    let second = reader
        .decode_buffered()
        .expect("second buffered decode")
        .expect("second buffered message");

    assert_eq!(&first[..], b"first");
    assert_eq!(&second[..], b"second");
    assert!(reader.buffer().is_empty());
}

#[cfg(feature = "buffer-rotation")]
#[test]
fn reader_rotates_only_empty_oversized_buffers() {
    let (_client, server) = tokio::io::duplex(64);
    let mut buffer = BytesMut::with_capacity(1024);
    buffer.extend_from_slice(b"pending");
    let mut reader = MessageReader::with_buffer(server, LengthDelimitedDecoder::new(32), buffer);

    reader.rotate_empty_buffer(64);
    assert!(reader.buffer().capacity() >= 1024);

    reader.buffer_mut().clear();
    reader.rotate_empty_buffer(64);
    assert_eq!(reader.buffer().capacity(), 64);
}

struct CountingRead {
    input: &'static [u8],
    chunk_size: usize,
    returned: usize,
    offered: Vec<usize>,
}

impl AsyncRead for CountingRead {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.offered.push(destination.remaining());
        let count = destination
            .remaining()
            .min(this.chunk_size)
            .min(this.input.len());
        let (chunk, remaining) = this.input.split_at(count);
        destination.put_slice(chunk);
        this.input = remaining;
        this.returned += count;
        Poll::Ready(Ok(()))
    }
}

fn counting_read(input: &'static [u8], chunk_size: usize) -> CountingRead {
    CountingRead {
        input,
        chunk_size,
        returned: 0,
        offered: Vec::new(),
    }
}

#[tokio::test]
async fn bounded_reads_cap_fragmented_input_and_preserve_shared_slices() {
    let transport = counting_read(b"\0\0\0\x04head\0\0\0\x04next", 2);
    let mut reader =
        MessageReader::with_capacity(transport, LengthDelimitedDecoder::new(32), 64 * 1024);
    let pointer = reader.buffer().as_ptr();
    let first = reader
        .read_message_bounded(8)
        .await
        .expect("read")
        .expect("head");
    assert_eq!(&first[..], b"head");
    assert_eq!(first.as_ptr(), pointer.wrapping_add(4));
    assert_eq!(reader.transport().returned, 8);
    assert_eq!(reader.transport().offered, [8, 6, 4, 2]);

    #[cfg(feature = "buffer-rotation")]
    reader.rotate_empty_buffer(16);
    let second = reader
        .read_message_bounded(8)
        .await
        .expect("read")
        .expect("next");
    assert_eq!(&second[..], b"next");
    assert_eq!(&first[..], b"head");
    assert_eq!(reader.transport().returned, 16);
    assert_eq!(reader.transport().offered, [8, 6, 4, 2, 8, 6, 4, 2]);
}

#[tokio::test]
async fn bounded_exhaustion_is_cumulative_and_does_not_observe_eof() {
    let transport = counting_read(b"\0\0\0\x04head", 2);
    let mut reader = MessageReader::new(transport, LengthDelimitedDecoder::new(32));
    for _ in 0..2 {
        assert!(matches!(
            reader.read_message_bounded(7).await,
            Err(MessageReadError::ReadBudgetExceeded {
                limit: 7,
                buffered: 7
            })
        ));
        assert_eq!(reader.transport().returned, 7);
        assert_eq!(reader.transport().offered, [7, 5, 3, 1]);
        assert_eq!(&reader.buffer()[..], b"\0\0\0\x04hea");
    }
    let message = reader
        .read_message_bounded(8)
        .await
        .expect("resume")
        .expect("message");
    assert_eq!(&message[..], b"head");
}

#[tokio::test]
async fn bounded_reads_reject_oversized_initial_buffers_before_decoding() {
    let buffer = BytesMut::from(&b"\0\0\0\x04headextra"[..]);
    let pointer = buffer.as_ptr();
    let mut reader = MessageReader::with_buffer(
        counting_read(b"", 8),
        LengthDelimitedDecoder::new(32),
        buffer,
    );
    assert!(matches!(
        reader.read_message_bounded(8).await,
        Err(MessageReadError::ReadBudgetExceeded {
            limit: 8,
            buffered: 13
        })
    ));
    assert!(matches!(
        reader.read_more_bounded(8).await,
        Err(MessageReadError::ReadBudgetExceeded { .. })
    ));
    assert_eq!(reader.buffer().as_ptr(), pointer);
    assert_eq!(&reader.buffer()[..], b"\0\0\0\x04headextra");
    assert_eq!(reader.transport().offered, []);

    let mut empty = MessageReader::new(counting_read(b"", 8), LengthDelimitedDecoder::new(32));
    assert!(matches!(
        empty.read_more_bounded(0).await,
        Err(MessageReadError::ReadBudgetExceeded {
            limit: 0,
            buffered: 0
        })
    ));
    assert_eq!(empty.transport().offered, []);
}

struct EofDecoder;

impl MessageDecoder for EofDecoder {
    type Message = BytesMut;
    type Error = Infallible;

    fn decode(&mut self, _: &mut BytesMut) -> Result<Option<BytesMut>, Infallible> {
        Ok(None)
    }

    fn decode_eof(&mut self, input: &mut BytesMut) -> Result<Option<BytesMut>, Infallible> {
        Ok((!input.is_empty()).then(|| input.split()))
    }
}

#[tokio::test]
async fn bounded_reads_preserve_eof_and_progress_errors() {
    let mut reader = MessageReader::new(counting_read(b"end", 8), EofDecoder);
    assert_eq!(
        &reader
            .read_message_bounded(4)
            .await
            .expect("EOF decode")
            .expect("message")[..],
        b"end"
    );
    assert!(reader.read_message_bounded(4).await.expect("EOF").is_none());
    assert_eq!(reader.read_more_bounded(0).await.expect("known EOF"), 0);
    assert_eq!(reader.transport().offered, [4, 1]);

    let mut incomplete =
        MessageReader::new(counting_read(b"\0\0", 8), LengthDelimitedDecoder::new(32));
    assert!(matches!(
        incomplete.read_message_bounded(8).await,
        Err(MessageReadError::UnexpectedEof { remaining: 2 })
    ));
    let mut stalled = MessageReader::new(counting_read(b"", 8), NoProgressDecoder);
    assert!(matches!(
        stalled.read_message_bounded(8).await,
        Err(MessageReadError::MessageWithoutProgress)
    ));
    let mut consuming = MessageReader::new(counting_read(b"ab", 8), ConsumingWaitDecoder);
    assert!(matches!(
        consuming.read_message_bounded(2).await,
        Err(MessageReadError::ConsumedOnNeedMore {
            before: 2,
            after: 1
        })
    ));
}

#[tokio::test]
async fn cancelling_bounded_read_retains_partial_input() {
    let (mut client, server) = tokio::io::duplex(16);
    client.write_all(&[0, 0]).await.expect("prefix");
    let mut reader = MessageReader::new(server, LengthDelimitedDecoder::new(32));
    {
        let pending = reader.read_message_bounded(5);
        tokio::pin!(pending);
        std::future::poll_fn(|context| {
            assert!(pending.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_eq!(&reader.buffer()[..], &[0, 0]);
    client.write_all(&[0, 1, b'x']).await.expect("suffix");
    assert_eq!(
        &reader
            .read_message_bounded(5)
            .await
            .expect("resume")
            .expect("message")[..],
        b"x"
    );
}

#[tokio::test]
async fn writer_feeds_and_sends_messages() {
    let (client, mut server) = tokio::io::duplex(32);
    let mut writer = MessageWriter::new(client, LengthDelimitedEncoder::new(16));
    writer.feed(&b"hello"[..]).expect("feed");
    assert_eq!(writer.buffer().len(), 9);

    let write = async {
        writer.send(&b"world"[..]).await.expect("send");
        writer.shutdown().await.expect("shutdown");
    };
    let read = async {
        let mut received = Vec::new();
        server.read_to_end(&mut received).await.expect("read");
        received
    };
    let ((), received) = tokio::join!(write, read);
    assert_eq!(
        received,
        [
            0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o', 0, 0, 0, 5, b'w', b'o', b'r', b'l', b'd'
        ]
    );
}

enum WriteStep {
    Accept(usize),
    Pending,
    Error(std::io::Error),
}

struct ScriptedWrite {
    steps: VecDeque<WriteStep>,
    output: Vec<u8>,
    flushes: usize,
}

fn scripted_write(steps: impl IntoIterator<Item = WriteStep>) -> ScriptedWrite {
    ScriptedWrite {
        steps: steps.into_iter().collect(),
        output: Vec::new(),
        flushes: 0,
    }
}

impl AsyncWrite for ScriptedWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        match this
            .steps
            .pop_front()
            .unwrap_or(WriteStep::Accept(buffer.len()))
        {
            WriteStep::Accept(limit) => {
                let count = buffer.len().min(limit);
                let (accepted, _) = buffer.split_at(count);
                this.output.extend_from_slice(accepted);
                Poll::Ready(Ok(count))
            }
            WriteStep::Pending => {
                context.waker().wake_by_ref();
                Poll::Pending
            }
            WriteStep::Error(error) => Poll::Ready(Err(error)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.get_mut().flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn writer_resumes_cancelled_partial_flush_without_repeating_bytes() {
    let transport = scripted_write([
        WriteStep::Accept(3),
        WriteStep::Pending,
        WriteStep::Accept(2),
    ]);
    let mut writer = MessageWriter::with_capacity(transport, LengthDelimitedEncoder::new(16), 32);
    writer.feed(&b"abcdef"[..]).expect("feed");
    let pointer = writer.buffer().as_ptr();
    let wire = b"\0\0\0\x06abcdef";
    {
        let pending = writer.flush();
        tokio::pin!(pending);
        std::future::poll_fn(|context| {
            assert!(pending.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }

    assert_eq!(writer.transport().output.as_slice(), &wire[..3]);
    assert_eq!(&writer.buffer()[..], &wire[3..]);
    assert_eq!(writer.buffer().as_ptr(), pointer.wrapping_add(3));
    assert_eq!(writer.transport().flushes, 0);

    writer.flush().await.expect("resume flush");
    writer.feed(&b"next"[..]).expect("feed next");
    writer.flush().await.expect("next flush");

    assert_eq!(
        writer.transport().output.as_slice(),
        b"\0\0\0\x06abcdef\0\0\0\x04next"
    );
    assert!(writer.buffer().is_empty());
    assert_eq!(writer.transport().flushes, 2);
}

#[tokio::test]
async fn writer_resumes_after_partial_write_error_with_shared_buffer() {
    let transport = scripted_write([
        WriteStep::Accept(5),
        WriteStep::Error(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "scripted write failure",
        )),
        WriteStep::Accept(1),
    ]);
    let mut buffer = BytesMut::with_capacity(32);
    buffer.extend_from_slice(b"kept");
    let retained = buffer.split_to(4).freeze();
    let mut writer = MessageWriter::with_buffer(transport, LengthDelimitedEncoder::new(16), buffer);
    writer.feed(&b"abcdef"[..]).expect("feed");
    let pointer = writer.buffer().as_ptr();
    let wire = b"\0\0\0\x06abcdef";

    let error = writer.flush().await.expect_err("scripted write error");
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(writer.transport().output.as_slice(), &wire[..5]);
    assert_eq!(&writer.buffer()[..], &wire[5..]);
    assert_eq!(writer.buffer().as_ptr(), pointer.wrapping_add(5));
    assert_eq!(writer.transport().flushes, 0);

    writer.feed(&b"next"[..]).expect("feed next");
    writer.flush().await.expect("resume flush");

    assert_eq!(
        writer.transport().output.as_slice(),
        b"\0\0\0\x06abcdef\0\0\0\x04next"
    );
    assert_eq!(&retained[..], b"kept");
    assert!(writer.buffer().is_empty());
    assert_eq!(writer.transport().flushes, 1);
}

#[tokio::test]
async fn writer_retains_unwritten_suffix_after_write_zero() {
    let transport = scripted_write([WriteStep::Accept(2), WriteStep::Accept(0)]);
    let mut writer = MessageWriter::new(transport, LengthDelimitedEncoder::new(16));
    writer.feed(&b"abcdef"[..]).expect("feed");
    let wire = b"\0\0\0\x06abcdef";

    let error = writer.flush().await.expect_err("write zero");
    assert_eq!(error.kind(), std::io::ErrorKind::WriteZero);
    assert_eq!(writer.transport().output.as_slice(), &wire[..2]);
    assert_eq!(&writer.buffer()[..], &wire[2..]);
    assert_eq!(writer.transport().flushes, 0);

    writer.flush().await.expect("resume flush");

    assert_eq!(writer.transport().output.as_slice(), wire);
    assert!(writer.buffer().is_empty());
    assert_eq!(writer.transport().flushes, 1);
}

#[derive(Debug, Default)]
struct FailingEncoder;

impl MessageEncoder<()> for FailingEncoder {
    type Error = &'static str;

    fn encode(&mut self, (): (), output: &mut BytesMut) -> Result<(), Self::Error> {
        output.extend_from_slice(b"discarded");
        Err("rejected")
    }
}

#[test]
fn writer_rolls_back_failed_encoding() {
    let (client, _server) = tokio::io::duplex(8);
    let mut writer = MessageWriter::new(client, FailingEncoder);

    let error = writer.feed(()).expect_err("encoder must fail");
    assert_eq!(error, "rejected");
    assert!(writer.buffer().is_empty());
}
