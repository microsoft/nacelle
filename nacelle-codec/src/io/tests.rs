use std::convert::Infallible;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
