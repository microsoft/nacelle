use bytes::BytesMut;
use tokio::io::AsyncRead;

use crate::{MessageDecoder, MessageReadError, MessageReader};

const DEFAULT_REPLACEMENT_CAPACITY: usize = 8 * 1024;

/// Rotates an empty reader buffer after decoding a large `BytesMut` message.
///
/// The decoded message keeps its existing allocation. Rotation drops the
/// reader's reference to that allocation and installs a smaller buffer for
/// subsequent reads. When unread bytes follow a large message, rotation waits
/// until later messages consume them.
#[derive(Debug)]
pub struct RotatingMessageReader<R, D> {
    reader: MessageReader<R, D>,
    rotation_threshold: usize,
    replacement_capacity: usize,
    rotation_pending: bool,
}

impl<R, D> RotatingMessageReader<R, D> {
    /// Wrap a message reader and replace empty buffers after messages whose
    /// capacity exceeds `rotation_threshold`.
    ///
    /// Replacement buffers start with 8 KiB of capacity.
    #[must_use]
    pub const fn new(reader: MessageReader<R, D>, rotation_threshold: usize) -> Self {
        Self::with_replacement_capacity(reader, rotation_threshold, DEFAULT_REPLACEMENT_CAPACITY)
    }

    /// Wrap a message reader with an explicit replacement-buffer capacity.
    #[must_use]
    pub const fn with_replacement_capacity(
        reader: MessageReader<R, D>,
        rotation_threshold: usize,
        replacement_capacity: usize,
    ) -> Self {
        Self {
            reader,
            rotation_threshold,
            replacement_capacity,
            rotation_pending: false,
        }
    }

    /// Return the wrapped message reader.
    #[must_use]
    pub const fn inner(&self) -> &MessageReader<R, D> {
        &self.reader
    }

    /// Return the mutable wrapped message reader.
    pub const fn inner_mut(&mut self) -> &mut MessageReader<R, D> {
        &mut self.reader
    }

    /// Consume this wrapper and return the message reader.
    #[must_use]
    pub fn into_inner(self) -> MessageReader<R, D> {
        self.reader
    }

    /// Return the message-capacity threshold that schedules rotation.
    #[must_use]
    pub const fn rotation_threshold(&self) -> usize {
        self.rotation_threshold
    }

    /// Return the initial capacity installed by a rotation.
    #[must_use]
    pub const fn replacement_capacity(&self) -> usize {
        self.replacement_capacity
    }
}

impl<R, D> RotatingMessageReader<R, D>
where
    R: AsyncRead + Unpin,
    D: MessageDecoder<Message = BytesMut>,
{
    /// Read and decode the next message, rotating an empty buffer when needed.
    ///
    /// # Errors
    ///
    /// Returns transport, decoder, progress-contract, or incomplete-EOF errors.
    #[inline]
    pub async fn read_message(&mut self) -> Result<Option<BytesMut>, MessageReadError<D::Error>> {
        let message = self.reader.read_message().await?;

        if let Some(message) = &message {
            self.rotation_pending |= message.capacity() > self.rotation_threshold;
        }
        if self.rotation_pending && self.reader.buffer().is_empty() {
            *self.reader.buffer_mut() = BytesMut::with_capacity(self.replacement_capacity);
            self.rotation_pending = false;
        }

        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::LengthDelimitedDecoder;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let len = u32::try_from(payload.len()).expect("test payload length");
        let mut frame = len.to_be_bytes().to_vec();
        frame.extend_from_slice(payload);
        frame
    }

    async fn loaded_reader(
        encoded: &[u8],
        threshold: usize,
        replacement_capacity: usize,
    ) -> RotatingMessageReader<tokio::io::DuplexStream, LengthDelimitedDecoder> {
        let (mut sender, receiver) = tokio::io::duplex(encoded.len());
        sender.write_all(encoded).await.expect("write test frames");
        sender.shutdown().await.expect("close test transport");
        RotatingMessageReader::with_replacement_capacity(
            MessageReader::new(receiver, LengthDelimitedDecoder::new(encoded.len())),
            threshold,
            replacement_capacity,
        )
    }

    #[tokio::test]
    async fn rotates_empty_tail_after_large_message() {
        let encoded = frame(&vec![7; 4096]);
        let mut reader = loaded_reader(&encoded, 1024, 128).await;

        let message = reader
            .read_message()
            .await
            .expect("read message")
            .expect("decoded message");

        assert_eq!(message.len(), 4096);
        assert_eq!(reader.inner().buffer().len(), 0);
        assert_eq!(reader.inner().buffer().capacity(), 128);
        assert!(!reader.rotation_pending);
    }

    #[tokio::test]
    async fn defers_rotation_until_coalesced_input_is_consumed() {
        let mut encoded = frame(&vec![7; 4096]);
        encoded.extend_from_slice(&frame(b"next"));
        let mut reader = loaded_reader(&encoded, 1024, 128).await;

        let first = reader
            .read_message()
            .await
            .expect("read first message")
            .expect("decoded first message");
        assert_eq!(first.len(), 4096);
        assert!(!reader.inner().buffer().is_empty());
        assert!(reader.rotation_pending);

        let second = reader
            .read_message()
            .await
            .expect("read second message")
            .expect("decoded second message");
        assert_eq!(&second[..], b"next");
        assert!(reader.inner().buffer().is_empty());
        assert_eq!(reader.inner().buffer().capacity(), 128);
        assert!(!reader.rotation_pending);
    }

    #[tokio::test]
    async fn keeps_existing_buffer_after_small_message() {
        let encoded = frame(b"small");
        let mut reader = loaded_reader(&encoded, 1024, 128).await;

        let message = reader
            .read_message()
            .await
            .expect("read message")
            .expect("decoded message");

        assert_eq!(&message[..], b"small");
        assert!(reader.inner().buffer().capacity() > 128);
        assert!(!reader.rotation_pending);
    }

    #[tokio::test]
    async fn rotates_a_large_message_without_copying_it() {
        let encoded = frame(&vec![7; 256 * 1024]);
        let mut reader = loaded_reader(&encoded, 64 * 1024, 8 * 1024).await;

        let message = reader
            .read_message()
            .await
            .expect("read message")
            .expect("decoded message");

        assert_eq!(message.len(), 256 * 1024);
        assert_eq!(message.capacity(), 256 * 1024);
        assert_eq!(message.first(), Some(&7));
        assert_eq!(message.last(), Some(&7));
        assert_eq!(reader.inner().buffer().capacity(), 8 * 1024);
    }
}
