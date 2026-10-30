use std::fmt;
use std::io;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::MessageDecoder;

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024;

// Keep the unbounded future's layout unchanged while sharing EOF/progress logic.
macro_rules! read_message_loop {
    ($reader:ident, $read_more:expr) => {
        loop {
            if $reader.eof_complete {
                return Ok(None);
            }
            let message = if $reader.eof {
                let before = $reader.buffer.len();
                let message = $reader
                    .decoder
                    .decode_eof(&mut $reader.buffer)
                    .map_err(MessageReadError::Decoder)?;
                validate_decode_progress(message, before, $reader.buffer.len())?
            } else {
                $reader.decode_buffered()?
            };
            if let Some(message) = message {
                return Ok(Some(message));
            }
            if $reader.eof {
                if $reader.buffer.is_empty() {
                    $reader.eof_complete = true;
                    return Ok(None);
                }
                return Err(MessageReadError::UnexpectedEof {
                    remaining: $reader.buffer.len(),
                });
            }
            $read_more?;
        }
    };
}

/// A transport, decoder, or decode-contract error.
#[derive(Debug)]
pub enum MessageReadError<E> {
    /// The asynchronous byte transport failed.
    Io(io::Error),
    /// The protocol decoder rejected input.
    Decoder(E),
    /// A decoder produced a message without consuming input.
    MessageWithoutProgress,
    /// A decoder consumed input before asking for more data.
    ConsumedOnNeedMore {
        /// Readable bytes before decoding.
        before: usize,
        /// Readable bytes after decoding.
        after: usize,
    },
    /// EOF left bytes that did not form a complete message.
    UnexpectedEof {
        /// Undecoded bytes remaining at EOF.
        remaining: usize,
    },
    /// The cumulative readable-byte budget cannot accommodate another read.
    ReadBudgetExceeded {
        /// Maximum cumulative readable bytes.
        limit: usize,
        /// Bytes already buffered, which have not been discarded.
        buffered: usize,
    },
}

impl<E> fmt::Display for MessageReadError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Decoder(error) => write!(f, "decoder error: {error}"),
            Self::MessageWithoutProgress => {
                f.write_str("decoder produced a message without consuming input")
            }
            Self::ConsumedOnNeedMore { before, after } => write!(
                f,
                "decoder requested more data after changing readable bytes from {before} to {after}"
            ),
            Self::UnexpectedEof { remaining } => {
                write!(f, "unexpected EOF with {remaining} undecoded bytes")
            }
            Self::ReadBudgetExceeded { limit, buffered } => {
                write!(
                    f,
                    "read budget of {limit} bytes exhausted with {buffered} buffered bytes"
                )
            }
        }
    }
}

impl<E> std::error::Error for MessageReadError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Decoder(error) => Some(error),
            Self::MessageWithoutProgress
            | Self::ConsumedOnNeedMore { .. }
            | Self::UnexpectedEof { .. }
            | Self::ReadBudgetExceeded { .. } => None,
        }
    }
}

/// Reads bytes into one decoder and cumulative `BytesMut`.
#[derive(Debug)]
pub struct MessageReader<R, D> {
    reader: R,
    decoder: D,
    buffer: BytesMut,
    eof: bool,
    eof_complete: bool,
}

impl<R, D> MessageReader<R, D>
where
    R: AsyncRead + Unpin,
    D: MessageDecoder,
{
    /// Create a reader with the default 8 KiB cumulative input capacity.
    #[must_use]
    pub fn new(reader: R, decoder: D) -> Self {
        Self::with_capacity(reader, decoder, DEFAULT_BUFFER_CAPACITY)
    }

    /// Create a reader with an explicit cumulative input capacity.
    #[must_use]
    pub fn with_capacity(reader: R, decoder: D, capacity: usize) -> Self {
        Self::with_buffer(reader, decoder, BytesMut::with_capacity(capacity))
    }

    /// Create a reader with a caller-provided cumulative input buffer.
    #[must_use]
    pub const fn with_buffer(reader: R, decoder: D, buffer: BytesMut) -> Self {
        Self {
            reader,
            decoder,
            buffer,
            eof: false,
            eof_complete: false,
        }
    }

    /// Read and decode the next message, or return `None` after clean EOF.
    ///
    /// # Errors
    ///
    /// Returns transport, decoder, progress-contract, or incomplete-EOF errors.
    pub async fn read_message(&mut self) -> Result<Option<D::Message>, MessageReadError<D::Error>> {
        read_message_loop!(self, self.read_more().await.map_err(MessageReadError::Io));
    }

    /// Decode the next message while bounding cumulative readable input.
    ///
    /// Buffered bytes count toward `max_readable_bytes`, including any following
    /// message or body prefix. A complete message exactly at the limit succeeds.
    /// This bounds readable plaintext, not allocation capacity or storage owned
    /// by the decoder or transport. Decoded slices retain their shared allocation.
    ///
    /// Cancellation preserves buffered input. EOF is processed only after a
    /// transport read observes it; an EOF-delimited message needs spare budget
    /// for that read.
    ///
    /// # Errors
    ///
    /// Returns the ordinary read/decode errors or
    /// [`MessageReadError::ReadBudgetExceeded`]. An initial buffer larger than
    /// the limit is rejected before decoding, without discarding its contents.
    pub async fn read_message_bounded(
        &mut self,
        max_readable_bytes: usize,
    ) -> Result<Option<D::Message>, MessageReadError<D::Error>> {
        if self.buffer.len() > max_readable_bytes {
            return Err(self.read_budget_error(max_readable_bytes));
        }
        read_message_loop!(self, self.read_more_bounded(max_readable_bytes).await);
    }

    /// Append one transport read to the input buffer without decoding it.
    ///
    /// Returns zero at EOF without polling the transport again on later calls.
    /// Use [`Self::read_message`] to process buffered input and decoder EOF logic.
    /// This operation is cancellation safe: cancelling a pending read does not
    /// consume bytes. Callers must decode between reads to enforce input limits.
    ///
    /// # Errors
    ///
    /// Returns an error if the asynchronous byte transport fails.
    pub async fn read_more(&mut self) -> io::Result<usize> {
        if self.eof {
            return Ok(0);
        }
        let read = self.reader.read_buf(&mut self.buffer).await?;
        if read == 0 {
            self.eof = true;
        }
        Ok(read)
    }

    /// Append one read without exceeding a cumulative readable-byte budget.
    ///
    /// The destination is limited even when the buffer has excess capacity.
    /// Exhaustion does not poll the transport and is not EOF. Known EOF returns
    /// zero without polling again, provided the initial buffer is within budget.
    /// A zero budget cannot discover EOF. Cancellation preserves buffered bytes.
    /// Decode between reads, normally using [`Self::read_message_bounded`].
    ///
    /// # Errors
    ///
    /// Returns an I/O error or [`MessageReadError::ReadBudgetExceeded`], leaving
    /// already buffered bytes intact.
    pub async fn read_more_bounded(
        &mut self,
        max_readable_bytes: usize,
    ) -> Result<usize, MessageReadError<D::Error>> {
        if self.buffer.len() > max_readable_bytes {
            return Err(self.read_budget_error(max_readable_bytes));
        }
        if self.eof {
            return Ok(0);
        }
        let remaining = max_readable_bytes - self.buffer.len();
        if remaining == 0 {
            return Err(self.read_budget_error(max_readable_bytes));
        }
        let read = self
            .reader
            .read_buf(&mut (&mut self.buffer).limit(remaining))
            .await
            .map_err(MessageReadError::Io)?;
        if read == 0 {
            self.eof = true;
        }
        Ok(read)
    }

    fn read_budget_error(&self, limit: usize) -> MessageReadError<D::Error> {
        MessageReadError::ReadBudgetExceeded {
            limit,
            buffered: self.buffer.len(),
        }
    }

    /// Decode one message from currently buffered bytes without reading the transport.
    ///
    /// # Errors
    ///
    /// Returns a decoder or progress-contract error.
    pub fn decode_buffered(&mut self) -> Result<Option<D::Message>, MessageReadError<D::Error>> {
        let before = self.buffer.len();
        let message = self
            .decoder
            .decode(&mut self.buffer)
            .map_err(MessageReadError::Decoder)?;
        validate_decode_progress(message, before, self.buffer.len())
    }

    /// Return a shared reference to the transport.
    #[must_use]
    pub const fn transport(&self) -> &R {
        &self.reader
    }

    /// Return a mutable reference to the transport.
    pub const fn transport_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    /// Return a shared reference to the decoder.
    #[must_use]
    pub const fn decoder(&self) -> &D {
        &self.decoder
    }

    /// Return a mutable reference to the decoder.
    pub const fn decoder_mut(&mut self) -> &mut D {
        &mut self.decoder
    }

    /// Return the cumulative input buffer.
    #[must_use]
    pub const fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Return the mutable cumulative input buffer.
    pub const fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Return mutable references to the transport and input buffer.
    pub const fn transport_and_buffer_mut(&mut self) -> (&mut R, &mut BytesMut) {
        (&mut self.reader, &mut self.buffer)
    }

    /// Replace an empty oversized input buffer with a fresh allocation.
    #[cfg(feature = "buffer-rotation")]
    pub fn rotate_empty_buffer(&mut self, capacity: usize) {
        if self.buffer.is_empty() && self.buffer.capacity() > capacity {
            self.buffer = BytesMut::with_capacity(capacity);
        }
    }

    /// Consume this reader and return its transport, decoder, and input buffer.
    #[must_use]
    pub fn into_parts(self) -> (R, D, BytesMut) {
        (self.reader, self.decoder, self.buffer)
    }
}

fn validate_decode_progress<M, E>(
    message: Option<M>,
    before: usize,
    after: usize,
) -> Result<Option<M>, MessageReadError<E>> {
    if message.is_some() && after >= before {
        return Err(MessageReadError::MessageWithoutProgress);
    }
    if message.is_none() && after != before {
        return Err(MessageReadError::ConsumedOnNeedMore { before, after });
    }
    Ok(message)
}
