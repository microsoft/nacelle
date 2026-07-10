use std::fmt;
use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::MessageDecoder;

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024;

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
            | Self::UnexpectedEof { .. } => None,
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
        loop {
            if self.eof_complete {
                return Ok(None);
            }

            let before = self.buffer.len();
            let message = if self.eof {
                self.decoder.decode_eof(&mut self.buffer)
            } else {
                self.decoder.decode(&mut self.buffer)
            }
            .map_err(MessageReadError::Decoder)?;
            let after = self.buffer.len();

            if let Some(message) = message {
                if after >= before {
                    return Err(MessageReadError::MessageWithoutProgress);
                }
                return Ok(Some(message));
            }
            if after != before {
                return Err(MessageReadError::ConsumedOnNeedMore { before, after });
            }

            if self.eof {
                if self.buffer.is_empty() {
                    self.eof_complete = true;
                    return Ok(None);
                }
                return Err(MessageReadError::UnexpectedEof {
                    remaining: self.buffer.len(),
                });
            }

            let read = self
                .reader
                .read_buf(&mut self.buffer)
                .await
                .map_err(MessageReadError::Io)?;
            if read == 0 {
                self.eof = true;
            }
        }
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

    /// Consume this reader and return its transport, decoder, and input buffer.
    #[must_use]
    pub fn into_parts(self) -> (R, D, BytesMut) {
        (self.reader, self.decoder, self.buffer)
    }
}
