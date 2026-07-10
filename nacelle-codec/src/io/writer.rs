use std::fmt;
use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::MessageEncoder;

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024;

/// A transport or encoder error while writing a message.
#[derive(Debug)]
pub enum MessageWriteError<E> {
    /// The asynchronous byte transport failed.
    Io(io::Error),
    /// The protocol encoder rejected a message.
    Encoder(E),
}

impl<E> fmt::Display for MessageWriteError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Encoder(error) => write!(f, "encoder error: {error}"),
        }
    }
}

impl<E> std::error::Error for MessageWriteError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Encoder(error) => Some(error),
        }
    }
}

/// Encodes messages into one cumulative `BytesMut` and writes them.
#[derive(Debug)]
pub struct MessageWriter<W, E> {
    writer: W,
    encoder: E,
    buffer: BytesMut,
}

impl<W, E> MessageWriter<W, E>
where
    W: AsyncWrite + Unpin,
{
    /// Create a writer with the default 8 KiB cumulative output capacity.
    #[must_use]
    pub fn new(writer: W, encoder: E) -> Self {
        Self::with_capacity(writer, encoder, DEFAULT_BUFFER_CAPACITY)
    }

    /// Create a writer with an explicit cumulative output capacity.
    #[must_use]
    pub fn with_capacity(writer: W, encoder: E, capacity: usize) -> Self {
        Self::with_buffer(writer, encoder, BytesMut::with_capacity(capacity))
    }

    /// Create a writer with a caller-provided cumulative output buffer.
    #[must_use]
    pub const fn with_buffer(writer: W, encoder: E, buffer: BytesMut) -> Self {
        Self {
            writer,
            encoder,
            buffer,
        }
    }

    /// Encode one message into the cumulative output buffer.
    ///
    /// Bytes appended during a failed encoder call are rolled back.
    ///
    /// # Errors
    ///
    /// Returns the protocol encoder error.
    pub fn feed<M>(&mut self, message: M) -> Result<(), E::Error>
    where
        E: MessageEncoder<M>,
    {
        let checkpoint = self.buffer.len();
        if let Err(error) = self.encoder.encode(message, &mut self.buffer) {
            self.buffer.truncate(checkpoint);
            return Err(error);
        }
        Ok(())
    }

    /// Encode one message, write all queued bytes, and flush the transport.
    ///
    /// # Errors
    ///
    /// Returns a transport or protocol encoder error.
    pub async fn send<M>(&mut self, message: M) -> Result<(), MessageWriteError<E::Error>>
    where
        E: MessageEncoder<M>,
    {
        self.feed(message).map_err(MessageWriteError::Encoder)?;
        self.flush().await.map_err(MessageWriteError::Io)
    }

    /// Write all queued bytes and flush the transport.
    ///
    /// # Errors
    ///
    /// Returns a transport error.
    pub async fn flush(&mut self) -> io::Result<()> {
        while !self.buffer.is_empty() {
            let written = self.writer.write(&self.buffer).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "transport accepted no queued bytes",
                ));
            }
            drop(self.buffer.split_to(written));
        }
        self.writer.flush().await
    }

    /// Flush queued bytes and shut down the write side of the transport.
    ///
    /// # Errors
    ///
    /// Returns a transport error.
    pub async fn shutdown(&mut self) -> io::Result<()> {
        self.flush().await?;
        self.writer.shutdown().await
    }

    /// Return a shared reference to the transport.
    #[must_use]
    pub const fn transport(&self) -> &W {
        &self.writer
    }

    /// Return a mutable reference to the transport.
    pub const fn transport_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// Return a shared reference to the encoder.
    #[must_use]
    pub const fn encoder(&self) -> &E {
        &self.encoder
    }

    /// Return a mutable reference to the encoder.
    pub const fn encoder_mut(&mut self) -> &mut E {
        &mut self.encoder
    }

    /// Return the cumulative output buffer.
    #[must_use]
    pub const fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Return the mutable cumulative output buffer.
    pub const fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Consume this writer and return its transport, encoder, and output buffer.
    #[must_use]
    pub fn into_parts(self) -> (W, E, BytesMut) {
        (self.writer, self.encoder, self.buffer)
    }
}
