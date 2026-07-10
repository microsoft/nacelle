//! Message-to-byte encoding contracts.

use bytes::BytesMut;

/// Encodes messages into a cumulative output buffer.
///
/// An encoder instance belongs to one connection. A failed call is rolled back
/// by [`crate::MessageWriter`], so bytes appended during that call are never
/// written. Implementations should also leave their own protocol state usable
/// after returning an error.
pub trait MessageEncoder<M> {
    /// Protocol-specific encode error.
    type Error;

    /// Append one encoded message to `output`.
    ///
    /// # Errors
    ///
    /// Returns a protocol-specific error.
    fn encode(&mut self, message: M, output: &mut BytesMut) -> Result<(), Self::Error>;
}
