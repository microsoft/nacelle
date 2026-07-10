//! Per-connection byte-to-message decoder contract.

use bytes::BytesMut;

/// Converts cumulative inbound bytes into messages.
///
/// A decoder instance belongs to one connection. Return `Ok(None)` without
/// consuming input when a complete message has not arrived.
pub trait MessageDecoder {
    /// Decoded message type.
    type Message;
    /// Protocol-specific decode error.
    type Error;

    /// Attempt to decode one message from cumulative input.
    ///
    /// # Errors
    ///
    /// Returns a protocol-specific error for malformed input.
    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error>;

    /// Attempt to decode one final message after transport EOF.
    ///
    /// The default delegates to [`Self::decode`].
    ///
    /// # Errors
    ///
    /// Returns a protocol-specific error for malformed final input.
    fn decode_eof(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        self.decode(input)
    }
}
