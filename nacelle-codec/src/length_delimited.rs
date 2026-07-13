//! Four-byte length-delimited message encoding and decoding.

use std::fmt;

use bytes::{Buf, BytesMut};

use crate::{MessageDecoder, MessageEncoder};

const HEADER_LEN: usize = 4;

/// Decodes a big-endian `u32` payload length followed by that payload.
#[derive(Debug, Clone, Copy)]
pub struct LengthDelimitedDecoder {
    max_frame_len: usize,
    little_endian: bool,
}

impl LengthDelimitedDecoder {
    /// Create a big-endian length-delimited decoder.
    #[must_use]
    pub const fn new(max_frame_len: usize) -> Self {
        Self {
            max_frame_len,
            little_endian: false,
        }
    }

    /// Decode the four-byte length field as little-endian.
    #[must_use]
    pub const fn with_little_endian(mut self) -> Self {
        self.little_endian = true;
        self
    }

    /// Return the configured maximum payload length.
    #[must_use]
    pub const fn max_frame_len(self) -> usize {
        self.max_frame_len
    }
}

/// A length-delimited decode error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LengthDelimitedError {
    /// The declared payload exceeds the configured frame limit.
    FrameTooLarge {
        /// Declared payload length.
        len: usize,
        /// Configured maximum payload length.
        max: usize,
    },
    /// The frame length cannot be represented on this platform.
    LengthOverflow,
}

impl fmt::Display for LengthDelimitedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { len, max } => {
                write!(f, "frame length {len} exceeds limit {max}")
            }
            Self::LengthOverflow => f.write_str("frame length overflow"),
        }
    }
}

impl std::error::Error for LengthDelimitedError {}

impl MessageDecoder for LengthDelimitedDecoder {
    type Message = BytesMut;
    type Error = LengthDelimitedError;

    #[inline]
    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let Some(header) = input.get(..HEADER_LEN) else {
            return Ok(None);
        };
        let length_bytes: [u8; HEADER_LEN] = header
            .try_into()
            .map_err(|_| LengthDelimitedError::LengthOverflow)?;
        let payload_len = if self.little_endian {
            u32::from_le_bytes(length_bytes)
        } else {
            u32::from_be_bytes(length_bytes)
        } as usize;
        if payload_len > self.max_frame_len {
            return Err(LengthDelimitedError::FrameTooLarge {
                len: payload_len,
                max: self.max_frame_len,
            });
        }
        let frame_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(LengthDelimitedError::LengthOverflow)?;
        if input.len() < frame_len {
            return Ok(None);
        }

        let mut payload = input.split_to(frame_len);
        payload.advance(HEADER_LEN);
        Ok(Some(payload))
    }
}

/// Encodes a `u32` payload length followed by the payload bytes.
#[derive(Debug, Clone, Copy)]
pub struct LengthDelimitedEncoder {
    max_frame_len: usize,
    little_endian: bool,
}

impl LengthDelimitedEncoder {
    /// Create a big-endian length-delimited encoder.
    #[must_use]
    pub const fn new(max_frame_len: usize) -> Self {
        Self {
            max_frame_len,
            little_endian: false,
        }
    }

    /// Encode the four-byte length field as little-endian.
    #[must_use]
    pub const fn with_little_endian(mut self) -> Self {
        self.little_endian = true;
        self
    }

    /// Return the configured maximum payload length.
    #[must_use]
    pub const fn max_frame_len(self) -> usize {
        self.max_frame_len
    }
}

/// A length-delimited encode error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LengthDelimitedEncodeError {
    /// The payload exceeds the configured frame limit.
    FrameTooLarge {
        /// Payload length.
        len: usize,
        /// Configured maximum payload length.
        max: usize,
    },
    /// The payload length cannot be represented by the four-byte header.
    LengthOverflow,
}

impl fmt::Display for LengthDelimitedEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { len, max } => {
                write!(f, "frame length {len} exceeds limit {max}")
            }
            Self::LengthOverflow => f.write_str("frame length cannot be represented as u32"),
        }
    }
}

impl std::error::Error for LengthDelimitedEncodeError {}

impl<M> MessageEncoder<M> for LengthDelimitedEncoder
where
    M: AsRef<[u8]>,
{
    type Error = LengthDelimitedEncodeError;

    #[inline]
    fn encode(&mut self, message: M, output: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = message.as_ref();
        if payload.len() > self.max_frame_len {
            return Err(LengthDelimitedEncodeError::FrameTooLarge {
                len: payload.len(),
                max: self.max_frame_len,
            });
        }
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| LengthDelimitedEncodeError::LengthOverflow)?;
        output.reserve(HEADER_LEN.saturating_add(payload.len()));
        if self.little_endian {
            output.extend_from_slice(&payload_len.to_le_bytes());
        } else {
            output.extend_from_slice(&payload_len.to_be_bytes());
        }
        output.extend_from_slice(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let len = u32::try_from(payload.len()).expect("test payload length");
        let mut frame = len.to_be_bytes().to_vec();
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn decodes_fragmented_and_coalesced_messages() {
        let mut decoder = LengthDelimitedDecoder::new(1024);
        let hello = frame(b"hello");
        let (first_fragment, second_fragment) = hello.split_at(3);
        let mut input = BytesMut::from(first_fragment);
        assert!(
            decoder
                .decode(&mut input)
                .expect("partial decode")
                .is_none()
        );

        input.extend_from_slice(second_fragment);
        input.extend_from_slice(&frame(b"world"));
        assert_eq!(
            &decoder.decode(&mut input).expect("decode").expect("first")[..],
            b"hello"
        );
        assert_eq!(
            &decoder.decode(&mut input).expect("decode").expect("second")[..],
            b"world"
        );
        assert!(decoder.decode(&mut input).expect("empty decode").is_none());
    }

    #[test]
    fn rejects_oversized_frame_without_consuming_header() {
        let mut decoder = LengthDelimitedDecoder::new(4);
        let mut input = BytesMut::from(frame(b"hello").as_slice());

        let error = decoder.decode(&mut input).expect_err("oversized frame");
        assert_eq!(
            error,
            LengthDelimitedError::FrameTooLarge { len: 5, max: 4 }
        );
        assert_eq!(input.len(), 9);
    }

    #[test]
    fn supports_little_endian_and_empty_payloads() {
        let mut decoder = LengthDelimitedDecoder::new(0).with_little_endian();
        let mut input = BytesMut::from(&0_u32.to_le_bytes()[..]);

        assert!(
            decoder
                .decode(&mut input)
                .expect("decode")
                .expect("empty message")
                .is_empty()
        );
    }

    #[test]
    fn encodes_big_and_little_endian_frames() {
        let mut big = BytesMut::new();
        LengthDelimitedEncoder::new(16)
            .encode(&b"hello"[..], &mut big)
            .expect("big-endian encode");
        assert_eq!(&big[..], &[0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']);

        let mut little = BytesMut::new();
        LengthDelimitedEncoder::new(16)
            .with_little_endian()
            .encode(&b"hello"[..], &mut little)
            .expect("little-endian encode");
        assert_eq!(&little[..], &[5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn oversized_payload_does_not_modify_output() {
        let mut output = BytesMut::from(&b"kept"[..]);
        let error = LengthDelimitedEncoder::new(4)
            .encode(&b"hello"[..], &mut output)
            .expect_err("oversized frame");

        assert_eq!(
            error,
            LengthDelimitedEncodeError::FrameTooLarge { len: 5, max: 4 }
        );
        assert_eq!(&output[..], b"kept");
    }
}
