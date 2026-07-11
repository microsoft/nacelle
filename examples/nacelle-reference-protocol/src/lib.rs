//! Example length-delimited protocol used by Nacelle examples and tests.

use std::convert::Infallible;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::pipeline::ConnectionInfo;
use nacelle_core::{NacelleBody, NacelleError};
use nacelle_tcp::{DecodedMessage, DecodedRequest, FrameBuffer, Protocol, TcpResponse};

const HEADER_LEN: usize = 24;
const FIXED_FRAME_FIELDS_LEN: usize = HEADER_LEN - 4;
pub const FRAME_FLAG_START: u32 = 0b0001;
pub const FRAME_FLAG_END: u32 = 0b0010;
pub const FRAME_FLAG_ERROR: u32 = 0b0100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameRequest {
    pub request_id: u64,
    pub opcode: u64,
    pub flags: u32,
    pub body_len: usize,
}

#[derive(Debug, Clone, Default)]
pub struct LengthDelimitedProtocol;

#[derive(Debug, Clone)]
pub struct LengthDelimitedRequestDecoder {
    max_frame_len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct FrameResponseContext {
    request_id: u64,
    opcode: u64,
    started: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct FrameErrorContext {
    request_id: u64,
    opcode: u64,
}

impl LengthDelimitedProtocol {
    pub fn encode_request_frame(
        &self,
        request_id: u64,
        opcode: u64,
        flags: u32,
        body: &[u8],
    ) -> Result<Bytes, NacelleError> {
        let mut dst = BytesMut::with_capacity(HEADER_LEN + body.len());
        let mut frame = FrameBuffer::new(&mut dst, HEADER_LEN + body.len());
        encode_frame(request_id, opcode, flags, body, &mut frame)?;
        Ok(dst.freeze())
    }
}

impl MessageDecoder for LengthDelimitedRequestDecoder {
    type Message = DecodedMessage<FrameRequest, Infallible>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let frame_len =
            u32::from_le_bytes(src[0..4].try_into().expect("slice length checked")) as usize;
        if frame_len < FIXED_FRAME_FIELDS_LEN {
            return Err(NacelleError::InvalidFrame(
                "frame length is smaller than the fixed header",
            ));
        }
        if frame_len > self.max_frame_len {
            return Err(NacelleError::FrameTooLarge {
                len: frame_len,
                max: self.max_frame_len,
            });
        }
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let request_id = u64::from_le_bytes(src[4..12].try_into().expect("slice length checked"));
        let opcode = u64::from_le_bytes(src[12..20].try_into().expect("slice length checked"));
        let flags = u32::from_le_bytes(src[20..24].try_into().expect("slice length checked"));
        drop(src.split_to(HEADER_LEN));
        let body_len = frame_len - FIXED_FRAME_FIELDS_LEN;

        Ok(Some(DecodedMessage::Request(DecodedRequest {
            request: FrameRequest {
                request_id,
                opcode,
                flags,
                body_len,
            },
            body_len,
        })))
    }
}

impl Protocol for LengthDelimitedProtocol {
    type Request = FrameRequest;
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = ();
    type Decoder = LengthDelimitedRequestDecoder;
    type ResponseContext = FrameResponseContext;
    type ErrorContext = FrameErrorContext;

    fn decoder(&self, max_frame_len: usize) -> Self::Decoder {
        LengthDelimitedRequestDecoder { max_frame_len }
    }

    fn connection_state(&self, _: &ConnectionInfo) {}

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        HEADER_LEN + body_len
    }

    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
        match *request {}
    }

    fn response_context(&self, req: &FrameRequest) -> Self::ResponseContext {
        FrameResponseContext {
            request_id: req.request_id,
            opcode: req.opcode,
            started: false,
        }
    }

    fn error_context(&self, req: &FrameRequest) -> Self::ErrorContext {
        FrameErrorContext {
            request_id: req.request_id,
            opcode: req.opcode,
        }
    }

    fn apply_response(&self, _context: &mut Self::ResponseContext, _response: &Self::Response) {}

    fn max_response_frame_overhead(&self) -> usize {
        HEADER_LEN
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        response.body
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        let mut flags = 0;
        if !context.started {
            flags |= FRAME_FLAG_START;
            context.started = true;
        }

        encode_frame(context.request_id, context.opcode, flags, &chunk, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        let mut flags = FRAME_FLAG_END;
        if !context.started {
            flags |= FRAME_FLAG_START;
            context.started = true;
        }

        encode_frame(context.request_id, context.opcode, flags, &[], dst)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        let mut flags = FRAME_FLAG_END;
        if !context.started {
            flags |= FRAME_FLAG_START;
            context.started = true;
        }

        encode_frame(context.request_id, context.opcode, flags, &chunk, dst)
    }

    fn encode_error(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        let (request_id, opcode) = context
            .map(|context| (context.request_id, context.opcode))
            .unwrap_or((0, 0));
        let message = error.to_string();
        encode_frame(
            request_id,
            opcode,
            FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR,
            message.as_bytes(),
            dst,
        )
    }
}

fn encode_frame(
    request_id: u64,
    opcode: u64,
    flags: u32,
    body: &[u8],
    dst: &mut FrameBuffer<'_>,
) -> Result<(), NacelleError> {
    let frame_len = FIXED_FRAME_FIELDS_LEN + body.len();
    let frame_len = u32::try_from(frame_len).map_err(|_| NacelleError::FrameTooLarge {
        len: frame_len,
        max: u32::MAX as usize,
    })?;
    dst.put_u32_le(frame_len)?;
    dst.put_u64_le(request_id)?;
    dst.put_u64_le(opcode)?;
    dst.put_u32_le(flags)?;
    dst.extend_from_slice(body)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_partial_head_incrementally() {
        let protocol = LengthDelimitedProtocol;
        let frame = protocol
            .encode_request_frame(7, 42, 0, b"hello")
            .expect("frame encoded");
        let mut decoder = protocol.decoder(1024);

        let mut buf = BytesMut::from(&frame[..10]);
        assert!(
            decoder
                .decode(&mut buf)
                .expect("decode should succeed")
                .is_none()
        );

        buf.extend_from_slice(&frame[10..]);
        let decoded = decoder
            .decode(&mut buf)
            .expect("decode should succeed")
            .expect("head should decode");
        let decoded = match decoded {
            DecodedMessage::Request(decoded) => decoded,
            DecodedMessage::OneWay(decoded) => match decoded.request {},
        };
        assert_eq!(decoded.request.request_id, 7);
        assert_eq!(decoded.request.opcode, 42);
        assert_eq!(decoded.body_len, 5);
        assert_eq!(&buf[..5], b"hello");
    }

    #[test]
    fn rejects_malformed_frame_lengths() {
        let protocol = LengthDelimitedProtocol;
        let mut decoder = protocol.decoder(1024);
        let mut buf = BytesMut::from(&[4_u8, 0, 0, 0][..]);
        let error = decoder.decode(&mut buf).expect_err("frame must fail");
        assert!(matches!(error, NacelleError::InvalidFrame(_)));
    }
}
