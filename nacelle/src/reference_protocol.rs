use bytes::{BufMut, Bytes, BytesMut};

use crate::MessageDecoder;
use crate::util::checked_u32_len;
use nacelle_core::error::NacelleError;
use nacelle_core::request::{RequestMetadata, TcpRequestMeta};
use nacelle_tcp::protocol::{DecodedRequest, Protocol};

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

impl RequestMetadata for FrameRequest {
    fn opcode(&self) -> u64 {
        self.opcode
    }

    fn tcp_meta(&self, _body_len: usize) -> TcpRequestMeta {
        TcpRequestMeta {
            request_id: Some(self.request_id),
            opcode: self.opcode,
            flags: self.flags,
            body_len: self.body_len,
        }
    }
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
        encode_frame(request_id, opcode, flags, body, &mut dst)?;
        Ok(dst.freeze())
    }
}

impl MessageDecoder for LengthDelimitedRequestDecoder {
    type Message = DecodedRequest<FrameRequest>;
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

        Ok(Some(DecodedRequest {
            request: FrameRequest {
                request_id,
                opcode,
                flags,
                body_len,
            },
            body_len,
        }))
    }
}

impl Protocol<FrameRequest> for LengthDelimitedProtocol {
    type Decoder = LengthDelimitedRequestDecoder;
    type ResponseContext = FrameResponseContext;
    type ErrorContext = FrameErrorContext;

    fn decoder(&self, max_frame_len: usize) -> Self::Decoder {
        LengthDelimitedRequestDecoder { max_frame_len }
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

    fn apply_tcp_response_meta(
        &self,
        context: &mut Self::ResponseContext,
        meta: &nacelle_core::response::TcpResponseMeta,
    ) {
        if let Some(request_id) = meta.request_id {
            context.request_id = request_id;
        }
        if let Some(opcode) = meta.opcode {
            context.opcode = opcode;
        }
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
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
        dst: &mut BytesMut,
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
        dst: &mut BytesMut,
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
        dst: &mut BytesMut,
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
    dst: &mut BytesMut,
) -> Result<(), NacelleError> {
    let frame_len = FIXED_FRAME_FIELDS_LEN + body.len();
    let frame_len = checked_u32_len(frame_len)?;
    dst.reserve(HEADER_LEN + body.len());
    dst.put_u32_le(frame_len);
    dst.put_u64_le(request_id);
    dst.put_u64_le(opcode);
    dst.put_u32_le(flags);
    dst.extend_from_slice(body);
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
