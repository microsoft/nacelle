use bytes::{Bytes, BytesMut};

use nacelle_core::error::NacelleError;
use nacelle_core::request::RequestMetadata;
use nacelle_core::response::TcpResponseMeta;

#[derive(Debug)]
pub struct DecodedRequest<Req> {
    pub request: Req,
    pub body_len: usize,
}

/// Translates one TCP wire protocol into Nacelle's app-facing request/response model.
///
/// Implementations decode request heads from bytes, expose low-cardinality
/// request metadata to the app core, and encode [`nacelle_core::response::NacelleResponse`]
/// bodies back into protocol frames. Protocols should stay focused on wire
/// translation; application behavior belongs in the [`nacelle_core::handler::Handler`].
pub trait Protocol<Req>: Send + Sync + 'static
where
    Req: RequestMetadata,
{
    type ResponseContext: Send + 'static;
    type ErrorContext: Send + 'static;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    fn decode_head(
        &self,
        src: &mut BytesMut,
        max_frame_len: usize,
    ) -> Result<Option<DecodedRequest<Req>>, NacelleError>;

    fn response_context(&self, req: &Req) -> Self::ResponseContext;

    fn error_context(&self, req: &Req) -> Self::ErrorContext;

    fn apply_tcp_response_meta(
        &self,
        _context: &mut Self::ResponseContext,
        _meta: &TcpResponseMeta,
    ) {
    }

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError>;

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)?;
        self.encode_response_end(context, dst)
    }

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError>;

    fn encode_error(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError>;
}
