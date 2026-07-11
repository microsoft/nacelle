use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;

use nacelle_core::error::NacelleError;
use nacelle_core::request::NacelleConnectionMeta;
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
pub trait Protocol: Send + Sync + 'static {
    /// Decoded request head for this wire protocol.
    type Request: Send + 'static;
    type Decoder: MessageDecoder<Message = DecodedRequest<Self::Request>, Error = NacelleError>
        + Send
        + 'static;
    type ResponseContext: Send + 'static;
    type ErrorContext: Send + 'static;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Create a decoder for one connection.
    fn decoder(&self, max_frame_len: usize) -> Self::Decoder;

    /// Build transport-neutral metadata while the legacy detached handler path
    /// is being deleted from the connection loop.
    fn request_meta(
        &self,
        request: &Self::Request,
        body_len: usize,
    ) -> nacelle_core::TcpRequestMeta;

    /// Select the body limit after decoding the request head.
    fn max_request_body_bytes(
        &self,
        _request: &Self::Request,
        _connection: &NacelleConnectionMeta,
        default_limit: usize,
    ) -> usize {
        default_limit
    }

    fn response_context(&self, req: &Self::Request) -> Self::ResponseContext;

    fn error_context(&self, req: &Self::Request) -> Self::ErrorContext;

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
