use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use nacelle_codec::MessageDecoder;

use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::{
    ConnectionContext, ConnectionInfo, Handler, RequestContext, RequiredCompletion,
    RequiredResponder, Respond,
};
use nacelle_core::request::NacelleBody;
use nacelle_core::request::NacelleConnectionMeta;

#[derive(Debug)]
pub struct DecodedRequest<Req> {
    pub request: Req,
    pub body_len: usize,
}

/// Bounded response-frame encoder backed by runtime-accounted storage.
pub struct FrameBuffer<'buffer> {
    inner: &'buffer mut BytesMut,
    max_len: usize,
}

impl<'buffer> FrameBuffer<'buffer> {
    /// Wrap response-frame storage with its maximum encoded length.
    pub const fn new(inner: &'buffer mut BytesMut, max_len: usize) -> Self {
        Self { inner, max_len }
    }

    /// Current encoded frame length.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Return whether the encoded frame is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Append bytes if they fit inside the declared frame bound.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), NacelleError> {
        self.ensure_capacity(bytes.len())?;
        self.inner.extend_from_slice(bytes);
        Ok(())
    }

    /// Append one little-endian `u32`.
    pub fn put_u32_le(&mut self, value: u32) -> Result<(), NacelleError> {
        self.ensure_capacity(std::mem::size_of::<u32>())?;
        self.inner.put_u32_le(value);
        Ok(())
    }

    /// Append one little-endian `u64`.
    pub fn put_u64_le(&mut self, value: u64) -> Result<(), NacelleError> {
        self.ensure_capacity(std::mem::size_of::<u64>())?;
        self.inner.put_u64_le(value);
        Ok(())
    }

    fn ensure_capacity(&self, additional: usize) -> Result<(), NacelleError> {
        let next = self
            .inner
            .len()
            .checked_add(additional)
            .ok_or(NacelleError::ResourceLimit("response_frame_bytes"))?;
        if next > self.max_len {
            return Err(NacelleError::ResourceLimit("response_frame_bytes"));
        }
        Ok(())
    }
}

/// Application-facing TCP request containing a protocol head and body stream.
pub struct TcpRequest<Request> {
    /// Protocol-specific decoded request head.
    pub head: Request,
    /// Bounded request body supplied by the TCP runtime.
    pub body: NacelleBody,
}

/// Default application-facing TCP response.
pub struct TcpResponse {
    /// Response body encoded by the originating protocol.
    pub body: NacelleBody,
}

impl TcpResponse {
    /// Construct a response body with inherited protocol metadata.
    pub fn new(body: NacelleBody) -> Self {
        Self { body }
    }

    /// Construct a byte response with inherited protocol metadata.
    pub fn bytes(bytes: impl Into<Bytes>) -> Self {
        Self::new(NacelleBody::bytes(bytes))
    }

    /// Construct an empty response.
    pub fn empty() -> Self {
        Self::new(NacelleBody::empty())
    }
}

/// Zero-allocation response capability for one decoded TCP request.
#[derive(Debug)]
pub struct TcpResponder<Response, ResponseContext> {
    response_context: ResponseContext,
    _response: PhantomData<fn(Response)>,
}

impl<Response, ResponseContext> TcpResponder<Response, ResponseContext> {
    pub(crate) const fn new(response_context: ResponseContext) -> Self {
        Self {
            response_context,
            _response: PhantomData,
        }
    }
}

/// Typed response and protocol context returned to the connection loop.
#[must_use = "TCP completion must be encoded by the connection loop"]
#[derive(Debug)]
pub struct TcpCompletion<Response, ResponseContext> {
    pub(crate) response: Response,
    pub(crate) response_context: ResponseContext,
}

/// Concrete application context for one required-response TCP request.
pub type TcpRequestContext<P> = RequestContext<
    TcpRequest<<P as Protocol>::Request>,
    RequiredResponder<TcpResponder<<P as Protocol>::Response, <P as Protocol>::ResponseContext>>,
    (),
    ConnectionContext<Arc<<P as Protocol>::ConnectionState>>,
>;

/// Successful completion required from a typed TCP handler.
pub type TcpHandlerCompletion<P> =
    RequiredCompletion<TcpCompletion<<P as Protocol>::Response, <P as Protocol>::ResponseContext>>;

/// Statically dispatched application handler for one TCP protocol.
pub trait TcpHandler<P>:
    Handler<TcpRequestContext<P>, Completion = TcpHandlerCompletion<P>, Error = NacelleError>
where
    P: Protocol,
{
}

impl<P, H> TcpHandler<P> for H
where
    P: Protocol,
    H: Handler<TcpRequestContext<P>, Completion = TcpHandlerCompletion<P>, Error = NacelleError>,
{
}

impl<Response, ResponseContext> Respond for TcpResponder<Response, ResponseContext> {
    type Response = Response;
    type Completion = TcpCompletion<Response, ResponseContext>;
    type Error = NacelleError;

    async fn respond(self, response: Self::Response) -> Result<Self::Completion, Self::Error> {
        Ok(TcpCompletion {
            response,
            response_context: self.response_context,
        })
    }
}

/// Translates one TCP wire protocol into typed application requests and responses.
///
/// Implementations decode request heads, select request limits, and encode only
/// their associated [`Protocol::Response`] type. Application behavior runs
/// through a statically dispatched [`TcpHandler`] and cannot return an HTTP or
/// other transport response by mistake.
pub trait Protocol: Send + Sync + 'static {
    /// Decoded request head for this wire protocol.
    type Request: Send + 'static;
    /// Application response accepted by this protocol.
    type Response: Send + 'static;
    /// Concrete state shared by requests on one accepted connection.
    type ConnectionState: Send + Sync + 'static;
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

    /// Construct connection state once after accept/TLS handshake.
    fn connection_state(&self, connection: &ConnectionInfo) -> Self::ConnectionState;

    /// Return total wire bytes for this request, including protocol framing.
    fn request_wire_bytes(&self, request: &Self::Request, body_len: usize) -> usize;

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

    /// Apply protocol-specific response values before body encoding.
    fn apply_response(&self, context: &mut Self::ResponseContext, response: &Self::Response);

    /// Maximum framing bytes added around one encoded response chunk.
    fn max_response_frame_overhead(&self) -> usize;

    /// Extract the streaming body from a typed protocol response.
    fn response_body(&self, response: Self::Response) -> NacelleBody;

    fn encode_response_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError>;

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError>;

    fn encode_response_end(
        &self,
        context: &mut Self::ResponseContext,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError>;

    fn encode_error(
        &self,
        context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError>;
}
