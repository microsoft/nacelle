use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use nacelle_codec::MessageDecoder;

use nacelle_core::error::{NacelleError, NacelleResourceLimitReason};
use nacelle_core::pipeline::{
    Completed, ConnectionContext, ConnectionInfo, Handler, LocalHandler, NoResponse,
    RequestContext, RequiredCompletion, RequiredResponder, Respond,
};
use nacelle_core::request::NacelleBody;

#[derive(Debug)]
pub struct DecodedRequest<Req> {
    pub request: Req,
    pub body_len: usize,
}

/// One decoded protocol message classified before application dispatch.
#[derive(Debug)]
pub enum DecodedMessage<Request, OneWayRequest> {
    /// Message whose handler must produce a typed response completion.
    Request(DecodedRequest<Request>),
    /// Message whose handler cannot produce transport output.
    OneWay(DecodedRequest<OneWayRequest>),
}

/// Bounded response-frame encoder backed by runtime-accounted storage.
pub struct FrameBuffer<'buffer> {
    inner: &'buffer mut BytesMut,
    start_len: usize,
    max_len: usize,
}

impl<'buffer> FrameBuffer<'buffer> {
    /// Wrap response-frame storage with its maximum encoded length.
    pub const fn new(inner: &'buffer mut BytesMut, max_len: usize) -> Self {
        Self {
            inner,
            start_len: 0,
            max_len,
        }
    }

    pub(crate) fn append_to(inner: &'buffer mut BytesMut, max_len: usize) -> Self {
        let start_len = inner.len();
        Self {
            inner,
            start_len,
            max_len,
        }
    }

    /// Current encoded frame length.
    pub fn len(&self) -> usize {
        self.inner.len().saturating_sub(self.start_len)
    }

    /// Return whether the encoded frame is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
            .len()
            .checked_add(additional)
            .ok_or(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ResponseFrameBytes,
            ))?;
        if next > self.max_len {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ResponseFrameBytes,
            ));
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

/// Delivery stage reported when a required response fails to reach the peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseDeliveryPhase {
    /// The response failed while its streaming body produced the next chunk.
    ResponseBody,
    /// The response failed while being encoded into a wire frame.
    Encode,
    /// The response failed while its encoded bytes were written to the socket.
    SocketWrite,
    /// The response failed while the transport write was being flushed.
    SocketFlush,
}

/// Reason a required response was aborted before a delivery outcome was known.
///
/// An abort is distinct from a delivery failure: no transport error was
/// produced for this response. Nacelle settles the completion item with an
/// abort when it accepted ownership but the connection was torn down before the
/// final write and flush could be attempted or observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResponseAbortReason {
    /// The connection task was cancelled or dropped before delivery completed.
    Cancelled,
    /// The connection closed before delivery completed.
    ConnectionClosed,
    /// The runtime began shutting down before delivery completed.
    Shutdown,
}

/// Transport-only outcome reported to a request-scoped completion item after a
/// required response is delivered, its delivery fails, or it is aborted.
///
/// Nacelle reports transport facts exclusively. The application owns any
/// higher-level interpretation such as logs, metrics, or product events.
///
/// `encoded_wire_bytes` is the number of protocol wire bytes Nacelle encoded
/// for this response, including framing. `written_wire_bytes` is the subset
/// accepted by the transport's `AsyncWrite` before the outcome was known. Both
/// exclude TLS record, TCP, IP, and link-layer overhead, and neither replaces
/// an application-owned response payload length.
#[derive(Debug)]
pub enum ResponseDeliveryOutcome<'error> {
    /// The response was fully encoded, written, and flushed to the transport.
    Delivered {
        /// Protocol wire bytes encoded for this response, including framing.
        encoded_wire_bytes: usize,
        /// Wire bytes accepted by the transport before delivery completed.
        written_wire_bytes: usize,
    },
    /// The response could not be delivered.
    Failed {
        /// Stage at which delivery failed.
        phase: ResponseDeliveryPhase,
        /// Protocol wire bytes encoded before the failure.
        encoded_wire_bytes: usize,
        /// Wire bytes accepted by the transport before the failure.
        written_wire_bytes: usize,
        /// The transport error returned by the connection loop.
        error: &'error NacelleError,
    },
    /// Delivery was aborted before a transport outcome was known.
    Aborted {
        /// Why the response was aborted.
        reason: ResponseAbortReason,
        /// Protocol wire bytes encoded before the abort.
        encoded_wire_bytes: usize,
        /// Wire bytes accepted by the transport before the abort.
        written_wire_bytes: usize,
    },
}

/// Boxed one-shot completion item invoked exactly once after response delivery.
///
/// Only handlers that attach an item allocate this box; the default response
/// path stores `None` and pays no allocation or callback cost.
pub type ResponseCompletionCallback =
    Box<dyn for<'outcome> FnOnce(ResponseDeliveryOutcome<'outcome>) + Send>;

/// Typed response and protocol context returned to the connection loop.
#[must_use = "TCP completion must be encoded by the connection loop"]
pub struct TcpCompletion<Response, ResponseContext> {
    pub(crate) response: Response,
    pub(crate) response_context: ResponseContext,
    pub(crate) completion: Option<ResponseCompletionCallback>,
}

impl<Response, ResponseContext> TcpCompletion<Response, ResponseContext> {
    /// Attach an application-owned completion item invoked exactly once after
    /// this response is delivered, its delivery fails, or it is aborted.
    ///
    /// The item runs synchronously in the connection loop immediately after the
    /// final transport outcome is known and before Nacelle publishes its generic
    /// request-completion telemetry. It receives transport facts only via
    /// [`ResponseDeliveryOutcome`]. The application owns any offloading required
    /// by its telemetry sink.
    ///
    /// Attaching an item requests a per-response write-and-flush boundary: the
    /// encoded bytes for this response are written and flushed before the item
    /// settles as [`ResponseDeliveryOutcome::Delivered`], regardless of the
    /// configured response write policy. Responses without an item keep the
    /// configured policy and its cost.
    ///
    /// If the connection is torn down after the handler returns but before the
    /// outcome is known, the item settles as
    /// [`ResponseDeliveryOutcome::Aborted`] rather than being dropped silently.
    ///
    /// Attaching an item is independent of Nacelle's metrics and generic
    /// telemetry configuration; it runs even when both are disabled.
    #[must_use = "the completion item is only attached to the returned value"]
    pub fn with_completion_item<F>(mut self, item: F) -> Self
    where
        F: for<'outcome> FnOnce(ResponseDeliveryOutcome<'outcome>) + Send + 'static,
    {
        self.completion = Some(Box::new(item));
        self
    }
}

impl<Response, ResponseContext> fmt::Debug for TcpCompletion<Response, ResponseContext>
where
    Response: fmt::Debug,
    ResponseContext: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpCompletion")
            .field("response", &self.response)
            .field("response_context", &self.response_context)
            .field("completion", &self.completion.as_ref().map(|_| "..."))
            .finish()
    }
}

/// Concrete application context for one required-response TCP request.
pub type TcpRequestContext<P, AppState = ()> = RequestContext<
    TcpRequest<<P as Protocol>::Request>,
    RequiredResponder<TcpResponder<<P as Protocol>::Response, <P as Protocol>::ResponseContext>>,
    AppState,
    ConnectionContext<Arc<<P as Protocol>::ConnectionState>>,
>;

/// Successful completion required from a typed TCP handler.
pub type TcpHandlerCompletion<P> =
    RequiredCompletion<TcpCompletion<<P as Protocol>::Response, <P as Protocol>::ResponseContext>>;

/// Statically dispatched application handler for one TCP protocol.
pub trait TcpHandler<P, AppState = ()>:
    Handler<TcpRequestContext<P, AppState>, Completion = TcpHandlerCompletion<P>, Error = NacelleError>
where
    P: SharedProtocol,
{
}

impl<P, H, AppState> TcpHandler<P, AppState> for H
where
    P: SharedProtocol,
    H: Handler<
            TcpRequestContext<P, AppState>,
            Completion = TcpHandlerCompletion<P>,
            Error = NacelleError,
        >,
{
}

/// Worker-local application handler for one TCP protocol.
///
/// Unlike [`TcpHandler`], this contract permits `!Send` futures and handler
/// state. It is accepted only by the explicit thread-per-core runtime.
pub trait LocalTcpHandler<P, AppState = ()>:
    LocalHandler<
        TcpRequestContext<P, AppState>,
        Completion = TcpHandlerCompletion<P>,
        Error = NacelleError,
    >
where
    P: Protocol,
{
}

/// Exclusive connection-state context for one serial required-response request.
pub type SerialTcpRequestContext<'connection, P, AppState = ()> = RequestContext<
    TcpRequest<<P as Protocol>::Request>,
    RequiredResponder<TcpResponder<<P as Protocol>::Response, <P as Protocol>::ResponseContext>>,
    AppState,
    &'connection mut ConnectionContext<<P as Protocol>::ConnectionState>,
>;

/// Shared-runtime handler with exclusive access to one connection's state.
///
/// The connection loop awaits each call before decoding the next request, so
/// safe implementations cannot overlap mutable access for one connection.
pub trait SerialTcpHandler<P, AppState = ()>: Send + Sync + 'static
where
    P: Protocol,
    P::ConnectionState: Send,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, P, AppState>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + Send + 'connection;
}

/// Worker-local serial handler with exclusive mutable connection state.
#[allow(clippy::future_not_send)]
pub trait LocalSerialTcpHandler<P, AppState = ()>
where
    P: Protocol,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, P, AppState>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection;
}

impl<P, H, AppState> LocalTcpHandler<P, AppState> for H
where
    P: Protocol,
    H: LocalHandler<
            TcpRequestContext<P, AppState>,
            Completion = TcpHandlerCompletion<P>,
            Error = NacelleError,
        >,
{
}

/// Concrete application context for one one-way TCP message.
pub type TcpOneWayContext<P, AppState = ()> = RequestContext<
    TcpRequest<<P as Protocol>::OneWayRequest>,
    NoResponse,
    AppState,
    ConnectionContext<Arc<<P as Protocol>::ConnectionState>>,
>;

/// Statically dispatched one-way handler for one TCP protocol.
pub trait TcpOneWayHandler<P, AppState = ()>:
    Handler<TcpOneWayContext<P, AppState>, Completion = Completed, Error = NacelleError>
where
    P: SharedProtocol,
{
}

impl<P, H, AppState> TcpOneWayHandler<P, AppState> for H
where
    P: SharedProtocol,
    H: Handler<TcpOneWayContext<P, AppState>, Completion = Completed, Error = NacelleError>,
{
}

/// Worker-local one-way handler for one TCP protocol.
pub trait LocalTcpOneWayHandler<P, AppState = ()>:
    LocalHandler<TcpOneWayContext<P, AppState>, Completion = Completed, Error = NacelleError>
where
    P: Protocol,
{
}

/// Exclusive connection-state context for one serial one-way message.
pub type SerialTcpOneWayContext<'connection, P, AppState = ()> = RequestContext<
    TcpRequest<<P as Protocol>::OneWayRequest>,
    NoResponse,
    AppState,
    &'connection mut ConnectionContext<<P as Protocol>::ConnectionState>,
>;

/// Shared-runtime serial one-way handler.
pub trait SerialTcpOneWayHandler<P, AppState = ()>: Send + Sync + 'static
where
    P: Protocol,
    P::ConnectionState: Send,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpOneWayContext<'connection, P, AppState>,
    ) -> impl Future<Output = Result<Completed, NacelleError>> + Send + 'connection;
}

/// Worker-local serial one-way handler.
#[allow(clippy::future_not_send)]
pub trait LocalSerialTcpOneWayHandler<P, AppState = ()>
where
    P: Protocol,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpOneWayContext<'connection, P, AppState>,
    ) -> impl Future<Output = Result<Completed, NacelleError>> + 'connection;
}

impl<P, H, AppState> LocalTcpOneWayHandler<P, AppState> for H
where
    P: Protocol,
    H: LocalHandler<TcpOneWayContext<P, AppState>, Completion = Completed, Error = NacelleError>,
{
}

/// Zero-sized handler for protocols that cannot decode one-way messages.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOneWayHandler<P>(PhantomData<fn() -> P>);

impl<P> NoOneWayHandler<P> {
    pub(crate) const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P, AppState> Handler<TcpOneWayContext<P, AppState>> for NoOneWayHandler<P>
where
    P: SharedProtocol<OneWayRequest = Infallible>,
    AppState: Send + Sync + 'static,
{
    type Completion = Completed;
    type Error = NacelleError;

    async fn call(
        &self,
        _context: TcpOneWayContext<P, AppState>,
    ) -> Result<Self::Completion, Self::Error> {
        unreachable!("an Infallible one-way request cannot be decoded")
    }
}

#[allow(clippy::future_not_send)]
impl<P, AppState> LocalHandler<TcpOneWayContext<P, AppState>> for NoOneWayHandler<P>
where
    P: Protocol<OneWayRequest = Infallible>,
{
    type Completion = Completed;
    type Error = NacelleError;

    async fn call(
        &self,
        _context: TcpOneWayContext<P, AppState>,
    ) -> Result<Self::Completion, Self::Error> {
        unreachable!("an Infallible one-way request cannot be decoded")
    }
}

impl<P, AppState> SerialTcpOneWayHandler<P, AppState> for NoOneWayHandler<P>
where
    P: Protocol<OneWayRequest = Infallible>,
    P::ConnectionState: Send,
    AppState: Send + Sync + 'static,
{
    async fn call<'connection>(
        &'connection self,
        _context: SerialTcpOneWayContext<'connection, P, AppState>,
    ) -> Result<Completed, NacelleError> {
        unreachable!("an Infallible one-way request cannot be decoded")
    }
}

#[allow(clippy::future_not_send)]
impl<P, AppState> LocalSerialTcpOneWayHandler<P, AppState> for NoOneWayHandler<P>
where
    P: Protocol<OneWayRequest = Infallible>,
    AppState: 'static,
{
    async fn call<'connection>(
        &'connection self,
        _context: SerialTcpOneWayContext<'connection, P, AppState>,
    ) -> Result<Completed, NacelleError> {
        unreachable!("an Infallible one-way request cannot be decoded")
    }
}

/// Shared-runtime adapter that installs application state before user dispatch.
#[doc(hidden)]
pub struct SharedAppStateHandler<P, H, AppState> {
    handler: Arc<H>,
    app_state: Arc<AppState>,
    protocol: PhantomData<fn() -> P>,
}

impl<P, H, AppState> SharedAppStateHandler<P, H, AppState> {
    #[doc(hidden)]
    pub const fn new(handler: Arc<H>, app_state: Arc<AppState>) -> Self {
        Self {
            handler,
            app_state,
            protocol: PhantomData,
        }
    }
}

impl<P, H, AppState> Handler<TcpRequestContext<P>> for SharedAppStateHandler<P, H, AppState>
where
    P: SharedProtocol,
    H: TcpHandler<P, AppState>,
    AppState: Send + Sync + 'static,
{
    type Completion = TcpHandlerCompletion<P>;
    type Error = NacelleError;

    fn call(
        &self,
        context: TcpRequestContext<P>,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> + Send {
        Handler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

impl<P, H, AppState> Handler<TcpOneWayContext<P>> for SharedAppStateHandler<P, H, AppState>
where
    P: SharedProtocol,
    H: TcpOneWayHandler<P, AppState>,
    AppState: Send + Sync + 'static,
{
    type Completion = Completed;
    type Error = NacelleError;

    fn call(
        &self,
        context: TcpOneWayContext<P>,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> + Send {
        Handler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

impl<P, H, AppState> SerialTcpHandler<P> for SharedAppStateHandler<P, H, AppState>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P, AppState>,
    AppState: Send + Sync + 'static,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, P>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + Send + 'connection
    {
        SerialTcpHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

impl<P, H, AppState> SerialTcpOneWayHandler<P> for SharedAppStateHandler<P, H, AppState>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpOneWayHandler<P, AppState>,
    AppState: Send + Sync + 'static,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpOneWayContext<'connection, P>,
    ) -> impl Future<Output = Result<Completed, NacelleError>> + Send + 'connection {
        SerialTcpOneWayHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

/// Worker-local adapter that installs application state before user dispatch.
#[doc(hidden)]
pub struct LocalAppStateHandler<P, H, AppState> {
    handler: Rc<H>,
    app_state: Arc<AppState>,
    protocol: PhantomData<fn() -> P>,
}

impl<P, H, AppState> LocalAppStateHandler<P, H, AppState> {
    #[doc(hidden)]
    pub const fn new(handler: Rc<H>, app_state: Arc<AppState>) -> Self {
        Self {
            handler,
            app_state,
            protocol: PhantomData,
        }
    }
}

#[allow(clippy::future_not_send)]
impl<P, H, AppState> LocalHandler<TcpRequestContext<P>> for LocalAppStateHandler<P, H, AppState>
where
    P: Protocol,
    H: LocalTcpHandler<P, AppState>,
{
    type Completion = TcpHandlerCompletion<P>;
    type Error = NacelleError;

    fn call(
        &self,
        context: TcpRequestContext<P>,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
        LocalHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

#[allow(clippy::future_not_send)]
impl<P, H, AppState> LocalHandler<TcpOneWayContext<P>> for LocalAppStateHandler<P, H, AppState>
where
    P: Protocol,
    H: LocalTcpOneWayHandler<P, AppState>,
{
    type Completion = Completed;
    type Error = NacelleError;

    fn call(
        &self,
        context: TcpOneWayContext<P>,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
        LocalHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

#[allow(clippy::future_not_send)]
impl<P, H, AppState> LocalSerialTcpHandler<P> for LocalAppStateHandler<P, H, AppState>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P, AppState>,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, P>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        LocalSerialTcpHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

#[allow(clippy::future_not_send)]
impl<P, H, AppState> LocalSerialTcpOneWayHandler<P> for LocalAppStateHandler<P, H, AppState>
where
    P: Protocol,
    H: LocalSerialTcpOneWayHandler<P, AppState>,
{
    fn call<'connection>(
        &'connection self,
        context: SerialTcpOneWayContext<'connection, P>,
    ) -> impl Future<Output = Result<Completed, NacelleError>> + 'connection {
        LocalSerialTcpOneWayHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

impl<Response, ResponseContext> Respond for TcpResponder<Response, ResponseContext> {
    type Response = Response;
    type Completion = TcpCompletion<Response, ResponseContext>;
    type Error = NacelleError;

    async fn respond(self, response: Self::Response) -> Result<Self::Completion, Self::Error> {
        Ok(TcpCompletion {
            response,
            response_context: self.response_context,
            completion: None,
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
    /// Decoded one-way request head, or [`Infallible`] when unsupported.
    type OneWayRequest: Send + 'static;
    /// Application response accepted by this protocol.
    type Response: Send + 'static;
    /// Concrete state shared by requests on one accepted connection.
    type ConnectionState: 'static;
    type Decoder: MessageDecoder<
            Message = DecodedMessage<Self::Request, Self::OneWayRequest>,
            Error = NacelleError,
        > + Send
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

    /// Return total wire bytes for one one-way message.
    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, body_len: usize) -> usize;

    /// Select the body limit after decoding the request head.
    ///
    /// `state` is the same connection-local value exposed to handlers. The
    /// runtime calls this hook before body-specific allocation or additional
    /// body reads.
    fn max_request_body_bytes(
        &self,
        _request: &Self::Request,
        _connection: &ConnectionInfo,
        _state: &Self::ConnectionState,
        default_limit: usize,
    ) -> usize {
        default_limit
    }

    /// Select the body limit after decoding a one-way message head.
    ///
    /// Required-response and one-way messages observe identical connection
    /// metadata and state semantics.
    fn max_one_way_body_bytes(
        &self,
        _request: &Self::OneWayRequest,
        _connection: &ConnectionInfo,
        _state: &Self::ConnectionState,
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

/// Protocol whose connection state may be shared across runtime threads.
pub trait SharedProtocol: Protocol<ConnectionState: Send + Sync> {}

impl<P> SharedProtocol for P
where
    P: Protocol,
    P::ConnectionState: Send + Sync,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_buffer_new_keeps_cumulative_buffer_semantics() {
        let mut bytes = BytesMut::from(&b"prior"[..]);
        let mut frame = FrameBuffer::new(&mut bytes, 6);

        assert_eq!(frame.len(), 5);
        frame
            .extend_from_slice(b"!")
            .expect("remaining cumulative capacity should be available");
        assert!(matches!(
            frame.extend_from_slice(b"x"),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ResponseFrameBytes
            ))
        ));
        assert_eq!(&bytes[..], b"prior!");
    }
}
