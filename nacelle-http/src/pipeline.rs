use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode, Uri};
use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::{
    ConnectionContext, ConnectionInfo, Handler, LocalHandler, RequestContext, RequiredCompletion,
    RequiredResponder, Respond,
};
use nacelle_core::request::NacelleBody;

/// Typed application-facing HTTP request.
pub struct HttpRequest {
    /// Request method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// Request headers.
    pub headers: HeaderMap,
    /// Effective peer IP after trusted proxy processing.
    pub peer_ip: Option<IpAddr>,
    /// Bounded streaming request body.
    pub body: NacelleBody,
}

/// Typed application-facing HTTP response.
pub struct HttpResponse {
    /// Response status.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Streaming response body.
    pub body: NacelleBody,
}

impl HttpResponse {
    /// Construct an HTTP response.
    pub fn new(status: StatusCode, headers: HeaderMap, body: NacelleBody) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }

    /// Construct a byte response without additional headers.
    pub fn bytes(status: StatusCode, bytes: impl Into<Bytes>) -> Self {
        Self::new(status, HeaderMap::new(), NacelleBody::bytes(bytes))
    }

    /// Construct an empty response without additional headers.
    pub fn empty(status: StatusCode) -> Self {
        Self::new(status, HeaderMap::new(), NacelleBody::empty())
    }
}

/// Zero-allocation HTTP response capability.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpResponder;

/// Typed completion consumed by the Hyper adapter.
#[must_use = "HTTP completion must be converted by the Hyper adapter"]
pub struct HttpCompletion {
    pub(crate) response: HttpResponse,
}

impl Respond for HttpResponder {
    type Response = HttpResponse;
    type Completion = HttpCompletion;
    type Error = NacelleError;

    async fn respond(self, response: Self::Response) -> Result<Self::Completion, Self::Error> {
        Ok(HttpCompletion { response })
    }
}

/// Typed HTTP request context with concrete connection state.
pub type HttpRequestContext<State> = RequestContext<
    HttpRequest,
    RequiredResponder<HttpResponder>,
    (),
    ConnectionContext<Arc<State>>,
>;

/// Successful completion required from a typed HTTP handler.
pub type HttpHandlerCompletion = RequiredCompletion<HttpCompletion>;

/// Worker-local HTTP request context with concrete `!Send` connection state.
pub type LocalHttpRequestContext<State> =
    RequestContext<HttpRequest, RequiredResponder<HttpResponder>, (), ConnectionContext<Rc<State>>>;

/// Statically dispatched typed HTTP handler.
pub trait HttpHandler<State>:
    Handler<HttpRequestContext<State>, Completion = HttpHandlerCompletion, Error = NacelleError>
where
    State: Send + Sync + 'static,
{
}

impl<State, H> HttpHandler<State> for H
where
    State: Send + Sync + 'static,
    H: Handler<HttpRequestContext<State>, Completion = HttpHandlerCompletion, Error = NacelleError>,
{
}

/// Worker-local HTTP handler for explicit thread-per-core execution.
pub trait LocalHttpHandler<State>:
    LocalHandler<
        LocalHttpRequestContext<State>,
        Completion = HttpHandlerCompletion,
        Error = NacelleError,
    >
where
    State: 'static,
{
}

impl<State, H> LocalHttpHandler<State> for H
where
    State: 'static,
    H: LocalHandler<
            LocalHttpRequestContext<State>,
            Completion = HttpHandlerCompletion,
            Error = NacelleError,
        >,
{
}

/// Constructs concrete state once for each accepted HTTP connection.
pub trait HttpConnectionStateFactory: Send + Sync + 'static {
    /// State shared by requests on one keep-alive connection.
    type State: Send + Sync + 'static;

    /// Construct state after accept/TLS handshake metadata is available.
    fn create(&self, connection: &ConnectionInfo) -> Self::State;
}

/// Constructs worker-local state once for each accepted HTTP connection.
pub trait LocalHttpConnectionStateFactory: 'static {
    /// State shared locally by requests on one keep-alive connection.
    type State: 'static;

    /// Construct state after accept/TLS handshake metadata is available.
    fn create(&self, connection: &ConnectionInfo) -> Self::State;
}

/// Default no-state HTTP connection factory.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoHttpConnectionState;

impl HttpConnectionStateFactory for NoHttpConnectionState {
    type State = ();

    fn create(&self, _connection: &ConnectionInfo) -> Self::State {}
}

impl LocalHttpConnectionStateFactory for NoHttpConnectionState {
    type State = ();

    fn create(&self, _connection: &ConnectionInfo) -> Self::State {}
}
