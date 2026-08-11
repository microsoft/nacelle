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
    body: NacelleBody,
}

impl HttpRequest {
    pub(crate) fn new(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        peer_ip: Option<IpAddr>,
        body: NacelleBody,
    ) -> Self {
        Self {
            method,
            uri,
            headers,
            peer_ip,
            body,
        }
    }

    /// Read the next bounded request-body chunk.
    pub async fn next_body_chunk(&mut self) -> Option<Result<Bytes, NacelleError>> {
        self.body.next_chunk().await
    }

    /// Return the exact remaining size when known, or zero for streaming bodies.
    pub fn remaining_body_bytes(&self) -> usize {
        self.body.remaining_bytes()
    }
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

/// Typed HTTP request context with concrete connection and application state.
pub type HttpRequestContext<State, AppState = ()> = RequestContext<
    HttpRequest,
    RequiredResponder<HttpResponder>,
    AppState,
    ConnectionContext<Arc<State>>,
>;

/// Successful completion required from a typed HTTP handler.
pub type HttpHandlerCompletion = RequiredCompletion<HttpCompletion>;

/// Worker-local HTTP request context with concrete `!Send` connection state.
pub type LocalHttpRequestContext<State, AppState = ()> = RequestContext<
    HttpRequest,
    RequiredResponder<HttpResponder>,
    AppState,
    ConnectionContext<Rc<State>>,
>;

/// Statically dispatched typed HTTP handler.
pub trait HttpHandler<State, AppState = ()>:
    Handler<
        HttpRequestContext<State, AppState>,
        Completion = HttpHandlerCompletion,
        Error = NacelleError,
    >
where
    State: Send + Sync + 'static,
{
}

impl<State, AppState, H> HttpHandler<State, AppState> for H
where
    State: Send + Sync + 'static,
    H: Handler<
            HttpRequestContext<State, AppState>,
            Completion = HttpHandlerCompletion,
            Error = NacelleError,
        >,
{
}

/// Worker-local HTTP handler for explicit thread-per-core execution.
pub trait LocalHttpHandler<State, AppState = ()>:
    LocalHandler<
        LocalHttpRequestContext<State, AppState>,
        Completion = HttpHandlerCompletion,
        Error = NacelleError,
    >
where
    State: 'static,
{
}

impl<State, AppState, H> LocalHttpHandler<State, AppState> for H
where
    State: 'static,
    H: LocalHandler<
            LocalHttpRequestContext<State, AppState>,
            Completion = HttpHandlerCompletion,
            Error = NacelleError,
        >,
{
}

/// Shared-runtime adapter that installs application state before user dispatch.
#[doc(hidden)]
pub struct SharedHttpAppStateHandler<H, AppState> {
    handler: Arc<H>,
    app_state: Arc<AppState>,
}

impl<H, AppState> SharedHttpAppStateHandler<H, AppState> {
    #[doc(hidden)]
    pub const fn new(handler: Arc<H>, app_state: Arc<AppState>) -> Self {
        Self { handler, app_state }
    }
}

impl<State, H, AppState> Handler<HttpRequestContext<State>>
    for SharedHttpAppStateHandler<H, AppState>
where
    State: Send + Sync + 'static,
    H: HttpHandler<State, AppState>,
    AppState: Send + Sync + 'static,
{
    type Completion = HttpHandlerCompletion;
    type Error = NacelleError;

    fn call(
        &self,
        context: HttpRequestContext<State>,
    ) -> impl std::future::Future<Output = Result<Self::Completion, Self::Error>> + Send {
        Handler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
}

/// Worker-local adapter that installs application state before user dispatch.
#[doc(hidden)]
pub struct LocalHttpAppStateHandler<H, AppState> {
    handler: Rc<H>,
    app_state: Arc<AppState>,
}

impl<H, AppState> LocalHttpAppStateHandler<H, AppState> {
    #[doc(hidden)]
    pub const fn new(handler: Rc<H>, app_state: Arc<AppState>) -> Self {
        Self { handler, app_state }
    }
}

#[allow(clippy::future_not_send)]
impl<State, H, AppState> LocalHandler<LocalHttpRequestContext<State>>
    for LocalHttpAppStateHandler<H, AppState>
where
    State: 'static,
    H: LocalHttpHandler<State, AppState>,
    AppState: 'static,
{
    type Completion = HttpHandlerCompletion;
    type Error = NacelleError;

    fn call(
        &self,
        context: LocalHttpRequestContext<State>,
    ) -> impl std::future::Future<Output = Result<Self::Completion, Self::Error>> {
        LocalHandler::call(
            self.handler.as_ref(),
            context.map_app_state(self.app_state.clone()),
        )
    }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use nacelle_core::pipeline::{RequiredResponder, handler_fn};
    use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};

    use super::*;

    #[tokio::test]
    async fn shared_adapter_preserves_application_state_identity() {
        let app_state = Arc::new(String::from("shared"));
        let expected_state = app_state.clone();
        let observed = Arc::new(AtomicBool::new(false));
        let handler_observed = observed.clone();
        let handler = handler_fn(move |context: HttpRequestContext<(), String>| {
            handler_observed.store(
                std::ptr::eq(context.app_state(), expected_state.as_ref()),
                Ordering::Relaxed,
            );
            async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
        });
        let adapter = SharedHttpAppStateHandler::new(Arc::new(handler), app_state);
        let request = HttpRequest::new(
            Method::GET,
            Uri::from_static("/"),
            HeaderMap::new(),
            None,
            NacelleBody::empty(),
        );
        let connection = ConnectionContext::new(
            ConnectionInfo::from(&NacelleConnectionMeta::http_socket(None, None)),
            Arc::new(()),
        );
        let context = HttpRequestContext::without_app_state(
            request,
            RequiredResponder::new(HttpResponder),
            connection,
        );

        let _completion = Handler::call(&adapter, context)
            .await
            .expect("handler should complete");

        assert!(observed.load(Ordering::Relaxed));
    }
}
