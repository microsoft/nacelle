//! Compile-time prototype for Nacelle's typed request pipeline.

use std::convert::Infallible;
use std::future::{Future, ready};

use bytes::{BufMut, Bytes, BytesMut};
use http::{Response, StatusCode};
use http_body_util::Full;

/// Completes one request through its originating transport.
pub trait Respond {
    /// Transport-specific application response.
    type Response;
    /// Value returned to the transport runtime after completion.
    type Completion;
    /// Completion failure.
    type Error;

    /// Consume this capability and complete the request exactly once.
    fn respond(
        self,
        response: Self::Response,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>>;
}

/// Concrete shared-runtime handler created from a closure.
#[derive(Debug, Clone, Copy)]
pub struct HandlerFn<Function>(Function);

impl<Context, Function, HandlerFuture, Completion, Error> Handler<Context> for HandlerFn<Function>
where
    Function: Fn(Context) -> HandlerFuture + Sync,
    HandlerFuture: Future<Output = Result<Completion, Error>> + Send,
{
    type Completion = Completion;
    type Error = Error;

    fn call(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> + Send {
        (self.0)(context)
    }
}

/// Create a concrete shared-runtime handler from a closure.
pub const fn handler_fn<Function>(function: Function) -> HandlerFn<Function> {
    HandlerFn(function)
}

/// Concrete worker-local handler created from a closure.
#[derive(Debug, Clone, Copy)]
pub struct LocalHandlerFn<Function>(Function);

#[allow(clippy::future_not_send)]
impl<Context, Function, HandlerFuture, Completion, Error> LocalHandler<Context>
    for LocalHandlerFn<Function>
where
    Function: Fn(Context) -> HandlerFuture,
    HandlerFuture: Future<Output = Result<Completion, Error>>,
{
    type Completion = Completion;
    type Error = Error;

    fn call(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
        (self.0)(context)
    }
}

/// Create a concrete worker-local handler from a closure.
pub const fn local_handler_fn<Function>(function: Function) -> LocalHandlerFn<Function> {
    LocalHandlerFn(function)
}

/// A typed request with exclusive ownership of its completion capability.
#[derive(Debug)]
pub struct RequestContext<Request, Responder, State> {
    request: Request,
    responder: Responder,
    state: State,
}

impl<Request, Responder, State> RequestContext<Request, Responder, State> {
    /// Construct a context at the protocol/application boundary.
    pub const fn new(request: Request, responder: Responder, state: State) -> Self {
        Self {
            request,
            responder,
            state,
        }
    }

    /// Borrow the typed request.
    pub const fn request(&self) -> &Request {
        &self.request
    }

    /// Borrow worker or application state.
    pub const fn state(&self) -> &State {
        &self.state
    }

    /// Complete the request through its originating transport.
    pub fn respond(
        self,
        response: Responder::Response,
    ) -> impl Future<Output = Result<Responder::Completion, Responder::Error>>
    where
        Responder: Respond,
    {
        self.responder.respond(response)
    }
}

/// Handles one concrete request context on the shared multi-thread runtime.
pub trait Handler<Context>: Sync {
    /// Value returned to the transport runtime after completion.
    type Completion;
    /// Request handling failure.
    type Error;

    /// Process one request without boxing the returned `Send` future.
    fn call(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> + Send;
}

/// Handles one concrete request context on an explicitly local worker.
///
/// The future is intentionally not required to be `Send`; thread-per-core
/// workers keep it on the accepting worker's `LocalSet`.
#[allow(clippy::future_not_send)]
pub trait LocalHandler<Context> {
    /// Value returned to the transport runtime after completion.
    type Completion;
    /// Request handling failure.
    type Error;

    /// Process one worker-local request without boxing the returned future.
    fn call(&self, context: Context)
    -> impl Future<Output = Result<Self::Completion, Self::Error>>;
}

/// Constructs one statically nested middleware layer.
pub trait Layer<Inner> {
    /// Concrete middleware service produced by this layer.
    type Service;

    /// Wrap a concrete inner handler.
    fn layer(self, inner: Inner) -> Self::Service;
}

/// No-op observation layer used to prove static middleware composition.
#[derive(Debug, Clone, Copy, Default)]
pub struct ObserveLayer;

impl<Inner> Layer<Inner> for ObserveLayer {
    type Service = Observe<Inner>;

    fn layer(self, inner: Inner) -> Self::Service {
        Observe { inner }
    }
}

/// Concrete observation middleware around another handler.
#[derive(Debug, Clone, Copy)]
pub struct Observe<Inner> {
    inner: Inner,
}

impl<Context, Inner> Handler<Context> for Observe<Inner>
where
    Context: Send,
    Inner: Handler<Context>,
{
    type Completion = Inner::Completion;
    type Error = Inner::Error;

    async fn call(&self, context: Context) -> Result<Self::Completion, Self::Error> {
        self.inner.call(context).await
    }
}

#[allow(clippy::future_not_send)]
impl<Context, Inner> LocalHandler<Context> for Observe<Inner>
where
    Inner: LocalHandler<Context>,
{
    type Completion = Inner::Completion;
    type Error = Inner::Error;

    async fn call(&self, context: Context) -> Result<Self::Completion, Self::Error> {
        self.inner.call(context).await
    }
}

/// Example decoded TCP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpRequest {
    /// Correlation id copied into the encoded response.
    pub request_id: u64,
    /// Decoded request body.
    pub body: Bytes,
}

/// Example typed TCP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpResponse {
    /// Response body to encode into the connection write buffer.
    pub body: Bytes,
}

/// TCP completion capability borrowing the connection-local write buffer.
#[derive(Debug)]
pub struct TcpResponder<'buffer> {
    request_id: u64,
    write_buffer: &'buffer mut BytesMut,
}

impl<'buffer> TcpResponder<'buffer> {
    /// Create a responder for one decoded request.
    pub const fn new(request_id: u64, write_buffer: &'buffer mut BytesMut) -> Self {
        Self {
            request_id,
            write_buffer,
        }
    }
}

impl Respond for TcpResponder<'_> {
    type Response = TcpResponse;
    type Completion = usize;
    type Error = Infallible;

    fn respond(
        self,
        response: Self::Response,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
        let initial_len = self.write_buffer.len();
        self.write_buffer.put_u64_le(self.request_id);
        self.write_buffer
            .put_u32_le(u32::try_from(response.body.len()).unwrap_or(u32::MAX));
        self.write_buffer.extend_from_slice(&response.body);
        ready(Ok(self.write_buffer.len() - initial_len))
    }
}

/// Example normalized HTTP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// Request body after Hyper decoding.
    pub body: Bytes,
}

/// Example typed HTTP response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP response status.
    pub status: StatusCode,
    /// HTTP response body.
    pub body: Bytes,
}

/// HTTP completion capability producing Hyper's concrete response value.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpResponder;

impl Respond for HttpResponder {
    type Response = HttpResponse;
    type Completion = Response<Full<Bytes>>;
    type Error = http::Error;

    fn respond(
        self,
        response: Self::Response,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
        ready(
            Response::builder()
                .status(response.status)
                .body(Full::new(response.body)),
        )
    }
}

/// Shared-runtime TCP echo handler used by the prototype tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct TcpEcho;

impl<State> Handler<RequestContext<TcpRequest, TcpResponder<'_>, State>> for TcpEcho
where
    State: Send,
{
    type Completion = usize;
    type Error = Infallible;

    async fn call(
        &self,
        context: RequestContext<TcpRequest, TcpResponder<'_>, State>,
    ) -> Result<Self::Completion, Self::Error> {
        let body = context.request().body.clone();
        context.respond(TcpResponse { body }).await
    }
}

/// Shared-runtime HTTP echo handler used by the prototype tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpEcho;

impl<State> Handler<RequestContext<HttpRequest, HttpResponder, State>> for HttpEcho
where
    State: Send,
{
    type Completion = Response<Full<Bytes>>;
    type Error = http::Error;

    async fn call(
        &self,
        context: RequestContext<HttpRequest, HttpResponder, State>,
    ) -> Result<Self::Completion, Self::Error> {
        let body = context.request().body.clone();
        context
            .respond(HttpResponse {
                status: StatusCode::OK,
                body,
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of_val;
    use std::rc::Rc;

    use http_body_util::BodyExt;

    use super::*;

    #[derive(Debug, Clone, Copy, Default)]
    struct LocalStateHandler;

    #[allow(clippy::future_not_send)]
    impl LocalHandler<RequestContext<(), HttpResponder, Rc<str>>> for LocalStateHandler {
        type Completion = Response<Full<Bytes>>;
        type Error = http::Error;

        async fn call(
            &self,
            context: RequestContext<(), HttpResponder, Rc<str>>,
        ) -> Result<Self::Completion, Self::Error> {
            let body = Bytes::copy_from_slice(context.state().as_bytes());
            context
                .respond(HttpResponse {
                    status: StatusCode::OK,
                    body,
                })
                .await
        }
    }

    #[tokio::test]
    async fn one_generic_middleware_shape_handles_borrowed_tcp_responder() {
        let mut output = BytesMut::new();
        let context = RequestContext::new(
            TcpRequest {
                request_id: 7,
                body: Bytes::from_static(b"tcp"),
            },
            TcpResponder::new(7, &mut output),
            (),
        );
        let pipeline = ObserveLayer.layer(TcpEcho);

        let encoded = pipeline.call(context).await.expect("infallible response");

        assert_eq!(encoded, 15);
        assert_eq!(output.get(12..), Some(b"tcp".as_slice()));
    }

    #[tokio::test]
    async fn one_generic_middleware_shape_handles_concrete_http_response() {
        let context = RequestContext::new(
            HttpRequest {
                body: Bytes::from_static(b"http"),
            },
            HttpResponder,
            (),
        );
        let pipeline = ObserveLayer.layer(HttpEcho);

        let response = pipeline.call(context).await.expect("valid response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("infallible body")
            .to_bytes();

        assert_eq!(body, Bytes::from_static(b"http"));
    }

    #[tokio::test]
    async fn local_handler_accepts_non_send_worker_state() {
        let context = RequestContext::new((), HttpResponder, Rc::<str>::from("local"));
        let pipeline = ObserveLayer.layer(LocalStateHandler);

        let response = LocalHandler::call(&pipeline, context)
            .await
            .expect("valid response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("infallible body")
            .to_bytes();

        assert_eq!(body, Bytes::from_static(b"local"));
    }

    #[tokio::test]
    async fn closure_adapters_preserve_shared_and_local_future_bounds() {
        let shared = handler_fn(
            |context: RequestContext<(), HttpResponder, ()>| async move {
                context
                    .respond(HttpResponse {
                        status: StatusCode::NO_CONTENT,
                        body: Bytes::new(),
                    })
                    .await
            },
        );
        let local = local_handler_fn(
            |context: RequestContext<(), HttpResponder, Rc<str>>| async move {
                let body = Bytes::copy_from_slice(context.state().as_bytes());
                context
                    .respond(HttpResponse {
                        status: StatusCode::OK,
                        body,
                    })
                    .await
            },
        );

        let shared_response = shared
            .call(RequestContext::new((), HttpResponder, ()))
            .await
            .expect("valid shared response");
        let local_response = LocalHandler::call(
            &local,
            RequestContext::new((), HttpResponder, Rc::<str>::from("local")),
        )
        .await
        .expect("valid local response");

        assert_eq!(shared_response.status(), StatusCode::NO_CONTENT);
        assert_eq!(local_response.status(), StatusCode::OK);
    }

    #[test]
    fn responder_futures_are_concrete_stack_values() {
        let mut output = BytesMut::new();
        let tcp_future =
            TcpResponder::new(1, &mut output).respond(TcpResponse { body: Bytes::new() });
        let http_future = HttpResponder.respond(HttpResponse {
            status: StatusCode::NO_CONTENT,
            body: Bytes::new(),
        });

        assert!(size_of_val(&tcp_future) <= 64);
        assert!(size_of_val(&http_future) <= 256);
    }
}
