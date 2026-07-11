//! Transport integration prototype for Nacelle's typed request pipeline.

use std::convert::Infallible;
use std::future::{Future, ready};

use bytes::{BufMut, Bytes, BytesMut};
use http::{Response, StatusCode};
use http_body_util::Full;
use nacelle_core::NacelleConnectionMeta;

pub use nacelle_core::pipeline::{
    ConnectionContext, ConnectionInfo, Handler, Layer, LocalHandler, RequestContext,
    RequiredCompletion, RequiredResponder, Respond, handler_fn, local_handler_fn,
};

/// Construct direct-listener connection context for prototype requests.
pub fn connection<State>(state: State) -> ConnectionContext<State> {
    ConnectionContext::new(
        ConnectionInfo::from(&NacelleConnectionMeta::tcp(None, None)),
        state,
    )
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

impl<State, Connection>
    Handler<RequestContext<TcpRequest, RequiredResponder<TcpResponder<'_>>, State, Connection>>
    for TcpEcho
where
    State: Send,
    Connection: Send,
{
    type Completion = RequiredCompletion<usize>;
    type Error = Infallible;

    async fn call(
        &self,
        context: RequestContext<TcpRequest, RequiredResponder<TcpResponder<'_>>, State, Connection>,
    ) -> Result<Self::Completion, Self::Error> {
        let body = context.request().body.clone();
        context.respond(TcpResponse { body }).await
    }
}

/// Shared-runtime HTTP echo handler used by the prototype tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpEcho;

impl<State, Connection>
    Handler<RequestContext<HttpRequest, RequiredResponder<HttpResponder>, State, Connection>>
    for HttpEcho
where
    State: Send,
    Connection: Send,
{
    type Completion = RequiredCompletion<Response<Full<Bytes>>>;
    type Error = http::Error;

    async fn call(
        &self,
        context: RequestContext<HttpRequest, RequiredResponder<HttpResponder>, State, Connection>,
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
    impl
        LocalHandler<
            RequestContext<(), RequiredResponder<HttpResponder>, Rc<str>, ConnectionContext<()>>,
        > for LocalStateHandler
    {
        type Completion = RequiredCompletion<Response<Full<Bytes>>>;
        type Error = http::Error;

        async fn call(
            &self,
            context: RequestContext<
                (),
                RequiredResponder<HttpResponder>,
                Rc<str>,
                ConnectionContext<()>,
            >,
        ) -> Result<Self::Completion, Self::Error> {
            let body = Bytes::copy_from_slice(context.app_state().as_bytes());
            context
                .respond(HttpResponse {
                    status: StatusCode::OK,
                    body,
                })
                .await
        }
    }

    #[tokio::test]
    async fn core_context_handles_borrowed_tcp_responder() {
        let mut output = BytesMut::new();
        let context = RequestContext::new(
            TcpRequest {
                request_id: 7,
                body: Bytes::from_static(b"tcp"),
            },
            RequiredResponder::new(TcpResponder::new(7, &mut output)),
            (),
            connection(()),
        );
        let pipeline = ObserveLayer.layer(TcpEcho);

        let encoded = pipeline
            .call(context)
            .await
            .expect("infallible response")
            .into_inner();

        assert_eq!(encoded, 15);
        assert_eq!(output.get(12..), Some(b"tcp".as_slice()));
    }

    #[tokio::test]
    async fn core_context_handles_concrete_http_response() {
        let context = RequestContext::new(
            HttpRequest {
                body: Bytes::from_static(b"http"),
            },
            RequiredResponder::new(HttpResponder),
            (),
            connection(()),
        );
        let pipeline = ObserveLayer.layer(HttpEcho);

        let response = pipeline
            .call(context)
            .await
            .expect("valid response")
            .into_inner();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("infallible body")
            .to_bytes();

        assert_eq!(body, Bytes::from_static(b"http"));
    }

    #[tokio::test]
    async fn core_local_handler_accepts_non_send_worker_state() {
        let context = RequestContext::new(
            (),
            RequiredResponder::new(HttpResponder),
            Rc::<str>::from("local"),
            connection(()),
        );
        let pipeline = ObserveLayer.layer(LocalStateHandler);

        let response = LocalHandler::call(&pipeline, context)
            .await
            .expect("valid response")
            .into_inner();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("infallible body")
            .to_bytes();

        assert_eq!(body, Bytes::from_static(b"local"));
    }

    #[tokio::test]
    async fn core_closure_adapters_preserve_shared_and_local_bounds() {
        let shared = handler_fn(
            |context: RequestContext<
                (),
                RequiredResponder<HttpResponder>,
                (),
                ConnectionContext<()>,
            >| async move {
                context
                    .respond(HttpResponse {
                        status: StatusCode::NO_CONTENT,
                        body: Bytes::new(),
                    })
                    .await
            },
        );
        let local = local_handler_fn(
            |context: RequestContext<
                (),
                RequiredResponder<HttpResponder>,
                Rc<str>,
                ConnectionContext<()>,
            >| async move {
                let body = Bytes::copy_from_slice(context.app_state().as_bytes());
                context
                    .respond(HttpResponse {
                        status: StatusCode::OK,
                        body,
                    })
                    .await
            },
        );

        let shared_response = shared
            .call(RequestContext::new(
                (),
                RequiredResponder::new(HttpResponder),
                (),
                connection(()),
            ))
            .await
            .expect("valid shared response")
            .into_inner();
        let local_response = LocalHandler::call(
            &local,
            RequestContext::new(
                (),
                RequiredResponder::new(HttpResponder),
                Rc::<str>::from("local"),
                connection(()),
            ),
        )
        .await
        .expect("valid local response")
        .into_inner();

        assert_eq!(shared_response.status(), StatusCode::NO_CONTENT);
        assert_eq!(local_response.status(), StatusCode::OK);
    }

    #[test]
    fn responder_futures_are_concrete_stack_values() {
        let mut output = BytesMut::new();
        let tcp_future = RequiredResponder::new(TcpResponder::new(1, &mut output))
            .respond(TcpResponse { body: Bytes::new() });
        let http_future = RequiredResponder::new(HttpResponder).respond(HttpResponse {
            status: StatusCode::NO_CONTENT,
            body: Bytes::new(),
        });

        let tcp_size = size_of_val(&tcp_future);
        let http_size = size_of_val(&http_future);
        assert!(tcp_size <= 256, "TCP completion future is {tcp_size} bytes");
        assert!(
            http_size <= 256,
            "HTTP completion future is {http_size} bytes"
        );
    }
}
