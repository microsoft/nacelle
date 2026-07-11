//! Experimental statically dispatched request pipeline contracts.
//!
//! These contracts are additive and are not yet used by the TCP or HTTP
//! transports. They remain under `nacelle_core::pipeline` until both adapters
//! prove their completion, cancellation, and connection-state semantics.

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use crate::request::{NacelleConnectionMeta, NacelleConnectionTlsMeta};
use crate::telemetry::NacelleTransport;

/// Immutable transport metadata for one accepted connection.
///
/// Unlike [`NacelleConnectionMeta`], this type does not contain a dynamically
/// typed extension. New low-latency pipelines should carry connection state in
/// [`ConnectionContext::state`] instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    /// Stable connection identifier.
    pub connection_id: u64,
    /// Transport that accepted the connection.
    pub transport: NacelleTransport,
    /// Configured listener label.
    pub listener: Arc<str>,
    /// Remote socket address when available.
    pub peer_addr: Option<SocketAddr>,
    /// Effective remote IP when available.
    pub peer_ip: Option<IpAddr>,
    /// Local socket address when available.
    pub local_addr: Option<SocketAddr>,
    /// Local Unix socket path when available.
    pub local_path: Option<PathBuf>,
    /// Negotiated TLS metadata when TLS is active.
    pub tls: Option<NacelleConnectionTlsMeta>,
}

impl From<&NacelleConnectionMeta> for ConnectionInfo {
    fn from(metadata: &NacelleConnectionMeta) -> Self {
        Self {
            connection_id: metadata.connection_id,
            transport: metadata.transport,
            listener: metadata.listener.clone(),
            peer_addr: metadata.peer_addr,
            peer_ip: metadata.peer_ip,
            local_addr: metadata.local_addr,
            local_path: metadata.local_path.clone(),
            tls: metadata.tls.clone(),
        }
    }
}

/// Stable connection metadata plus concrete connection-local state.
#[derive(Debug, Clone)]
pub struct ConnectionContext<State = ()> {
    /// Immutable connection metadata.
    pub info: ConnectionInfo,
    /// Concrete state owned by this connection.
    pub state: State,
}

impl<State> ConnectionContext<State> {
    /// Construct a typed connection context.
    pub const fn new(info: ConnectionInfo, state: State) -> Self {
        Self { info, state }
    }

    /// Convert the connection state while preserving metadata.
    pub fn map_state<Mapped>(self, map: impl FnOnce(State) -> Mapped) -> ConnectionContext<Mapped> {
        ConnectionContext {
            info: self.info,
            state: map(self.state),
        }
    }
}

/// Completes one request through its originating transport.
///
/// Implementations return concrete futures; the contract does not require a
/// boxed future or dynamically dispatched responder.
///
/// ```compile_fail
/// use nacelle_core::{NoResponse, Respond};
/// fn requires_response<Responder: Respond>() {}
/// requires_response::<NoResponse>();
/// ```
pub trait Respond {
    /// Transport-specific application response.
    type Response;
    /// Value returned to the transport after completion.
    type Completion;
    /// Completion failure.
    type Error;

    /// Consume this capability and complete the request exactly once.
    fn respond(
        self,
        response: Self::Response,
    ) -> impl Future<Output = Result<Self::Completion, Self::Error>>;
}

/// A typed request with exclusive ownership of its completion capability.
///
/// `Connection` is normally a borrowed [`ConnectionContext`], allowing the
/// transport to retain connection state across requests. It can also be another
/// concrete handle when a transport has different sharing requirements.
#[must_use = "request contexts must be completed, responded to, or returned as an error"]
#[derive(Debug)]
pub struct RequestContext<Request, Responder, AppState, Connection = ConnectionContext<()>> {
    request: Request,
    responder: Responder,
    app_state: AppState,
    connection: Connection,
}

impl<Request, Responder, AppState, Connection>
    RequestContext<Request, Responder, AppState, Connection>
{
    /// Construct a context at the protocol/application boundary.
    pub const fn new(
        request: Request,
        responder: Responder,
        app_state: AppState,
        connection: Connection,
    ) -> Self {
        Self {
            request,
            responder,
            app_state,
            connection,
        }
    }

    /// Borrow the typed request.
    pub const fn request(&self) -> &Request {
        &self.request
    }

    /// Mutably borrow the typed request.
    pub const fn request_mut(&mut self) -> &mut Request {
        &mut self.request
    }

    /// Borrow application state.
    pub const fn app_state(&self) -> &AppState {
        &self.app_state
    }

    /// Borrow the concrete connection access value.
    pub const fn connection(&self) -> &Connection {
        &self.connection
    }

    /// Mutably borrow the concrete connection access value.
    pub const fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
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

/// Marker returned when a request completes without transport output.
#[must_use = "completion tokens must be returned to the transport runtime"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Completed(());

/// Completion capability for a request that cannot produce a response.
#[must_use = "one-way requests must be completed explicitly"]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoResponse;

impl<Request, AppState, Connection> RequestContext<Request, NoResponse, AppState, Connection> {
    /// Complete this one-way request without producing transport output.
    pub fn complete(self) -> Completed {
        Completed(())
    }
}

/// Required response capability around a concrete transport responder.
#[must_use = "required responders must produce a response completion token"]
#[derive(Debug)]
pub struct RequiredResponder<Inner>(Inner);

impl<Inner> RequiredResponder<Inner> {
    /// Require completion through this responder.
    pub const fn new(inner: Inner) -> Self {
        Self(inner)
    }
}

/// Unforgeable successful completion of a required response.
#[must_use = "required completion tokens must be returned to the transport runtime"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredCompletion<Inner>(Inner);

impl<Inner> RequiredCompletion<Inner> {
    /// Recover the transport-specific completion value.
    pub fn into_inner(self) -> Inner {
        self.0
    }
}

impl<Inner> Respond for RequiredResponder<Inner>
where
    Inner: Respond,
{
    type Response = Inner::Response;
    type Completion = RequiredCompletion<Inner::Completion>;
    type Error = Inner::Error;

    async fn respond(self, response: Self::Response) -> Result<Self::Completion, Self::Error> {
        self.0.respond(response).await.map(RequiredCompletion)
    }
}

/// Result of explicitly completing an optional response capability.
#[must_use = "optional completion tokens must be returned to the transport runtime"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalCompletion<ResponseCompletion>(Option<ResponseCompletion>);

impl<ResponseCompletion> OptionalCompletion<ResponseCompletion> {
    /// Return whether the request produced transport output.
    pub const fn responded(&self) -> bool {
        self.0.is_some()
    }

    /// Recover the transport completion when a response was produced.
    pub fn into_response(self) -> Option<ResponseCompletion> {
        self.0
    }
}

/// Optional response capability around a concrete transport responder.
#[must_use = "optional responders must respond or complete explicitly"]
#[derive(Debug)]
pub struct OptionalResponder<Inner>(Inner);

impl<Inner> OptionalResponder<Inner> {
    /// Allow either response or explicit silent completion.
    pub const fn new(inner: Inner) -> Self {
        Self(inner)
    }
}

impl<Inner> Respond for OptionalResponder<Inner>
where
    Inner: Respond,
{
    type Response = Inner::Response;
    type Completion = OptionalCompletion<Inner::Completion>;
    type Error = Inner::Error;

    async fn respond(self, response: Self::Response) -> Result<Self::Completion, Self::Error> {
        self.0
            .respond(response)
            .await
            .map(|completion| OptionalCompletion(Some(completion)))
    }
}

impl<Request, Inner, AppState, Connection>
    RequestContext<Request, OptionalResponder<Inner>, AppState, Connection>
{
    /// Complete this optional-response request without transport output.
    pub fn complete(self) -> OptionalCompletion<Inner::Completion>
    where
        Inner: Respond,
    {
        OptionalCompletion(None)
    }
}

/// Handles one concrete request context on the shared multi-thread runtime.
pub trait Handler<Context>: Send + Sync + 'static {
    /// Value returned to the transport after completion.
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
/// workers keep it on the accepting worker's local executor.
#[allow(clippy::future_not_send)]
pub trait LocalHandler<Context> {
    /// Value returned to the transport after completion.
    type Completion;
    /// Request handling failure.
    type Error;

    /// Process one local request without boxing the returned future.
    fn call(&self, context: Context)
    -> impl Future<Output = Result<Self::Completion, Self::Error>>;
}

/// Concrete shared-runtime handler created from a closure.
///
/// This adapter is intended for owned or otherwise non-lending context types.
/// A handler whose future borrows from a context lifetime should implement
/// [`Handler`] directly because stable Rust cannot name that family of closure
/// future types without boxing or a generated concrete future.
#[derive(Debug, Clone, Copy)]
pub struct HandlerFn<Function>(Function);

impl<Context, Function, HandlerFuture, Completion, Error> Handler<Context> for HandlerFn<Function>
where
    Function: Fn(Context) -> HandlerFuture + Send + Sync + 'static,
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
///
/// Lending local handlers should implement [`LocalHandler`] directly for the
/// same stable-Rust reason described on [`HandlerFn`].
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

/// Constructs one statically nested middleware layer.
pub trait Layer<Inner> {
    /// Concrete middleware service produced by this layer.
    type Service;

    /// Wrap a concrete inner handler.
    fn layer(self, inner: Inner) -> Self::Service;
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::future::ready;
    use std::rc::Rc;

    use super::*;

    #[derive(Debug)]
    struct TestResponder;

    #[derive(Debug, Clone, Copy)]
    struct BorrowingHandler;

    impl Respond for TestResponder {
        type Response = u64;
        type Completion = u64;
        type Error = Infallible;

        fn respond(
            self,
            response: Self::Response,
        ) -> impl Future<Output = Result<Self::Completion, Self::Error>> {
            ready(Ok(response))
        }
    }

    impl<'connection>
        Handler<
            RequestContext<
                u64,
                RequiredResponder<TestResponder>,
                (),
                &'connection mut ConnectionContext<u64>,
            >,
        > for BorrowingHandler
    {
        type Completion = RequiredCompletion<u64>;
        type Error = Infallible;

        async fn call(
            &self,
            mut context: RequestContext<
                u64,
                RequiredResponder<TestResponder>,
                (),
                &'connection mut ConnectionContext<u64>,
            >,
        ) -> Result<Self::Completion, Self::Error> {
            context.connection_mut().state += 1;
            let response = context.request() + context.connection().state;
            context.respond(response).await
        }
    }

    fn connection<State>(state: State) -> ConnectionContext<State> {
        ConnectionContext::new(
            ConnectionInfo::from(&NacelleConnectionMeta::tcp(None, None)),
            state,
        )
    }

    #[tokio::test]
    async fn required_responder_completes_once() {
        let context =
            RequestContext::new(5, RequiredResponder::new(TestResponder), (), connection(()));

        let completion = context.respond(7).await.expect("infallible response");

        assert_eq!(completion.into_inner(), 7);
    }

    #[test]
    fn optional_and_one_way_requests_complete_without_output() {
        let optional =
            RequestContext::new(5, OptionalResponder::new(TestResponder), (), connection(()));
        let one_way = RequestContext::new(5, NoResponse, (), connection(()));

        assert!(!optional.complete().responded());
        let completion = one_way.complete();
        assert_eq!(std::mem::size_of_val(&completion), 0);
    }

    #[tokio::test]
    async fn shared_handler_accepts_send_state() {
        let handler = handler_fn(
            |context: RequestContext<u64, RequiredResponder<TestResponder>, Arc<str>>| async move {
                let response = context.request() + context.app_state().len() as u64;
                context.respond(response).await
            },
        );
        let context = RequestContext::new(
            5,
            RequiredResponder::new(TestResponder),
            Arc::<str>::from("state"),
            connection(()),
        );

        let completion = handler.call(context).await.expect("infallible response");

        assert_eq!(completion.into_inner(), 10);
    }

    #[tokio::test]
    async fn request_borrows_connection_state_without_consuming_it() {
        let mut connection = connection(10_u64);
        let context = RequestContext::new(
            5,
            RequiredResponder::new(TestResponder),
            (),
            &mut connection,
        );

        let completion = BorrowingHandler
            .call(context)
            .await
            .expect("infallible response");

        assert_eq!(completion.into_inner(), 16);
        assert_eq!(connection.state, 11);
    }

    #[tokio::test]
    async fn local_handler_accepts_non_send_state() {
        let handler = local_handler_fn(
            |context: RequestContext<u64, RequiredResponder<TestResponder>, Rc<str>>| async move {
                let response = context.request() + context.app_state().len() as u64;
                context.respond(response).await
            },
        );
        let context = RequestContext::new(
            5,
            RequiredResponder::new(TestResponder),
            Rc::<str>::from("local"),
            connection(()),
        );

        let completion = handler.call(context).await.expect("infallible response");

        assert_eq!(completion.into_inner(), 10);
    }

    #[test]
    fn connection_info_drops_dynamic_extension() {
        let metadata = NacelleConnectionMeta::tcp(None, None).with_extension(42_u64);
        let info = ConnectionInfo::from(&metadata);

        assert_eq!(info.connection_id, metadata.connection_id);
        assert_eq!(info.transport, metadata.transport);
    }
}
