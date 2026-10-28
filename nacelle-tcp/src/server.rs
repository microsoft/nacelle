use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::config::NacelleTcpConfig;
use crate::connection::{serve_shared_halves_with_policies, serve_shared_io_with_policies};
use crate::limits::NacelleTcpLimits;
use crate::policies::{LocalTcpPolicySet, NoTcpPolicies, SharedTcpPolicySet};
use crate::protocol::{LocalAppStateHandler, LocalTcpHandler};
use crate::protocol::{Protocol, SharedAppStateHandler, SharedProtocol, TcpHandler};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};
use std::sync::Arc as StdArc;
use tokio::io::{AsyncRead, AsyncWrite};

mod listeners;

pub struct Missing;
pub struct Present;

/// Shared-runtime framed TCP server.
///
/// The server owns its protocol and handler through [`Arc`], plus framing,
/// timeout, runtime-limit, telemetry, and listener-label configuration. Its
/// `serve_*` methods borrow the server and own the supplied I/O values for the
/// lifetime of their futures. Cancelling those futures drops owned I/O without
/// guaranteeing graceful socket shutdown.
///
/// Each direct serving method acquires one connection permit, then enforces
/// process-wide limits from [`NacelleRuntimeState`] and transport limits from
/// [`NacelleTcpLimits`].
///
/// # Errors
///
/// Serving returns [`NacelleError`] for admission, framing,
/// protocol, handler, body, socket, timeout, or shutdown failure.
///
/// # Panics
///
/// Serving futures must be polled inside a Tokio runtime. Nacelle does not
/// intentionally panic for peer or configuration input.
///
/// # Example
///
/// ```text
/// cargo run -p nacelle-examples --bin direct_tcp
/// ```
pub struct TcpServer<P, H = (), Observer = NoopObserver, Policies = NoTcpPolicies> {
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    policies: Policies,
}

/// Worker-local TCP server for explicit thread-per-core execution.
///
/// Protocol and handler ownership uses [`Rc`] so application handlers may hold
/// `!Send` worker-local state. Instances must be constructed and used on their
/// owning worker.
pub struct LocalTcpServer<P, H, Observer = NoopObserver, Policies = NoTcpPolicies> {
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    policies: Policies,
}

#[doc(hidden)]
pub type LocalAppStateTcpServer<P, H, Observer, AppState, Policies = NoTcpPolicies> =
    LocalTcpServer<P, LocalAppStateHandler<P, H, AppState>, Observer, Policies>;

impl<P, H, Observer, Policies> LocalTcpServer<P, H, Observer, Policies> {
    /// Bind application state before worker-local request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> LocalAppStateTcpServer<P, H, Observer, AppState, Policies>
    where
        P: Protocol,
        H: LocalTcpHandler<P, AppState>,
        AppState: 'static,
    {
        LocalTcpServer {
            protocol: self.protocol,
            handler: Rc::new(LocalAppStateHandler::new(self.handler, app_state)),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }
}

impl<P, H> LocalTcpServer<P, H, NoopObserver>
where
    P: Protocol,
    H: LocalTcpHandler<P>,
{
    /// Construct a worker-local server with one handler for all requests.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Rc::new(protocol),
            handler: Rc::new(handler),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: StdArc::from("direct"),
            policies: NoTcpPolicies,
        }
    }
}

impl<P, H, Observer, Policies> LocalTcpServer<P, H, Observer, Policies>
where
    P: Protocol,
    H: LocalTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: LocalTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
{
    /// Set worker-local TCP framing and buffering configuration.
    pub fn with_tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    /// Set worker-local TCP socket and handler timeouts.
    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    /// Set runtime limits/accounting for this worker.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.telemetry.register_runtime_state(runtime_state.clone());
        self.runtime_state = runtime_state;
        self
    }

    /// Set telemetry for this worker.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> LocalTcpServer<P, H, Next, Policies>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        LocalTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }

    #[doc(hidden)]
    pub fn with_runtime_context<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> LocalTcpServer<P, H, Next, Policies>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        LocalTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }

    /// Set the stable listener label recorded in connection metadata.
    pub fn with_listener_label(mut self, listener: impl Into<StdArc<str>>) -> Self {
        self.listener = listener.into();
        self
    }

    pub(crate) fn protocol(&self) -> Rc<P> {
        self.protocol.clone()
    }

    pub(crate) fn handler(&self) -> Rc<H> {
        self.handler.clone()
    }

    pub(crate) fn config(&self) -> NacelleTcpConfig {
        self.config.clone()
    }

    pub(crate) fn telemetry(&self) -> NacelleTelemetry<Observer> {
        self.telemetry.clone()
    }

    pub(crate) fn runtime_state(&self) -> NacelleRuntimeState {
        self.runtime_state.clone()
    }

    pub(crate) const fn tcp_limits(&self) -> NacelleTcpLimits {
        self.tcp_limits
    }

    pub(crate) fn listener_label(&self) -> StdArc<str> {
        self.listener.clone()
    }

    pub(crate) fn policies(&self) -> &Policies {
        &self.policies
    }

    /// Serve one worker-local I/O connection, starting at transport-ready admission.
    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let _connection_permit = self.runtime_state.acquire_connection_tracked()?;
        crate::connection::serve_local_io_with_policies(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
            &self.policies,
        )
        .await
    }
}

impl<P, H, Observer, Policies> Clone for TcpServer<P, H, Observer, Policies>
where
    Observer: Clone,
    Policies: Clone,
{
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol.clone(),
            handler: self.handler.clone(),
            config: self.config.clone(),
            telemetry: self.telemetry.clone(),
            runtime_state: self.runtime_state.clone(),
            tcp_limits: self.tcp_limits,
            listener: self.listener.clone(),
            policies: self.policies.clone(),
        }
    }
}

#[doc(hidden)]
pub type AppStateTcpServer<P, H, Observer, AppState, Policies = NoTcpPolicies> =
    TcpServer<P, SharedAppStateHandler<P, H, AppState>, Observer, Policies>;

impl<P, H, Observer, Policies> TcpServer<P, H, Observer, Policies> {
    /// Bind application state before shared-runtime request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> AppStateTcpServer<P, H, Observer, AppState, Policies>
    where
        P: SharedProtocol,
        H: TcpHandler<P, AppState>,
        AppState: Send + Sync + 'static,
    {
        TcpServer {
            protocol: self.protocol,
            handler: Arc::new(SharedAppStateHandler::new(self.handler, app_state)),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }
}

impl<P> TcpServer<P, (), NoopObserver> {
    pub fn builder() -> TcpServerBuilder<Missing, Missing, P, (), NoopObserver> {
        TcpServerBuilder {
            protocol: None,
            handler: None,
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: StdArc::from("direct"),
            policies: NoTcpPolicies,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<P, H, Observer> TcpServer<P, H, Observer> {
    /// Attach one server-owned checkpoint set without changing handler types.
    pub fn with_policies<Next>(self, policies: Next) -> TcpServer<P, H, Observer, Next>
    where
        P: SharedProtocol,
        Next: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    {
        TcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies,
        }
    }
}

impl<P, H, Observer> LocalTcpServer<P, H, Observer> {
    /// Attach worker-local checkpoints, which may own `!Send` state.
    pub fn with_policies<Next>(self, policies: Next) -> LocalTcpServer<P, H, Observer, Next>
    where
        P: Protocol,
        Next: LocalTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    {
        LocalTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies,
        }
    }
}

impl<P, H, Observer, Policies> TcpServer<P, H, Observer, Policies>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
{
    pub fn tcp_config(&self) -> &NacelleTcpConfig {
        &self.config
    }

    pub fn runtime_state(&self) -> &NacelleRuntimeState {
        &self.runtime_state
    }

    pub fn telemetry(&self) -> &NacelleTelemetry<Observer> {
        &self.telemetry
    }

    pub fn tcp_limits(&self) -> &NacelleTcpLimits {
        &self.tcp_limits
    }

    pub fn listener_label(&self) -> StdArc<str> {
        self.listener.clone()
    }

    pub fn protocol(&self) -> &P {
        self.protocol.as_ref()
    }

    pub(crate) fn policies(&self) -> &Policies {
        &self.policies
    }

    pub fn with_listener_label(mut self, listener: impl Into<StdArc<str>>) -> Self {
        self.listener = listener.into();
        self
    }

    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.telemetry.register_runtime_state(runtime_state.clone());
        self.runtime_state = runtime_state;
        self
    }

    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> TcpServer<P, H, Next, Policies>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        TcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }

    #[doc(hidden)]
    pub fn with_runtime_context<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> TcpServer<P, H, Next, Policies>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        TcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        }
    }

    /// Serve one connection from independently owned read and write halves.
    pub async fn serve_halves<R, W>(&self, reader: R, writer: W) -> Result<(), NacelleError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        serve_shared_halves_with_policies(
            reader,
            writer,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
            &self.policies,
            true,
        )
        .await
    }

    /// Serve split I/O halves with caller-supplied connection metadata.
    pub async fn serve_halves_with_connection_meta<R, W>(
        &self,
        reader: R,
        writer: W,
        connection: NacelleConnectionMeta,
    ) -> Result<(), NacelleError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        serve_shared_halves_with_policies(
            reader,
            writer,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
            &self.policies,
            true,
        )
        .await
    }

    /// Serve an I/O stream that implements Tokio's `AsyncRead + AsyncWrite`.
    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_shared_io_with_policies(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
            &self.policies,
            true,
        )
        .await
    }

    /// Serve an I/O stream with caller-supplied connection metadata.
    pub async fn serve_io_with_connection_meta<IO>(
        &self,
        io: IO,
        connection: NacelleConnectionMeta,
    ) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_shared_io_with_policies(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
            &self.policies,
            true,
        )
        .await
    }

    pub(crate) async fn serve_io_without_connection_limit<IO>(
        &self,
        io: IO,
        connection: NacelleConnectionMeta,
    ) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_shared_io_with_policies(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
            &self.policies,
            false,
        )
        .await
    }
}

pub struct TcpServerBuilder<
    ProtocolState,
    HandlerState,
    P,
    H,
    Observer = NoopObserver,
    Policies = NoTcpPolicies,
> {
    protocol: Option<Arc<P>>,
    handler: Option<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    policies: Policies,
    _protocol: PhantomData<ProtocolState>,
    _handler: PhantomData<HandlerState>,
}

impl<ProtocolState, HandlerState, P, H, Observer, Policies>
    TcpServerBuilder<ProtocolState, HandlerState, P, H, Observer, Policies>
{
    pub fn tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    pub fn telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> TcpServerBuilder<ProtocolState, HandlerState, P, H, Next, Policies>
    where
        Next: NacelleTelemetryObserver,
    {
        TcpServerBuilder {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }

    pub fn runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    pub fn tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    pub fn listener_label(mut self, listener: impl Into<StdArc<str>>) -> Self {
        self.listener = listener.into();
        self
    }
}

impl<HandlerState, P, H, Observer, Policies>
    TcpServerBuilder<Missing, HandlerState, P, H, Observer, Policies>
{
    pub fn protocol<P2>(
        self,
        protocol: P2,
    ) -> TcpServerBuilder<Present, HandlerState, P2, H, Observer, Policies> {
        TcpServerBuilder {
            protocol: Some(Arc::new(protocol)),
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<ProtocolState, P, H, Observer, Policies>
    TcpServerBuilder<ProtocolState, Missing, P, H, Observer, Policies>
{
    pub fn handler<H2>(
        self,
        handler: H2,
    ) -> TcpServerBuilder<ProtocolState, Present, P, H2, Observer, Policies> {
        TcpServerBuilder {
            protocol: self.protocol,
            handler: Some(handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<P, H, Observer, Policies> TcpServerBuilder<Present, Present, P, H, Observer, Policies>
where
    Observer: NacelleTelemetryObserver,
    P: SharedProtocol,
    Policies: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
{
    pub fn build(self) -> Result<TcpServer<P, H, Observer, Policies>, NacelleError> {
        let protocol = self.protocol.ok_or(NacelleError::MissingProtocol)?;
        let handler = self.handler.expect("handler state guarantees a handler");

        self.telemetry
            .register_runtime_state(self.runtime_state.clone());

        Ok(TcpServer {
            protocol,
            handler: Arc::new(handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies: self.policies,
        })
    }
}

impl<PS, HS, P, H, Observer> TcpServerBuilder<PS, HS, P, H, Observer> {
    /// Attach a checkpoint set once; later registration appends within that set.
    pub fn policies<Next>(self, policies: Next) -> TcpServerBuilder<PS, HS, P, H, Observer, Next>
    where
        P: SharedProtocol,
        Next: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    {
        TcpServerBuilder {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            policies,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}
