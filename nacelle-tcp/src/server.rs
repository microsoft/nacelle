use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::admission::{LocalTcpAdmitted, SharedTcpAdmitted, TcpAdmitted};
use crate::config::NacelleTcpConfig;
use crate::connection::{
    serve_connection_with_connection_meta_and_tcp_state,
    serve_stream_with_connection_meta_and_tcp_state,
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state,
};
use crate::limits::NacelleTcpLimits;
use crate::protocol::LocalAppStateHandler;
use crate::protocol::{Protocol, SharedAppStateHandler, SharedProtocol};
use nacelle_core::AdmissionConfig;
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
pub struct TcpServer<P, H = (), Observer = NoopObserver> {
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
}

/// Worker-local TCP server for explicit thread-per-core execution.
///
/// Protocol and handler ownership uses [`Rc`] so application handlers may hold
/// `!Send` worker-local state. Instances must be constructed and used on their
/// owning worker.
pub struct LocalTcpServer<P, H, Observer = NoopObserver> {
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
}

#[doc(hidden)]
pub type LocalAppStateTcpServer<P, H, Observer, AppState> =
    LocalTcpServer<P, LocalAppStateHandler<P, H, AppState>, Observer>;

#[doc(hidden)]
pub type AdmittedTcpServerBuilder<ProtocolState, P, H, A, Observer> =
    TcpServerBuilder<ProtocolState, Present, P, SharedTcpAdmitted<H, A>, Observer>;

impl<P, H, Observer> LocalTcpServer<P, H, Observer> {
    /// Install one worker-local admission policy before body dispatch.
    ///
    /// # Errors
    ///
    /// Configure [`NacelleTcpConfig::head_read_limits`] before enabling admission.
    pub fn with_admission<A>(
        self,
        policy: A,
        config: AdmissionConfig,
    ) -> Result<LocalTcpServer<P, LocalTcpAdmitted<H, A>, Observer>, NacelleError> {
        if self.config.head_read_limits.is_none() {
            return Err(nacelle_core::NacelleConfigurationError::AdmissionReadBounds.into());
        }
        Ok(LocalTcpServer {
            protocol: self.protocol,
            handler: Rc::new(TcpAdmitted::new(self.handler, policy, config)),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        })
    }

    /// Bind application state before worker-local request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> LocalAppStateTcpServer<P, H, Observer, AppState>
    where
        P: Protocol,
        H: crate::admission::LocalTcpService<P, AppState>,
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
        }
    }
}

impl<P, H> LocalTcpServer<P, H, NoopObserver> {
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
        }
    }
}

impl<P, H, Observer> LocalTcpServer<P, H, Observer>
where
    Observer: NacelleTelemetryObserver,
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
    ///
    /// Rejects replacement while the previous state has live handles or clones.
    pub fn with_runtime_state(
        mut self,
        runtime_state: NacelleRuntimeState,
    ) -> Result<Self, NacelleError> {
        self.runtime_state.replace(runtime_state)?;
        self.telemetry
            .register_runtime_state(self.runtime_state.clone());
        Ok(self)
    }

    /// Control this server's installed state. Exported handles prevent replacement.
    pub fn runtime_control(&self) -> nacelle_core::NacelleRuntimeControl {
        self.runtime_state.control()
    }

    /// Set telemetry for this worker.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> LocalTcpServer<P, H, Next>
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
        }
    }

    #[doc(hidden)]
    pub fn with_runtime_context<Next>(
        mut self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> Result<LocalTcpServer<P, H, Next>, NacelleError>
    where
        Next: NacelleTelemetryObserver,
    {
        self.runtime_state.replace(runtime_state)?;
        telemetry.register_runtime_state(self.runtime_state.clone());
        Ok(LocalTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        })
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
}

impl<P, H, Observer> Clone for TcpServer<P, H, Observer>
where
    Observer: Clone,
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
        }
    }
}

#[doc(hidden)]
pub type AppStateTcpServer<P, H, Observer, AppState> =
    TcpServer<P, SharedAppStateHandler<P, H, AppState>, Observer>;

impl<P, H, Observer> TcpServer<P, H, Observer> {
    /// Install startup state before exporting controls or sharing the server.
    ///
    /// Reinstalling the same state is allowed; replacing shared state is a
    /// configuration error.
    pub fn with_runtime_state(
        mut self,
        runtime_state: NacelleRuntimeState,
    ) -> Result<Self, NacelleError>
    where
        Observer: NacelleTelemetryObserver,
    {
        self.runtime_state.replace(runtime_state)?;
        self.telemetry
            .register_runtime_state(self.runtime_state.clone());
        Ok(self)
    }

    /// Control this server's installed state. Exported handles prevent replacement.
    pub fn runtime_control(&self) -> nacelle_core::NacelleRuntimeControl {
        self.runtime_state.control()
    }

    /// Bind application state before shared-runtime request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> AppStateTcpServer<P, H, Observer, AppState>
    where
        P: SharedProtocol,
        H: crate::admission::TcpService<P, AppState>,
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
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<P, H, Observer> TcpServer<P, H, Observer>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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

    pub fn with_listener_label(mut self, listener: impl Into<StdArc<str>>) -> Self {
        self.listener = listener.into();
        self
    }

    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    pub fn with_telemetry<Next>(self, telemetry: NacelleTelemetry<Next>) -> TcpServer<P, H, Next>
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
        }
    }

    #[doc(hidden)]
    pub fn with_runtime_context<Next>(
        mut self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> Result<TcpServer<P, H, Next>, NacelleError>
    where
        Next: NacelleTelemetryObserver,
    {
        self.runtime_state.replace(runtime_state)?;
        telemetry.register_runtime_state(self.runtime_state.clone());
        Ok(TcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        })
    }

    /// Serve one connection from independently owned read and write halves.
    pub async fn serve_halves<R, W>(&self, reader: R, writer: W) -> Result<(), NacelleError>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        serve_connection_with_connection_meta_and_tcp_state(
            reader,
            writer,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
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
        serve_connection_with_connection_meta_and_tcp_state(
            reader,
            writer,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
        )
        .await
    }

    /// Serve an I/O stream that implements Tokio's `AsyncRead + AsyncWrite`.
    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_stream_with_connection_meta_and_tcp_state(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
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
        serve_stream_with_connection_meta_and_tcp_state(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
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
        serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
        )
        .await
    }
}

pub struct TcpServerBuilder<ProtocolState, HandlerState, P, H, Observer = NoopObserver> {
    protocol: Option<Arc<P>>,
    handler: Option<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    _protocol: PhantomData<ProtocolState>,
    _handler: PhantomData<HandlerState>,
}

impl<ProtocolState, HandlerState, P, H, Observer>
    TcpServerBuilder<ProtocolState, HandlerState, P, H, Observer>
{
    pub fn tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    pub fn telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> TcpServerBuilder<ProtocolState, HandlerState, P, H, Next>
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
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }

    /// Install startup state, rejecting replacement of a state already shared.
    pub fn runtime_state(
        mut self,
        runtime_state: NacelleRuntimeState,
    ) -> Result<Self, NacelleError> {
        self.runtime_state.replace(runtime_state)?;
        Ok(self)
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

impl<HandlerState, P, H, Observer> TcpServerBuilder<Missing, HandlerState, P, H, Observer> {
    pub fn protocol<P2>(
        self,
        protocol: P2,
    ) -> TcpServerBuilder<Present, HandlerState, P2, H, Observer> {
        TcpServerBuilder {
            protocol: Some(Arc::new(protocol)),
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<ProtocolState, P, H, Observer> TcpServerBuilder<ProtocolState, Missing, P, H, Observer> {
    pub fn handler<H2>(
        self,
        handler: H2,
    ) -> TcpServerBuilder<ProtocolState, Present, P, H2, Observer> {
        TcpServerBuilder {
            protocol: self.protocol,
            handler: Some(handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            _protocol: PhantomData,
            _handler: PhantomData,
        }
    }
}

impl<P, H, Observer> TcpServerBuilder<Present, Present, P, H, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    pub fn build(self) -> Result<TcpServer<P, H, Observer>, NacelleError> {
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
        })
    }
}

impl<ProtocolState, P, H, Observer> TcpServerBuilder<ProtocolState, Present, P, H, Observer> {
    /// Install one concrete admission policy and its finite waiting limits.
    ///
    /// The policy sees the typed head and state before body work. Ready policies
    /// do not start a timer or force a flush; pending policies retain their
    /// original future while prior output is flushed under the admission deadline.
    ///
    /// # Errors
    ///
    /// Configure [`NacelleTcpConfig::head_read_limits`] before this call.
    pub fn admission<A>(
        self,
        policy: A,
        config: AdmissionConfig,
    ) -> Result<AdmittedTcpServerBuilder<ProtocolState, P, H, A, Observer>, NacelleError> {
        if self.config.head_read_limits.is_none() {
            return Err(nacelle_core::NacelleConfigurationError::AdmissionReadBounds.into());
        }
        Ok(TcpServerBuilder {
            protocol: self.protocol,
            handler: self
                .handler
                .map(|handler| TcpAdmitted::new(Arc::new(handler), policy, config)),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            _protocol: PhantomData,
            _handler: PhantomData,
        })
    }
}
