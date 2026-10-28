use std::{rc::Rc, sync::Arc};

use nacelle_core::{
    error::NacelleError, limits::NacelleRuntimeState, request::NacelleConnectionMeta,
    telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver},
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    config::NacelleTcpConfig,
    connection::{
        serve_local_serial_stream_without_connection_limit,
        serve_serial_stream_without_connection_limit,
    },
    limits::NacelleTcpLimits,
    protocol::{
        LocalAppStateHandler, LocalSerialTcpHandler, Protocol, SerialTcpHandler,
        SharedAppStateHandler,
    },
};

/// Shared-runtime TCP server with exclusive mutable connection state.
///
/// Protocol connection state is created once per connection and lent mutably
/// to one awaited handler at a time. The server owns protocol and handler
/// through [`Arc`], and [`serve_io`](Self::serve_io) borrows the server while
/// owning the supplied I/O value. Cancelling the future drops that I/O without
/// guaranteeing graceful socket shutdown.
///
/// Direct serving acquires one connection permit and enforces
/// [`NacelleRuntimeState`] plus [`NacelleTcpLimits`].
///
/// # Errors
///
/// Serving returns [`NacelleError`] for admission, framing, protocol, handler, body, socket,
/// timeout, or shutdown failures.
///
/// # Panics
///
/// Serving futures must be polled inside a Tokio runtime. Nacelle does not
/// intentionally panic for peer or configuration input.
///
/// # Example
///
/// The shared-runtime direct-I/O shape is demonstrated by:
///
/// ```text
/// cargo run -p nacelle-examples --bin direct_tcp
/// ```
pub struct SerialTcpServer<P, H, Observer = NoopObserver> {
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: Arc<str>,
}

#[doc(hidden)]
pub type AppStateSerialTcpServer<P, H, Observer, AppState> =
    SerialTcpServer<P, SharedAppStateHandler<P, H, AppState>, Observer>;

impl<P, H> SerialTcpServer<P, H, NoopObserver> {
    /// Construct a serial server with one handler for all requests.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Arc::new(protocol),
            handler: Arc::new(handler),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, Observer> SerialTcpServer<P, H, Observer> {
    /// Bind application state before serial request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> AppStateSerialTcpServer<P, H, Observer, AppState>
    where
        P: Protocol,
        P::ConnectionState: Send,
        H: SerialTcpHandler<P, AppState>,
        AppState: Send + Sync + 'static,
    {
        SerialTcpServer {
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

impl<P, H, Observer> SerialTcpServer<P, H, Observer>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    /// Set TCP framing and buffering configuration.
    pub fn with_tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    /// Set socket, idle, and handler timeouts.
    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    /// Replace the runtime limits and accounting state.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.telemetry.register_runtime_state(runtime_state.clone());
        self.runtime_state = runtime_state;
        self
    }

    /// Replace telemetry with one concrete observer type.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> SerialTcpServer<P, H, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        SerialTcpServer {
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
        self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> SerialTcpServer<P, H, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        SerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        }
    }

    /// Set the stable listener label recorded in connection metadata.
    pub fn with_listener_label(mut self, listener: impl Into<Arc<str>>) -> Self {
        self.listener = listener.into();
        self
    }

    /// Borrow the concrete protocol.
    pub fn protocol(&self) -> &P {
        self.protocol.as_ref()
    }

    /// Borrow the configured telemetry pipeline.
    pub fn telemetry(&self) -> &NacelleTelemetry<Observer> {
        &self.telemetry
    }

    /// Borrow runtime limits and accounting.
    pub fn runtime_state(&self) -> &NacelleRuntimeState {
        &self.runtime_state
    }

    /// Return TCP-specific limits.
    pub const fn tcp_limits(&self) -> NacelleTcpLimits {
        self.tcp_limits
    }

    /// Return the stable listener label.
    pub fn listener_label(&self) -> Arc<str> {
        self.listener.clone()
    }

    /// Serve one serial connection.
    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let _connection_permit = self.runtime_state.acquire_connection_tracked()?;
        self.serve_io_without_connection_limit(
            io,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
        )
        .await
    }

    pub(crate) async fn serve_io_without_connection_limit<IO>(
        &self,
        mut io: IO,
        connection: NacelleConnectionMeta,
    ) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_serial_stream_without_connection_limit(
            &mut io,
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

impl<P, H, Observer> Clone for SerialTcpServer<P, H, Observer>
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

/// Worker-local TCP server with exclusive mutable connection state.
pub struct LocalSerialTcpServer<P, H, Observer = NoopObserver> {
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: Arc<str>,
}

#[doc(hidden)]
pub type LocalAppStateSerialTcpServer<P, H, Observer, AppState> =
    LocalSerialTcpServer<P, LocalAppStateHandler<P, H, AppState>, Observer>;

impl<P, H> LocalSerialTcpServer<P, H, NoopObserver> {
    /// Construct a worker-local serial server with one handler for all requests.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Rc::new(protocol),
            handler: Rc::new(handler),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, Observer> LocalSerialTcpServer<P, H, Observer> {
    /// Bind application state before worker-local serial request dispatch.
    #[doc(hidden)]
    pub fn with_app_state<AppState>(
        self,
        app_state: Arc<AppState>,
    ) -> LocalAppStateSerialTcpServer<P, H, Observer, AppState>
    where
        P: Protocol,
        H: LocalSerialTcpHandler<P, AppState>,
        AppState: 'static,
    {
        LocalSerialTcpServer {
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

impl<P, H, Observer> LocalSerialTcpServer<P, H, Observer>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    /// Set worker-local TCP framing and buffering configuration.
    pub fn with_tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    /// Set worker-local TCP timeouts.
    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    /// Set worker-local runtime limits and accounting.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.telemetry.register_runtime_state(runtime_state.clone());
        self.runtime_state = runtime_state;
        self
    }

    /// Replace worker-local telemetry with one concrete observer type.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> LocalSerialTcpServer<P, H, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        LocalSerialTcpServer {
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
        self,
        telemetry: NacelleTelemetry<Next>,
        runtime_state: NacelleRuntimeState,
    ) -> LocalSerialTcpServer<P, H, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        LocalSerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            config: self.config,
            telemetry,
            runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        }
    }

    /// Set the stable listener label recorded in connection metadata.
    pub fn with_listener_label(mut self, listener: impl Into<Arc<str>>) -> Self {
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

    pub(crate) fn listener_label(&self) -> Arc<str> {
        self.listener.clone()
    }

    /// Serve one worker-local serial connection.
    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        let _connection_permit = self.runtime_state.acquire_connection_tracked()?;
        serve_local_serial_stream_without_connection_limit(
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
}
