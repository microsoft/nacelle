use std::convert::Infallible;
use std::rc::Rc;
use std::sync::Arc;

use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::NacelleTcpConfig;
use crate::connection::{
    serve_local_serial_stream_without_connection_limit,
    serve_serial_stream_without_connection_limit,
};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    LocalSerialTcpHandler, LocalSerialTcpOneWayHandler, NoOneWayHandler, Protocol,
    SerialTcpHandler, SerialTcpOneWayHandler,
};

/// Shared-runtime TCP server with exclusive mutable connection state.
pub struct SerialTcpServer<P, H, OH = NoOneWayHandler<P>, Observer = NoopObserver> {
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: Arc<str>,
}

impl<P, H> SerialTcpServer<P, H, NoOneWayHandler<P>, NoopObserver>
where
    P: Protocol<OneWayRequest = Infallible>,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
{
    /// Construct a serial server for a required-response-only protocol.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Arc::new(protocol),
            handler: Arc::new(handler),
            one_way_handler: Arc::new(NoOneWayHandler::new()),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, OH> SerialTcpServer<P, H, OH, NoopObserver>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
{
    /// Construct a serial server with required-response and one-way handlers.
    pub fn with_handlers(protocol: P, handler: H, one_way_handler: OH) -> Self {
        Self {
            protocol: Arc::new(protocol),
            handler: Arc::new(handler),
            one_way_handler: Arc::new(one_way_handler),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, OH, Observer> SerialTcpServer<P, H, OH, Observer>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    /// Replace the serial one-way handler.
    pub fn with_one_way_handler<OH2>(
        self,
        one_way_handler: OH2,
    ) -> SerialTcpServer<P, H, OH2, Observer>
    where
        OH2: SerialTcpOneWayHandler<P>,
    {
        SerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: Arc::new(one_way_handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        }
    }

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
    ) -> SerialTcpServer<P, H, OH, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        SerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: self.one_way_handler,
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
    ) -> SerialTcpServer<P, H, OH, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        SerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: self.one_way_handler,
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
        io: IO,
        connection: NacelleConnectionMeta,
    ) -> Result<(), NacelleError>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        serve_serial_stream_without_connection_limit(
            io,
            self.protocol.clone(),
            self.handler.clone(),
            self.one_way_handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            connection.with_listener(self.listener.clone()),
        )
        .await
    }
}

impl<P, H, OH, Observer> Clone for SerialTcpServer<P, H, OH, Observer>
where
    Observer: Clone,
{
    fn clone(&self) -> Self {
        Self {
            protocol: self.protocol.clone(),
            handler: self.handler.clone(),
            one_way_handler: self.one_way_handler.clone(),
            config: self.config.clone(),
            telemetry: self.telemetry.clone(),
            runtime_state: self.runtime_state.clone(),
            tcp_limits: self.tcp_limits,
            listener: self.listener.clone(),
        }
    }
}

/// Worker-local TCP server with exclusive mutable connection state.
pub struct LocalSerialTcpServer<P, H, OH = NoOneWayHandler<P>, Observer = NoopObserver> {
    protocol: Rc<P>,
    handler: Rc<H>,
    one_way_handler: Rc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: Arc<str>,
}

impl<P, H> LocalSerialTcpServer<P, H, NoOneWayHandler<P>, NoopObserver>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: LocalSerialTcpHandler<P>,
{
    /// Construct a worker-local serial server for a required-response-only protocol.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Rc::new(protocol),
            handler: Rc::new(handler),
            one_way_handler: Rc::new(NoOneWayHandler::new()),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, OH> LocalSerialTcpServer<P, H, OH, NoopObserver>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    OH: LocalSerialTcpOneWayHandler<P>,
{
    /// Construct a local serial server with required-response and one-way handlers.
    pub fn with_handlers(protocol: P, handler: H, one_way_handler: OH) -> Self {
        Self {
            protocol: Rc::new(protocol),
            handler: Rc::new(handler),
            one_way_handler: Rc::new(one_way_handler),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: Arc::from("direct"),
        }
    }
}

impl<P, H, OH, Observer> LocalSerialTcpServer<P, H, OH, Observer>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    OH: LocalSerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    /// Replace the worker-local serial one-way handler.
    pub fn with_one_way_handler<OH2>(
        self,
        one_way_handler: OH2,
    ) -> LocalSerialTcpServer<P, H, OH2, Observer>
    where
        OH2: LocalSerialTcpOneWayHandler<P>,
    {
        LocalSerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: Rc::new(one_way_handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        }
    }

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
    ) -> LocalSerialTcpServer<P, H, OH, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime_state.clone());
        LocalSerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: self.one_way_handler,
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
    ) -> LocalSerialTcpServer<P, H, OH, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(runtime_state.clone());
        LocalSerialTcpServer {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler: self.one_way_handler,
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

    pub(crate) fn one_way_handler(&self) -> Rc<OH> {
        self.one_way_handler.clone()
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
            self.one_way_handler.clone(),
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            NacelleConnectionMeta::tcp(None, None).with_listener(self.listener.clone()),
        )
        .await
    }
}
