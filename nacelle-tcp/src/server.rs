use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::config::NacelleTcpConfig;
use crate::connection::{
    serve_connection_with_connection_meta_and_tcp_state,
    serve_stream_with_connection_meta_and_tcp_state,
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state,
};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{LocalTcpHandler, LocalTcpOneWayHandler};
use crate::protocol::{NoOneWayHandler, Protocol, TcpHandler, TcpOneWayHandler};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::NacelleTelemetry;
use std::sync::Arc as StdArc;
use tokio::io::{AsyncRead, AsyncWrite};

mod listeners;

pub struct Missing;
pub struct Present;

pub struct NacelleServer<P, H = (), OH = NoOneWayHandler<P>> {
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
}

pub type TcpServer<P, H = (), OH = NoOneWayHandler<P>> = NacelleServer<P, H, OH>;

/// Worker-local TCP server for explicit thread-per-core execution.
///
/// Protocol and handler ownership uses [`Rc`] so application handlers may hold
/// `!Send` worker-local state. Instances must be constructed and used on their
/// owning worker.
pub struct LocalTcpServer<P, H, OH = NoOneWayHandler<P>> {
    protocol: Rc<P>,
    handler: Rc<H>,
    one_way_handler: Rc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
}

impl<P, H> LocalTcpServer<P, H, NoOneWayHandler<P>>
where
    P: Protocol<OneWayRequest = std::convert::Infallible>,
    H: LocalTcpHandler<P>,
{
    /// Construct a worker-local server without one-way messages.
    pub fn new(protocol: P, handler: H) -> Self {
        Self {
            protocol: Rc::new(protocol),
            handler: Rc::new(handler),
            one_way_handler: Rc::new(NoOneWayHandler::new()),
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: StdArc::from("direct"),
        }
    }
}

impl<P, H, OH> LocalTcpServer<P, H, OH>
where
    P: Protocol,
    H: LocalTcpHandler<P>,
    OH: LocalTcpOneWayHandler<P>,
{
    /// Replace the worker-local one-way handler.
    pub fn with_one_way_handler<OH2>(self, one_way_handler: OH2) -> LocalTcpServer<P, H, OH2>
    where
        OH2: LocalTcpOneWayHandler<P>,
    {
        LocalTcpServer {
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

    /// Set worker-local TCP socket and handler timeouts.
    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    /// Set runtime limits/accounting for this worker.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    /// Set telemetry for this worker.
    pub fn with_telemetry(mut self, telemetry: NacelleTelemetry) -> Self {
        telemetry.register_runtime_state(self.runtime_state.clone());
        self.telemetry = telemetry;
        self
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

    pub(crate) fn one_way_handler(&self) -> Rc<OH> {
        self.one_way_handler.clone()
    }

    pub(crate) fn config(&self) -> NacelleTcpConfig {
        self.config.clone()
    }

    pub(crate) fn telemetry(&self) -> NacelleTelemetry {
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

impl<P, H, OH> Clone for NacelleServer<P, H, OH> {
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

impl<P> NacelleServer<P, ()> {
    pub fn builder() -> NacelleServerBuilder<Missing, Missing, P, (), NoOneWayHandler<P>> {
        NacelleServerBuilder {
            protocol: None,
            handler: None,
            one_way_handler: NoOneWayHandler::new(),
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

impl<P, H, OH> NacelleServer<P, H, OH>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    pub fn tcp_config(&self) -> &NacelleTcpConfig {
        &self.config
    }

    pub fn runtime_state(&self) -> &NacelleRuntimeState {
        &self.runtime_state
    }

    pub fn telemetry(&self) -> &NacelleTelemetry {
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

    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.telemetry.register_runtime_state(runtime_state.clone());
        self.runtime_state = runtime_state;
        self
    }

    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    pub fn with_telemetry(mut self, telemetry: NacelleTelemetry) -> Self {
        telemetry.register_runtime_state(self.runtime_state.clone());
        self.telemetry = telemetry;
        self
    }

    #[doc(hidden)]
    pub fn with_runtime_context(
        mut self,
        telemetry: NacelleTelemetry,
        runtime_state: NacelleRuntimeState,
    ) -> Self {
        telemetry.register_runtime_state(runtime_state.clone());
        self.telemetry = telemetry;
        self.runtime_state = runtime_state;
        self
    }

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
            self.one_way_handler.clone(),
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
            self.one_way_handler.clone(),
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
            self.one_way_handler.clone(),
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
            self.one_way_handler.clone(),
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

pub struct NacelleServerBuilder<ProtocolState, HandlerState, P, H, OH> {
    protocol: Option<Arc<P>>,
    handler: Option<H>,
    one_way_handler: OH,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    _protocol: PhantomData<ProtocolState>,
    _handler: PhantomData<HandlerState>,
}

impl<ProtocolState, HandlerState, P, H, OH>
    NacelleServerBuilder<ProtocolState, HandlerState, P, H, OH>
{
    pub fn tcp_config(mut self, config: NacelleTcpConfig) -> Self {
        self.config = config;
        self
    }

    pub fn telemetry(mut self, telemetry: NacelleTelemetry) -> Self {
        self.telemetry = telemetry;
        self
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

impl<HandlerState, P, H, OH> NacelleServerBuilder<Missing, HandlerState, P, H, OH> {
    pub fn protocol<P2>(
        self,
        protocol: P2,
    ) -> NacelleServerBuilder<Present, HandlerState, P2, H, OH> {
        NacelleServerBuilder {
            protocol: Some(Arc::new(protocol)),
            handler: self.handler,
            one_way_handler: self.one_way_handler,
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

impl<ProtocolState, P, H, OH> NacelleServerBuilder<ProtocolState, Missing, P, H, OH> {
    pub fn handler<H2>(
        self,
        handler: H2,
    ) -> NacelleServerBuilder<ProtocolState, Present, P, H2, OH> {
        NacelleServerBuilder {
            protocol: self.protocol,
            handler: Some(handler),
            one_way_handler: self.one_way_handler,
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

impl<ProtocolState, HandlerState, P, H, OH>
    NacelleServerBuilder<ProtocolState, HandlerState, P, H, OH>
{
    /// Install the concrete one-way handler for this protocol.
    pub fn one_way_handler<OH2>(
        self,
        one_way_handler: OH2,
    ) -> NacelleServerBuilder<ProtocolState, HandlerState, P, H, OH2> {
        NacelleServerBuilder {
            protocol: self.protocol,
            handler: self.handler,
            one_way_handler,
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

impl<P, H, OH> NacelleServerBuilder<Present, Present, P, H, OH>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    pub fn build(self) -> Result<NacelleServer<P, H, OH>, NacelleError> {
        let protocol = self.protocol.ok_or(NacelleError::MissingProtocol)?;
        let handler = self.handler.expect("handler state guarantees a handler");

        self.telemetry
            .register_runtime_state(self.runtime_state.clone());

        Ok(NacelleServer {
            protocol,
            handler: Arc::new(handler),
            one_way_handler: Arc::new(self.one_way_handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
        })
    }
}
