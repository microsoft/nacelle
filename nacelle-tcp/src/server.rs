use std::marker::PhantomData;
use std::sync::Arc;

use crate::config::NacelleTcpConfig;
use crate::connection::{
    serve_connection_with_connection_meta_and_tcp_state,
    serve_stream_with_connection_meta_and_tcp_state,
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state,
};
use crate::limits::NacelleTcpLimits;
use crate::protocol::Protocol;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::{
    NacelleConnectionExtension, NacelleConnectionExtensionFactory, NacelleConnectionMeta,
    RequestMetadata,
};
use nacelle_core::telemetry::NacelleTelemetry;
use std::sync::Arc as StdArc;
use tokio::io::{AsyncRead, AsyncWrite};

mod listeners;

pub struct Missing;
pub struct Present;

pub struct NacelleServer<Req, P, H = ()> {
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    connection_extension_factory: Option<NacelleConnectionExtensionFactory>,
    _request: PhantomData<fn() -> Req>,
}

pub type TcpServer<Req, P, H = ()> = NacelleServer<Req, P, H>;

impl<Req, P, H> Clone for NacelleServer<Req, P, H>
where
    H: Clone,
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
            connection_extension_factory: self.connection_extension_factory.clone(),
            _request: PhantomData,
        }
    }
}

impl<Req> NacelleServer<Req, (), ()> {
    pub fn builder() -> NacelleServerBuilder<Req, Missing, Missing, (), ()> {
        NacelleServerBuilder {
            protocol: None,
            handler: None,
            config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            tcp_limits: NacelleTcpLimits::default(),
            listener: StdArc::from("direct"),
            connection_extension_factory: None,
            _protocol: PhantomData,
            _handler: PhantomData,
            _request: PhantomData,
        }
    }
}

impl<Req, P, H> NacelleServer<Req, P, H>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
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

    fn attach_connection_extension(
        &self,
        connection: NacelleConnectionMeta,
    ) -> NacelleConnectionMeta {
        let Some(factory) = &self.connection_extension_factory else {
            return connection.with_listener(self.listener.clone());
        };
        let connection = connection.with_listener(self.listener.clone());
        let Some(extension) = factory(&connection) else {
            return connection;
        };
        connection.with_extension_arc(extension)
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
            self.config.clone(),
            self.telemetry.clone(),
            self.runtime_state.clone(),
            self.tcp_limits,
            self.attach_connection_extension(NacelleConnectionMeta::tcp(None, None)),
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
            self.attach_connection_extension(connection),
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
            self.attach_connection_extension(NacelleConnectionMeta::tcp(None, None)),
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
            self.attach_connection_extension(connection),
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
            self.attach_connection_extension(connection),
        )
        .await
    }
}

pub struct NacelleServerBuilder<Req, ProtocolState, HandlerState, P, H> {
    protocol: Option<Arc<P>>,
    handler: Option<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    listener: StdArc<str>,
    connection_extension_factory: Option<NacelleConnectionExtensionFactory>,
    _protocol: PhantomData<ProtocolState>,
    _handler: PhantomData<HandlerState>,
    _request: PhantomData<fn() -> Req>,
}

impl<Req, ProtocolState, HandlerState, P, H>
    NacelleServerBuilder<Req, ProtocolState, HandlerState, P, H>
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

    pub fn connection_extension_factory<F, E>(mut self, factory: F) -> Self
    where
        F: Fn(&NacelleConnectionMeta) -> E + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.connection_extension_factory = Some(Arc::new(move |meta| {
            Some(Arc::new(factory(meta)) as NacelleConnectionExtension)
        }));
        self
    }

    pub fn optional_connection_extension_factory<F, E>(mut self, factory: F) -> Self
    where
        F: Fn(&NacelleConnectionMeta) -> Option<E> + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.connection_extension_factory = Some(Arc::new(move |meta| {
            factory(meta).map(|extension| Arc::new(extension) as NacelleConnectionExtension)
        }));
        self
    }

    pub fn connection_extension_factory_arc(
        mut self,
        factory: NacelleConnectionExtensionFactory,
    ) -> Self {
        self.connection_extension_factory = Some(factory);
        self
    }
}

impl<Req, HandlerState, P, H> NacelleServerBuilder<Req, Missing, HandlerState, P, H> {
    pub fn protocol<P2>(
        self,
        protocol: P2,
    ) -> NacelleServerBuilder<Req, Present, HandlerState, P2, H> {
        NacelleServerBuilder {
            protocol: Some(Arc::new(protocol)),
            handler: self.handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            connection_extension_factory: self.connection_extension_factory,
            _protocol: PhantomData,
            _handler: PhantomData,
            _request: PhantomData,
        }
    }
}

impl<Req, ProtocolState, P, H> NacelleServerBuilder<Req, ProtocolState, Missing, P, H> {
    pub fn handler<H2>(
        self,
        handler: H2,
    ) -> NacelleServerBuilder<Req, ProtocolState, Present, P, H2> {
        NacelleServerBuilder {
            protocol: self.protocol,
            handler: Some(handler),
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            connection_extension_factory: self.connection_extension_factory,
            _protocol: PhantomData,
            _handler: PhantomData,
            _request: PhantomData,
        }
    }
}

impl<Req, P, H> NacelleServerBuilder<Req, Present, Present, P, H>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
{
    pub fn build(self) -> Result<NacelleServer<Req, P, H>, NacelleError> {
        let protocol = self.protocol.ok_or(NacelleError::MissingProtocol)?;
        let handler = self.handler.expect("handler state guarantees a handler");

        self.telemetry
            .register_runtime_state(self.runtime_state.clone());

        Ok(NacelleServer {
            protocol,
            handler,
            config: self.config,
            telemetry: self.telemetry,
            runtime_state: self.runtime_state,
            tcp_limits: self.tcp_limits,
            listener: self.listener,
            connection_extension_factory: self.connection_extension_factory,
            _request: PhantomData,
        })
    }
}
