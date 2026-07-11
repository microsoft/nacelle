#[cfg(feature = "tcp")]
use std::marker::PhantomData;
#[cfg(feature = "tcp")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;
use std::sync::Arc;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleShutdown;
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::telemetry::NacelleTelemetry;

use crate::host::NacelleHost;

#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_tcp::NacelleTlsDetectionOptions;
#[cfg(all(feature = "tcp", unix))]
use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
use nacelle_tcp::{
    NacelleTcpBindOptions, NacelleTcpConfig, NacelleTcpLimits, NacelleTcpOptions, Protocol,
    TcpHandler, TcpServer,
};

pub struct NacelleApp<H> {
    handler: Arc<H>,
    #[cfg(feature = "tcp")]
    tcp_config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    #[cfg(feature = "tcp")]
    tcp_limits: NacelleTcpLimits,
    shutdown: NacelleShutdown,
    ctrl_c_shutdown: bool,
    drain_timeout: std::time::Duration,
}

impl<H> NacelleApp<H> {
    /// Create an app from the handler used by every configured transport.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            #[cfg(feature = "tcp")]
            tcp_config: NacelleTcpConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            #[cfg(feature = "tcp")]
            tcp_limits: NacelleTcpLimits::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
        }
    }

    #[cfg(feature = "tcp")]
    pub fn with_tcp_config(mut self, tcp_config: NacelleTcpConfig) -> Self {
        self.tcp_config = tcp_config;
        self
    }

    pub fn with_telemetry(mut self, telemetry: NacelleTelemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_limits(mut self, limits: NacelleLimits) -> Self {
        self.runtime_state = NacelleRuntimeState::new(limits);
        self
    }

    #[cfg(feature = "tcp")]
    pub fn with_tcp_limits(mut self, tcp_limits: NacelleTcpLimits) -> Self {
        self.tcp_limits = tcp_limits;
        self
    }

    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Request graceful shutdown when the process receives Ctrl-C.
    ///
    /// This is a convenience for binaries that want the common local/production
    /// signal path without manually wiring a [`NacelleShutdown`] handle.
    pub fn with_ctrl_c_shutdown(mut self) -> Self {
        self.ctrl_c_shutdown = true;
        self
    }

    pub fn with_ctrl_c_shutdown_enabled(mut self, enabled: bool) -> Self {
        self.ctrl_c_shutdown = enabled;
        self
    }

    pub fn with_shutdown_drain_timeout(mut self, drain_timeout: std::time::Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    pub fn handler(&self) -> &H {
        self.handler.as_ref()
    }

    /// Install the configured protocols and run the app until shutdown.
    pub async fn serve(self, protocols: NacelleProtocols<H>) -> Result<(), NacelleError> {
        serve(protocols, self).await
    }
}

type ProtocolInstaller<H> =
    Box<dyn FnOnce(&mut NacelleHost, &NacelleApp<H>) -> Result<(), NacelleError> + Send>;

pub struct NacelleProtocols<H> {
    installers: Vec<ProtocolInstaller<H>>,
}

impl<H> Default for NacelleProtocols<H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H> NacelleProtocols<H> {
    pub fn new() -> Self {
        Self {
            installers: Vec::new(),
        }
    }
}

#[cfg(feature = "tcp")]
impl<H> NacelleProtocols<H> {
    pub fn tcp<P>(self, name: impl Into<String>, addr: SocketAddr, protocol: P) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_with_options(name, addr, protocol, NacelleTcpOptions::default())
    }

    pub fn tcp_with_options<P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_with_bind_options(
            name,
            addr,
            protocol,
            NacelleTcpBindOptions::from(tcp_options),
        )
    }

    pub fn tcp_with_bind_options<P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        bind_options: NacelleTcpBindOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<P, H>(protocol, app)?;
            host.enable_tcp_with_bind_options(name, addr, bind_options, server);
            Ok(())
        }));
        self
    }

    pub fn tcp_dual_stack<P>(self, name: impl Into<String>, port: u16, protocol: P) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        self.tcp_dual_stack_with_options(name, port, protocol, NacelleTcpOptions::default())
    }

    pub fn tcp_dual_stack_with_options<P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let ipv6_bind_options =
            NacelleTcpBindOptions::from(tcp_options.clone()).with_ipv6_only(true);

        self.tcp_with_options(
            format!("{name}-ipv4"),
            ipv4_addr,
            protocol.clone(),
            tcp_options,
        )
        .tcp_with_bind_options(
            format!("{name}-ipv6"),
            ipv6_addr,
            protocol,
            ipv6_bind_options,
        )
    }

    #[cfg(all(feature = "tcp", unix))]
    pub fn unix_socket<P>(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        protocol: P,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.unix_socket_with_options(name, path, protocol, NacelleUnixSocketOptions::default())
    }

    #[cfg(all(feature = "tcp", unix))]
    pub fn unix_socket_with_options<P>(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        protocol: P,
        unix_options: NacelleUnixSocketOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<P, H>(protocol, app)?;
            host.enable_unix_socket_with_options(name, path, unix_options, server);
            Ok(())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl<P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_openssl_with_options(
            name,
            addr,
            protocol,
            tls_config,
            NacelleTcpOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl_with_options<P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_openssl_with_bind_options(
            name,
            addr,
            protocol,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl_with_bind_options<P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<P, H>(protocol, app)?;
            host.enable_tcp_openssl_with_bind_options(name, addr, server, tls_config, bind_options);
            Ok(())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl_dual_stack<P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        self.tcp_openssl_dual_stack_with_options(
            name,
            port,
            protocol,
            tls_config,
            NacelleTcpOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl_dual_stack_with_options<P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let ipv6_bind_options =
            NacelleTcpBindOptions::from(tcp_options.clone()).with_ipv6_only(true);

        self.tcp_openssl_with_options(
            format!("{name}-ipv4"),
            ipv4_addr,
            protocol.clone(),
            tls_config.clone(),
            tcp_options,
        )
        .tcp_openssl_with_bind_options(
            format!("{name}-ipv6"),
            ipv6_addr,
            protocol,
            tls_config,
            ipv6_bind_options,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_optional_openssl<P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_optional_openssl_with_options(
            name,
            addr,
            protocol,
            tls_config,
            NacelleTcpOptions::default(),
            NacelleTlsDetectionOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_optional_openssl_with_options<P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        self.tcp_optional_openssl_with_bind_options(
            name,
            addr,
            protocol,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
            detection_options,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    #[allow(clippy::too_many_arguments)]
    pub fn tcp_optional_openssl_with_bind_options<P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        P: Protocol,
        H: TcpHandler<P>,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<P, H>(protocol, app)?;
            host.enable_tcp_optional_openssl_with_bind_options(
                name,
                addr,
                server,
                tls_config,
                bind_options,
                detection_options,
            );
            Ok(())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_optional_openssl_dual_stack<P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        self.tcp_optional_openssl_dual_stack_with_options(
            name,
            port,
            protocol,
            tls_config,
            NacelleTcpOptions::default(),
            NacelleTlsDetectionOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    #[allow(clippy::too_many_arguments)]
    pub fn tcp_optional_openssl_dual_stack_with_options<P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        P: Protocol + Clone,
        H: TcpHandler<P>,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let ipv6_bind_options =
            NacelleTcpBindOptions::from(tcp_options.clone()).with_ipv6_only(true);

        self.tcp_optional_openssl_with_options(
            format!("{name}-ipv4"),
            ipv4_addr,
            protocol.clone(),
            tls_config.clone(),
            tcp_options,
            detection_options.clone(),
        )
        .tcp_optional_openssl_with_bind_options(
            format!("{name}-ipv6"),
            ipv6_addr,
            protocol,
            tls_config,
            ipv6_bind_options,
            detection_options,
        )
    }
}

pub async fn serve<H>(
    protocols: NacelleProtocols<H>,
    app: NacelleApp<H>,
) -> Result<(), NacelleError> {
    let ctrl_c_task = app
        .ctrl_c_shutdown
        .then(|| spawn_ctrl_c_shutdown(app.shutdown.clone()));
    let mut host = NacelleHost::new()
        .with_telemetry(app.telemetry.clone())
        .with_runtime_state(app.runtime_state.clone())
        .with_shutdown(app.shutdown.clone())
        .with_shutdown_drain_timeout(app.drain_timeout);
    for installer in protocols.installers {
        installer(&mut host, &app)?;
    }
    let result = host.wait().await;
    if let Some(task) = ctrl_c_task {
        task.abort();
    }
    result
}

fn spawn_ctrl_c_shutdown(shutdown: NacelleShutdown) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.shutdown();
    })
}

#[cfg(feature = "tcp")]
struct SharedTcpHandler<P, H> {
    handler: Arc<H>,
    _protocol: PhantomData<fn() -> P>,
}

#[cfg(feature = "tcp")]
impl<P, H> nacelle_core::pipeline::Handler<nacelle_tcp::TcpRequestContext<P>>
    for SharedTcpHandler<P, H>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    type Completion = nacelle_tcp::TcpHandlerCompletion<P>;
    type Error = NacelleError;

    async fn call(
        &self,
        context: nacelle_tcp::TcpRequestContext<P>,
    ) -> Result<Self::Completion, Self::Error> {
        nacelle_core::pipeline::Handler::call(self.handler.as_ref(), context).await
    }
}

#[cfg(feature = "tcp")]
fn tcp_server<P, H>(
    protocol: P,
    app: &NacelleApp<H>,
) -> Result<TcpServer<P, SharedTcpHandler<P, H>>, NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let builder = TcpServer::<P>::builder()
        .protocol(protocol)
        .tcp_config(app.tcp_config.clone())
        .telemetry(app.telemetry.clone())
        .runtime_state(app.runtime_state.clone());
    let builder = builder.tcp_limits(app.tcp_limits);
    builder
        .handler(SharedTcpHandler {
            handler: app.handler.clone(),
            _protocol: PhantomData,
        })
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocols_start_empty() {
        let protocols = NacelleProtocols::<()>::new();

        assert_eq!(protocols.installers.len(), 0);
    }

    #[cfg(feature = "tcp")]
    mod tcp_tests {
        use bytes::{Bytes, BytesMut};
        use nacelle_core::pipeline::{ConnectionInfo, Handler as PipelineHandler};
        use nacelle_tcp::{
            DecodedRequest, FrameBuffer, MessageDecoder, TcpHandlerCompletion, TcpRequestContext,
            TcpResponse,
        };

        use super::*;

        #[derive(Debug)]
        struct TestRequest;

        #[derive(Clone)]
        struct TestProtocol;

        struct TestHandler;

        impl PipelineHandler<TcpRequestContext<TestProtocol>> for TestHandler {
            type Completion = TcpHandlerCompletion<TestProtocol>;
            type Error = NacelleError;

            async fn call(
                &self,
                context: TcpRequestContext<TestProtocol>,
            ) -> Result<Self::Completion, Self::Error> {
                context.respond(TcpResponse::empty()).await
            }
        }

        struct TestDecoder;

        impl MessageDecoder for TestDecoder {
            type Message = DecodedRequest<TestRequest>;
            type Error = NacelleError;

            fn decode(
                &mut self,
                _src: &mut BytesMut,
            ) -> Result<Option<Self::Message>, Self::Error> {
                Ok(None)
            }
        }

        impl Protocol for TestProtocol {
            type Request = TestRequest;
            type Response = TcpResponse;
            type ConnectionState = ();
            type Decoder = TestDecoder;
            type ResponseContext = ();
            type ErrorContext = ();

            fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
                TestDecoder
            }

            fn connection_state(&self, _: &ConnectionInfo) {}

            fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
                body_len
            }

            fn response_context(&self, _req: &TestRequest) -> Self::ResponseContext {}

            fn error_context(&self, _req: &TestRequest) -> Self::ErrorContext {}

            fn apply_response(
                &self,
                _context: &mut Self::ResponseContext,
                _response: &Self::Response,
            ) {
            }

            fn max_response_frame_overhead(&self) -> usize {
                0
            }

            fn response_body(&self, response: Self::Response) -> nacelle_core::NacelleBody {
                response.body
            }

            fn encode_response_chunk(
                &self,
                _context: &mut Self::ResponseContext,
                chunk: Bytes,
                dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                dst.extend_from_slice(&chunk)?;
                Ok(())
            }

            fn encode_response_terminal_chunk(
                &self,
                context: &mut Self::ResponseContext,
                chunk: Bytes,
                dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                self.encode_response_chunk(context, chunk, dst)
            }

            fn encode_response_end(
                &self,
                _context: &mut Self::ResponseContext,
                _dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                Ok(())
            }

            fn encode_error(
                &self,
                _context: Option<&Self::ErrorContext>,
                _error: &NacelleError,
                _dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                Ok(())
            }
        }

        #[test]
        fn tcp_dual_stack_registers_ipv4_and_ipv6_installers() {
            let protocols = NacelleProtocols::<TestHandler>::new().tcp_dual_stack(
                "gateway",
                27017,
                TestProtocol,
            );

            assert_eq!(protocols.installers.len(), 2);
        }

        #[test]
        fn app_can_enable_ctrl_c_shutdown() {
            let app = NacelleApp::new(TestHandler).with_ctrl_c_shutdown();

            assert!(app.ctrl_c_shutdown);
        }
    }
}
