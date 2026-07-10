#[cfg(feature = "tcp")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;
use std::sync::Arc;

use nacelle_core::config::NacelleConfig;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::lifecycle::NacelleShutdown;
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::request::{
    NacelleConnectionExtension, NacelleConnectionExtensionFactory, NacelleConnectionMeta,
};
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
    NacelleTcpBindOptions, NacelleTcpLimits, NacelleTcpOptions, Protocol, TcpServer,
};

pub struct NacelleApp<H> {
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    #[cfg(feature = "tcp")]
    tcp_limits: NacelleTcpLimits,
    shutdown: NacelleShutdown,
    ctrl_c_shutdown: bool,
    drain_timeout: std::time::Duration,
    connection_extension_factory: Option<NacelleConnectionExtensionFactory>,
}

impl<H> NacelleApp<H>
where
    H: Handler,
{
    /// Create an app from the handler used by every configured transport.
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            config: NacelleConfig::default(),
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            #[cfg(feature = "tcp")]
            tcp_limits: NacelleTcpLimits::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            connection_extension_factory: None,
        }
    }

    pub fn with_config(mut self, config: NacelleConfig) -> Self {
        self.config = config;
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

    pub fn with_connection_extension_factory<F, E>(mut self, factory: F) -> Self
    where
        F: Fn(&NacelleConnectionMeta) -> E + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.connection_extension_factory = Some(Arc::new(move |meta| {
            Some(Arc::new(factory(meta)) as NacelleConnectionExtension)
        }));
        self
    }

    pub fn with_optional_connection_extension_factory<F, E>(mut self, factory: F) -> Self
    where
        F: Fn(&NacelleConnectionMeta) -> Option<E> + Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        self.connection_extension_factory = Some(Arc::new(move |meta| {
            factory(meta).map(|extension| Arc::new(extension) as NacelleConnectionExtension)
        }));
        self
    }

    pub fn handler(&self) -> &H {
        &self.handler
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
impl<H> NacelleProtocols<H>
where
    H: Handler,
{
    pub fn tcp<Req, P>(self, name: impl Into<String>, addr: SocketAddr, protocol: P) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        self.tcp_with_options(name, addr, protocol, NacelleTcpOptions::default())
    }

    pub fn tcp_with_options<Req, P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        self.tcp_with_bind_options(
            name,
            addr,
            protocol,
            NacelleTcpBindOptions::from(tcp_options),
        )
    }

    pub fn tcp_with_bind_options<Req, P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        bind_options: NacelleTcpBindOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<Req, P, H>(protocol, app)?;
            host.enable_tcp_with_bind_options(name, addr, bind_options, server);
            Ok(())
        }));
        self
    }

    pub fn tcp_dual_stack<Req, P>(self, name: impl Into<String>, port: u16, protocol: P) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
    {
        self.tcp_dual_stack_with_options(name, port, protocol, NacelleTcpOptions::default())
    }

    pub fn tcp_dual_stack_with_options<Req, P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
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
    pub fn unix_socket<Req, P>(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        protocol: P,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        self.unix_socket_with_options(name, path, protocol, NacelleUnixSocketOptions::default())
    }

    #[cfg(all(feature = "tcp", unix))]
    pub fn unix_socket_with_options<Req, P>(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        protocol: P,
        unix_options: NacelleUnixSocketOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<Req, P, H>(protocol, app)?;
            host.enable_unix_socket_with_options(name, path, unix_options, server);
            Ok(())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl<Req, P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
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
    pub fn tcp_openssl_with_options<Req, P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
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
    pub fn tcp_openssl_with_bind_options<Req, P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<Req, P, H>(protocol, app)?;
            host.enable_tcp_openssl_with_bind_options(name, addr, server, tls_config, bind_options);
            Ok(())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn tcp_openssl_dual_stack<Req, P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
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
    pub fn tcp_openssl_dual_stack_with_options<Req, P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
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
    pub fn tcp_optional_openssl<Req, P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
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
    pub fn tcp_optional_openssl_with_options<Req, P>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
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
    pub fn tcp_optional_openssl_with_bind_options<Req, P>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Send + Sync + 'static,
    {
        let name = name.into();
        self.installers.push(Box::new(move |host, app| {
            let server = tcp_server::<Req, P, H>(protocol, app)?;
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
    pub fn tcp_optional_openssl_dual_stack<Req, P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
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
    pub fn tcp_optional_openssl_dual_stack_with_options<Req, P>(
        self,
        name: impl Into<String>,
        port: u16,
        protocol: P,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        Req: nacelle_core::request::RequestMetadata + Send + 'static,
        P: Protocol<Req> + Clone + Send + Sync + 'static,
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
) -> Result<(), NacelleError>
where
    H: Handler,
{
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
fn tcp_server<Req, P, H>(
    protocol: P,
    app: &NacelleApp<H>,
) -> Result<TcpServer<Req, P, H>, NacelleError>
where
    Req: nacelle_core::request::RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
{
    let builder = TcpServer::<Req, ()>::builder()
        .protocol(protocol)
        .config(app.config.clone())
        .telemetry(app.telemetry.clone())
        .runtime_state(app.runtime_state.clone());
    let builder = builder.tcp_limits(app.tcp_limits);
    let builder = if let Some(factory) = app.connection_extension_factory.clone() {
        builder.connection_extension_factory_arc(factory)
    } else {
        builder
    };
    builder.handler(app.handler.clone()).build()
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
        use std::future::{Ready, ready};

        use bytes::{Bytes, BytesMut};
        use nacelle_core::request::{NacelleRequest, RequestMetadata};
        use nacelle_core::response::NacelleResponse;
        use nacelle_tcp::{DecodedRequest, MessageDecoder};

        use super::*;

        #[derive(Clone)]
        struct TestHandler;

        impl Handler for TestHandler {
            type Future = Ready<Result<NacelleResponse, NacelleError>>;

            fn call(&self, _request: NacelleRequest) -> Self::Future {
                ready(Ok(NacelleResponse::empty_tcp()))
            }
        }

        #[derive(Debug)]
        struct TestRequest;

        impl RequestMetadata for TestRequest {
            fn opcode(&self) -> u64 {
                1
            }
        }

        #[derive(Clone)]
        struct TestProtocol;

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

        impl Protocol<TestRequest> for TestProtocol {
            type Decoder = TestDecoder;
            type ResponseContext = ();
            type ErrorContext = ();

            fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
                TestDecoder
            }

            fn response_context(&self, _req: &TestRequest) -> Self::ResponseContext {}

            fn error_context(&self, _req: &TestRequest) -> Self::ErrorContext {}

            fn encode_response_chunk(
                &self,
                _context: &mut Self::ResponseContext,
                chunk: Bytes,
                dst: &mut BytesMut,
            ) -> Result<(), NacelleError> {
                dst.extend_from_slice(&chunk);
                Ok(())
            }

            fn encode_response_end(
                &self,
                _context: &mut Self::ResponseContext,
                _dst: &mut BytesMut,
            ) -> Result<(), NacelleError> {
                Ok(())
            }

            fn encode_error(
                &self,
                _context: Option<&Self::ErrorContext>,
                _error: &NacelleError,
                _dst: &mut BytesMut,
            ) -> Result<(), NacelleError> {
                Ok(())
            }
        }

        #[derive(Debug)]
        struct TestConnectionContext {
            connection_id: u64,
        }

        #[test]
        fn tcp_dual_stack_registers_ipv4_and_ipv6_installers() {
            let protocols = NacelleProtocols::<TestHandler>::new()
                .tcp_dual_stack::<TestRequest, _>("gateway", 27017, TestProtocol);

            assert_eq!(protocols.installers.len(), 2);
        }

        #[test]
        fn app_connection_extension_factory_builds_typed_state() {
            let app = NacelleApp::new(TestHandler).with_connection_extension_factory(|meta| {
                TestConnectionContext {
                    connection_id: meta.connection_id,
                }
            });
            let meta = NacelleConnectionMeta::tcp(None, None);
            let extension = app
                .connection_extension_factory
                .as_ref()
                .and_then(|factory| factory(&meta))
                .expect("extension should be created");
            let context = extension
                .downcast::<TestConnectionContext>()
                .expect("extension should have expected type");

            assert_eq!(context.connection_id, meta.connection_id);
        }

        #[test]
        fn app_can_enable_ctrl_c_shutdown() {
            let app = NacelleApp::new(TestHandler).with_ctrl_c_shutdown();

            assert!(app.ctrl_c_shutdown);
        }
    }
}
