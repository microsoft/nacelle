#[cfg(any(feature = "tcp", feature = "http"))]
use std::net::SocketAddr;
#[cfg(feature = "tcp")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleShutdown;
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};

use crate::host::NacelleHost;

#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(all(any(feature = "tcp", feature = "http"), feature = "rustls"))]
use nacelle_core::tls::NacelleTlsConfig;
#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_tcp::NacelleTlsDetectionOptions;
#[cfg(all(feature = "tcp", unix))]
use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
use nacelle_tcp::{NacelleTcpBindOptions, NacelleTcpOptions};

type ListenerInstaller<Observer> = Box<dyn FnOnce(&mut NacelleHost<Observer>) + Send + 'static>;

/// Canonical application composition root.
///
/// Listener registrations may erase their concrete startup closure type, but
/// each transport retains its concrete protocol, handler, and responder types
/// after installation. No listener registry participates in request dispatch.
pub struct NacelleApp<Observer = NoopObserver> {
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    shutdown: NacelleShutdown,
    ctrl_c_shutdown: bool,
    drain_timeout: std::time::Duration,
    listeners: Vec<ListenerInstaller<Observer>>,
}

impl Default for NacelleApp<NoopObserver> {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleApp<NoopObserver> {
    /// Create an application with no registered listeners.
    pub fn new() -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }
}

impl<Observer> NacelleApp<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    /// Create an application with concrete process-wide telemetry.
    pub fn with_telemetry(telemetry: NacelleTelemetry<Observer>) -> Self {
        Self {
            telemetry,
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }
}

impl<Observer> NacelleApp<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    /// Set process-wide limits used by every registered listener.
    pub fn with_limits(mut self, limits: NacelleLimits) -> Self {
        self.runtime_state = NacelleRuntimeState::new(limits);
        self
    }

    /// Set the process-wide runtime state used by every registered listener.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    /// Set the shared application shutdown source.
    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Request graceful shutdown when the process receives Ctrl-C.
    pub fn with_ctrl_c_shutdown(mut self) -> Self {
        self.ctrl_c_shutdown = true;
        self
    }

    /// Enable or disable Ctrl-C shutdown handling.
    pub fn with_ctrl_c_shutdown_enabled(mut self, enabled: bool) -> Self {
        self.ctrl_c_shutdown = enabled;
        self
    }

    /// Set the shared graceful-shutdown drain timeout.
    pub fn with_shutdown_drain_timeout(mut self, drain_timeout: std::time::Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    #[cfg(feature = "tcp")]
    /// Register a typed TCP listener.
    pub fn tcp<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_with_bind_options(name, addr, NacelleTcpBindOptions::default(), server)
    }

    #[cfg(feature = "tcp")]
    /// Register a typed TCP listener with socket options.
    pub fn tcp_with_options<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_with_bind_options(name, addr, NacelleTcpBindOptions::from(options), server)
    }

    #[cfg(feature = "tcp")]
    /// Register a typed TCP listener with bind options.
    pub fn tcp_with_bind_options<P, H, OH, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_with_bind_options(name, addr, bind_options, server);
        }));
        self
    }

    #[cfg(feature = "tcp")]
    /// Register IPv4 and IPv6 TCP listeners for one typed server.
    pub fn tcp_dual_stack<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_dual_stack_with_options(name, port, NacelleTcpOptions::default(), server)
    }

    #[cfg(feature = "tcp")]
    /// Register IPv4 and IPv6 TCP listeners with socket options.
    pub fn tcp_dual_stack_with_options<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let ipv6_options = NacelleTcpBindOptions::from(options.clone()).with_ipv6_only(true);
        self.tcp_with_options(format!("{name}-ipv4"), ipv4_addr, options, server.clone())
            .tcp_with_bind_options(format!("{name}-ipv6"), ipv6_addr, ipv6_options, server)
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a typed Unix-domain socket listener.
    pub fn unix_socket<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.unix_socket_with_options(name, path, NacelleUnixSocketOptions::default(), server)
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a typed Unix-domain socket listener with socket options.
    pub fn unix_socket_with_options<P, H, OH, ServerObserver>(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        options: NacelleUnixSocketOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        self.listeners.push(Box::new(move |host| {
            host.enable_unix_socket_with_options(name, path, options, server);
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "rustls"))]
    /// Register a typed Rustls TCP listener.
    pub fn tcp_tls<P, H, OH, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_tls(name, addr, server, tls_config);
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a typed OpenSSL TCP listener.
    pub fn tcp_openssl<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_openssl_with_bind_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            server,
            tls_config,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a typed OpenSSL TCP listener with bind options.
    pub fn tcp_openssl_with_bind_options<P, H, OH, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_openssl_with_bind_options(name, addr, server, tls_config, bind_options);
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register IPv4 and IPv6 OpenSSL TCP listeners.
    pub fn tcp_openssl_dual_stack<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        self.tcp_openssl(
            format!("{name}-ipv4"),
            ipv4_addr,
            server.clone(),
            tls_config.clone(),
        )
        .tcp_openssl_with_bind_options(
            format!("{name}-ipv6"),
            ipv6_addr,
            NacelleTcpBindOptions::default().with_ipv6_only(true),
            server,
            tls_config,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a listener that accepts typed plain or OpenSSL TCP connections.
    pub fn tcp_optional_openssl<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_optional_openssl_with_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            NacelleTlsDetectionOptions::default(),
            server,
            tls_config,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a plain-or-OpenSSL TCP listener with explicit edge options.
    #[allow(clippy::too_many_arguments)]
    pub fn tcp_optional_openssl_with_options<P, H, OH, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_optional_openssl_with_bind_options(
                name,
                addr,
                server,
                tls_config,
                bind_options,
                detection_options,
            );
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register IPv4 and IPv6 plain-or-OpenSSL TCP listeners.
    pub fn tcp_optional_openssl_dual_stack<P, H, OH, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        self.tcp_optional_openssl_with_options(
            format!("{name}-ipv4"),
            ipv4_addr,
            NacelleTcpBindOptions::default(),
            detection_options.clone(),
            server.clone(),
            tls_config.clone(),
        )
        .tcp_optional_openssl_with_options(
            format!("{name}-ipv6"),
            ipv6_addr,
            NacelleTcpBindOptions::default().with_ipv6_only(true),
            detection_options,
            server,
            tls_config,
        )
    }

    #[cfg(feature = "http")]
    /// Register a typed HTTP/1 listener.
    pub fn http<H, F, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
    ) -> Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_http(name, addr, server);
        }));
        self
    }

    #[cfg(all(feature = "http", feature = "rustls"))]
    /// Register a typed Rustls HTTP/1 listener.
    pub fn http_tls<H, F, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_http_tls(name, addr, server, tls_config);
        }));
        self
    }

    /// Install all listeners and run until shutdown or listener failure.
    pub async fn run(self) -> Result<(), NacelleError> {
        let ctrl_c_task = self
            .ctrl_c_shutdown
            .then(|| spawn_ctrl_c_shutdown(self.shutdown.clone()));
        let mut host = NacelleHost::with_telemetry(self.telemetry)
            .with_runtime_state(self.runtime_state)
            .with_shutdown(self.shutdown)
            .with_shutdown_drain_timeout(self.drain_timeout);
        for install in self.listeners {
            install(&mut host);
        }
        let result = host.wait().await;
        if let Some(task) = ctrl_c_task {
            task.abort();
        }
        result
    }

    #[cfg(test)]
    fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}

fn spawn_ctrl_c_shutdown(shutdown: NacelleShutdown) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.shutdown();
    })
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tcp")]
    use std::convert::Infallible;
    #[cfg(feature = "tcp")]
    use std::sync::Arc;

    #[cfg(feature = "tcp")]
    use bytes::{Bytes, BytesMut};
    #[cfg(any(feature = "tcp", feature = "http"))]
    use nacelle_core::pipeline::handler_fn;
    #[cfg(feature = "tcp")]
    use nacelle_core::telemetry::{NacelleInMemoryObserver, NacelleTelemetryEventKind};
    #[cfg(feature = "tcp")]
    use nacelle_tcp::{
        DecodedMessage, FrameBuffer, MessageDecoder, Protocol, TcpRequestContext, TcpResponse,
        TcpServer,
    };

    use super::*;

    #[test]
    fn app_starts_without_listeners() {
        assert_eq!(NacelleApp::new().listener_count(), 0);
    }

    #[test]
    fn app_can_enable_ctrl_c_shutdown() {
        let app = NacelleApp::new().with_ctrl_c_shutdown();

        assert!(app.ctrl_c_shutdown);
    }

    #[cfg(feature = "tcp")]
    #[test]
    fn app_registers_concrete_tcp_listener() {
        struct Decoder;

        impl MessageDecoder for Decoder {
            type Message = DecodedMessage<(), Infallible>;
            type Error = NacelleError;

            fn decode(
                &mut self,
                _src: &mut BytesMut,
            ) -> Result<Option<Self::Message>, Self::Error> {
                Ok(None)
            }
        }

        #[derive(Clone)]
        struct TestProtocol;

        impl Protocol for TestProtocol {
            type Request = ();
            type OneWayRequest = Infallible;
            type Response = TcpResponse;
            type ConnectionState = ();
            type Decoder = Decoder;
            type ResponseContext = ();
            type ErrorContext = ();

            fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
                Decoder
            }

            fn connection_state(&self, _connection: &nacelle_core::pipeline::ConnectionInfo) {}

            fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
                body_len
            }

            fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
                match *request {}
            }

            fn response_context(&self, _request: &Self::Request) -> Self::ResponseContext {}

            fn error_context(&self, _request: &Self::Request) -> Self::ErrorContext {}

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
                dst.extend_from_slice(&chunk)
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

        let handler = handler_fn(|context: TcpRequestContext<TestProtocol>| async move {
            context.respond(TcpResponse::empty()).await
        });
        let server = TcpServer::<TestProtocol>::builder()
            .protocol(TestProtocol)
            .handler(handler)
            .build()
            .expect("typed TCP server should build");
        let app = NacelleApp::new().tcp(
            "tcp-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            server,
        );

        assert_eq!(app.listener_count(), 1);
    }

    #[cfg(feature = "tcp")]
    #[tokio::test]
    async fn app_with_concrete_sink_observer_propagates_connection_events() {
        struct Decoder;

        impl MessageDecoder for Decoder {
            type Message = DecodedMessage<(), Infallible>;
            type Error = NacelleError;

            fn decode(
                &mut self,
                _src: &mut BytesMut,
            ) -> Result<Option<Self::Message>, Self::Error> {
                Ok(None)
            }
        }

        #[derive(Clone)]
        struct TestProtocol;

        impl Protocol for TestProtocol {
            type Request = ();
            type OneWayRequest = Infallible;
            type Response = TcpResponse;
            type ConnectionState = ();
            type Decoder = Decoder;
            type ResponseContext = ();
            type ErrorContext = ();

            fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
                Decoder
            }

            fn connection_state(&self, _connection: &nacelle_core::pipeline::ConnectionInfo) {}

            fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
                body_len
            }

            fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
                match *request {}
            }

            fn response_context(&self, _request: &Self::Request) -> Self::ResponseContext {}

            fn error_context(&self, _request: &Self::Request) -> Self::ErrorContext {}

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
                dst.extend_from_slice(&chunk)
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

        let handler = handler_fn(|context: TcpRequestContext<TestProtocol>| async move {
            context.respond(TcpResponse::empty()).await
        });
        let server = TcpServer::<TestProtocol>::builder()
            .protocol(TestProtocol)
            .handler(handler)
            .build()
            .expect("typed TCP server should build");
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("loopback address should be available");
        let addr = listener
            .local_addr()
            .expect("listener should have an address");
        drop(listener);
        let sink = Arc::new(NacelleInMemoryObserver::new());
        let shutdown = NacelleShutdown::new();
        let app = NacelleApp::with_telemetry(
            nacelle_core::telemetry::NacelleTelemetry::new().with_observer(sink.clone()),
        )
        .with_shutdown(shutdown.clone())
        .tcp("tcp-observer-test", addr, server);

        assert_eq!(app.listener_count(), 1);

        let app_task = tokio::spawn(app.run());
        let client = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(client) => break client,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("application should start accepting connections");

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if sink
                    .events()
                    .iter()
                    .any(|event| event.kind == NacelleTelemetryEventKind::ConnectionOpened)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("installed server should use the application observer");

        drop(client);
        shutdown.shutdown();
        app_task
            .await
            .expect("application task should join")
            .expect("application should shut down cleanly");
    }

    #[cfg(feature = "http")]
    #[test]
    fn app_registers_concrete_http_listener() {
        let handler = handler_fn(
            |_context: nacelle_http::HttpRequestContext<()>| async move {
                Err(NacelleError::ResourceLimit("test_http_handler"))
            },
        );
        let app = NacelleApp::new().http(
            "http-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            nacelle_http::HyperServer::new(handler),
        );

        assert_eq!(app.listener_count(), 1);
    }
}
