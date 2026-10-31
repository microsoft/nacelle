#[cfg(any(feature = "tcp", feature = "http"))]
use std::net::SocketAddr;
#[cfg(feature = "tcp")]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;
use std::sync::Arc;

use nacelle_core::error::NacelleError;
#[cfg(all(test, feature = "http"))]
use nacelle_core::error::NacelleResourceLimitReason;
use nacelle_core::lifecycle::NacelleShutdown;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};

use crate::host::NacelleHost;

#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_openssl::NacelleOpenSslConfig;
#[cfg(all(any(feature = "tcp", feature = "http"), feature = "rustls"))]
use nacelle_rustls::NacelleTlsConfig;
#[cfg(feature = "experimental-openssl-detection")]
use nacelle_tcp::NacelleTlsDetectionOptions;
#[cfg(all(feature = "tcp", unix))]
use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
use nacelle_tcp::{NacelleTcpBindOptions, NacelleTcpOptions};

type ListenerInstaller<Observer, AppState> = Box<
    dyn FnOnce(&mut NacelleHost<Observer, AppState>) -> Result<(), NacelleError> + Send + 'static,
>;

/// Canonical application composition root.
///
/// Listener registrations may erase their concrete startup closure type, but
/// each transport retains its concrete protocol, handler, and responder types
/// after installation. No listener registry participates in request dispatch.
///
/// # Serving contract
///
/// Every public listener-registration method on this type follows this
/// contract unless the method documents a narrower requirement:
///
/// - The app owns its application state, runtime state, telemetry, shutdown
///   source, and registered server values. Registration does not bind sockets
///   or spawn tasks; [`run`](Self::run) installs every listener into one
///   [`NacelleHost`] and owns that host until completion.
/// - A listener failure requests process-wide shutdown. Shutdown-aware
///   listeners stop accepting, drain active connections for the configured
///   timeout, and then abort remaining connection tasks. Dropping the `run`
///   future cancels supervision without guaranteeing that drain sequence.
/// - Process-wide connection, per-peer, request, streaming-task, and body-size
///   limits come from [`NacelleRuntimeState`]. Concrete TCP
///   and HTTP servers retain their transport limits and policy.
/// - Plain TCP and Unix listeners require `tcp`; HTTP/1 requires `http`;
///   Rustls listeners require `rustls`; and OpenSSL listeners require
///   `openssl`. Unix listeners are available only on Unix. Features prefixed
///   with `experimental-` are outside the supported `0.3` contract.
///
/// # Errors
///
/// [`run`](Self::run) returns the first listener bind, accept, transport,
/// resource-limit, timeout, or supervised-task error after requesting shutdown
/// from the remaining listeners. Match [`NacelleError`] variants and reason
/// enums rather than parsing their display text.
///
/// # Panics
///
/// `run` and Ctrl-C handling must execute inside an entered Tokio runtime.
/// Tokio may panic if they are polled without one. Nacelle does not otherwise
/// intentionally panic for peer or configuration input. Application-handler
/// panics are supervised task failures; panic-abort builds terminate instead
/// of unwinding.
///
/// # Example
///
/// Run the typed TCP echo application from the workspace root:
///
/// ```text
/// cargo run -p nacelle-examples --bin echo
/// ```
pub struct NacelleApp<Observer = NoopObserver, AppState = ()> {
    telemetry: NacelleTelemetry<Observer>,
    app_state: Arc<AppState>,
    runtime_state: NacelleRuntimeState,
    shutdown: NacelleShutdown,
    ctrl_c_shutdown: bool,
    drain_timeout: std::time::Duration,
    listeners: Vec<ListenerInstaller<Observer, AppState>>,
}

impl Default for NacelleApp<NoopObserver, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleApp<NoopObserver, ()> {
    /// Create an application with no registered listeners.
    pub fn new() -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            app_state: Arc::new(()),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }
}

impl<AppState> NacelleApp<NoopObserver, AppState> {
    /// Create an application with one typed dependency root.
    pub fn with_state(app_state: AppState) -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            app_state: Arc::new(app_state),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }
}

impl<Observer> NacelleApp<Observer, ()>
where
    Observer: NacelleTelemetryObserver,
{
    /// Create an application with concrete process-wide telemetry.
    pub fn with_telemetry(telemetry: NacelleTelemetry<Observer>) -> Self {
        Self {
            telemetry,
            app_state: Arc::new(()),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }
}

impl<Observer, AppState> NacelleApp<Observer, AppState>
where
    Observer: NacelleTelemetryObserver,
    AppState: Send + Sync + 'static,
{
    /// Create an application with concrete process-wide telemetry and application state.
    pub fn with_state_and_telemetry(
        app_state: AppState,
        telemetry: NacelleTelemetry<Observer>,
    ) -> Self {
        Self {
            telemetry,
            app_state: Arc::new(app_state),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            ctrl_c_shutdown: false,
            drain_timeout: std::time::Duration::from_secs(30),
            listeners: Vec::new(),
        }
    }

    /// Set the process-wide runtime state used by every registered listener.
    ///
    /// Returns a configuration error if the previous state has been shared.
    pub fn with_runtime_state(
        mut self,
        runtime_state: NacelleRuntimeState,
    ) -> Result<Self, NacelleError> {
        self.runtime_state.replace(runtime_state)?;
        Ok(self)
    }

    /// Obtain a handle for the state installed by [`Self::run`].
    pub fn runtime_control(&self) -> nacelle_core::NacelleRuntimeControl {
        self.runtime_state.control()
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
    pub fn tcp<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_with_bind_options(name, addr, NacelleTcpBindOptions::default(), server)
    }

    #[cfg(feature = "tcp")]
    /// Register a serial TCP listener with exclusive mutable connection state.
    pub fn serial_tcp<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.serial_tcp_with_bind_options(name, addr, NacelleTcpBindOptions::default(), server)
    }

    #[cfg(feature = "tcp")]
    /// Register a serial TCP listener with explicit bind options.
    pub fn serial_tcp_with_bind_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_serial_tcp_with_bind_options(name, addr, bind_options, server)
                .map(|_| ())
        }));
        self
    }

    #[cfg(feature = "tcp")]
    /// Register a typed TCP listener with socket options.
    pub fn tcp_with_options<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_with_bind_options(name, addr, NacelleTcpBindOptions::from(options), server)
    }

    #[cfg(feature = "tcp")]
    /// Register a typed TCP listener with bind options.
    pub fn tcp_with_bind_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_with_bind_options(name, addr, bind_options, server)
                .map(|_| ())
        }));
        self
    }

    #[cfg(feature = "tcp")]
    /// Register IPv4 and IPv6 TCP listeners for one typed server.
    pub fn tcp_dual_stack<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Result<Self, NacelleError>
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.tcp_dual_stack_with_options(name, port, NacelleTcpOptions::default(), server)
    }

    #[cfg(feature = "tcp")]
    /// Register IPv4 and IPv6 TCP listeners with socket options.
    ///
    /// Installs app state before cloning the server. Configure startup state
    /// first; conflicting exported server handles return a configuration error.
    pub fn tcp_dual_stack_with_options<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Result<Self, NacelleError>
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let ipv6_options = NacelleTcpBindOptions::from(options.clone()).with_ipv6_only(true);
        let server = server.with_runtime_state(self.runtime_state.clone())?;
        Ok(self
            .tcp_with_options(format!("{name}-ipv4"), ipv4_addr, options, server.clone())
            .tcp_with_bind_options(format!("{name}-ipv6"), ipv6_addr, ipv6_options, server))
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a typed Unix-domain socket listener.
    pub fn unix_socket<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.unix_socket_with_options(name, path, NacelleUnixSocketOptions::default(), server)
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a typed Unix-domain socket listener with socket options.
    pub fn unix_socket_with_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        options: NacelleUnixSocketOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        self.listeners.push(Box::new(move |host| {
            host.enable_unix_socket_with_options(name, path, options, server)
                .map(|_| ())
        }));
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a serial Unix-domain socket listener.
    pub fn serial_unix_socket<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.serial_unix_socket_with_options(
            name,
            path,
            NacelleUnixSocketOptions::default(),
            server,
        )
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Register a serial Unix-domain socket listener with socket options.
    pub fn serial_unix_socket_with_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        options: NacelleUnixSocketOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        self.listeners.push(Box::new(move |host| {
            host.enable_serial_unix_socket_with_options(name, path, options, server)
                .map(|_| ())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "rustls"))]
    /// Register a typed Rustls TCP listener.
    pub fn tcp_tls<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_tls(name, addr, server, tls_config)
                .map(|_| ())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a typed OpenSSL TCP listener.
    pub fn tcp_openssl<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
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
    pub fn tcp_openssl_with_bind_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_tcp_openssl_with_bind_options(name, addr, server, tls_config, bind_options)
                .map(|_| ())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a serial OpenSSL TCP listener.
    pub fn serial_tcp_openssl<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.serial_tcp_openssl_with_bind_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            server,
            tls_config,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register a serial OpenSSL TCP listener with bind options.
    pub fn serial_tcp_openssl_with_bind_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_serial_tcp_openssl_with_bind_options(
                name,
                addr,
                server,
                tls_config,
                bind_options,
            )
            .map(|_| ())
        }));
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Register IPv4 and IPv6 OpenSSL TCP listeners.
    ///
    /// Configure startup state first. State installation errors are returned
    /// before the server is cloned for the two listeners.
    pub fn tcp_openssl_dual_stack<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Result<Self, NacelleError>
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let server = server.with_runtime_state(self.runtime_state.clone())?;
        Ok(self
            .tcp_openssl(
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
            ))
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Register a listener that accepts typed plain or OpenSSL TCP connections.
    pub fn tcp_optional_openssl<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
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

    #[cfg(feature = "experimental-openssl-detection")]
    /// Register a plain-or-OpenSSL TCP listener with explicit edge options.
    #[allow(clippy::too_many_arguments)]
    pub fn tcp_optional_openssl_with_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
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
            )
            .map(|_| ())
        }));
        self
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Register a serial listener that accepts plaintext or OpenSSL TCP connections.
    pub fn serial_tcp_optional_openssl<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.serial_tcp_optional_openssl_with_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            NacelleTlsDetectionOptions::default(),
            server,
            tls_config,
        )
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Register a serial plaintext-or-OpenSSL TCP listener with edge options.
    #[allow(clippy::too_many_arguments)]
    pub fn serial_tcp_optional_openssl_with_options<P, H, ServerObserver>(
        mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_serial_tcp_optional_openssl_with_bind_options(
                name,
                addr,
                server,
                tls_config,
                bind_options,
                detection_options,
            )
            .map(|_| ())
        }));
        self
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Register IPv4 and IPv6 plain-or-OpenSSL TCP listeners.
    ///
    /// Configure startup state first. State installation errors are returned
    /// before the server is cloned for the two listeners.
    pub fn tcp_optional_openssl_dual_stack<P, H, ServerObserver>(
        self,
        name: impl Into<String>,
        port: u16,
        server: nacelle_tcp::TcpServer<P, H, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Result<Self, NacelleError>
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpService<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let ipv4_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port);
        let ipv6_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
        let server = server.with_runtime_state(self.runtime_state.clone())?;
        Ok(self
            .tcp_optional_openssl_with_options(
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
            ))
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
        H: nacelle_http::HttpService<F::State, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_http(name, addr, server).map(|_| ())
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
        H: nacelle_http::HttpService<F::State, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        self.listeners.push(Box::new(move |host| {
            host.enable_http_tls(name, addr, server, tls_config)
                .map(|_| ())
        }));
        self
    }

    /// Install all listeners and run until shutdown or listener failure.
    ///
    /// This is the terminal operation for the [`NacelleApp`] serving contract.
    ///
    /// # Errors
    ///
    /// Returns the first listener or supervised-task error after requesting
    /// shutdown from the remaining listeners. If listener installation fails,
    /// drains already-installed listeners using the configured timeout before
    /// returning the original installation error.
    ///
    /// # Panics
    ///
    /// Tokio may panic if this future is polled outside an entered runtime.
    pub async fn run(self) -> Result<(), NacelleError> {
        let shutdown = self.shutdown.clone();
        let mut host = NacelleHost::with_shared_state_and_telemetry(self.app_state, self.telemetry)
            .with_runtime_state(self.runtime_state)?
            .with_shutdown(self.shutdown)
            .with_shutdown_drain_timeout(self.drain_timeout);
        for install in self.listeners {
            if let Err(error) = install(&mut host) {
                host.shutdown();
                // Preserve the installation error even if draining also fails.
                let _ = host.wait().await;
                return Err(error);
            }
        }
        let ctrl_c_task = self
            .ctrl_c_shutdown
            .then(|| spawn_ctrl_c_shutdown(shutdown));
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
    use std::sync::Arc;

    #[cfg(feature = "tcp")]
    use bytes::{Bytes, BytesMut};
    #[cfg(feature = "tcp")]
    use nacelle_codec::MessageDecoder;
    #[cfg(any(feature = "tcp", feature = "http"))]
    use nacelle_core::pipeline::handler_fn;
    #[cfg(feature = "tcp")]
    use nacelle_core::telemetry::{NacelleInMemoryObserver, NacelleTelemetryEventKind};
    #[cfg(feature = "tcp")]
    use nacelle_tcp::{
        DecodedRequest, FrameBuffer, Protocol, SerialTcpHandler, SerialTcpRequestContext,
        SerialTcpServer, TcpRequestContext, TcpResponse, TcpServer,
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

    #[test]
    fn app_rejects_replacing_state_with_an_exported_control() {
        let app = NacelleApp::new();
        let control = app.runtime_control();
        assert!(matches!(
            app.with_runtime_state(NacelleRuntimeState::default()),
            Err(NacelleError::Configuration(
                nacelle_core::NacelleConfigurationError::RuntimeStateInUse
            ))
        ));
        assert_eq!(control.scope(), nacelle_core::NacelleControlScope::Shared);
    }

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn installation_failure_shuts_down_and_drains_installed_listeners() {
        let listener_state = std::sync::Arc::new(());
        let handler_state = listener_state.clone();
        let server = nacelle_http::HyperServer::new(handler_fn(
            move |_context: nacelle_http::HttpRequestContext<()>| {
                let _ = &handler_state;
                async { Err(NacelleError::ConnectionClosed) }
            },
        ));
        let invalid_server = nacelle_http::HyperServer::new(handler_fn(
            |_context: nacelle_http::HttpRequestContext<()>| async {
                Err(NacelleError::ConnectionClosed)
            },
        ));
        let _server_control = invalid_server.runtime_control();
        let shutdown = NacelleShutdown::new();
        let shutdown_token = shutdown.token();
        let addr = "127.0.0.1:0".parse().expect("valid socket address");
        let app = NacelleApp::new()
            .with_shutdown(shutdown)
            .http("installed", addr, server)
            .http("invalid", addr, invalid_server);

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), app.run())
            .await
            .expect("installation failure should finish draining")
            .expect_err("exported server control should prevent installation");

        assert!(matches!(
            error,
            NacelleError::Configuration(nacelle_core::NacelleConfigurationError::RuntimeStateInUse)
        ));
        assert!(shutdown_token.is_shutdown());
        assert_eq!(
            std::sync::Arc::strong_count(&listener_state),
            1,
            "installed listener must release its handler before run returns"
        );
    }

    #[cfg(feature = "tcp")]
    #[test]
    fn app_registers_concrete_tcp_listener() {
        struct Decoder;

        impl MessageDecoder for Decoder {
            type Message = DecodedRequest<()>;
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

        let dual_server = TcpServer::<TestProtocol>::builder()
            .protocol(TestProtocol)
            .handler(handler_fn(
                |context: TcpRequestContext<TestProtocol>| async move {
                    context.respond(TcpResponse::empty()).await
                },
            ))
            .build()
            .expect("dual server");
        let dual_app = NacelleApp::new();
        let control = dual_app.runtime_control();
        let dual_app = dual_app
            .tcp_dual_stack("dual", 0, dual_server)
            .expect("dual registration");
        assert_eq!(dual_app.listener_count(), 2);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let _entered = runtime.enter();
        let mut host = NacelleHost::new()
            .with_runtime_state(dual_app.runtime_state.clone())
            .expect("app state");
        for install in dual_app.listeners {
            install(&mut host).expect("both cloned servers retain the installed state");
        }
        control
            .set_max_connections(Some(0))
            .expect("shared control");
        assert_eq!(host.runtime_control().snapshot().max_connections, Some(0));
        drop(host);

        let state_handler = handler_fn(
            |context: TcpRequestContext<TestProtocol, String>| async move {
                assert_eq!(context.app_state(), "application-state");
                context.respond(TcpResponse::empty()).await
            },
        );
        let state_server = TcpServer::<TestProtocol>::builder()
            .protocol(TestProtocol)
            .handler(state_handler)
            .build()
            .expect("state-aware TCP server should build");
        let state_app = NacelleApp::with_state(String::from("application-state")).tcp(
            "state-tcp-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            state_server,
        );
        assert_eq!(state_app.listener_count(), 1);

        struct SerialHandler;

        impl SerialTcpHandler<TestProtocol> for SerialHandler {
            async fn call<'connection>(
                &'connection self,
                context: SerialTcpRequestContext<'connection, TestProtocol>,
            ) -> Result<nacelle_tcp::TcpHandlerCompletion<TestProtocol>, NacelleError> {
                context.respond(TcpResponse::empty()).await
            }
        }

        let serial_app = NacelleApp::new().serial_tcp(
            "serial-tcp-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            SerialTcpServer::new(TestProtocol, SerialHandler),
        );
        assert_eq!(serial_app.listener_count(), 1);

        #[cfg(unix)]
        {
            let serial_unix_app = NacelleApp::new().serial_unix_socket(
                "serial-unix-test",
                "/tmp/nacelle-serial-unix-test.sock",
                SerialTcpServer::new(TestProtocol, SerialHandler),
            );
            assert_eq!(serial_unix_app.listener_count(), 1);
        }
    }

    #[cfg(feature = "tcp")]
    #[tokio::test]
    async fn app_control_and_observer_reach_installed_tcp_listener() {
        struct Decoder;

        impl MessageDecoder for Decoder {
            type Message = DecodedRequest<()>;
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

        let control = app.runtime_control();
        control
            .set_max_connections(Some(0))
            .expect("pause before run");
        let app_task = tokio::spawn(app.run());
        let mut client = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(client) => break client,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("application should start accepting connections");

        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                tokio::io::AsyncReadExt::read(&mut client, &mut byte),
            )
            .await
            .expect("rejected connection closes")
            .expect("read EOF"),
            0
        );
        assert!(
            !sink
                .events()
                .iter()
                .any(|event| event.kind == NacelleTelemetryEventKind::ConnectionOpened)
        );
        control
            .set_max_connections(Some(1))
            .expect("resume live listener");
        let client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("resumed listener");

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
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::Other("test_http_handler"),
                ))
            },
        );
        let app = NacelleApp::new().http(
            "http-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            nacelle_http::HyperServer::new(handler),
        );

        assert_eq!(app.listener_count(), 1);

        let state_handler = handler_fn(
            |context: nacelle_http::HttpRequestContext<(), String>| async move {
                assert_eq!(context.app_state(), "application-state");
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::Other("test_http_handler"),
                ))
            },
        );
        let state_app = NacelleApp::with_state(String::from("application-state")).http(
            "state-http-test",
            "127.0.0.1:0".parse().expect("valid socket address"),
            nacelle_http::HyperServer::new(state_handler),
        );

        assert_eq!(state_app.listener_count(), 1);
    }
}
