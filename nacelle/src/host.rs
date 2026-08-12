#[cfg(any(feature = "tcp", feature = "http"))]
use std::net::SocketAddr;
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;
use std::sync::Arc;

use tokio::task::JoinSet;

#[cfg(test)]
use nacelle_core::error::NacelleResourceLimitReason;
use nacelle_core::error::{NacelleError, NacelleTimeoutReason};
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdown, NacelleShutdownToken};
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
#[cfg(any(feature = "tcp", feature = "http"))]
use nacelle_core::telemetry::NacelleTransport;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};
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

/// Manual listener supervisor for advanced serving integrations.
///
/// `NacelleApp` is the primary composition API. A host is appropriate when an
/// application must start listeners incrementally, hold the shutdown source,
/// or choose explicitly between waiting and initiating shutdown.
///
/// # Serving contract
///
/// Every public `enable_*` method immediately configures the concrete server,
/// spawns one listener task on the entered Tokio runtime, and returns a mutable
/// borrow of the host. The host owns those listener tasks, one shared
/// application-state `Arc`, runtime limits, telemetry, shutdown source, and
/// drain deadline until [`wait`](Self::wait) or
/// [`shutdown_and_wait`](Self::shutdown_and_wait) consumes it.
///
/// A listener failure requests shutdown from all other listeners. Token-aware
/// listeners stop accepting, drain active connections until the shared
/// deadline, and then abort remaining connection tasks. Dropping the host or a
/// terminal future cancels supervision without guaranteeing a graceful drain.
/// Process-wide limits come from [`NacelleRuntimeState`]; concrete servers
/// retain their TCP or HTTP limits and policy.
///
/// Plain TCP and Unix listeners require `tcp`; HTTP/1 requires `http`; Rustls
/// listeners require `rustls`; and OpenSSL listeners require `openssl`. Unix
/// listeners are available only on Unix. Features prefixed with
/// `experimental-` are outside the supported `0.3` contract.
///
/// # Errors
///
/// Terminal methods return listener bind, accept, transport, resource-limit,
/// timeout, or supervised-task errors as [`NacelleError`]. Match variants and
/// reason enums rather than parsing display text.
///
/// # Panics
///
/// Constructors are runtime-independent, but every `enable_*` method spawns a
/// Tokio task and may panic unless called while a Tokio runtime is entered.
/// Application-handler panics are supervised task failures; panic-abort builds
/// terminate instead of unwinding.
///
/// # Example
///
/// Run the manual-host TCP echo application from the workspace root:
///
/// ```text
/// cargo run -p nacelle-examples --bin manual_host
/// ```
pub struct NacelleHost<Observer = NoopObserver, AppState = ()> {
    telemetry: NacelleTelemetry<Observer>,
    app_state: Arc<AppState>,
    runtime_state: NacelleRuntimeState,
    shutdown: NacelleShutdown,
    drain_deadline: NacelleDrainDeadline,
    tasks: JoinSet<Result<(), NacelleError>>,
}

impl Default for NacelleHost<NoopObserver, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleHost<NoopObserver, ()> {
    /// Create a host with default telemetry, limits, and shutdown policy.
    pub fn new() -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            app_state: Arc::new(()),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }
}

impl<AppState> NacelleHost<NoopObserver, AppState> {
    /// Create a host with one typed application dependency root.
    pub fn with_state(app_state: AppState) -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            app_state: Arc::new(app_state),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }
}

impl<Observer> NacelleHost<Observer, ()>
where
    Observer: NacelleTelemetryObserver,
{
    /// Create a host with concrete process-wide telemetry.
    pub fn with_telemetry(telemetry: NacelleTelemetry<Observer>) -> Self {
        Self {
            telemetry,
            app_state: Arc::new(()),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }
}

impl<Observer, AppState> NacelleHost<Observer, AppState>
where
    Observer: NacelleTelemetryObserver,
    AppState: Send + Sync + 'static,
{
    /// Create a host with concrete process-wide telemetry and application state.
    pub fn with_state_and_telemetry(
        app_state: AppState,
        telemetry: NacelleTelemetry<Observer>,
    ) -> Self {
        Self::with_shared_state_and_telemetry(Arc::new(app_state), telemetry)
    }

    pub(crate) fn with_shared_state_and_telemetry(
        app_state: Arc<AppState>,
        telemetry: NacelleTelemetry<Observer>,
    ) -> Self {
        Self {
            telemetry,
            app_state,
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }

    /// Borrow the typed application dependency root.
    pub fn app_state(&self) -> &AppState {
        self.app_state.as_ref()
    }

    /// Replace the process-wide limits used by every enabled listener.
    pub fn with_limits(mut self, limits: NacelleLimits) -> Self {
        self.runtime_state = NacelleRuntimeState::new(limits);
        self
    }

    /// Replace the process-wide runtime state used by every enabled listener.
    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    /// Return a token that observes this host's shutdown source.
    pub fn shutdown_token(&self) -> NacelleShutdownToken {
        self.shutdown.token()
    }

    /// Replace the process-wide shutdown source.
    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Request shutdown without waiting for listener or connection tasks.
    pub fn shutdown(&self) {
        self.telemetry.shutdown_requested();
        self.shutdown.shutdown();
    }

    /// Set the shared graceful-shutdown drain timeout.
    pub fn with_shutdown_drain_timeout(self, drain_timeout: std::time::Duration) -> Self {
        self.drain_deadline.set(drain_timeout);
        self
    }

    #[cfg(feature = "tcp")]
    /// Start a typed TCP listener under this host's supervision.
    pub fn enable_tcp<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    /// Start a serial TCP listener with exclusive mutable connection state.
    pub fn enable_serial_tcp<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_serial_tcp_with_bind_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            server,
        )
    }

    #[cfg(feature = "tcp")]
    /// Start a serial TCP listener with explicit bind options.
    pub fn enable_serial_tcp_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_serial_tcp_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    bind_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    /// Start a typed TCP listener with explicit stream options.
    pub fn enable_tcp_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        tcp_options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tcp_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    /// Start a typed TCP listener with explicit bind and stream options.
    pub fn enable_tcp_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_bind_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                bind_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Start a typed Unix-domain socket listener.
    pub fn enable_unix_socket<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        let path_label = path.display().to_string();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("unix_socket"), &name, &path_label);
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_unix_with_shutdown_deadline(
                std::sync::Arc::new(server),
                path,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("unix_socket"),
                    &name,
                    &path_label,
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Start a Unix-domain socket listener with explicit path options.
    pub fn enable_unix_socket_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        let path_label = path.display().to_string();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("unix_socket"), &name, &path_label);
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_unix_with_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                path,
                unix_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("unix_socket"),
                    &name,
                    &path_label,
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Start a serial Unix-domain socket listener.
    pub fn enable_serial_unix_socket<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_serial_unix_socket_with_options(
            name,
            path,
            NacelleUnixSocketOptions::default(),
            server,
        )
    }

    #[cfg(all(feature = "tcp", unix))]
    /// Start a serial Unix-domain listener with explicit path options.
    pub fn enable_serial_unix_socket_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        let path_label = path.display().to_string();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("unix_socket"), &name, &path_label);
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_serial_unix_with_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    path,
                    unix_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("unix_socket"),
                    &name,
                    &path_label,
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "rustls"))]
    /// Start a typed Rustls TCP listener.
    pub fn enable_tcp_tls<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_tls_with_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tls_config,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Start a typed OpenSSL TCP listener.
    pub fn enable_tcp_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_openssl_with_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Start an OpenSSL TCP listener with explicit stream options.
    pub fn enable_tcp_openssl_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Start an OpenSSL TCP listener with explicit bind and stream options.
    pub fn enable_tcp_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    tls_config,
                    bind_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Start a serial OpenSSL TCP listener.
    pub fn enable_serial_tcp_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_serial_tcp_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    /// Start a serial OpenSSL TCP listener with explicit bind options.
    pub fn enable_serial_tcp_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_serial_tcp_openssl_with_bind_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tls_config,
                bind_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Start an experimental listener that accepts plaintext or OpenSSL TCP.
    pub fn enable_tcp_optional_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_optional_openssl_with_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpOptions::default(),
            NacelleTlsDetectionOptions::default(),
        )
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Start optional OpenSSL detection with explicit edge options.
    pub fn enable_tcp_optional_openssl_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_optional_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
            detection_options,
        )
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Start optional OpenSSL detection with explicit bind and detection options.
    pub fn enable_tcp_optional_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P, AppState>,
        OH: nacelle_tcp::TcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    tls_config,
                    bind_options,
                    detection_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(NacelleTransport::new("tcp"), &name, &addr.to_string(), error);
            }
            result
        });
        self
    }

    #[cfg(feature = "experimental-openssl-detection")]
    /// Start a serial listener that accepts plaintext or OpenSSL TCP.
    pub fn enable_serial_tcp_optional_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_serial_tcp_optional_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::default(),
            NacelleTlsDetectionOptions::default(),
        )
    }

    #[cfg(feature = "experimental-openssl-detection")]
    #[allow(clippy::too_many_arguments)]
    /// Start serial optional OpenSSL detection with explicit edge options.
    pub fn enable_serial_tcp_optional_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P, AppState>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_serial_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tls_config,
                bind_options,
                detection_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "http")]
    /// Start a typed HTTP/1 listener.
    pub fn enable_http<H, F, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
    ) -> &mut Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("http"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = server
                .serve_with_shutdown_deadline(addr, shutdown, drain_deadline)
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("http"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "http", feature = "rustls"))]
    /// Start a typed HTTP/1 listener over Rustls.
    pub fn enable_http_tls<H, F, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> &mut Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State, AppState>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_app_state(self.app_state.clone())
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("http"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let result = server
                .serve_tls_listener_with_shutdown_deadline(
                    listener,
                    tls_config,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("http"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    /// Wait until every listener exits, requesting shutdown on the first failure.
    ///
    /// # Errors
    ///
    /// Returns the first listener or supervised-task error after requesting
    /// shutdown from the remaining listeners.
    pub async fn wait(mut self) -> Result<(), NacelleError> {
        let mut first_error = None;
        while let Some(result) = self.tasks.join_next().await {
            let error = match result {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => error,
                Err(error) => NacelleError::from(error),
            };
            if first_error.is_none() {
                self.telemetry.shutdown_requested();
                self.shutdown.shutdown();
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Request shutdown and wait up to the configured default drain timeout.
    ///
    /// # Errors
    ///
    /// Returns a listener or task error, or `ShutdownDrain` when active
    /// connections remain after the timeout.
    pub async fn shutdown_and_wait(self) -> Result<(), NacelleError> {
        self.shutdown_and_wait_timeout(std::time::Duration::from_secs(30))
            .await
    }

    /// Request shutdown and wait up to `drain_timeout` for all work to finish.
    ///
    /// # Errors
    ///
    /// Returns a listener or task error, or `ShutdownDrain` when active
    /// connections remain after `drain_timeout`.
    pub async fn shutdown_and_wait_timeout(
        mut self,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        self.drain_deadline.set(drain_timeout);
        self.telemetry.shutdown_requested();
        self.shutdown.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            result??;
        }

        let drain = async {
            while self.runtime_state.active_connections() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(drain_timeout, drain)
            .await
            .map_err(|_| NacelleError::Timeout(NacelleTimeoutReason::ShutdownDrain))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn listener_failure_requests_shutdown_and_drains_remaining_tasks() {
        let mut host = NacelleHost::new();
        let mut shutdown = host.shutdown_token();
        let (drained_tx, drained_rx) = oneshot::channel();

        host.tasks.spawn(async {
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Other("test_listener_failure"),
            ))
        });
        host.tasks.spawn(async move {
            assert!(shutdown.changed().await);
            drained_tx.send(()).expect("drain observer should be open");
            Ok(())
        });

        let error = host.wait().await.expect_err("listener failure should win");

        assert!(matches!(
            error,
            NacelleError::ResourceLimit(NacelleResourceLimitReason::Other("test_listener_failure"))
        ));
        drained_rx
            .await
            .expect("remaining listener should observe shutdown and drain");
    }
}
