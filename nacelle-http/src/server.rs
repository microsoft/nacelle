use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use hyper::body::{Body, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::encoder::{HttpBody, incoming_to_body, incoming_to_local_body, response_to_http};
use crate::limits::NacelleHttpLimits;
pub use crate::policy::NacelleHttpPolicy;
use crate::policy::validate_http_policy;
use crate::rate_limit::{PeerRateWindow, allow_peer_request, forwarded_peer_ip};
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::{ConnectionContext, ConnectionInfo, RequiredResponder};
use nacelle_core::request::NacelleConnectionMeta;
#[cfg(feature = "rustls")]
use nacelle_core::request::NacelleConnectionTlsMeta;
use nacelle_core::telemetry::{
    NacelleTelemetry, NacelleTelemetryEventKind, NacelleTelemetryObserver, NacelleTransport,
    NoopObserver,
};
#[cfg(feature = "rustls")]
use nacelle_core::tls::NacelleTlsConfig;

use crate::pipeline::{
    HttpConnectionStateFactory, HttpHandler, HttpHandlerCompletion, HttpRequest,
    HttpRequestContext, HttpResponder, HttpResponse, LocalHttpConnectionStateFactory,
    LocalHttpHandler, LocalHttpRequestContext, NoHttpConnectionState,
};

trait HttpDispatch<State, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    fn connection_info(&self) -> &ConnectionInfo;

    fn call(
        &self,
        request: HttpRequest,
    ) -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>>;

    fn incoming_body(
        &self,
        incoming: Incoming,
        body_len_hint: Option<usize>,
        request_body_bytes: Option<Arc<AtomicUsize>>,
        runtime_state: NacelleRuntimeState,
        http_limits: NacelleHttpLimits,
        telemetry: NacelleTelemetry<Observer>,
    ) -> nacelle_core::NacelleBody;
}

struct SharedHttpDispatch<H, State> {
    handler: Arc<H>,
    connection: ConnectionContext<Arc<State>>,
}

impl<H, State> Clone for SharedHttpDispatch<H, State> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            connection: self.connection.clone(),
        }
    }
}

impl<H, State, Observer> HttpDispatch<State, Observer> for SharedHttpDispatch<H, State>
where
    State: Send + Sync + 'static,
    H: HttpHandler<State>,
    Observer: NacelleTelemetryObserver,
{
    fn connection_info(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    fn call(
        &self,
        request: HttpRequest,
    ) -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> {
        let context = HttpRequestContext::new(
            request,
            RequiredResponder::new(HttpResponder),
            (),
            self.connection.clone(),
        );
        nacelle_core::pipeline::Handler::call(self.handler.as_ref(), context)
    }

    fn incoming_body(
        &self,
        incoming: Incoming,
        body_len_hint: Option<usize>,
        request_body_bytes: Option<Arc<AtomicUsize>>,
        runtime_state: NacelleRuntimeState,
        http_limits: NacelleHttpLimits,
        telemetry: NacelleTelemetry<Observer>,
    ) -> nacelle_core::NacelleBody {
        incoming_to_body(
            incoming,
            body_len_hint,
            request_body_bytes,
            runtime_state,
            http_limits,
            telemetry,
        )
    }
}

struct LocalHttpDispatch<H, State> {
    handler: Rc<H>,
    connection: ConnectionContext<Rc<State>>,
}

impl<H, State> Clone for LocalHttpDispatch<H, State> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            connection: self.connection.clone(),
        }
    }
}

#[allow(clippy::future_not_send)]
impl<H, State, Observer> HttpDispatch<State, Observer> for LocalHttpDispatch<H, State>
where
    State: 'static,
    H: LocalHttpHandler<State>,
    Observer: NacelleTelemetryObserver,
{
    fn connection_info(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    fn call(
        &self,
        request: HttpRequest,
    ) -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> {
        let context = LocalHttpRequestContext::new(
            request,
            RequiredResponder::new(HttpResponder),
            (),
            self.connection.clone(),
        );
        nacelle_core::pipeline::LocalHandler::call(self.handler.as_ref(), context)
    }

    fn incoming_body(
        &self,
        incoming: Incoming,
        body_len_hint: Option<usize>,
        request_body_bytes: Option<Arc<AtomicUsize>>,
        runtime_state: NacelleRuntimeState,
        http_limits: NacelleHttpLimits,
        telemetry: NacelleTelemetry<Observer>,
    ) -> nacelle_core::NacelleBody {
        incoming_to_local_body(
            incoming,
            body_len_hint,
            request_body_bytes,
            runtime_state,
            http_limits,
            telemetry,
        )
    }
}

pub struct HyperServer<H = (), F = NoHttpConnectionState, Observer = NoopObserver> {
    handler: Arc<H>,
    connection_state_factory: Arc<F>,
    runtime: HttpRuntime<Observer>,
}

#[derive(Clone)]
struct HttpRuntime<Observer> {
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    http_limits: NacelleHttpLimits,
    http_policy: NacelleHttpPolicy,
    access_log_enabled: bool,
    listener: Arc<str>,
    peer_rate_limits: Arc<Mutex<HashMap<IpAddr, PeerRateWindow>>>,
}

impl Default for HttpRuntime<NoopObserver> {
    fn default() -> Self {
        Self::new(NacelleTelemetry::default(), NacelleRuntimeState::default())
    }
}

impl<Observer> HttpRuntime<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    fn new(telemetry: NacelleTelemetry<Observer>, runtime_state: NacelleRuntimeState) -> Self {
        Self {
            telemetry,
            runtime_state,
            http_limits: NacelleHttpLimits::default(),
            http_policy: NacelleHttpPolicy::default(),
            access_log_enabled: false,
            listener: Arc::from("direct"),
            peer_rate_limits: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Worker-local HTTP/1 server for explicit thread-per-core execution.
pub struct LocalHyperServer<H, F = NoHttpConnectionState, Observer = NoopObserver> {
    handler: Rc<H>,
    connection_state_factory: Rc<F>,
    runtime: HttpRuntime<Observer>,
}

/// Process-wide HTTP edge state shared by worker-local listeners.
///
/// Clones share per-peer request-rate windows so thread-per-core execution
/// preserves the same process-wide policy semantics as one shared listener.
#[derive(Clone, Default)]
pub struct LocalHttpSharedState {
    peer_rate_limits: Arc<Mutex<HashMap<IpAddr, PeerRateWindow>>>,
}

impl<H> LocalHyperServer<H, NoHttpConnectionState, NoopObserver> {
    /// Construct a worker-local HTTP server without connection state.
    pub fn new(handler: H) -> Self {
        Self {
            handler: Rc::new(handler),
            connection_state_factory: Rc::new(NoHttpConnectionState),
            runtime: HttpRuntime::default(),
        }
    }
}

impl<H, F, Observer> LocalHyperServer<H, F, Observer> {
    /// Replace the worker-local connection state factory.
    pub fn with_connection_state_factory<F2>(self, factory: F2) -> LocalHyperServer<H, F2, Observer>
    where
        F2: LocalHttpConnectionStateFactory,
    {
        LocalHyperServer {
            handler: self.handler,
            connection_state_factory: Rc::new(factory),
            runtime: self.runtime,
        }
    }
}

impl<H, F, Observer> LocalHyperServer<H, F, Observer>
where
    F: LocalHttpConnectionStateFactory,
    H: LocalHttpHandler<F::State> + 'static,
    Observer: NacelleTelemetryObserver,
{
    /// Install final worker telemetry and runtime state atomically.
    pub fn with_runtime_context(
        mut self,
        telemetry: NacelleTelemetry<Observer>,
        runtime_state: NacelleRuntimeState,
        shared_state: LocalHttpSharedState,
    ) -> Self {
        telemetry.register_runtime_state(runtime_state.clone());
        self.runtime.telemetry = telemetry;
        self.runtime.runtime_state = runtime_state;
        self.runtime.peer_rate_limits = shared_state.peer_rate_limits;
        self
    }

    /// Set worker-local HTTP edge limits.
    pub fn with_http_limits(mut self, limits: NacelleHttpLimits) -> Self {
        self.runtime.http_limits = limits;
        self
    }

    /// Set worker-local HTTP edge policy.
    pub fn with_http_policy(mut self, policy: NacelleHttpPolicy) -> Self {
        self.runtime.http_policy = policy;
        self
    }

    /// Enable or disable HTTP access logging.
    pub fn with_access_log(mut self, enabled: bool) -> Self {
        self.runtime.access_log_enabled = enabled;
        self
    }

    /// Set the stable listener label recorded in connection metadata.
    pub fn with_listener_label(mut self, listener: impl Into<Arc<str>>) -> Self {
        self.runtime.listener = listener.into();
        self
    }

    /// Serve one worker-local HTTP listener until shared shutdown.
    pub async fn serve_listener(
        self,
        listener: tokio::net::TcpListener,
        mut shutdown: NacelleShutdownToken,
        drain_deadline: NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        let server = Rc::new(self);
        let local_addr = listener.local_addr().ok();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    log_http_connection_result(joined);
                    continue;
                }
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    let connection_permit = match server
                        .runtime
                        .runtime_state
                        .acquire_connection_for_peer(peer_addr.ip())
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            server.runtime.telemetry.connection_rejected(
                                NacelleTransport::new("http"),
                                connection_rejection_reason(&error),
                            );
                            continue;
                        }
                    };
                    server.runtime.telemetry.connection_opened(NacelleTransport::new("http"));
                    let connection = NacelleConnectionMeta::http_socket(Some(peer_addr), local_addr)
                        .with_listener(server.runtime.listener.clone());
                    let server = server.clone();
                    connections.spawn_local(async move {
                        run_local_http_connection(server, stream, connection, connection_permit).await
                    });
                }
            }
        }
        server.runtime.telemetry.shutdown_event(
            NacelleTelemetryEventKind::ListenerStoppedAccepting,
            NacelleTransport::new("http"),
        );
        drain_http_connection_tasks(
            connections,
            drain_deadline.get(),
            server.runtime.telemetry.clone(),
        )
        .await;
        Ok(())
    }

    /// Serve one worker-local Rustls HTTP listener until shared shutdown.
    #[cfg(feature = "rustls")]
    pub async fn serve_tls_listener(
        self,
        listener: tokio::net::TcpListener,
        tls_config: NacelleTlsConfig,
        mut shutdown: NacelleShutdownToken,
        drain_deadline: NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        let server = Rc::new(self);
        let local_addr = listener.local_addr().ok();
        let handshake_timeout = tls_config.handshake_timeout();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    log_http_connection_result(joined);
                    continue;
                }
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    let connection_permit = match server
                        .runtime
                        .runtime_state
                        .acquire_connection_for_peer(peer_addr.ip())
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            server.runtime.telemetry.connection_rejected(
                                NacelleTransport::new("http"),
                                connection_rejection_reason(&error),
                            );
                            continue;
                        }
                    };
                    server.runtime.telemetry.connection_opened(NacelleTransport::new("http"));
                    let connection = NacelleConnectionMeta::http_socket(Some(peer_addr), local_addr)
                        .with_listener(server.runtime.listener.clone());
                    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
                    let server = server.clone();
                    connections.spawn_local(async move {
                        let stream = match tokio::time::timeout(
                            handshake_timeout,
                            acceptor.accept(stream),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(error)) => return Err(NacelleError::protocol(error)),
                            Err(_) => {
                                server.runtime.telemetry.timeout(
                                    NacelleTransport::new("http"),
                                    "tls_handshake",
                                );
                                return Err(NacelleError::Timeout("tls_handshake"));
                            }
                        };
                        let connection =
                            connection.with_tls(rustls_tls_meta(stream.get_ref().1));
                        run_local_http_connection(server, stream, connection, connection_permit)
                            .await
                    });
                }
            }
        }
        server.runtime.telemetry.shutdown_event(
            NacelleTelemetryEventKind::ListenerStoppedAccepting,
            NacelleTransport::new("http"),
        );
        drain_http_connection_tasks(
            connections,
            drain_deadline.get(),
            server.runtime.telemetry.clone(),
        )
        .await;
        Ok(())
    }
}

impl<H, F, Observer> Clone for HyperServer<H, F, Observer>
where
    Observer: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            connection_state_factory: self.connection_state_factory.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<H> HyperServer<H, NoHttpConnectionState, NoopObserver> {
    pub fn new(handler: H) -> Self {
        let telemetry = NacelleTelemetry::default();
        let runtime_state = NacelleRuntimeState::default();
        telemetry.register_runtime_state(runtime_state.clone());
        Self {
            handler: Arc::new(handler),
            connection_state_factory: Arc::new(NoHttpConnectionState),
            runtime: HttpRuntime::new(telemetry, runtime_state),
        }
    }
}

impl<H, F, Observer> HyperServer<H, F, Observer> {
    pub fn with_connection_state_factory<F2>(self, factory: F2) -> HyperServer<H, F2, Observer>
    where
        F2: HttpConnectionStateFactory,
    {
        HyperServer {
            handler: self.handler,
            connection_state_factory: Arc::new(factory),
            runtime: self.runtime,
        }
    }
}

impl<H, F, Observer> HyperServer<H, F, Observer>
where
    F: HttpConnectionStateFactory,
    H: HttpHandler<F::State>,
    Observer: NacelleTelemetryObserver,
{
    pub fn with_telemetry<Next>(self, telemetry: NacelleTelemetry<Next>) -> HyperServer<H, F, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        telemetry.register_runtime_state(self.runtime.runtime_state.clone());
        HyperServer {
            handler: self.handler,
            connection_state_factory: self.connection_state_factory,
            runtime: HttpRuntime {
                telemetry,
                runtime_state: self.runtime.runtime_state,
                http_limits: self.runtime.http_limits,
                http_policy: self.runtime.http_policy,
                access_log_enabled: self.runtime.access_log_enabled,
                listener: self.runtime.listener,
                peer_rate_limits: self.runtime.peer_rate_limits,
            },
        }
    }

    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime
            .telemetry
            .register_runtime_state(runtime_state.clone());
        self.runtime.runtime_state = runtime_state;
        self
    }

    #[doc(hidden)]
    pub fn with_runtime_context(
        mut self,
        telemetry: NacelleTelemetry<Observer>,
        runtime_state: NacelleRuntimeState,
    ) -> Self {
        telemetry.register_runtime_state(runtime_state.clone());
        self.runtime.telemetry = telemetry;
        self.runtime.runtime_state = runtime_state;
        self
    }

    pub fn with_http_limits(mut self, http_limits: NacelleHttpLimits) -> Self {
        self.runtime.http_limits = http_limits;
        self
    }

    pub fn http_limits(&self) -> &NacelleHttpLimits {
        &self.runtime.http_limits
    }

    pub fn with_http_policy(mut self, http_policy: NacelleHttpPolicy) -> Self {
        self.runtime.http_policy = http_policy;
        self
    }

    pub fn with_access_log(mut self, enabled: bool) -> Self {
        self.runtime.access_log_enabled = enabled;
        self
    }

    pub fn with_listener_label(mut self, listener: impl Into<Arc<str>>) -> Self {
        self.runtime.listener = listener.into();
        self
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<(), NacelleError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        self.serve_listener(listener).await
    }

    pub async fn serve_with_shutdown(
        self,
        addr: SocketAddr,
        shutdown: NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        self.serve_with_shutdown_timeout(addr, shutdown, Duration::from_secs(30))
            .await
    }

    pub async fn serve_with_shutdown_timeout(
        self,
        addr: SocketAddr,
        shutdown: NacelleShutdownToken,
        drain_timeout: Duration,
    ) -> Result<(), NacelleError> {
        self.serve_with_shutdown_deadline(addr, shutdown, NacelleDrainDeadline::new(drain_timeout))
            .await
    }

    #[doc(hidden)]
    pub async fn serve_with_shutdown_deadline(
        self,
        addr: SocketAddr,
        shutdown: NacelleShutdownToken,
        drain_deadline: NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        self.serve_listener_with_shutdown_deadline(listener, shutdown, drain_deadline)
            .await
    }

    pub async fn serve_listener(
        self,
        listener: tokio::net::TcpListener,
    ) -> Result<(), NacelleError> {
        let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        self.serve_listener_with_shutdown(listener, token).await
    }

    pub async fn serve_listener_with_shutdown(
        self,
        listener: tokio::net::TcpListener,
        shutdown: NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        self.serve_listener_with_shutdown_timeout(listener, shutdown, Duration::from_secs(30))
            .await
    }

    pub async fn serve_listener_with_shutdown_timeout(
        self,
        listener: tokio::net::TcpListener,
        shutdown: NacelleShutdownToken,
        drain_timeout: Duration,
    ) -> Result<(), NacelleError> {
        self.serve_listener_with_shutdown_deadline(
            listener,
            shutdown,
            NacelleDrainDeadline::new(drain_timeout),
        )
        .await
    }

    #[doc(hidden)]
    pub async fn serve_listener_with_shutdown_deadline(
        self,
        listener: tokio::net::TcpListener,
        mut shutdown: NacelleShutdownToken,
        drain_deadline: NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        let server = Arc::new(self);
        let mut connections = tokio::task::JoinSet::new();
        let local_addr = listener.local_addr().ok();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    log_http_connection_result(joined);
                    continue;
                }
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    let server = server.clone();
                    let connection_permit = match server
                        .runtime.runtime_state
                        .acquire_connection_for_peer(peer_addr.ip())
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            server
                                .runtime.telemetry
                                .connection_rejected(NacelleTransport::new("http"), connection_rejection_reason(&error));
                            continue;
                        }
                    };
                    server.runtime.telemetry.connection_opened(NacelleTransport::new("http"));
                    let connection = NacelleConnectionMeta::http_socket(
                        Some(peer_addr),
                        local_addr,
                    )
                    .with_listener(server.runtime.listener.clone());
                    connections.spawn(run_http_connection(
                        server,
                        stream,
                        connection,
                        connection_permit,
                    ));
                }
            }
        }
        server.runtime.telemetry.shutdown_event(
            NacelleTelemetryEventKind::ListenerStoppedAccepting,
            NacelleTransport::new("http"),
        );
        drain_http_connection_tasks(
            connections,
            drain_deadline.get(),
            server.runtime.telemetry.clone(),
        )
        .await;
        Ok(())
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tls(
        self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
    ) -> Result<(), NacelleError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        self.serve_tls_listener(listener, tls_config).await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tls_with_shutdown(
        self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
        shutdown: NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        self.serve_tls_with_shutdown_timeout(addr, tls_config, shutdown, Duration::from_secs(30))
            .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tls_with_shutdown_timeout(
        self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
        shutdown: NacelleShutdownToken,
        drain_timeout: Duration,
    ) -> Result<(), NacelleError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        self.serve_tls_listener_with_shutdown_deadline(
            listener,
            tls_config,
            shutdown,
            NacelleDrainDeadline::new(drain_timeout),
        )
        .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tls_listener(
        self,
        listener: tokio::net::TcpListener,
        tls_config: NacelleTlsConfig,
    ) -> Result<(), NacelleError> {
        let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        self.serve_tls_listener_with_shutdown(listener, tls_config, token)
            .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tls_listener_with_shutdown(
        self,
        listener: tokio::net::TcpListener,
        tls_config: NacelleTlsConfig,
        shutdown: NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        self.serve_tls_listener_with_shutdown_deadline(
            listener,
            tls_config,
            shutdown,
            NacelleDrainDeadline::default(),
        )
        .await
    }

    #[cfg(feature = "rustls")]
    #[doc(hidden)]
    pub async fn serve_tls_listener_with_shutdown_deadline(
        self,
        listener: tokio::net::TcpListener,
        tls_config: NacelleTlsConfig,
        mut shutdown: NacelleShutdownToken,
        drain_deadline: NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        let server = Arc::new(self);
        let handshake_timeout = tls_config.handshake_timeout();
        let mut connections = tokio::task::JoinSet::new();
        let local_addr = listener.local_addr().ok();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    log_http_connection_result(joined);
                    continue;
                }
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    let server = server.clone();
                    let connection_permit = match server
                        .runtime.runtime_state
                        .acquire_connection_for_peer(peer_addr.ip())
                    {
                        Ok(permit) => permit,
                        Err(error) => {
                            server
                                .runtime.telemetry
                                .connection_rejected(NacelleTransport::new("http"), connection_rejection_reason(&error));
                            continue;
                        }
                    };
                    server.runtime.telemetry.connection_opened(NacelleTransport::new("http"));
                    let connection = NacelleConnectionMeta::http_socket(
                        Some(peer_addr),
                        local_addr,
                    )
                    .with_listener(server.runtime.listener.clone());
                    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
                    connections.spawn(async move {
                        let tls_stream = match tokio::time::timeout(
                            handshake_timeout,
                            acceptor.accept(stream),
                        )
                        .await
                        {
                            Ok(Ok(stream)) => stream,
                            Ok(Err(error)) => return Err(NacelleError::protocol(error)),
                            Err(_) => {
                                server
                                    .runtime.telemetry
                                    .timeout(NacelleTransport::new("http"), "tls_handshake");
                                return Err(NacelleError::Timeout("tls_handshake"));
                            }
                        };
                        let connection = connection.with_tls(rustls_tls_meta(tls_stream.get_ref().1));
                        run_http_connection(
                            server,
                            tls_stream,
                            connection,
                            connection_permit,
                        )
                        .await
                    });
                }
            }
        }
        server.runtime.telemetry.shutdown_event(
            NacelleTelemetryEventKind::ListenerStoppedAccepting,
            NacelleTransport::new("http"),
        );
        drain_http_connection_tasks(
            connections,
            drain_deadline.get(),
            server.runtime.telemetry.clone(),
        )
        .await;
        Ok(())
    }

    async fn handle(
        &self,
        request: Request<Incoming>,
        connection: &ConnectionContext<Arc<F::State>>,
    ) -> Result<Response<HttpBody>, NacelleError> {
        self.runtime
            .handle_with_dispatch(
                request,
                &SharedHttpDispatch {
                    handler: self.handler.clone(),
                    connection: connection.clone(),
                },
            )
            .await
    }
}

impl<Observer> HttpRuntime<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    async fn handle_with_dispatch<State, D>(
        &self,
        request: Request<Incoming>,
        dispatch: &D,
    ) -> Result<Response<HttpBody>, NacelleError>
    where
        D: HttpDispatch<State, Observer>,
    {
        let request_started = self.request_timing_enabled().then(Instant::now);
        let method = request.method().clone();
        let uri = request.uri().clone();
        let effective_peer_ip =
            self.effective_peer_ip(dispatch.connection_info().peer_ip, &request);
        if let Some(rejection) = validate_http_policy(&self.http_policy, &request) {
            self.telemetry
                .request_rejected(NacelleTransport::new("http"), rejection.reason);
            self.access_log(HttpAccessLog {
                method: &method,
                uri: &uri,
                peer_ip: effective_peer_ip,
                status: rejection.status,
                request_bytes: 0,
                elapsed: elapsed_since(request_started),
                reason: Some(rejection.reason),
            });
            return response_to_http(
                HttpResponse::bytes(rejection.status, rejection.reason),
                self.runtime_state.clone(),
                self.telemetry.clone(),
                &self.http_policy,
            );
        }
        if let Some(peer_ip) = effective_peer_ip
            && !self.allow_peer_request(peer_ip)
        {
            self.telemetry
                .request_rejected(NacelleTransport::new("http"), "peer_rate");
            self.access_log(HttpAccessLog {
                method: &method,
                uri: &uri,
                peer_ip: effective_peer_ip,
                status: StatusCode::TOO_MANY_REQUESTS,
                request_bytes: 0,
                elapsed: elapsed_since(request_started),
                reason: Some("peer_rate"),
            });
            return response_to_http(
                HttpResponse::bytes(StatusCode::TOO_MANY_REQUESTS, "peer_rate"),
                self.runtime_state.clone(),
                self.telemetry.clone(),
                &self.http_policy,
            );
        }
        let _request_permit = match self.runtime_state.acquire_request_tracked() {
            Ok(permit) => permit,
            Err(error) => {
                self.telemetry.request_failed(
                    NacelleTransport::new("http"),
                    elapsed_since(request_started),
                    &error,
                );
                self.access_log(HttpAccessLog {
                    method: &method,
                    uri: &uri,
                    peer_ip: effective_peer_ip,
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    request_bytes: 0,
                    elapsed: elapsed_since(request_started),
                    reason: Some("in_flight_requests"),
                });
                return response_to_http(
                    HttpResponse::bytes(StatusCode::SERVICE_UNAVAILABLE, error.to_string()),
                    self.runtime_state.clone(),
                    self.telemetry.clone(),
                    &self.http_policy,
                );
            }
        };
        let (parts, body) = request.into_parts();
        let body_len_hint = body
            .size_hint()
            .upper()
            .and_then(|bytes| usize::try_from(bytes).ok());
        let request_body_bytes = (body_len_hint != Some(0)).then(|| Arc::new(AtomicUsize::new(0)));
        let request = HttpRequest {
            method: parts.method,
            uri: parts.uri,
            headers: parts.headers,
            peer_ip: effective_peer_ip,
            body: dispatch.incoming_body(
                body,
                body_len_hint,
                request_body_bytes.clone(),
                self.runtime_state.clone(),
                self.http_limits,
                self.telemetry.clone(),
            ),
        };
        let handler_future = dispatch.call(request);
        let handler_result = if let Some(timeout) = self.runtime_state.limits().handler_timeout {
            tokio::time::timeout(timeout, handler_future)
                .await
                .map_err(|_| NacelleError::Timeout("handler"))?
        } else {
            handler_future.await
        };

        match handler_result {
            Ok(completion) => {
                let request_bytes = request_body_bytes
                    .as_ref()
                    .map_or(0, |bytes| bytes.load(Ordering::Relaxed));
                let response = response_to_http(
                    completion.into_inner().response,
                    self.runtime_state.clone(),
                    self.telemetry.clone(),
                    &self.http_policy,
                );
                if let Ok(response) = &response {
                    self.access_log(HttpAccessLog {
                        method: &method,
                        uri: &uri,
                        peer_ip: effective_peer_ip,
                        status: response.status(),
                        request_bytes,
                        elapsed: elapsed_since(request_started),
                        reason: None,
                    });
                }
                self.telemetry.request_completed(
                    NacelleTransport::new("http"),
                    request_bytes,
                    0,
                    elapsed_since(request_started),
                );
                response
            }
            Err(error) => {
                self.telemetry.request_failed(
                    NacelleTransport::new("http"),
                    elapsed_since(request_started),
                    &error,
                );
                let request_bytes = request_body_bytes
                    .as_ref()
                    .map_or(0, |bytes| bytes.load(Ordering::Relaxed));
                let response = response_to_http(
                    HttpResponse::bytes(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
                    self.runtime_state.clone(),
                    self.telemetry.clone(),
                    &self.http_policy,
                );
                if let Ok(response) = &response {
                    self.access_log(HttpAccessLog {
                        method: &method,
                        uri: &uri,
                        peer_ip: effective_peer_ip,
                        status: response.status(),
                        request_bytes,
                        elapsed: elapsed_since(request_started),
                        reason: Some("handler"),
                    });
                }
                self.telemetry.request_completed(
                    NacelleTransport::new("http"),
                    request_bytes,
                    0,
                    elapsed_since(request_started),
                );
                response
            }
        }
    }

    fn request_timing_enabled(&self) -> bool {
        self.access_log_enabled || self.telemetry.request_duration_metrics_enabled()
    }

    fn allow_peer_request(&self, peer_ip: IpAddr) -> bool {
        let Some(limit) = self.http_policy.max_requests_per_peer_per_second else {
            return true;
        };
        allow_peer_request(&self.peer_rate_limits, limit, peer_ip)
    }

    fn effective_peer_ip(
        &self,
        socket_peer_ip: Option<IpAddr>,
        request: &Request<Incoming>,
    ) -> Option<IpAddr> {
        let socket_peer_ip = socket_peer_ip?;
        let Some(trusted_proxy_ips) = &self.http_policy.trusted_proxy_ips else {
            return Some(socket_peer_ip);
        };
        if !trusted_proxy_ips.contains(&socket_peer_ip) {
            return Some(socket_peer_ip);
        }
        Some(forwarded_peer_ip(request).unwrap_or(socket_peer_ip))
    }

    fn access_log(&self, log: HttpAccessLog<'_>) {
        if !self.access_log_enabled {
            return;
        }
        tracing::info!(
            target: "nacelle::access",
            transport = "http",
            method = %log.method,
            uri = %log.uri,
            peer_ip = ?log.peer_ip,
            status = log.status.as_u16(),
            request_bytes = log.request_bytes,
            elapsed_us = log.elapsed.as_micros() as u64,
            reason = log.reason,
            "http access"
        );
    }
}

#[cfg(feature = "rustls")]
fn rustls_tls_meta(connection: &rustls::ServerConnection) -> NacelleConnectionTlsMeta {
    let mut metadata = NacelleConnectionTlsMeta::new("rustls");
    if let Some(protocol) = connection.protocol_version() {
        metadata = metadata.with_protocol(format!("{protocol:?}"));
    }
    if let Some(cipher_suite) = connection.negotiated_cipher_suite() {
        metadata = metadata.with_cipher_suite(format!("{:?}", cipher_suite.suite()));
    }
    if let Some(server_name) = connection.server_name() {
        metadata = metadata.with_server_name(server_name);
    }
    metadata
}

struct HttpAccessLog<'a> {
    method: &'a Method,
    uri: &'a http::Uri,
    peer_ip: Option<IpAddr>,
    status: StatusCode,
    request_bytes: usize,
    elapsed: Duration,
    reason: Option<&'static str>,
}

fn elapsed_since(started: Option<Instant>) -> Duration {
    started.map_or(Duration::ZERO, |started| started.elapsed())
}

fn connection_rejection_reason(error: &NacelleError) -> &'static str {
    match error {
        NacelleError::ResourceLimit(reason) => reason,
        _ => "connections",
    }
}

async fn run_http_connection<H, F, Observer, I>(
    server: Arc<HyperServer<H, F, Observer>>,
    stream: I,
    connection: NacelleConnectionMeta,
    _connection_permit: nacelle_core::limits::TrackedPermit,
) -> Result<(), NacelleError>
where
    F: HttpConnectionStateFactory,
    H: HttpHandler<F::State>,
    Observer: NacelleTelemetryObserver,
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let connection_info = ConnectionInfo::from(&connection);
    let connection_state = Arc::new(server.connection_state_factory.create(&connection_info));
    let connection = ConnectionContext::new(connection_info, connection_state);
    let write_timeout = server.runtime.http_limits.response_write_timeout;
    let io = TimeoutIo::new(TokioIo::new(stream), write_timeout);
    let service_server = server.clone();
    let service = service_fn(move |request| {
        let server = service_server.clone();
        let connection = connection.clone();
        async move { server.handle(request, &connection).await }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(server.runtime.http_limits.header_read_timeout)
        .keep_alive(server.runtime.http_limits.keep_alive);
    let connection = builder.serve_connection(io, service);
    if let Some(max_age) = server.runtime.http_limits.max_connection_age {
        match tokio::time::timeout(max_age, connection).await {
            Ok(result) => result.map_err(NacelleError::protocol),
            Err(_) => {
                server
                    .runtime
                    .telemetry
                    .timeout(NacelleTransport::new("http"), "http_max_connection_age");
                Err(NacelleError::Timeout("http_max_connection_age"))
            }
        }
    } else {
        connection.await.map_err(NacelleError::protocol)
    }
}

async fn run_local_http_connection<H, F, Observer, I>(
    server: Rc<LocalHyperServer<H, F, Observer>>,
    stream: I,
    connection: NacelleConnectionMeta,
    _connection_permit: nacelle_core::limits::TrackedPermit,
) -> Result<(), NacelleError>
where
    F: LocalHttpConnectionStateFactory,
    H: LocalHttpHandler<F::State>,
    Observer: NacelleTelemetryObserver,
    I: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let connection_info = ConnectionInfo::from(&connection);
    let connection_state = Rc::new(server.connection_state_factory.create(&connection_info));
    let dispatch = LocalHttpDispatch {
        handler: server.handler.clone(),
        connection: ConnectionContext::new(connection_info, connection_state),
    };
    let write_timeout = server.runtime.http_limits.response_write_timeout;
    let io = TimeoutIo::new(TokioIo::new(stream), write_timeout);
    let service_server = server.clone();
    let service_dispatch = dispatch.clone();
    let service = service_fn(move |request| {
        let server = service_server.clone();
        let dispatch = service_dispatch.clone();
        async move {
            server
                .runtime
                .handle_with_dispatch(request, &dispatch)
                .await
        }
    });
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(server.runtime.http_limits.header_read_timeout)
        .keep_alive(server.runtime.http_limits.keep_alive);
    let connection = builder.serve_connection(io, service);
    if let Some(max_age) = server.runtime.http_limits.max_connection_age {
        match tokio::time::timeout(max_age, connection).await {
            Ok(result) => result.map_err(NacelleError::protocol),
            Err(_) => {
                server
                    .runtime
                    .telemetry
                    .timeout(NacelleTransport::new("http"), "http_max_connection_age");
                Err(NacelleError::Timeout("http_max_connection_age"))
            }
        }
    } else {
        connection.await.map_err(NacelleError::protocol)
    }
}

struct TimeoutIo<I> {
    inner: I,
    write_timeout: Option<Duration>,
    write_sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<I> TimeoutIo<I> {
    fn new(inner: I, write_timeout: Option<Duration>) -> Self {
        Self {
            inner,
            write_timeout,
            write_sleep: None,
        }
    }

    fn poll_write_deadline(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let Some(timeout) = self.write_timeout else {
            return Poll::Pending;
        };
        let sleep = self
            .write_sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep(timeout)));
        match sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.write_sleep = None;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "http response write timed out",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I> hyper::rt::Read for TimeoutIo<I>
where
    I: hyper::rt::Read + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<I> hyper::rt::Write for TimeoutIo<I>
where
    I: hyper::rt::Write + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.write_sleep.is_some()
            && let Poll::Ready(result) = self.poll_write_deadline(cx)
        {
            return Poll::Ready(result.map(|()| 0));
        }
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(result) => {
                self.write_sleep = None;
                Poll::Ready(result)
            }
            Poll::Pending => match self.poll_write_deadline(cx) {
                Poll::Ready(result) => Poll::Ready(result.map(|()| 0)),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        if self.write_sleep.is_some()
            && let Poll::Ready(result) = self.poll_write_deadline(cx)
        {
            return Poll::Ready(result.map(|()| 0));
        }
        match Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(result) => {
                self.write_sleep = None;
                Poll::Ready(result)
            }
            Poll::Pending => match self.poll_write_deadline(cx) {
                Poll::Ready(result) => Poll::Ready(result.map(|()| 0)),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

fn log_http_connection_result(
    result: Option<Result<Result<(), NacelleError>, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(Ok(()))) | None => {}
        Some(Ok(Err(error))) => {
            tracing::debug!(target: "nacelle", transport = "http", error = %error, "connection finished with error");
        }
        Some(Err(error)) => {
            tracing::warn!(target: "nacelle", transport = "http", error = %error, "connection task failed");
        }
    }
}

async fn drain_http_connection_tasks<Observer>(
    mut connections: tokio::task::JoinSet<Result<(), NacelleError>>,
    drain_timeout: Duration,
    telemetry: NacelleTelemetry<Observer>,
) where
    Observer: NacelleTelemetryObserver,
{
    telemetry.shutdown_event(
        NacelleTelemetryEventKind::DrainStarted,
        NacelleTransport::new("http"),
    );
    let drain = async {
        while let Some(result) = connections.join_next().await {
            log_http_connection_result(Some(result));
        }
    };

    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        tracing::info!(target: "nacelle", transport = "http", "connection drain completed");
        telemetry.shutdown_event(
            NacelleTelemetryEventKind::DrainCompleted,
            NacelleTransport::new("http"),
        );
        return;
    }

    let aborted = connections.len();
    tracing::warn!(target: "nacelle", transport = "http", aborted, "connection drain timed out; aborting active tasks");
    telemetry.shutdown_event(
        NacelleTelemetryEventKind::DrainTimedOut,
        NacelleTransport::new("http"),
    );
    telemetry.connections_aborted(NacelleTransport::new("http"), aborted);
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        log_http_connection_result(Some(result));
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use nacelle_core::pipeline::handler_fn;
    use nacelle_core::request::NacelleBody;

    use super::*;

    #[cfg(feature = "tls-self-signed")]
    #[tokio::test(flavor = "current_thread")]
    async fn local_https_self_signed_server_accepts_request() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("listener");
                let addr = listener.local_addr().expect("address");
                let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("TLS config");
                let certificate =
                    nacelle_core::tls::parse_pem_certificates(generated.certificate_pem.as_bytes())
                        .expect("certificate")
                        .remove(0);
                let mut roots = rustls::RootCertStore::empty();
                roots.add(certificate).expect("root");
                let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(
                    rustls::ClientConfig::builder()
                        .with_root_certificates(roots)
                        .with_no_client_auth(),
                ));
                let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
                let handler = nacelle_core::pipeline::local_handler_fn(
                    |context: crate::LocalHttpRequestContext<()>| async move {
                        context
                            .respond(HttpResponse::bytes(StatusCode::OK, "local tls"))
                            .await
                    },
                );
                let task =
                    tokio::task::spawn_local(LocalHyperServer::new(handler).serve_tls_listener(
                        listener,
                        generated.tls_config,
                        token,
                        NacelleDrainDeadline::default(),
                    ));
                let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
                let name = rustls::pki_types::ServerName::try_from("localhost").expect("name");
                let mut client = connector.connect(name, stream).await.expect("handshake");
                client
                    .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .await
                    .expect("write");
                let mut response = Vec::new();
                client.read_to_end(&mut response).await.expect("read");
                let response = String::from_utf8(response).expect("utf8");
                assert!(response.starts_with("HTTP/1.1 200 OK"));
                assert!(response.contains("local tls"));
                shutdown.shutdown();
                task.await.expect("join").expect("server");
            })
            .await;
    }

    #[test]
    fn local_http_workers_share_peer_rate_limit_state() {
        let shared_state = LocalHttpSharedState::default();
        let policy = NacelleHttpPolicy::new().with_max_requests_per_peer_per_second(1);
        let first = LocalHyperServer::new(nacelle_core::pipeline::local_handler_fn(
            |_context: crate::LocalHttpRequestContext<()>| async move {
                Err(NacelleError::ResourceLimit("unused_test_handler"))
            },
        ))
        .with_http_policy(policy.clone())
        .with_runtime_context(
            NacelleTelemetry::default(),
            NacelleRuntimeState::default(),
            shared_state.clone(),
        );
        let second = LocalHyperServer::new(nacelle_core::pipeline::local_handler_fn(
            |_context: crate::LocalHttpRequestContext<()>| async move {
                Err(NacelleError::ResourceLimit("unused_test_handler"))
            },
        ))
        .with_http_policy(policy)
        .with_runtime_context(
            NacelleTelemetry::default(),
            NacelleRuntimeState::default(),
            shared_state,
        );
        let peer = "127.0.0.1".parse().expect("valid peer ip");

        assert!(first.runtime.allow_peer_request(peer));
        assert!(!second.runtime.allow_peer_request(peer));
    }

    struct CountingConnectionStateFactory {
        creations: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
    }

    struct CountingConnectionState {
        requests: Arc<AtomicUsize>,
    }

    impl HttpConnectionStateFactory for CountingConnectionStateFactory {
        type State = CountingConnectionState;

        fn create(&self, _connection: &ConnectionInfo) -> Self::State {
            self.creations.fetch_add(1, Ordering::SeqCst);
            CountingConnectionState {
                requests: self.requests.clone(),
            }
        }
    }

    #[tokio::test]
    async fn http_keep_alive_reuses_connection_identity_and_metadata() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let observed = Arc::new(Mutex::new(Vec::new()));
        let server = HyperServer::new(handler_fn({
            let observed = observed.clone();
            move |context: HttpRequestContext<()>| {
                let observed = observed.clone();
                async move {
                    observed
                        .lock()
                        .expect("observed metadata lock poisoned")
                        .push(context.connection().info.clone());
                    context
                        .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                        .await
                }
            }
        }))
        .with_listener_label("keep-alive-test");
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n\
                  GET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("requests should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("responses should read");
        assert_eq!(
            String::from_utf8_lossy(&response)
                .matches("HTTP/1.1 200 OK")
                .count(),
            2
        );

        let observed = observed.lock().expect("observed metadata lock poisoned");
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[0].connection_id, observed[1].connection_id);
        assert_eq!(observed[0].listener.as_ref(), "keep-alive-test");
        assert!(observed[0].peer_addr.is_some());
        assert_eq!(observed[0].local_addr, Some(addr));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_connection_state_is_created_once_and_reused_for_keep_alive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let creations = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let server = HyperServer::new(handler_fn(
            |context: HttpRequestContext<CountingConnectionState>| async move {
                context
                    .connection()
                    .state
                    .requests
                    .fetch_add(1, Ordering::SeqCst);
                context.respond(HttpResponse::empty(StatusCode::OK)).await
            },
        ))
        .with_connection_state_factory(CountingConnectionStateFactory {
            creations: creations.clone(),
            requests: requests.clone(),
        });
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n\
                  GET /two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("requests should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("responses should read");

        assert_eq!(creations.load(Ordering::SeqCst), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        server_task.abort();
    }

    #[tokio::test]
    async fn http_server_streams_request_and_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let server = HyperServer::new(handler_fn(
            |mut context: HttpRequestContext<()>| async move {
                assert_eq!(context.request().uri.path(), "/echo");
                let (tx, body) = NacelleBody::channel(2);
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    tx.send(chunk)
                        .await
                        .expect("response receiver should be open");
                }
                drop(tx);
                context
                    .respond(HttpResponse::new(
                        StatusCode::CREATED,
                        http::HeaderMap::new(),
                        body,
                    ))
                    .await
            },
        ));
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"POST /echo HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Content-Length: 11\r\n\
                  Connection: close\r\n\
                  \r\n\
                  hello world",
            )
            .await
            .expect("request should write");

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");
        let response = String::from_utf8(response).expect("response should be utf8");

        assert!(response.starts_with("HTTP/1.1 201 Created"));
        assert!(response.contains("hello world"));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_response_body_bytes_are_recorded() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let sink = Arc::new(nacelle_core::NacelleInMemoryTelemetrySink::new());
        let telemetry = NacelleTelemetry::new().with_sink(sink.clone());
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "hello bytes"))
                .await
        }))
        .with_telemetry(telemetry);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("request should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");

        assert!(sink.events().iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::ResponseBodyBytes
                && event.transport == Some(NacelleTransport::new("http"))
                && event.count == "hello bytes".len() as u64
        }));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_server_stops_accepting_when_shutdown_is_requested() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        }));
        let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        let server_task =
            tokio::spawn(async move { server.serve_listener_with_shutdown(listener, token).await });

        shutdown.shutdown();

        tokio::time::timeout(std::time::Duration::from_secs(1), server_task)
            .await
            .expect("server should stop promptly")
            .expect("server task should join")
            .expect("server should exit cleanly");
    }

    #[tokio::test]
    async fn http_shutdown_aborts_after_drain_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let runtime_state = nacelle_core::limits::NacelleRuntimeState::default();
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        }))
        .with_runtime_state(runtime_state.clone());
        let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        let server_task = tokio::spawn(async move {
            server
                .serve_listener_with_shutdown_timeout(
                    listener,
                    token,
                    std::time::Duration::from_millis(10),
                )
                .await
        });

        let _client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        wait_for_active_connections(&runtime_state, 1).await;

        shutdown.shutdown();

        tokio::time::timeout(std::time::Duration::from_secs(1), server_task)
            .await
            .expect("server should stop before test timeout")
            .expect("server task should join")
            .expect("server should exit cleanly");
        assert_eq!(runtime_state.active_connections(), 0);
    }

    #[tokio::test]
    async fn http_slow_header_client_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let runtime_state = nacelle_core::limits::NacelleRuntimeState::default();
        let http_limits = NacelleHttpLimits::default()
            .with_header_read_timeout(std::time::Duration::from_millis(25));
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        }))
        .with_runtime_state(runtime_state.clone())
        .with_http_limits(http_limits);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
            .await
            .expect("partial request should write");

        let mut buf = [0_u8; 1];
        let read = tokio::time::timeout(std::time::Duration::from_secs(1), client.read(&mut buf))
            .await
            .expect("connection should close before test timeout");
        assert!(read.is_err() || read.expect("read result should be available") == 0);
        wait_for_active_connections(&runtime_state, 0).await;
        server_task.abort();
    }

    #[tokio::test]
    async fn http_content_length_body_reserves_memory_until_consumed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let runtime_state = nacelle_core::limits::NacelleRuntimeState::new(
            nacelle_core::limits::NacelleLimits::default().with_max_memory_bytes(1024 * 1024),
        );
        let server = HyperServer::new(handler_fn(
            move |mut context: HttpRequestContext<()>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    let _ = chunk?;
                }
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                    .await
            },
        ))
        .with_runtime_state(runtime_state.clone());
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"POST / HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Content-Length: 11\r\n\
                  Connection: close\r\n\
                  \r\n",
            )
            .await
            .expect("headers should write");
        wait_for_memory(&runtime_state, 11).await;
        client
            .write_all(b"hello world")
            .await
            .expect("body should write");

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");

        wait_for_memory(&runtime_state, 0).await;
        server_task.abort();
    }

    #[tokio::test]
    async fn http_trickle_request_body_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let http_limits = NacelleHttpLimits::default()
            .with_request_body_read_timeout(std::time::Duration::from_millis(20));
        let server = HyperServer::new(handler_fn(
            |mut context: HttpRequestContext<()>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    let _ = chunk?;
                }
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                    .await
            },
        ))
        .with_http_limits(http_limits);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"POST / HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Content-Length: 11\r\n\
                  Connection: close\r\n\
                  \r\n",
            )
            .await
            .expect("headers should write");

        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.read_to_end(&mut response),
        )
        .await
        .expect("server should respond before test timeout")
        .expect("response should read");
        let response = String::from_utf8(response).expect("response should be utf8");
        assert!(response.starts_with("HTTP/1.1 500 Internal Server Error"));
        assert!(response.contains("http_body_read"));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_slow_response_reader_times_out_or_drains() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let http_limits = NacelleHttpLimits::default()
            .with_response_write_timeout(std::time::Duration::from_millis(20));
        let runtime_state = nacelle_core::limits::NacelleRuntimeState::default();
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            let (tx, body) = NacelleBody::channel(1);
            tokio::spawn(async move {
                let chunk = Bytes::from(vec![0x5A; 16 * 1024]);
                for _ in 0..512 {
                    if tx.send(Ok(chunk.clone())).await.is_err() {
                        break;
                    }
                }
            });
            context
                .respond(HttpResponse::new(
                    StatusCode::OK,
                    http::HeaderMap::new(),
                    body,
                ))
                .await
        }))
        .with_runtime_state(runtime_state.clone())
        .with_http_limits(http_limits);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("request should write");
        let mut headers = [0_u8; 128];
        let _ = client
            .read(&mut headers)
            .await
            .expect("response headers should start");

        wait_for_active_connections(&runtime_state, 0).await;
        drop(client);
        server_task.abort();
    }

    #[tokio::test]
    async fn http_handler_error_becomes_500_response() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let server = HyperServer::new(handler_fn(|_context: HttpRequestContext<()>| async move {
            Err(NacelleError::handler(std::io::Error::other("boom")))
        }));
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(
                b"GET / HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Connection: close\r\n\
                  \r\n",
            )
            .await
            .expect("request should write");

        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");
        let response = String::from_utf8(response).expect("response should be utf8");

        assert!(response.starts_with("HTTP/1.1 500 Internal Server Error"));
        assert!(response.contains("handler error: boom"));
        server_task.abort();
    }

    #[cfg(feature = "tls-self-signed")]
    #[tokio::test]
    async fn http_tls_self_signed_server_accepts_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let generated = nacelle_core::tls::NacelleTlsConfig::self_signed(["localhost"])
            .expect("self-signed tls");
        let certificate =
            nacelle_core::tls::parse_pem_certificates(generated.certificate_pem.as_bytes())
                .expect("certificate should parse")
                .remove(0);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).expect("root cert should add");
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client_config));
        let observed = Arc::new(Mutex::new(None));
        let server = HyperServer::new(handler_fn({
            let observed = observed.clone();
            move |context: HttpRequestContext<()>| {
                let observed = observed.clone();
                async move {
                    *observed.lock().expect("observed metadata lock poisoned") =
                        Some(context.connection().info.clone());
                    context
                        .respond(HttpResponse::bytes(StatusCode::OK, "tls ok"))
                        .await
                }
            }
        }));
        let server_task = tokio::spawn(async move {
            server
                .serve_tls_listener(listener, generated.tls_config)
                .await
        });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");
        let mut client = connector
            .connect(server_name, stream)
            .await
            .expect("tls should connect");
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("request should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");
        let response = String::from_utf8(response).expect("response should be utf8");

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("tls ok"));
        let observed = observed.lock().expect("observed metadata lock poisoned");
        let tls = observed
            .as_ref()
            .and_then(|connection| connection.tls.as_ref())
            .expect("TLS metadata should reach the handler context");
        assert_eq!(tls.provider, "rustls");
        assert!(tls.protocol.is_some());
        assert!(tls.cipher_suite.is_some());
        assert_eq!(tls.server_name.as_deref(), Some("localhost"));
        server_task.abort();
    }

    #[cfg(feature = "tls-self-signed")]
    #[tokio::test]
    async fn http_tls_sni_allowlist_rejects_disallowed_name() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let generated =
            nacelle_core::tls::NacelleTlsConfig::self_signed(["localhost", "wrong.local"])
                .expect("self-signed tls");
        let tls_config = nacelle_core::tls::NacelleTlsConfig::from_pem_with_allowed_server_names(
            generated.certificate_pem.as_bytes(),
            generated.private_key_pem.as_bytes(),
            ["localhost"],
        )
        .expect("SNI allowlist config should build");
        let certificate =
            nacelle_core::tls::parse_pem_certificates(generated.certificate_pem.as_bytes())
                .expect("certificate should parse")
                .remove(0);
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).expect("root cert should add");
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client_config));
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "tls ok"))
                .await
        }));
        let server_task =
            tokio::spawn(async move { server.serve_tls_listener(listener, tls_config).await });

        let stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        let server_name =
            rustls::pki_types::ServerName::try_from("wrong.local").expect("valid server name");
        let result = connector.connect(server_name, stream).await;

        assert!(result.is_err());
        server_task.abort();
    }

    #[tokio::test]
    async fn http_policy_rejects_disallowed_host_before_handler() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_for_handler = called.clone();
        let (response, events) = http_policy_response(
            NacelleHttpPolicy::new().with_allowed_hosts(["example.com"]),
            handler_fn(move |context: HttpRequestContext<()>| {
                let called_for_handler = called_for_handler.clone();
                async move {
                    called_for_handler.store(true, std::sync::atomic::Ordering::SeqCst);
                    context
                        .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                        .await
                }
            }),
            b"GET / HTTP/1.1\r\nHost: wrong.example\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 421 Misdirected Request"));
        assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(events.iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && event.reason == Some("host")
        }));
    }

    #[tokio::test]
    async fn http_policy_rejects_disallowed_method_before_handler() {
        let (response, events) = http_policy_response(
            NacelleHttpPolicy::new().with_allowed_methods([Method::GET]),
            handler_fn(|context: HttpRequestContext<()>| async move {
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                    .await
            }),
            b"POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 405 Method Not Allowed"));
        assert!(events.iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && event.reason == Some("method_not_allowed")
        }));
    }

    #[tokio::test]
    async fn http_policy_rejects_long_uri_before_handler() {
        let (response, events) = http_policy_response(
            NacelleHttpPolicy::new().with_max_uri_len(4),
            handler_fn(|context: HttpRequestContext<()>| async move {
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                    .await
            }),
            b"GET /too-long HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 414 URI Too Long"));
        assert!(events.iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && event.reason == Some("uri_too_long")
        }));
    }

    #[tokio::test]
    async fn http_policy_rejects_header_count_and_bytes_before_handler() {
        let (response, events) = http_policy_response(
            NacelleHttpPolicy::new()
                .with_max_header_count(1)
                .with_max_header_bytes(16),
            handler_fn(|context: HttpRequestContext<()>| async move {
                context.respond(HttpResponse::bytes(StatusCode::OK, "handler")).await
            }),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Long: abcdefghijklmnop\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(response.starts_with("HTTP/1.1 431 Request Header Fields Too Large"));
        assert!(events.iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && matches!(event.reason, Some("header_count" | "header_bytes"))
        }));
    }

    #[tokio::test]
    async fn http_policy_adds_security_headers_to_normal_and_rejected_responses() {
        let policy = NacelleHttpPolicy::new()
            .with_default_security_headers()
            .with_allowed_hosts(["example.com"]);

        let (normal_response, _events) = http_policy_response(
            policy.clone(),
            handler_fn(|context: HttpRequestContext<()>| async move {
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                    .await
            }),
            b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(normal_response.starts_with("HTTP/1.1 200 OK"));
        assert!(normal_response.contains("x-content-type-options: nosniff"));
        assert!(normal_response.contains("x-frame-options: deny"));

        let (rejected_response, _events) = http_policy_response(
            policy,
            handler_fn(|context: HttpRequestContext<()>| async move {
                context
                    .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                    .await
            }),
            b"GET / HTTP/1.1\r\nHost: wrong.example\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(rejected_response.starts_with("HTTP/1.1 421 Misdirected Request"));
        assert!(rejected_response.contains("x-content-type-options: nosniff"));
        assert!(rejected_response.contains("x-frame-options: deny"));
    }

    #[tokio::test]
    async fn http_policy_rate_limits_requests_per_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let sink = Arc::new(nacelle_core::NacelleInMemoryTelemetrySink::new());
        let telemetry = NacelleTelemetry::new().with_sink(sink.clone());
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                .await
        }))
        .with_telemetry(telemetry)
        .with_http_policy(NacelleHttpPolicy::new().with_max_requests_per_peer_per_second(1));
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let first = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        let second = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(first.starts_with("HTTP/1.1 200 OK"));
        assert!(second.starts_with("HTTP/1.1 429 Too Many Requests"));
        assert!(sink.events().iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && event.reason == Some("peer_rate")
        }));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_policy_ignores_forwarded_peer_by_default() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "handler"))
                .await
        }))
        .with_http_policy(NacelleHttpPolicy::new().with_max_requests_per_peer_per_second(1));
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let first = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 203.0.113.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        let second = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 203.0.113.2\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(first.starts_with("HTTP/1.1 200 OK"));
        assert!(second.starts_with("HTTP/1.1 429 Too Many Requests"));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_policy_uses_forwarded_peer_only_from_trusted_proxy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            let peer_ip = context.request().peer_ip.expect("effective peer ip");
            context
                .respond(HttpResponse::bytes(StatusCode::OK, peer_ip.to_string()))
                .await
        }))
        .with_http_policy(
            NacelleHttpPolicy::new()
                .with_max_requests_per_peer_per_second(1)
                .with_trusted_proxy_ips(["127.0.0.1".parse().expect("trusted proxy ip")]),
        );
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let first = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nForwarded: for=203.0.113.1\r\nConnection: close\r\n\r\n",
        )
        .await;
        let second = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 203.0.113.2\r\nConnection: close\r\n\r\n",
        )
        .await;
        let third = one_shot_http(
            addr,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 203.0.113.1\r\nConnection: close\r\n\r\n",
        )
        .await;

        assert!(first.starts_with("HTTP/1.1 200 OK"));
        assert!(first.contains("203.0.113.1"));
        assert!(second.starts_with("HTTP/1.1 200 OK"));
        assert!(second.contains("203.0.113.2"));
        assert!(third.starts_with("HTTP/1.1 429 Too Many Requests"));
        server_task.abort();
    }

    #[tokio::test]
    async fn http_per_peer_connection_limit_rejects_second_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let runtime_state = nacelle_core::limits::NacelleRuntimeState::new(
            nacelle_core::limits::NacelleLimits::default().with_max_connections_per_peer(1),
        );
        let sink = Arc::new(nacelle_core::NacelleInMemoryTelemetrySink::new());
        let telemetry = NacelleTelemetry::new().with_sink(sink.clone());
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        }))
        .with_runtime_state(runtime_state.clone())
        .with_telemetry(telemetry);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let held = tokio::net::TcpStream::connect(addr)
            .await
            .expect("held client should connect");
        wait_for_active_connections(&runtime_state, 1).await;

        let mut rejected = tokio::net::TcpStream::connect(addr)
            .await
            .expect("second client should connect before rejection");
        let mut buf = [0_u8; 1];
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rejected.read(&mut buf))
            .await
            .expect("rejected connection should close promptly");

        assert_eq!(runtime_state.active_connections(), 1);
        assert!(sink.events().iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::ConnectionRejected
                && event.reason == Some("peer_connections")
        }));

        drop(rejected);
        drop(held);
        wait_for_active_connections(&runtime_state, 0).await;
        server_task.abort();
    }

    async fn wait_for_active_connections(
        runtime_state: &nacelle_core::limits::NacelleRuntimeState,
        expected: usize,
    ) {
        for _ in 0..100 {
            if runtime_state.active_connections() == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!(
            "active connections did not reach {expected}; observed {}",
            runtime_state.active_connections()
        );
    }

    async fn wait_for_memory(
        runtime_state: &nacelle_core::limits::NacelleRuntimeState,
        expected: usize,
    ) {
        for _ in 0..100 {
            if runtime_state.memory_used_bytes() == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!(
            "memory did not reach {expected}; observed {}",
            runtime_state.memory_used_bytes()
        );
    }

    async fn http_policy_response<H>(
        policy: NacelleHttpPolicy,
        handler: H,
        request: &[u8],
    ) -> (String, Vec<nacelle_core::NacelleTelemetryEvent>)
    where
        H: HttpHandler<()>,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have addr");
        let sink = Arc::new(nacelle_core::NacelleInMemoryTelemetrySink::new());
        let telemetry = NacelleTelemetry::new().with_sink(sink.clone());
        let server = HyperServer::new(handler)
            .with_telemetry(telemetry)
            .with_http_policy(policy);
        let server_task = tokio::spawn(async move { server.serve_listener(listener).await });

        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(request)
            .await
            .expect("request should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");
        server_task.abort();

        (
            String::from_utf8(response).expect("response should be utf8"),
            sink.events(),
        )
    }

    async fn one_shot_http(addr: SocketAddr, request: &[u8]) -> String {
        let mut client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");
        client
            .write_all(request)
            .await
            .expect("request should write");
        let mut response = Vec::new();
        client
            .read_to_end(&mut response)
            .await
            .expect("response should read");
        String::from_utf8(response).expect("response should be utf8")
    }
}
