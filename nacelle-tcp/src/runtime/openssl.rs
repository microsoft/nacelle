use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openssl::ssl::Ssl;

use crate::options::{NacelleTcpBindOptions, NacelleTcpOptions};
use crate::protocol::{Protocol, SharedProtocol};
use crate::serial_server::SerialTcpServer;
use crate::server::TcpServer;
use nacelle_core::error::{NacelleError, NacelleTimeoutReason};
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleMetricsContext, NacelleTelemetryEventKind, NacelleTelemetryObserver, NacelleTransport,
};
use nacelle_openssl::NacelleOpenSslConfig;

use super::common::{
    bind_tcp_listener, connection_rejection_reason, drain_connection_tasks, log_connection_result,
    reap_finished_connections, run_accept_loop,
};

/// Listen on `addr` and serve framed TCP connections over OpenSSL.
pub async fn serve_tcp_openssl<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_openssl_with_shutdown(server, addr, tls_config, token).await
}

/// Serve OpenSSL TCP connections until shutdown is requested.
pub async fn serve_tcp_openssl_with_shutdown<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_shutdown_timeout(
        server,
        addr,
        tls_config,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Serve OpenSSL TCP connections, then drain for at most `drain_timeout`.
pub async fn serve_tcp_openssl_with_shutdown_timeout<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_shutdown_deadline(
        server,
        addr,
        tls_config,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

/// Serve OpenSSL TCP connections with explicit socket options.
pub async fn serve_tcp_openssl_with_options<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_openssl_with_options_and_shutdown(server, addr, tls_config, tcp_options, token).await
}

/// Serve OpenSSL TCP connections with socket options until shutdown.
pub async fn serve_tcp_openssl_with_options_and_shutdown<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_options_and_shutdown_timeout(
        server,
        addr,
        tls_config,
        tcp_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Serve OpenSSL TCP connections with socket options and a bounded drain.
pub async fn serve_tcp_openssl_with_options_and_shutdown_timeout<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        tcp_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_openssl_with_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_openssl_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::from(tcp_options),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_openssl_with_bind_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    bind_options: NacelleTcpBindOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_tcp_openssl_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tls_config,
        bind_options.stream,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_openssl_listener_with_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_openssl_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tls_config,
        NacelleTcpOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_openssl_listener_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let handshake_timeout = tls_config.handshake_timeout();
    run_accept_loop(
        server,
        listener,
        "openssl",
        shutdown,
        drain_deadline,
        move |stream| {
            tcp_options
                .apply_to_stream(stream)
                .map_err(NacelleError::from)
        },
        move |server, stream, connection, activity| {
            let acceptor = tls_config.acceptor();
            async move {
                let ssl = Ssl::new(acceptor.context()).map_err(NacelleError::protocol)?;
                let mut stream =
                    tokio_openssl::SslStream::new(ssl, stream).map_err(NacelleError::protocol)?;
                match tokio::time::timeout(
                    handshake_timeout,
                    std::pin::Pin::new(&mut stream).accept(),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(NacelleError::protocol(error)),
                    Err(_) => {
                        server
                            .telemetry()
                            .timeout(NacelleTransport::new("tcp"), "tls_handshake");
                        return Err(NacelleError::Timeout(NacelleTimeoutReason::TlsHandshake));
                    }
                }
                let connection =
                    connection.with_tls(nacelle_openssl::connection_tls_meta(stream.ssl()));
                server.serve_io_admitted(stream, connection, activity).await
            }
        },
    )
    .await
}

/// Listen on `addr` and serve serial mutable-state connections over OpenSSL.
pub async fn serve_serial_tcp_openssl<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_serial_tcp_openssl_with_shutdown(server, addr, tls_config, token).await
}

/// Listen on `addr` for serial OpenSSL connections until shutdown is requested.
pub async fn serve_serial_tcp_openssl_with_shutdown<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::default(),
        shutdown,
        NacelleDrainDeadline::new(Duration::from_secs(30)),
    )
    .await
}

/// Listen for serial OpenSSL connections with a bounded shutdown drain.
pub async fn serve_serial_tcp_openssl_with_shutdown_timeout<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_shutdown_deadline(
        server,
        addr,
        tls_config,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

/// Listen for serial OpenSSL connections with explicit TCP options.
pub async fn serve_serial_tcp_openssl_with_options<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_serial_tcp_openssl_with_options_and_shutdown(server, addr, tls_config, tcp_options, token)
        .await
}

/// Listen for serial OpenSSL connections with TCP options until shutdown.
pub async fn serve_serial_tcp_openssl_with_options_and_shutdown<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_options_and_shutdown_timeout(
        server,
        addr,
        tls_config,
        tcp_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Listen for serial OpenSSL connections with options and bounded drain.
pub async fn serve_serial_tcp_openssl_with_options_and_shutdown_timeout<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        tcp_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_openssl_with_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_openssl_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::from(tcp_options),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_openssl_with_bind_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    bind_options: NacelleTcpBindOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_serial_tcp_openssl_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tls_config,
        bind_options.stream,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_openssl_listener_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let handshake_timeout = tls_config.handshake_timeout();
    let local_addr = listener.local_addr().ok();
    let mut connections = tokio::task::JoinSet::new();
    nacelle_core::runtime::report_runtime_topology(
        "tcp",
        server.telemetry().runtime_metrics_enabled(),
    );

    loop {
        // Reap finished connection tasks without competing with `accept()`.
        reap_finished_connections(&mut connections, transport);
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr)
                    .with_listener(server.listener_label());
                let registration = match server
                    .runtime_state()
                    .register_connection(&connection)
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        let context = NacelleMetricsContext::new(
                            transport,
                            server.listener_label(),
                            server.protocol().name(),
                            "openssl",
                        );
                        server.telemetry().operation_error(&context, "accept", &error);
                        server.telemetry().connection_rejected(
                            transport,
                            connection_rejection_reason(&error),
                        );
                        continue;
                    }
                };
                let owned = (stream, registration);
                tcp_options.apply_to_stream(&owned.0)?;
                let (stream, registration) = owned;
                let activity = registration.activity();
                let server = server.clone();
                let acceptor = tls_config.acceptor();
                connections.spawn(registration.run(async move {
                    let ssl = Ssl::new(acceptor.context()).map_err(NacelleError::protocol)?;
                    let mut stream = tokio_openssl::SslStream::new(ssl, stream)
                        .map_err(NacelleError::protocol)?;
                    match tokio::time::timeout(
                        handshake_timeout,
                        std::pin::Pin::new(&mut stream).accept(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => return Err(NacelleError::protocol(error)),
                        Err(_) => {
                            server.telemetry().timeout(transport, "tls_handshake");
                            return Err(NacelleError::Timeout(NacelleTimeoutReason::TlsHandshake));
                        }
                    }
                    let connection =
                        connection.with_tls(nacelle_openssl::connection_tls_meta(stream.ssl()));
                    server
                        .serve_io_admitted(stream, connection, activity)
                        .await
                }));
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined, transport);
            }
        }
    }

    server.telemetry().shutdown_event(
        NacelleTelemetryEventKind::ListenerStoppedAccepting,
        transport,
    );
    drain_connection_tasks(
        connections,
        drain_deadline.get(),
        transport,
        server.telemetry().clone(),
    )
    .await;
    Ok(())
}

#[doc(hidden)]
pub async fn serve_serial_tcp_openssl_listener_with_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_openssl_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tls_config,
        NacelleTcpOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}
