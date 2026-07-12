use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::options::{NacelleTcpBindOptions, NacelleTcpOptions};
use crate::protocol::{
    Protocol, SerialTcpHandler, SerialTcpOneWayHandler, SharedProtocol, TcpHandler,
    TcpOneWayHandler,
};
use crate::serial_server::SerialTcpServer;
use crate::server::TcpServer;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleMetricsContext, NacelleTelemetryEventKind, NacelleTelemetryObserver, NacelleTransport,
};

use super::common::{
    bind_tcp_listener, connection_rejection_reason, drain_connection_tasks, log_connection_result,
    run_accept_loop,
};

/// Listen on `addr` and serve each accepted TCP connection in its own task.
pub async fn serve_tcp<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_with_shutdown(server, addr, token).await
}

/// Listen on `addr` until shutdown is requested.
pub async fn serve_tcp_with_shutdown<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_with_shutdown_timeout(server, addr, shutdown, Duration::from_secs(30)).await
}

/// Listen on `addr` until shutdown is requested, then drain or abort active
/// connection tasks after `drain_timeout`.
pub async fn serve_tcp_with_shutdown_timeout<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_with_shutdown_deadline(
        server,
        addr,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

/// Listen on `addr` with explicit TCP socket options.
pub async fn serve_tcp_with_options<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_with_options_and_shutdown(server, addr, tcp_options, token).await
}

/// Listen on `addr` with explicit TCP socket options until shutdown is requested.
pub async fn serve_tcp_with_options_and_shutdown<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_with_options_and_shutdown_timeout(
        server,
        addr,
        tcp_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Listen on `addr` with explicit TCP socket options, then drain or abort active
/// connection tasks after `drain_timeout`.
pub async fn serve_tcp_with_options_and_shutdown_timeout<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_with_options_and_shutdown_deadline(
        server,
        addr,
        tcp_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_with_bind_options_and_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    bind_options: NacelleTcpBindOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_tcp_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        bind_options.stream,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_with_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let listener = bind_tcp_listener(addr, &NacelleTcpBindOptions::default())?;
    serve_tcp_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        NacelleTcpOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_with_options_and_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let bind_options = NacelleTcpBindOptions::from(tcp_options.clone());
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_tcp_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tcp_options,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_listener_with_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_tcp_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        NacelleTcpOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_listener_with_options_and_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<TcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    run_accept_loop(
        server,
        listener,
        "none",
        shutdown,
        drain_deadline,
        move |stream| {
            tcp_options
                .apply_to_stream(stream)
                .map_err(NacelleError::from)
        },
        |server, stream, connection, connection_permit| async move {
            let _connection_permit = connection_permit;
            server
                .serve_io_without_connection_limit(stream, connection)
                .await
        },
    )
    .await
}

/// Listen on `addr` and serve serial mutable-state connections.
pub async fn serve_serial_tcp<P, H, OH, Observer>(
    server: Arc<SerialTcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_serial_tcp_with_shutdown(server, addr, token).await
}

/// Listen on `addr` for serial connections until shutdown is requested.
pub async fn serve_serial_tcp_with_shutdown<P, H, OH, Observer>(
    server: Arc<SerialTcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_tcp_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        NacelleTcpBindOptions::default(),
        shutdown,
        NacelleDrainDeadline::new(Duration::from_secs(30)),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_with_bind_options_and_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<SerialTcpServer<P, H, OH, Observer>>,
    addr: SocketAddr,
    bind_options: NacelleTcpBindOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_serial_tcp_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        bind_options.stream,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_tcp_listener_with_options_and_shutdown_deadline<P, H, OH, Observer>(
    server: Arc<SerialTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined, transport);
                continue;
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                tcp_options.apply_to_stream(&stream)?;
                let connection_permit = match server
                    .runtime_state()
                    .acquire_connection_for_peer(peer_addr.ip())
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        let context = NacelleMetricsContext::new(
                            transport,
                            server.listener_label(),
                            server.protocol().name(),
                            "none",
                        );
                        server.telemetry().operation_error(&context, "accept", &error);
                        server.telemetry().connection_rejected(
                            transport,
                            connection_rejection_reason(&error),
                        );
                        continue;
                    }
                };
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr)
                    .with_listener(server.listener_label());
                let server = server.clone();
                connections.spawn(async move {
                    let _connection_permit = connection_permit;
                    server.serve_io_without_connection_limit(stream, connection).await
                });
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
