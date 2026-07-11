use std::net::SocketAddr;
use std::time::Duration;

use crate::options::NacelleTcpBindOptions;
use crate::protocol::{Protocol, TcpHandler};
use crate::server::NacelleServer;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::limits::TrackedPermit;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryEventKind, NacelleTransport,
};
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

pub(super) fn bind_tcp_listener(
    addr: SocketAddr,
    bind_options: &NacelleTcpBindOptions,
) -> std::io::Result<tokio::net::TcpListener> {
    let domain = if addr.is_ipv4() {
        socket2::Domain::IPV4
    } else {
        socket2::Domain::IPV6
    };
    let socket = socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    if addr.is_ipv6()
        && let Some(ipv6_only) = bind_options.ipv6_only
    {
        socket.set_only_v6(ipv6_only)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&socket2::SockAddr::from(addr))?;
    socket.listen(1024)?;
    let listener: std::net::TcpListener = socket.into();
    tokio::net::TcpListener::from_std(listener)
}

pub(super) fn log_connection_result(
    result: Option<Result<Result<(), NacelleError>, tokio::task::JoinError>>,
    transport: NacelleTransport,
) {
    match result {
        Some(Ok(Ok(()))) | None => {}
        Some(Ok(Err(error))) => {
            tracing::debug!(target: "nacelle", transport = transport.as_str(), error = %error, "connection finished with error");
        }
        Some(Err(error)) => {
            tracing::warn!(target: "nacelle", transport = transport.as_str(), error = %error, "connection task failed");
        }
    }
}

pub(super) fn connection_rejection_reason(error: &NacelleError) -> &'static str {
    match error {
        NacelleError::ResourceLimit(reason) => reason,
        _ => "connections",
    }
}

pub(super) fn record_connection_rejection<P, H>(
    server: &NacelleServer<P, H>,
    transport: NacelleTransport,
    tls: &'static str,
    error: &NacelleError,
) where
    P: Protocol,
    H: TcpHandler<P>,
{
    let context = NacelleMetricsContext::new(
        transport,
        server.listener_label(),
        server.protocol().name(),
        tls,
    );
    server
        .telemetry()
        .operation_error(&context, "accept", error);
}

pub(super) async fn drain_connection_tasks(
    mut connections: tokio::task::JoinSet<Result<(), NacelleError>>,
    drain_timeout: Duration,
    transport: NacelleTransport,
    telemetry: NacelleTelemetry,
) {
    telemetry.shutdown_event(NacelleTelemetryEventKind::DrainStarted, transport);
    let drain = async {
        while let Some(result) = connections.join_next().await {
            log_connection_result(Some(result), transport);
        }
    };

    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        tracing::info!(target: "nacelle", transport = transport.as_str(), "connection drain completed");
        telemetry.shutdown_event(NacelleTelemetryEventKind::DrainCompleted, transport);
        return;
    }

    let aborted = connections.len();
    tracing::warn!(target: "nacelle", transport = transport.as_str(), aborted, "connection drain timed out; aborting active tasks");
    telemetry.shutdown_event(NacelleTelemetryEventKind::DrainTimedOut, transport);
    telemetry.connections_aborted(transport, aborted);
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        log_connection_result(Some(result), transport);
    }
}

/// Shared TCP accept loop for the plain and TLS listener variants.
///
/// Accepts connections until `shutdown` fires, enforcing the connection limit
/// and per-connection setup. The two per-variant differences are injected as
/// closures: `prepare_stream` applies socket options (and decides whether to
/// propagate failures), and `serve_connection` turns an accepted stream into
/// the future spawned onto the connection task set (performing the TLS
/// handshake where applicable). `tls_label` is used only for rejection
/// telemetry.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_accept_loop<P, H, Prepare, Serve, Fut>(
    server: Arc<NacelleServer<P, H>>,
    listener: TcpListener,
    tls_label: &'static str,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
    prepare_stream: Prepare,
    mut serve_connection: Serve,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    Prepare: Fn(&TcpStream) -> Result<(), NacelleError>,
    Serve: FnMut(Arc<NacelleServer<P, H>>, TcpStream, NacelleConnectionMeta, TrackedPermit) -> Fut,
    Fut: Future<Output = Result<(), NacelleError>> + Send + 'static,
{
    let transport = NacelleTransport::new("tcp");
    let mut connections = tokio::task::JoinSet::new();
    let local_addr = listener.local_addr().ok();
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
                prepare_stream(&stream)?;
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr);
                let connection_permit = match server.runtime_state().acquire_connection_for_peer(peer_addr.ip()) {
                    Ok(permit) => permit,
                    Err(error) => {
                        record_connection_rejection(server.as_ref(), transport, tls_label, &error);
                        server
                            .telemetry()
                            .connection_rejected(transport, connection_rejection_reason(&error));
                        continue;
                    }
                };
                let task = serve_connection(server.clone(), stream, connection, connection_permit);
                connections.spawn(task);
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
