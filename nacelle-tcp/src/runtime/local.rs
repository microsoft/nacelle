use std::rc::Rc;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetryEventKind, NacelleTransport};

use crate::connection::serve_local_stream_without_connection_limit;
use crate::options::NacelleTcpOptions;
use crate::protocol::{LocalTcpHandler, LocalTcpOneWayHandler, Protocol};
use crate::server::LocalTcpServer;

/// Serve one worker-local TCP listener until shared shutdown is requested.
///
/// This function must run inside a Tokio [`tokio::task::LocalSet`]. Each
/// accepted stream is spawned locally and remains on the accepting worker.
pub async fn serve_local_tcp_listener<P, H, OH>(
    server: Rc<LocalTcpServer<P, H, OH>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                log_local_connection_result(joined);
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
                        server
                            .telemetry()
                            .connection_rejected(transport, connection_rejection_reason(&error));
                        continue;
                    }
                };
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr)
                    .with_listener(server.listener_label());
                let server = server.clone();
                connections.spawn_local(async move {
                    let _connection_permit = connection_permit;
                    serve_local_stream_without_connection_limit(
                        stream,
                        server.protocol(),
                        server.handler(),
                        server.one_way_handler(),
                        server.config(),
                        server.telemetry(),
                        server.runtime_state(),
                        server.tcp_limits(),
                        connection,
                    )
                    .await
                });
            }
        }
    }

    server.telemetry().shutdown_event(
        NacelleTelemetryEventKind::ListenerStoppedAccepting,
        transport,
    );
    drain_local_connections(
        connections,
        drain_deadline.get(),
        server.telemetry(),
        transport,
    )
    .await;
    Ok(())
}

fn connection_rejection_reason(error: &NacelleError) -> &'static str {
    match error {
        NacelleError::ResourceLimit(reason) => reason,
        _ => "connections",
    }
}

fn log_local_connection_result(
    result: Option<Result<Result<(), NacelleError>, tokio::task::JoinError>>,
) {
    match result {
        Some(Ok(Ok(()))) | None => {}
        Some(Ok(Err(error))) => {
            tracing::debug!(target: "nacelle", transport = "tcp", error = %error, "local connection finished with error");
        }
        Some(Err(error)) => {
            tracing::warn!(target: "nacelle", transport = "tcp", error = %error, "local connection task failed");
        }
    }
}

async fn drain_local_connections(
    mut connections: tokio::task::JoinSet<Result<(), NacelleError>>,
    drain_timeout: std::time::Duration,
    telemetry: nacelle_core::telemetry::NacelleTelemetry,
    transport: NacelleTransport,
) {
    telemetry.shutdown_event(NacelleTelemetryEventKind::DrainStarted, transport);
    let drain = async {
        while let Some(result) = connections.join_next().await {
            log_local_connection_result(Some(result));
        }
    };
    if tokio::time::timeout(drain_timeout, drain).await.is_ok() {
        telemetry.shutdown_event(NacelleTelemetryEventKind::DrainCompleted, transport);
        return;
    }

    let aborted = connections.len();
    telemetry.shutdown_event(NacelleTelemetryEventKind::DrainTimedOut, transport);
    telemetry.connections_aborted(transport, aborted);
    connections.abort_all();
    while let Some(result) = connections.join_next().await {
        log_local_connection_result(Some(result));
    }
}
