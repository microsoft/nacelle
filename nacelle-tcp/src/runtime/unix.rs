use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;

use crate::options::NacelleUnixSocketOptions;
use crate::protocol::{Protocol, TcpHandler, TcpOneWayHandler};
use crate::server::NacelleServer;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetryEventKind, NacelleTransport};

use super::common::{
    connection_rejection_reason, drain_connection_tasks, log_connection_result,
    record_connection_rejection,
};

/// Listen on a Unix domain socket and serve each accepted connection.
///
/// The socket path is passed directly to Tokio. Existing socket files are not
/// removed automatically.
pub async fn serve_unix<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_unix_with_shutdown(server, path, token).await
}

/// Listen on a Unix domain socket until shutdown is requested.
pub async fn serve_unix_with_shutdown<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    serve_unix_with_shutdown_timeout(server, path, shutdown, Duration::from_secs(30)).await
}

/// Listen on a Unix domain socket until shutdown is requested, then drain or
/// abort active connection tasks after `drain_timeout`.
pub async fn serve_unix_with_shutdown_timeout<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    serve_unix_with_shutdown_deadline(
        server,
        path,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

/// Listen on a Unix domain socket with explicit socket-file lifecycle options.
pub async fn serve_unix_with_options<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_unix_with_options_and_shutdown(server, path, unix_options, token).await
}

/// Listen on a Unix domain socket with explicit lifecycle options until
/// shutdown is requested.
pub async fn serve_unix_with_options_and_shutdown<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    serve_unix_with_options_and_shutdown_timeout(
        server,
        path,
        unix_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Listen on a Unix domain socket with explicit lifecycle options, then drain
/// or abort active connection tasks after `drain_timeout`.
pub async fn serve_unix_with_options_and_shutdown_timeout<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    serve_unix_with_options_and_shutdown_deadline(
        server,
        path,
        unix_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_unix_with_shutdown_deadline<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    serve_unix_with_options_and_shutdown_deadline(
        server,
        path,
        NacelleUnixSocketOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_unix_with_options_and_shutdown_deadline<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    let path = path.as_ref();
    unix_options.prepare_path(path)?;
    let listener = UnixListener::bind(path)?;
    unix_options.apply_to_path(path)?;
    serve_unix_listener_with_shutdown_deadline(
        server,
        listener,
        Some(path.to_path_buf()),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_unix_listener_with_shutdown_deadline<P, H, OH>(
    server: Arc<NacelleServer<P, H, OH>>,
    listener: UnixListener,
    local_path: Option<PathBuf>,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
{
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined, NacelleTransport::new("unix_socket"));
                continue;
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let connection = NacelleConnectionMeta::unix_socket(local_path.clone());
                let connection_permit = match server.runtime_state().acquire_connection_tracked() {
                    Ok(permit) => permit,
                    Err(error) => {
                        record_connection_rejection(
                            server.as_ref(),
                            NacelleTransport::new("unix_socket"),
                            "none",
                            &error,
                        );
                        server
                            .telemetry()
                            .connection_rejected(NacelleTransport::new("unix_socket"), connection_rejection_reason(&error));
                        continue;
                    }
                };
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
        NacelleTransport::new("unix_socket"),
    );
    drain_connection_tasks(
        connections,
        drain_deadline.get(),
        NacelleTransport::new("unix_socket"),
        server.telemetry().clone(),
    )
    .await;
    Ok(())
}
