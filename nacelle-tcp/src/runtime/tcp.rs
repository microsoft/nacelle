use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::options::{NacelleTcpBindOptions, NacelleTcpOptions};
use crate::protocol::Protocol;
use crate::server::NacelleServer;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};

use super::common::{bind_tcp_listener, run_accept_loop};

/// Listen on `addr` and serve each accepted TCP connection in its own task.
pub async fn serve_tcp<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_with_shutdown(server, addr, token).await
}

/// Listen on `addr` until shutdown is requested.
pub async fn serve_tcp_with_shutdown<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
{
    serve_tcp_with_shutdown_timeout(server, addr, shutdown, Duration::from_secs(30)).await
}

/// Listen on `addr` until shutdown is requested, then drain or abort active
/// connection tasks after `drain_timeout`.
pub async fn serve_tcp_with_shutdown_timeout<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_with_options<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_with_options_and_shutdown(server, addr, tcp_options, token).await
}

/// Listen on `addr` with explicit TCP socket options until shutdown is requested.
pub async fn serve_tcp_with_options_and_shutdown<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_with_options_and_shutdown_timeout<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_with_bind_options_and_shutdown_deadline<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    bind_options: NacelleTcpBindOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_with_shutdown_deadline<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_with_options_and_shutdown_deadline<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_listener_with_shutdown_deadline<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    listener: tokio::net::TcpListener,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
pub async fn serve_tcp_listener_with_options_and_shutdown_deadline<Req, P, H>(
    server: Arc<NacelleServer<Req, P, H>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    Req: Send + 'static,
    P: Protocol<Request = Req> + Send + Sync + 'static,
    H: Handler,
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
