use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::protocol::Protocol;
use crate::server::NacelleServer;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionTlsMeta;
use nacelle_core::telemetry::NacelleTransport;
use nacelle_core::tls::NacelleTlsConfig;

use super::common::{bind_tcp_listener, run_accept_loop};

pub async fn serve_tcp_tls<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleTlsConfig,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_tls_with_shutdown(server, addr, tls_config, token).await
}

pub async fn serve_tcp_tls_with_shutdown<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleTlsConfig,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
{
    serve_tcp_tls_with_shutdown_timeout(server, addr, tls_config, shutdown, Duration::from_secs(30))
        .await
}

pub async fn serve_tcp_tls_with_shutdown_timeout<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleTlsConfig,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
{
    serve_tcp_tls_with_shutdown_deadline(
        server,
        addr,
        tls_config,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_tls_with_shutdown_deadline<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleTlsConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
{
    let listener = bind_tcp_listener(addr, &Default::default())?;
    serve_tcp_tls_listener_with_shutdown_deadline(
        server,
        listener,
        tls_config,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_tcp_tls_listener_with_shutdown_deadline<P, H>(
    server: Arc<NacelleServer<P, H>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleTlsConfig,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
{
    let handshake_timeout = tls_config.handshake_timeout();
    run_accept_loop(
        server,
        listener,
        "rustls",
        shutdown,
        drain_deadline,
        |stream| {
            let _ = stream.set_nodelay(true);
            Ok(())
        },
        move |server, stream, connection, connection_permit| {
            let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
            async move {
                let _connection_permit = connection_permit;
                let stream =
                    match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(error)) => return Err(NacelleError::Io(error)),
                        Err(_) => {
                            server
                                .telemetry()
                                .timeout(NacelleTransport::new("tcp"), "tls_handshake");
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    };
                let connection = connection.with_tls(NacelleConnectionTlsMeta::new("rustls"));
                server
                    .serve_io_without_connection_limit(stream, connection)
                    .await
            }
        },
    )
    .await
}
