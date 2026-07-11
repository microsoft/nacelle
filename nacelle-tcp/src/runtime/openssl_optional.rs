use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openssl::ssl::Ssl;

use crate::options::{NacelleTcpBindOptions, NacelleTcpOptions, NacelleTlsDetectionOptions};
use crate::protocol::{Protocol, TcpHandler};
use crate::server::NacelleServer;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetryEventKind, NacelleTransport};
use nacelle_core::tls::NacelleOpenSslConfig;

use super::common::{
    bind_tcp_listener, connection_rejection_reason, drain_connection_tasks, log_connection_result,
    record_connection_rejection,
};
use super::openssl::openssl_tls_meta;

pub async fn serve_tcp_optional_openssl<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_optional_openssl_with_shutdown(server, addr, tls_config, token).await
}

pub async fn serve_tcp_optional_openssl_with_shutdown<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    serve_tcp_optional_openssl_with_options_and_shutdown(
        server,
        addr,
        tls_config,
        NacelleTcpOptions::default(),
        NacelleTlsDetectionOptions::default(),
        shutdown,
    )
    .await
}

pub async fn serve_tcp_optional_openssl_with_shutdown_timeout<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    serve_tcp_optional_openssl_with_options_and_shutdown_timeout(
        server,
        addr,
        tls_config,
        NacelleTcpOptions::default(),
        NacelleTlsDetectionOptions::default(),
        shutdown,
        drain_timeout,
    )
    .await
}

pub async fn serve_tcp_optional_openssl_with_options<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    detection_options: NacelleTlsDetectionOptions,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_tcp_optional_openssl_with_options_and_shutdown(
        server,
        addr,
        tls_config,
        tcp_options,
        detection_options,
        token,
    )
    .await
}

pub async fn serve_tcp_optional_openssl_with_options_and_shutdown<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    detection_options: NacelleTlsDetectionOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    serve_tcp_optional_openssl_with_options_and_shutdown_timeout(
        server,
        addr,
        tls_config,
        tcp_options,
        detection_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn serve_tcp_optional_openssl_with_options_and_shutdown_timeout<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    detection_options: NacelleTlsDetectionOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    serve_tcp_optional_openssl_with_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        tcp_options,
        detection_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn serve_tcp_optional_openssl_with_options_and_shutdown_deadline<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    detection_options: NacelleTlsDetectionOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
        server,
        addr,
        tls_config,
        NacelleTcpBindOptions::from(tcp_options),
        detection_options,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline<P, H>(
    server: Arc<NacelleServer<P, H>>,
    addr: SocketAddr,
    tls_config: NacelleOpenSslConfig,
    bind_options: NacelleTcpBindOptions,
    detection_options: NacelleTlsDetectionOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let listener = bind_tcp_listener(addr, &bind_options)?;
    serve_tcp_optional_openssl_listener_with_options_and_shutdown_deadline(
        server,
        listener,
        tls_config,
        bind_options.stream,
        detection_options,
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
#[allow(clippy::too_many_arguments)]
pub async fn serve_tcp_optional_openssl_listener_with_options_and_shutdown_deadline<P, H>(
    server: Arc<NacelleServer<P, H>>,
    listener: tokio::net::TcpListener,
    tls_config: NacelleOpenSslConfig,
    tcp_options: NacelleTcpOptions,
    detection_options: NacelleTlsDetectionOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let handshake_timeout = tls_config.handshake_timeout();
    let mut connections = tokio::task::JoinSet::new();
    let local_addr = listener.local_addr().ok();
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined, NacelleTransport::new("tcp"));
                continue;
            }
            accepted = listener.accept() => {
                let (stream, peer_addr) = accepted?;
                tcp_options.apply_to_stream(&stream)?;
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr);
                let connection_permit = match server.runtime_state().acquire_connection_for_peer(peer_addr.ip()) {
                    Ok(permit) => permit,
                    Err(error) => {
                        record_connection_rejection(server.as_ref(), NacelleTransport::new("tcp"), "unknown", &error);
                        server
                            .telemetry()
                            .connection_rejected(NacelleTransport::new("tcp"), connection_rejection_reason(&error));
                        continue;
                    }
                };
                let server = server.clone();
                let acceptor = tls_config.acceptor();
                let detection_timeout = detection_options.timeout;
                connections.spawn(async move {
                    let _connection_permit = connection_permit;
                    let is_tls = match detect_tls_handshake(&stream, detection_timeout).await {
                        Ok(is_tls) => is_tls,
                        Err(error) => {
                            if matches!(error, NacelleError::Timeout(_)) {
                                server
                                    .telemetry()
                                    .timeout(NacelleTransport::new("tcp"), "tls_detect");
                            }
                            return Err(error);
                        }
                    };

                    if !is_tls {
                        return server.serve_io_without_connection_limit(stream, connection).await;
                    }

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
                            server
                                .telemetry()
                                .timeout(NacelleTransport::new("tcp"), "tls_handshake");
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    }
                    let connection = connection.with_tls(openssl_tls_meta(stream.ssl()));
                    server.serve_io_without_connection_limit(stream, connection).await
                });
            }
        }
    }
    server.telemetry().shutdown_event(
        NacelleTelemetryEventKind::ListenerStoppedAccepting,
        NacelleTransport::new("tcp"),
    );
    drain_connection_tasks(
        connections,
        drain_deadline.get(),
        NacelleTransport::new("tcp"),
        server.telemetry().clone(),
    )
    .await;
    Ok(())
}

async fn detect_tls_handshake(
    stream: &tokio::net::TcpStream,
    timeout: Duration,
) -> Result<bool, NacelleError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut peek_buf = [0_u8; 3];
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(NacelleError::Timeout("tls_detect"));
        }

        match tokio::time::timeout(remaining, stream.peek(&mut peek_buf)).await {
            Ok(Ok(0)) => return Err(NacelleError::ConnectionClosed),
            Ok(Ok(len)) if !peeked_bytes_can_be_tls(&peek_buf[..len]) => return Ok(false),
            Ok(Ok(len)) if len >= 3 => return Ok(true),
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(NacelleError::Io(error)),
            Err(_) => return Err(NacelleError::Timeout("tls_detect")),
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) fn peeked_bytes_can_be_tls(bytes: &[u8]) -> bool {
    match bytes {
        [] => true,
        [record_type] => *record_type == 0x16,
        [record_type, major] => *record_type == 0x16 && *major == 0x03,
        [record_type, major, minor, ..] => {
            *record_type == 0x16 && *major == 0x03 && (0x01..=0x04).contains(minor)
        }
    }
}
