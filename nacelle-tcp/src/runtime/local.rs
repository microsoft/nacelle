use std::rc::Rc;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
#[cfg(feature = "rustls")]
use nacelle_core::request::NacelleConnectionTlsMeta;
use nacelle_core::telemetry::{
    NacelleTelemetryEventKind, NacelleTelemetryObserver, NacelleTransport,
};
#[cfg(feature = "openssl")]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(feature = "rustls")]
use nacelle_core::tls::NacelleTlsConfig;
#[cfg(feature = "openssl")]
use openssl::ssl::Ssl;

use crate::connection::{
    serve_local_serial_stream_without_connection_limit, serve_local_stream_without_connection_limit,
};
use crate::options::NacelleTcpOptions;
#[cfg(feature = "openssl")]
use crate::options::NacelleTlsDetectionOptions;
use crate::protocol::{
    LocalSerialTcpHandler, LocalSerialTcpOneWayHandler, LocalTcpHandler, LocalTcpOneWayHandler,
    Protocol,
};
use crate::serial_server::LocalSerialTcpServer;
use crate::server::LocalTcpServer;

#[cfg(feature = "openssl")]
use super::openssl_optional::detect_tls_handshake;

/// Serve one worker-local TCP listener until shared shutdown is requested.
///
/// This function must run inside a Tokio [`tokio::task::LocalSet`]. Each
/// accepted stream is spawned locally and remains on the accepting worker.
pub async fn serve_local_tcp_listener<P, H, OH, Observer>(
    server: Rc<LocalTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
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

/// Serve one worker-local serial TCP listener until shared shutdown.
pub async fn serve_local_serial_tcp_listener<P, H, OH, Observer>(
    server: Rc<LocalSerialTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P> + 'static,
    OH: LocalSerialTcpOneWayHandler<P> + 'static,
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
                    serve_local_serial_stream_without_connection_limit(
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

/// Serve one worker-local Rustls TCP listener until shared shutdown.
#[cfg(feature = "rustls")]
pub async fn serve_local_tcp_tls_listener<P, H, OH, Observer>(
    server: Rc<LocalTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    tls_config: NacelleTlsConfig,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let handshake_timeout = tls_config.handshake_timeout();
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
                let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
                let server = server.clone();
                connections.spawn_local(async move {
                    let _connection_permit = connection_permit;
                    let stream = match tokio::time::timeout(
                        handshake_timeout,
                        acceptor.accept(stream),
                    )
                    .await
                    {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(error)) => return Err(NacelleError::Io(error)),
                        Err(_) => {
                            server
                                .telemetry()
                                .timeout(transport, "tls_handshake");
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    };
                    let connection =
                        connection.with_tls(NacelleConnectionTlsMeta::new("rustls"));
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

/// Serve one worker-local required-OpenSSL TCP listener until shared shutdown.
#[cfg(feature = "openssl")]
pub async fn serve_local_tcp_openssl_listener<P, H, OH, Observer>(
    server: Rc<LocalTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    tls_config: NacelleOpenSslConfig,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let handshake_timeout = tls_config.handshake_timeout();
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
                let acceptor = tls_config.acceptor();
                let server = server.clone();
                connections.spawn_local(async move {
                    let _connection_permit = connection_permit;
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
                                .timeout(transport, "tls_handshake");
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    }
                    let connection = connection.with_tls(
                        crate::runtime::openssl::openssl_tls_meta(stream.ssl()),
                    );
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

/// Serve one worker-local serial required-OpenSSL listener until shutdown.
#[cfg(feature = "openssl")]
pub async fn serve_local_serial_tcp_openssl_listener<P, H, OH, Observer>(
    server: Rc<LocalSerialTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    tls_config: NacelleOpenSslConfig,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P> + 'static,
    OH: LocalSerialTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let handshake_timeout = tls_config.handshake_timeout();
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
                        server.telemetry().connection_rejected(
                            transport,
                            connection_rejection_reason(&error),
                        );
                        continue;
                    }
                };
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr)
                    .with_listener(server.listener_label());
                let acceptor = tls_config.acceptor();
                let server = server.clone();
                connections.spawn_local(async move {
                    let _connection_permit = connection_permit;
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
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    }
                    let connection = connection.with_tls(
                        crate::runtime::openssl::openssl_tls_meta(stream.ssl()),
                    );
                    serve_local_serial_stream_without_connection_limit(
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

/// Serve one worker-local serial plaintext-or-OpenSSL listener until shutdown.
#[cfg(feature = "openssl")]
#[allow(clippy::too_many_arguments)]
pub async fn serve_local_serial_tcp_optional_openssl_listener<P, H, OH, Observer>(
    server: Rc<LocalSerialTcpServer<P, H, OH, Observer>>,
    listener: tokio::net::TcpListener,
    tcp_options: NacelleTcpOptions,
    tls_config: NacelleOpenSslConfig,
    detection_options: NacelleTlsDetectionOptions,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P> + 'static,
    OH: LocalSerialTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("tcp");
    let local_addr = listener.local_addr().ok();
    let handshake_timeout = tls_config.handshake_timeout();
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
                        server.telemetry().connection_rejected(
                            transport,
                            connection_rejection_reason(&error),
                        );
                        continue;
                    }
                };
                let connection = NacelleConnectionMeta::tcp(Some(peer_addr), local_addr)
                    .with_listener(server.listener_label());
                let acceptor = tls_config.acceptor();
                let detection_timeout = detection_options.timeout;
                let server = server.clone();
                connections.spawn_local(async move {
                    let _connection_permit = connection_permit;
                    let is_tls = match detect_tls_handshake(&stream, detection_timeout).await {
                        Ok(is_tls) => is_tls,
                        Err(error) => {
                            if matches!(error, NacelleError::Timeout(_)) {
                                server.telemetry().timeout(transport, "tls_detect");
                            }
                            return Err(error);
                        }
                    };

                    if !is_tls {
                        return serve_local_serial_stream_without_connection_limit(
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
                        .await;
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
                            server.telemetry().timeout(transport, "tls_handshake");
                            return Err(NacelleError::Timeout("tls_handshake"));
                        }
                    }
                    let connection = connection.with_tls(
                        crate::runtime::openssl::openssl_tls_meta(stream.ssl()),
                    );
                    serve_local_serial_stream_without_connection_limit(
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

async fn drain_local_connections<Observer>(
    mut connections: tokio::task::JoinSet<Result<(), NacelleError>>,
    drain_timeout: std::time::Duration,
    telemetry: nacelle_core::telemetry::NacelleTelemetry<Observer>,
    transport: NacelleTransport,
) where
    Observer: NacelleTelemetryObserver,
{
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

#[cfg(all(test, feature = "tls-self-signed"))]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use bytes::BytesMut;
    use nacelle_codec::MessageDecoder;
    use nacelle_core::pipeline::local_handler_fn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::{DecodedMessage, DecodedRequest, FrameBuffer, TcpRequestContext, TcpResponse};

    struct Decoder;

    impl MessageDecoder for Decoder {
        type Message = DecodedMessage<(), std::convert::Infallible>;
        type Error = NacelleError;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
            if src.is_empty() {
                return Ok(None);
            }
            let _ = src.split_to(1);
            Ok(Some(DecodedMessage::Request(DecodedRequest {
                request: (),
                body_len: 0,
            })))
        }
    }

    struct TestProtocol;

    impl Protocol for TestProtocol {
        type Request = ();
        type OneWayRequest = std::convert::Infallible;
        type Response = TcpResponse;
        type ConnectionState = ();
        type Decoder = Decoder;
        type ResponseContext = ();
        type ErrorContext = ();

        fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
            Decoder
        }
        fn connection_state(&self, _connection: &nacelle_core::pipeline::ConnectionInfo) {}
        fn request_wire_bytes(&self, _request: &(), body_len: usize) -> usize {
            body_len
        }
        fn one_way_wire_bytes(
            &self,
            request: &std::convert::Infallible,
            _body_len: usize,
        ) -> usize {
            match *request {}
        }
        fn response_context(&self, _request: &()) {}
        fn error_context(&self, _request: &()) {}
        fn apply_response(&self, _context: &mut (), _response: &TcpResponse) {}
        fn max_response_frame_overhead(&self) -> usize {
            0
        }
        fn response_body(&self, response: TcpResponse) -> nacelle_core::NacelleBody {
            response.body
        }
        fn encode_response_chunk(
            &self,
            _context: &mut (),
            chunk: bytes::Bytes,
            dst: &mut FrameBuffer<'_>,
        ) -> Result<(), NacelleError> {
            dst.extend_from_slice(&chunk)
        }
        fn encode_response_terminal_chunk(
            &self,
            context: &mut (),
            chunk: bytes::Bytes,
            dst: &mut FrameBuffer<'_>,
        ) -> Result<(), NacelleError> {
            self.encode_response_chunk(context, chunk, dst)
        }
        fn encode_response_end(
            &self,
            _context: &mut (),
            _dst: &mut FrameBuffer<'_>,
        ) -> Result<(), NacelleError> {
            Ok(())
        }
        fn encode_error(
            &self,
            _context: Option<&()>,
            _error: &NacelleError,
            _dst: &mut FrameBuffer<'_>,
        ) -> Result<(), NacelleError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_rustls_tcp_accepts_request() {
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
                let requests = Rc::new(RefCell::new(0));
                let handler = local_handler_fn({
                    let requests = requests.clone();
                    move |mut context: TcpRequestContext<TestProtocol>| {
                        let requests = requests.clone();
                        async move {
                            *requests.borrow_mut() += 1;
                            let _ = context
                                .request_mut()
                                .body
                                .next_chunk()
                                .await
                                .transpose()?
                                .unwrap_or_default();
                            context.respond(TcpResponse::bytes("x")).await
                        }
                    }
                });
                let server = Rc::new(LocalTcpServer::new(TestProtocol, handler));
                let task = tokio::task::spawn_local(serve_local_tcp_tls_listener(
                    server,
                    listener,
                    NacelleTcpOptions::default(),
                    generated.tls_config,
                    token,
                    NacelleDrainDeadline::new(std::time::Duration::from_secs(1)),
                ));
                let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
                let name = rustls::pki_types::ServerName::try_from("localhost").expect("name");
                let mut client = connector.connect(name, stream).await.expect("handshake");
                client.write_all(b"x").await.expect("write");
                let mut response = [0_u8; 1];
                client.read_exact(&mut response).await.expect("read");
                assert_eq!(response, [b'x']);
                client.shutdown().await.expect("client shutdown");
                shutdown.shutdown();
                task.await.expect("join").expect("server");
                assert_eq!(*requests.borrow(), 1);
            })
            .await;
    }
}
