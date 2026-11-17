use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;

use crate::options::NacelleUnixSocketOptions;
use crate::protocol::{Protocol, SharedProtocol};
use crate::runtime::common::reap_finished_connections;
use crate::serial_server::SerialTcpServer;
use crate::server::TcpServer;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdownToken};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleTelemetryEventKind, NacelleTelemetryObserver, NacelleTransport,
};

use super::common::{
    connection_rejection_reason, drain_connection_tasks, log_connection_result,
    record_connection_rejection,
};

/// Listen on a Unix domain socket and serve each accepted connection.
///
/// The socket path is passed directly to Tokio. Existing socket files are not
/// removed automatically.
pub async fn serve_unix<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_unix_with_shutdown(server, path, token).await
}

/// Listen on a Unix domain socket until shutdown is requested.
pub async fn serve_unix_with_shutdown<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_unix_with_shutdown_timeout(server, path, shutdown, Duration::from_secs(30)).await
}

/// Listen on a Unix domain socket until shutdown is requested, then drain or
/// abort active connection tasks after `drain_timeout`.
pub async fn serve_unix_with_shutdown_timeout<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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
pub async fn serve_unix_with_options<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_unix_with_options_and_shutdown(server, path, unix_options, token).await
}

/// Listen on a Unix domain socket with explicit lifecycle options until
/// shutdown is requested.
pub async fn serve_unix_with_options_and_shutdown<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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
pub async fn serve_unix_with_options_and_shutdown_timeout<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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
pub async fn serve_unix_with_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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
pub async fn serve_unix_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
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
pub async fn serve_unix_listener_with_shutdown_deadline<P, H, Observer>(
    server: Arc<TcpServer<P, H, Observer>>,
    listener: UnixListener,
    local_path: Option<PathBuf>,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let mut connections = tokio::task::JoinSet::new();
    nacelle_core::runtime::report_runtime_topology(
        "unix_socket",
        server.telemetry().runtime_metrics_enabled(),
    );
    loop {
        // Reap finished connection tasks without competing with `accept()`.
        reap_finished_connections(&mut connections, NacelleTransport::new("unix_socket"));
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let connection = NacelleConnectionMeta::unix_socket(local_path.clone())
                    .with_listener(server.listener_label());
                let registration = match server.runtime_state().register_connection(&connection) {
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
                let activity = registration.activity();
                let server = server.clone();
                connections.spawn(registration.run(async move {
                    server.serve_io_admitted(stream, connection, activity).await
                }));
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(joined, NacelleTransport::new("unix_socket"));
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

/// Listen on a Unix domain socket and serve serial mutable-state connections.
pub async fn serve_serial_unix<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_serial_unix_with_shutdown(server, path, token).await
}

/// Listen on a Unix domain socket for serial connections until shutdown.
pub async fn serve_serial_unix_with_shutdown<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_unix_with_options_and_shutdown_deadline(
        server,
        path,
        NacelleUnixSocketOptions::default(),
        shutdown,
        NacelleDrainDeadline::new(Duration::from_secs(30)),
    )
    .await
}

/// Listen for serial Unix connections with a bounded shutdown drain.
pub async fn serve_serial_unix_with_shutdown_timeout<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_unix_with_shutdown_deadline(
        server,
        path,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

/// Listen on a Unix domain socket for serial connections with explicit options.
pub async fn serve_serial_unix_with_options<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let (_shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    serve_serial_unix_with_options_and_shutdown_deadline(
        server,
        path,
        unix_options,
        token,
        NacelleDrainDeadline::new(Duration::from_secs(30)),
    )
    .await
}

/// Listen with serial Unix socket options until shutdown is requested.
pub async fn serve_serial_unix_with_options_and_shutdown<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_unix_with_options_and_shutdown_timeout(
        server,
        path,
        unix_options,
        shutdown,
        Duration::from_secs(30),
    )
    .await
}

/// Listen with serial Unix options and a bounded shutdown drain.
pub async fn serve_serial_unix_with_options_and_shutdown_timeout<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_timeout: Duration,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_unix_with_options_and_shutdown_deadline(
        server,
        path,
        unix_options,
        shutdown,
        NacelleDrainDeadline::new(drain_timeout),
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_unix_with_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    serve_serial_unix_with_options_and_shutdown_deadline(
        server,
        path,
        NacelleUnixSocketOptions::default(),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_unix_with_options_and_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    path: impl AsRef<Path>,
    unix_options: NacelleUnixSocketOptions,
    shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let path = path.as_ref();
    unix_options.prepare_path(path)?;
    let listener = UnixListener::bind(path)?;
    unix_options.apply_to_path(path)?;
    serve_serial_unix_listener_with_shutdown_deadline(
        server,
        listener,
        Some(path.to_path_buf()),
        shutdown,
        drain_deadline,
    )
    .await
}

#[doc(hidden)]
pub async fn serve_serial_unix_listener_with_shutdown_deadline<P, H, Observer>(
    server: Arc<SerialTcpServer<P, H, Observer>>,
    listener: UnixListener,
    local_path: Option<PathBuf>,
    mut shutdown: NacelleShutdownToken,
    drain_deadline: NacelleDrainDeadline,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
{
    let transport = NacelleTransport::new("unix_socket");
    let mut connections = tokio::task::JoinSet::new();
    nacelle_core::runtime::report_runtime_topology(
        "unix_socket",
        server.telemetry().runtime_metrics_enabled(),
    );

    loop {
        // Reap finished connection tasks without competing with `accept()`.
        reap_finished_connections(&mut connections, transport);
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let connection = NacelleConnectionMeta::unix_socket(local_path.clone())
                    .with_listener(server.listener_label());
                let registration = match server.runtime_state().register_connection(&connection) {
                    Ok(permit) => permit,
                    Err(error) => {
                        let context = nacelle_core::telemetry::NacelleMetricsContext::new(
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
                let activity = registration.activity();
                let server = server.clone();
                connections.spawn(registration.run(async move {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::{Bytes, BytesMut};
    use nacelle_codec::MessageDecoder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::protocol::{
        DecodedRequest, FrameBuffer, SerialTcpRequestContext, TcpHandlerCompletion, TcpResponse,
    };
    use nacelle_core::pipeline::ConnectionInfo;

    static NEXT_SOCKET: AtomicUsize = AtomicUsize::new(0);

    struct TestProtocol;
    struct TestDecoder;

    impl MessageDecoder for TestDecoder {
        type Message = DecodedRequest<()>;
        type Error = NacelleError;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
            if src.is_empty() {
                return Ok(None);
            }
            let _ = src.split_to(1);
            Ok(Some(DecodedRequest {
                request: (),
                body_len: 0,
            }))
        }
    }

    impl Protocol for TestProtocol {
        type Request = ();
        type Response = TcpResponse;
        type ConnectionState = ();
        type Decoder = TestDecoder;
        type ResponseContext = ();
        type ErrorContext = ();

        fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
            TestDecoder
        }

        fn connection_state(&self, _connection: &ConnectionInfo) {}

        fn request_wire_bytes(&self, _request: &(), _body_len: usize) -> usize {
            1
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
            chunk: Bytes,
            dst: &mut FrameBuffer<'_>,
        ) -> Result<(), NacelleError> {
            dst.extend_from_slice(&chunk)
        }

        fn encode_response_terminal_chunk(
            &self,
            context: &mut (),
            chunk: Bytes,
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
    }

    struct TestHandler;

    #[tokio::test]
    async fn global_connection_rate_is_shared_by_tcp_and_unix_listeners() {
        let socket = std::env::temp_dir().join(format!(
            "nacelle-global-rate-{}-{}.sock",
            std::process::id(),
            NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
        ));
        let unix_listener = UnixListener::bind(&socket).expect("Unix listener");
        let tcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("TCP listener");
        let tcp_addr = tcp_listener.local_addr().expect("TCP address");
        let state = nacelle_core::NacelleRuntimeState::default();
        let control = state.control();
        control
            .set_global_connection_open_rate(Some(1))
            .expect("global rate");
        let observer = nacelle_core::NacelleInMemoryObserver::new();
        let server = Arc::new(
            TcpServer::<TestProtocol>::builder()
                .protocol(TestProtocol)
                .runtime_state(state.clone())
                .expect("runtime state")
                .handler(nacelle_core::pipeline::handler_fn(
                    |context: crate::TcpRequestContext<TestProtocol>| async move {
                        context.respond(TcpResponse::bytes("ok")).await
                    },
                ))
                .build()
                .expect("server")
                .with_telemetry(
                    nacelle_core::NacelleTelemetry::default().with_observer(observer.clone()),
                ),
        );
        let shutdown = nacelle_core::NacelleShutdown::new();
        let tcp_task = tokio::spawn(
            super::super::tcp::serve_tcp_listener_with_shutdown_deadline(
                server.clone(),
                tcp_listener,
                shutdown.token(),
                NacelleDrainDeadline::new(Duration::from_secs(1)),
            ),
        );
        let unix_task = tokio::spawn(serve_unix_listener_with_shutdown_deadline(
            server,
            unix_listener,
            Some(socket.clone()),
            shutdown.token(),
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ));
        let mut tcp = tokio::net::TcpStream::connect(tcp_addr)
            .await
            .expect("TCP client");
        tcp.write_all(b"x").await.expect("TCP request");
        let mut response = [0; 2];
        tokio::time::timeout(Duration::from_secs(1), tcp.read_exact(&mut response))
            .await
            .expect("TCP response deadline")
            .expect("TCP response");
        assert_eq!(&response, b"ok");

        let mut rejected = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("Unix client");
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut byte))
                .await
                .expect("rejection deadline")
                .expect("Unix EOF"),
            0,
        );
        assert!(observer.events().iter().any(|event| {
            event.kind == NacelleTelemetryEventKind::ConnectionRejected
                && event.transport == Some(NacelleTransport::new("unix_socket"))
                && event.reason == Some("connection_rate")
        }));
        assert_eq!(state.active_connections(), 1);
        control
            .set_global_connection_open_rate(None)
            .expect("disable global rate");
        let mut unix = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("Unix client");
        unix.write_all(b"x").await.expect("Unix request");
        tokio::time::timeout(Duration::from_secs(1), unix.read_exact(&mut response))
            .await
            .expect("Unix response deadline")
            .expect("Unix response");
        assert_eq!(&response, b"ok");

        drop((tcp, unix, rejected));
        shutdown.shutdown();
        tcp_task.await.expect("TCP task").expect("TCP shutdown");
        unix_task.await.expect("Unix task").expect("Unix shutdown");
        assert_eq!(state.active_connections(), 0);
        std::fs::remove_file(socket).expect("remove Unix socket");
    }

    impl crate::SerialTcpHandler<TestProtocol> for TestHandler {
        async fn call<'connection>(
            &'connection self,
            context: SerialTcpRequestContext<'connection, TestProtocol>,
        ) -> Result<TcpHandlerCompletion<TestProtocol>, NacelleError> {
            context.respond(TcpResponse::bytes("ok")).await
        }
    }

    #[tokio::test]
    async fn serial_unix_accepts_request() {
        let socket = std::env::temp_dir().join(format!(
            "nacelle-serial-{}-{}.sock",
            std::process::id(),
            NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&socket).expect("listener should bind");
        let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        let server = Arc::new(
            SerialTcpServer::new(TestProtocol, TestHandler)
                .with_tcp_config(
                    crate::NacelleTcpConfig::default()
                        .with_head_read_limits(crate::TcpHeadReadLimits::new(1, 16).unwrap()),
                )
                .with_admission(
                    nacelle_core::AllowAll,
                    nacelle_core::AdmissionConfig::new(Duration::from_secs(1), 8).unwrap(),
                )
                .unwrap(),
        );
        let server_task = tokio::spawn(serve_serial_unix_listener_with_shutdown_deadline(
            server,
            listener,
            Some(socket.clone()),
            token,
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ));

        let mut client = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("client should connect");
        client.write_all(b"x").await.expect("request should write");
        let mut response = [0_u8; 2];
        client
            .read_exact(&mut response)
            .await
            .expect("response should read");
        assert_eq!(&response, b"ok");

        client.shutdown().await.expect("client shutdown");
        shutdown.shutdown();
        server_task
            .await
            .expect("server task should join")
            .expect("server should stop");
        std::fs::remove_file(socket).expect("socket should be removed");
    }
}
