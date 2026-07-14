use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::config::{NacelleTcpConfig, ResponseWritePolicy, TcpRequestBodyMode};
use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdown};
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::pipeline::{ConnectionInfo, handler_fn, local_handler_fn};
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::{
    NacelleInMemoryObserver, NacelleTelemetry, NacelleTelemetryEventKind,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::protocol::{
    DecodedMessage, DecodedRequest, FrameBuffer, LocalSerialTcpHandler,
    LocalSerialTcpOneWayHandler, Protocol, SerialTcpHandler, SerialTcpOneWayContext,
    SerialTcpOneWayHandler, SerialTcpRequestContext, TcpOneWayContext, TcpRequestContext,
    TcpResponse,
};
use crate::serial_server::{LocalSerialTcpServer, SerialTcpServer};
use crate::server::TcpServer;

use super::{serve_local_stream_without_connection_limit, serve_stream_with_connection_meta};

const PRE_AUTH_BODY_LIMIT: usize = 2;

struct RecordingIo {
    input: Vec<u8>,
    position: usize,
    writes: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
}

impl AsyncRead for RecordingIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.position >= self.input.len() {
            return Poll::Ready(Ok(()));
        }
        let available = &self.input[self.position..];
        let take = available.len().min(buf.remaining());
        buf.put_slice(&available[..take]);
        self.position += take;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for RecordingIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes
            .lock()
            .expect("write recorder poisoned")
            .push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct PhaseRequest;

#[derive(Debug)]
struct AuthState {
    authenticated: AtomicBool,
}

struct PhaseProtocol {
    authenticated: bool,
    request_wire_bytes: Option<Arc<AtomicUsize>>,
    encoder_writes_then_errors: bool,
}

struct PhaseDecoder;

impl MessageDecoder for PhaseDecoder {
    type Message = DecodedMessage<PhaseRequest, Infallible>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let body_len = src[0] as usize;
        let _head = src.split_to(1);
        Ok(Some(DecodedMessage::Request(DecodedRequest {
            request: PhaseRequest,
            body_len,
        })))
    }
}

impl Protocol for PhaseProtocol {
    type Request = PhaseRequest;
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = AuthState;
    type Decoder = PhaseDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PhaseDecoder
    }

    fn connection_state(&self, _: &ConnectionInfo) -> Self::ConnectionState {
        AuthState {
            authenticated: AtomicBool::new(self.authenticated),
        }
    }

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        let wire_bytes = 1 + body_len;
        if let Some(observed) = &self.request_wire_bytes {
            observed.store(wire_bytes, Ordering::SeqCst);
        }
        wire_bytes
    }

    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
        match *request {}
    }

    fn max_request_body_bytes(
        &self,
        _request: &Self::Request,
        _connection: &ConnectionInfo,
        state: &Self::ConnectionState,
        default_limit: usize,
    ) -> usize {
        if state.authenticated.load(Ordering::Acquire) {
            default_limit
        } else {
            PRE_AUTH_BODY_LIMIT
        }
    }

    fn response_context(&self, _req: &PhaseRequest) -> Self::ResponseContext {}

    fn error_context(&self, _req: &PhaseRequest) -> Self::ErrorContext {}

    fn apply_response(&self, _context: &mut Self::ResponseContext, _response: &Self::Response) {}

    fn max_response_frame_overhead(&self) -> usize {
        0
    }

    fn response_body(&self, response: Self::Response) -> nacelle_core::request::NacelleBody {
        response.body
    }

    fn encode_response_chunk(
        &self,
        _context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)?;
        if self.encoder_writes_then_errors {
            return Err(NacelleError::InvalidFrame("test response encoder"));
        }
        Ok(())
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        _context: &mut Self::ResponseContext,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }

    fn encode_error(
        &self,
        _context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(error.to_string().as_bytes())?;
        Ok(())
    }
}

async fn recorded_response_writes(policy: ResponseWritePolicy) -> Vec<Vec<u8>> {
    let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let responses = Arc::new(AtomicUsize::new(0));
    let io = RecordingIo {
        input: vec![0, 0, 0],
        position: 0,
        writes: writes.clone(),
    };
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn({
            let responses = responses.clone();
            move |context: TcpRequestContext<PhaseProtocol>| {
                let responses = responses.clone();
                async move {
                    let body = match responses.fetch_add(1, Ordering::Relaxed) {
                        0 => "a",
                        1 => "b",
                        _ => "c",
                    };
                    context.respond(TcpResponse::bytes(body)).await
                }
            }
        }))
        .tcp_config(NacelleTcpConfig::default().with_response_write_policy(policy))
        .build()
        .expect("recording server should build");

    server
        .serve_io(io)
        .await
        .expect("recording server should run");
    writes.lock().expect("write recorder poisoned").clone()
}

#[tokio::test]
async fn response_write_policies_reduce_writes_and_preserve_order() {
    let immediate = recorded_response_writes(ResponseWritePolicy::Immediate).await;
    let coalesced = recorded_response_writes(ResponseWritePolicy::CoalesceBuffered).await;
    let threshold = recorded_response_writes(ResponseWritePolicy::FlushAtBytes(2)).await;

    assert_eq!(immediate, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(coalesced, vec![b"abc".to_vec()]);
    assert_eq!(threshold, vec![b"ab".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn coalesced_single_response_flushes_before_waiting_for_input() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn(
            |context: TcpRequestContext<PhaseProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .build()
        .expect("coalesced server should build");
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client.write_all(&[0]).await.expect("request should write");
    let mut response = [0_u8; 2];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .expect("response should flush before another request")
        .expect("response should read");
    assert_eq!(&response, b"ok");

    client.shutdown().await.expect("client should close");
    server_task
        .await
        .expect("server task should join")
        .expect("server should close cleanly");
}

#[tokio::test]
async fn phase_limit_rejects_unauthenticated_body_before_reading_body() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let handler_called = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn({
            let handler_called = handler_called.clone();
            move |context: TcpRequestContext<PhaseProtocol>| {
                handler_called.store(true, Ordering::SeqCst);
                async move { context.respond(TcpResponse::empty()).await }
            }
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[3]).await.expect("head should write");
    let mut response = [0_u8; 128];
    let bytes_read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut response))
        .await
        .expect("response should arrive")
        .expect("response should read");
    let response = std::str::from_utf8(&response[..bytes_read]).expect("utf8 error response");

    assert!(response.contains("request_body_bytes"));
    assert!(!handler_called.load(Ordering::SeqCst));
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit("request_body_bytes"))
    ));
}

#[tokio::test]
async fn authenticated_phase_uses_default_request_body_limit() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let handler_called = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn({
            let handler_called = handler_called.clone();
            move |mut context: TcpRequestContext<PhaseProtocol>| {
                let handler_called = handler_called.clone();
                async move {
                    let auth_state: &Arc<AuthState> = &context.connection().state;
                    assert!(auth_state.authenticated.load(Ordering::Acquire));
                    handler_called.store(true, Ordering::SeqCst);
                    let mut body = Vec::new();
                    while let Some(chunk) = context.request_mut().body.next_chunk().await {
                        body.extend_from_slice(&chunk?);
                    }
                    assert_eq!(body, b"hey");
                    context.respond(TcpResponse::bytes("ok")).await
                }
            }
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client
        .write_all(&[3, b'h', b'e', b'y'])
        .await
        .expect("request should write");
    let mut response = [0_u8; 2];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .expect("response should arrive")
        .expect("response should read");
    assert_eq!(&response, b"ok");
    assert!(handler_called.load(Ordering::SeqCst));

    drop(client);
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());
}

#[tokio::test]
async fn connection_state_transition_changes_next_request_body_limit() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let calls = Arc::new(AtomicUsize::new(0));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn({
            let calls = calls.clone();
            move |mut context: TcpRequestContext<PhaseProtocol>| {
                let calls = calls.clone();
                async move {
                    let call = calls.fetch_add(1, Ordering::SeqCst);
                    if call == 0 {
                        context
                            .connection()
                            .state
                            .authenticated
                            .store(true, Ordering::Release);
                    } else {
                        let mut body = Vec::new();
                        while let Some(chunk) = context.request_mut().body.next_chunk().await {
                            body.extend_from_slice(&chunk?);
                        }
                        assert_eq!(body, b"hey");
                    }
                    context.respond(TcpResponse::bytes("ok")).await
                }
            }
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client
        .write_all(&[0, 3, b'h', b'e', b'y'])
        .await
        .expect("requests should write");
    let mut responses = [0_u8; 4];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut responses))
        .await
        .expect("responses should arrive")
        .expect("responses should read");
    assert_eq!(&responses, b"okok");
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    drop(client);
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join")
        .expect("connection should close cleanly");
}

#[tokio::test(flavor = "current_thread")]
async fn local_dispatch_uses_identical_state_aware_body_limits() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let handler = local_handler_fn({
        let calls = calls.clone();
        move |mut context: TcpRequestContext<PhaseProtocol>| {
            let calls = calls.clone();
            async move {
                let call = calls.get();
                calls.set(call + 1);
                if call == 0 {
                    context
                        .connection()
                        .state
                        .authenticated
                        .store(true, Ordering::Release);
                } else {
                    let mut body = Vec::new();
                    while let Some(chunk) = context.request_mut().body.next_chunk().await {
                        body.extend_from_slice(&chunk?);
                    }
                    assert_eq!(body, b"hey");
                }
                context.respond(TcpResponse::bytes("ok")).await
            }
        }
    });
    let server = serve_local_stream_without_connection_limit(
        server_io,
        std::rc::Rc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        std::rc::Rc::new(handler),
        std::rc::Rc::new(crate::protocol::NoOneWayHandler::<PhaseProtocol>::new()),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        crate::limits::NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    );
    let client = async move {
        client
            .write_all(&[0, 3, b'h', b'e', b'y'])
            .await
            .expect("requests should write");
        let mut responses = [0_u8; 4];
        client
            .read_exact(&mut responses)
            .await
            .expect("responses should read");
        assert_eq!(&responses, b"okok");
        client.shutdown().await.expect("client should close");
    };

    let (server_result, ()) = tokio::join!(server, client);
    server_result.expect("local connection should close cleanly");
    assert_eq!(calls.get(), 2);
}

#[tokio::test]
async fn streaming_handler_timeout_does_not_wait_for_incomplete_body() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn(|_context: TcpRequestContext<PhaseProtocol>| async move {
            std::future::pending::<Result<_, NacelleError>>().await
        }),
        NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Streaming),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_max_request_body_bytes(16)
                .with_handler_timeout(Duration::from_millis(20)),
        ),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client
        .write_all(&[4, b'a'])
        .await
        .expect("partial body should write");

    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("handler timeout should finish the connection")
        .expect("server task should join");
    assert!(matches!(result, Err(NacelleError::Timeout("handler"))));
}

#[tokio::test]
async fn streaming_body_eof_cancels_handler_and_connection() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let handler_started = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn({
            let handler_started = handler_started.clone();
            move |_context: TcpRequestContext<PhaseProtocol>| {
                let handler_started = handler_started.clone();
                async move {
                    handler_started.store(true, Ordering::SeqCst);
                    std::future::pending::<Result<_, NacelleError>>().await
                }
            }
        }),
        NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Streaming),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(16)),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client
        .write_all(&[4, b'a'])
        .await
        .expect("partial body should write");
    tokio::time::timeout(Duration::from_millis(250), async {
        while !handler_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler should start before client closes");
    client
        .shutdown()
        .await
        .expect("client should close write half");

    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("body EOF should finish the connection")
        .expect("server task should join");
    assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
    assert!(handler_started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn request_metrics_use_protocol_wire_byte_hook() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let observed_wire_bytes = Arc::new(AtomicUsize::new(0));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: Some(observed_wire_bytes.clone()),
            encoder_writes_then_errors: false,
        }),
        handler_fn(|context: TcpRequestContext<PhaseProtocol>| async move {
            context.respond(TcpResponse::bytes("ok")).await
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[0]).await.expect("request should write");
    client.shutdown().await.expect("request side should close");
    let mut response = [0_u8; 2];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .expect("response should arrive")
        .expect("response should read");

    assert_eq!(&response, b"ok");
    assert_eq!(observed_wire_bytes.load(Ordering::SeqCst), 1);
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());
}

#[tokio::test]
async fn response_encoder_error_rolls_back_partial_staging() {
    let (mut client, server_io) = tokio::io::duplex(8);
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: true,
        }),
        handler_fn(|context: TcpRequestContext<PhaseProtocol>| async move {
            context.respond(TcpResponse::bytes("partial")).await
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[0]).await.expect("request should write");
    client.shutdown().await.expect("request side should close");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut response))
        .await
        .expect("connection should close")
        .expect("response should read");

    assert!(response.is_empty());
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("test response encoder"))
    ));
}

#[tokio::test]
async fn multi_chunk_response_progresses_with_tiny_socket_capacity() {
    const CHUNKS: usize = 64;
    const CHUNK: &[u8] = b"data";

    let (mut client, server_io) = tokio::io::duplex(1);
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn(|context: TcpRequestContext<PhaseProtocol>| async move {
            let (body_tx, body) = NacelleBody::channel(1);
            tokio::spawn(async move {
                for _ in 0..CHUNKS {
                    body_tx
                        .send(Ok(Bytes::from_static(CHUNK)))
                        .await
                        .expect("response body receiver should remain open");
                }
            });
            context.respond(TcpResponse::new(body)).await
        }),
        NacelleTcpConfig::default().with_response_buffer_capacity(1),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[0]).await.expect("request should write");
    client.shutdown().await.expect("request side should close");
    let mut response = vec![0_u8; CHUNKS * CHUNK.len()];
    tokio::time::timeout(Duration::from_secs(1), client.read_exact(&mut response))
        .await
        .expect("streamed response should make progress")
        .expect("response should read");

    assert_eq!(response, CHUNK.repeat(CHUNKS));
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());
}

#[tokio::test]
async fn first_streaming_response_chunk_is_written_before_body_eof() {
    let (mut client, server_io) = tokio::io::duplex(1);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let release_rx = Arc::new(std::sync::Mutex::new(Some(release_rx)));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn(move |context: TcpRequestContext<PhaseProtocol>| {
            let release_rx = release_rx
                .lock()
                .expect("release receiver lock poisoned")
                .take()
                .expect("test handles one request");
            async move {
                let (body_tx, body) = NacelleBody::channel(1);
                body_tx
                    .send(Ok(Bytes::from_static(b"first")))
                    .await
                    .expect("first chunk should queue");
                tokio::spawn(async move {
                    let _ = release_rx.await;
                    drop(body_tx);
                });
                context.respond(TcpResponse::new(body)).await
            }
        }),
        NacelleTcpConfig::default()
            .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[0]).await.expect("request should write");
    let mut first = [0_u8; 5];
    tokio::time::timeout(Duration::from_millis(250), client.read_exact(&mut first))
        .await
        .expect("first chunk should arrive before body EOF")
        .expect("first chunk should read");
    assert_eq!(&first, b"first");
    release_tx
        .send(())
        .expect("response producer should be waiting");
    client
        .shutdown()
        .await
        .expect("client should close write half");

    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("server should finish after body EOF")
        .expect("server task should join");
    assert!(result.is_ok());
}

#[tokio::test]
async fn body_error_after_streamed_frame_drops_pending_chunk() {
    let (mut client, server_io) = tokio::io::duplex(4);
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol {
            authenticated: false,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }),
        handler_fn(|context: TcpRequestContext<PhaseProtocol>| async move {
            let (body_tx, body) = NacelleBody::channel(3);
            body_tx
                .send(Ok(Bytes::from_static(b"sent")))
                .await
                .expect("first chunk should queue");
            body_tx
                .send(Ok(Bytes::from_static(b"drop")))
                .await
                .expect("pending chunk should queue");
            body_tx
                .send(Err(NacelleError::InvalidFrame("test body")))
                .await
                .expect("body error should queue");
            context.respond(TcpResponse::new(body)).await
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[0]).await.expect("request should write");
    client.shutdown().await.expect("request side should close");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), client.read_to_end(&mut response))
        .await
        .expect("connection should close after body error")
        .expect("response should read");

    assert_eq!(response, b"sentdrop");
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("test body"))
    ));
}

#[derive(Debug)]
struct OneWayHead;

struct MixedProtocol;

struct MixedDecoder;

impl MessageDecoder for MixedDecoder {
    type Message = DecodedMessage<PhaseRequest, OneWayHead>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.len() < 2 {
            return Ok(None);
        }
        let kind = src[0];
        let body_len = usize::from(src[1]);
        drop(src.split_to(2));
        match kind {
            1 => Ok(Some(DecodedMessage::OneWay(DecodedRequest {
                request: OneWayHead,
                body_len,
            }))),
            2 => Ok(Some(DecodedMessage::Request(DecodedRequest {
                request: PhaseRequest,
                body_len,
            }))),
            _ => Err(NacelleError::InvalidFrame("mixed message kind")),
        }
    }
}

#[test]
fn incomplete_classification_prefix_does_not_consume_input() {
    let mut decoder = MixedDecoder;
    let mut input = BytesMut::from(&[1_u8][..]);
    let before = input.clone();

    let decoded = decoder.decode(&mut input).expect("prefix should decode");

    assert!(decoded.is_none());
    assert_eq!(input, before);
}

impl Protocol for MixedProtocol {
    type Request = PhaseRequest;
    type OneWayRequest = OneWayHead;
    type Response = TcpResponse;
    type ConnectionState = AtomicUsize;
    type Decoder = MixedDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        MixedDecoder
    }

    fn connection_state(&self, _connection: &ConnectionInfo) -> Self::ConnectionState {
        AtomicUsize::new(0)
    }

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        2 + body_len
    }

    fn one_way_wire_bytes(&self, _request: &Self::OneWayRequest, body_len: usize) -> usize {
        2 + body_len
    }

    fn max_one_way_body_bytes(
        &self,
        _request: &Self::OneWayRequest,
        _connection: &ConnectionInfo,
        state: &Self::ConnectionState,
        default_limit: usize,
    ) -> usize {
        if state.load(Ordering::Acquire) == 0 {
            PRE_AUTH_BODY_LIMIT
        } else {
            default_limit
        }
    }

    fn response_context(&self, _req: &Self::Request) -> Self::ResponseContext {}

    fn error_context(&self, _req: &Self::Request) -> Self::ErrorContext {}

    fn apply_response(&self, _context: &mut Self::ResponseContext, _response: &Self::Response) {}

    fn max_response_frame_overhead(&self) -> usize {
        0
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        response.body
    }

    fn encode_response_chunk(
        &self,
        _context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        _context: &mut Self::ResponseContext,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }

    fn encode_error(
        &self,
        _context: Option<&Self::ErrorContext>,
        _error: &NacelleError,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
}

#[tokio::test]
async fn one_way_message_emits_no_bytes_and_preserves_connection_framing() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = TcpServer::<MixedProtocol>::builder()
        .protocol(MixedProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<MixedProtocol>| async move {
                let seen = context.connection().state.fetch_add(1, Ordering::SeqCst);
                context.respond(TcpResponse::bytes(seen.to_string())).await
            },
        ))
        .one_way_handler(handler_fn(
            |mut context: TcpOneWayContext<MixedProtocol>| async move {
                let mut body = Vec::new();
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    body.extend_from_slice(&chunk?);
                }
                assert_eq!(body, b"event");
                context.connection().state.fetch_add(1, Ordering::SeqCst);
                Ok(context.complete())
            },
        ))
        .build()
        .expect("typed mixed server should build");
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[2, 0, 1, 5, b'e', b'v', b'e', b'n', b't', 2, 0])
        .await
        .expect("messages should write");
    let mut response = [0_u8; 2];
    tokio::time::timeout(Duration::from_millis(250), client.read_exact(&mut response))
        .await
        .expect("request response should arrive")
        .expect("response should read");
    assert_eq!(&response, b"02");
    client.shutdown().await.expect("client should close");

    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());
}

#[tokio::test]
async fn one_way_limit_rejects_before_body_read_or_handler_dispatch() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let handler_called = Arc::new(AtomicBool::new(false));
    let server = TcpServer::<MixedProtocol>::builder()
        .protocol(MixedProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<MixedProtocol>| async move {
                context.respond(TcpResponse::empty()).await
            },
        ))
        .one_way_handler(handler_fn({
            let handler_called = handler_called.clone();
            move |context: TcpOneWayContext<MixedProtocol>| {
                handler_called.store(true, Ordering::SeqCst);
                async move { Ok(context.complete()) }
            }
        }))
        .build()
        .expect("typed mixed server should build");
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[1, 3])
        .await
        .expect("one-way head should write");
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should reject without waiting for body bytes")
        .expect("server task should join");

    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit("request_body_bytes"))
    ));
    assert!(!handler_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn tcp_server_with_arc_in_memory_observer_serves_and_records_request_events() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let sink = Arc::new(NacelleInMemoryObserver::new());
    let telemetry = NacelleTelemetry::default().with_observer(sink.clone());

    let server = TcpServer::<MixedProtocol>::builder()
        .protocol(MixedProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<MixedProtocol>| async move {
                let seen = context.connection().state.load(Ordering::SeqCst);
                context.respond(TcpResponse::bytes(seen.to_string())).await
            },
        ))
        .one_way_handler(handler_fn(
            |mut context: TcpOneWayContext<MixedProtocol>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    let _ = chunk?;
                }
                Ok(context.complete())
            },
        ))
        .telemetry(telemetry)
        .build()
        .expect("typed mixed server should build with concrete observer");

    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[2, 0])
        .await
        .expect("request should write");
    let mut response = [0_u8; 1];
    tokio::time::timeout(Duration::from_millis(250), client.read_exact(&mut response))
        .await
        .expect("response should arrive")
        .expect("response should read");
    assert_eq!(&response, b"0");

    client.shutdown().await.expect("client should close");
    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());

    let events = sink.events();
    assert!(events.iter().any(|event| {
        event.kind == nacelle_core::telemetry::NacelleTelemetryEventKind::ConnectionOpened
    }));
    assert!(events.iter().any(|event| {
        event.kind == nacelle_core::telemetry::NacelleTelemetryEventKind::RequestCompleted
    }));
}

struct SerialCounterProtocol;

struct SerialCounterState {
    count: usize,
    in_call: bool,
}

impl Protocol for SerialCounterProtocol {
    type Request = PhaseRequest;
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = SerialCounterState;
    type Decoder = PhaseDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PhaseDecoder
    }

    fn connection_state(&self, _connection: &ConnectionInfo) -> Self::ConnectionState {
        SerialCounterState {
            count: 0,
            in_call: false,
        }
    }

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        1 + body_len
    }

    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
        match *request {}
    }

    fn max_request_body_bytes(
        &self,
        _request: &Self::Request,
        _connection: &ConnectionInfo,
        state: &Self::ConnectionState,
        default_limit: usize,
    ) -> usize {
        if state.count == 0 {
            PRE_AUTH_BODY_LIMIT
        } else {
            default_limit
        }
    }

    fn response_context(&self, _req: &Self::Request) -> Self::ResponseContext {}

    fn error_context(&self, _req: &Self::Request) -> Self::ErrorContext {}

    fn apply_response(&self, _context: &mut Self::ResponseContext, _response: &Self::Response) {}

    fn max_response_frame_overhead(&self) -> usize {
        0
    }

    fn response_body(&self, response: Self::Response) -> NacelleBody {
        response.body
    }

    fn encode_response_chunk(
        &self,
        _context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)
    }

    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }

    fn encode_response_end(
        &self,
        _context: &mut Self::ResponseContext,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }

    fn encode_error(
        &self,
        _context: Option<&Self::ErrorContext>,
        _error: &NacelleError,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
}

struct SerialCounterHandler;

impl SerialTcpHandler<SerialCounterProtocol> for SerialCounterHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, SerialCounterProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<SerialCounterProtocol>, NacelleError> {
        let state = &mut context.connection_mut().state;
        assert!(!state.in_call);
        state.in_call = true;
        tokio::task::yield_now().await;
        let response = state.count.to_string();
        state.count += 1;
        state.in_call = false;
        context.respond(TcpResponse::bytes(response)).await
    }
}

struct DrainSerialHandler {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl SerialTcpHandler<SerialCounterProtocol> for DrainSerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, SerialCounterProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<SerialCounterProtocol>, NacelleError> {
        self.started.notify_one();
        self.release.notified().await;
        context.respond(TcpResponse::bytes("ok")).await
    }
}

#[tokio::test]
async fn serial_state_mutation_is_ordered_and_not_reentrant() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler);
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[0, 0])
        .await
        .expect("requests should write");
    let mut responses = [0_u8; 2];
    client
        .read_exact(&mut responses)
        .await
        .expect("responses should read");
    assert_eq!(&responses, b"01");
    client.shutdown().await.expect("client should close");
    server_task
        .await
        .expect("server task should join")
        .expect("server should close cleanly");
}

#[tokio::test]
async fn serial_state_is_scoped_to_each_connection() {
    let server = SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler);
    let (mut first_client, first_io) = tokio::io::duplex(64);
    let (mut second_client, second_io) = tokio::io::duplex(64);
    let first_server = server.clone();
    let second_server = server.clone();
    let first = tokio::spawn(async move { first_server.serve_io(first_io).await });
    let second = tokio::spawn(async move { second_server.serve_io(second_io).await });

    first_client.write_all(&[0]).await.expect("first request");
    second_client.write_all(&[0]).await.expect("second request");
    let mut first_response = [0_u8; 1];
    let mut second_response = [0_u8; 1];
    first_client
        .read_exact(&mut first_response)
        .await
        .expect("first response");
    second_client
        .read_exact(&mut second_response)
        .await
        .expect("second response");
    assert_eq!(&first_response, b"0");
    assert_eq!(&second_response, b"0");
    drop(first_client);
    drop(second_client);
    first.await.expect("first join").expect("first server");
    second.await.expect("second join").expect("second server");
}

#[tokio::test]
async fn serial_state_transition_changes_next_body_limit() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
        .with_runtime_state(NacelleRuntimeState::new(
            NacelleLimits::default().with_max_request_body_bytes(4),
        ));
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[0, 3, b'h', b'e', b'y'])
        .await
        .expect("requests should write");
    let mut responses = [0_u8; 2];
    client
        .read_exact(&mut responses)
        .await
        .expect("responses should read");
    assert_eq!(&responses, b"01");
    client.shutdown().await.expect("client should close");
    server_task
        .await
        .expect("server task should join")
        .expect("server should close cleanly");
}

struct ResetOnDrop(Arc<AtomicBool>);

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

struct SerialTimeoutHandler {
    in_call: Arc<AtomicBool>,
}

impl SerialTcpHandler<SerialCounterProtocol> for SerialTimeoutHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, SerialCounterProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<SerialCounterProtocol>, NacelleError> {
        assert!(!self.in_call.swap(true, Ordering::SeqCst));
        let _reset = ResetOnDrop(self.in_call.clone());
        tokio::time::sleep(Duration::from_secs(60)).await;
        context.respond(TcpResponse::empty()).await
    }
}

#[tokio::test]
async fn serial_handler_timeout_cancels_mutable_loan_without_reentry() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let in_call = Arc::new(AtomicBool::new(false));
    let server = SerialTcpServer::new(
        SerialCounterProtocol,
        SerialTimeoutHandler {
            in_call: in_call.clone(),
        },
    )
    .with_runtime_state(NacelleRuntimeState::new(
        NacelleLimits::default().with_handler_timeout(Duration::from_millis(10)),
    ));
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client
        .write_all(&[0, 0])
        .await
        .expect("requests should write");
    let result = tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("handler timeout should finish the connection")
        .expect("server task should join");
    assert!(matches!(result, Err(NacelleError::Timeout("handler"))));
    assert!(!in_call.load(Ordering::SeqCst));
}

struct LocalSerialCounterHandler;

impl LocalSerialTcpHandler<SerialCounterProtocol> for LocalSerialCounterHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, SerialCounterProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<SerialCounterProtocol>, NacelleError> {
        let state = &mut context.connection_mut().state;
        let response = state.count.to_string();
        state.count += 1;
        context.respond(TcpResponse::bytes(response)).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shared_and_local_serial_results_match() {
    let (mut shared_client, shared_io) = tokio::io::duplex(64);
    let shared_server = SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler);
    let shared = async move { shared_server.serve_io(shared_io).await };
    let shared_client = async move {
        shared_client
            .write_all(&[0, 0])
            .await
            .expect("shared requests");
        shared_client.shutdown().await.expect("shared shutdown");
        let mut response = Vec::new();
        shared_client
            .read_to_end(&mut response)
            .await
            .expect("shared response");
        response
    };
    let (shared_result, shared_response) = tokio::join!(shared, shared_client);
    shared_result.expect("shared serial server");

    let (mut local_client, local_io) = tokio::io::duplex(64);
    let local_server = LocalSerialTcpServer::new(SerialCounterProtocol, LocalSerialCounterHandler);
    let local = async move { local_server.serve_io(local_io).await };
    let local_client = async move {
        local_client
            .write_all(&[0, 0])
            .await
            .expect("local requests");
        local_client.shutdown().await.expect("local shutdown");
        let mut response = Vec::new();
        local_client
            .read_to_end(&mut response)
            .await
            .expect("local response");
        response
    };
    let (local_result, local_response) = tokio::join!(local, local_client);
    local_result.expect("local serial server");

    assert_eq!(shared_response, b"01");
    assert_eq!(local_response, shared_response);
}

struct SerialMixedHandler;

impl SerialTcpHandler<MixedProtocol> for SerialMixedHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, MixedProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<MixedProtocol>, NacelleError> {
        let state = context.connection_mut().state.get_mut();
        let response = state.to_string();
        *state += 1;
        context.respond(TcpResponse::bytes(response)).await
    }
}

impl SerialTcpOneWayHandler<MixedProtocol> for SerialMixedHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpOneWayContext<'connection, MixedProtocol>,
    ) -> Result<nacelle_core::pipeline::Completed, NacelleError> {
        *context.connection_mut().state.get_mut() += 1;
        Ok(context.complete())
    }
}

impl LocalSerialTcpHandler<MixedProtocol> for SerialMixedHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, MixedProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<MixedProtocol>, NacelleError> {
        let state = context.connection_mut().state.get_mut();
        let response = state.to_string();
        *state += 1;
        context.respond(TcpResponse::bytes(response)).await
    }
}

impl LocalSerialTcpOneWayHandler<MixedProtocol> for SerialMixedHandler {
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpOneWayContext<'connection, MixedProtocol>,
    ) -> Result<nacelle_core::pipeline::Completed, NacelleError> {
        *context.connection_mut().state.get_mut() += 1;
        Ok(context.complete())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn serial_one_way_mutation_emits_no_bytes_in_shared_and_local_modes() {
    let messages = [2, 0, 1, 0, 2, 0];

    let (mut shared_client, shared_io) = tokio::io::duplex(64);
    let shared_server =
        SerialTcpServer::with_handlers(MixedProtocol, SerialMixedHandler, SerialMixedHandler);
    let shared = async move { shared_server.serve_io(shared_io).await };
    let shared_client = async move {
        shared_client
            .write_all(&messages)
            .await
            .expect("shared messages");
        shared_client.shutdown().await.expect("shared shutdown");
        let mut response = Vec::new();
        shared_client
            .read_to_end(&mut response)
            .await
            .expect("shared read");
        response
    };
    let (shared_result, shared_response) = tokio::join!(shared, shared_client);
    shared_result.expect("shared mixed server");

    let (mut local_client, local_io) = tokio::io::duplex(64);
    let local_server =
        LocalSerialTcpServer::with_handlers(MixedProtocol, SerialMixedHandler, SerialMixedHandler);
    let local = async move { local_server.serve_io(local_io).await };
    let local_client = async move {
        local_client
            .write_all(&messages)
            .await
            .expect("local messages");
        local_client.shutdown().await.expect("local shutdown");
        let mut response = Vec::new();
        local_client
            .read_to_end(&mut response)
            .await
            .expect("local read");
        response
    };
    let (local_result, local_response) = tokio::join!(local, local_client);
    local_result.expect("local mixed server");

    assert_eq!(shared_response, b"02");
    assert_eq!(local_response, shared_response);
}

struct LocalOnlyProtocol;

impl Protocol for LocalOnlyProtocol {
    type Request = PhaseRequest;
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = std::rc::Rc<std::cell::Cell<usize>>;
    type Decoder = PhaseDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PhaseDecoder
    }

    fn connection_state(&self, _connection: &ConnectionInfo) -> Self::ConnectionState {
        std::rc::Rc::new(std::cell::Cell::new(0))
    }

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        1 + body_len
    }

    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
        match *request {}
    }

    fn response_context(&self, _req: &Self::Request) -> Self::ResponseContext {}
    fn error_context(&self, _req: &Self::Request) -> Self::ErrorContext {}
    fn apply_response(&self, _context: &mut Self::ResponseContext, _response: &Self::Response) {}
    fn max_response_frame_overhead(&self) -> usize {
        0
    }
    fn response_body(&self, response: Self::Response) -> NacelleBody {
        response.body
    }
    fn encode_response_chunk(
        &self,
        _context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)
    }
    fn encode_response_terminal_chunk(
        &self,
        context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }
    fn encode_response_end(
        &self,
        _context: &mut Self::ResponseContext,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
    fn encode_error(
        &self,
        _context: Option<&Self::ErrorContext>,
        _error: &NacelleError,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
}

struct LocalOnlyHandler;

impl LocalSerialTcpHandler<LocalOnlyProtocol> for LocalOnlyHandler {
    #[allow(clippy::future_not_send)]
    async fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, LocalOnlyProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<LocalOnlyProtocol>, NacelleError> {
        let state = context.connection_mut().state.clone();
        let response = state.get().to_string();
        state.set(state.get() + 1);
        tokio::task::yield_now().await;
        context.respond(TcpResponse::bytes(response)).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn local_serial_accepts_non_send_state_and_future() {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = LocalSerialTcpServer::new(LocalOnlyProtocol, LocalOnlyHandler);
    let server_future = server.serve_io(server_io);
    let client_future = async move {
        client
            .write_all(&[0, 0])
            .await
            .expect("requests should write");
        let mut responses = [0_u8; 2];
        client
            .read_exact(&mut responses)
            .await
            .expect("responses should read");
        client.shutdown().await.expect("client should close");
        responses
    };
    let (server_result, responses) = tokio::join!(server_future, client_future);
    server_result.expect("local serial server should close cleanly");
    assert_eq!(&responses, b"01");
}

#[tokio::test]
async fn shared_serial_listener_serves_and_drains_connection_permits() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener address");
    let runtime_state = NacelleRuntimeState::default();
    let server = Arc::new(
        SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
            .with_runtime_state(runtime_state.clone()),
    );
    let shutdown = NacelleShutdown::new();
    let server_task = tokio::spawn(
        crate::runtime::serve_serial_tcp_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            crate::options::NacelleTcpOptions::default(),
            shutdown.token(),
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

    let mut client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime_state.active_connections() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection should become active");
    client.write_all(&[0]).await.expect("request should write");
    let mut response = [0_u8; 1];
    client
        .read_exact(&mut response)
        .await
        .expect("response should read");
    assert_eq!(&response, b"0");
    assert_eq!(runtime_state.active_connections(), 1);

    shutdown.shutdown();
    drop(client);
    server_task
        .await
        .expect("listener task should join")
        .expect("listener should drain cleanly");
    assert_eq!(runtime_state.active_connections(), 0);
}

#[tokio::test]
async fn graceful_drain_delivers_completed_coalesced_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener address");
    let runtime_state = NacelleRuntimeState::default();
    let observer = NacelleInMemoryObserver::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let server = Arc::new(
        SerialTcpServer::new(
            SerialCounterProtocol,
            DrainSerialHandler {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .with_tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .with_telemetry(NacelleTelemetry::new().with_observer(observer.clone()))
        .with_runtime_state(runtime_state.clone()),
    );
    let shutdown = NacelleShutdown::new();
    let server_task = tokio::spawn(
        crate::runtime::serve_serial_tcp_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            crate::options::NacelleTcpOptions::default(),
            shutdown.token(),
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

    let mut client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    client.write_all(&[0]).await.expect("request should write");
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");
    shutdown.shutdown();
    release.notify_one();

    let mut response = [0_u8; 2];
    client
        .read_exact(&mut response)
        .await
        .expect("completed response should drain");
    assert_eq!(&response, b"ok");
    drop(client);
    server_task
        .await
        .expect("listener task should join")
        .expect("listener should drain cleanly");
    assert_eq!(runtime_state.active_connections(), 0);
    let events = observer.events();
    assert!(
        events
            .iter()
            .any(|event| event.kind == NacelleTelemetryEventKind::DrainCompleted)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != NacelleTelemetryEventKind::DrainTimedOut)
    );
}

#[tokio::test]
async fn forced_drain_deadline_aborts_idle_connection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener address");
    let runtime_state = NacelleRuntimeState::default();
    let observer = NacelleInMemoryObserver::new();
    let server = Arc::new(
        SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
            .with_tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .with_telemetry(NacelleTelemetry::new().with_observer(observer.clone()))
            .with_runtime_state(runtime_state.clone()),
    );
    let shutdown = NacelleShutdown::new();
    let server_task = tokio::spawn(
        crate::runtime::serve_serial_tcp_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            crate::options::NacelleTcpOptions::default(),
            shutdown.token(),
            NacelleDrainDeadline::new(Duration::ZERO),
        ),
    );

    let client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime_state.active_connections() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection should become active");
    shutdown.shutdown();
    server_task
        .await
        .expect("listener task should join")
        .expect("listener should abort after deadline");
    drop(client);

    assert_eq!(runtime_state.active_connections(), 0);
    let events = observer.events();
    assert!(
        events
            .iter()
            .any(|event| event.kind == NacelleTelemetryEventKind::DrainTimedOut)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == NacelleTelemetryEventKind::ConnectionsAborted)
    );
}

#[tokio::test]
async fn shared_serial_listener_records_connection_rejection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener address");
    let runtime_state = NacelleRuntimeState::new(NacelleLimits::default().with_max_connections(1));
    let observer = NacelleInMemoryObserver::new();
    let server = Arc::new(
        SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
            .with_telemetry(NacelleTelemetry::new().with_observer(observer.clone()))
            .with_runtime_state(runtime_state.clone()),
    );
    let shutdown = NacelleShutdown::new();
    let server_task = tokio::spawn(
        crate::runtime::serve_serial_tcp_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            crate::options::NacelleTcpOptions::default(),
            shutdown.token(),
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

    let first = tokio::net::TcpStream::connect(addr)
        .await
        .expect("first client should connect");
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime_state.active_connections() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first connection should acquire the permit");
    let second = tokio::net::TcpStream::connect(addr)
        .await
        .expect("second TCP handshake should complete");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !observer
            .events()
            .iter()
            .any(|event| event.kind == NacelleTelemetryEventKind::ConnectionRejected)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second connection should be rejected by runtime limits");

    drop(second);
    shutdown.shutdown();
    drop(first);
    server_task
        .await
        .expect("listener task should join")
        .expect("listener should drain cleanly");
    assert_eq!(runtime_state.active_connections(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn local_serial_listener_serves_non_send_state_and_drains() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener address");
            let runtime_state = NacelleRuntimeState::default();
            let server = std::rc::Rc::new(
                LocalSerialTcpServer::new(LocalOnlyProtocol, LocalOnlyHandler)
                    .with_runtime_state(runtime_state.clone()),
            );
            let shutdown = NacelleShutdown::new();
            let listener_task =
                tokio::task::spawn_local(crate::runtime::serve_local_serial_tcp_listener(
                    server,
                    listener,
                    crate::options::NacelleTcpOptions::default(),
                    shutdown.token(),
                    NacelleDrainDeadline::new(Duration::from_secs(1)),
                ));

            let mut client = tokio::net::TcpStream::connect(addr)
                .await
                .expect("client should connect");
            client.write_all(&[0]).await.expect("request should write");
            let mut response = [0_u8; 1];
            client
                .read_exact(&mut response)
                .await
                .expect("response should read");
            assert_eq!(&response, b"0");
            assert_eq!(runtime_state.active_connections(), 1);

            shutdown.shutdown();
            drop(client);
            listener_task
                .await
                .expect("listener task should join")
                .expect("listener should drain cleanly");
            assert_eq!(runtime_state.active_connections(), 0);
        })
        .await;
}
