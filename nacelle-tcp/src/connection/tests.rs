use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::config::{NacelleTcpConfig, TcpRequestBodyMode};
use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::NacelleTelemetry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{
    DecodedMessage, DecodedRequest, FrameBuffer, Protocol, TcpOneWayContext, TcpRequestContext,
    TcpResponse,
};
use crate::server::TcpServer;

use super::serve_stream_with_connection_meta;

const PRE_AUTH_BODY_LIMIT: usize = 2;

#[derive(Debug)]
struct PhaseRequest;

#[derive(Debug)]
struct AuthState {
    authenticated: bool,
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
            authenticated: self.authenticated,
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
        _connection: &NacelleConnectionMeta,
        default_limit: usize,
    ) -> usize {
        if self.authenticated {
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
                    assert!(auth_state.authenticated);
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
        NacelleTcpConfig::default(),
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
                let seen = context.connection().state.load(Ordering::SeqCst);
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
        .write_all(&[1, 5, b'e', b'v', b'e', b'n', b't', 2, 0])
        .await
        .expect("messages should write");
    let mut response = [0_u8; 1];
    tokio::time::timeout(Duration::from_millis(250), client.read_exact(&mut response))
        .await
        .expect("request response should arrive")
        .expect("response should read");
    assert_eq!(&response, b"1");
    client.shutdown().await.expect("client should close");

    let result = tokio::time::timeout(Duration::from_millis(250), server_task)
        .await
        .expect("server should finish")
        .expect("server task should join");
    assert!(result.is_ok());
}
