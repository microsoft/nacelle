use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::config::NacelleTcpConfig;
use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::handler_fn;
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
use nacelle_core::request::{
    NacelleConnectionMeta, NacelleRequest, RequestMetadata, TcpRequestMeta,
};
use nacelle_core::response::NacelleResponse;
use nacelle_core::telemetry::NacelleTelemetry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{DecodedRequest, Protocol};

use super::serve_stream_with_connection_meta;

const PRE_AUTH_BODY_LIMIT: usize = 2;

struct AuthState {
    authenticated: AtomicBool,
}

#[derive(Debug)]
struct PhaseRequest {
    body_len: usize,
}

impl RequestMetadata for PhaseRequest {
    fn opcode(&self) -> u64 {
        1
    }

    fn max_body_bytes(&self, connection: &NacelleConnectionMeta, default_limit: usize) -> usize {
        let authenticated = connection
            .extension::<AuthState>()
            .is_some_and(|state| state.authenticated.load(Ordering::SeqCst));
        if authenticated {
            default_limit
        } else {
            PRE_AUTH_BODY_LIMIT
        }
    }

    fn tcp_meta(&self, _body_len: usize) -> TcpRequestMeta {
        TcpRequestMeta {
            request_id: None,
            opcode: 1,
            flags: 0,
            body_len: self.body_len,
        }
    }
}

struct PhaseProtocol;

struct PhaseDecoder;

impl MessageDecoder for PhaseDecoder {
    type Message = DecodedRequest<PhaseRequest>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let body_len = src[0] as usize;
        let _head = src.split_to(1);
        Ok(Some(DecodedRequest {
            request: PhaseRequest { body_len },
            body_len,
        }))
    }
}

impl Protocol<PhaseRequest> for PhaseProtocol {
    type Decoder = PhaseDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PhaseDecoder
    }

    fn response_context(&self, _req: &PhaseRequest) -> Self::ResponseContext {}

    fn error_context(&self, _req: &PhaseRequest) -> Self::ErrorContext {}

    fn encode_response_chunk(
        &self,
        _context: &mut Self::ResponseContext,
        chunk: Bytes,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk);
        Ok(())
    }

    fn encode_response_end(
        &self,
        _context: &mut Self::ResponseContext,
        _dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        Ok(())
    }

    fn encode_error(
        &self,
        _context: Option<&Self::ErrorContext>,
        error: &NacelleError,
        dst: &mut BytesMut,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(error.to_string().as_bytes());
        Ok(())
    }
}

#[tokio::test]
async fn phase_limit_rejects_unauthenticated_body_before_reading_body() {
    let (mut client, server_io) = tokio::io::duplex(1024);
    let handler_called = Arc::new(AtomicBool::new(false));
    let server_task = tokio::spawn(serve_stream_with_connection_meta(
        server_io,
        Arc::new(PhaseProtocol),
        handler_fn({
            let handler_called = handler_called.clone();
            move |_request: NacelleRequest| {
                handler_called.store(true, Ordering::SeqCst);
                async move { Ok(NacelleResponse::empty_tcp()) }
            }
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        NacelleConnectionMeta::tcp(None, None).with_extension(AuthState {
            authenticated: AtomicBool::new(false),
        }),
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
        Arc::new(PhaseProtocol),
        handler_fn({
            let handler_called = handler_called.clone();
            move |mut request: NacelleRequest| {
                let handler_called = handler_called.clone();
                async move {
                    handler_called.store(true, Ordering::SeqCst);
                    let mut body = Vec::new();
                    while let Some(chunk) = request.body.next_chunk().await {
                        body.extend_from_slice(&chunk?);
                    }
                    assert_eq!(body, b"hey");
                    Ok(NacelleResponse::tcp_bytes("ok"))
                }
            }
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(4)),
        NacelleConnectionMeta::tcp(None, None).with_extension(AuthState {
            authenticated: AtomicBool::new(true),
        }),
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
