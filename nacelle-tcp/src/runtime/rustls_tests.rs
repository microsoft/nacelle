use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleDrainDeadline;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{DecodedRequest, FrameBuffer, Protocol, TcpRequestContext, TcpResponse};
use crate::server::TcpServer;

use super::rustls::serve_tcp_tls_listener_with_shutdown_deadline;

#[derive(Debug)]
struct TestRequest;

struct TestProtocol;

struct TestDecoder;

impl MessageDecoder for TestDecoder {
    type Message = DecodedRequest<TestRequest>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        drop(src.split_to(1));
        Ok(Some(DecodedRequest {
            request: TestRequest,
            body_len: 0,
        }))
    }
}

impl Protocol for TestProtocol {
    type Request = TestRequest;
    type Response = TcpResponse;
    type ConnectionState = ();
    type Decoder = TestDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        TestDecoder
    }

    fn connection_state(&self, _: &ConnectionInfo) {}

    fn request_wire_bytes(&self, _request: &Self::Request, body_len: usize) -> usize {
        1 + body_len
    }

    fn response_context(&self, _req: &TestRequest) -> Self::ResponseContext {}

    fn error_context(&self, _req: &TestRequest) -> Self::ErrorContext {}

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
        _error: &NacelleError,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
}

#[tokio::test]
async fn tcp_tls_self_signed_server_accepts_request() {
    let generated =
        nacelle_core::tls::NacelleTlsConfig::self_signed(["localhost"]).expect("self-signed tls");
    let certificate =
        nacelle_core::tls::parse_pem_certificates(generated.certificate_pem.as_bytes())
            .expect("certificate should parse")
            .remove(0);
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("root cert should add");
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener should have addr");
    let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    let server = TcpServer::<TestProtocol>::builder()
        .protocol(TestProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<TestProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .build()
        .expect("server should build");
    let server_task = tokio::spawn(serve_tcp_tls_listener_with_shutdown_deadline(
        Arc::new(server),
        listener,
        generated.tls_config,
        token,
        NacelleDrainDeadline::new(Duration::from_millis(25)),
    ));

    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    let server_name =
        rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");
    let mut client = connector
        .connect(server_name, stream)
        .await
        .expect("tls should connect");
    client
        .write_all(&[0x01])
        .await
        .expect("request should write");
    let mut response = [0_u8; 2];
    client
        .read_exact(&mut response)
        .await
        .expect("response should read");
    assert_eq!(&response, b"ok");

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should stop")
        .expect("server task should join")
        .expect("server should exit");
}
