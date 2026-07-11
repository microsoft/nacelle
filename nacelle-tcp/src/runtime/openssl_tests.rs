use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleDrainDeadline;
use nacelle_core::pipeline::local_handler_fn;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_core::tls::NacelleOpenSslConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{
    DecodedMessage, DecodedRequest, FrameBuffer, Protocol, TcpRequestContext, TcpResponse,
};
use crate::server::LocalTcpServer;
use crate::server::TcpServer;

use super::local::serve_local_tcp_openssl_listener;
use super::openssl::serve_tcp_openssl_listener_with_shutdown_deadline;
use super::openssl_optional::peeked_bytes_can_be_tls;

#[derive(Debug)]
struct PlainRequest {
    head_len: usize,
}

struct PlainProtocol;

struct PlainDecoder;

impl MessageDecoder for PlainDecoder {
    type Message = DecodedMessage<PlainRequest, Infallible>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let head_len = src.len();
        src.clear();
        Ok(Some(DecodedMessage::Request(DecodedRequest {
            request: PlainRequest { head_len },
            body_len: 0,
        })))
    }
}

impl Protocol for PlainProtocol {
    type Request = PlainRequest;
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = ();
    type Decoder = PlainDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PlainDecoder
    }

    fn connection_state(&self, _: &ConnectionInfo) {}

    fn request_wire_bytes(&self, request: &Self::Request, body_len: usize) -> usize {
        request.head_len + body_len
    }

    fn one_way_wire_bytes(&self, request: &Self::OneWayRequest, _body_len: usize) -> usize {
        match *request {}
    }

    fn response_context(&self, _req: &PlainRequest) -> Self::ResponseContext {}

    fn error_context(&self, _req: &PlainRequest) -> Self::ErrorContext {}

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

#[test]
fn tls_detection_accepts_tls_handshake_prefix() {
    assert!(peeked_bytes_can_be_tls(&[0x16]));
    assert!(peeked_bytes_can_be_tls(&[0x16, 0x03]));
    assert!(peeked_bytes_can_be_tls(&[0x16, 0x03, 0x03]));
}

#[test]
fn tls_detection_rejects_plain_protocol_prefix() {
    assert!(!peeked_bytes_can_be_tls(&[0x01]));
    assert!(!peeked_bytes_can_be_tls(&[0x16, 0x02]));
    assert!(!peeked_bytes_can_be_tls(&[0x16, 0x03, 0x05]));
}

#[tokio::test]
async fn required_openssl_rejects_plaintext_before_handler() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener should have addr");
    let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    let handler_called = Arc::new(AtomicBool::new(false));
    let server = TcpServer::<PlainProtocol>::builder()
        .protocol(PlainProtocol)
        .handler(handler_fn({
            let handler_called = handler_called.clone();
            move |context: TcpRequestContext<PlainProtocol>| {
                handler_called.store(true, Ordering::SeqCst);
                async move { context.respond(TcpResponse::empty()).await }
            }
        }))
        .build()
        .expect("server should build");
    let server_task = tokio::spawn(serve_tcp_openssl_listener_with_shutdown_deadline(
        Arc::new(server),
        listener,
        test_open_ssl_config(),
        token,
        NacelleDrainDeadline::new(Duration::from_millis(25)),
    ));

    let mut client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");
    client
        .write_all(&[0x01, 0x02, 0x03])
        .await
        .expect("plaintext should write");
    let mut response = [0_u8; 1];
    let _ = tokio::time::timeout(Duration::from_millis(500), client.read(&mut response)).await;

    assert!(!handler_called.load(Ordering::SeqCst));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should stop")
        .expect("server task should join")
        .expect("server should exit");
}

#[tokio::test(flavor = "current_thread")]
async fn local_required_openssl_accepts_tls_request() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener should have addr");
            let config = test_open_ssl_config();
            let mut connector = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
                .expect("connector builder");
            connector.set_verify(openssl::ssl::SslVerifyMode::NONE);
            let connector = connector.build();
            let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
            let server = std::rc::Rc::new(LocalTcpServer::new(
                PlainProtocol,
                local_handler_fn(|context: TcpRequestContext<PlainProtocol>| async move {
                    context.respond(TcpResponse::bytes("ok")).await
                }),
            ));
            let server_task = tokio::task::spawn_local(serve_local_tcp_openssl_listener(
                server,
                listener,
                crate::NacelleTcpOptions::default(),
                config,
                token,
                NacelleDrainDeadline::new(Duration::from_secs(1)),
            ));

            let stream = tokio::net::TcpStream::connect(addr)
                .await
                .expect("client should connect");
            let ssl = connector
                .configure()
                .expect("connector config")
                .verify_hostname(false)
                .into_ssl("localhost")
                .expect("client ssl");
            let mut client = tokio_openssl::SslStream::new(ssl, stream).expect("ssl stream");
            std::pin::Pin::new(&mut client)
                .connect()
                .await
                .expect("TLS handshake");
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
        })
        .await;
}

fn test_open_ssl_config() -> NacelleOpenSslConfig {
    use openssl::asn1::Asn1Time;
    use openssl::bn::BigNum;
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::rsa::Rsa;
    use openssl::ssl::{SslAcceptor, SslMethod};
    use openssl::x509::{X509, X509NameBuilder};

    let rsa = Rsa::generate(2048).expect("rsa key should generate");
    let private_key = PKey::from_rsa(rsa).expect("pkey should build");
    let mut name = X509NameBuilder::new().expect("name should build");
    name.append_entry_by_text("CN", "localhost")
        .expect("common name should set");
    let name = name.build();
    let serial = BigNum::from_u32(1)
        .expect("serial should build")
        .to_asn1_integer()
        .expect("serial should convert");
    let mut certificate = X509::builder().expect("certificate should build");
    certificate.set_version(2).expect("version should set");
    certificate
        .set_serial_number(&serial)
        .expect("serial should set");
    certificate
        .set_subject_name(&name)
        .expect("subject should set");
    certificate
        .set_issuer_name(&name)
        .expect("issuer should set");
    certificate
        .set_pubkey(&private_key)
        .expect("public key should set");
    certificate
        .set_not_before(Asn1Time::days_from_now(0).expect("not before").as_ref())
        .expect("not before should set");
    certificate
        .set_not_after(Asn1Time::days_from_now(1).expect("not after").as_ref())
        .expect("not after should set");
    certificate
        .sign(&private_key, MessageDigest::sha256())
        .expect("certificate should sign");
    let certificate = certificate.build();

    let mut acceptor =
        SslAcceptor::mozilla_intermediate(SslMethod::tls_server()).expect("acceptor should build");
    acceptor
        .set_private_key(&private_key)
        .expect("private key should set");
    acceptor
        .set_certificate(&certificate)
        .expect("certificate should set");
    acceptor
        .check_private_key()
        .expect("private key should match");
    NacelleOpenSslConfig::from_acceptor(acceptor.build())
}
