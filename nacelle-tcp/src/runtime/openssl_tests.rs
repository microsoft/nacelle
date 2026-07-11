use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::handler_fn;
use nacelle_core::lifecycle::NacelleDrainDeadline;
use nacelle_core::request::{NacelleRequest, TcpRequestMeta};
use nacelle_core::response::NacelleResponse;
use nacelle_core::tls::NacelleOpenSslConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{DecodedRequest, Protocol};
use crate::server::TcpServer;

use super::openssl::serve_tcp_openssl_listener_with_shutdown_deadline;
use super::openssl_optional::peeked_bytes_can_be_tls;

#[derive(Debug)]
struct PlainRequest;

struct PlainProtocol;

struct PlainDecoder;

impl MessageDecoder for PlainDecoder {
    type Message = DecodedRequest<PlainRequest>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        src.clear();
        Ok(Some(DecodedRequest {
            request: PlainRequest,
            body_len: 0,
        }))
    }
}

impl Protocol for PlainProtocol {
    type Request = PlainRequest;
    type Decoder = PlainDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        PlainDecoder
    }

    fn request_meta(&self, _request: &Self::Request, body_len: usize) -> TcpRequestMeta {
        TcpRequestMeta {
            request_id: None,
            opcode: 1,
            flags: 0,
            body_len,
        }
    }

    fn response_context(&self, _req: &PlainRequest) -> Self::ResponseContext {}

    fn error_context(&self, _req: &PlainRequest) -> Self::ErrorContext {}

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
        _error: &NacelleError,
        _dst: &mut BytesMut,
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
    let server = TcpServer::<PlainRequest, ()>::builder()
        .protocol(PlainProtocol)
        .handler(handler_fn({
            let handler_called = handler_called.clone();
            move |_request: NacelleRequest| {
                handler_called.store(true, Ordering::SeqCst);
                async move { Ok(NacelleResponse::empty_tcp()) }
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
