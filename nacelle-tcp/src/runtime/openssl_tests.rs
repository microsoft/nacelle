use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleDrainDeadline;
use nacelle_core::pipeline::local_handler_fn;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_openssl::NacelleOpenSslConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::protocol::{
    DecodedRequest, FrameBuffer, LocalSerialTcpHandler, Protocol, SerialTcpHandler,
    SerialTcpRequestContext, TcpHandlerCompletion, TcpRequestContext, TcpResponse,
};
use crate::serial_server::{LocalSerialTcpServer, SerialTcpServer};
use crate::server::LocalTcpServer;
use crate::server::TcpServer;

#[cfg(feature = "experimental-openssl-detection")]
use super::local::serve_local_serial_tcp_optional_openssl_listener;
use super::local::{serve_local_serial_tcp_openssl_listener, serve_local_tcp_openssl_listener};
use super::openssl::{
    serve_serial_tcp_openssl_listener_with_options_and_shutdown_deadline,
    serve_tcp_openssl_listener_with_shutdown_deadline,
};
#[cfg(feature = "experimental-openssl-detection")]
use super::openssl_optional::{
    peeked_bytes_can_be_tls,
    serve_serial_tcp_optional_openssl_listener_with_options_and_shutdown_deadline,
};

#[derive(Debug)]
struct PlainRequest {
    head_len: usize,
}

struct PlainProtocol;

struct PlainDecoder;

impl MessageDecoder for PlainDecoder {
    type Message = DecodedRequest<PlainRequest>;
    type Error = NacelleError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let head_len = src.len();
        src.clear();
        Ok(Some(DecodedRequest {
            request: PlainRequest { head_len },
            body_len: 0,
        }))
    }
}

impl Protocol for PlainProtocol {
    type Request = PlainRequest;
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
}

#[test]
#[cfg(feature = "experimental-openssl-detection")]
fn tls_detection_accepts_tls_handshake_prefix() {
    assert!(peeked_bytes_can_be_tls(&[0x16]));
    assert!(peeked_bytes_can_be_tls(&[0x16, 0x03]));
    assert!(peeked_bytes_can_be_tls(&[0x16, 0x03, 0x03]));
}

#[test]
#[cfg(feature = "experimental-openssl-detection")]
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

struct SerialHandler;

impl SerialTcpHandler<PlainProtocol> for SerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, PlainProtocol>,
    ) -> Result<TcpHandlerCompletion<PlainProtocol>, NacelleError> {
        context.respond(TcpResponse::bytes("ok")).await
    }
}

#[cfg(feature = "policies")]
#[tokio::test]
async fn openssl_reload_after_accepted_admission_uses_current_certificate() {
    use nacelle_core::policy::{Admission, Rejection};
    use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
    use tokio::sync::Notify;

    struct WaitAccepted {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Admission<crate::policies::TcpTransport<'_>> for WaitAccepted {
        type Lease = ();

        async fn enter<'a>(
            &'a self,
            _: &'a mut crate::policies::TcpTransport<'_>,
        ) -> Result<(), Rejection> {
            self.entered.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[cfg(not(feature = "experimental-openssl-detection"))]
    let modes = ["required"];
    #[cfg(feature = "experimental-openssl-detection")]
    let modes = ["required", "optional"];
    for mode in modes {
        let config = test_open_ssl_config();
        let replacement = test_open_ssl_config().acceptor();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let server = Arc::new(
            TcpServer::<PlainProtocol>::builder()
                .protocol(PlainProtocol)
                .handler(handler_fn(
                    |context: TcpRequestContext<PlainProtocol>| async move {
                        context.respond(TcpResponse::bytes("ok")).await
                    },
                ))
                .policies(crate::TcpPolicies::new().on_accepted(WaitAccepted {
                    entered: entered.clone(),
                    release: release.clone(),
                }))
                .build()
                .expect("server"),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
        let listener_config = config.clone();
        let task = tokio::spawn(async move {
            #[cfg(feature = "experimental-openssl-detection")]
            if mode == "optional" {
                return super::openssl_optional::serve_tcp_optional_openssl_listener_with_options_and_shutdown_deadline(
                    server,
                    listener,
                    listener_config,
                    crate::NacelleTcpOptions::default(),
                    crate::NacelleTlsDetectionOptions::default(),
                    token,
                    NacelleDrainDeadline::new(Duration::ZERO),
                ).await;
            }
            serve_tcp_openssl_listener_with_shutdown_deadline(
                server,
                listener,
                listener_config,
                token,
                NacelleDrainDeadline::new(Duration::ZERO),
            )
            .await
        });
        let stream = tokio::net::TcpStream::connect(addr).await.expect("client");
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("accepted admission");
        config.replace_acceptor(replacement.as_ref().clone());
        release.notify_one();

        let mut connector = SslConnector::builder(SslMethod::tls()).expect("connector");
        connector.set_verify(SslVerifyMode::NONE);
        let ssl = connector
            .build()
            .configure()
            .expect("configuration")
            .verify_hostname(false)
            .into_ssl("localhost")
            .expect("client TLS");
        let mut client = tokio_openssl::SslStream::new(ssl, stream).expect("TLS stream");
        tokio::time::timeout(
            Duration::from_secs(2),
            std::pin::Pin::new(&mut client).connect(),
        )
        .await
        .expect("handshake timeout")
        .expect("handshake");
        let certificate = client.ssl().peer_certificate().expect("peer certificate");
        assert!(
            certificate.as_ref()
                == replacement
                    .context()
                    .certificate()
                    .expect("replacement certificate"),
            "{mode}: handshake must use the replacement certificate"
        );
        client.write_all(b"request").await.expect("request");
        let mut response = [0; 2];
        client.read_exact(&mut response).await.expect("response");
        assert_eq!(&response, b"ok");
        shutdown.shutdown();
        task.await.expect("listener join").expect("listener");
    }
}

#[tokio::test]
async fn serial_required_openssl_accepts_tls_request() {
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
    let server = Arc::new(SerialTcpServer::new(PlainProtocol, SerialHandler));
    let server_task = tokio::spawn(
        serve_serial_tcp_openssl_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            config,
            crate::NacelleTcpOptions::default(),
            token,
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

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
}

#[tokio::test]
#[cfg(feature = "experimental-openssl-detection")]
async fn serial_optional_openssl_preserves_plaintext_request() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener should have addr");
    let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
    let server = Arc::new(SerialTcpServer::new(PlainProtocol, SerialHandler));
    let server_task = tokio::spawn(
        serve_serial_tcp_optional_openssl_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            test_open_ssl_config(),
            crate::NacelleTcpOptions::default(),
            crate::NacelleTlsDetectionOptions::default(),
            token,
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

    let mut client = tokio::net::TcpStream::connect(addr)
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
}

#[tokio::test]
#[cfg(feature = "experimental-openssl-detection")]
async fn serial_optional_openssl_accepts_tls_request() {
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
    let server = Arc::new(SerialTcpServer::new(PlainProtocol, SerialHandler));
    let server_task = tokio::spawn(
        serve_serial_tcp_optional_openssl_listener_with_options_and_shutdown_deadline(
            server,
            listener,
            config,
            crate::NacelleTcpOptions::default(),
            crate::NacelleTlsDetectionOptions::default(),
            token,
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ),
    );

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

struct LocalSerialHandler;

impl LocalSerialTcpHandler<PlainProtocol> for LocalSerialHandler {
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<'connection, PlainProtocol>,
    ) -> Result<TcpHandlerCompletion<PlainProtocol>, NacelleError> {
        context.respond(TcpResponse::bytes("ok")).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn local_serial_required_openssl_accepts_tls_request() {
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
            let server =
                std::rc::Rc::new(LocalSerialTcpServer::new(PlainProtocol, LocalSerialHandler));
            let server_task = tokio::task::spawn_local(serve_local_serial_tcp_openssl_listener(
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

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "experimental-openssl-detection")]
async fn local_serial_optional_openssl_preserves_plaintext_request() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener should have addr");
            let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
            let server =
                std::rc::Rc::new(LocalSerialTcpServer::new(PlainProtocol, LocalSerialHandler));
            let server_task =
                tokio::task::spawn_local(serve_local_serial_tcp_optional_openssl_listener(
                    server,
                    listener,
                    crate::NacelleTcpOptions::default(),
                    test_open_ssl_config(),
                    crate::NacelleTlsDetectionOptions::default(),
                    token,
                    NacelleDrainDeadline::new(Duration::from_secs(1)),
                ));

            let mut client = tokio::net::TcpStream::connect(addr)
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
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
#[cfg(feature = "experimental-openssl-detection")]
async fn local_serial_optional_openssl_accepts_tls_request() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let addr = listener.local_addr().expect("listener should have addr");
            let mut connector = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls())
                .expect("connector builder");
            connector.set_verify(openssl::ssl::SslVerifyMode::NONE);
            let connector = connector.build();
            let (shutdown, token) = nacelle_core::lifecycle::NacelleShutdown::pair();
            let server =
                std::rc::Rc::new(LocalSerialTcpServer::new(PlainProtocol, LocalSerialHandler));
            let server_task =
                tokio::task::spawn_local(serve_local_serial_tcp_optional_openssl_listener(
                    server,
                    listener,
                    crate::NacelleTcpOptions::default(),
                    test_open_ssl_config(),
                    crate::NacelleTlsDetectionOptions::default(),
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

pub(crate) fn test_open_ssl_config() -> NacelleOpenSslConfig {
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
