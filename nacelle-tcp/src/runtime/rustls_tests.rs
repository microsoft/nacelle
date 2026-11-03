use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::NacelleDrainDeadline;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::connection::tests::{registered, tracked_runtime};
use crate::protocol::{DecodedRequest, FrameBuffer, Protocol, TcpRequestContext, TcpResponse};
use crate::server::TcpServer;

use super::rustls::serve_tcp_tls_listener_with_shutdown_deadline;

pub(super) fn test_certificate(pem: &[u8]) -> rustls::pki_types::CertificateDer<'static> {
    let parsed = nacelle_rustls::parse_pem_certificates(pem);
    let certificates = match parsed {
        Ok(certificates) => certificates,
        Err(_) => panic!("test certificate should parse"),
    };
    match certificates.into_iter().next() {
        Some(certificate) => certificate,
        None => panic!("test certificate should be present"),
    }
}

#[cfg(panic = "unwind")]
#[test]
fn test_certificate_failure_does_not_include_parser_details() {
    for pem in [
        b"".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nSENSITIVE_TEST_INPUT!\n-----END CERTIFICATE-----",
    ] {
        let outcome = std::panic::catch_unwind(|| test_certificate(pem));
        let payload = match outcome {
            Err(payload) => payload,
            Ok(_) => panic!("invalid test certificate should fail"),
        };
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
        assert!(
            message == Some("test certificate should parse"),
            "certificate failure must contain only the fixed diagnostic"
        );
    }
}

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
}

#[tokio::test]
async fn tcp_tls_self_signed_server_accepts_request() {
    let runtime = tracked_runtime(8);
    let generated =
        nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).expect("self-signed tls");
    let certificate = test_certificate(generated.certificate_pem.as_bytes());
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
        .runtime_state(runtime.clone())
        .unwrap()
        .handler(handler_fn(
            |context: TcpRequestContext<TestProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .tcp_config(
            crate::NacelleTcpConfig::default()
                .with_head_read_limits(crate::TcpHeadReadLimits::new(1, 16).unwrap()),
        )
        .admission(
            nacelle_core::AllowAll,
            nacelle_core::AdmissionConfig::new(Duration::from_secs(1), 8).unwrap(),
        )
        .unwrap()
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
    let snapshot = registered(&runtime).await;
    assert!(snapshot.connection.tls.is_some());
    assert_eq!(snapshot.connection.local_addr, Some(addr));

    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .expect("server should stop")
        .expect("server task should join")
        .expect("server should exit");
    assert_eq!(runtime.active_connections(), 0);
    assert!(runtime.control().connections(8).unwrap().is_empty());
}

#[tokio::test]
async fn tracked_tls_setup_is_busy_and_cleans_up_close_failure_timeout_and_abort() {
    use nacelle_core::{
        NacelleCloseOutcome, NacelleConnectionActivityState, NacelleReclaimOutcome,
    };
    for mode in ["close", "disconnect", "timeout", "abort"] {
        let runtime = tracked_runtime(1);
        let control = runtime.control();
        let generated = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown, token) = nacelle_core::NacelleShutdown::pair();
        let observer = nacelle_core::NacelleInMemoryObserver::new();
        let server = TcpServer::<TestProtocol>::builder()
            .protocol(TestProtocol)
            .runtime_state(runtime.clone())
            .unwrap()
            .handler(handler_fn(|_: TcpRequestContext<TestProtocol>| async {
                Err::<crate::protocol::TcpHandlerCompletion<TestProtocol>, _>(
                    NacelleError::InvalidFrame("unexpected handler dispatch before TLS setup"),
                )
            }))
            .build()
            .unwrap()
            .with_telemetry(
                nacelle_core::NacelleTelemetry::default().with_observer(observer.clone()),
            );
        let task = tokio::spawn(serve_tcp_tls_listener_with_shutdown_deadline(
            Arc::new(server),
            listener,
            generated
                .tls_config
                .with_handshake_timeout(Duration::from_millis(100)),
            token,
            NacelleDrainDeadline::new(Duration::ZERO),
        ));
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let snapshot = registered(&runtime).await;
        assert_eq!(snapshot.activity, NacelleConnectionActivityState::Setup);
        control.set_max_connections(Some(0)).unwrap();
        assert!(matches!(
            control.reclaim_idle(1),
            NacelleReclaimOutcome::Observed {
                selected: 0,
                unfilled: 1,
                ..
            }
        ));
        match mode {
            "close" => {
                assert_eq!(
                    control.close_connection(snapshot.connection.connection_id),
                    NacelleCloseOutcome::Selected
                );
            }
            "disconnect" => {
                drop(client);
            }
            "abort" => {
                shutdown.shutdown();
            }
            _ => {}
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while !control.connections(1).unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.shutdown();
        task.await.unwrap().unwrap();
        assert_eq!(runtime.active_connections(), 0);
        assert!(observer.events().iter().all(|event| !matches!(
            event.kind,
            nacelle_core::NacelleTelemetryEventKind::ConnectionOpened
                | nacelle_core::NacelleTelemetryEventKind::ConnectionClosed
        )));
    }
}

#[tokio::test]
async fn tcp_head_budget_caps_plaintext_returned_by_tls() {
    let generated =
        nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).expect("self-signed tls");
    let certificate = test_certificate(generated.certificate_pem.as_bytes());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("root certificate");
    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let acceptor = tokio_rustls::TlsAcceptor::from(generated.tls_config.server_config());
    let (client_io, server_io) = tokio::io::duplex(128 * 1024);
    let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let written = Arc::new(tokio::sync::Notify::new());
    let server = TcpServer::<TestProtocol>::builder()
        .protocol(TestProtocol)
        .tcp_config(
            crate::NacelleTcpConfig::default()
                .with_head_read_limits(crate::TcpHeadReadLimits::new(1, 16).expect("head limits")),
        )
        .handler(handler_fn({
            let written = written.clone();
            move |_: TcpRequestContext<TestProtocol>| {
                let written = written.clone();
                async move {
                    written.notified().await;
                    Err::<crate::protocol::TcpHandlerCompletion<TestProtocol>, _>(
                        NacelleError::InvalidFrame("test stop after head"),
                    )
                }
            }
        }))
        .build()
        .expect("server");
    let serve = async {
        let tls = acceptor.accept(server_io).await.expect("TLS accept");
        server
            .serve_io(crate::connection::tests::CountingIo {
                inner: tls,
                reads: reads.clone(),
            })
            .await
    };
    let send = async {
        let name = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
        let mut tls = connector
            .connect(name, client_io)
            .await
            .expect("TLS connect");
        tls.write_all(&vec![1; 64 * 1024])
            .await
            .expect("offered plaintext");
        written.notify_one();
        tls.read_to_end(&mut Vec::new()).await.expect("TLS close");
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(serve, send) })
            .await
            .expect("TLS exchange");
    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("test stop after head"))
    ));
    assert_eq!(*reads.lock().expect("reads"), [(16, 16)]);
}
