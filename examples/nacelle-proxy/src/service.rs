//! Runtime-reloadable proxy application service.

use std::sync::{Arc, RwLock};

use bytes::BytesMut;
use nacelle::rustls::NacelleTlsConfig;
use nacelle::tcp::{TcpHandlerCompletion, TcpRequestContext, TcpResponse};

use crate::ProxyError;
use crate::config::ProxyConfiguration;
use crate::wire::{ProxyProtocol, ReferenceProtocolClient};

/// TCP proxy behavior and its reloadable process state.
#[derive(Debug, Clone)]
pub struct ProxyService {
    configuration: Arc<RwLock<Arc<ProxyConfiguration>>>,
    tls_config: NacelleTlsConfig,
}

impl ProxyService {
    /// Create a proxy with an initial, validated configuration and TLS identity.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when a timeout or response bound is zero.
    pub fn new(
        configuration: ProxyConfiguration,
        tls_config: NacelleTlsConfig,
    ) -> Result<Self, ProxyError> {
        configuration.validate()?;
        Ok(Self {
            configuration: Arc::new(RwLock::new(Arc::new(configuration))),
            tls_config,
        })
    }

    /// Return the configuration snapshot used by newly started requests.
    #[must_use]
    pub fn configuration(&self) -> Arc<ProxyConfiguration> {
        self.configuration
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Atomically replace the configuration used by newly started requests.
    ///
    /// # Errors
    ///
    /// Returns a configuration error without changing the active snapshot when
    /// a timeout or response bound is zero.
    pub fn set_configuration(&self, configuration: ProxyConfiguration) -> Result<(), ProxyError> {
        configuration.validate()?;
        *self
            .configuration
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(configuration);
        Ok(())
    }

    /// Validate and replace the certificate used by new TLS handshakes.
    ///
    /// # Errors
    ///
    /// Returns a TLS proxy error when Rustls rejects the certificate or key.
    pub fn reload_tls(&self, certificates: &[u8], private_key: &[u8]) -> Result<(), ProxyError> {
        self.tls_config
            .reload_from_pem(certificates, private_key)
            .map_err(ProxyError::tls)
    }

    /// Handle one fully buffered request using a stable configuration snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the inbound body cannot be read, the upstream
    /// exchange fails, or the response cannot be delivered.
    pub async fn handle(
        &self,
        mut context: TcpRequestContext<ProxyProtocol>,
    ) -> Result<TcpHandlerCompletion<ProxyProtocol>, ProxyError> {
        let configuration = self.configuration();
        let request = context.request().head;
        let mut body = BytesMut::with_capacity(request.body_len);
        while let Some(chunk) = context.request_mut().body.next_chunk().await {
            body.extend_from_slice(&chunk.map_err(ProxyError::runtime)?);
        }

        let client = ReferenceProtocolClient::new(
            configuration.backend_addr,
            configuration.connect_timeout,
            configuration.upstream_io_timeout,
            configuration.max_upstream_response_bytes,
        );
        let response = client
            .exchange(request.request_id, request.opcode, &body)
            .await?;
        context
            .respond(TcpResponse::bytes(response))
            .await
            .map_err(ProxyError::runtime)
    }
}

#[cfg(all(test, feature = "tls-self-signed"))]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::ProxyErrorKind;
    use rustls::pki_types::{CertificateDer, ServerName};
    use rustls::{ClientConfig, RootCertStore};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    #[test]
    fn set_configuration_replaces_the_snapshot() {
        let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("TLS config");
        let initial = ProxyConfiguration {
            backend_addr: "127.0.0.1:9000".parse().expect("address"),
            connect_timeout: Duration::from_secs(1),
            upstream_io_timeout: Duration::from_secs(2),
            max_upstream_response_bytes: 1024,
        };
        let service = ProxyService::new(initial, generated.tls_config).expect("service");
        let replacement = ProxyConfiguration {
            backend_addr: "127.0.0.1:9001".parse().expect("address"),
            connect_timeout: Duration::from_secs(3),
            upstream_io_timeout: Duration::from_secs(4),
            max_upstream_response_bytes: 2048,
        };

        service
            .set_configuration(replacement.clone())
            .expect("replacement configuration");

        assert_eq!(*service.configuration(), replacement);
    }

    #[test]
    fn reload_tls_accepts_a_new_valid_identity_and_rejects_invalid_pem() {
        let initial = NacelleTlsConfig::self_signed(["localhost"]).expect("initial TLS config");
        let replacement =
            NacelleTlsConfig::self_signed(["localhost"]).expect("replacement TLS config");
        let service = ProxyService::new(
            ProxyConfiguration {
                backend_addr: "127.0.0.1:9000".parse().expect("address"),
                connect_timeout: Duration::from_secs(1),
                upstream_io_timeout: Duration::from_secs(1),
                max_upstream_response_bytes: 1024,
            },
            initial.tls_config,
        )
        .expect("service");

        service
            .reload_tls(
                replacement.certificate_pem.as_bytes(),
                replacement.private_key_pem.as_bytes(),
            )
            .expect("valid replacement identity");

        let error = service
            .reload_tls(b"not a certificate", b"not a key")
            .expect_err("invalid identity");
        assert_eq!(error.kind(), ProxyErrorKind::Tls);
    }

    #[tokio::test]
    async fn reload_tls_changes_the_identity_used_by_new_handshakes() {
        let initial = NacelleTlsConfig::self_signed(["localhost"]).expect("initial TLS config");
        let replacement =
            NacelleTlsConfig::self_signed(["localhost"]).expect("replacement TLS config");
        let service = ProxyService::new(
            ProxyConfiguration {
                backend_addr: "127.0.0.1:9000".parse().expect("address"),
                connect_timeout: Duration::from_secs(1),
                upstream_io_timeout: Duration::from_secs(1),
                max_upstream_response_bytes: 1024,
            },
            initial.tls_config.clone(),
        )
        .expect("service");

        complete_tls_handshake(&initial.tls_config, &initial.certificate_pem).await;
        service
            .reload_tls(
                replacement.certificate_pem.as_bytes(),
                replacement.private_key_pem.as_bytes(),
            )
            .expect("replacement identity");
        complete_tls_handshake(&initial.tls_config, &replacement.certificate_pem).await;
    }

    #[test]
    fn rejects_invalid_runtime_configuration_without_replacing_snapshot() {
        let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("TLS config");
        let initial = ProxyConfiguration {
            backend_addr: "127.0.0.1:9000".parse().expect("address"),
            connect_timeout: Duration::from_secs(1),
            upstream_io_timeout: Duration::from_secs(1),
            max_upstream_response_bytes: 1024,
        };
        let service = ProxyService::new(initial.clone(), generated.tls_config).expect("service");
        let invalid = ProxyConfiguration {
            connect_timeout: Duration::ZERO,
            ..initial
        };

        let error = service
            .set_configuration(invalid)
            .expect_err("zero timeout should be rejected");

        assert_eq!(error.kind(), ProxyErrorKind::Configuration);
        assert_eq!(*service.configuration(), initial);
    }

    async fn complete_tls_handshake(tls_config: &NacelleTlsConfig, certificate_pem: &str) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let acceptor = TlsAcceptor::from(tls_config.server_config());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            acceptor.accept(stream).await.expect("server handshake");
        });

        let certificate = pem::parse(certificate_pem)
            .expect("certificate PEM")
            .into_contents();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(certificate))
            .expect("trusted certificate");
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));
        let stream = TcpStream::connect(address).await.expect("connect");
        let server_name = ServerName::try_from("localhost").expect("server name");

        connector
            .connect(server_name, stream)
            .await
            .expect("client handshake");
        server.await.expect("server task");
    }
}
