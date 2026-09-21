//! OpenSSL configuration and connection metadata for Nacelle transports.

use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nacelle_core::request::NacelleConnectionTlsMeta;
use openssl::ssl::{
    NameType, SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod, SslRef, SslVersion,
};

/// Reloadable OpenSSL server configuration.
#[derive(Clone)]
pub struct NacelleOpenSslConfig {
    acceptor: Arc<RwLock<Arc<SslAcceptor>>>,
    handshake_timeout: Duration,
}

impl std::fmt::Debug for NacelleOpenSslConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NacelleOpenSslConfig")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish_non_exhaustive()
    }
}

impl NacelleOpenSslConfig {
    /// Construct configuration from a prepared acceptor.
    #[must_use]
    pub fn from_acceptor(acceptor: SslAcceptor) -> Self {
        Self {
            acceptor: Arc::new(RwLock::new(Arc::new(acceptor))),
            handshake_timeout: Duration::from_secs(10),
        }
    }

    /// Load a certificate chain and private key from PEM files.
    ///
    /// Uses Mozilla's v5 intermediate profile with a TLS 1.2 minimum. Supply a
    /// custom acceptor through [`Self::from_acceptor`] for other TLS policies.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when files cannot be read or OpenSSL rejects the
    /// certificate, key, or resulting configuration.
    pub fn from_pem_files(
        certificate_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let mut builder = default_acceptor_builder()?;
        builder
            .set_private_key_file(private_key_path, SslFiletype::PEM)
            .map_err(io::Error::other)?;
        builder
            .set_certificate_chain_file(certificate_path)
            .map_err(io::Error::other)?;
        builder.check_private_key().map_err(io::Error::other)?;
        Ok(Self::from_acceptor(builder.build()))
    }

    /// Set the TLS handshake timeout.
    #[must_use]
    pub const fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Atomically replace the acceptor used by new handshakes.
    ///
    /// # Panics
    ///
    /// Panics if the internal reload lock is poisoned.
    pub fn replace_acceptor(&self, acceptor: SslAcceptor) {
        *self
            .acceptor
            .write()
            .expect("OpenSSL acceptor lock poisoned") = Arc::new(acceptor);
    }

    /// Snapshot the acceptor used by a new connection.
    #[doc(hidden)]
    #[must_use]
    pub fn acceptor(&self) -> Arc<SslAcceptor> {
        self.acceptor
            .read()
            .expect("OpenSSL acceptor lock poisoned")
            .clone()
    }

    /// Return the configured handshake timeout.
    #[doc(hidden)]
    #[must_use]
    pub const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }
}

fn default_acceptor_builder() -> io::Result<SslAcceptorBuilder> {
    let mut builder =
        SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).map_err(io::Error::other)?;
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_2))
        .map_err(io::Error::other)?;
    Ok(builder)
}

/// Extract negotiated TLS metadata from an established OpenSSL connection.
#[must_use]
pub fn connection_tls_meta(ssl: &SslRef) -> NacelleConnectionTlsMeta {
    let mut metadata = NacelleConnectionTlsMeta::new("openssl").with_protocol(ssl.version_str());
    if let Some(cipher) = ssl.current_cipher() {
        metadata = metadata.with_cipher_suite(cipher.name());
        let bits = cipher.bits();
        if let Ok(secret_bits) = u16::try_from(bits.secret) {
            metadata = metadata.with_cipher_bits(secret_bits);
        }
        if let Ok(algorithm_bits) = u16::try_from(bits.algorithm) {
            metadata = metadata.with_cipher_algorithm_bits(algorithm_bits);
        }
    }
    if let Some(server_name) = ssl.servername(NameType::HOST_NAME) {
        metadata = metadata.with_server_name(server_name);
    }
    metadata
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_from_missing_files_fails() {
        let result = NacelleOpenSslConfig::from_pem_files("missing-cert.pem", "missing-key.pem");
        result.expect_err("missing files should fail");
    }

    #[test]
    fn default_profile_requires_tls12_and_tests_tls13_when_available() {
        use openssl::asn1::Asn1Time;
        use openssl::hash::MessageDigest;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::ssl::{SslConnector, SslOptions, SslVerifyMode};
        use openssl::x509::{X509, X509NameBuilder};

        let mut builder = default_acceptor_builder().expect("acceptor builder");
        assert_eq!(builder.min_proto_version(), Some(SslVersion::TLS1_2));
        assert!(
            builder
                .options()
                .contains(SslOptions::NO_TLSV1 | SslOptions::NO_TLSV1_1)
        );
        #[cfg(nacelle_openssl_tls13)]
        assert!(!builder.options().contains(SslOptions::NO_TLSV1_3));
        let key = PKey::from_rsa(Rsa::generate(2048).expect("RSA key")).expect("private key");
        let mut name = X509NameBuilder::new().expect("name builder");
        name.append_entry_by_text("CN", "localhost")
            .expect("common name");
        let name = name.build();
        let mut certificate = X509::builder().expect("certificate builder");
        certificate.set_version(2).expect("version");
        certificate.set_subject_name(&name).expect("subject");
        certificate.set_issuer_name(&name).expect("issuer");
        certificate.set_pubkey(&key).expect("public key");
        certificate
            .set_not_before(&Asn1Time::days_from_now(0).expect("not before"))
            .expect("validity start");
        certificate
            .set_not_after(&Asn1Time::days_from_now(1).expect("not after"))
            .expect("validity end");
        certificate
            .sign(&key, MessageDigest::sha256())
            .expect("sign");
        builder
            .set_certificate(&certificate.build())
            .expect("certificate");
        builder.set_private_key(&key).expect("key");
        let acceptor = Arc::new(builder.build());

        #[cfg(nacelle_openssl_tls13)]
        let versions = [
            (SslVersion::TLS1, false),
            (SslVersion::TLS1_1, false),
            (SslVersion::TLS1_2, true),
            (SslVersion::TLS1_3, true),
        ];
        #[cfg(not(nacelle_openssl_tls13))]
        let versions = [
            (SslVersion::TLS1, false),
            (SslVersion::TLS1_1, false),
            (SslVersion::TLS1_2, true),
        ];

        for (version, accepted) in versions {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            let address = listener.local_addr().expect("address");
            let acceptor = acceptor.clone();
            let server = std::thread::spawn(move || {
                let (stream, _) = listener.accept().expect("accept");
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read timeout");
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .expect("write timeout");
                acceptor.accept(stream).is_ok()
            });
            let mut client =
                SslConnector::builder(SslMethod::tls_client()).expect("client builder");
            client.set_verify(SslVerifyMode::NONE);
            client.set_security_level(0);
            client
                .set_min_proto_version(Some(version))
                .expect("client minimum");
            client
                .set_max_proto_version(Some(version))
                .expect("client maximum");
            let stream = std::net::TcpStream::connect(address).expect("connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .expect("write timeout");
            let connected = client.build().connect("localhost", stream).is_ok();
            let server_connected = server.join().expect("server thread");
            assert_eq!(connected, accepted, "client {version:?}");
            assert_eq!(server_connected, accepted, "server {version:?}");
        }
    }
}
