//! OpenSSL configuration and connection metadata for Nacelle transports.

use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nacelle_core::request::NacelleConnectionTlsMeta;
use nacelle_core::tls::NacelleTlsProvider;
use openssl::ssl::{NameType, SslAcceptor, SslFiletype, SslMethod, SslRef};

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
            .field("provider", &NacelleTlsProvider::OpenSsl)
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
    /// # Errors
    ///
    /// Returns an I/O error when files cannot be read or OpenSSL rejects the
    /// certificate, key, or resulting configuration.
    pub fn from_pem_files(
        certificate_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let mut builder =
            SslAcceptor::mozilla_intermediate(SslMethod::tls_server()).map_err(io::Error::other)?;
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

    /// Return the provider identity.
    #[must_use]
    pub const fn provider(&self) -> NacelleTlsProvider {
        NacelleTlsProvider::OpenSsl
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

/// Extract provider-neutral metadata from an established OpenSSL connection.
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
}
