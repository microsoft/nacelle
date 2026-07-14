//! Rustls configuration and connection metadata for Nacelle transports.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nacelle_core::request::NacelleConnectionTlsMeta;
use nacelle_core::tls::NacelleTlsProvider;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// Reloadable Rustls server configuration.
#[derive(Debug, Clone)]
pub struct NacelleTlsConfig {
    server_config: Arc<RwLock<Arc<ServerConfig>>>,
    handshake_timeout: Duration,
    allowed_server_names: Arc<RwLock<Option<Vec<String>>>>,
}

/// Self-signed Rustls configuration generated for local testing.
#[cfg(feature = "self-signed")]
#[derive(Debug, Clone)]
pub struct NacelleGeneratedTlsConfig {
    /// Ready-to-use server configuration.
    pub tls_config: NacelleTlsConfig,
    /// Generated certificate in PEM form.
    pub certificate_pem: String,
    /// Generated private key in PEM form.
    pub private_key_pem: String,
}

impl NacelleTlsConfig {
    /// Construct from a Rustls server configuration.
    #[must_use]
    pub fn from_server_config(server_config: ServerConfig) -> Self {
        Self::from_server_config_arc(Arc::new(server_config))
    }

    /// Construct from a shared Rustls server configuration.
    #[must_use]
    pub fn from_server_config_arc(server_config: Arc<ServerConfig>) -> Self {
        Self {
            server_config: Arc::new(RwLock::new(server_config)),
            handshake_timeout: Duration::from_secs(10),
            allowed_server_names: Arc::new(RwLock::new(None)),
        }
    }

    /// Construct from DER certificates and a private key.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when Rustls rejects the certificate or key.
    pub fn from_der(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> io::Result<Self> {
        let config = server_config_from_der(certificates, private_key, None)?;
        Ok(Self::from_server_config(config))
    }

    /// Construct from DER material with an SNI allowlist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error for an empty allowlist or invalid certificate/key.
    pub fn from_der_with_allowed_server_names(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        allowed_server_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> io::Result<Self> {
        let allowed_server_names = normalize_allowed_server_names(allowed_server_names)?;
        let config = server_config_from_der(
            certificates,
            private_key,
            Some(allowed_server_names.clone()),
        )?;
        Ok(Self {
            server_config: Arc::new(RwLock::new(Arc::new(config))),
            handshake_timeout: Duration::from_secs(10),
            allowed_server_names: Arc::new(RwLock::new(Some(allowed_server_names))),
        })
    }

    /// Construct from PEM certificates and a private key.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when PEM parsing or Rustls configuration fails.
    pub fn from_pem(certificates: &[u8], private_key: &[u8]) -> io::Result<Self> {
        Self::from_der(
            parse_pem_certificates(certificates)?,
            parse_pem_private_key(private_key)?,
        )
    }

    /// Construct from PEM material with an SNI allowlist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when parsing, allowlist validation, or Rustls
    /// configuration fails.
    pub fn from_pem_with_allowed_server_names(
        certificates: &[u8],
        private_key: &[u8],
        allowed_server_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> io::Result<Self> {
        Self::from_der_with_allowed_server_names(
            parse_pem_certificates(certificates)?,
            parse_pem_private_key(private_key)?,
            allowed_server_names,
        )
    }

    /// Load PEM certificates and a private key from files.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when files cannot be read or configuration fails.
    pub fn from_pem_files(
        certificate_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        Self::from_pem(&fs::read(certificate_path)?, &fs::read(private_key_path)?)
    }

    /// Generate a self-signed configuration for local testing.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when certificate generation or parsing fails.
    #[cfg(feature = "self-signed")]
    pub fn self_signed(
        subject_alt_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> io::Result<NacelleGeneratedTlsConfig> {
        let certified_key = rcgen::generate_simple_self_signed(
            subject_alt_names
                .into_iter()
                .map(Into::into)
                .collect::<Vec<_>>(),
        )
        .map_err(io::Error::other)?;
        let certificate_pem = certified_key.cert.pem();
        let private_key_pem = certified_key.signing_key.serialize_pem();
        let tls_config = Self::from_pem(certificate_pem.as_bytes(), private_key_pem.as_bytes())?;
        Ok(NacelleGeneratedTlsConfig {
            tls_config,
            certificate_pem,
            private_key_pem,
        })
    }

    /// Set the TLS handshake timeout.
    #[must_use]
    pub const fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Atomically replace the server configuration used by new handshakes.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn replace_server_config(&self, server_config: ServerConfig) {
        self.replace_server_config_arc(Arc::new(server_config));
    }

    /// Atomically replace the shared server configuration used by new handshakes.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn replace_server_config_arc(&self, server_config: Arc<ServerConfig>) {
        *self
            .server_config
            .write()
            .expect("TLS server config lock poisoned") = server_config;
        *self
            .allowed_server_names
            .write()
            .expect("TLS allowed server names lock poisoned") = None;
    }

    /// Reload DER material while preserving the configured SNI allowlist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when Rustls rejects the replacement material.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn reload_from_der(
        &self,
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> io::Result<()> {
        let allowed = self.allowed_server_names();
        let config = server_config_from_der(certificates, private_key, allowed.clone())?;
        self.replace_server_config(config);
        *self
            .allowed_server_names
            .write()
            .expect("TLS allowed server names lock poisoned") = allowed;
        Ok(())
    }

    /// Reload PEM material while preserving the configured SNI allowlist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when parsing or Rustls configuration fails.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn reload_from_pem(&self, certificates: &[u8], private_key: &[u8]) -> io::Result<()> {
        self.reload_from_der(
            parse_pem_certificates(certificates)?,
            parse_pem_private_key(private_key)?,
        )
    }

    /// Reload PEM files while preserving the configured SNI allowlist.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when files cannot be read or configuration fails.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn reload_from_pem_files(
        &self,
        certificate_path: impl AsRef<Path>,
        private_key_path: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.reload_from_pem(&fs::read(certificate_path)?, &fs::read(private_key_path)?)
    }

    /// Return the provider identity.
    #[must_use]
    pub const fn provider(&self) -> NacelleTlsProvider {
        NacelleTlsProvider::Rustls
    }

    /// Return the normalized SNI allowlist.
    ///
    /// # Panics
    ///
    /// Panics if the internal reload lock is poisoned.
    #[must_use]
    pub fn allowed_server_names(&self) -> Option<Vec<String>> {
        self.allowed_server_names
            .read()
            .expect("TLS allowed server names lock poisoned")
            .clone()
    }

    /// Snapshot the server configuration used by a new connection.
    ///
    /// # Panics
    ///
    /// Panics if the internal reload lock is poisoned.
    #[doc(hidden)]
    #[must_use]
    pub fn server_config(&self) -> Arc<ServerConfig> {
        self.server_config
            .read()
            .expect("TLS server config lock poisoned")
            .clone()
    }

    /// Return the configured handshake timeout.
    #[doc(hidden)]
    #[must_use]
    pub const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }
}

/// Extract provider-neutral metadata from an established Rustls connection.
#[must_use]
pub fn connection_tls_meta(connection: &rustls::ServerConnection) -> NacelleConnectionTlsMeta {
    let mut metadata = NacelleConnectionTlsMeta::new("rustls");
    if let Some(protocol) = connection.protocol_version() {
        metadata = metadata.with_protocol(format!("{protocol:?}"));
    }
    if let Some(cipher_suite) = connection.negotiated_cipher_suite() {
        metadata = metadata.with_cipher_suite(format!("{:?}", cipher_suite.suite()));
    }
    if let Some(server_name) = connection.server_name() {
        metadata = metadata.with_server_name(server_name);
    }
    metadata
}

fn server_config_from_der(
    certificates: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    allowed_server_names: Option<Vec<String>>,
) -> io::Result<ServerConfig> {
    let builder = ServerConfig::builder().with_no_client_auth();
    if let Some(allowed_server_names) = allowed_server_names {
        let certified_key =
            CertifiedKey::from_der(certificates, private_key, builder.crypto_provider())
                .map_err(io::Error::other)?;
        Ok(builder.with_cert_resolver(Arc::new(SniAllowlistResolver {
            certified_key: Arc::new(certified_key),
            allowed_server_names,
        })))
    } else {
        builder
            .with_single_cert(certificates, private_key)
            .map_err(io::Error::other)
    }
}

#[derive(Debug)]
struct SniAllowlistResolver {
    certified_key: Arc<CertifiedKey>,
    allowed_server_names: Vec<String>,
}

impl ResolvesServerCert for SniAllowlistResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        let server_name = client_hello.server_name()?;
        self.allowed_server_names
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(server_name.trim_end_matches('.')))
            .then(|| self.certified_key.clone())
    }
}

fn normalize_allowed_server_names(
    names: impl IntoIterator<Item = impl Into<String>>,
) -> io::Result<Vec<String>> {
    let names = names
        .into_iter()
        .map(|name| name.into().trim_end_matches('.').to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing allowed server names",
        ));
    }
    Ok(names)
}

/// Parse PEM certificates into Rustls DER values.
///
/// # Errors
///
/// Returns an I/O error for malformed PEM or missing certificates.
#[doc(hidden)]
pub fn parse_pem_certificates(input: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    let certificates = parse_pem_blocks(input)?
        .into_iter()
        .filter(|block| block.tag() == "CERTIFICATE")
        .map(|block| CertificateDer::from(block.into_contents()))
        .collect::<Vec<_>>();
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing certificate",
        ));
    }
    Ok(certificates)
}

fn parse_pem_private_key(input: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    for block in parse_pem_blocks(input)? {
        match block.tag() {
            "PRIVATE KEY" | "RSA PRIVATE KEY" | "EC PRIVATE KEY" => {
                return PrivateKeyDer::try_from(block.into_contents()).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid private key: {error}"),
                    )
                });
            }
            "ENCRYPTED PRIVATE KEY" => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "encrypted private keys are not supported",
                ));
            }
            _ => {}
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "missing private key",
    ))
}

fn parse_pem_blocks(input: &[u8]) -> io::Result<Vec<pem::Pem>> {
    pem::parse_many(input)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_from_pem_rejects_missing_key() {
        NacelleTlsConfig::from_pem(b"", b"").expect_err("missing key should fail");
    }

    #[cfg(feature = "self-signed")]
    #[test]
    fn self_signed_config_generates_usable_pem() {
        let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("self-signed config");
        assert!(generated.certificate_pem.contains("BEGIN CERTIFICATE"));
        assert!(generated.private_key_pem.contains("BEGIN PRIVATE KEY"));
        assert_eq!(generated.tls_config.provider(), NacelleTlsProvider::Rustls);
        generated
            .tls_config
            .reload_from_pem(
                generated.certificate_pem.as_bytes(),
                generated.private_key_pem.as_bytes(),
            )
            .expect("generated certificate should reload");
    }

    #[cfg(feature = "self-signed")]
    #[test]
    fn pem_config_can_restrict_allowed_server_names() {
        let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("self-signed config");
        let tls = NacelleTlsConfig::from_pem_with_allowed_server_names(
            generated.certificate_pem.as_bytes(),
            generated.private_key_pem.as_bytes(),
            ["LOCALHOST."],
        )
        .expect("SNI allowlist config should build");
        assert_eq!(
            tls.allowed_server_names(),
            Some(vec!["localhost".to_string()])
        );
    }
}
