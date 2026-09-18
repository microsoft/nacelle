//! Rustls configuration and connection metadata for Nacelle transports.

use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use nacelle_core::request::NacelleConnectionTlsMeta;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;

/// Reloadable Rustls server configuration.
#[derive(Debug, Clone)]
pub struct NacelleTlsConfig {
    state: Arc<RwLock<TlsServerState>>,
    handshake_timeout: Duration,
}

#[derive(Debug)]
struct TlsServerState {
    server_config: Arc<ServerConfig>,
    allowed_server_names: Option<Vec<String>>,
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
            state: Arc::new(RwLock::new(TlsServerState {
                server_config,
                allowed_server_names: None,
            })),
            handshake_timeout: Duration::from_secs(10),
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
            state: Arc::new(RwLock::new(TlsServerState {
                server_config: Arc::new(config),
                allowed_server_names: Some(allowed_server_names),
            })),
            handshake_timeout: Duration::from_secs(10),
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
    /// The supplied configuration owns its certificate policy; this clears the
    /// stored SNI allowlist used by subsequent certificate-only reloads.
    ///
    /// # Panics
    ///
    /// Panics if an internal reload lock is poisoned.
    pub fn replace_server_config_arc(&self, server_config: Arc<ServerConfig>) {
        *self.state.write().expect("TLS server config lock poisoned") = TlsServerState {
            server_config,
            allowed_server_names: None,
        };
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
        let mut state = self.state.write().expect("TLS server config lock poisoned");
        let config = server_config_from_der(
            certificates,
            private_key,
            state.allowed_server_names.clone(),
        )?;
        state.server_config = Arc::new(config);
        drop(state);
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

    /// Return the normalized SNI allowlist.
    ///
    /// # Panics
    ///
    /// Panics if the internal reload lock is poisoned.
    #[must_use]
    pub fn allowed_server_names(&self) -> Option<Vec<String>> {
        self.state
            .read()
            .expect("TLS server config lock poisoned")
            .allowed_server_names
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
        self.state
            .read()
            .expect("TLS server config lock poisoned")
            .server_config
            .clone()
    }

    /// Return the configured handshake timeout.
    #[doc(hidden)]
    #[must_use]
    pub const fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }
}

/// Extract negotiated TLS metadata from an established Rustls connection.
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

    #[cfg(feature = "self-signed")]
    fn assert_sni_policy(config: &Arc<ServerConfig>) {
        for (name, enable_sni, accepted) in [
            ("localhost", true, true),
            ("outside.invalid", true, false),
            ("localhost", false, false),
        ] {
            let mut client_config = rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth();
            client_config.enable_sni = enable_sni;
            let mut client = rustls::ClientConnection::new(
                Arc::new(client_config),
                rustls::pki_types::ServerName::try_from(name).expect("server name"),
            )
            .expect("client");
            let mut hello = Vec::new();
            client.write_tls(&mut hello).expect("client hello");
            let mut server = rustls::ServerConnection::new(config.clone()).expect("server");
            server.read_tls(&mut hello.as_slice()).expect("read hello");
            assert_eq!(
                server.process_new_packets().is_ok(),
                accepted,
                "{name}, SNI={enable_sni}"
            );
        }
    }

    #[cfg(feature = "self-signed")]
    #[test]
    fn concurrent_certificate_reloads_preserve_sni_policy() {
        let generated = NacelleTlsConfig::self_signed(["localhost"]).expect("certificate");
        let tls = NacelleTlsConfig::from_pem_with_allowed_server_names(
            generated.certificate_pem.as_bytes(),
            generated.private_key_pem.as_bytes(),
            ["localhost"],
        )
        .expect("config");
        let original = tls.server_config();
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let tls = &tls;
                let generated = &generated;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..32 {
                        tls.reload_from_pem(
                            generated.certificate_pem.as_bytes(),
                            generated.private_key_pem.as_bytes(),
                        )
                        .expect("reload");
                        assert_eq!(
                            tls.allowed_server_names(),
                            Some(vec!["localhost".to_string()])
                        );
                    }
                });
            }
        });
        assert_sni_policy(&original);
        assert_sni_policy(&tls.server_config());
        let current = tls.server_config();
        assert!(
            tls.reload_from_der(
                Vec::new(),
                parse_pem_private_key(generated.private_key_pem.as_bytes()).expect("key")
            )
            .is_err()
        );
        assert!(Arc::ptr_eq(&current, &tls.server_config()));
        assert_sni_policy(&tls.server_config());
    }
}
