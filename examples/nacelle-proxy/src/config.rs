//! Proxy configuration loading, validation, and TLS material snapshots.

use std::fmt::{Debug, Formatter};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use nacelle::core::NacelleTlsConfig;
use serde::Deserialize;

use crate::ProxyError;

/// Settings that can be replaced while the proxy is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfiguration {
    /// Upstream server receiving the length-delimited requests.
    pub backend_addr: SocketAddr,
    /// Maximum time allowed to establish an upstream connection.
    pub connect_timeout: Duration,
    /// Maximum time allowed for the complete upstream write and response read.
    pub upstream_io_timeout: Duration,
    /// Maximum complete response body accepted from an upstream server.
    pub max_upstream_response_bytes: usize,
}

impl ProxyConfiguration {
    /// Validate configuration supplied directly through the service API.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when a timeout or response bound is zero.
    pub fn validate(&self) -> Result<(), ProxyError> {
        if self.connect_timeout.is_zero() || self.upstream_io_timeout.is_zero() {
            return Err(ProxyError::invalid_configuration(
                "proxy timeouts must be greater than zero",
            ));
        }
        if self.max_upstream_response_bytes == 0 {
            return Err(ProxyError::invalid_configuration(
                "max_upstream_response_bytes must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProxyStartupConfiguration {
    pub(crate) listen_addr: SocketAddr,
    pub(crate) max_connections: usize,
    pub(crate) max_in_flight_requests: usize,
    pub(crate) max_connections_per_peer: usize,
    pub(crate) max_memory_bytes: usize,
    pub(crate) max_request_body_bytes: usize,
    pub(crate) max_response_body_bytes: usize,
    pub(crate) handler_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProxyFileConfiguration {
    pub(crate) listen_addr: SocketAddr,
    backend_addr: SocketAddr,
    certificate_path: Option<PathBuf>,
    private_key_path: Option<PathBuf>,
    #[serde(default = "default_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_upstream_io_timeout_ms")]
    upstream_io_timeout_ms: u64,
    #[serde(default = "default_max_body_bytes")]
    pub(crate) max_request_body_bytes: usize,
    #[serde(default = "default_max_body_bytes")]
    pub(crate) max_response_body_bytes: usize,
    #[serde(default = "default_max_body_bytes")]
    max_upstream_response_bytes: usize,
    #[serde(default = "default_max_connections")]
    max_connections: usize,
    #[serde(default = "default_max_in_flight_requests")]
    max_in_flight_requests: usize,
    #[serde(default = "default_max_connections_per_peer")]
    max_connections_per_peer: usize,
    #[serde(default = "default_max_memory_bytes")]
    max_memory_bytes: usize,
    #[serde(default = "default_handler_timeout_ms")]
    handler_timeout_ms: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct ConfigurationSnapshot {
    pub(crate) file: ProxyFileConfiguration,
    pub(crate) certificate: Option<Vec<u8>>,
    pub(crate) private_key: Option<Vec<u8>>,
}

impl Debug for ConfigurationSnapshot {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfigurationSnapshot")
            .field("file", &self.file)
            .field("certificate_configured", &self.certificate.is_some())
            .field("private_key_configured", &self.private_key.is_some())
            .finish()
    }
}

#[cfg(test)]
impl ConfigurationSnapshot {
    pub(crate) fn for_reload_test(listen_addr: SocketAddr, request_limit: usize) -> Self {
        Self {
            file: ProxyFileConfiguration {
                listen_addr,
                backend_addr: "127.0.0.1:9000".parse().expect("address"),
                certificate_path: None,
                private_key_path: None,
                connect_timeout_ms: 1000,
                upstream_io_timeout_ms: 1000,
                max_request_body_bytes: request_limit,
                max_response_body_bytes: 1024,
                max_upstream_response_bytes: 1024,
                max_connections: 64,
                max_in_flight_requests: 32,
                max_connections_per_peer: 16,
                max_memory_bytes: 128 * 1024 * 1024,
                handler_timeout_ms: 60_000,
            },
            certificate: None,
            private_key: None,
        }
    }
}

impl ProxyFileConfiguration {
    pub(crate) const fn runtime_configuration(&self) -> ProxyConfiguration {
        ProxyConfiguration {
            backend_addr: self.backend_addr,
            connect_timeout: Duration::from_millis(self.connect_timeout_ms),
            upstream_io_timeout: Duration::from_millis(self.upstream_io_timeout_ms),
            max_upstream_response_bytes: self.max_upstream_response_bytes,
        }
    }

    pub(crate) const fn startup_configuration(&self) -> ProxyStartupConfiguration {
        ProxyStartupConfiguration {
            listen_addr: self.listen_addr,
            max_connections: self.max_connections,
            max_in_flight_requests: self.max_in_flight_requests,
            max_connections_per_peer: self.max_connections_per_peer,
            max_memory_bytes: self.max_memory_bytes,
            max_request_body_bytes: self.max_request_body_bytes,
            max_response_body_bytes: self.max_response_body_bytes,
            handler_timeout: Duration::from_millis(self.handler_timeout_ms),
        }
    }

    fn validate(&self) -> io::Result<()> {
        if self.connect_timeout_ms == 0 || self.upstream_io_timeout_ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy timeouts must be greater than zero",
            ));
        }
        if self.max_request_body_bytes == 0
            || self.max_response_body_bytes == 0
            || self.max_upstream_response_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy body limits must be greater than zero",
            ));
        }
        if self.max_upstream_response_bytes > self.max_response_body_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "max_upstream_response_bytes cannot exceed max_response_body_bytes",
            ));
        }
        if self.max_connections == 0
            || self.max_in_flight_requests == 0
            || self.max_connections_per_peer == 0
            || self.max_memory_bytes == 0
            || self.handler_timeout_ms == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy resource limits and handler timeout must be greater than zero",
            ));
        }
        if self.max_in_flight_requests > self.max_connections
            || self.max_connections_per_peer > self.max_connections
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request and per-peer connection limits cannot exceed max_connections",
            ));
        }
        if self.max_request_body_bytes > self.max_memory_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "max_request_body_bytes cannot exceed max_memory_bytes",
            ));
        }
        if self.certificate_path.is_some() != self.private_key_path.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "certificate_path and private_key_path must be configured together",
            ));
        }
        #[cfg(not(feature = "tls-self-signed"))]
        if self.certificate_path.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "certificate_path and private_key_path are required without tls-self-signed",
            ));
        }
        Ok(())
    }
}

pub(crate) async fn load_snapshot(path: &Path) -> Result<ConfigurationSnapshot, ProxyError> {
    load_snapshot_inner(path)
        .await
        .map_err(|error| ProxyError::configuration(path.display(), error))
}

pub(crate) fn initial_tls_config(
    initial: &ConfigurationSnapshot,
) -> Result<NacelleTlsConfig, ProxyError> {
    match (&initial.certificate, &initial.private_key) {
        (Some(certificate), Some(private_key)) => {
            NacelleTlsConfig::from_pem(certificate, private_key).map_err(ProxyError::tls)
        }
        #[cfg(feature = "tls-self-signed")]
        (None, None) => {
            let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])
                .map_err(ProxyError::tls)?;
            println!("using an ephemeral self-signed certificate");
            Ok(generated.tls_config)
        }
        #[cfg(not(feature = "tls-self-signed"))]
        (None, None) => unreachable!("configuration validation requires TLS files"),
        _ => unreachable!("configuration validation requires both TLS files"),
    }
}

async fn load_snapshot_inner(path: &Path) -> io::Result<ConfigurationSnapshot> {
    let source = tokio::fs::read_to_string(path).await?;
    let file: ProxyFileConfiguration = toml::from_str(&source)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    file.validate()?;

    let (certificate, private_key) = match (&file.certificate_path, &file.private_key_path) {
        (Some(certificate_path), Some(private_key_path)) => {
            let certificate_path = resolve_path(path, certificate_path);
            let private_key_path = resolve_path(path, private_key_path);
            (
                Some(tokio::fs::read(certificate_path).await?),
                Some(tokio::fs::read(private_key_path).await?),
            )
        }
        (None, None) => (None, None),
        _ => unreachable!("configuration validation requires both TLS files"),
    };

    Ok(ConfigurationSnapshot {
        file,
        certificate,
        private_key,
    })
}

fn resolve_path(config_path: &Path, configured_path: &Path) -> PathBuf {
    if configured_path.is_absolute() {
        return configured_path.to_path_buf();
    }
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(configured_path)
}

const fn default_connect_timeout_ms() -> u64 {
    3_000
}

const fn default_upstream_io_timeout_ms() -> u64 {
    30_000
}

const fn default_max_body_bytes() -> usize {
    1024 * 1024
}

const fn default_max_connections() -> usize {
    64
}

const fn default_max_in_flight_requests() -> usize {
    32
}

const fn default_max_connections_per_peer() -> usize {
    16
}

const fn default_max_memory_bytes() -> usize {
    128 * 1024 * 1024
}

const fn default_handler_timeout_ms() -> u64 {
    60_000
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_CONFIGURATION: &str = r#"
listen_addr = "127.0.0.1:8443"
backend_addr = "127.0.0.1:8080"
"#;

    #[test]
    fn rejects_unknown_configuration_fields() {
        let source = format!("{MINIMAL_CONFIGURATION}connect_timout_ms = 1000\n");

        let error = toml::from_str::<ProxyFileConfiguration>(&source)
            .expect_err("unknown field should be rejected");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn uses_conservative_resource_defaults() {
        let file = toml::from_str::<ProxyFileConfiguration>(MINIMAL_CONFIGURATION)
            .expect("minimal configuration");
        let startup = file.startup_configuration();

        assert_eq!(startup.max_connections, 64);
        assert_eq!(startup.max_in_flight_requests, 32);
        assert_eq!(startup.max_connections_per_peer, 16);
        assert_eq!(startup.max_memory_bytes, 128 * 1024 * 1024);
        assert_eq!(startup.max_request_body_bytes, 1024 * 1024);
    }

    #[test]
    fn snapshot_debug_redacts_private_key_material() {
        let mut snapshot = ConfigurationSnapshot::for_reload_test(
            "127.0.0.1:8443".parse().expect("address"),
            1024,
        );
        snapshot.private_key = Some(b"secret-private-key".to_vec());

        let debug = format!("{snapshot:?}");

        assert!(debug.contains("private_key_configured: true"));
        assert!(!debug.contains("secret-private-key"));
    }

    #[cfg(not(feature = "tls-self-signed"))]
    #[test]
    fn requires_tls_files_without_self_signed_support() {
        let file = toml::from_str::<ProxyFileConfiguration>(MINIMAL_CONFIGURATION)
            .expect("minimal configuration");

        let error = file.validate().expect_err("TLS files should be required");

        assert!(
            error
                .to_string()
                .contains("required without tls-self-signed")
        );
    }
}
