//! Proxy-owned error boundary and stable error classifications.

use std::error::Error;
use std::fmt::{Display, Formatter};

type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Stable error categories exposed by the proxy application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyErrorKind {
    /// Configuration could not be read, parsed, or validated.
    Configuration,
    /// TLS identity material could not be loaded or replaced.
    Tls,
    /// A connection to the configured upstream could not be established.
    UpstreamConnect,
    /// An upstream operation timed out or failed.
    UpstreamIo,
    /// The upstream exchanged an invalid or unexpected frame.
    UpstreamProtocol,
    /// The upstream response exceeded the configured bound.
    ResponseTooLarge,
    /// The Nacelle server runtime or request pipeline failed.
    Runtime,
    /// A proxy-owned asynchronous task failed.
    Task,
}

/// Application-owned error returned by public proxy APIs.
#[derive(Debug)]
pub struct ProxyError {
    kind: ProxyErrorKind,
    message: String,
    source: Option<BoxError>,
}

impl ProxyError {
    /// Return the stable category callers can use for error handling.
    #[must_use]
    pub const fn kind(&self) -> ProxyErrorKind {
        self.kind
    }

    pub(crate) fn configuration(
        path: impl Display,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(
            ProxyErrorKind::Configuration,
            format!("could not load configuration from {path}"),
            source,
        )
    }

    pub(crate) fn invalid_configuration(message: impl Into<String>) -> Self {
        Self::new(ProxyErrorKind::Configuration, message)
    }

    pub(crate) fn tls(source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(ProxyErrorKind::Tls, "could not load TLS identity", source)
    }

    pub(crate) fn upstream_connect(
        address: impl Display,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(
            ProxyErrorKind::UpstreamConnect,
            format!("could not connect to upstream {address}"),
            source,
        )
    }

    pub(crate) fn upstream_timeout(operation: &'static str) -> Self {
        Self::new(
            ProxyErrorKind::UpstreamIo,
            format!("upstream {operation} timed out"),
        )
    }

    pub(crate) fn upstream_io(
        operation: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(
            ProxyErrorKind::UpstreamIo,
            format!("upstream {operation} failed"),
            source,
        )
    }

    pub(crate) fn upstream_protocol(message: impl Into<String>) -> Self {
        Self::new(ProxyErrorKind::UpstreamProtocol, message)
    }

    pub(crate) fn upstream_protocol_source(
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self::with_source(ProxyErrorKind::UpstreamProtocol, message, source)
    }

    pub(crate) fn response_too_large(len: usize, max: usize) -> Self {
        Self::new(
            ProxyErrorKind::ResponseTooLarge,
            format!("upstream response length {len} exceeds configured maximum {max}"),
        )
    }

    pub(crate) fn runtime(source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(ProxyErrorKind::Runtime, "proxy runtime failed", source)
    }

    pub(crate) fn task(source: impl Error + Send + Sync + 'static) -> Self {
        Self::with_source(ProxyErrorKind::Task, "proxy task failed", source)
    }

    fn new(kind: ProxyErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    fn with_source(
        kind: ProxyErrorKind,
        message: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

impl Display for ProxyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ProxyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(error_source)
    }
}

fn error_source<'a>(source: &'a (dyn Error + Send + Sync + 'static)) -> &'a (dyn Error + 'static) {
    source
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_kind_and_source_without_exposing_concrete_runtime_errors() {
        let error = ProxyError::runtime(std::io::Error::other("listener failed"));

        assert_eq!(error.kind(), ProxyErrorKind::Runtime);
        assert_eq!(error.to_string(), "proxy runtime failed");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("listener failed")
        );
    }
}
