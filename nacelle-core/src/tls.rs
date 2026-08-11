//! Provider-neutral TLS identity.

/// Identifies a TLS backend without linking its implementation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleTlsProvider {
    /// Rustls provider.
    Rustls,
    /// OpenSSL provider.
    OpenSsl,
}
