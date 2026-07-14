//! Runtime-reloadable TCP proxy service used by the `nacelle-proxy` example.
#![cfg(feature = "tls")]

/// Application composition, lifecycle, and Nacelle runtime assembly.
pub mod app;
/// Configuration schema, loading, validation, and TLS snapshots.
pub mod config;
/// Proxy-owned error types and stable classifications.
pub mod error;
/// Runtime configuration watching and reload policy.
pub mod reload;
/// Runtime-reloadable proxy request handling.
pub mod service;
/// Reference-protocol framing and upstream client exchange.
pub mod wire;

pub use app::ProxyApp;
pub use config::ProxyConfiguration;
pub use error::{ProxyError, ProxyErrorKind};
pub use service::ProxyService;
