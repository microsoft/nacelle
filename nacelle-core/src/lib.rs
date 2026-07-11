//! Shared primitives for Nacelle transports.

pub mod error;
pub mod lifecycle;
pub mod limits;
pub mod pipeline;
pub mod request;
pub mod runtime;
pub mod telemetry;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub mod tls;

pub use error::{BoxError, NacelleError};
pub use lifecycle::{NacelleShutdown, NacelleShutdownToken};
pub use limits::{
    NacelleLimits, NacelleMemoryAllocation, NacelleMemoryBudget, NacelleRuntimeState, TrackedPermit,
};
pub use request::{NacelleBody, NacelleConnectionMeta, NacelleConnectionTlsMeta};
pub use telemetry::{
    NacelleInMemoryTelemetrySink, NacelleMetricsContext, NacelleRequestMetricsConfig,
    NacelleTelemetry, NacelleTelemetryConfig, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetrySink, NacelleTransport,
};
#[cfg(feature = "tls-self-signed")]
pub use tls::NacelleGeneratedTlsConfig;
#[cfg(feature = "openssl")]
pub use tls::NacelleOpenSslConfig;
#[cfg(feature = "rustls")]
pub use tls::NacelleTlsConfig;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub use tls::NacelleTlsProvider;
