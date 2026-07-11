//! Shared primitives for Nacelle transports.

pub mod error;
pub mod handler;
pub mod lifecycle;
pub mod limits;
pub mod pipeline;
pub mod request;
pub mod response;
pub mod runtime;
pub mod telemetry;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub mod tls;
#[cfg(feature = "tower")]
pub mod tower;

pub use error::{BoxError, NacelleError};
pub use handler::{Handler, HandlerFn, handler_fn};
pub use lifecycle::{NacelleShutdown, NacelleShutdownToken};
pub use limits::{
    NacelleLimits, NacelleMemoryAllocation, NacelleMemoryBudget, NacelleRuntimeState, TrackedPermit,
};
#[cfg(feature = "http-types")]
pub use request::HttpRequestMeta;
pub use request::{
    NacelleBody, NacelleConnectionExtension, NacelleConnectionExtensionFactory,
    NacelleConnectionMeta, NacelleConnectionTlsMeta, NacelleRequest, NacelleRequestMeta,
    TcpRequestMeta,
};
#[cfg(feature = "http-types")]
pub use response::HttpResponseMeta;
pub use response::{NacelleResponse, NacelleResponseMeta, TcpResponseMeta};
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
#[cfg(feature = "tower")]
pub use tower::handler_from_tower_service;
