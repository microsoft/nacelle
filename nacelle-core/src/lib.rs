//! Shared primitives for Nacelle transports.

#[cfg(all(feature = "openssl", feature = "rustls", not(rust_analyzer)))]
compile_error!("Nacelle supports exactly one TLS backend; enable either `rustls` or `openssl`");

pub mod error;
pub mod lifecycle;
pub mod limits;
pub mod peer_rate;
pub mod pipeline;
pub mod request;
pub mod runtime;
pub mod telemetry;

pub use error::{BoxError, NacelleError, NacelleResourceLimitReason, NacelleTimeoutReason};
pub use lifecycle::{NacelleShutdown, NacelleShutdownToken};
pub use limits::{NacelleLimits, NacelleRuntimeState, TrackedPermit};
#[cfg(feature = "experimental-memory")]
pub use limits::{NacelleMemoryAllocation, NacelleMemoryBudget};
pub use peer_rate::{
    DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY, NacellePeerRateLimitResult, NacellePeerRateLimiter,
};
pub use request::{NacelleBody, NacelleConnectionMeta, NacelleConnectionTlsMeta};
pub use telemetry::{
    CompositeObserver, NacelleInMemoryObserver, NacelleMetricsContext, NacelleRequestMetricsConfig,
    NacelleTelemetry, NacelleTelemetryConfig, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetryObserver, NacelleTransport, NoopObserver,
};
