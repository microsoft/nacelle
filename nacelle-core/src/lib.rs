//! Shared primitives for Nacelle transports.

pub mod error;
pub mod lifecycle;
pub mod limits;
pub mod peer_rate;
pub mod pipeline;
pub mod request;
pub mod runtime;
pub mod telemetry;
#[cfg(feature = "tls")]
pub mod tls;

pub use error::{BoxError, NacelleError};
pub use lifecycle::{NacelleShutdown, NacelleShutdownToken};
pub use limits::{NacelleLimits, NacelleRuntimeState, TrackedPermit};
#[cfg(feature = "exp-memory-limits")]
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
#[cfg(feature = "tls")]
pub use tls::NacelleTlsProvider;
