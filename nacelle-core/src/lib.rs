//! Shared primitives for Nacelle transports.

#[cfg(all(feature = "openssl", feature = "rustls", not(rust_analyzer)))]
compile_error!("Nacelle supports exactly one TLS backend; enable either `rustls` or `openssl`");

pub mod admission;
pub mod error;
pub mod lifecycle;
pub mod limits;
pub mod peer_rate;
pub mod pipeline;
pub mod request;
pub mod runtime;
pub mod telemetry;
pub mod tracking;

pub use admission::{
    Admission, AdmissionConfig, AdmissionContext, AdmissionRejection, AllowAll, LocalAdmission,
};
pub use error::{
    BoxError, NacelleConfigurationError, NacelleError, NacelleResourceLimitReason,
    NacelleTimeoutReason,
};
pub use lifecycle::{NacelleShutdown, NacelleShutdownToken};
pub use limits::{
    NacelleConnectionLimits, NacelleControlScope, NacelleLimits, NacelleRuntimeControl,
    NacelleRuntimeState, TrackedPermit,
};
pub use peer_rate::{
    DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY, NacellePeerRateLimitResult, NacellePeerRateLimiter,
};
pub use request::{NacelleBody, NacelleConnectionMeta, NacelleConnectionTlsMeta};
pub use runtime::{NacelleRuntimeTopology, declare_worker_topology};
pub use telemetry::{
    CompositeObserver, NacelleInMemoryObserver, NacelleMetricsContext, NacelleRequestMetricsConfig,
    NacelleTelemetry, NacelleTelemetryConfig, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetryObserver, NacelleTransport, NoopObserver,
};
pub use tracking::{
    NacelleCloseOutcome, NacelleConnectionActivityState, NacelleConnectionCloseReason,
    NacelleConnectionLookup, NacelleConnectionSnapshot, NacelleConnectionTracking,
    NacelleReclaimOutcome, NacelleReclaimPartition,
};
