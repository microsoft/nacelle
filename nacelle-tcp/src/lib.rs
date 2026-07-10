//! TCP transport for Nacelle.

pub mod connection;
pub mod limits;
pub mod options;
pub mod protocol;
pub mod runtime;
pub mod server;
pub mod telemetry;

pub use connection::{serve_connection, serve_stream};
pub use limits::NacelleTcpLimits;
#[cfg(unix)]
pub use options::NacelleUnixSocketOptions;
pub use options::{
    NacelleTcpBindOptions, NacelleTcpKeepalive, NacelleTcpOptions, NacelleTlsDetectionOptions,
};
pub use protocol::{DecodedRequest, Protocol};
pub use server::{NacelleServer, NacelleServerBuilder, TcpServer};
pub use telemetry::{
    NacelleMetricsContext, NacelleRequestMetricsConfig, NacelleTelemetry, NacelleTelemetryConfig,
};
