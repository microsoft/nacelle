//! TCP transport for Nacelle.

pub use nacelle_codec::MessageDecoder;

pub mod config;
pub mod connection;
pub mod limits;
pub mod options;
pub mod protocol;
pub mod runtime;
pub mod server;
pub mod telemetry;

pub use config::{NacelleTcpConfig, TcpRequestBodyMode};
pub use connection::{serve_connection, serve_stream};
pub use limits::NacelleTcpLimits;
#[cfg(unix)]
pub use options::NacelleUnixSocketOptions;
pub use options::{
    NacelleTcpBindOptions, NacelleTcpKeepalive, NacelleTcpOptions, NacelleTlsDetectionOptions,
};
pub use protocol::{
    DecodedMessage, DecodedRequest, FrameBuffer, LocalTcpHandler, LocalTcpOneWayHandler,
    NoOneWayHandler, Protocol, TcpCompletion, TcpHandler, TcpHandlerCompletion, TcpOneWayContext,
    TcpOneWayHandler, TcpRequest, TcpRequestContext, TcpResponder, TcpResponse,
};
pub use server::{LocalTcpServer, TcpServer, TcpServerBuilder};
pub use telemetry::{
    NacelleMetricsContext, NacelleRequestMetricsConfig, NacelleTelemetry, NacelleTelemetryConfig,
};
