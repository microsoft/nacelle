//! TCP transport for Nacelle.
//!
//! The optional `compact-connections` feature allocates immediate-response
//! buffers lazily and retains small buffers between requests. Coalescing keeps
//! its configured initial capacity. Limits, deadlines, and wire behavior are
//! unchanged; the feature does not pool buffers or box request futures.

pub mod admission;
pub mod config;
pub mod connection;
pub mod limits;
pub mod options;
pub mod protocol;
pub mod runtime;
mod serial_server;
pub mod server;

#[doc(hidden)]
pub use admission::{LocalSerialTcpService, LocalTcpService, SerialTcpService, TcpService};
pub use admission::{SerialTcpAdmissionContext, TcpAdmissionContext};
pub use config::{
    NacelleTcpConfig, ResponseWritePolicy, TcpHeadReadLimits, TcpHeadReadLimitsError,
    TcpRequestBodyMode,
};
pub use connection::{serve_connection, serve_stream};
pub use limits::NacelleTcpLimits;
#[cfg(feature = "experimental-openssl-detection")]
pub use options::NacelleTlsDetectionOptions;
#[cfg(unix)]
pub use options::NacelleUnixSocketOptions;
pub use options::{NacelleTcpBindOptions, NacelleTcpKeepalive, NacelleTcpOptions};
pub use protocol::{
    DecodedRequest, FrameBuffer, LocalSerialTcpHandler, LocalTcpHandler, Protocol,
    ResponseAbortReason, ResponseCompletionCallback, ResponseDeliveryOutcome,
    ResponseDeliveryPhase, SerialTcpHandler, SerialTcpRequestContext, SharedProtocol,
    TcpCompletion, TcpHandler, TcpHandlerCompletion, TcpRequest, TcpRequestContext, TcpResponder,
    TcpResponse,
};
pub use serial_server::{LocalSerialTcpServer, SerialTcpServer};
pub use server::{LocalTcpServer, TcpServer, TcpServerBuilder};
