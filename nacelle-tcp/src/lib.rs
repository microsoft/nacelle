//! TCP transport for Nacelle.

pub mod config;
pub mod connection;
pub mod limits;
pub mod options;
pub mod policies;
#[cfg(feature = "policies")]
pub mod policy;
pub mod protocol;
pub mod runtime;
mod serial_server;
pub mod server;

pub use config::{NacelleTcpConfig, ResponseWritePolicy, TcpRequestBodyMode};
pub use connection::{serve_connection, serve_stream};
pub use limits::NacelleTcpLimits;
#[cfg(feature = "experimental-openssl-detection")]
pub use options::NacelleTlsDetectionOptions;
#[cfg(unix)]
pub use options::NacelleUnixSocketOptions;
pub use options::{NacelleTcpBindOptions, NacelleTcpKeepalive, NacelleTcpOptions};
pub use policies::{NoTcpPolicies, TcpConnection, TcpHead, TcpPolicies, TcpTransport};
#[cfg(feature = "policies")]
pub use policy::{TcpLifecyclePolicy, WithTcpPolicy};
pub use protocol::{
    DecodedRequest, FrameBuffer, LocalSerialTcpHandler, LocalTcpHandler, Protocol,
    ResponseAbortReason, ResponseCompletionCallback, ResponseDeliveryOutcome,
    ResponseDeliveryPhase, SerialTcpHandler, SerialTcpRequestContext, SharedProtocol,
    TcpCompletion, TcpHandler, TcpHandlerCompletion, TcpRequest, TcpRequestContext, TcpResponder,
    TcpResponse,
};
pub use serial_server::{LocalSerialTcpServer, SerialTcpServer};
pub use server::{LocalTcpServer, TcpServer, TcpServerBuilder};
