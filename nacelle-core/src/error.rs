use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io;

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Structured reason for a bounded resource admission failure.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NacelleResourceLimitReason {
    /// Process-wide concurrent connection capacity.
    Connections,
    /// Peer-rate table capacity is too small for worker partitioning.
    ConnectionRateLimitTableCapacity,
    /// Concurrent in-flight request capacity.
    InFlightRequests,
    /// Experimental process-wide memory budget.
    MemoryBytes,
    /// Partitioned runtime states disagree on the process memory limit.
    MemoryLimitMismatch,
    /// Per-peer connection-open rate.
    PeerConnectionRate,
    /// Peer-rate limiter state is unavailable.
    PeerConnectionRateTable,
    /// Peer-rate table has no capacity for another peer.
    PeerConnectionRateTableFull,
    /// Concurrent connections from one peer.
    PeerConnections,
    /// Request body size.
    RequestBodyBytes,
    /// Response body size.
    ResponseBodyBytes,
    /// Encoded response frame size.
    ResponseFrameBytes,
    /// Concurrent streaming task capacity.
    StreamingTasks,
    /// Multiple workers cannot share an ephemeral listener port.
    ThreadPerCoreEphemeralPort,
    /// Thread-per-core execution is unavailable on this platform.
    ThreadPerCoreUnsupportedPlatform,
    /// Worker CPU affinity could not be applied.
    WorkerAffinity,
    /// Worker core identity is invalid.
    WorkerCore,
    /// Worker count is invalid.
    WorkerCount,
    /// Worker topology discovery failed.
    WorkerDiscovery,
    /// Worker topology contains a duplicate core.
    WorkerDuplicate,
    /// Worker index is invalid.
    WorkerIndex,
    /// A worker thread panicked.
    WorkerPanic,
    /// Application-defined static reason.
    Other(&'static str),
}

impl NacelleResourceLimitReason {
    /// Return the stable low-cardinality reason label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connections => "connections",
            Self::ConnectionRateLimitTableCapacity => "connection_rate_limit_table_capacity",
            Self::InFlightRequests => "in_flight_requests",
            Self::MemoryBytes => "memory_bytes",
            Self::MemoryLimitMismatch => "memory_limit_mismatch",
            Self::PeerConnectionRate => "peer_connection_rate",
            Self::PeerConnectionRateTable => "peer_connection_rate_table",
            Self::PeerConnectionRateTableFull => "peer_connection_rate_table_full",
            Self::PeerConnections => "peer_connections",
            Self::RequestBodyBytes => "request_body_bytes",
            Self::ResponseBodyBytes => "response_body_bytes",
            Self::ResponseFrameBytes => "response_frame_bytes",
            Self::StreamingTasks => "streaming_tasks",
            Self::ThreadPerCoreEphemeralPort => "thread_per_core_ephemeral_port",
            Self::ThreadPerCoreUnsupportedPlatform => "thread_per_core_unsupported_platform",
            Self::WorkerAffinity => "worker_affinity",
            Self::WorkerCore => "worker_core",
            Self::WorkerCount => "worker_count",
            Self::WorkerDiscovery => "worker_discovery",
            Self::WorkerDuplicate => "worker_duplicate",
            Self::WorkerIndex => "worker_index",
            Self::WorkerPanic => "worker_panic",
            Self::Other(reason) => reason,
        }
    }
}

impl Display for NacelleResourceLimitReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Structured reason for a bounded operation timeout.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NacelleTimeoutReason {
    /// Application handler execution.
    Handler,
    /// HTTP request-body read.
    HttpBodyRead,
    /// HTTP response-body write.
    HttpBodyWrite,
    /// HTTP request-header read.
    HttpHeaders,
    /// Maximum HTTP connection age.
    HttpMaxConnectionAge,
    /// Idle connection activity.
    Idle,
    /// Experimental memory-budget allocation wait.
    MemoryAllocation,
    /// TCP request-body read.
    RequestBodyRead,
    /// Graceful shutdown drain.
    ShutdownDrain,
    /// Final TCP response write.
    TcpFinalWrite,
    /// TCP socket read.
    TcpRead,
    /// Closing a TCP connection after a rejection.
    TcpRejectionClose,
    /// TCP socket shutdown.
    TcpShutdown,
    /// TCP socket write.
    TcpWrite,
    /// Optional TLS protocol detection.
    TlsDetect,
    /// TLS handshake.
    TlsHandshake,
    /// Application-defined static reason.
    Other(&'static str),
}

impl NacelleTimeoutReason {
    /// Return the stable low-cardinality reason label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Handler => "handler",
            Self::HttpBodyRead => "http_body_read",
            Self::HttpBodyWrite => "http_body_write",
            Self::HttpHeaders => "http_headers",
            Self::HttpMaxConnectionAge => "http_max_connection_age",
            Self::Idle => "idle",
            Self::MemoryAllocation => "memory_allocation",
            Self::RequestBodyRead => "request_body_read",
            Self::ShutdownDrain => "shutdown_drain",
            Self::TcpFinalWrite => "tcp_final_write",
            Self::TcpRead => "tcp_read",
            Self::TcpRejectionClose => "tcp_rejection_close",
            Self::TcpShutdown => "tcp_shutdown",
            Self::TcpWrite => "tcp_write",
            Self::TlsDetect => "tls_detect",
            Self::TlsHandshake => "tls_handshake",
            Self::Other(reason) => reason,
        }
    }
}

impl Display for NacelleTimeoutReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
pub enum NacelleError {
    MissingProtocol,
    InvalidFrame(&'static str),
    FrameTooLarge { len: usize, max: usize },
    UnexpectedEof,
    ConnectionClosed,
    ResourceLimit(NacelleResourceLimitReason),
    Timeout(NacelleTimeoutReason),
    Io(io::Error),
    Protocol(BoxError),
    Handler(BoxError),
    Join(crate::runtime::JoinError),
}

impl NacelleError {
    pub fn protocol(error: impl Into<BoxError>) -> Self {
        Self::Protocol(error.into())
    }

    pub fn handler(error: impl Into<BoxError>) -> Self {
        Self::Handler(error.into())
    }

    #[cfg(feature = "error-hints")]
    /// Return optional operator guidance without changing this error's display text.
    pub fn hint(&self) -> Option<&'static str> {
        match self {
            Self::MissingProtocol => {
                Some("call TcpServer::<YourProtocol>::builder().protocol(...) before build")
            }
            Self::InvalidFrame(_) => {
                Some("verify the protocol decoder and the peer's frame format")
            }
            Self::FrameTooLarge { .. } => {
                Some("raise NacelleTcpConfig::max_frame_len or reject larger client frames")
            }
            Self::UnexpectedEof => {
                Some("check client disconnects, frame lengths, and socket timeouts")
            }
            Self::ConnectionClosed => {
                Some("the peer closed the connection before the operation completed")
            }
            Self::ResourceLimit(NacelleResourceLimitReason::Connections) => {
                Some("raise NacelleLimits::max_connections or reduce concurrent clients")
            }
            Self::ResourceLimit(NacelleResourceLimitReason::PeerConnectionRate) => Some(
                "raise NacelleLimits::max_connection_opens_per_peer_per_second or slow reconnect churn",
            ),
            Self::ResourceLimit(NacelleResourceLimitReason::PeerConnectionRateTable)
            | Self::ResourceLimit(NacelleResourceLimitReason::PeerConnectionRateTableFull) => Some(
                "raise NacelleLimits::connection_rate_limit_table_capacity or reduce active peer cardinality",
            ),
            Self::ResourceLimit(NacelleResourceLimitReason::PeerConnections) => Some(
                "raise NacelleLimits::max_connections_per_peer or distribute clients across peers",
            ),
            Self::ResourceLimit(NacelleResourceLimitReason::InFlightRequests) => {
                Some("raise NacelleLimits::max_in_flight_requests or reduce request concurrency")
            }
            Self::ResourceLimit(NacelleResourceLimitReason::StreamingTasks) => {
                Some("raise NacelleLimits::max_streaming_tasks or use buffered request bodies")
            }
            #[cfg(feature = "experimental-memory")]
            Self::ResourceLimit(NacelleResourceLimitReason::MemoryBytes) => {
                Some("raise NacelleLimits::max_memory_bytes or lower buffer/body sizes")
            }
            Self::ResourceLimit(NacelleResourceLimitReason::RequestBodyBytes) => {
                Some("raise NacelleLimits::max_request_body_bytes or lower client payload sizes")
            }
            Self::ResourceLimit(NacelleResourceLimitReason::ResponseBodyBytes) => {
                Some("raise NacelleLimits::max_response_body_bytes or stream smaller responses")
            }
            Self::ResourceLimit(_) => {
                Some("adjust the matching NacelleLimits or transport limits value")
            }
            Self::Timeout(
                NacelleTimeoutReason::TcpRead | NacelleTimeoutReason::RequestBodyRead,
            ) => Some("raise NacelleTcpLimits::read_timeout or fix slow request readers"),
            Self::Timeout(NacelleTimeoutReason::TcpWrite | NacelleTimeoutReason::TcpFinalWrite) => {
                Some("raise NacelleTcpLimits::write_timeout or fix slow response readers")
            }
            Self::Timeout(NacelleTimeoutReason::TcpShutdown) => {
                Some("raise NacelleTcpLimits::shutdown_timeout or fix slow connection shutdown")
            }
            Self::Timeout(NacelleTimeoutReason::Idle) => {
                Some("raise NacelleTcpLimits::idle_timeout or close idle clients sooner")
            }
            Self::Timeout(NacelleTimeoutReason::Handler) => {
                Some("raise NacelleLimits::handler_timeout or make the handler complete sooner")
            }
            Self::Timeout(NacelleTimeoutReason::HttpHeaders) => {
                Some("raise NacelleHttpLimits::header_read_timeout or reject slow header clients")
            }
            Self::Timeout(NacelleTimeoutReason::HttpBodyRead) => Some(
                "raise NacelleHttpLimits::request_body_read_timeout or reject slow request bodies",
            ),
            Self::Timeout(NacelleTimeoutReason::HttpBodyWrite) => {
                Some("raise NacelleHttpLimits::response_write_timeout or fix slow response readers")
            }
            Self::Timeout(_) => Some("adjust the matching Nacelle timeout limit"),
            Self::Io(_) | Self::Protocol(_) | Self::Handler(_) | Self::Join(_) => None,
        }
    }
}

impl Display for NacelleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProtocol => f.write_str("protocol is required"),
            Self::InvalidFrame(message) => write!(f, "invalid frame: {message}"),
            Self::FrameTooLarge { len, max } => {
                write!(f, "frame length {len} exceeds configured maximum {max}")
            }
            Self::UnexpectedEof => f.write_str("connection closed before the frame completed"),
            Self::ConnectionClosed => f.write_str("connection closed"),
            Self::ResourceLimit(reason) => write!(f, "resource limit exceeded: {reason}"),
            Self::Timeout(reason) => write!(f, "operation timed out: {reason}"),
            Self::Io(error) => write!(f, "i/o error: {error}"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
            Self::Handler(error) => write!(f, "handler error: {error}"),
            Self::Join(error) => write!(f, "task join error: {error}"),
        }
    }
}

impl StdError for NacelleError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error.as_ref()),
            Self::Handler(error) => Some(error.as_ref()),
            Self::Join(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for NacelleError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<crate::runtime::JoinError> for NacelleError {
    fn from(value: crate::runtime::JoinError) -> Self {
        Self::Join(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_stable_without_implicit_hints() {
        let error = NacelleError::ResourceLimit(NacelleResourceLimitReason::RequestBodyBytes);

        assert_eq!(
            error.to_string(),
            "resource limit exceeded: request_body_bytes"
        );
    }

    #[test]
    fn reasons_support_structured_matching_and_stable_labels() {
        let error = NacelleError::Timeout(NacelleTimeoutReason::TcpRead);

        assert!(matches!(
            error,
            NacelleError::Timeout(NacelleTimeoutReason::TcpRead)
        ));
        assert_eq!(NacelleTimeoutReason::TcpRead.as_str(), "tcp_read");
        assert_eq!(
            NacelleResourceLimitReason::InFlightRequests.as_str(),
            "in_flight_requests"
        );
    }

    #[test]
    fn application_defined_reasons_require_the_other_variant() {
        let error = NacelleError::ResourceLimit(NacelleResourceLimitReason::Other("cache_slots"));

        assert!(matches!(
            error,
            NacelleError::ResourceLimit(NacelleResourceLimitReason::Other("cache_slots"))
        ));
        assert_eq!(error.to_string(), "resource limit exceeded: cache_slots");
    }

    #[cfg(feature = "error-hints")]
    #[test]
    fn owned_errors_expose_hints_without_changing_display() {
        let error = NacelleError::ResourceLimit(NacelleResourceLimitReason::RequestBodyBytes);

        assert_eq!(
            error.hint(),
            Some("raise NacelleLimits::max_request_body_bytes or lower client payload sizes")
        );
        assert_eq!(
            error.to_string(),
            "resource limit exceeded: request_body_bytes"
        );
    }

    #[cfg(feature = "error-hints")]
    #[test]
    fn tcp_shutdown_uses_shutdown_timeout_hint() {
        let error = NacelleError::Timeout(NacelleTimeoutReason::TcpShutdown);

        assert_eq!(
            error.hint(),
            Some("raise NacelleTcpLimits::shutdown_timeout or fix slow connection shutdown")
        );
    }

    #[cfg(feature = "error-hints")]
    #[test]
    fn http_body_timeouts_use_http_limit_field_names() {
        let cases = [
            (
                NacelleTimeoutReason::HttpBodyRead,
                "raise NacelleHttpLimits::request_body_read_timeout or reject slow request bodies",
            ),
            (
                NacelleTimeoutReason::HttpBodyWrite,
                "raise NacelleHttpLimits::response_write_timeout or fix slow response readers",
            ),
        ];

        for (reason, expected) in cases {
            assert_eq!(NacelleError::Timeout(reason).hint(), Some(expected));
        }
    }

    #[cfg(all(feature = "error-hints", feature = "experimental-memory"))]
    #[test]
    fn memory_limit_uses_allocator_resource_name_for_hint() {
        let error = NacelleError::ResourceLimit(NacelleResourceLimitReason::MemoryBytes);

        assert_eq!(
            error.hint(),
            Some("raise NacelleLimits::max_memory_bytes or lower buffer/body sizes")
        );
    }

    #[cfg(feature = "error-hints")]
    #[test]
    fn wrapped_errors_do_not_invent_hints() {
        let error = NacelleError::handler(std::io::Error::other("boom"));

        assert_eq!(error.hint(), None);
        assert_eq!(error.to_string(), "handler error: boom");
    }
}
