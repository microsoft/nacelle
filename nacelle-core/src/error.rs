use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use std::io;

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug)]
pub enum NacelleError {
    MissingProtocol,
    InvalidFrame(&'static str),
    FrameTooLarge { len: usize, max: usize },
    UnexpectedEof,
    ConnectionClosed,
    ResourceLimit(&'static str),
    Timeout(&'static str),
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
            Self::ResourceLimit("connections") => {
                Some("raise NacelleLimits::max_connections or reduce concurrent clients")
            }
            Self::ResourceLimit("peer_connection_rate") => Some(
                "raise NacelleLimits::max_connection_opens_per_peer_per_second or slow reconnect churn",
            ),
            Self::ResourceLimit("peer_connection_rate_table_full") => Some(
                "raise NacelleLimits::connection_rate_limit_table_capacity or reduce active peer cardinality",
            ),
            Self::ResourceLimit("connections_per_peer") => Some(
                "raise NacelleLimits::max_connections_per_peer or distribute clients across peers",
            ),
            Self::ResourceLimit("requests") => {
                Some("raise NacelleLimits::max_in_flight_requests or reduce request concurrency")
            }
            Self::ResourceLimit("streaming_tasks") => {
                Some("raise NacelleLimits::max_streaming_tasks or use buffered request bodies")
            }
            Self::ResourceLimit("memory") => {
                Some("raise NacelleLimits::max_memory_bytes or lower buffer/body sizes")
            }
            Self::ResourceLimit("request_body_bytes") => {
                Some("raise NacelleLimits::max_request_body_bytes or lower client payload sizes")
            }
            Self::ResourceLimit("response_body_bytes") => {
                Some("raise NacelleLimits::max_response_body_bytes or stream smaller responses")
            }
            Self::ResourceLimit(_) => {
                Some("adjust the matching NacelleLimits or transport limits value")
            }
            Self::Timeout("tcp_read") | Self::Timeout("request_body_read") => {
                Some("raise NacelleTcpLimits::read_timeout or fix slow request readers")
            }
            Self::Timeout("tcp_write") | Self::Timeout("tcp_final_write") => {
                Some("raise NacelleTcpLimits::write_timeout or fix slow response readers")
            }
            Self::Timeout("idle") => {
                Some("raise NacelleTcpLimits::idle_timeout or close idle clients sooner")
            }
            Self::Timeout("handler") => {
                Some("raise NacelleLimits::handler_timeout or make the handler complete sooner")
            }
            Self::Timeout("http_headers") => {
                Some("raise NacelleHttpLimits::header_read_timeout or reject slow header clients")
            }
            Self::Timeout("http_body_read") => {
                Some("raise NacelleHttpLimits::body_read_timeout or reject slow request bodies")
            }
            Self::Timeout("http_body_write") => {
                Some("raise NacelleHttpLimits::body_write_timeout or fix slow response readers")
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
            Self::ResourceLimit(name) => write!(f, "resource limit exceeded: {name}"),
            Self::Timeout(name) => write!(f, "operation timed out: {name}"),
            Self::Io(error) => write!(f, "i/o error: {error}"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
            Self::Handler(error) => write!(f, "handler error: {error}"),
            Self::Join(error) => write!(f, "task join error: {error}"),
        }?;
        #[cfg(feature = "error-hints")]
        {
            if let Some(hint) = self.hint() {
                write!(f, "; hint: {hint}")?;
            }
        }
        Ok(())
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

#[cfg(all(test, feature = "error-hints"))]
mod tests {
    use super::*;

    #[test]
    fn owned_errors_include_actionable_hints() {
        let error = NacelleError::ResourceLimit("request_body_bytes");

        assert_eq!(
            error.hint(),
            Some("raise NacelleLimits::max_request_body_bytes or lower client payload sizes")
        );
        assert!(error.to_string().contains("hint: raise NacelleLimits"));
    }

    #[test]
    fn wrapped_errors_do_not_invent_hints() {
        let error = NacelleError::handler(std::io::Error::other("boom"));

        assert_eq!(error.hint(), None);
        assert_eq!(error.to_string(), "handler error: boom");
    }
}
