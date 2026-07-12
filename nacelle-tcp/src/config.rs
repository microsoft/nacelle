/// How a decoded TCP request body is delivered to the application pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpRequestBodyMode {
    /// Read the complete declared body before invoking the handler.
    Buffered,
    /// Pump body chunks concurrently while the handler runs.
    Streaming,
}

/// When complete encoded response frames are delivered to the socket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ResponseWritePolicy {
    /// Write each complete frame immediately.
    #[default]
    Immediate,
    /// Coalesce already-buffered responses up to `response_buffer_capacity`.
    CoalesceBuffered,
    /// Coalesce complete frames until at least the configured byte threshold.
    FlushAtBytes(usize),
}

/// TCP framing, buffering, and request-body delivery configuration.
#[derive(Debug, Clone)]
pub struct NacelleTcpConfig {
    /// Initial cumulative socket read-buffer capacity.
    pub read_buffer_capacity: usize,
    /// Initial coalesced response write-buffer capacity.
    pub response_buffer_capacity: usize,
    /// Maximum accepted protocol frame length.
    pub max_frame_len: usize,
    /// Chunk size used when exposing buffered request bodies.
    pub request_body_chunk_size: usize,
    /// Bounded channel capacity used for streaming request bodies.
    pub request_body_channel_capacity: usize,
    /// Request-body delivery mode.
    pub request_body_mode: TcpRequestBodyMode,
    /// Response frame delivery policy.
    pub response_write_policy: ResponseWritePolicy,
}

impl Default for NacelleTcpConfig {
    fn default() -> Self {
        Self {
            read_buffer_capacity: 64 * 1024,
            response_buffer_capacity: 64 * 1024,
            max_frame_len: 16 * 1024 * 1024,
            request_body_chunk_size: 64 * 1024,
            request_body_channel_capacity: 4,
            request_body_mode: TcpRequestBodyMode::Buffered,
            response_write_policy: ResponseWritePolicy::Immediate,
        }
    }
}

impl NacelleTcpConfig {
    /// Set the initial cumulative read-buffer capacity.
    pub fn with_read_buffer_capacity(mut self, capacity: usize) -> Self {
        self.read_buffer_capacity = capacity.max(1024);
        self
    }

    /// Set the initial coalesced response-buffer capacity.
    pub fn with_response_buffer_capacity(mut self, capacity: usize) -> Self {
        self.response_buffer_capacity = capacity.max(1024);
        self
    }

    /// Set the maximum accepted frame length.
    pub fn with_max_frame_len(mut self, max_frame_len: usize) -> Self {
        self.max_frame_len = max_frame_len.max(24);
        self
    }

    /// Set the request-body chunk size.
    pub fn with_request_body_chunk_size(mut self, chunk_size: usize) -> Self {
        self.request_body_chunk_size = chunk_size.max(1);
        self
    }

    /// Set the bounded streaming body channel capacity.
    pub fn with_request_body_channel_capacity(mut self, capacity: usize) -> Self {
        self.request_body_channel_capacity = capacity.max(1);
        self
    }

    /// Select buffered or streaming request-body delivery.
    pub fn with_request_body_mode(mut self, mode: TcpRequestBodyMode) -> Self {
        self.request_body_mode = mode;
        self
    }

    /// Select immediate or bounded coalesced response delivery.
    pub fn with_response_write_policy(mut self, policy: ResponseWritePolicy) -> Self {
        self.response_write_policy = policy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_bounded_and_buffered() {
        let config = NacelleTcpConfig::default();

        assert_eq!(config.read_buffer_capacity, 64 * 1024);
        assert_eq!(config.response_buffer_capacity, 64 * 1024);
        assert_eq!(config.max_frame_len, 16 * 1024 * 1024);
        assert_eq!(config.request_body_mode, TcpRequestBodyMode::Buffered);
        assert_eq!(config.response_write_policy, ResponseWritePolicy::Immediate);
    }

    #[test]
    fn builders_preserve_minimum_valid_capacities() {
        let config = NacelleTcpConfig::default()
            .with_read_buffer_capacity(1)
            .with_response_buffer_capacity(1)
            .with_max_frame_len(1)
            .with_request_body_chunk_size(0)
            .with_request_body_channel_capacity(0)
            .with_request_body_mode(TcpRequestBodyMode::Streaming);
        let config = config.with_response_write_policy(ResponseWritePolicy::FlushAtBytes(0));

        assert_eq!(config.read_buffer_capacity, 1024);
        assert_eq!(config.response_buffer_capacity, 1024);
        assert_eq!(config.max_frame_len, 24);
        assert_eq!(config.request_body_chunk_size, 1);
        assert_eq!(config.request_body_channel_capacity, 1);
        assert_eq!(
            config.response_write_policy,
            ResponseWritePolicy::FlushAtBytes(0)
        );
        assert_eq!(config.request_body_mode, TcpRequestBodyMode::Streaming);
    }
}
