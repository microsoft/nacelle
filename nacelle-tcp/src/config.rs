/// Invalid TCP head limits: require `0 < H <= B <= isize::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpHeadReadLimitsError;

impl std::fmt::Display for TcpHeadReadLimitsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(
            "TCP head limits require 0 < max_head_bytes <= max_undecided_bytes <= isize::MAX",
        )
    }
}

impl std::error::Error for TcpHeadReadLimitsError {}

/// Opt-in limits on encoded heads and cumulative undecided plaintext.
///
/// `H` bounds the input visible to the decoder. `B` includes the head and any
/// body or following-request prefix returned by `AsyncRead`. Neither bounds
/// allocation capacity, retained shared slices, decoder-owned allocations, TLS
/// transport buffers, or kernel buffers. Admitted bodies retain their separate
/// frame/body limits.
///
/// ```
/// use nacelle_tcp::{NacelleTcpConfig, TcpHeadReadLimits, TcpHeadReadLimitsError};
///
/// let config = NacelleTcpConfig::default()
///     .with_head_read_limits(TcpHeadReadLimits::new(24, 4096)?);
/// # let _ = config;
/// # Ok::<(), TcpHeadReadLimitsError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpHeadReadLimits {
    max_head_bytes: usize,
    max_undecided_bytes: usize,
}

impl TcpHeadReadLimits {
    /// Validate the maximum encoded head size `H` and undecided-byte budget `B`.
    ///
    /// # Errors
    ///
    /// Returns [`TcpHeadReadLimitsError`] unless `0 < H <= B <= isize::MAX`.
    pub const fn new(
        max_head_bytes: usize,
        max_undecided_bytes: usize,
    ) -> Result<Self, TcpHeadReadLimitsError> {
        if max_head_bytes == 0
            || max_undecided_bytes < max_head_bytes
            || max_undecided_bytes > isize::MAX as usize
        {
            return Err(TcpHeadReadLimitsError);
        }
        Ok(Self {
            max_head_bytes,
            max_undecided_bytes,
        })
    }

    /// Maximum encoded head size (`H`).
    #[must_use]
    pub const fn max_head_bytes(self) -> usize {
        self.max_head_bytes
    }

    /// Maximum cumulative undecided plaintext bytes (`B`).
    #[must_use]
    pub const fn max_undecided_bytes(self) -> usize {
        self.max_undecided_bytes
    }
}

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
    /// Coalesce complete frames until pending bytes reach or cross
    /// `response_buffer_capacity`.
    CoalesceBuffered,
    /// Coalesce complete frames until at least the configured byte threshold.
    FlushAtBytes(usize),
}

/// TCP framing, buffering, and request-body delivery configuration.
///
/// Construct this with [`Default`] and the `with_*` builders so newly added
/// settings retain their defaults.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NacelleTcpConfig {
    /// Initial cumulative socket read-buffer capacity.
    pub read_buffer_capacity: usize,
    /// Initial coalesced response write-buffer capacity.
    pub response_buffer_capacity: usize,
    /// Maximum accepted protocol frame length.
    pub max_frame_len: usize,
    /// Optional encoded-head and cumulative undecided-plaintext limits.
    ///
    /// Defaults to `None`, retaining unbounded decoder read-ahead and batching.
    /// Custom decoders must bound their own allocations in either mode.
    pub head_read_limits: Option<TcpHeadReadLimits>,
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
            head_read_limits: None,
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

    /// Bound request-head reads without restricting admitted body size.
    ///
    /// A complete head exactly at `H` succeeds; needing more input at `H`
    /// rejects the connection. EOF-delimited heads therefore need spare space
    /// below `H` to observe transport EOF. Reads stop once a head is decoded,
    /// until the existing body-limit and runtime checks permit continuation.
    pub const fn with_head_read_limits(mut self, limits: TcpHeadReadLimits) -> Self {
        self.head_read_limits = Some(limits);
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
        self.response_write_policy = match policy {
            ResponseWritePolicy::FlushAtBytes(bytes) => {
                ResponseWritePolicy::FlushAtBytes(bytes.max(1))
            }
            policy => policy,
        };
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_limits_reject_invalid_bounds() {
        for (head, undecided) in [
            (0, 0),
            (0, 8),
            (9, 8),
            (1, usize::MAX),
            (usize::MAX, usize::MAX),
        ] {
            assert_eq!(
                TcpHeadReadLimits::new(head, undecided),
                Err(TcpHeadReadLimitsError)
            );
        }
        assert!(TcpHeadReadLimits::new(8, 8).is_ok());
        assert!(TcpHeadReadLimits::new(8, 16).is_ok());
    }

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
            ResponseWritePolicy::FlushAtBytes(1)
        );
        assert_eq!(config.request_body_mode, TcpRequestBodyMode::Streaming);
    }
}
