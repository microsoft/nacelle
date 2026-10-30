use std::time::Duration;

/// Invalid Hyper parser limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpParserLimitsError;

impl std::fmt::Display for HttpParserLimitsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("HTTP parser buffer must be at least 8192 bytes and finite; header count must be positive and finite")
    }
}

impl std::error::Error for HttpParserLimitsError {}

/// Hyper parser bounds, separate from post-parse HTTP policy checks.
///
/// Defaults retain Hyper's bounded buffer strategy and 100-header parser.
/// A custom header count selects Hyper's configurable-count path. In the
/// resolved implementation, counts through 100 use inline header arrays; larger
/// counts allocate those arrays. The buffer limit is not an exact read
/// allowance: a complete head may arrive with body bytes beyond the configured
/// threshold.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpParserLimits {
    max_buffer_bytes: Option<usize>,
    max_headers: Option<usize>,
}

impl HttpParserLimits {
    /// Configure parser buffer and optional header-count bounds.
    ///
    /// # Errors
    ///
    /// Hyper requires at least 8192 buffer bytes. Counts must be positive and
    /// fit a finite parser allocation; `None` retains its 100-header default.
    pub const fn new(
        max_buffer_bytes: usize,
        max_headers: Option<usize>,
    ) -> Result<Self, HttpParserLimitsError> {
        if max_buffer_bytes < 8192 || max_buffer_bytes > isize::MAX as usize {
            return Err(HttpParserLimitsError);
        }
        if let Some(count) = max_headers
            && (count == 0 || count > isize::MAX as usize / (4 * size_of::<usize>()))
        {
            return Err(HttpParserLimitsError);
        }
        Ok(Self {
            max_buffer_bytes: Some(max_buffer_bytes),
            max_headers,
        })
    }

    pub(crate) fn apply(self, builder: &mut hyper::server::conn::http1::Builder) {
        if let Some(bytes) = self.max_buffer_bytes {
            builder.max_buf_size(bytes);
        }
        if let Some(count) = self.max_headers
            && count != 100
        {
            builder.max_headers(count);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NacelleHttpLimits {
    /// Parser allocation/read-ahead strategy, distinct from application policy.
    pub parser: HttpParserLimits,
    pub header_read_timeout: Option<Duration>,
    pub request_body_read_timeout: Option<Duration>,
    pub response_write_timeout: Option<Duration>,
    pub keep_alive: bool,
    pub max_connection_age: Option<Duration>,
}

impl Default for NacelleHttpLimits {
    fn default() -> Self {
        Self {
            parser: HttpParserLimits::default(),
            header_read_timeout: Some(Duration::from_secs(30)),
            request_body_read_timeout: Some(Duration::from_secs(30)),
            response_write_timeout: Some(Duration::from_secs(30)),
            keep_alive: true,
            max_connection_age: None,
        }
    }
}

impl NacelleHttpLimits {
    /// Set validated Hyper parser limits.
    pub const fn with_parser_limits(mut self, parser: HttpParserLimits) -> Self {
        self.parser = parser;
        self
    }

    pub fn with_header_read_timeout(mut self, timeout: Duration) -> Self {
        self.header_read_timeout = Some(timeout);
        self
    }

    pub fn without_header_read_timeout(mut self) -> Self {
        self.header_read_timeout = None;
        self
    }

    pub fn with_request_body_read_timeout(mut self, timeout: Duration) -> Self {
        self.request_body_read_timeout = Some(timeout);
        self
    }

    pub fn without_request_body_read_timeout(mut self) -> Self {
        self.request_body_read_timeout = None;
        self
    }

    pub fn with_response_write_timeout(mut self, timeout: Duration) -> Self {
        self.response_write_timeout = Some(timeout);
        self
    }

    pub fn without_response_write_timeout(mut self) -> Self {
        self.response_write_timeout = None;
        self
    }

    pub fn with_keep_alive(mut self, keep_alive: bool) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn with_max_connection_age(mut self, max_age: Duration) -> Self {
        self.max_connection_age = Some(max_age);
        self
    }

    pub fn without_max_connection_age(mut self) -> Self {
        self.max_connection_age = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_parser_bounds_fail_before_hyper_construction() {
        for (bytes, headers) in [
            (8191, None),
            (usize::MAX, None),
            (8192, Some(0)),
            (8192, Some(usize::MAX)),
        ] {
            assert!(HttpParserLimits::new(bytes, headers).is_err());
        }
        assert!(HttpParserLimits::new(8192, None).is_ok());
        assert!(HttpParserLimits::new(8192, Some(1)).is_ok());
    }

    #[test]
    fn http_limits_default_to_bounded_edge_timeouts() {
        let limits = NacelleHttpLimits::default();

        assert_eq!(limits.header_read_timeout, Some(Duration::from_secs(30)));
        assert_eq!(
            limits.request_body_read_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(limits.response_write_timeout, Some(Duration::from_secs(30)));
        assert!(limits.keep_alive);
        assert_eq!(limits.max_connection_age, None);
    }

    #[test]
    fn http_timeouts_and_connection_age_can_be_disabled() {
        let limits = NacelleHttpLimits::default()
            .without_header_read_timeout()
            .without_request_body_read_timeout()
            .without_response_write_timeout()
            .with_max_connection_age(Duration::from_secs(1))
            .without_max_connection_age();

        assert_eq!(limits.header_read_timeout, None);
        assert_eq!(limits.request_body_read_timeout, None);
        assert_eq!(limits.response_write_timeout, None);
        assert_eq!(limits.max_connection_age, None);
    }
}
