use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct NacelleHttpLimits {
    pub header_read_timeout: Option<Duration>,
    pub request_body_read_timeout: Option<Duration>,
    pub response_write_timeout: Option<Duration>,
    pub keep_alive: bool,
    pub max_connection_age: Option<Duration>,
}

impl Default for NacelleHttpLimits {
    fn default() -> Self {
        Self {
            header_read_timeout: Some(Duration::from_secs(30)),
            request_body_read_timeout: Some(Duration::from_secs(30)),
            response_write_timeout: Some(Duration::from_secs(30)),
            keep_alive: true,
            max_connection_age: None,
        }
    }
}

impl NacelleHttpLimits {
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
