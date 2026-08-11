use std::time::Duration;

/// TCP socket and connection-finalization timeouts.
///
/// Construct this with [`Default`] and the `with_*` builders so newly added
/// limits retain their defaults.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct NacelleTcpLimits {
    pub read_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    /// Deadline for final writer shutdown after a connection result is known.
    pub shutdown_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
}

impl Default for NacelleTcpLimits {
    fn default() -> Self {
        Self {
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            shutdown_timeout: Some(Duration::from_secs(30)),
            idle_timeout: Some(Duration::from_secs(120)),
        }
    }
}

impl NacelleTcpLimits {
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    pub fn without_read_timeout(mut self) -> Self {
        self.read_timeout = None;
        self
    }

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    pub fn without_write_timeout(mut self) -> Self {
        self.write_timeout = None;
        self
    }

    /// Set the writer-finalization deadline independently of response writes.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    pub fn without_shutdown_timeout(mut self) -> Self {
        self.shutdown_timeout = None;
        self
    }

    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tcp_limits_default_to_bounded_socket_timeouts() {
        let limits = NacelleTcpLimits::default();

        assert_eq!(limits.read_timeout, Some(Duration::from_secs(30)));
        assert_eq!(limits.write_timeout, Some(Duration::from_secs(30)));
        assert_eq!(limits.shutdown_timeout, Some(Duration::from_secs(30)));
        assert_eq!(limits.idle_timeout, Some(Duration::from_secs(120)));
    }

    #[test]
    fn tcp_timeouts_can_be_disabled() {
        let limits = NacelleTcpLimits::default()
            .without_read_timeout()
            .without_write_timeout()
            .without_shutdown_timeout()
            .without_idle_timeout();

        assert_eq!(limits.read_timeout, None);
        assert_eq!(limits.write_timeout, None);
        assert_eq!(limits.shutdown_timeout, None);
        assert_eq!(limits.idle_timeout, None);
    }
}
