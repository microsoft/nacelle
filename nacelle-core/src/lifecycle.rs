use tokio::sync::watch;

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct NacelleDrainDeadline {
    millis: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct NacelleShutdown {
    sender: watch::Sender<bool>,
}

#[derive(Debug, Clone)]
pub struct NacelleShutdownToken {
    receiver: watch::Receiver<bool>,
}

impl Default for NacelleShutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for NacelleDrainDeadline {
    fn default() -> Self {
        Self::new(std::time::Duration::from_secs(30))
    }
}

impl NacelleDrainDeadline {
    #[doc(hidden)]
    pub fn new(timeout: std::time::Duration) -> Self {
        Self {
            millis: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(duration_millis(
                timeout,
            ))),
        }
    }

    #[doc(hidden)]
    pub fn set(&self, timeout: std::time::Duration) {
        self.millis.store(
            duration_millis(timeout),
            std::sync::atomic::Ordering::Release,
        );
    }

    #[doc(hidden)]
    pub fn get(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.millis.load(std::sync::atomic::Ordering::Acquire))
    }
}

fn duration_millis(timeout: std::time::Duration) -> u64 {
    timeout.as_millis().min(u128::from(u64::MAX)) as u64
}

impl NacelleShutdown {
    pub fn new() -> Self {
        Self::pair().0
    }

    pub fn pair() -> (Self, NacelleShutdownToken) {
        let (sender, receiver) = watch::channel(false);
        (Self { sender }, NacelleShutdownToken { receiver })
    }

    pub fn shutdown(&self) {
        self.sender.send_replace(true);
    }

    pub fn token(&self) -> NacelleShutdownToken {
        NacelleShutdownToken {
            receiver: self.sender.subscribe(),
        }
    }

    pub fn from_cancellation_token(token: tokio_util::sync::CancellationToken) -> Self {
        let shutdown = Self::new();
        let relay = shutdown.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            relay.shutdown();
        });
        shutdown
    }
}

impl NacelleShutdownToken {
    pub fn is_shutdown(&self) -> bool {
        *self.receiver.borrow()
    }

    pub async fn changed(&mut self) -> bool {
        if self.is_shutdown() {
            return true;
        }
        self.receiver.changed().await.is_ok() && self.is_shutdown()
    }

    pub fn from_cancellation_token(token: tokio_util::sync::CancellationToken) -> Self {
        NacelleShutdown::from_cancellation_token(token).token()
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn token_created_after_shutdown_is_immediately_ready() {
        let shutdown = super::NacelleShutdown::new();
        shutdown.shutdown();
        let mut token = shutdown.token();

        let observed = tokio::time::timeout(std::time::Duration::from_millis(25), token.changed())
            .await
            .expect("already-shutdown token should not wait");
        assert!(observed);
    }

    #[tokio::test]
    async fn cancellation_token_triggers_shutdown_token() {
        let cancellation = tokio_util::sync::CancellationToken::new();
        let mut token = super::NacelleShutdownToken::from_cancellation_token(cancellation.clone());

        cancellation.cancel();

        let observed = tokio::time::timeout(std::time::Duration::from_secs(1), token.changed())
            .await
            .expect("cancellation should trigger shutdown");
        assert!(observed);
    }
}
