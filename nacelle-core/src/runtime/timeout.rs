use std::future::{Future, Pending, pending};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::time::{Instant, Timeout, error::Elapsed};

/// Bound an operation without constructing a timer when its first poll completes.
///
/// The deadline starts at construction, not at the first pending poll. As with
/// Tokio's timeout, ready output takes priority over an expired deadline.
/// Pending operations require a Tokio runtime with its time driver enabled.
pub fn timeout<F: Future>(
    duration: Duration,
    future: F,
) -> impl Future<Output = Result<F::Output, Elapsed>> {
    ReadyTimeout {
        future,
        deadline: Instant::now().checked_add(duration),
        timer: None,
    }
}

pin_project! {
    struct ReadyTimeout<F> {
        #[pin]
        future: F,
        deadline: Option<Instant>,
        #[pin]
        timer: Option<Timeout<Pending<()>>>,
    }
}

impl<F: Future> Future for ReadyTimeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let had_budget = tokio::task::coop::has_budget_remaining();
        if let Poll::Ready(output) = this.future.poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if this.timer.is_none() {
            let timer = match *this.deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, pending()),
                None => tokio::time::timeout(Duration::MAX, pending()),
            };
            this.timer.set(Some(timer));
        }
        let timer = this
            .timer
            .as_pin_mut()
            .expect("pending operation has a timer");
        // An operation that exhausts its cooperative budget must not starve
        // the deadline. Tokio's timeout makes the same exception.
        let result = if had_budget && !tokio::task::coop::has_budget_remaining() {
            Pin::new(&mut tokio::task::coop::unconstrained(timer)).poll(cx)
        } else {
            timer.poll(cx)
        };
        match result {
            Poll::Ready(Err(elapsed)) => Poll::Ready(Err(elapsed)),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => unreachable!("pending timer future cannot complete"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;

    use super::*;

    #[test]
    fn ready_operation_needs_no_timer_and_wins_at_zero_timeout() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(
            runtime
                .block_on(timeout(Duration::ZERO, async { 7 }))
                .unwrap(),
            7
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_includes_delay_before_first_poll_and_is_not_reset() {
        let deadline = Instant::now() + Duration::from_secs(5);
        let operation = timeout(Duration::from_secs(5), pending::<()>());
        tokio::pin!(operation);
        tokio::time::advance(Duration::from_secs(3)).await;
        assert!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert!(operation.await.is_err());
        assert!(Instant::now() >= deadline);
        assert!(Instant::now() < deadline + Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn pending_operation_is_polled_once_per_poll_and_dropped_on_cancellation() {
        struct DropCheck(Arc<AtomicUsize>);
        impl Drop for DropCheck {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let dropped = Arc::new(AtomicUsize::new(0));
        let owned = DropCheck(dropped.clone());
        let polls = AtomicUsize::new(0);
        let future = poll_fn(|_| {
            polls.fetch_add(1, Ordering::Relaxed);
            Poll::<()>::Pending
        });
        let mut operation = Box::pin(timeout(Duration::from_secs(5), async move {
            let _owned = owned;
            future.await;
        }));
        assert!(
            operation
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        drop(operation);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_cooperative_budget_does_not_hide_expired_deadline() {
        let operation = async {
            while tokio::task::coop::has_budget_remaining() {
                tokio::task::consume_budget().await;
            }
            pending::<()>().await;
        };
        assert!(timeout(Duration::ZERO, operation).await.is_err());
    }
}
