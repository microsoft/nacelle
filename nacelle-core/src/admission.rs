//! Concrete application admission, separate from transport scheduling.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::error::{NacelleConfigurationError, NacelleError, NacelleResourceLimitReason};
use crate::pipeline::ConnectionContext;

/// Bounded, transport-neutral application rejection categories.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejection {
    /// Application policy forbids this request.
    Forbidden,
    /// Application rate or quota was exceeded.
    RateLimited,
    /// Application capacity is unavailable.
    Unavailable,
}

impl AdmissionRejection {
    /// Stable, low-cardinality telemetry reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forbidden => "admission_forbidden",
            Self::RateLimited => "admission_rate_limited",
            Self::Unavailable => "admission_unavailable",
        }
    }
}

impl std::fmt::Display for AdmissionRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Borrowed head, connection, and application state before body dispatch.
///
/// Exclusive borrows allow a `Send` but non-`Sync` head or serial connection
/// state to cross an await. Policies inspect these through shared accessors;
/// they cannot change the head after the transport's size checks.
pub struct AdmissionContext<'a, Head, State, AppState = ()> {
    head: &'a mut Head,
    connection: &'a mut ConnectionContext<State>,
    app_state: &'a AppState,
}

impl<'a, Head, State, AppState> AdmissionContext<'a, Head, State, AppState> {
    /// Construct a transport-owned borrowing context.
    pub const fn new(
        head: &'a mut Head,
        connection: &'a mut ConnectionContext<State>,
        app_state: &'a AppState,
    ) -> Self {
        Self {
            head,
            connection,
            app_state,
        }
    }

    /// Inspect the decoded transport head.
    pub const fn head(&self) -> &Head {
        self.head
    }

    /// Inspect connection metadata and concrete state.
    pub const fn connection(&self) -> &ConnectionContext<State> {
        self.connection
    }

    /// Borrow the typed application dependency root.
    pub const fn app_state(&self) -> &AppState {
        self.app_state
    }

    /// Install application state without cloning the head or connection.
    #[doc(hidden)]
    pub fn map_app_state<Next>(
        self,
        app_state: &'a Next,
    ) -> AdmissionContext<'a, Head, State, Next> {
        AdmissionContext::new(self.head, self.connection, app_state)
    }
}

/// Shared-runtime policy returning an owned application permit.
///
/// The permit protects handler work, request-body resolution, and response
/// production, not remote receipt or socket flush. Implementations must not
/// block the runtime. Pending futures are cancelled by dropping the same
/// future; queue discipline belongs to the policy.
pub trait Admission<Context>: Send + Sync + 'static {
    /// Owned production capacity; use `()` for a rule without capacity.
    type Permit: Send + 'static;

    /// Allow, reject, or asynchronously wait before any application body work.
    fn admit(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + Send;
}

/// Worker-local admission permitting `!Send` policy state and futures.
#[allow(clippy::future_not_send)]
pub trait LocalAdmission<Context> {
    /// Owned capacity, released at the same boundary as shared admission.
    type Permit: 'static;

    /// Decide before the transport starts application body work.
    fn admit(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>>;
}

impl<Context, A: Admission<Context>> LocalAdmission<Context> for A {
    type Permit = A::Permit;

    fn admit(
        &self,
        context: Context,
    ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> {
        Admission::admit(self, context)
    }
}

/// Zero-sized immediate allowance without an application lease.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAll;

impl<Context> Admission<Context> for AllowAll {
    type Permit = ();

    fn admit(
        &self,
        _context: Context,
    ) -> impl Future<Output = Result<(), AdmissionRejection>> + Send {
        ready(Ok(()))
    }
}

/// Finite pending-admission limits for one configured server.
///
/// Server clones share waiter accounting. Independently constructed servers
/// and worker-local factories have separate bounds. Existing runtime request
/// capacity also applies. No timer is created for an immediately ready policy.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionConfig {
    timeout: Duration,
    max_pending: usize,
}

impl AdmissionConfig {
    /// Validate a positive representable deadline and finite nonzero capacity.
    ///
    /// # Errors
    ///
    /// Rejects zero/overflowing timeouts and zero or `usize::MAX` capacity.
    pub fn new(timeout: Duration, max_pending: usize) -> Result<Self, NacelleConfigurationError> {
        if timeout.is_zero()
            || std::time::Instant::now().checked_add(timeout).is_none()
            || max_pending == 0
            || max_pending == usize::MAX
        {
            return Err(NacelleConfigurationError::AdmissionLimits);
        }
        Ok(Self {
            timeout,
            max_pending,
        })
    }

    /// Deadline measured from the first pending poll, including pre-wait flush.
    pub const fn timeout(self) -> Duration {
        self.timeout
    }

    /// Maximum concurrent policy invocations, including queued acquisitions.
    pub const fn max_pending(self) -> usize {
        self.max_pending
    }
}

/// Transport-owned policy accounting. Not an application scheduling extension.
#[doc(hidden)]
pub struct AdmissionState {
    config: AdmissionConfig,
    pending: AtomicUsize,
}

impl AdmissionState {
    pub const fn new(config: AdmissionConfig) -> Self {
        Self {
            config,
            pending: AtomicUsize::new(0),
        }
    }

    pub const fn config(&self) -> AdmissionConfig {
        self.config
    }

    pub fn acquire(&self) -> Result<AdmissionWaiter<'_>, NacelleError> {
        self.pending
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |pending| {
                (pending < self.config.max_pending).then_some(pending + 1)
            })
            .map_err(|_| {
                NacelleError::ResourceLimit(NacelleResourceLimitReason::AdmissionWaiters)
            })?;
        Ok(AdmissionWaiter(self))
    }
}

#[doc(hidden)]
pub struct AdmissionWaiter<'a>(&'a AdmissionState);

impl Drop for AdmissionWaiter<'_> {
    fn drop(&mut self) {
        self.0.pending.fetch_sub(1, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    use super::*;
    use crate::pipeline::ConnectionInfo;
    use crate::request::NacelleConnectionMeta;

    struct Borrowing;

    impl<'a> Admission<AdmissionContext<'a, Cell<u8>, Cell<u8>, Arc<Semaphore>>> for Borrowing {
        type Permit = tokio::sync::OwnedSemaphorePermit;

        async fn admit(
            &self,
            context: AdmissionContext<'a, Cell<u8>, Cell<u8>, Arc<Semaphore>>,
        ) -> Result<Self::Permit, AdmissionRejection> {
            let permit = context
                .app_state()
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| AdmissionRejection::Unavailable)?;
            assert_eq!(context.head().get(), context.connection().state.get());
            Ok(permit)
        }
    }

    struct LocalRule(Rc<Cell<usize>>);

    impl<'a> LocalAdmission<AdmissionContext<'a, Cell<u8>, Rc<Cell<u8>>>> for LocalRule {
        type Permit = Rc<Cell<usize>>;

        async fn admit(
            &self,
            context: AdmissionContext<'a, Cell<u8>, Rc<Cell<u8>>>,
        ) -> Result<Self::Permit, AdmissionRejection> {
            tokio::task::yield_now().await;
            self.0.set(usize::from(context.head().get()));
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn borrowing_send_only_head_and_state_survive_wait() {
        fn require_send<T: Send>(value: T) -> T {
            value
        }
        let semaphore = Arc::new(Semaphore::new(1));
        let mut head = Cell::new(3);
        let mut connection = ConnectionContext::new(
            ConnectionInfo::from(&NacelleConnectionMeta::tcp(None, None)),
            Cell::new(3),
        );
        let permit = require_send(Admission::admit(
            &Borrowing,
            AdmissionContext::new(&mut head, &mut connection, &semaphore),
        ))
        .await
        .expect("admission");
        assert_eq!(semaphore.available_permits(), 0);
        drop(permit);
        assert_eq!(semaphore.available_permits(), 1);
        let mut local_connection = connection.map_state(Rc::new);
        let local = LocalRule(Rc::new(Cell::new(0)));
        let permit = local
            .admit(AdmissionContext::new(&mut head, &mut local_connection, &()))
            .await
            .expect("local admission");
        assert_eq!(permit.get(), 3);
    }

    #[test]
    fn finite_waiters_release_on_cancellation() {
        let state = AdmissionState::new(AdmissionConfig::new(Duration::from_secs(1), 1).unwrap());
        let waiter = state.acquire().unwrap();
        assert!(matches!(
            state.acquire(),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::AdmissionWaiters
            ))
        ));
        drop(waiter);
        assert!(state.acquire().is_ok());
        assert!(AdmissionConfig::new(Duration::ZERO, 1).is_err());
        assert!(AdmissionConfig::new(Duration::MAX, 1).is_err());
        assert!(AdmissionConfig::new(Duration::from_secs(1), usize::MAX).is_err());
    }
}
