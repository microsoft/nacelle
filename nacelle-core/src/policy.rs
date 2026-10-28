//! Statically composed admission, resource leases, and opt-in cancellation.
//!
//! Initialize policies before serving. Admission futures run in the calling
//! transport task; they must not block its executor. A pending future pauses
//! only that checkpoint. Use [`bounded_wait`] to cap its waiters and deadline.

use std::fmt;
use std::future::{Future, ready};
use std::net::IpAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::sync::Semaphore;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use crate::{NacelleError, NacellePeerRateLimitResult, NacellePeerRateLimiter};
use crate::{NacelleResourceLimitReason, NacelleTimeoutReason};

/// Marker retained for applications that group their policy types.
///
/// Implement [`Admission`] or [`LocalAdmission`] to execute a policy.
pub trait NacellePolicy {}

/// Transport-neutral rejection category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RejectionKind {
    /// Request-rate capacity was exhausted.
    RateLimited,
    /// Concurrent work or waiting capacity was exhausted.
    Overloaded,
    /// An application predicate denied admission.
    Denied,
    /// Admission or active work was cancelled.
    Closed,
    /// The configured policy wait expired.
    Timeout,
}

/// An admission failure with a bounded, application-selected reason vocabulary.
///
/// Reasons must not contain secrets or peer-controlled values. Transports map
/// the category to a response when possible; active cancellation closes I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection {
    kind: RejectionKind,
    reason: &'static str,
}

impl Rejection {
    /// Construct a rejection without allocating.
    pub const fn new(kind: RejectionKind, reason: &'static str) -> Self {
        Self { kind, reason }
    }

    /// Return the transport-neutral category.
    pub const fn kind(self) -> RejectionKind {
        self.kind
    }

    /// Return the static diagnostic reason.
    pub const fn reason(self) -> &'static str {
        self.reason
    }

    /// Map to the existing TCP/framework error boundary.
    pub const fn into_error(self) -> NacelleError {
        match self.kind {
            RejectionKind::Timeout => {
                NacelleError::Timeout(NacelleTimeoutReason::Other(self.reason))
            }
            _ => NacelleError::ResourceLimit(NacelleResourceLimitReason::Other(self.reason)),
        }
    }
}

impl fmt::Display for Rejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.reason)
    }
}

impl std::error::Error for Rejection {}

mod sealed {
    pub trait Lease {}
}

/// A library-defined resource lifetime, released synchronously on drop.
///
/// Use `()` for a decision with no retained resource and [`Hold`] to retain an
/// application guard. Consumers cannot accidentally infer delivery requirements
/// from the size or representation of a custom guard.
pub trait PolicyLease: sealed::Lease {
    /// Whether a TCP response must be flushed before releasing this lease.
    fn requires_flush(&self) -> bool;
}

impl sealed::Lease for () {}
impl PolicyLease for () {
    fn requires_flush(&self) -> bool {
        false
    }
}

/// Retain a custom resource guard for the checkpoint's documented lifetime.
///
/// The contained guard's `Drop` releases capacity. Rate tokens should normally
/// return `()` instead: finishing a request does not refund its rate charge.
#[derive(Debug)]
pub struct Hold<T>(pub T);

impl<T> Hold<T> {
    /// Wrap an owned resource guard.
    pub const fn new(guard: T) -> Self {
        Self(guard)
    }
}

impl<T> sealed::Lease for Hold<T> {}
impl<T> PolicyLease for Hold<T> {
    fn requires_flush(&self) -> bool {
        true
    }
}

/// Composite leases released in reverse acquisition order.
#[derive(Debug)]
pub struct LeasePair<A, B> {
    // Rust drops fields in declaration order.
    second: B,
    first: A,
}

impl<A, B> sealed::Lease for LeasePair<A, B> {}
impl<A: PolicyLease, B: PolicyLease> PolicyLease for LeasePair<A, B> {
    fn requires_flush(&self) -> bool {
        self.first.requires_flush() || self.second.requires_flush()
    }
}

/// Admit work on a shared executor without boxing its future.
///
/// Inputs are exclusively reborrowed for composition; transport input types
/// expose only read access. Leases must not borrow the input. Return `()` for
/// decision-only checks, [`Hold`] for a resource, or [`Rejection`] to stop.
///
/// ```rust
/// use std::cell::Cell;
/// use nacelle_core::policy::{Admission, Rejection, RejectionKind};
///
/// struct RejectZero;
///
/// impl Admission<Cell<usize>> for RejectZero {
///     type Lease = ();
///
///     async fn enter<'a>(&'a self, input: &'a mut Cell<usize>)
///         -> Result<(), Rejection>
///     {
///         if input.get() == 0 {
///             Err(Rejection::new(RejectionKind::Denied, "zero"))
///         } else {
///             Ok(())
///         }
///     }
/// }
/// ```
///
/// Shared admission cannot retain worker-local guards:
///
/// ```compile_fail
/// use std::rc::Rc;
/// use nacelle_core::policy::{Admission, Hold, Rejection};
///
/// struct LocalGuard;
/// impl Admission<()> for LocalGuard {
///     type Lease = Hold<Rc<()>>;
///
///     async fn enter<'a>(&'a self, _: &'a mut ()) -> Result<Self::Lease, Rejection> {
///         Ok(Hold::new(Rc::new(())))
///     }
/// }
/// ```
pub trait Admission<Input: ?Sized>: Send + Sync {
    /// Owned resource retained for the checkpoint's scope.
    type Lease: PolicyLease + Send + 'static;

    /// Evaluate once; pending futures are retained rather than restarted.
    fn enter<'a>(
        &'a self,
        input: &'a mut Input,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a;
}

/// Admit work on a worker-local executor, allowing non-`Send` state and futures.
#[allow(clippy::future_not_send)]
pub trait LocalAdmission<Input: ?Sized> {
    /// Owned resource retained for the checkpoint's scope.
    type Lease: PolicyLease + 'static;

    /// Evaluate once on the current worker.
    fn enter<'a>(
        &'a self,
        input: &'a mut Input,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + 'a;
}

/// Nonblocking socket admission before a connection task is spawned.
///
/// Do not perform I/O, wait for capacity, or block the listener in this method.
pub trait SocketAdmission<Input: ?Sized>: Send + Sync {
    /// Connection-scoped resource reservation.
    type Lease: PolicyLease + Send + 'static;

    /// Check or reserve capacity synchronously.
    fn admit(&self, input: &Input) -> Result<Self::Lease, Rejection>;
}

/// Worker-local nonblocking socket admission.
pub trait LocalSocketAdmission<Input: ?Sized> {
    /// Worker-local connection reservation.
    type Lease: PolicyLease + 'static;

    /// Check capacity without blocking the listener.
    fn admit(&self, input: &Input) -> Result<Self::Lease, Rejection>;
}

impl<I: ?Sized, P: Admission<I> + ?Sized> Admission<I> for Arc<P> {
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        self.as_ref().enter(input)
    }
}

impl<I: ?Sized, P: SocketAdmission<I> + ?Sized> SocketAdmission<I> for Arc<P> {
    type Lease = P::Lease;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        self.as_ref().admit(input)
    }
}

impl<I: ?Sized, P: LocalAdmission<I> + ?Sized> LocalAdmission<I> for Rc<P> {
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + 'a {
        self.as_ref().enter(input)
    }
}

impl<I: ?Sized, P: LocalSocketAdmission<I> + ?Sized> LocalSocketAdmission<I> for Rc<P> {
    type Lease = P::Lease;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        self.as_ref().admit(input)
    }
}

/// Adapt a shared policy to a local checkpoint without changing its algorithm.
#[derive(Debug, Clone, Copy)]
pub struct Shared<P>(pub P);

impl<I: ?Sized, P: Admission<I>> LocalAdmission<I> for Shared<P> {
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + 'a {
        self.0.enter(input)
    }
}

impl<I: ?Sized, P: SocketAdmission<I>> LocalSocketAdmission<I> for Shared<P> {
    type Lease = P::Lease;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        self.0.admit(input)
    }
}

/// Empty checkpoint; carries no allocation, capacity, or diagnostic state.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoPolicy;

impl<I: ?Sized> Admission<I> for NoPolicy {
    type Lease = ();

    fn enter<'a>(
        &'a self,
        _: &'a mut I,
    ) -> impl Future<Output = Result<(), Rejection>> + Send + 'a {
        ready(Ok(()))
    }
}

impl<I: ?Sized> LocalAdmission<I> for NoPolicy {
    type Lease = ();

    fn enter<'a>(&'a self, _: &'a mut I) -> impl Future<Output = Result<(), Rejection>> + 'a {
        ready(Ok(()))
    }
}

impl<I: ?Sized> SocketAdmission<I> for NoPolicy {
    type Lease = ();

    fn admit(&self, _: &I) -> Result<(), Rejection> {
        Ok(())
    }
}

impl<I: ?Sized> LocalSocketAdmission<I> for NoPolicy {
    type Lease = ();

    fn admit(&self, _: &I) -> Result<(), Rejection> {
        Ok(())
    }
}

/// Sequential composition; the second policy runs only after the first admits.
///
/// A later rejection releases earlier leases. Other side effects, including
/// rate charges, are not rolled back.
#[derive(Debug, Clone, Copy)]
pub struct Chain<A, B> {
    first: A,
    second: B,
}

impl<A, B> Chain<A, B> {
    /// Compose two policies in registration order.
    pub const fn new(first: A, second: B) -> Self {
        Self { first, second }
    }
}

impl<I: ?Sized + Send, A: Admission<I>, B: Admission<I>> Admission<I> for Chain<A, B> {
    type Lease = LeasePair<A::Lease, B::Lease>;

    async fn enter<'a>(&'a self, input: &'a mut I) -> Result<Self::Lease, Rejection> {
        let first = self.first.enter(input).await?;
        let second = self.second.enter(input).await?;
        Ok(LeasePair { second, first })
    }
}

#[allow(clippy::future_not_send)]
impl<I: ?Sized, A: LocalAdmission<I>, B: LocalAdmission<I>> LocalAdmission<I> for Chain<A, B> {
    type Lease = LeasePair<A::Lease, B::Lease>;

    async fn enter<'a>(&'a self, input: &'a mut I) -> Result<Self::Lease, Rejection> {
        let first = self.first.enter(input).await?;
        let second = self.second.enter(input).await?;
        Ok(LeasePair { second, first })
    }
}

impl<I: ?Sized, A: SocketAdmission<I>, B: SocketAdmission<I>> SocketAdmission<I> for Chain<A, B> {
    type Lease = LeasePair<A::Lease, B::Lease>;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        let first = self.first.admit(input)?;
        let second = self.second.admit(input)?;
        Ok(LeasePair { second, first })
    }
}

impl<I: ?Sized, A: LocalSocketAdmission<I>, B: LocalSocketAdmission<I>> LocalSocketAdmission<I>
    for Chain<A, B>
{
    type Lease = LeasePair<A::Lease, B::Lease>;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        let first = self.first.admit(input)?;
        let second = self.second.admit(input)?;
        Ok(LeasePair { second, first })
    }
}

/// A synchronous decision-only policy.
#[derive(Debug, Clone, Copy)]
pub struct CheckFn<F>(F);

/// Adapt a synchronous predicate without allocating a future.
pub const fn check_fn<F>(function: F) -> CheckFn<F> {
    CheckFn(function)
}

impl<I: ?Sized, F> Admission<I> for CheckFn<F>
where
    F: Fn(&I) -> Result<(), Rejection> + Send + Sync,
{
    type Lease = ();

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<(), Rejection>> + Send + 'a {
        ready((self.0)(input))
    }
}

impl<I: ?Sized, F> SocketAdmission<I> for CheckFn<F>
where
    F: Fn(&I) -> Result<(), Rejection> + Send + Sync,
{
    type Lease = ();

    fn admit(&self, input: &I) -> Result<(), Rejection> {
        (self.0)(input)
    }
}

/// A worker-local synchronous decision-only policy.
#[derive(Debug, Clone, Copy)]
pub struct LocalCheckFn<F>(F);

/// Adapt a worker-local synchronous predicate.
pub const fn local_check_fn<F>(function: F) -> LocalCheckFn<F> {
    LocalCheckFn(function)
}

impl<I: ?Sized, F: Fn(&I) -> Result<(), Rejection>> LocalAdmission<I> for LocalCheckFn<F> {
    type Lease = ();

    fn enter<'a>(&'a self, input: &'a mut I) -> impl Future<Output = Result<(), Rejection>> + 'a {
        ready((self.0)(input))
    }
}

impl<I: ?Sized, F: Fn(&I) -> Result<(), Rejection>> LocalSocketAdmission<I> for LocalCheckFn<F> {
    type Lease = ();

    fn admit(&self, input: &I) -> Result<(), Rejection> {
        (self.0)(input)
    }
}

/// Extract owned facts for a transport-neutral policy.
#[derive(Debug, Clone, Copy)]
pub struct MapInput<M, P> {
    map: M,
    policy: P,
}

/// Project facts synchronously when this policy's position is reached.
pub const fn map_input<M, P>(map: M, policy: P) -> MapInput<M, P> {
    MapInput { map, policy }
}

impl<I: ?Sized, O: Send + 'static, M, P> Admission<I> for MapInput<M, P>
where
    M: Fn(&I) -> O + Send + Sync,
    P: Admission<O>,
{
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        let mut owned = (self.map)(input);
        async move { self.policy.enter(&mut owned).await }
    }
}

impl<I: ?Sized, O, M, P> SocketAdmission<I> for MapInput<M, P>
where
    M: Fn(&I) -> O + Send + Sync,
    P: SocketAdmission<O>,
{
    type Lease = P::Lease;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        self.policy.admit(&(self.map)(input))
    }
}

/// Worker-local fact projection.
#[derive(Debug, Clone, Copy)]
pub struct LocalMapInput<M, P> {
    map: M,
    policy: P,
}

/// Project facts for a worker-local policy.
pub const fn local_map_input<M, P>(map: M, policy: P) -> LocalMapInput<M, P> {
    LocalMapInput { map, policy }
}

#[allow(clippy::future_not_send)]
impl<I: ?Sized, O: 'static, M: Fn(&I) -> O, P: LocalAdmission<O>> LocalAdmission<I>
    for LocalMapInput<M, P>
{
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + 'a {
        let mut owned = (self.map)(input);
        async move { self.policy.enter(&mut owned).await }
    }
}

impl<I: ?Sized, O, M: Fn(&I) -> O, P: LocalSocketAdmission<O>> LocalSocketAdmission<I>
    for LocalMapInput<M, P>
{
    type Lease = P::Lease;

    fn admit(&self, input: &I) -> Result<Self::Lease, Rejection> {
        self.policy.admit(&(self.map)(input))
    }
}

/// Bound the time and concurrent waiters for an asynchronous policy.
#[derive(Debug)]
pub struct BoundedWait<P> {
    policy: P,
    max_wait: Duration,
    waiters: Semaphore,
}

/// Add a waiter cap and deadline; an immediately ready policy creates no timer.
///
/// # Panics
///
/// Panics during setup if `max_waiters` exceeds [`Semaphore::MAX_PERMITS`].
pub fn bounded_wait<P>(policy: P, max_wait: Duration, max_waiters: usize) -> BoundedWait<P> {
    BoundedWait {
        policy,
        max_wait,
        waiters: Semaphore::new(max_waiters),
    }
}

async fn wait_bounded<L>(
    future: impl Future<Output = Result<L, Rejection>>,
    waiters: &Semaphore,
    max_wait: Duration,
) -> Result<L, Rejection> {
    let mut future = std::pin::pin!(future);
    if let Poll::Ready(result) =
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    {
        return result;
    }
    let _waiter = waiters
        .try_acquire()
        .map_err(|_| Rejection::new(RejectionKind::Overloaded, "policy_waiters"))?;
    tokio::time::timeout(max_wait, future)
        .await
        .map_err(|_| Rejection::new(RejectionKind::Timeout, "policy_wait"))?
}

impl<I: ?Sized, P: Admission<I>> Admission<I> for BoundedWait<P> {
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        wait_bounded(self.policy.enter(input), &self.waiters, self.max_wait)
    }
}

#[allow(clippy::future_not_send)]
impl<I: ?Sized, P: LocalAdmission<I>> LocalAdmission<I> for BoundedWait<P> {
    type Lease = P::Lease;

    fn enter<'a>(
        &'a self,
        input: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + 'a {
        wait_bounded(self.policy.enter(input), &self.waiters, self.max_wait)
    }
}

/// Shared fixed-window peer request-rate admission for TCP and HTTP.
///
/// Clones share a bounded table initialized by the application. Each attempt
/// charges once and is not refunded by completion or a later policy rejection.
#[derive(Debug, Clone)]
pub struct RequestRate {
    peers: Arc<NacellePeerRateLimiter>,
    limit: usize,
}

impl RequestRate {
    /// Use the same table and limit in each transport for one combined budget.
    pub const fn new(peers: Arc<NacellePeerRateLimiter>, limit: usize) -> Self {
        Self { peers, limit }
    }
}

impl Admission<Option<IpAddr>> for RequestRate {
    type Lease = ();

    fn enter<'a>(
        &'a self,
        input: &'a mut Option<IpAddr>,
    ) -> impl Future<Output = Result<(), Rejection>> + Send + 'a {
        ready(SocketAdmission::admit(self, input))
    }
}

impl SocketAdmission<Option<IpAddr>> for RequestRate {
    type Lease = ();

    fn admit(&self, input: &Option<IpAddr>) -> Result<(), Rejection> {
        match input {
            None => Err(Rejection::new(RejectionKind::Denied, "policy_peer_missing")),
            Some(peer) => match self.peers.try_acquire(*peer, self.limit) {
                NacellePeerRateLimitResult::Allowed => Ok(()),
                NacellePeerRateLimitResult::RateLimited => Err(Rejection::new(
                    RejectionKind::RateLimited,
                    "policy_request_rate",
                )),
                NacellePeerRateLimitResult::TableFull => Err(Rejection::new(
                    RejectionKind::Overloaded,
                    "policy_peer_table",
                )),
            },
        }
    }
}

/// Opt-in execution control, with a concrete future preserving local/shared bounds.
pub trait Control: Send + Sync {
    /// Concrete future; `Send` follows its contained work and control state.
    type Running<F: Future>: Future<Output = Result<F::Output, Rejection>>;

    /// Run work until completion or explicit cancellation.
    fn run<F: Future>(&self, future: F) -> Self::Running<F>;

    /// Run shared-executor work while preserving its `Send` contract.
    fn run_send<F: Future + Send>(
        &self,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Rejection>> + Send;
}

/// No cancellation state or wakeup registration.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoControl;

pin_project! {
    /// Work without cancellation; wraps its output without allocating.
    #[derive(Debug)]
    pub struct Uncontrolled<F> {
        #[pin]
        future: F,
    }
}

impl<F: Future> Future for Uncontrolled<F> {
    type Output = Result<F::Output, Rejection>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().future.poll(cx).map(Ok)
    }
}

impl Control for NoControl {
    type Running<F: Future> = Uncontrolled<F>;

    fn run<F: Future>(&self, future: F) -> Self::Running<F> {
        Uncontrolled { future }
    }

    fn run_send<F: Future + Send>(
        &self,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Rejection>> + Send {
        self.run(future)
    }
}

/// Explicit cancellation of all work registered with this handle.
///
/// Use independent handles or [`Self::child`] for narrower eviction groups.
/// Cancelling closes transport-owned I/O; it cannot reclaim application-retained
/// buffers, roll back side effects, or stop detached tasks.
#[derive(Debug, Default, Clone)]
pub struct Cancellation {
    token: CancellationToken,
}

impl Cancellation {
    /// Construct a handle without creating tasks or timers.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an independently cancellable child, also cancelled by its parent.
    pub fn child(&self) -> Self {
        Self {
            token: self.token.child_token(),
        }
    }

    /// Cancel current and future work in this group.
    pub fn cancel(&self) {
        self.token.cancel();
    }
}

pin_project! {
    /// Concrete cancellation-aware work; allocates no framework task or box.
    pub struct Controlled<F> {
        #[pin]
        future: F,
        #[pin]
        cancelled: WaitForCancellationFutureOwned,
    }
}

impl<F: Future> Future for Controlled<F> {
    type Output = Result<F::Output, Rejection>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if this.cancelled.poll(cx).is_ready() {
            return Poll::Ready(Err(Rejection::new(
                RejectionKind::Closed,
                "policy_cancelled",
            )));
        }
        this.future.poll(cx).map(Ok)
    }
}

impl<F> fmt::Debug for Controlled<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Controlled").finish_non_exhaustive()
    }
}

impl Control for Cancellation {
    type Running<F: Future> = Controlled<F>;

    fn run<F: Future>(&self, future: F) -> Self::Running<F> {
        Controlled {
            future,
            cancelled: self.token.clone().cancelled_owned(),
        }
    }

    fn run_send<F: Future + Send>(
        &self,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Rejection>> + Send {
        self.run(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let mut future = future;
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    struct DropLog(Arc<Mutex<Vec<usize>>>, usize);

    impl Drop for DropLog {
        fn drop(&mut self) {
            self.0.lock().unwrap().push(self.1);
        }
    }

    struct Reserve {
        log: Arc<Mutex<Vec<usize>>>,
        id: usize,
    }

    impl Admission<Cell<usize>> for Reserve {
        type Lease = Hold<DropLog>;

        async fn enter<'a>(&'a self, _: &'a mut Cell<usize>) -> Result<Self::Lease, Rejection> {
            Ok(Hold::new(DropLog(self.log.clone(), self.id)))
        }
    }

    #[tokio::test]
    async fn chain_preserves_non_sync_inputs_and_releases_in_reverse_order() {
        fn require_send(_: impl Send) {}
        let log = Arc::new(Mutex::new(Vec::new()));
        let policies = Chain::new(
            Reserve {
                log: log.clone(),
                id: 1,
            },
            Reserve {
                log: log.clone(),
                id: 2,
            },
        );
        let mut input = Cell::new(0);
        require_send(Admission::enter(&policies, &mut input));
        let lease = Admission::enter(&policies, &mut input).await.unwrap();
        assert!(lease.requires_flush());
        drop(lease);
        assert_eq!(*log.lock().unwrap(), [2, 1]);
    }

    #[tokio::test]
    async fn later_rejection_releases_prior_leases_without_running_later_projection() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let policies = Chain::new(
            Chain::new(
                Reserve {
                    log: log.clone(),
                    id: 1,
                },
                check_fn(|_: &Cell<usize>| Err(Rejection::new(RejectionKind::Denied, "denied"))),
            ),
            map_input(
                |_: &Cell<usize>| -> () { panic!("rejected chain must not project later input") },
                NoPolicy,
            ),
        );
        let error = Admission::enter(&policies, &mut Cell::new(0))
            .await
            .err()
            .unwrap();
        assert_eq!(error.reason(), "denied");
        assert_eq!(*log.lock().unwrap(), [1]);
    }

    struct Capacity {
        semaphore: Arc<Semaphore>,
        attempts: AtomicUsize,
    }

    impl Admission<()> for Capacity {
        type Lease = Hold<tokio::sync::OwnedSemaphorePermit>;

        async fn enter<'a>(&'a self, _: &'a mut ()) -> Result<Self::Lease, Rejection> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            self.semaphore
                .clone()
                .acquire_owned()
                .await
                .map(Hold::new)
                .map_err(|_| Rejection::new(RejectionKind::Closed, "capacity_closed"))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_wait_caps_waiters_and_recovers_after_timeout_and_cancellation() {
        let semaphore = Arc::new(Semaphore::new(0));
        let policy = bounded_wait(
            Capacity {
                semaphore: semaphore.clone(),
                attempts: AtomicUsize::new(0),
            },
            Duration::from_secs(2),
            1,
        );
        let mut first_input = ();
        let mut first = Box::pin(Admission::enter(&policy, &mut first_input));
        assert!(poll_once(first.as_mut()).await.is_pending());
        assert!(poll_once(first.as_mut()).await.is_pending());
        assert_eq!(policy.policy.attempts.load(Ordering::Relaxed), 1);
        assert_eq!(
            Admission::enter(&policy, &mut ())
                .await
                .err()
                .unwrap()
                .reason(),
            "policy_waiters"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(first.await.err().unwrap().kind(), RejectionKind::Timeout);
        let mut next_input = ();
        let mut next = Box::pin(Admission::enter(&policy, &mut next_input));
        assert!(poll_once(next.as_mut()).await.is_pending());
        drop(next);
        semaphore.add_permits(1);
        drop(Admission::enter(&policy, &mut ()).await.unwrap());
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[test]
    fn ready_policy_needs_neither_waiter_capacity_nor_a_timer_runtime() {
        let policy = bounded_wait(NoPolicy, Duration::ZERO, 0);
        let mut input = ();
        let mut future = std::pin::pin!(Admission::enter(&policy, &mut input));
        let mut context = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
    }

    #[tokio::test]
    async fn local_chain_supports_rc_and_shared_projection() {
        let count = Rc::new(Cell::new(0));
        let observed = count.clone();
        let policy = Chain::new(
            local_check_fn(move |value: &Cell<usize>| {
                observed.set(observed.get() + value.get());
                Ok(())
            }),
            Shared(check_fn(|_: &Cell<usize>| Ok(()))),
        );
        let lease = LocalAdmission::enter(&policy, &mut Cell::new(3))
            .await
            .unwrap();
        assert!(!lease.requires_flush());
        assert_eq!(count.get(), 3);
    }

    #[tokio::test]
    async fn rate_clones_share_one_budget_and_missing_peers_reject() {
        let rate = RequestRate::new(Arc::new(NacellePeerRateLimiter::new(2)), 2);
        let other_transport = rate.clone();
        let mut peer = Some(IpAddr::from([127, 0, 0, 1]));
        Admission::enter(&rate, &mut peer).await.unwrap();
        Admission::enter(&other_transport, &mut peer).await.unwrap();
        assert_eq!(
            Admission::enter(&rate, &mut peer).await.unwrap_err().kind(),
            RejectionKind::RateLimited
        );
        assert_eq!(
            Admission::enter(&rate, &mut None)
                .await
                .unwrap_err()
                .reason(),
            "policy_peer_missing"
        );
    }

    #[tokio::test]
    async fn cancellation_releases_pending_work_and_does_not_cancel_parent() {
        let parent = Cancellation::new();
        let child = parent.child();
        let log = Arc::new(Mutex::new(Vec::new()));
        let observed = log.clone();
        let mut running = Box::pin(child.run(async move {
            let _guard = DropLog(observed, 1);
            std::future::pending::<()>().await;
        }));
        assert!(poll_once(running.as_mut()).await.is_pending());
        child.cancel();
        assert_eq!(running.await.unwrap_err().reason(), "policy_cancelled");
        assert_eq!(*log.lock().unwrap(), [1]);
        assert_eq!(parent.run(ready(7)).await.unwrap(), 7);
        parent.cancel();
        assert!(parent.child().run(ready(7)).await.is_err());
    }
}
