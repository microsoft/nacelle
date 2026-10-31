//! Opt-in connection registration and administrative lifecycle observations.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::error::{NacelleConfigurationError, NacelleError, NacelleResourceLimitReason};
use crate::limits::{NacelleRuntimeState, TrackedPermit};
use crate::request::NacelleConnectionMeta;

/// Finite registry capacity, fixed when the runtime state is constructed.
///
/// Storage grows on registration up to this capacity. Map storage retains its
/// high-water allocation until the state is dropped; entries are removed at
/// teardown. This bounds registrations, not transport or application memory.
#[derive(Debug, Clone, Copy)]
pub struct NacelleConnectionTracking {
    capacity: usize,
}

impl NacelleConnectionTracking {
    /// Configure a positive finite capacity.
    ///
    /// # Errors
    ///
    /// Rejects zero and capacities that cannot fit a registry allocation.
    pub fn new(capacity: usize) -> Result<Self, NacelleConfigurationError> {
        if capacity == 0 || capacity > isize::MAX as usize / size_of::<RegistryRecord>() {
            return Err(NacelleConfigurationError::ConnectionTrackingCapacity);
        }
        Ok(Self { capacity })
    }

    /// Maximum live registrations, including setup and closing connections.
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    /// Divide the capacity in worker order, retaining tracking for zero shares.
    ///
    /// # Errors
    ///
    /// Rejects an empty partition set or an out-of-range index.
    #[doc(hidden)]
    pub fn partition(
        self,
        partitions: usize,
        index: usize,
    ) -> Result<Self, NacelleConfigurationError> {
        if partitions == 0 || index >= partitions {
            return Err(NacelleConfigurationError::PartitionCount);
        }
        Ok(Self {
            capacity: self.capacity / partitions + usize::from(index < self.capacity % partitions),
        })
    }
}

/// Observed activity, not a guarantee that a connection remains in that state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleConnectionActivityState {
    /// Controlled socket setup, protocol setup, or TLS handshake is incomplete.
    Setup,
    /// Input, request work, response delivery, or a transport flush is pending.
    Active,
    /// No accumulated input or pending work; the underlying writer is flushed.
    Idle,
    /// Administrative cancellation was selected or normal teardown is pending.
    Closing,
}

/// Owned metadata snapshot. It contains no socket, task, or application state.
#[derive(Debug, Clone)]
pub struct NacelleConnectionSnapshot {
    /// Accepted metadata, updated with negotiated TLS metadata after setup.
    pub connection: NacelleConnectionMeta,
    /// Point-in-time transport observation.
    pub activity: NacelleConnectionActivityState,
    /// Zero for shared accounting; otherwise the configured worker index.
    pub partition: usize,
}

/// Lookup distinguishes unavailable tracking from an unknown or finished ID.
#[derive(Debug, Clone)]
pub enum NacelleConnectionLookup {
    /// This state was constructed without tracking.
    TrackingDisabled,
    /// The ID is absent, finished, or belongs to a different worker scope.
    Unknown,
    /// An owned snapshot; subsequent operations must recheck the ID.
    Found(Box<NacelleConnectionSnapshot>),
}

/// Selection is not confirmation of teardown or successful response delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleCloseOutcome {
    /// This state was constructed without tracking.
    TrackingDisabled,
    /// The ID is absent, finished, or outside this handle's worker scope.
    Unknown,
    /// Cancellation was signalled; capacity remains held until teardown.
    Selected,
    /// An administrative close is already selected.
    AlreadyClosing,
}

/// Static administrative cancellation reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleConnectionCloseReason {
    /// Explicit cancellation, including active work or pending setup/shutdown.
    Forced,
    /// Idle-only cancellation selected toward a connection target.
    IdleReclaim,
}

impl NacelleConnectionCloseReason {
    /// Low-cardinality telemetry reason.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forced => "administrative_close",
            Self::IdleReclaim => "idle_reclaim",
        }
    }
}

/// One partition's observations at selection time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NacelleReclaimPartition {
    /// Configured worker index, or zero for nonpartitioned state.
    pub partition: usize,
    /// Counter occupancy at the scan, including setup and closing.
    pub active_connections: usize,
    /// Current connection ceiling; `None` produces no target gap.
    pub target: Option<usize>,
    /// Closing entries counted as reservations before selecting more.
    pub already_closing: usize,
    /// Newly selected idle entries in this partition.
    pub selected: usize,
    /// Remaining target gap after closing reservations and new selections.
    ///
    /// A nonzero gap means insufficient idle capacity or selection budget.
    pub unfilled: usize,
}

/// Bounded best-effort reclaim, with occupancy retained until teardown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NacelleReclaimOutcome {
    /// This state was constructed without tracking.
    TrackingDisabled,
    /// Observations can change as connections arrive, activate, and retire.
    Observed {
        /// Per-partition target accounting, in configured worker order.
        partitions: Vec<NacelleReclaimPartition>,
        /// Total newly selected closes, never exceeding the supplied budget.
        selected: usize,
        /// Total remaining gap, not a promise about later occupancy.
        unfilled: usize,
    },
}

const SETUP: u8 = 0;
const ACTIVE: u8 = 1;
const IDLE: u8 = 2;
const FORCED: u8 = 3;
const RECLAIMED: u8 = 4;
const FINISHING: u8 = 5;
const CLOSED: u8 = 6;

#[derive(Debug)]
struct Entry {
    metadata: Mutex<NacelleConnectionMeta>,
    state: AtomicU8,
    close: tokio::sync::Notify,
}

/// Transport-owned activity signal; never owns a connection task.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct NacelleConnectionActivity(Arc<Entry>);

impl NacelleConnectionActivity {
    pub fn established(&self, metadata: &NacelleConnectionMeta) {
        *self.0.metadata.lock().expect("connection metadata lock") = metadata.clone();
        let _ = self
            .0
            .state
            .compare_exchange(SETUP, ACTIVE, Ordering::AcqRel, Ordering::Acquire);
    }

    /// First input must win this transition before decoding or dispatch.
    pub fn activate(&self) -> bool {
        self.0
            .state
            .compare_exchange(IDLE, ACTIVE, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            || self.0.state.load(Ordering::Acquire) == ACTIVE
    }

    /// Only the transport can establish a quiescent, fully flushed boundary.
    pub fn idle(&self) {
        let _ = self
            .0
            .state
            .compare_exchange(ACTIVE, IDLE, Ordering::AcqRel, Ordering::Acquire);
    }

    pub fn finishing(&self) {
        let _ = self
            .0
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < FORCED).then_some(FINISHING)
            });
    }

    pub fn close_reason(&self) -> Option<NacelleConnectionCloseReason> {
        match self.0.state.load(Ordering::Acquire) {
            FORCED => Some(NacelleConnectionCloseReason::Forced),
            RECLAIMED => Some(NacelleConnectionCloseReason::IdleReclaim),
            _ => None,
        }
    }

    async fn cancelled(&self) -> NacelleConnectionCloseReason {
        loop {
            let notified = self.0.close.notified();
            if let Some(reason) = self.close_reason() {
                return reason;
            }
            notified.await;
        }
    }

    fn select_close(&self, idle_only: bool) -> bool {
        let selected = if idle_only {
            self.0
                .state
                .compare_exchange(IDLE, RECLAIMED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        } else {
            self.0
                .state
                .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                    (state < FORCED || state == FINISHING).then_some(FORCED)
                })
                .is_ok()
        };
        if selected {
            self.0.close.notify_one();
        }
        selected
    }
}

#[derive(Debug)]
struct RegistryRecord {
    partition: usize,
    activity: NacelleConnectionActivity,
}

#[derive(Debug)]
struct RegistryInner {
    entries: HashMap<u64, RegistryRecord>,
    counts: Vec<usize>,
}

#[derive(Debug)]
pub(crate) struct Registry(Mutex<RegistryInner>);

impl Registry {
    pub(crate) fn new(partitions: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(RegistryInner {
            entries: HashMap::new(),
            counts: vec![0; partitions],
        })))
    }

    pub(crate) fn lookup(&self, partition: Option<usize>, id: u64) -> NacelleConnectionLookup {
        let inner = self.0.lock().expect("connection registry lock");
        match inner
            .entries
            .get(&id)
            .filter(|entry| partition.is_none_or(|p| p == entry.partition))
        {
            Some(entry) => NacelleConnectionLookup::Found(Box::new(entry.snapshot())),
            None => NacelleConnectionLookup::Unknown,
        }
    }

    pub(crate) fn snapshots(
        &self,
        partition: Option<usize>,
        limit: usize,
    ) -> Vec<NacelleConnectionSnapshot> {
        self.0
            .lock()
            .expect("connection registry lock")
            .entries
            .values()
            .filter(|entry| partition.is_none_or(|p| p == entry.partition))
            .take(limit)
            .map(RegistryRecord::snapshot)
            .collect()
    }

    pub(crate) fn close(&self, partition: Option<usize>, id: u64) -> NacelleCloseOutcome {
        let inner = self.0.lock().expect("connection registry lock");
        let Some(entry) = inner
            .entries
            .get(&id)
            .filter(|entry| partition.is_none_or(|p| p == entry.partition))
        else {
            return NacelleCloseOutcome::Unknown;
        };
        if entry.activity.select_close(false) {
            NacelleCloseOutcome::Selected
        } else {
            NacelleCloseOutcome::AlreadyClosing
        }
    }

    pub(crate) fn reclaim(
        &self,
        states: &[NacelleRuntimeState],
        budget: usize,
    ) -> NacelleReclaimOutcome {
        // The same lock serializes reservations, registration, and removal.
        // No connection future is awaited while holding it.
        let inner = self.0.lock().expect("connection registry lock");
        let mut partitions: Vec<_> = states
            .iter()
            .map(|state| NacelleReclaimPartition {
                partition: state.tracking().expect("tracked partition").partition,
                active_connections: state.active_connections(),
                target: state.connection_limits().max_connections,
                already_closing: 0,
                selected: 0,
                unfilled: 0,
            })
            .collect();
        let first_partition = partitions[0].partition;
        let report_index = |partition: usize| {
            if states.len() == 1 {
                (first_partition == partition).then_some(0)
            } else {
                Some(partition)
            }
        };
        for entry in inner.entries.values() {
            if let Some(index) = report_index(entry.partition)
                && entry.activity.0.state.load(Ordering::Acquire) >= FORCED
            {
                partitions[index].already_closing += 1;
            }
        }
        for report in &mut partitions {
            report.unfilled = report.target.map_or(0, |target| {
                report
                    .active_connections
                    .saturating_sub(target)
                    .saturating_sub(report.already_closing)
            });
        }
        let mut selected = 0;
        for entry in inner.entries.values() {
            if selected == budget {
                break;
            }
            let Some(index) = report_index(entry.partition) else {
                continue;
            };
            let report = &mut partitions[index];
            if report.unfilled > 0 && entry.activity.select_close(true) {
                report.unfilled -= 1;
                report.selected += 1;
                selected += 1;
            }
        }
        let unfilled = partitions.iter().map(|report| report.unfilled).sum();
        NacelleReclaimOutcome::Observed {
            partitions,
            selected,
            unfilled,
        }
    }
}

impl RegistryRecord {
    fn snapshot(&self) -> NacelleConnectionSnapshot {
        NacelleConnectionSnapshot {
            connection: self
                .activity
                .0
                .metadata
                .lock()
                .expect("connection metadata lock")
                .clone(),
            activity: match self.activity.0.state.load(Ordering::Acquire) {
                SETUP => NacelleConnectionActivityState::Setup,
                ACTIVE => NacelleConnectionActivityState::Active,
                IDLE => NacelleConnectionActivityState::Idle,
                _ => NacelleConnectionActivityState::Closing,
            },
            partition: self.partition,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RegistryPartition {
    pub(crate) registry: Arc<Registry>,
    pub(crate) partition: usize,
    pub(crate) capacity: usize,
}

impl RegistryPartition {
    pub(crate) fn register(
        &self,
        metadata: &NacelleConnectionMeta,
    ) -> Result<Registration, NacelleError> {
        let mut inner = self.registry.0.lock().expect("connection registry lock");
        if inner.entries.contains_key(&metadata.connection_id) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::DuplicateConnectionId,
            ));
        }
        if inner.counts[self.partition] >= self.capacity {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ConnectionRegistry,
            ));
        }
        let activity = NacelleConnectionActivity(Arc::new(Entry {
            metadata: Mutex::new(metadata.clone()),
            state: AtomicU8::new(SETUP),
            close: tokio::sync::Notify::new(),
        }));
        inner.entries.insert(
            metadata.connection_id,
            RegistryRecord {
                partition: self.partition,
                activity: activity.clone(),
            },
        );
        inner.counts[self.partition] += 1;
        Ok(Registration {
            partition: self.clone(),
            id: metadata.connection_id,
            activity,
        })
    }
}

pub(crate) struct Registration {
    partition: RegistryPartition,
    id: u64,
    activity: NacelleConnectionActivity,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut inner = self
            .partition
            .registry
            .0
            .lock()
            .expect("connection registry lock");
        self.activity.0.state.store(CLOSED, Ordering::Release);
        inner.entries.remove(&self.id);
        inner.counts[self.partition.partition] -= 1;
    }
}

/// Owns registration and capacity through controlled setup and serving.
#[doc(hidden)]
pub struct NacelleConnectionRegistration {
    permit: Option<TrackedPermit>,
    registered: Option<Registration>,
}

impl NacelleConnectionRegistration {
    pub(crate) fn new(permit: TrackedPermit, registered: Option<Registration>) -> Self {
        Self {
            permit: Some(permit),
            registered,
        }
    }

    pub fn activity(&self) -> Option<NacelleConnectionActivity> {
        self.registered.as_ref().map(|entry| entry.activity.clone())
    }

    /// Cancels tracked work without restarting its I/O or waiting for shutdown.
    ///
    /// Untracked work has no administrative waiter. Work is pinned in place
    /// rather than copied into another async state machine on its first poll.
    #[allow(clippy::future_not_send)]
    pub fn run<F>(self, future: F) -> impl Future<Output = Result<(), NacelleError>>
    where
        F: Future<Output = Result<(), NacelleError>>,
    {
        let activity = self.activity();
        RegisteredConnection {
            work: Some(future),
            close: Some(async move {
                match activity {
                    Some(activity) => activity.cancelled().await,
                    None => std::future::pending().await,
                }
            }),
            registration: Some(self),
        }
    }
}

pin_project_lite::pin_project! {
    // Field order also releases I/O before capacity when dropped unpolled.
    struct RegisteredConnection<F, C> {
        #[pin]
        work: Option<F>,
        #[pin]
        close: Option<C>,
        registration: Option<NacelleConnectionRegistration>,
    }
}

impl<F, C> Future for RegisteredConnection<F, C>
where
    F: Future<Output = Result<(), NacelleError>>,
    C: Future<Output = NacelleConnectionCloseReason>,
{
    type Output = Result<(), NacelleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let registration = this.registration.as_ref().expect("connection completed");
        let reason = if registration.registered.is_some() {
            this.close
                .as_mut()
                .as_pin_mut()
                .expect("close future")
                .poll(cx)
                .map(Some)
        } else {
            Poll::Ready(None)
        };
        let result = match reason {
            Poll::Ready(Some(reason)) => {
                Poll::Ready(Err(NacelleError::ConnectionTerminated(reason)))
            }
            _ => this
                .work
                .as_mut()
                .as_pin_mut()
                .expect("connection work")
                .poll(cx),
        };
        let Poll::Ready(mut result) = result else {
            return Poll::Pending;
        };
        if let Some(reason) = registration
            .registered
            .as_ref()
            .and_then(|entry| entry.activity.close_reason())
        {
            result = Err(NacelleError::ConnectionTerminated(reason));
        }
        this.work.set(None);
        this.close.set(None);
        this.registration.take();
        Poll::Ready(result)
    }
}

impl Drop for NacelleConnectionRegistration {
    fn drop(&mut self) {
        // Retaining a closing reservation briefly after decrement is conservative;
        // removing it before decrement would let a concurrent scan over-select.
        drop(self.permit.take());
        drop(self.registered.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NacelleLimits;

    fn state(capacity: usize) -> NacelleRuntimeState {
        NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_connection_tracking(NacelleConnectionTracking::new(capacity).unwrap()),
        )
    }

    fn selected(outcome: NacelleReclaimOutcome) -> (usize, usize) {
        match outcome {
            NacelleReclaimOutcome::Observed {
                selected, unfilled, ..
            } => (selected, unfilled),
            NacelleReclaimOutcome::TrackingDisabled => panic!("tracking enabled"),
        }
    }

    #[test]
    fn disabled_full_duplicate_and_finished_connections_are_distinct() {
        let off = NacelleRuntimeState::default();
        assert!(matches!(
            off.control().connection(1),
            NacelleConnectionLookup::TrackingDisabled
        ));
        assert_eq!(
            off.control().close_connection(1),
            NacelleCloseOutcome::TrackingDisabled
        );
        assert_eq!(
            off.control().reclaim_idle(1),
            NacelleReclaimOutcome::TrackingDisabled
        );
        let state = state(1);
        let connection = NacelleConnectionMeta::tcp(None, None);
        let guard = state.register_connection(&connection).unwrap();
        assert!(
            matches!(state.control().connection(connection.connection_id),
            NacelleConnectionLookup::Found(snapshot)
                if snapshot.activity == NacelleConnectionActivityState::Setup)
        );
        assert!(matches!(
            state.register_connection(&connection),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::DuplicateConnectionId
            ))
        ));
        assert!(matches!(
            state.register_connection(&NacelleConnectionMeta::tcp(None, None)),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ConnectionRegistry
            ))
        ));
        assert_eq!(state.active_connections(), 1);
        drop(guard);
        assert_eq!(state.active_connections(), 0);
        assert!(matches!(
            state.control().connection(connection.connection_id),
            NacelleConnectionLookup::Unknown
        ));
        assert_eq!(
            state.control().close_connection(connection.connection_id),
            NacelleCloseOutcome::Unknown
        );
        assert!(state.register_connection(&connection).is_ok());
    }

    #[test]
    fn activation_and_reclaim_have_one_winner() {
        for activate_first in [false, true] {
            let state = state(1);
            let connection = NacelleConnectionMeta::tcp(None, None);
            let guard = state.register_connection(&connection).unwrap();
            let activity = guard.activity().unwrap();
            state.control().set_max_connections(Some(0)).unwrap();
            assert_eq!(selected(state.control().reclaim_idle(1)), (0, 1));
            activity.established(&connection);
            assert_eq!(selected(state.control().reclaim_idle(1)), (0, 1));
            activity.idle();
            if activate_first {
                assert!(activity.activate());
                assert_eq!(selected(state.control().reclaim_idle(1)), (0, 1));
                assert_eq!(
                    state.control().close_connection(connection.connection_id),
                    NacelleCloseOutcome::Selected
                );
            } else {
                assert_eq!(selected(state.control().reclaim_idle(1)), (1, 0));
                assert!(!activity.activate());
            }
            assert_eq!(
                state.control().close_connection(connection.connection_id),
                NacelleCloseOutcome::AlreadyClosing
            );
            assert_eq!(state.active_connections(), 1);
        }
    }

    #[test]
    fn concurrent_scans_reserve_only_the_target_gap() {
        let state = state(8);
        let guards: Vec<_> = (0..8)
            .map(|_| {
                let connection = NacelleConnectionMeta::tcp(None, None);
                let guard = state.register_connection(&connection).unwrap();
                let activity = guard.activity().unwrap();
                activity.established(&connection);
                activity.idle();
                guard
            })
            .collect();
        state.control().set_max_connections(Some(5)).unwrap();
        let barrier = std::sync::Barrier::new(8);
        let count: usize = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        selected(state.control().reclaim_idle(2)).0
                    })
                })
                .collect();
            tasks.into_iter().map(|task| task.join().unwrap()).sum()
        });
        assert_eq!(count, 3);
        assert_eq!(state.active_connections(), 8);
        assert_eq!(selected(state.control().reclaim_idle(8)), (0, 0));
        drop(guards);
        assert!(state.control().connections(8).unwrap().is_empty());
        assert_eq!(state.active_connections(), 0);
    }

    #[test]
    fn partitions_reclaim_their_own_gap_and_reject_duplicate_ids() {
        let limits = NacelleLimits::default()
            .with_connection_tracking(NacelleConnectionTracking::new(4).unwrap());
        let (states, control) = NacelleRuntimeState::partitioned([limits.clone(), limits]).unwrap();
        let mut guards = Vec::new();
        for (state, count) in states.iter().zip([1, 3]) {
            for _ in 0..count {
                let connection = NacelleConnectionMeta::tcp(None, None);
                let guard = state.register_connection(&connection).unwrap();
                guard.activity().unwrap().established(&connection);
                guard.activity().unwrap().idle();
                guards.push(guard);
            }
        }
        let duplicate = states[0]
            .control()
            .connections(1)
            .unwrap()
            .remove(0)
            .connection;
        assert!(matches!(
            states[1].register_connection(&duplicate),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::DuplicateConnectionId
            ))
        ));
        control.set_max_connections(Some(3)).unwrap();
        let NacelleReclaimOutcome::Observed {
            partitions,
            selected,
            unfilled,
        } = control.reclaim_idle(4)
        else {
            panic!("tracking enabled");
        };
        assert_eq!((selected, unfilled), (2, 0));
        assert_eq!(partitions[0].selected, 0);
        assert_eq!(partitions[1].selected, 2);
        assert_eq!(control.connections(10).unwrap().len(), 4);
        assert_eq!(
            states[0].active_connections() + states[1].active_connections(),
            4
        );
        drop(guards);
        assert!(control.connections(10).unwrap().is_empty());
    }

    #[test]
    fn zero_registry_share_remains_tracked_and_rejects_registration() {
        let config = NacelleConnectionTracking::new(1).unwrap();
        let limits = (0..2).map(|i| {
            NacelleLimits::default().with_connection_tracking(config.partition(2, i).unwrap())
        });
        let (states, control) = NacelleRuntimeState::partitioned(limits).unwrap();
        assert!(control.connections(2).is_some());
        assert!(matches!(
            states[1].register_connection(&NacelleConnectionMeta::tcp(None, None)),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ConnectionRegistry
            ))
        ));
        assert_eq!(states[1].active_connections(), 0);
        assert!(NacelleConnectionTracking::new(0).is_err());
        assert!(NacelleConnectionTracking::new(usize::MAX).is_err());
    }

    struct DropCheck {
        state: NacelleRuntimeState,
        observed: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Drop for DropCheck {
        fn drop(&mut self) {
            self.observed
                .store(self.state.active_connections(), Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn completion_releases_work_then_capacity_before_future_is_dropped() {
        for tracked in [false, true] {
            let state = if tracked {
                state(1)
            } else {
                NacelleRuntimeState::default()
            };
            let connection = NacelleConnectionMeta::tcp(None, None);
            let guard = state.register_connection(&connection).unwrap();
            let observed = Arc::new(std::sync::atomic::AtomicUsize::new(99));
            let owned = DropCheck {
                state: state.clone(),
                observed: observed.clone(),
            };
            let future = guard.run(std::future::poll_fn(move |_| {
                let _ = &owned;
                Poll::Ready(Ok(()))
            }));
            tokio::pin!(future);
            future.as_mut().await.unwrap();
            assert_eq!(observed.load(Ordering::SeqCst), 1);
            assert_eq!(state.active_connections(), 0);
            if tracked {
                assert!(state.control().connections(1).unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn setup_close_error_and_unpolled_abort_drop_work_before_capacity() {
        for mode in 0..3 {
            let state = state(1);
            let connection = NacelleConnectionMeta::tcp(None, None);
            let guard = state.register_connection(&connection).unwrap();
            let observed = Arc::new(std::sync::atomic::AtomicUsize::new(99));
            let owned = DropCheck {
                state: state.clone(),
                observed: observed.clone(),
            };
            let future = guard.run(async move {
                let _owned = owned;
                if mode == 1 {
                    Err(NacelleError::InvalidFrame("setup"))
                } else {
                    std::future::pending::<Result<(), NacelleError>>().await
                }
            });
            if mode == 0 {
                assert_eq!(
                    state.control().close_connection(connection.connection_id),
                    NacelleCloseOutcome::Selected
                );
                assert!(matches!(
                    future.await,
                    Err(NacelleError::ConnectionTerminated(
                        NacelleConnectionCloseReason::Forced
                    ))
                ));
            } else if mode == 1 {
                assert!(future.await.is_err());
            } else {
                drop(future);
            }
            assert_eq!(observed.load(Ordering::SeqCst), 1);
            assert_eq!(state.active_connections(), 0);
            assert!(state.control().connections(1).unwrap().is_empty());
        }
    }

    #[cfg(panic = "unwind")]
    #[tokio::test]
    async fn setup_panic_drops_work_before_capacity() {
        let state = state(1);
        let connection = NacelleConnectionMeta::tcp(None, None);
        let guard = state.register_connection(&connection).unwrap();
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(99));
        let owned = DropCheck {
            state: state.clone(),
            observed: observed.clone(),
        };
        let future = guard.run(async move {
            let _owned = owned;
            panic!("setup panic");
        });
        assert!(tokio::spawn(future).await.unwrap_err().is_panic());
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        assert_eq!(state.active_connections(), 0);
        assert!(state.control().connections(1).unwrap().is_empty());
    }
}
