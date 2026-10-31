use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::error::{NacelleConfigurationError, NacelleError, NacelleResourceLimitReason};
use crate::peer_rate::{
    DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY, NacellePeerRateLimitResult, NacellePeerRateLimiter,
};
use crate::tracking::{
    NacelleCloseOutcome, NacelleConnectionLookup, NacelleConnectionRegistration,
    NacelleConnectionSnapshot, NacelleConnectionTracking, NacelleReclaimOutcome, Registry,
    RegistryPartition,
};

/// Startup-only limits. Live connection thresholds belong to [`NacelleRuntimeControl`].
#[derive(Debug, Clone)]
pub struct NacelleLimits {
    /// Optional bounded TCP/Unix registry, fixed before state installation.
    pub connection_tracking: Option<NacelleConnectionTracking>,
    pub max_in_flight_requests: usize,
    pub max_streaming_tasks: usize,
    pub max_connections_per_peer: Option<usize>,
    pub connection_rate_limit_table_capacity: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub handler_timeout: Option<Duration>,
}

impl Default for NacelleLimits {
    fn default() -> Self {
        Self {
            connection_tracking: None,
            max_in_flight_requests: 65_536,
            max_streaming_tasks: 8_192,
            max_connections_per_peer: None,
            connection_rate_limit_table_capacity: DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            handler_timeout: Some(Duration::from_secs(60)),
        }
    }
}

impl NacelleLimits {
    /// Enable bounded connection management for transports implementing tracking.
    pub fn with_connection_tracking(mut self, tracking: NacelleConnectionTracking) -> Self {
        self.connection_tracking = Some(tracking);
        self
    }

    pub fn with_max_in_flight_requests(mut self, max: usize) -> Self {
        self.max_in_flight_requests = max.max(1);
        self
    }

    pub fn with_max_streaming_tasks(mut self, max: usize) -> Self {
        self.max_streaming_tasks = max.max(1);
        self
    }

    pub fn with_max_connections_per_peer(mut self, max: usize) -> Self {
        self.max_connections_per_peer = Some(max.max(1));
        self
    }

    /// Set the maximum number of peers tracked by the connection-open rate limiter.
    ///
    /// When the bounded table is full or cannot find an inactive entry within
    /// its fixed probe budget, new peers are rejected. This bound applies only when
    /// connection-open rate limiting is enabled through [`NacelleRuntimeControl`].
    /// Zero reserves no table capacity and prevents enabling rate limiting.
    pub fn with_connection_rate_limit_table_capacity(mut self, capacity: usize) -> Self {
        self.connection_rate_limit_table_capacity = capacity;
        self
    }

    pub fn with_max_request_body_bytes(mut self, max: usize) -> Self {
        self.max_request_body_bytes = max;
        self
    }

    pub fn with_max_response_body_bytes(mut self, max: usize) -> Self {
        self.max_response_body_bytes = max;
        self
    }

    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }

    pub fn without_handler_timeout(mut self) -> Self {
        self.handler_timeout = None;
        self
    }
}

#[derive(Debug, Clone)]
pub struct NacelleRuntimeState {
    inner: Arc<NacelleRuntimeStateInner>,
}

/// A counter that owns its cache line.
///
/// The connection, request, and streaming-task counters are updated on every
/// accept and every request. Packed adjacently they share a single cache line,
/// so an accept on one core invalidates the request counter on every other
/// core (false sharing) even though the two counters are unrelated.
///
/// The alignment matches the coherence granularity of the target rather than a
/// universal 64 bytes: x86-64's L2 spatial prefetcher pulls lines in aligned
/// pairs, and arm64 (notably Apple silicon) and powerpc64 use 128-byte lines,
/// so 64 bytes would still leave neighbouring counters sharing a unit there.
/// These are the same thresholds `crossbeam-utils` uses for `CachePadded`.
#[cfg_attr(
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64"
    ),
    repr(align(128))
)]
#[cfg_attr(
    any(target_arch = "arm", target_arch = "mips", target_arch = "riscv64"),
    repr(align(32))
)]
#[cfg_attr(
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "riscv64",
    )),
    repr(align(64))
)]
#[derive(Debug)]
struct PaddedCounter(AtomicUsize);

impl PaddedCounter {
    /// The coherence unit this counter is padded to, in bytes.
    #[cfg(test)]
    const ALIGN: usize = align_of::<Self>();

    const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }
}

impl std::ops::Deref for PaddedCounter {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Per-peer connection counts, sharded to keep accepts from serializing.
///
/// A single `Mutex<HashMap>` here puts a process-wide lock on the connection
/// path for every deployment that enables `max_connections_per_peer` — that is,
/// precisely the internet-facing deployments that need the limit most. Sharding
/// by peer address keeps the per-peer count exact while letting unrelated peers
/// be admitted concurrently.
#[derive(Debug)]
struct PeerConnectionMap {
    shards: Box<[Mutex<HashMap<IpAddr, usize>>]>,
}

impl PeerConnectionMap {
    /// Shard count. A power of two so the index is a mask, and small enough
    /// that the idle footprint stays negligible.
    const SHARDS: usize = 64;

    fn new() -> Self {
        Self {
            shards: (0..Self::SHARDS)
                .map(|_| Mutex::new(HashMap::new()))
                .collect(),
        }
    }

    fn shard(&self, peer: IpAddr) -> &Mutex<HashMap<IpAddr, usize>> {
        use std::hash::{BuildHasher, RandomState};
        use std::sync::LazyLock;

        // One process-wide hasher keeps a peer pinned to a single shard for the
        // lifetime of the process, which is what makes the per-peer count exact.
        static HASHER: LazyLock<RandomState> = LazyLock::new(RandomState::new);
        let index = HASHER.hash_one(peer) as usize % Self::SHARDS;
        &self.shards[index]
    }

    fn try_acquire(&self, peer: IpAddr, limit: usize) -> bool {
        let mut peers = self
            .shard(peer)
            .lock()
            .expect("peer connection map poisoned");
        let current = peers.get(&peer).copied().unwrap_or(0);
        if current >= limit {
            return false;
        }
        peers.insert(peer, current + 1);
        true
    }

    fn release(&self, peer: IpAddr) {
        let mut peers = self
            .shard(peer)
            .lock()
            .expect("peer connection map poisoned");
        match peers.get_mut(&peer) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                peers.remove(&peer);
            }
            None => {}
        }
    }
}

#[derive(Debug)]
struct NacelleRuntimeStateInner {
    limits: NacelleLimits,
    max_connections: AtomicUsize,
    connection_open_rate: AtomicUsize,
    control_update: Mutex<()>,
    metrics_enabled: AtomicBool,
    active_connections: PaddedCounter,
    active_requests: PaddedCounter,
    active_streaming_tasks: PaddedCounter,
    active_connections_metric: OnceLock<metrics::Gauge>,
    active_requests_metric: OnceLock<metrics::Gauge>,
    active_streaming_tasks_metric: OnceLock<metrics::Gauge>,
    peer_connections: PeerConnectionMap,
    peer_connection_rates: OnceLock<NacellePeerRateLimiter>,
    tracking: Option<RegistryPartition>,
}

/// Owned administrative observation of live connection thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleConnectionLimits {
    /// `None` is unlimited; `Some(0)` rejects all new connections.
    pub max_connections: Option<usize>,
    /// `None` disables rate accounting; `Some(0)` rejects IP-based admissions.
    pub max_connection_opens_per_peer_per_second: Option<usize>,
}

impl Default for NacelleConnectionLimits {
    fn default() -> Self {
        Self {
            max_connections: Some(16_384),
            max_connection_opens_per_peer_per_second: None,
        }
    }
}

/// The counters addressed by a control handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleControlScope {
    /// One runtime state, shared by its installed listeners.
    Shared,
    /// Aggregate totals divided in the original partition order.
    Partitioned { partitions: usize },
}

/// Cloneable administrative handle; updates never revoke existing permits.
///
/// Admissions racing an update may finish with the threshold they observed.
/// Partition updates are serialized but are not an instantaneous cross-worker
/// cutover. Use the aggregate handle rather than independently updating workers.
///
/// ```
/// use nacelle_core::NacelleRuntimeState;
///
/// # fn main() -> Result<(), nacelle_core::NacelleError> {
/// let state = NacelleRuntimeState::default();
/// let control = state.control();
/// control.set_max_connections(Some(1))?;
/// let connection = state.acquire_connection()?;
/// control.set_max_connections(Some(0))?;
/// assert!(state.acquire_connection().is_err());
/// assert_eq!(state.active_connections(), 1);
/// drop(connection);
/// control.set_max_connections(None)?;
/// control.set_connection_open_rate(Some(100))?;
/// control.set_connection_open_rate(None)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct NacelleRuntimeControl {
    target: ControlTarget,
}

#[derive(Debug, Clone)]
enum ControlTarget {
    Shared(NacelleRuntimeState),
    Partitioned(Arc<PartitionedControl>),
}

#[derive(Debug)]
struct PartitionedControl {
    states: Vec<NacelleRuntimeState>,
    update: Mutex<()>,
}

impl NacelleRuntimeControl {
    /// Observe a registered ID, including controlled setup and closing.
    ///
    /// Finished IDs are unknown. Custom IDs must not be reused while callers
    /// can still hold them; lookup does not create a generation-aware handle.
    pub fn connection(&self, id: u64) -> NacelleConnectionLookup {
        self.registry().map_or(
            NacelleConnectionLookup::TrackingDisabled,
            |(registry, partition)| registry.lookup(partition, id),
        )
    }

    /// Return at most `limit` snapshots in unspecified order, or `None` if disabled.
    ///
    /// The scan is bounded by registry storage. Snapshots retain metadata only.
    pub fn connections(&self, limit: usize) -> Option<Vec<NacelleConnectionSnapshot>> {
        self.registry()
            .map(|(registry, partition)| registry.snapshots(partition, limit))
    }

    /// Select forced cancellation, including active requests and pending setup.
    ///
    /// Selection does not acknowledge teardown, flush, or delivery. Capacity is
    /// retained until owned work is dropped. Unresolved response callbacks
    /// report cancellation; completed callbacks are not repeated.
    /// Borrowed direct I/O remains the caller's responsibility after cancellation.
    pub fn close_connection(&self, id: u64) -> NacelleCloseOutcome {
        self.registry().map_or(
            NacelleCloseOutcome::TrackingDisabled,
            |(registry, partition)| registry.close(partition, id),
        )
    }

    /// Select idle connections toward the current per-partition targets.
    ///
    /// At most `selection_budget` new closes are selected. Already-closing
    /// entries reserve their slots; active work is never selected. The report
    /// is a best-effort observation, not confirmation that targets were reached.
    /// Lowering a target alone never invokes this scan.
    ///
    /// The selection budget does not limit entries visited. A scan may traverse
    /// all allocated registry storage while registration/removal wait for its lock.
    pub fn reclaim_idle(&self, selection_budget: usize) -> NacelleReclaimOutcome {
        let Some((registry, _)) = self.registry() else {
            return NacelleReclaimOutcome::TrackingDisabled;
        };
        match &self.target {
            ControlTarget::Shared(state) => {
                let _guard = state.inner.control_update.lock().expect("control lock");
                registry.reclaim(std::slice::from_ref(state), selection_budget)
            }
            ControlTarget::Partitioned(control) => {
                let _guard = control.update.lock().expect("control lock");
                registry.reclaim(&control.states, selection_budget)
            }
        }
    }

    fn registry(&self) -> Option<(&Registry, Option<usize>)> {
        match &self.target {
            ControlTarget::Shared(state) => state
                .tracking()
                .map(|part| (part.registry.as_ref(), Some(part.partition))),
            ControlTarget::Partitioned(control) => control.states[0]
                .tracking()
                .map(|part| (part.registry.as_ref(), None)),
        }
    }

    /// Identify whether values address one state or aggregate partition totals.
    pub fn scope(&self) -> NacelleControlScope {
        match &self.target {
            ControlTarget::Shared(_) => NacelleControlScope::Shared,
            ControlTarget::Partitioned(control) => NacelleControlScope::Partitioned {
                partitions: control.states.len(),
            },
        }
    }

    /// Observe current thresholds, not startup copies.
    ///
    /// The aggregate snapshot sums finite shares; any unlimited/disabled share
    /// produces `None` for that threshold. Counter occupancy is available on
    /// the runtime state and is not part of this administrative snapshot.
    pub fn snapshot(&self) -> NacelleConnectionLimits {
        match &self.target {
            ControlTarget::Shared(state) => {
                let _guard = state.inner.control_update.lock().expect("control lock");
                state.connection_limits()
            }
            ControlTarget::Partitioned(control) => {
                let _guard = control.update.lock().expect("control lock");
                let mut total = NacelleConnectionLimits {
                    max_connections: Some(0),
                    max_connection_opens_per_peer_per_second: Some(0),
                };
                for state in &control.states {
                    let limits = state.control().snapshot();
                    total.max_connections = total
                        .max_connections
                        .zip(limits.max_connections)
                        .map(|(left, right)| left.saturating_add(right));
                    total.max_connection_opens_per_peer_per_second = total
                        .max_connection_opens_per_peer_per_second
                        .zip(limits.max_connection_opens_per_peer_per_second)
                        .map(|(left, right)| left.saturating_add(right));
                }
                total
            }
        }
    }

    /// Set a finite cap, including zero, or `None` for unlimited connections.
    ///
    /// `Some(usize::MAX)` is reserved; use `None` instead. Lowering a cap below
    /// occupancy blocks new admissions without closing existing connections.
    pub fn set_max_connections(&self, limit: Option<usize>) -> Result<(), NacelleError> {
        if limit == Some(usize::MAX) {
            return Err(NacelleConfigurationError::ConnectionLimit.into());
        }
        self.publish(limit, false)
    }

    /// Set opens per peer per second, including zero, or disable with `None`.
    ///
    /// Finite rates must fit the limiter's 32-bit count and be below
    /// `usize::MAX`. First enable initializes bounded tables before publication.
    /// Disable retains table entries and their clocks; skipped opens are not
    /// counted retroactively. Every partition needs nonzero startup capacity.
    pub fn set_connection_open_rate(&self, rate: Option<usize>) -> Result<(), NacelleError> {
        if rate.is_some_and(|rate| rate == usize::MAX || rate > u32::MAX as usize) {
            return Err(NacelleConfigurationError::ConnectionOpenRate.into());
        }
        self.publish(rate, true)
    }

    fn publish(&self, value: Option<usize>, rate: bool) -> Result<(), NacelleError> {
        match &self.target {
            ControlTarget::Shared(state) => {
                let _guard = state.inner.control_update.lock().expect("control lock");
                if rate && value.is_some() {
                    state.prepare_connection_rate()?;
                }
                state.publish_connection_limit(value, rate);
            }
            ControlTarget::Partitioned(control) => {
                let _guard = control.update.lock().expect("control lock");
                if rate && value.is_some() {
                    if control.states.iter().any(|state| {
                        state.startup_limits().connection_rate_limit_table_capacity == 0
                    }) {
                        return Err(NacelleConfigurationError::ConnectionRateTableCapacity.into());
                    }
                    for state in &control.states {
                        state.prepare_connection_rate()?;
                    }
                }
                for (index, state) in control.states.iter().enumerate() {
                    let _guard = state.inner.control_update.lock().expect("control lock");
                    state.publish_connection_limit(
                        value.map(|total| partition_capacity(total, control.states.len(), index)),
                        rate,
                    );
                }
            }
        }
        Ok(())
    }
}

fn partition_capacity(total: usize, partitions: usize, index: usize) -> usize {
    total / partitions + usize::from(index < total % partitions)
}

impl Default for NacelleRuntimeState {
    fn default() -> Self {
        Self::new(NacelleLimits::default())
    }
}

impl NacelleRuntimeState {
    /// Create counters with startup configuration and default live thresholds.
    ///
    /// Configure live thresholds through [`Self::control`] before serving.
    pub fn new(limits: NacelleLimits) -> Self {
        let tracking = limits.connection_tracking.map(|config| RegistryPartition {
            registry: Registry::new(1),
            partition: 0,
            capacity: config.capacity(),
        });
        Self::with_registry(limits, tracking)
    }

    fn with_registry(limits: NacelleLimits, tracking: Option<RegistryPartition>) -> Self {
        Self {
            inner: Arc::new(NacelleRuntimeStateInner {
                limits,
                max_connections: AtomicUsize::new(16_384),
                connection_open_rate: AtomicUsize::new(usize::MAX),
                control_update: Mutex::new(()),
                metrics_enabled: AtomicBool::new(true),
                active_connections: PaddedCounter::new(),
                active_requests: PaddedCounter::new(),
                active_streaming_tasks: PaddedCounter::new(),
                active_connections_metric: OnceLock::new(),
                active_requests_metric: OnceLock::new(),
                active_streaming_tasks_metric: OnceLock::new(),
                peer_connections: PeerConnectionMap::new(),
                peer_connection_rates: OnceLock::new(),
                tracking,
            }),
        }
    }

    /// Construct independent counters and an aggregate control handle.
    ///
    /// Startup limits must already be partitioned in the intended order. The
    /// default connection total is divided across those states, including zero
    /// shares. Table capacity is reserved by configuration, not allocated here.
    pub fn partitioned(
        limits: impl IntoIterator<Item = NacelleLimits>,
    ) -> Result<(Vec<Self>, NacelleRuntimeControl), NacelleError> {
        let limits: Vec<_> = limits.into_iter().collect();
        if limits.is_empty() {
            return Err(NacelleConfigurationError::PartitionCount.into());
        }
        let tracked = limits[0].connection_tracking.is_some();
        if limits
            .iter()
            .any(|limits| limits.connection_tracking.is_some() != tracked)
        {
            return Err(NacelleConfigurationError::MixedConnectionTracking.into());
        }
        let capacity = limits
            .iter()
            .try_fold(0_usize, |total, limits| {
                total.checked_add(
                    limits
                        .connection_tracking
                        .map_or(0, NacelleConnectionTracking::capacity),
                )
            })
            .ok_or(NacelleConfigurationError::ConnectionTrackingCapacity)?;
        if capacity != 0 {
            NacelleConnectionTracking::new(capacity)?;
        }
        let registry = tracked.then(|| Registry::new(limits.len()));
        let states: Vec<_> = limits
            .into_iter()
            .enumerate()
            .map(|(partition, limits)| {
                let tracking = registry.as_ref().map(|registry| RegistryPartition {
                    registry: registry.clone(),
                    partition,
                    capacity: limits
                        .connection_tracking
                        .expect("tracked configuration")
                        .capacity(),
                });
                Self::with_registry(limits, tracking)
            })
            .collect();
        let control = NacelleRuntimeControl {
            target: ControlTarget::Partitioned(Arc::new(PartitionedControl {
                states: states.clone(),
                update: Mutex::new(()),
            })),
        };
        control.set_max_connections(Some(16_384))?;
        Ok((states, control))
    }

    /// Immutable startup-only limits; never contains stale live thresholds.
    pub fn startup_limits(&self) -> &NacelleLimits {
        &self.inner.limits
    }

    /// Control this exact state, including listeners and permits sharing it.
    pub fn control(&self) -> NacelleRuntimeControl {
        NacelleRuntimeControl {
            target: ControlTarget::Shared(self.clone()),
        }
    }

    /// Replace an unshared builder state, or reinstall the identical state.
    ///
    /// A live handle, server clone, listener, or permit prevents replacement.
    /// This cold-path check keeps exported handles attached to their listeners.
    pub fn replace(&mut self, replacement: Self) -> Result<(), NacelleError> {
        if !Arc::ptr_eq(&self.inner, &replacement.inner) && Arc::strong_count(&self.inner) != 1 {
            return Err(NacelleConfigurationError::RuntimeStateInUse.into());
        }
        *self = replacement;
        Ok(())
    }

    pub(crate) fn connection_limits(&self) -> NacelleConnectionLimits {
        let connections = self.inner.max_connections.load(Ordering::Acquire);
        let rate = self.inner.connection_open_rate.load(Ordering::Acquire);
        NacelleConnectionLimits {
            max_connections: (connections != usize::MAX).then_some(connections),
            max_connection_opens_per_peer_per_second: (rate != usize::MAX).then_some(rate),
        }
    }

    fn prepare_connection_rate(&self) -> Result<(), NacelleError> {
        let capacity = self.inner.limits.connection_rate_limit_table_capacity;
        if capacity == 0 {
            return Err(NacelleConfigurationError::ConnectionRateTableCapacity.into());
        }
        self.inner
            .peer_connection_rates
            .get_or_init(|| NacellePeerRateLimiter::new(capacity));
        Ok(())
    }

    fn publish_connection_limit(&self, limit: Option<usize>, rate: bool) {
        let target = if rate {
            &self.inner.connection_open_rate
        } else {
            &self.inner.max_connections
        };
        target.store(limit.unwrap_or(usize::MAX), Ordering::Release);
    }

    pub(crate) fn set_metrics_enabled(&self, enabled: bool) {
        self.inner.metrics_enabled.store(enabled, Ordering::Release);
    }

    fn metrics_enabled(&self) -> bool {
        self.inner.metrics_enabled.load(Ordering::Acquire)
    }

    fn active_connections_metric(&self) -> &metrics::Gauge {
        self.inner
            .active_connections_metric
            .get_or_init(|| metrics::gauge!("server.connection.active"))
    }

    fn active_requests_metric(&self) -> &metrics::Gauge {
        self.inner
            .active_requests_metric
            .get_or_init(|| metrics::gauge!("server.request.active"))
    }

    fn active_streaming_tasks_metric(&self) -> &metrics::Gauge {
        self.inner
            .active_streaming_tasks_metric
            .get_or_init(|| metrics::gauge!("server.streaming_task.active"))
    }

    pub fn acquire_connection(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_connection_tracked()
    }

    pub(crate) fn tracking(&self) -> Option<&RegistryPartition> {
        self.inner.tracking.as_ref()
    }

    /// Acquire connection capacity and, when enabled, register before setup.
    #[doc(hidden)]
    pub fn register_connection(
        &self,
        connection: &crate::request::NacelleConnectionMeta,
    ) -> Result<NacelleConnectionRegistration, NacelleError> {
        let permit = match connection.peer_ip {
            Some(peer) => self.acquire_connection_for_peer(peer)?,
            None => self.acquire_connection_tracked()?,
        };
        let registered = self
            .tracking()
            .map(|registry| registry.register(connection))
            .transpose()?;
        Ok(NacelleConnectionRegistration::new(permit, registered))
    }

    pub fn acquire_connection_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_connections,
            self.inner.max_connections.load(Ordering::Acquire),
        ) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Connections,
            ));
        }
        if self.metrics_enabled() {
            self.active_connections_metric().increment(1.0);
        }
        Ok(TrackedPermit::new(
            self.clone(),
            PermitKind::Connection { peer: None },
        ))
    }

    pub fn acquire_connection_for_peer(&self, peer: IpAddr) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_connections,
            self.inner.max_connections.load(Ordering::Acquire),
        ) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Connections,
            ));
        }
        if self.metrics_enabled() {
            self.active_connections_metric().increment(1.0);
        }
        if let Err(error) = self.acquire_peer_connection(peer) {
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            if self.metrics_enabled() {
                self.active_connections_metric().decrement(1.0);
            }
            return Err(error);
        }
        if let Err(error) = self.acquire_peer_connection_rate(peer) {
            self.release_peer_connection(peer);
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            if self.metrics_enabled() {
                self.active_connections_metric().decrement(1.0);
            }
            return Err(error);
        }
        Ok(TrackedPermit::new(
            self.clone(),
            PermitKind::Connection { peer: Some(peer) },
        ))
    }

    pub fn acquire_request(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_request_tracked()
    }

    pub fn acquire_request_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_requests,
            self.inner.limits.max_in_flight_requests,
        ) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::InFlightRequests,
            ));
        }
        if self.metrics_enabled() {
            self.active_requests_metric().increment(1.0);
        }
        Ok(TrackedPermit::new(self.clone(), PermitKind::Request))
    }

    pub fn acquire_streaming_task(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_streaming_task_tracked()
    }

    pub fn acquire_streaming_task_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_streaming_tasks,
            self.inner.limits.max_streaming_tasks,
        ) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::StreamingTasks,
            ));
        }
        if self.metrics_enabled() {
            self.active_streaming_tasks_metric().increment(1.0);
        }
        Ok(TrackedPermit::new(self.clone(), PermitKind::StreamingTask))
    }

    pub fn active_connections(&self) -> usize {
        self.inner.active_connections.load(Ordering::Relaxed)
    }

    pub fn active_requests(&self) -> usize {
        self.inner.active_requests.load(Ordering::Relaxed)
    }

    pub fn active_streaming_tasks(&self) -> usize {
        self.inner.active_streaming_tasks.load(Ordering::Relaxed)
    }

    fn acquire_peer_connection(&self, peer: IpAddr) -> Result<(), NacelleError> {
        let Some(limit) = self.inner.limits.max_connections_per_peer else {
            return Ok(());
        };
        if !self.inner.peer_connections.try_acquire(peer, limit) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnections,
            ));
        }
        Ok(())
    }

    fn release_peer_connection(&self, peer: IpAddr) {
        if self.inner.limits.max_connections_per_peer.is_none() {
            return;
        }
        self.inner.peer_connections.release(peer);
    }

    fn acquire_peer_connection_rate(&self, peer: IpAddr) -> Result<(), NacelleError> {
        let limit = self.inner.connection_open_rate.load(Ordering::Acquire);
        if limit == usize::MAX {
            return Ok(());
        }
        let limiter = self
            .inner
            .peer_connection_rates
            .get()
            .ok_or(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRateTable,
            ))?;
        match limiter.try_acquire(peer, limit) {
            NacellePeerRateLimitResult::Allowed => Ok(()),
            NacellePeerRateLimitResult::RateLimited => Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRate,
            )),
            NacellePeerRateLimitResult::TableFull => Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRateTableFull,
            )),
        }
    }
}

fn try_acquire_counter(counter: &AtomicUsize, limit: usize) -> bool {
    if limit == usize::MAX {
        // Unbounded: the compare-exchange below can only ever succeed, so a
        // single relaxed increment carries the same meaning for a fraction of
        // the cost under contention.
        counter.fetch_add(1, Ordering::Relaxed);
        return true;
    }
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= limit {
            return false;
        }
        let Some(next) = current.checked_add(1) else {
            return false;
        };
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PermitKind {
    Connection { peer: Option<IpAddr> },
    Request,
    StreamingTask,
}

#[derive(Debug)]
pub struct TrackedPermit {
    state: NacelleRuntimeState,
    kind: PermitKind,
}

impl TrackedPermit {
    fn new(state: NacelleRuntimeState, kind: PermitKind) -> Self {
        Self { state, kind }
    }
}

impl Drop for TrackedPermit {
    fn drop(&mut self) {
        match self.kind {
            PermitKind::Connection { peer } => {
                self.state
                    .inner
                    .active_connections
                    .fetch_sub(1, Ordering::Relaxed);
                if self.state.metrics_enabled() {
                    self.state.active_connections_metric().decrement(1.0);
                }
                if let Some(peer) = peer {
                    self.state.release_peer_connection(peer);
                }
            }
            PermitKind::Request => {
                self.state
                    .inner
                    .active_requests
                    .fetch_sub(1, Ordering::Relaxed);
                if self.state.metrics_enabled() {
                    self.state.active_requests_metric().decrement(1.0);
                }
            }
            PermitKind::StreamingTask => {
                self.state
                    .inner
                    .active_streaming_tasks
                    .fetch_sub(1, Ordering::Relaxed);
                if self.state.metrics_enabled() {
                    self.state.active_streaming_tasks_metric().decrement(1.0);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unbounded_limits_still_track_active_counts() {
        let state = NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_max_in_flight_requests(usize::MAX)
                .with_max_streaming_tasks(usize::MAX),
        );
        state
            .control()
            .set_max_connections(None)
            .expect("unlimited");

        let connection = state.acquire_connection().expect("connection");
        let request = state.acquire_request().expect("request");
        let streaming = state.acquire_streaming_task().expect("streaming task");
        assert_eq!(state.active_connections(), 1);
        assert_eq!(state.active_requests(), 1);
        assert_eq!(state.active_streaming_tasks(), 1);

        drop((connection, request, streaming));
        assert_eq!(state.active_connections(), 0);
        assert_eq!(state.active_requests(), 0);
        assert_eq!(state.active_streaming_tasks(), 0);
    }

    #[test]
    fn disabled_connection_limit_bypasses_admission_without_leaking_permits() {
        const ROUNDS: usize = 32;
        const CONNECTIONS_PER_ROUND: usize = 8;

        let state = NacelleRuntimeState::default();
        state
            .control()
            .set_max_connections(None)
            .expect("unlimited");

        for _ in 0..ROUNDS {
            let connections: Vec<_> = (0..CONNECTIONS_PER_ROUND)
                .map(|_| state.acquire_connection().expect("unbounded connection"))
                .collect();
            assert_eq!(state.active_connections(), CONNECTIONS_PER_ROUND);
            drop(connections);
            assert_eq!(state.active_connections(), 0);
        }
    }

    #[test]
    fn zero_connection_limit_rejects_all() {
        let state = NacelleRuntimeState::default();
        state
            .control()
            .set_max_connections(Some(0))
            .expect("zero cap");
        assert!(matches!(
            state.acquire_connection(),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Connections
            ))
        ));
    }

    #[test]
    fn hot_counters_do_not_share_a_cache_line() {
        let state = NacelleRuntimeState::default();
        let connections = std::ptr::from_ref(&state.inner.active_connections).addr();
        let requests = std::ptr::from_ref(&state.inner.active_requests).addr();
        let streaming = std::ptr::from_ref(&state.inner.active_streaming_tasks).addr();
        let unit = PaddedCounter::ALIGN;
        assert!(unit >= 32, "coherence unit should be a plausible line size");
        assert!(connections.abs_diff(requests) >= unit);
        assert!(requests.abs_diff(streaming) >= unit);
    }

    #[test]
    fn sharded_peer_limits_stay_exact_under_concurrency() {
        use std::sync::Barrier;
        use std::thread;

        const LIMIT: usize = 4;
        const THREADS: usize = 8;

        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_connections_per_peer(LIMIT));
        let peer: IpAddr = "203.0.113.7".parse().expect("peer address");
        let barrier = Arc::new(Barrier::new(THREADS));

        let permits: Vec<_> = thread::scope(|scope| {
            let handles: Vec<_> = (0..THREADS)
                .map(|_| {
                    let state = state.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        state.acquire_connection_for_peer(peer).ok()
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|handle| handle.join().expect("thread"))
                .collect()
        });

        assert_eq!(permits.len(), LIMIT);
        drop(permits);
        // Released peers must be admissible again.
        assert!(state.acquire_connection_for_peer(peer).is_ok());
    }

    #[test]
    fn default_limits_are_bounded() {
        let limits = NacelleLimits::default();

        let live = NacelleRuntimeState::default().control().snapshot();
        assert_eq!(live.max_connections, Some(16_384));
        assert_ne!(limits.max_in_flight_requests, usize::MAX);
        assert_ne!(limits.max_streaming_tasks, usize::MAX);
        assert!(live.max_connection_opens_per_peer_per_second.is_none());
        assert_eq!(
            limits.connection_rate_limit_table_capacity,
            DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY
        );
        assert!(limits.handler_timeout.is_some());
    }

    #[test]
    fn handler_timeout_can_be_disabled() {
        let limits = NacelleLimits::default().without_handler_timeout();

        assert_eq!(limits.handler_timeout, None);
    }

    #[test]
    fn bounded_counters_reject_at_limit_and_recover_after_drop() {
        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_in_flight_requests(1));

        let request = state.acquire_request_tracked().expect("first request");
        assert!(matches!(
            state.acquire_request_tracked(),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::InFlightRequests
            ))
        ));

        drop(request);
        let _request = state
            .acquire_request_tracked()
            .expect("request permit should recover after drop");
        assert_eq!(state.active_requests(), 1);
    }

    #[test]
    fn per_peer_connection_limit_rejects_and_recovers_after_drop() {
        let peer = "127.0.0.1".parse().expect("valid ip");
        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_connections_per_peer(1));

        let connection = state
            .acquire_connection_for_peer(peer)
            .expect("first peer connection");
        assert!(matches!(
            state.acquire_connection_for_peer(peer),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnections
            ))
        ));

        drop(connection);
        let _connection = state
            .acquire_connection_for_peer(peer)
            .expect("peer connection should recover after drop");
        assert_eq!(state.active_connections(), 1);
    }

    #[test]
    fn concurrent_per_peer_connection_admission_does_not_exceed_limit() {
        const CONTENDERS: usize = 16;

        let peer = "127.0.0.1".parse().expect("valid ip");
        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_connections_per_peer(1));
        state
            .control()
            .set_max_connections(Some(CONTENDERS))
            .expect("cap");
        let barrier = Arc::new(std::sync::Barrier::new(CONTENDERS + 1));
        let (results_tx, results_rx) = std::sync::mpsc::channel();
        let threads: Vec<_> = (0..CONTENDERS)
            .map(|_| {
                let state = state.clone();
                let barrier = barrier.clone();
                let results_tx = results_tx.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    results_tx
                        .send(state.acquire_connection_for_peer(peer))
                        .expect("result receiver should remain open");
                })
            })
            .collect();
        drop(results_tx);

        barrier.wait();
        let results: Vec<_> = results_rx.into_iter().collect();
        for thread in threads {
            thread.join().expect("admission thread should not panic");
        }

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(
            results
                .iter()
                .filter(|result| result.is_err())
                .all(|result| {
                    matches!(
                        result,
                        Err(NacelleError::ResourceLimit(
                            NacelleResourceLimitReason::PeerConnections
                        ))
                    )
                })
        );
        assert_eq!(state.active_connections(), 1);

        drop(results);
        assert_eq!(state.active_connections(), 0);
        let _connection = state
            .acquire_connection_for_peer(peer)
            .expect("peer capacity should recover after permits drop");
    }

    #[test]
    fn per_peer_connection_open_rate_rejects_churn_after_drop() {
        let peer = "127.0.0.1".parse().expect("valid ip");
        let state = NacelleRuntimeState::new(
            NacelleLimits::default().with_connection_rate_limit_table_capacity(1),
        );
        state
            .control()
            .set_connection_open_rate(Some(1))
            .expect("rate");

        let connection = state
            .acquire_connection_for_peer(peer)
            .expect("first peer connection");
        drop(connection);

        assert!(matches!(
            state.acquire_connection_for_peer(peer),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRate
            ))
        ));
        assert_eq!(state.active_connections(), 0);
    }

    #[test]
    fn peer_connection_rate_table_rejects_new_peers_when_full() {
        let first_peer = "127.0.0.1".parse().expect("valid first peer");
        let second_peer = "127.0.0.2".parse().expect("valid second peer");
        let state = NacelleRuntimeState::new(
            NacelleLimits::default().with_connection_rate_limit_table_capacity(1),
        );
        state
            .control()
            .set_connection_open_rate(Some(1))
            .expect("rate");

        let first = state
            .acquire_connection_for_peer(first_peer)
            .expect("first peer should occupy the table");
        drop(first);

        assert!(matches!(
            state.acquire_connection_for_peer(second_peer),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRateTableFull
            ))
        ));
        assert_eq!(state.active_connections(), 0);
    }

    #[test]
    fn live_caps_do_not_revoke_occupancy_and_recover_after_updates() {
        let state = NacelleRuntimeState::default();
        let control = state.control();
        control.set_max_connections(Some(2)).expect("cap");
        let first = state.acquire_connection().expect("first");
        let second = state.acquire_connection().expect("second");
        control.set_max_connections(Some(1)).expect("lower");
        assert_eq!(state.active_connections(), 2);
        assert!(state.acquire_connection().is_err());
        drop(first);
        assert!(state.acquire_connection().is_err());
        control.set_max_connections(Some(3)).expect("increase");
        let third = state.acquire_connection().expect("new capacity");
        control.set_max_connections(Some(0)).expect("pause");
        assert!(state.acquire_connection().is_err());
        drop((second, third));
        assert_eq!(state.active_connections(), 0);
        control.set_max_connections(None).expect("unlimited");
        assert!(state.acquire_connection().is_ok());
        assert!(matches!(
            control.set_max_connections(Some(usize::MAX)),
            Err(NacelleError::Configuration(
                NacelleConfigurationError::ConnectionLimit
            ))
        ));
        assert_eq!(control.snapshot().max_connections, None);
    }

    #[test]
    fn rate_updates_retain_the_table_and_window_without_counting_disabled_opens() {
        let state = NacelleRuntimeState::new(
            NacelleLimits::default().with_connection_rate_limit_table_capacity(1),
        );
        let control = state.control();
        assert!(state.inner.peer_connection_rates.get().is_none());
        control.set_connection_open_rate(Some(2)).expect("enable");
        let limiter = state.inner.peer_connection_rates.get().expect("table");
        let peer = "127.0.0.1".parse().expect("peer");
        assert!(limiter.try_acquire_at(peer, 2, 7).is_allowed());
        control.set_connection_open_rate(None).expect("disable");
        for _ in 0..10 {
            drop(
                state
                    .acquire_connection_for_peer(peer)
                    .expect("disabled rate"),
            );
        }
        control
            .set_connection_open_rate(Some(1))
            .expect("lower and enable");
        assert!(std::ptr::eq(
            limiter,
            state.inner.peer_connection_rates.get().expect("retained")
        ));
        assert_eq!(
            limiter.try_acquire_at(peer, 1, 7),
            NacellePeerRateLimitResult::RateLimited
        );
        control.set_connection_open_rate(Some(2)).expect("increase");
        assert!(limiter.try_acquire_at(peer, 2, 7).is_allowed());
        assert_eq!(
            limiter.try_acquire_at(peer, 2, 7),
            NacellePeerRateLimitResult::RateLimited
        );
        control
            .set_connection_open_rate(Some(0))
            .expect("reject all");
        assert!(matches!(
            state.acquire_connection_for_peer(peer),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRate
            ))
        ));
        assert_eq!(state.active_connections(), 0);
        assert!(control.set_connection_open_rate(Some(usize::MAX)).is_err());
        assert_eq!(
            control.snapshot().max_connection_opens_per_peer_per_second,
            Some(0)
        );
    }

    #[test]
    fn aggregate_updates_preserve_zero_shares_and_validate_before_publication() {
        let (states, control) = NacelleRuntimeState::partitioned([
            NacelleLimits::default().with_connection_rate_limit_table_capacity(1),
            NacelleLimits::default().with_connection_rate_limit_table_capacity(0),
        ])
        .expect("partitions");
        assert_eq!(
            control.scope(),
            NacelleControlScope::Partitioned { partitions: 2 }
        );
        control
            .set_max_connections(Some(1))
            .expect("partition total");
        let first = states[0].acquire_connection().expect("first share");
        assert!(states[1].acquire_connection().is_err());
        assert_eq!(control.snapshot().max_connections, Some(1));
        assert!(matches!(
            control.set_connection_open_rate(Some(10)),
            Err(NacelleError::Configuration(
                NacelleConfigurationError::ConnectionRateTableCapacity
            ))
        ));
        for state in &states {
            assert!(state.inner.peer_connection_rates.get().is_none());
            assert_eq!(
                state
                    .control()
                    .snapshot()
                    .max_connection_opens_per_peer_per_second,
                None
            );
        }
        control
            .set_max_connections(None)
            .expect("unlimited partitions");
        assert!(states[1].acquire_connection().is_ok());
        drop(first);
    }

    #[test]
    fn concurrent_control_and_admission_keep_exact_occupancy() {
        let state = NacelleRuntimeState::new(
            NacelleLimits::default().with_connection_rate_limit_table_capacity(4),
        );
        std::thread::scope(|scope| {
            for worker in 0..6 {
                let state = &state;
                scope.spawn(move || {
                    let peer = format!("127.0.0.{}", worker + 1).parse().expect("peer");
                    for round in 0..1000 {
                        if worker < 2 {
                            let control = state.control();
                            control
                                .set_max_connections(if round % 3 == 0 {
                                    None
                                } else {
                                    Some(round % 8)
                                })
                                .expect("cap");
                            control
                                .set_connection_open_rate(if round % 2 == 0 {
                                    None
                                } else {
                                    Some(round % 5)
                                })
                                .expect("rate");
                        } else {
                            drop(state.acquire_connection_for_peer(peer));
                        }
                    }
                });
            }
        });
        assert_eq!(state.active_connections(), 0);
        state
            .control()
            .set_max_connections(Some(0))
            .expect("settled cap");
        assert!(state.acquire_connection().is_err());
        state
            .control()
            .set_max_connections(Some(1))
            .expect("settled cap");
        let permit = state.acquire_connection().expect("one slot");
        assert!(state.acquire_connection().is_err());
        drop(permit);
        assert_eq!(state.active_connections(), 0);
    }

    #[test]
    fn earlier_rejections_do_not_allocate_rate_entries() {
        let state = NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_max_connections_per_peer(1)
                .with_connection_rate_limit_table_capacity(1),
        );
        let control = state.control();
        let first = "127.0.0.1".parse().expect("peer");
        let second = "127.0.0.2".parse().expect("peer");
        let occupied = state.acquire_connection_for_peer(first).expect("capacity");
        control.set_connection_open_rate(Some(1)).expect("rate");
        assert!(matches!(
            state.acquire_connection_for_peer(first),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnections
            ))
        ));
        control.set_max_connections(Some(0)).expect("pause");
        assert!(matches!(
            state.acquire_connection_for_peer(first),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Connections
            ))
        ));
        control.set_max_connections(None).expect("resume");
        drop(occupied);
        // A one-entry table still has room for a different peer.
        drop(
            state
                .acquire_connection_for_peer(second)
                .expect("unused rate capacity"),
        );
        assert_eq!(state.active_connections(), 0);
    }

    #[test]
    fn competing_aggregate_updates_publish_complete_totals() {
        let (states, control) = NacelleRuntimeState::partitioned(
            (0..3).map(|_| NacelleLimits::default().with_connection_rate_limit_table_capacity(1)),
        )
        .expect("partitions");
        control.set_max_connections(Some(1)).expect("initial total");
        std::thread::scope(|scope| {
            for total in [1, 5] {
                let control = &control;
                scope.spawn(move || {
                    for _ in 0..500 {
                        control.set_max_connections(Some(total)).expect("total");
                        assert!(matches!(control.snapshot().max_connections, Some(1 | 5)));
                        control.set_connection_open_rate(Some(total)).expect("rate");
                        assert!(matches!(
                            control.snapshot().max_connection_opens_per_peer_per_second,
                            Some(1 | 5)
                        ));
                    }
                });
            }
        });
        control
            .set_max_connections(Some(3))
            .expect("one slot per worker");
        control
            .set_connection_open_rate(Some(1))
            .expect("settled rate");
        let peer = "127.0.0.1".parse().expect("peer");
        drop(
            states[0]
                .acquire_connection_for_peer(peer)
                .expect("first share"),
        );
        assert!(matches!(
            states[1].acquire_connection_for_peer(peer),
            Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::PeerConnectionRate
            ))
        ));
        assert!(states.iter().all(|state| state.active_connections() == 0));
    }

    #[test]
    fn exported_handle_prevents_state_replacement() {
        let mut state = NacelleRuntimeState::default();
        let control = state.control();
        assert!(matches!(
            state.replace(NacelleRuntimeState::default()),
            Err(NacelleError::Configuration(
                NacelleConfigurationError::RuntimeStateInUse
            ))
        ));
        state.replace(state.clone()).expect("same state is safe");
        control
            .set_max_connections(Some(0))
            .expect("still installed");
        assert!(state.acquire_connection().is_err());
    }
}
