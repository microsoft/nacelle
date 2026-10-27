use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::error::{NacelleError, NacelleResourceLimitReason};
use crate::peer_rate::{
    DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY, NacellePeerRateLimitResult, NacellePeerRateLimiter,
};

#[derive(Debug, Clone)]
pub struct NacelleLimits {
    pub max_connections: usize,
    pub max_in_flight_requests: usize,
    pub max_streaming_tasks: usize,
    pub max_connections_per_peer: Option<usize>,
    pub max_connection_opens_per_peer_per_second: Option<usize>,
    pub connection_rate_limit_table_capacity: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    pub handler_timeout: Option<Duration>,
}

impl Default for NacelleLimits {
    fn default() -> Self {
        Self {
            max_connections: 16_384,
            max_in_flight_requests: 65_536,
            max_streaming_tasks: 8_192,
            max_connections_per_peer: None,
            max_connection_opens_per_peer_per_second: None,
            connection_rate_limit_table_capacity: DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            handler_timeout: Some(Duration::from_secs(60)),
        }
    }
}

impl NacelleLimits {
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max.max(1);
        self
    }

    /// Disable the process-wide concurrent connection limit.
    pub fn without_max_connections(mut self) -> Self {
        self.max_connections = usize::MAX;
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

    pub fn with_max_connection_opens_per_peer_per_second(mut self, max: usize) -> Self {
        self.max_connection_opens_per_peer_per_second = Some(max.max(1));
        self
    }

    /// Set the maximum number of peers tracked by the connection-open rate limiter.
    ///
    /// When the bounded table is full or cannot find an inactive entry within
    /// its fixed probe budget, new peers are rejected. This bound applies only when
    /// [`Self::with_max_connection_opens_per_peer_per_second`] is enabled.
    pub fn with_connection_rate_limit_table_capacity(mut self, capacity: usize) -> Self {
        self.connection_rate_limit_table_capacity = capacity.max(1);
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
    metrics_enabled: AtomicBool,
    active_connections: PaddedCounter,
    active_requests: PaddedCounter,
    active_streaming_tasks: PaddedCounter,
    active_connections_metric: OnceLock<metrics::Gauge>,
    active_requests_metric: OnceLock<metrics::Gauge>,
    active_streaming_tasks_metric: OnceLock<metrics::Gauge>,
    peer_connections: PeerConnectionMap,
    peer_connection_rates: Option<NacellePeerRateLimiter>,
}

impl Default for NacelleRuntimeState {
    fn default() -> Self {
        Self::new(NacelleLimits::default())
    }
}

impl NacelleRuntimeState {
    pub fn new(limits: NacelleLimits) -> Self {
        Self::new_inner(limits)
    }

    fn new_inner(limits: NacelleLimits) -> Self {
        let peer_connection_rates = limits
            .max_connection_opens_per_peer_per_second
            .map(|_| NacellePeerRateLimiter::new(limits.connection_rate_limit_table_capacity));
        Self {
            inner: Arc::new(NacelleRuntimeStateInner {
                limits,
                metrics_enabled: AtomicBool::new(true),
                active_connections: PaddedCounter::new(),
                active_requests: PaddedCounter::new(),
                active_streaming_tasks: PaddedCounter::new(),
                active_connections_metric: OnceLock::new(),
                active_requests_metric: OnceLock::new(),
                active_streaming_tasks_metric: OnceLock::new(),
                peer_connections: PeerConnectionMap::new(),
                peer_connection_rates,
            }),
        }
    }

    /// Construct states with independent counters.
    pub fn partitioned(
        limits: impl IntoIterator<Item = NacelleLimits>,
    ) -> Result<Vec<Self>, NacelleError> {
        Ok(limits.into_iter().map(Self::new).collect())
    }

    pub fn limits(&self) -> &NacelleLimits {
        &self.inner.limits
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

    pub fn acquire_connection_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_connections,
            self.inner.limits.max_connections,
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
            self.inner.limits.max_connections,
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
        let Some(limit) = self.inner.limits.max_connection_opens_per_peer_per_second else {
            return Ok(());
        };
        let limiter =
            self.inner
                .peer_connection_rates
                .as_ref()
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
                .with_max_connections(usize::MAX)
                .with_max_in_flight_requests(usize::MAX)
                .with_max_streaming_tasks(usize::MAX),
        );

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

        let state = NacelleRuntimeState::new(NacelleLimits::default().without_max_connections());

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
    fn max_connections_builder_clamps_zero_to_one() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_connections(0));

        let _connection = state.acquire_connection().expect("first connection");
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

        assert_ne!(limits.max_connections, usize::MAX);
        assert_ne!(limits.max_in_flight_requests, usize::MAX);
        assert_ne!(limits.max_streaming_tasks, usize::MAX);
        assert!(limits.max_connection_opens_per_peer_per_second.is_none());
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
        let state = NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_max_connections(CONTENDERS)
                .with_max_connections_per_peer(1),
        );
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
            NacelleLimits::default()
                .with_max_connection_opens_per_peer_per_second(1)
                .with_connection_rate_limit_table_capacity(1),
        );

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
            NacelleLimits::default()
                .with_max_connection_opens_per_peer_per_second(1)
                .with_connection_rate_limit_table_capacity(1),
        );

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
}
