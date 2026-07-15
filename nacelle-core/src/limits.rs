use std::collections::HashMap;
#[cfg(feature = "exp-memory-limits")]
use std::collections::VecDeque;
use std::net::IpAddr;
#[cfg(feature = "exp-memory-limits")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::NacelleError;
#[cfg(feature = "exp-memory-limits")]
use crate::lifecycle::NacelleShutdownToken;
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
    #[cfg(feature = "exp-memory-limits")]
    pub max_memory_bytes: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
    #[cfg(feature = "exp-memory-limits")]
    pub memory_allocation_timeout: Option<Duration>,
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
            #[cfg(feature = "exp-memory-limits")]
            max_memory_bytes: usize::MAX,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
            #[cfg(feature = "exp-memory-limits")]
            memory_allocation_timeout: Some(Duration::from_secs(5)),
            handler_timeout: Some(Duration::from_secs(60)),
        }
    }
}

impl NacelleLimits {
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max.max(1);
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

    #[cfg(feature = "exp-memory-limits")]
    pub fn with_max_memory_bytes(mut self, max: usize) -> Self {
        self.max_memory_bytes = max.max(1);
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

    #[cfg(feature = "exp-memory-limits")]
    pub fn with_memory_allocation_timeout(mut self, timeout: Duration) -> Self {
        self.memory_allocation_timeout = Some(timeout);
        self
    }

    #[cfg(feature = "exp-memory-limits")]
    pub fn without_memory_allocation_timeout(mut self) -> Self {
        self.memory_allocation_timeout = None;
        self
    }

    pub fn with_handler_timeout(mut self, timeout: Duration) -> Self {
        self.handler_timeout = Some(timeout);
        self
    }
}

#[derive(Debug, Clone)]
pub struct NacelleRuntimeState {
    inner: Arc<NacelleRuntimeStateInner>,
}

#[derive(Debug)]
struct NacelleRuntimeStateInner {
    limits: NacelleLimits,
    active_connections: AtomicUsize,
    active_requests: AtomicUsize,
    active_streaming_tasks: AtomicUsize,
    active_connections_metric: metrics::Gauge,
    active_requests_metric: metrics::Gauge,
    active_streaming_tasks_metric: metrics::Gauge,
    peer_connections: Mutex<HashMap<IpAddr, usize>>,
    peer_connection_rates: Option<NacellePeerRateLimiter>,
    #[cfg(feature = "exp-memory-limits")]
    memory: Arc<SharedMemoryBudget>,
}

#[cfg(feature = "exp-memory-limits")]
#[derive(Debug)]
struct SharedMemoryBudget {
    max_bytes: usize,
    memory_used: AtomicUsize,
    memory_waiters: AtomicUsize,
    memory_used_metric: metrics::Gauge,
    state: Mutex<MemoryBudgetState>,
}

#[cfg(feature = "exp-memory-limits")]
#[derive(Debug)]
struct MemoryBudgetState {
    waiters: VecDeque<MemoryWaiter>,
    next_waiter_id: usize,
}

#[cfg(feature = "exp-memory-limits")]
#[derive(Debug)]
struct MemoryWaiter {
    id: usize,
    bytes: usize,
    notify: Arc<tokio::sync::Notify>,
    granted: Arc<AtomicBool>,
}

#[cfg(feature = "exp-memory-limits")]
struct MemoryWaiterRegistration {
    state: NacelleRuntimeState,
    id: usize,
    bytes: usize,
    granted: Arc<AtomicBool>,
    active: bool,
}

#[cfg(feature = "exp-memory-limits")]
impl MemoryWaiterRegistration {
    fn take_grant(&mut self) -> Option<NacelleMemoryAllocation> {
        if !self.granted.load(Ordering::Acquire) {
            return None;
        }
        self.active = false;
        Some(NacelleMemoryAllocation {
            state: Some(self.state.clone()),
            bytes: self.bytes,
        })
    }

    fn cancel(&mut self) -> Option<NacelleMemoryAllocation> {
        let granted = self.state.cancel_memory_waiter(self.id, &self.granted);
        self.active = false;
        granted.then(|| NacelleMemoryAllocation {
            state: Some(self.state.clone()),
            bytes: self.bytes,
        })
    }
}

#[cfg(feature = "exp-memory-limits")]
impl Drop for MemoryWaiterRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if self.state.cancel_memory_waiter(self.id, &self.granted) {
            self.state.release_memory(self.bytes);
        }
    }
}

#[cfg(feature = "exp-memory-limits")]
impl SharedMemoryBudget {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            memory_used: AtomicUsize::new(0),
            memory_waiters: AtomicUsize::new(0),
            memory_used_metric: metrics::gauge!("nacelle.memory.used_bytes"),
            state: Mutex::new(MemoryBudgetState {
                waiters: VecDeque::new(),
                next_waiter_id: 1,
            }),
        }
    }
}

#[cfg(feature = "exp-memory-limits")]
#[derive(Debug, Clone)]
pub struct NacelleMemoryBudget {
    state: NacelleRuntimeState,
}

impl Default for NacelleRuntimeState {
    fn default() -> Self {
        Self::new(NacelleLimits::default())
    }
}

impl NacelleRuntimeState {
    pub fn new(limits: NacelleLimits) -> Self {
        #[cfg(feature = "exp-memory-limits")]
        {
            let memory = Arc::new(SharedMemoryBudget::new(limits.max_memory_bytes));
            Self::new_with_shared_memory(limits, memory)
        }
        #[cfg(not(feature = "exp-memory-limits"))]
        {
            Self::new_inner(limits)
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    fn new_with_shared_memory(limits: NacelleLimits, memory: Arc<SharedMemoryBudget>) -> Self {
        Self::new_inner(limits, memory)
    }

    fn new_inner(
        limits: NacelleLimits,
        #[cfg(feature = "exp-memory-limits")] memory: Arc<SharedMemoryBudget>,
    ) -> Self {
        let peer_connection_rates = limits
            .max_connection_opens_per_peer_per_second
            .map(|_| NacellePeerRateLimiter::new(limits.connection_rate_limit_table_capacity));
        Self {
            inner: Arc::new(NacelleRuntimeStateInner {
                limits,
                active_connections: AtomicUsize::new(0),
                active_requests: AtomicUsize::new(0),
                active_streaming_tasks: AtomicUsize::new(0),
                active_connections_metric: metrics::gauge!("nacelle.connections.active"),
                active_requests_metric: metrics::gauge!("nacelle.requests.active"),
                active_streaming_tasks_metric: metrics::gauge!("nacelle.streaming_tasks.active"),
                peer_connections: Mutex::new(HashMap::new()),
                peer_connection_rates,
                #[cfg(feature = "exp-memory-limits")]
                memory,
            }),
        }
    }

    /// Construct states with independent counters.
    ///
    /// With `exp-memory-limits`, all states also share one hard memory ceiling.
    pub fn partitioned(
        limits: impl IntoIterator<Item = NacelleLimits>,
    ) -> Result<Vec<Self>, NacelleError> {
        let limits: Vec<_> = limits.into_iter().collect();
        #[cfg(feature = "exp-memory-limits")]
        {
            let max_memory_bytes = limits
                .first()
                .map_or(usize::MAX, |limits| limits.max_memory_bytes);
            if limits
                .iter()
                .any(|limits| limits.max_memory_bytes != max_memory_bytes)
            {
                return Err(NacelleError::ResourceLimit("memory_limit_mismatch"));
            }
            let memory = Arc::new(SharedMemoryBudget::new(max_memory_bytes));
            Ok(limits
                .into_iter()
                .map(|limits| Self::new_with_shared_memory(limits, memory.clone()))
                .collect())
        }
        #[cfg(not(feature = "exp-memory-limits"))]
        {
            Ok(limits.into_iter().map(Self::new).collect())
        }
    }

    pub fn limits(&self) -> &NacelleLimits {
        &self.inner.limits
    }

    pub fn acquire_connection(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_connection_tracked()
    }

    pub fn acquire_connection_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        if !try_acquire_counter(
            &self.inner.active_connections,
            self.inner.limits.max_connections,
        ) {
            return Err(NacelleError::ResourceLimit("connections"));
        }
        self.inner.active_connections_metric.increment(1.0);
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
            return Err(NacelleError::ResourceLimit("connections"));
        }
        self.inner.active_connections_metric.increment(1.0);
        if let Err(error) = self.acquire_peer_connection(peer) {
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            self.inner.active_connections_metric.decrement(1.0);
            return Err(error);
        }
        if let Err(error) = self.acquire_peer_connection_rate(peer) {
            self.release_peer_connection(peer);
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            self.inner.active_connections_metric.decrement(1.0);
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
            return Err(NacelleError::ResourceLimit("in_flight_requests"));
        }
        self.inner.active_requests_metric.increment(1.0);
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
            return Err(NacelleError::ResourceLimit("streaming_tasks"));
        }
        self.inner.active_streaming_tasks_metric.increment(1.0);
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

    #[cfg(feature = "exp-memory-limits")]
    pub fn memory_used_bytes(&self) -> usize {
        self.inner.memory.memory_used.load(Ordering::Acquire)
    }

    #[cfg(feature = "exp-memory-limits")]
    pub fn memory_budget(&self) -> NacelleMemoryBudget {
        NacelleMemoryBudget {
            state: self.clone(),
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    pub fn allocate_memory(&self, bytes: usize) -> Result<NacelleMemoryAllocation, NacelleError> {
        if bytes == 0 || self.inner.memory.max_bytes == usize::MAX {
            return Ok(NacelleMemoryAllocation::empty());
        }
        self.try_allocate_memory_without_waiters(bytes)
    }

    #[cfg(feature = "exp-memory-limits")]
    pub async fn allocate_memory_wait(
        &self,
        bytes: usize,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.allocate_memory_with_timeout_and_shutdown(bytes, None, None)
            .await
    }

    #[cfg(feature = "exp-memory-limits")]
    pub async fn allocate_memory_with_timeout(
        &self,
        bytes: usize,
        timeout: Option<Duration>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.allocate_memory_with_timeout_and_shutdown(bytes, timeout, None)
            .await
    }

    #[cfg(feature = "exp-memory-limits")]
    pub async fn allocate_memory_with_timeout_and_shutdown(
        &self,
        bytes: usize,
        timeout: Option<Duration>,
        shutdown: Option<NacelleShutdownToken>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        if bytes == 0 || self.inner.memory.max_bytes == usize::MAX {
            return Ok(NacelleMemoryAllocation::empty());
        }
        if bytes > self.inner.memory.max_bytes {
            return Err(NacelleError::ResourceLimit("memory_bytes"));
        }

        if let Ok(allocation) = self.try_allocate_memory_without_waiters(bytes) {
            return Ok(allocation);
        }

        self.inner
            .memory
            .memory_waiters
            .fetch_add(1, Ordering::AcqRel);

        let notify = Arc::new(tokio::sync::Notify::new());
        let granted = Arc::new(AtomicBool::new(false));
        let id = {
            let mut memory = self
                .inner
                .memory
                .state
                .lock()
                .expect("memory budget poisoned");
            if memory.waiters.is_empty() {
                match self.try_allocate_memory_counter(bytes) {
                    Ok(allocation) => {
                        self.inner
                            .memory
                            .memory_waiters
                            .fetch_sub(1, Ordering::AcqRel);
                        return Ok(allocation);
                    }
                    Err(NacelleError::ResourceLimit("memory_bytes")) => {}
                    Err(error) => {
                        self.inner
                            .memory
                            .memory_waiters
                            .fetch_sub(1, Ordering::AcqRel);
                        return Err(error);
                    }
                }
            }

            let id = memory.next_waiter_id;
            memory.next_waiter_id = memory.next_waiter_id.wrapping_add(1).max(1);
            memory.waiters.push_back(MemoryWaiter {
                id,
                bytes,
                notify: notify.clone(),
                granted: granted.clone(),
            });
            id
        };

        self.wait_for_memory_allocation(
            MemoryWaiterRegistration {
                state: self.clone(),
                id,
                bytes,
                granted,
                active: true,
            },
            notify,
            timeout,
            shutdown,
        )
        .await
    }

    #[cfg(feature = "exp-memory-limits")]
    fn release_memory(&self, bytes: usize) {
        if bytes != 0 && self.inner.memory.max_bytes != usize::MAX {
            self.inner
                .memory
                .memory_used
                .fetch_sub(bytes, Ordering::AcqRel);
            self.inner.memory.memory_used_metric.decrement(bytes as f64);
            if self.inner.memory.memory_waiters.load(Ordering::Acquire) != 0 {
                let mut to_notify = Vec::new();
                {
                    let mut memory = self
                        .inner
                        .memory
                        .state
                        .lock()
                        .expect("memory budget poisoned");
                    self.grant_waiters_locked(&mut memory, &mut to_notify);
                }
                for notify in to_notify {
                    notify.notify_one();
                }
            }
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    fn try_allocate_memory_counter(
        &self,
        bytes: usize,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.try_reserve_memory_counter(bytes)?;
        Ok(NacelleMemoryAllocation {
            state: Some(self.clone()),
            bytes,
        })
    }

    #[cfg(feature = "exp-memory-limits")]
    fn try_allocate_memory_without_waiters(
        &self,
        bytes: usize,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        if self.inner.memory.memory_waiters.load(Ordering::Acquire) != 0 {
            return Err(NacelleError::ResourceLimit("memory_bytes"));
        }
        let allocation = self.try_allocate_memory_counter(bytes)?;
        if self.inner.memory.memory_waiters.load(Ordering::Acquire) != 0 {
            drop(allocation);
            return Err(NacelleError::ResourceLimit("memory_bytes"));
        }
        Ok(allocation)
    }

    #[cfg(feature = "exp-memory-limits")]
    fn try_reserve_memory_counter(&self, bytes: usize) -> Result<(), NacelleError> {
        let limit = self.inner.memory.max_bytes;
        let mut current = self.inner.memory.memory_used.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(NacelleError::ResourceLimit("memory_bytes"));
            };
            if next > limit {
                return Err(NacelleError::ResourceLimit("memory_bytes"));
            }
            match self.inner.memory.memory_used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.inner.memory.memory_used_metric.increment(bytes as f64);
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    fn grant_waiters_locked(
        &self,
        memory: &mut MemoryBudgetState,
        to_notify: &mut Vec<Arc<tokio::sync::Notify>>,
    ) {
        while let Some(waiter) = memory.waiters.front() {
            if self.try_reserve_memory_counter(waiter.bytes).is_err() {
                break;
            }
            let waiter = memory.waiters.pop_front().expect("waiter checked");
            self.inner
                .memory
                .memory_waiters
                .fetch_sub(1, Ordering::AcqRel);
            waiter.granted.store(true, Ordering::Release);
            to_notify.push(waiter.notify);
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    fn cancel_memory_waiter(&self, id: usize, granted: &AtomicBool) -> bool {
        if granted.load(Ordering::Acquire) {
            return true;
        }
        let mut to_notify = Vec::new();
        {
            let mut memory = self
                .inner
                .memory
                .state
                .lock()
                .expect("memory budget poisoned");
            if granted.load(Ordering::Acquire) {
                return true;
            }
            if let Some(index) = memory.waiters.iter().position(|waiter| waiter.id == id) {
                memory.waiters.remove(index);
                self.inner
                    .memory
                    .memory_waiters
                    .fetch_sub(1, Ordering::AcqRel);
                self.grant_waiters_locked(&mut memory, &mut to_notify);
            }
        }
        for notify in to_notify {
            notify.notify_one();
        }
        false
    }

    #[cfg(feature = "exp-memory-limits")]
    async fn wait_for_memory_allocation(
        &self,
        mut registration: MemoryWaiterRegistration,
        notify: Arc<tokio::sync::Notify>,
        timeout: Option<Duration>,
        shutdown: Option<NacelleShutdownToken>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        // Normalize the optional timeout/shutdown branches to always-present
        // futures so a single `select!` covers every combination. A `None`
        // branch resolves to `pending()`, i.e. it never fires.
        async fn wait_optional_timer(timer: &mut Option<std::pin::Pin<Box<tokio::time::Sleep>>>) {
            match timer {
                Some(timer) => timer.as_mut().await,
                None => std::future::pending().await,
            }
        }

        async fn wait_optional_shutdown(shutdown: &mut Option<NacelleShutdownToken>) {
            match shutdown {
                Some(shutdown) => {
                    shutdown.changed().await;
                }
                None => std::future::pending().await,
            }
        }

        let mut timer = timeout.map(|timeout| Box::pin(tokio::time::sleep(timeout)));
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                _ = notify.notified() => {
                    if let Some(allocation) = registration.take_grant() {
                        return Ok(allocation);
                    }
                }
                _ = wait_optional_timer(&mut timer) => {
                    if let Some(allocation) = registration.cancel() {
                        return Ok(allocation);
                    }
                    return Err(NacelleError::Timeout("memory_allocation"));
                }
                _ = wait_optional_shutdown(&mut shutdown) => {
                    if let Some(allocation) = registration.cancel() {
                        return Ok(allocation);
                    }
                    return Err(NacelleError::ConnectionClosed);
                }
            }
        }
    }

    fn acquire_peer_connection(&self, peer: IpAddr) -> Result<(), NacelleError> {
        let Some(limit) = self.inner.limits.max_connections_per_peer else {
            return Ok(());
        };
        let mut peers = self
            .inner
            .peer_connections
            .lock()
            .expect("peer connection map poisoned");
        let current = peers.get(&peer).copied().unwrap_or(0);
        if current >= limit {
            return Err(NacelleError::ResourceLimit("peer_connections"));
        }
        peers.insert(peer, current + 1);
        Ok(())
    }

    fn release_peer_connection(&self, peer: IpAddr) {
        if self.inner.limits.max_connections_per_peer.is_none() {
            return;
        }
        let mut peers = self
            .inner
            .peer_connections
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

    fn acquire_peer_connection_rate(&self, peer: IpAddr) -> Result<(), NacelleError> {
        let Some(limit) = self.inner.limits.max_connection_opens_per_peer_per_second else {
            return Ok(());
        };
        let limiter = self
            .inner
            .peer_connection_rates
            .as_ref()
            .ok_or(NacelleError::ResourceLimit("peer_connection_rate_table"))?;
        match limiter.try_acquire(peer, limit) {
            NacellePeerRateLimitResult::Allowed => Ok(()),
            NacellePeerRateLimitResult::RateLimited => {
                Err(NacelleError::ResourceLimit("peer_connection_rate"))
            }
            NacellePeerRateLimitResult::TableFull => Err(NacelleError::ResourceLimit(
                "peer_connection_rate_table_full",
            )),
        }
    }
}

#[cfg(feature = "exp-memory-limits")]
impl NacelleMemoryBudget {
    pub fn max_bytes(&self) -> usize {
        self.state.inner.memory.max_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.state.memory_used_bytes()
    }

    pub fn available_bytes(&self) -> usize {
        self.max_bytes().saturating_sub(self.used_bytes())
    }

    pub fn try_allocate(&self, bytes: usize) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.state.allocate_memory(bytes)
    }

    pub async fn allocate(&self, bytes: usize) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.state.allocate_memory_wait(bytes).await
    }

    pub async fn allocate_with_timeout(
        &self,
        bytes: usize,
        timeout: Option<Duration>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.state
            .allocate_memory_with_timeout(bytes, timeout)
            .await
    }

    pub async fn allocate_with_timeout_and_shutdown(
        &self,
        bytes: usize,
        timeout: Option<Duration>,
        shutdown: Option<NacelleShutdownToken>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.state
            .allocate_memory_with_timeout_and_shutdown(bytes, timeout, shutdown)
            .await
    }
}

fn try_acquire_counter(counter: &AtomicUsize, limit: usize) -> bool {
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
                self.state.inner.active_connections_metric.decrement(1.0);
                if let Some(peer) = peer {
                    self.state.release_peer_connection(peer);
                }
            }
            PermitKind::Request => {
                self.state
                    .inner
                    .active_requests
                    .fetch_sub(1, Ordering::Relaxed);
                self.state.inner.active_requests_metric.decrement(1.0);
            }
            PermitKind::StreamingTask => {
                self.state
                    .inner
                    .active_streaming_tasks
                    .fetch_sub(1, Ordering::Relaxed);
                self.state
                    .inner
                    .active_streaming_tasks_metric
                    .decrement(1.0);
            }
        }
    }
}

#[cfg(feature = "exp-memory-limits")]
#[derive(Debug)]
pub struct NacelleMemoryAllocation {
    state: Option<NacelleRuntimeState>,
    bytes: usize,
}

#[cfg(feature = "exp-memory-limits")]
impl NacelleMemoryAllocation {
    fn empty() -> Self {
        Self {
            state: None,
            bytes: 0,
        }
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Retain at most `bytes` in this guard and release the remainder.
    pub fn shrink_to(&mut self, bytes: usize) {
        let retained = self.bytes.min(bytes);
        let released = self.bytes.saturating_sub(retained);
        self.bytes = retained;
        if let Some(state) = &self.state {
            state.release_memory(released);
        }
    }
}

#[cfg(feature = "exp-memory-limits")]
impl Drop for NacelleMemoryAllocation {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.release_memory(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[cfg(feature = "exp-memory-limits")]
    use std::sync::{Arc, Barrier, mpsc};
    #[cfg(feature = "exp-memory-limits")]
    use std::thread;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    fn gauge_snapshot(snapshotter: &metrics_util::debugging::Snapshotter) -> HashMap<String, f64> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| match value {
                DebugValue::Gauge(value) => Some((key.key().name().to_owned(), value.into_inner())),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn runtime_gauges_track_partitioned_state_transitions() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let limits = [NacelleLimits::default(), NacelleLimits::default()];
            #[cfg(feature = "exp-memory-limits")]
            let limits = limits.map(|limits| limits.with_max_memory_bytes(16));
            let states =
                NacelleRuntimeState::partitioned(limits).expect("partitioned states should build");
            let connection = states[0].acquire_connection().expect("connection permit");
            let request = states[1].acquire_request().expect("request permit");
            let streaming = states[0]
                .acquire_streaming_task()
                .expect("streaming permit");
            #[cfg(feature = "exp-memory-limits")]
            let first_memory = states[0].allocate_memory(4).expect("first allocation");
            #[cfg(feature = "exp-memory-limits")]
            let second_memory = states[1].allocate_memory(6).expect("second allocation");

            let gauges = gauge_snapshot(&snapshotter);
            assert_eq!(gauges["nacelle.connections.active"], 1.0);
            assert_eq!(gauges["nacelle.requests.active"], 1.0);
            assert_eq!(gauges["nacelle.streaming_tasks.active"], 1.0);
            #[cfg(not(feature = "exp-memory-limits"))]
            assert!(!gauges.contains_key("nacelle.memory.used_bytes"));
            #[cfg(feature = "exp-memory-limits")]
            assert_eq!(gauges["nacelle.memory.used_bytes"], 10.0);

            drop(connection);
            drop(request);
            drop(streaming);
            #[cfg(feature = "exp-memory-limits")]
            drop(first_memory);
            #[cfg(feature = "exp-memory-limits")]
            drop(second_memory);

            let gauges = gauge_snapshot(&snapshotter);
            assert_eq!(gauges["nacelle.connections.active"], -1.0);
            assert_eq!(gauges["nacelle.requests.active"], -1.0);
            assert_eq!(gauges["nacelle.streaming_tasks.active"], -1.0);
            #[cfg(feature = "exp-memory-limits")]
            assert_eq!(gauges["nacelle.memory.used_bytes"], -10.0);
        });
    }

    #[cfg(feature = "exp-memory-limits")]
    #[test]
    fn memory_allocation_can_release_part_of_its_guard() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(16));
        let mut allocation = state.allocate_memory(12).expect("allocation should fit");

        allocation.shrink_to(5);

        assert_eq!(allocation.bytes(), 5);
        assert_eq!(state.memory_used_bytes(), 5);
        drop(allocation);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn shrinking_memory_allocation_grants_waiting_budget() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let mut held = state
            .allocate_memory(10)
            .expect("full allocation should fit");
        let waiter_state = state.clone();
        let waiter = tokio::spawn(async move { waiter_state.allocate_memory_wait(4).await });
        while state.inner.memory.memory_waiters.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }

        held.shrink_to(6);

        let granted = waiter
            .await
            .expect("waiter should join")
            .expect("released memory should grant waiter");
        assert_eq!(state.memory_used_bytes(), 10);
        drop(granted);
        drop(held);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[test]
    fn default_limits_keep_memory_limiting_opt_in() {
        let limits = NacelleLimits::default();

        assert_ne!(limits.max_connections, usize::MAX);
        assert_ne!(limits.max_in_flight_requests, usize::MAX);
        assert_ne!(limits.max_streaming_tasks, usize::MAX);
        #[cfg(feature = "exp-memory-limits")]
        assert_eq!(limits.max_memory_bytes, usize::MAX);
        assert!(limits.max_connection_opens_per_peer_per_second.is_none());
        assert_eq!(
            limits.connection_rate_limit_table_capacity,
            DEFAULT_PEER_RATE_LIMIT_TABLE_CAPACITY
        );
        #[cfg(feature = "exp-memory-limits")]
        assert_eq!(
            limits.memory_allocation_timeout,
            Some(Duration::from_secs(5))
        );
        assert!(limits.handler_timeout.is_some());
    }

    #[test]
    fn runtime_state_reports_active_permits_and_memory() {
        let limits = NacelleLimits::default()
            .with_max_connections(1)
            .with_max_in_flight_requests(1)
            .with_max_streaming_tasks(1);
        #[cfg(feature = "exp-memory-limits")]
        let limits = limits.with_max_memory_bytes(1024);
        let state = NacelleRuntimeState::new(limits);

        let connection = state
            .acquire_connection_tracked()
            .expect("connection permit");
        let request = state.acquire_request_tracked().expect("request permit");
        let streaming = state
            .acquire_streaming_task_tracked()
            .expect("streaming permit");
        #[cfg(feature = "exp-memory-limits")]
        let memory = state.allocate_memory(512).expect("memory allocation");
        #[cfg(feature = "exp-memory-limits")]
        assert_eq!(memory.bytes(), 512);

        assert_eq!(state.active_connections(), 1);
        assert_eq!(state.active_requests(), 1);
        assert_eq!(state.active_streaming_tasks(), 1);
        #[cfg(feature = "exp-memory-limits")]
        assert_eq!(state.memory_used_bytes(), 512);

        drop(connection);
        drop(request);
        drop(streaming);
        #[cfg(feature = "exp-memory-limits")]
        drop(memory);

        assert_eq!(state.active_connections(), 0);
        assert_eq!(state.active_requests(), 0);
        assert_eq!(state.active_streaming_tasks(), 0);
        #[cfg(feature = "exp-memory-limits")]
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[test]
    fn disabled_memory_budget_returns_empty_allocation_without_accounting() {
        let state = NacelleRuntimeState::default();

        let allocation = state
            .memory_budget()
            .try_allocate(usize::MAX)
            .expect("disabled memory budget should allow empty allocation");

        assert_eq!(allocation.bytes(), 0);
        assert_eq!(state.memory_used_bytes(), 0);
        drop(allocation);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[test]
    fn bounded_counters_reject_at_limit_and_recover_after_drop() {
        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_in_flight_requests(1));

        let request = state.acquire_request_tracked().expect("first request");
        assert!(matches!(
            state.acquire_request_tracked(),
            Err(NacelleError::ResourceLimit("in_flight_requests"))
        ));

        drop(request);
        let _request = state
            .acquire_request_tracked()
            .expect("request permit should recover after drop");
        assert_eq!(state.active_requests(), 1);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[test]
    fn partitioned_states_reject_mismatched_hard_memory_limits() {
        let result = NacelleRuntimeState::partitioned([
            NacelleLimits::default().with_max_memory_bytes(10),
            NacelleLimits::default().with_max_memory_bytes(20),
        ]);

        assert!(matches!(
            result,
            Err(NacelleError::ResourceLimit("memory_limit_mismatch"))
        ));
    }

    #[cfg(feature = "exp-memory-limits")]
    #[test]
    fn memory_budget_rejects_concurrent_allocations_without_oversubscription() {
        const MEMORY_LIMIT: usize = 1024;
        const ALLOCATION_BYTES: usize = 128;
        const WORKERS: usize = 16;

        let state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(MEMORY_LIMIT));
        let start = Arc::new(Barrier::new(WORKERS + 1));
        let release = Arc::new(Barrier::new(WORKERS + 1));
        let (tx, rx) = mpsc::channel();

        thread::scope(|scope| {
            for _ in 0..WORKERS {
                let state = state.clone();
                let start = start.clone();
                let release = release.clone();
                let tx = tx.clone();
                scope.spawn(move || {
                    start.wait();
                    let allocation = state.allocate_memory(ALLOCATION_BYTES);
                    tx.send(allocation.is_ok())
                        .expect("test receiver should be open");
                    let _allocation = allocation.ok();
                    release.wait();
                });
            }
            drop(tx);

            start.wait();
            let accepted = (0..WORKERS)
                .map(|_| rx.recv().expect("worker should report allocation result"))
                .filter(|accepted| *accepted)
                .count();

            assert_eq!(accepted, MEMORY_LIMIT / ALLOCATION_BYTES);
            assert_eq!(state.memory_used_bytes(), MEMORY_LIMIT);
            assert!(matches!(
                state.allocate_memory(1),
                Err(NacelleError::ResourceLimit("memory_bytes"))
            ));

            release.wait();
        });

        assert_eq!(state.memory_used_bytes(), 0);
        let allocation = state
            .allocate_memory(MEMORY_LIMIT)
            .expect("full memory budget should be reusable after release");
        assert_eq!(state.memory_used_bytes(), MEMORY_LIMIT);
        drop(allocation);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn memory_budget_waits_fifo_without_bypassing_larger_waiters() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let first_half = state.allocate_memory(5).expect("first allocation");
        let second_half = state.allocate_memory(5).expect("second allocation");

        let larger_waiter_state = state.clone();
        let larger_waiter = tokio::spawn(async move {
            larger_waiter_state
                .allocate_memory_wait(6)
                .await
                .expect("larger waiter should eventually allocate")
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        let smaller_waiter_state = state.clone();
        let smaller_waiter = tokio::spawn(async move {
            smaller_waiter_state
                .allocate_memory_wait(1)
                .await
                .expect("smaller waiter should not bypass FIFO")
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(first_half);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !larger_waiter.is_finished(),
            "larger waiter needs more than the first released half"
        );
        assert!(
            !smaller_waiter.is_finished(),
            "smaller waiter must not bypass the larger FIFO head"
        );

        drop(second_half);
        let larger = larger_waiter.await.expect("larger waiter should join");
        let smaller = smaller_waiter.await.expect("smaller waiter should join");
        assert_eq!(state.memory_used_bytes(), 7);

        drop(larger);
        drop(smaller);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn memory_budget_try_allocate_does_not_bypass_queued_waiter() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let first_half = state.allocate_memory(5).expect("first allocation");
        let second_half = state.allocate_memory(5).expect("second allocation");

        let waiter_state = state.clone();
        let waiter = tokio::spawn(async move {
            waiter_state
                .allocate_memory_wait(6)
                .await
                .expect("queued waiter should allocate")
        });
        tokio::time::sleep(Duration::from_millis(10)).await;

        drop(first_half);
        tokio::time::sleep(Duration::from_millis(10)).await;

        assert!(matches!(
            state.memory_budget().try_allocate(1),
            Err(NacelleError::ResourceLimit("memory_bytes"))
        ));

        drop(second_half);
        let allocation = waiter.await.expect("waiter should join");
        assert_eq!(allocation.bytes(), 6);
        assert_eq!(state.memory_used_bytes(), 6);
        drop(allocation);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn memory_budget_timeout_removes_waiter() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let held = state.allocate_memory(10).expect("held allocation");

        let error = state
            .allocate_memory_with_timeout(1, Some(Duration::from_millis(10)))
            .await
            .expect_err("waiter should time out");
        assert!(matches!(error, NacelleError::Timeout("memory_allocation")));

        drop(held);
        let recovered = state
            .allocate_memory(10)
            .expect("timed-out waiter should not block later allocations");
        assert_eq!(state.memory_used_bytes(), 10);
        drop(recovered);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn dropping_memory_waiter_releases_queue_and_racing_grant() {
        for release_before_cancel in [false, true] {
            let state =
                NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
            let held = state.allocate_memory(10).expect("held allocation");
            let waiter_state = state.clone();
            let waiter = tokio::spawn(async move { waiter_state.allocate_memory_wait(10).await });

            while state.inner.memory.memory_waiters.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }

            if release_before_cancel {
                drop(held);
                waiter.abort();
                let _ = waiter.await;
            } else {
                waiter.abort();
                waiter.await.expect_err("waiter should be cancelled");
                drop(held);
            }

            assert_eq!(state.inner.memory.memory_waiters.load(Ordering::Acquire), 0);
            assert_eq!(state.memory_used_bytes(), 0);
            let recovered = state
                .allocate_memory(10)
                .expect("cancelled waiter should leave the budget reusable");
            drop(recovered);
            assert_eq!(state.memory_used_bytes(), 0);
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_grants_and_direct_allocations_share_one_atomic_ceiling() {
        const ROUNDS: usize = if cfg!(miri) { 1 } else { 250 };
        const LIMIT: usize = 2;

        for _ in 0..ROUNDS {
            let state =
                NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(LIMIT));
            let held = state.allocate_memory(LIMIT).expect("initial allocation");
            let waiter_state = state.clone();
            let waiter = tokio::spawn(async move { waiter_state.allocate_memory_wait(1).await });

            while state.inner.memory.memory_waiters.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }

            let direct_state = state.clone();
            let direct = tokio::task::spawn_blocking(move || {
                loop {
                    if let Ok(allocation) = direct_state.allocate_memory(1) {
                        return allocation;
                    }
                    std::thread::yield_now();
                }
            });
            drop(held);

            let waiter_allocation = waiter
                .await
                .expect("waiter should join")
                .expect("waiter should allocate");
            let direct_allocation = direct.await.expect("direct allocator should join");
            assert_eq!(waiter_allocation.bytes() + direct_allocation.bytes(), LIMIT);
            assert_eq!(state.memory_used_bytes(), LIMIT);

            drop(waiter_allocation);
            drop(direct_allocation);
            assert_eq!(state.memory_used_bytes(), 0);
        }
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn memory_budget_timeout_at_fifo_head_grants_next_waiter() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let held = state.allocate_memory(5).expect("held allocation");

        let head_state = state.clone();
        let head = tokio::spawn(async move {
            head_state
                .allocate_memory_with_timeout(6, Some(Duration::from_millis(10)))
                .await
        });
        tokio::time::sleep(Duration::from_millis(5)).await;

        let next_state = state.clone();
        let next = tokio::spawn(async move { next_state.allocate_memory_wait(5).await });

        let head_error = head
            .await
            .expect("head waiter should join")
            .expect_err("head waiter should time out");
        assert!(matches!(
            head_error,
            NacelleError::Timeout("memory_allocation")
        ));

        let next_allocation = tokio::time::timeout(Duration::from_secs(1), next)
            .await
            .expect("next waiter should be granted after head timeout")
            .expect("next waiter should join")
            .expect("next waiter should allocate");
        assert_eq!(state.memory_used_bytes(), 10);

        drop(next_allocation);
        drop(held);
        assert_eq!(state.memory_used_bytes(), 0);
    }

    #[cfg(feature = "exp-memory-limits")]
    #[tokio::test]
    async fn memory_budget_shutdown_cancels_waiter() {
        let state = NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(10));
        let held = state.allocate_memory(10).expect("held allocation");
        let (shutdown, token) = crate::lifecycle::NacelleShutdown::pair();

        let waiter_state = state.clone();
        let waiter = tokio::spawn(async move {
            waiter_state
                .allocate_memory_with_timeout_and_shutdown(1, None, Some(token))
                .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        shutdown.shutdown();

        let error = waiter
            .await
            .expect("waiter should join")
            .expect_err("shutdown should cancel waiter");
        assert!(matches!(error, NacelleError::ConnectionClosed));

        drop(held);
        let recovered = state
            .memory_budget()
            .try_allocate(10)
            .expect("cancelled waiter should not block later allocations");
        assert_eq!(state.memory_budget().used_bytes(), 10);
        drop(recovered);
        assert_eq!(state.memory_budget().used_bytes(), 0);
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
            Err(NacelleError::ResourceLimit("peer_connections"))
        ));

        drop(connection);
        let _connection = state
            .acquire_connection_for_peer(peer)
            .expect("peer connection should recover after drop");
        assert_eq!(state.active_connections(), 1);
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
            Err(NacelleError::ResourceLimit("peer_connection_rate"))
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
                "peer_connection_rate_table_full"
            ))
        ));
        assert_eq!(state.active_connections(), 0);
    }
}
