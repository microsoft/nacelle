use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::NacelleError;
use crate::lifecycle::NacelleShutdownToken;

#[derive(Debug, Clone)]
pub struct NacelleLimits {
    pub max_connections: usize,
    pub max_in_flight_requests: usize,
    pub max_streaming_tasks: usize,
    pub max_connections_per_peer: Option<usize>,
    pub max_connection_opens_per_peer_per_second: Option<usize>,
    pub max_memory_bytes: usize,
    pub max_request_body_bytes: usize,
    pub max_response_body_bytes: usize,
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
            max_memory_bytes: usize::MAX,
            max_request_body_bytes: 16 * 1024 * 1024,
            max_response_body_bytes: 16 * 1024 * 1024,
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

    pub fn with_memory_allocation_timeout(mut self, timeout: Duration) -> Self {
        self.memory_allocation_timeout = Some(timeout);
        self
    }

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
    peer_connections: Mutex<HashMap<IpAddr, usize>>,
    peer_connection_rates: Mutex<HashMap<IpAddr, PeerConnectionRateWindow>>,
    memory: Arc<SharedMemoryBudget>,
}

#[derive(Debug)]
struct SharedMemoryBudget {
    max_bytes: usize,
    memory_used: AtomicUsize,
    memory_waiters: AtomicUsize,
    state: Mutex<MemoryBudgetState>,
}

#[derive(Debug, Clone)]
struct PeerConnectionRateWindow {
    started_at: Instant,
    count: usize,
}

#[derive(Debug)]
struct MemoryBudgetState {
    waiters: VecDeque<MemoryWaiter>,
    next_waiter_id: usize,
}

#[derive(Debug)]
struct MemoryWaiter {
    id: usize,
    bytes: usize,
    notify: Arc<tokio::sync::Notify>,
    granted: Arc<AtomicBool>,
}

impl SharedMemoryBudget {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            memory_used: AtomicUsize::new(0),
            memory_waiters: AtomicUsize::new(0),
            state: Mutex::new(MemoryBudgetState {
                waiters: VecDeque::new(),
                next_waiter_id: 1,
            }),
        }
    }
}

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
        let memory = Arc::new(SharedMemoryBudget::new(limits.max_memory_bytes));
        Self::new_with_shared_memory(limits, memory)
    }

    fn new_with_shared_memory(limits: NacelleLimits, memory: Arc<SharedMemoryBudget>) -> Self {
        Self {
            inner: Arc::new(NacelleRuntimeStateInner {
                limits,
                active_connections: AtomicUsize::new(0),
                active_requests: AtomicUsize::new(0),
                active_streaming_tasks: AtomicUsize::new(0),
                peer_connections: Mutex::new(HashMap::new()),
                peer_connection_rates: Mutex::new(HashMap::new()),
                memory,
            }),
        }
    }

    /// Construct states with independent counters and one shared hard memory ceiling.
    pub fn partitioned(
        limits: impl IntoIterator<Item = NacelleLimits>,
    ) -> Result<Vec<Self>, NacelleError> {
        let limits: Vec<_> = limits.into_iter().collect();
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

    pub fn limits(&self) -> &NacelleLimits {
        &self.inner.limits
    }

    #[cfg(feature = "otel")]
    pub(crate) fn shares_counters_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(feature = "otel")]
    pub(crate) fn memory_identity(&self) -> usize {
        Arc::as_ptr(&self.inner.memory) as usize
    }

    pub fn acquire_connection(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_connection_tracked()
    }

    pub fn acquire_connection_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        acquire_counter(
            &self.inner.active_connections,
            self.inner.limits.max_connections,
            "connections",
        )?;
        Ok(TrackedPermit::new(
            self.clone(),
            PermitKind::Connection { peer: None },
        ))
    }

    pub fn acquire_connection_for_peer(&self, peer: IpAddr) -> Result<TrackedPermit, NacelleError> {
        acquire_counter(
            &self.inner.active_connections,
            self.inner.limits.max_connections,
            "connections",
        )?;
        if let Err(error) = self.acquire_peer_connection(peer) {
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            return Err(error);
        }
        if let Err(error) = self.acquire_peer_connection_rate(peer) {
            self.release_peer_connection(peer);
            self.inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
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
        acquire_counter(
            &self.inner.active_requests,
            self.inner.limits.max_in_flight_requests,
            "in_flight_requests",
        )?;
        Ok(TrackedPermit::new(self.clone(), PermitKind::Request))
    }

    pub fn acquire_streaming_task(&self) -> Result<TrackedPermit, NacelleError> {
        self.acquire_streaming_task_tracked()
    }

    pub fn acquire_streaming_task_tracked(&self) -> Result<TrackedPermit, NacelleError> {
        acquire_counter(
            &self.inner.active_streaming_tasks,
            self.inner.limits.max_streaming_tasks,
            "streaming_tasks",
        )?;
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

    pub fn memory_used_bytes(&self) -> usize {
        self.inner.memory.memory_used.load(Ordering::Acquire)
    }

    pub fn memory_budget(&self) -> NacelleMemoryBudget {
        NacelleMemoryBudget {
            state: self.clone(),
        }
    }

    pub fn allocate_memory(&self, bytes: usize) -> Result<NacelleMemoryAllocation, NacelleError> {
        if bytes == 0 || self.inner.memory.max_bytes == usize::MAX {
            return Ok(NacelleMemoryAllocation::empty());
        }

        if self.inner.memory.memory_waiters.load(Ordering::Acquire) != 0 {
            return Err(NacelleError::ResourceLimit("memory_bytes"));
        }
        self.try_allocate_memory_counter(bytes)
    }

    pub async fn allocate_memory_wait(
        &self,
        bytes: usize,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.allocate_memory_with_timeout_and_shutdown(bytes, None, None)
            .await
    }

    pub async fn allocate_memory_with_timeout(
        &self,
        bytes: usize,
        timeout: Option<Duration>,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
        self.allocate_memory_with_timeout_and_shutdown(bytes, timeout, None)
            .await
    }

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

        if self.inner.memory.memory_waiters.load(Ordering::Acquire) == 0
            && let Ok(allocation) = self.try_allocate_memory_counter(bytes)
        {
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

        self.wait_for_memory_allocation(id, bytes, notify, granted, timeout, shutdown)
            .await
    }

    fn release_memory(&self, bytes: usize) {
        if bytes != 0 && self.inner.memory.max_bytes != usize::MAX {
            self.inner
                .memory
                .memory_used
                .fetch_sub(bytes, Ordering::AcqRel);
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

    fn try_allocate_memory_counter(
        &self,
        bytes: usize,
    ) -> Result<NacelleMemoryAllocation, NacelleError> {
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
                    return Ok(NacelleMemoryAllocation {
                        state: Some(self.clone()),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn grant_waiters_locked(
        &self,
        memory: &mut MemoryBudgetState,
        to_notify: &mut Vec<Arc<tokio::sync::Notify>>,
    ) {
        let mut used = self.inner.memory.memory_used.load(Ordering::Acquire);
        while let Some(waiter) = memory.waiters.front() {
            let Some(next) = used.checked_add(waiter.bytes) else {
                break;
            };
            if next > self.inner.memory.max_bytes {
                break;
            }
            let waiter = memory.waiters.pop_front().expect("waiter checked");
            used = next;
            self.inner.memory.memory_used.store(used, Ordering::Release);
            self.inner
                .memory
                .memory_waiters
                .fetch_sub(1, Ordering::AcqRel);
            waiter.granted.store(true, Ordering::Release);
            to_notify.push(waiter.notify);
        }
    }

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

    async fn wait_for_memory_allocation(
        &self,
        id: usize,
        bytes: usize,
        notify: Arc<tokio::sync::Notify>,
        granted: Arc<AtomicBool>,
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
                    if granted.load(Ordering::Acquire) {
                        return Ok(NacelleMemoryAllocation { state: Some(self.clone()), bytes });
                    }
                }
                _ = wait_optional_timer(&mut timer) => {
                    if self.cancel_memory_waiter(id, &granted) {
                        return Ok(NacelleMemoryAllocation { state: Some(self.clone()), bytes });
                    }
                    return Err(NacelleError::Timeout("memory_allocation"));
                }
                _ = wait_optional_shutdown(&mut shutdown) => {
                    if self.cancel_memory_waiter(id, &granted) {
                        return Ok(NacelleMemoryAllocation { state: Some(self.clone()), bytes });
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
        let now = Instant::now();
        let mut peers = self
            .inner
            .peer_connection_rates
            .lock()
            .expect("peer connection rate map poisoned");
        peers.retain(|_peer, window| {
            now.duration_since(window.started_at) < Duration::from_secs(60)
        });
        let window = peers.entry(peer).or_insert(PeerConnectionRateWindow {
            started_at: now,
            count: 0,
        });
        if now.duration_since(window.started_at) >= Duration::from_secs(1) {
            window.started_at = now;
            window.count = 0;
        }
        if window.count >= limit {
            return Err(NacelleError::ResourceLimit("peer_connection_rate"));
        }
        window.count += 1;
        Ok(())
    }
}

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

fn acquire_counter(
    counter: &AtomicUsize,
    limit: usize,
    name: &'static str,
) -> Result<(), NacelleError> {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= limit {
            return Err(NacelleError::ResourceLimit(name));
        }
        let Some(next) = current.checked_add(1) else {
            return Err(NacelleError::ResourceLimit(name));
        };
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return Ok(()),
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
                if let Some(peer) = peer {
                    self.state.release_peer_connection(peer);
                }
            }
            PermitKind::Request => {
                self.state
                    .inner
                    .active_requests
                    .fetch_sub(1, Ordering::Relaxed);
            }
            PermitKind::StreamingTask => {
                self.state
                    .inner
                    .active_streaming_tasks
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Debug)]
pub struct NacelleMemoryAllocation {
    state: Option<NacelleRuntimeState>,
    bytes: usize,
}

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
}

impl Drop for NacelleMemoryAllocation {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.release_memory(self.bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, mpsc};
    use std::thread;

    use super::*;

    #[test]
    fn default_limits_keep_memory_limiting_opt_in() {
        let limits = NacelleLimits::default();

        assert_ne!(limits.max_connections, usize::MAX);
        assert_ne!(limits.max_in_flight_requests, usize::MAX);
        assert_ne!(limits.max_streaming_tasks, usize::MAX);
        assert_eq!(limits.max_memory_bytes, usize::MAX);
        assert!(limits.max_connection_opens_per_peer_per_second.is_none());
        assert_eq!(
            limits.memory_allocation_timeout,
            Some(Duration::from_secs(5))
        );
        assert!(limits.handler_timeout.is_some());
    }

    #[test]
    fn runtime_state_reports_active_permits_and_memory() {
        let state = NacelleRuntimeState::new(
            NacelleLimits::default()
                .with_max_connections(1)
                .with_max_in_flight_requests(1)
                .with_max_streaming_tasks(1)
                .with_max_memory_bytes(1024),
        );

        let connection = state
            .acquire_connection_tracked()
            .expect("connection permit");
        let request = state.acquire_request_tracked().expect("request permit");
        let streaming = state
            .acquire_streaming_task_tracked()
            .expect("streaming permit");
        let memory = state.allocate_memory(512).expect("memory allocation");
        assert_eq!(memory.bytes(), 512);

        assert_eq!(state.active_connections(), 1);
        assert_eq!(state.active_requests(), 1);
        assert_eq!(state.active_streaming_tasks(), 1);
        assert_eq!(state.memory_used_bytes(), 512);

        drop(connection);
        drop(request);
        drop(streaming);
        drop(memory);

        assert_eq!(state.active_connections(), 0);
        assert_eq!(state.active_requests(), 0);
        assert_eq!(state.active_streaming_tasks(), 0);
        assert_eq!(state.memory_used_bytes(), 0);
    }

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
            NacelleLimits::default().with_max_connection_opens_per_peer_per_second(1),
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
}
