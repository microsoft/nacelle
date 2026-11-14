//! Telemetry event model and statically dispatched observer contract. These types are independent
//! of any metrics backend and describe the low-cardinality events Nacelle emits.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NacelleTransport(&'static str);

impl NacelleTransport {
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl From<&'static str> for NacelleTransport {
    fn from(name: &'static str) -> Self {
        Self::new(name)
    }
}

impl std::fmt::Display for NacelleTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleTelemetryEventKind {
    ListenerConfigured,
    ListenerFailed,
    ConnectionOpened,
    ConnectionClosed,
    ConnectionRejected,
    RequestRejected,
    RequestCompleted,
    RequestFailed,
    ResponseBodyBytes,
    Timeout,
    ShutdownRequested,
    ListenerStoppedAccepting,
    DrainStarted,
    DrainCompleted,
    DrainTimedOut,
    ConnectionsAborted,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleTelemetryEvent {
    pub kind: NacelleTelemetryEventKind,
    pub transport: Option<NacelleTransport>,
    pub reason: Option<&'static str>,
    pub count: u64,
}

/// Statically dispatched telemetry event observer.
pub trait NacelleTelemetryObserver: Clone + Send + Sync + Unpin + 'static {
    /// Whether this observer emits events.
    const ENABLED: bool = true;

    /// Observe one low-cardinality Nacelle event.
    fn record(&self, event: NacelleTelemetryEvent);
}

/// Zero-sized observer used by the default telemetry path.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopObserver;

impl NacelleTelemetryObserver for NoopObserver {
    const ENABLED: bool = false;

    #[inline]
    fn record(&self, _event: NacelleTelemetryEvent) {}
}

impl<T> NacelleTelemetryObserver for Arc<T>
where
    T: NacelleTelemetryObserver,
{
    const ENABLED: bool = T::ENABLED;

    #[inline]
    fn record(&self, event: NacelleTelemetryEvent) {
        self.as_ref().record(event);
    }
}

/// Statically composed pair of telemetry observers.
#[derive(Debug, Clone)]
pub struct CompositeObserver<First, Second> {
    first: First,
    second: Second,
}

impl<First, Second> CompositeObserver<First, Second> {
    /// Compose two concrete observers.
    pub const fn new(first: First, second: Second) -> Self {
        Self { first, second }
    }
}

impl<First, Second> NacelleTelemetryObserver for CompositeObserver<First, Second>
where
    First: NacelleTelemetryObserver,
    Second: NacelleTelemetryObserver,
{
    const ENABLED: bool = First::ENABLED || Second::ENABLED;

    #[inline]
    fn record(&self, event: NacelleTelemetryEvent) {
        self.first.record(event);
        self.second.record(event);
    }
}

/// Bounded in-memory telemetry observer intended for tests and diagnostics.
///
/// Once its capacity is reached, recording a new event evicts the oldest event.
#[derive(Debug, Clone)]
pub struct NacelleInMemoryObserver {
    state: Arc<Mutex<InMemoryState>>,
}

#[derive(Debug)]
struct InMemoryState {
    events: VecDeque<NacelleTelemetryEvent>,
    capacity: NonZeroUsize,
    dropped_events: u64,
}

impl Default for NacelleInMemoryObserver {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleInMemoryObserver {
    /// Default number of retained events.
    pub const DEFAULT_CAPACITY: NonZeroUsize = NonZeroUsize::new(4_096).expect("nonzero");

    /// Create an observer with [`Self::DEFAULT_CAPACITY`].
    pub fn new() -> Self {
        Self::with_capacity(Self::DEFAULT_CAPACITY)
    }

    /// Create an observer with an explicit maximum number of retained events.
    pub fn with_capacity(capacity: NonZeroUsize) -> Self {
        Self {
            state: Arc::new(Mutex::new(InMemoryState {
                events: VecDeque::with_capacity(capacity.get()),
                capacity,
                dropped_events: 0,
            })),
        }
    }

    /// Return a bounded snapshot of retained events, oldest first.
    pub fn events(&self) -> Vec<NacelleTelemetryEvent> {
        self.state
            .lock()
            .expect("telemetry sink poisoned")
            .events
            .iter()
            .copied()
            .collect()
    }

    /// Return the number of events evicted since this observer was created.
    pub fn dropped_events(&self) -> u64 {
        self.state
            .lock()
            .expect("telemetry sink poisoned")
            .dropped_events
    }
}

impl NacelleTelemetryObserver for NacelleInMemoryObserver {
    fn record(&self, event: NacelleTelemetryEvent) {
        let mut state = self.state.lock().expect("telemetry sink poisoned");
        if state.events.len() == state.capacity.get() {
            state.events.pop_front();
            state.dropped_events = state.dropped_events.saturating_add(1);
        }
        state.events.push_back(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_observer_evicts_oldest_events_at_capacity() {
        let observer =
            NacelleInMemoryObserver::with_capacity(NonZeroUsize::new(2).expect("nonzero"));
        for count in 1..=3 {
            observer.record(NacelleTelemetryEvent {
                kind: NacelleTelemetryEventKind::RequestCompleted,
                transport: None,
                reason: None,
                count,
            });
        }

        assert_eq!(
            observer
                .events()
                .iter()
                .map(|event| event.count)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(observer.dropped_events(), 1);
    }
}
