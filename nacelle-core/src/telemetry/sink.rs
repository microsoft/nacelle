//! Telemetry event model and pluggable sink trait. These types are independent
//! of any metrics backend and describe the low-cardinality events Nacelle emits.

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacelleTelemetryEventKind {
    ListenerConfigured,
    ListenerFailed,
    ConnectionOpened,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleTelemetryEvent {
    pub kind: NacelleTelemetryEventKind,
    pub transport: Option<NacelleTransport>,
    pub reason: Option<&'static str>,
    pub count: u64,
}

pub trait NacelleTelemetrySink: Send + Sync + 'static {
    fn record(&self, event: NacelleTelemetryEvent);
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
    T: NacelleTelemetrySink,
{
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

/// Explicit compatibility adapter for dynamically dispatched telemetry sinks.
#[derive(Clone)]
pub struct DynamicSinkObserver(Arc<dyn NacelleTelemetrySink>);

impl DynamicSinkObserver {
    /// Wrap a dynamically dispatched sink.
    pub fn new(sink: Arc<dyn NacelleTelemetrySink>) -> Self {
        Self(sink)
    }
}

impl std::fmt::Debug for DynamicSinkObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicSinkObserver")
            .finish_non_exhaustive()
    }
}

impl NacelleTelemetryObserver for DynamicSinkObserver {
    #[inline]
    fn record(&self, event: NacelleTelemetryEvent) {
        self.0.record(event);
    }
}

#[derive(Debug, Default)]
pub struct NacelleInMemoryTelemetrySink {
    events: Mutex<Vec<NacelleTelemetryEvent>>,
}

impl NacelleInMemoryTelemetrySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<NacelleTelemetryEvent> {
        self.events.lock().expect("telemetry sink poisoned").clone()
    }
}

impl NacelleTelemetrySink for NacelleInMemoryTelemetrySink {
    fn record(&self, event: NacelleTelemetryEvent) {
        self.events
            .lock()
            .expect("telemetry sink poisoned")
            .push(event);
    }
}
