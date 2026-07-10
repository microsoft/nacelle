//! Telemetry event model and pluggable sink trait. These types are independent
//! of any metrics backend and describe the low-cardinality events Nacelle emits.

use std::sync::Mutex;

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
