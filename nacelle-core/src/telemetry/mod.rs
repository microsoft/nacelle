use std::sync::Arc;
use std::time::Duration;

mod sink;
pub use sink::{
    CompositeObserver, NacelleInMemoryObserver, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetryObserver, NacelleTransport, NoopObserver,
};

#[derive(Clone)]
pub struct NacelleTelemetry<Observer = NoopObserver> {
    config: NacelleTelemetryConfig,
    observer: Observer,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NacelleTelemetryConfig {
    pub request_metrics: NacelleRequestMetricsConfig,
    pub phase_duration_metrics: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleRequestMetricsConfig {
    pub started: bool,
    pub completed: bool,
    pub in_flight: bool,
    pub duration_ms: bool,
    pub byte_counts: bool,
}

impl Default for NacelleRequestMetricsConfig {
    fn default() -> Self {
        Self {
            started: true,
            completed: true,
            in_flight: false,
            duration_ms: false,
            byte_counts: true,
        }
    }
}

impl NacelleRequestMetricsConfig {
    fn enabled(self) -> bool {
        self.started || self.completed || self.in_flight || self.duration_ms || self.byte_counts
    }
}

#[derive(Debug, Clone)]
pub struct NacelleMetricsContext {
    pub transport: NacelleTransport,
    pub listener: Arc<str>,
    pub protocol: &'static str,
    pub tls: &'static str,
    connection_attributes: Arc<[metrics::Label]>,
    request_attributes: Arc<[metrics::Label]>,
    connection_accepted: metrics::Counter,
    connection_active: metrics::Gauge,
    request_started: metrics::Counter,
    request_in_flight: metrics::Gauge,
    request_completed_ok: metrics::Counter,
    request_completed_error: metrics::Counter,
    request_duration_ok: metrics::Histogram,
    request_duration_error: metrics::Histogram,
    request_bytes_ok: metrics::Counter,
    request_bytes_error: metrics::Counter,
    response_bytes_ok: metrics::Counter,
    response_bytes_error: metrics::Counter,
    #[cfg(feature = "phase-timing")]
    phase_durations: [metrics::Histogram; 6],
}

impl NacelleMetricsContext {
    pub fn new(
        transport: NacelleTransport,
        listener: Arc<str>,
        protocol: &'static str,
        tls: &'static str,
    ) -> Self {
        let connection_attributes: Arc<[metrics::Label]> = Arc::from([
            metrics::Label::new("listener", listener.to_string()),
            metrics::Label::from_static_parts("transport", transport.as_str()),
            metrics::Label::from_static_parts("tls", tls),
        ]);
        let request_attributes: Arc<[metrics::Label]> = attributes_with_label(
            connection_attributes.as_ref(),
            metrics::Label::from_static_parts("protocol", protocol),
        )
        .into();
        let request_ok_attributes = attributes_with_label(
            request_attributes.as_ref(),
            metrics::Label::from_static_parts("status", "ok"),
        );
        let request_error_attributes = attributes_with_label(
            request_attributes.as_ref(),
            metrics::Label::from_static_parts("status", "error"),
        );

        Self {
            transport,
            listener,
            protocol,
            tls,
            connection_accepted: metrics::counter!(
                "nacelle.connections.accepted",
                connection_attributes.to_vec()
            ),
            connection_active: metrics::gauge!(
                "nacelle.connections.in_flight",
                connection_attributes.to_vec()
            ),
            request_started: metrics::counter!(
                "nacelle.requests.started",
                request_attributes.to_vec()
            ),
            request_in_flight: metrics::gauge!(
                "nacelle.requests.in_flight",
                request_attributes.to_vec()
            ),
            request_completed_ok: metrics::counter!(
                "nacelle.requests.completed",
                request_ok_attributes.to_vec()
            ),
            request_completed_error: metrics::counter!(
                "nacelle.requests.completed",
                request_error_attributes.to_vec()
            ),
            request_duration_ok: metrics::histogram!(
                "nacelle.request.duration_ms",
                request_ok_attributes.to_vec()
            ),
            request_duration_error: metrics::histogram!(
                "nacelle.request.duration_ms",
                request_error_attributes.to_vec()
            ),
            request_bytes_ok: metrics::counter!(
                "nacelle.request.bytes",
                request_ok_attributes.to_vec()
            ),
            request_bytes_error: metrics::counter!(
                "nacelle.request.bytes",
                request_error_attributes.to_vec()
            ),
            response_bytes_ok: metrics::counter!(
                "nacelle.response.bytes",
                request_ok_attributes.to_vec()
            ),
            response_bytes_error: metrics::counter!(
                "nacelle.response.bytes",
                request_error_attributes.to_vec()
            ),
            #[cfg(feature = "phase-timing")]
            phase_durations: PHASES.map(|phase| {
                metrics::histogram!(
                    "nacelle.phase.duration_ms",
                    attributes_with_label(
                        request_attributes.as_ref(),
                        metrics::Label::from_static_parts("phase", phase),
                    )
                )
            }),
            connection_attributes,
            request_attributes,
        }
    }

    fn request_completed(&self, status: &'static str) -> &metrics::Counter {
        match status {
            "error" => &self.request_completed_error,
            _ => &self.request_completed_ok,
        }
    }

    fn request_duration(&self, status: &'static str) -> &metrics::Histogram {
        match status {
            "error" => &self.request_duration_error,
            _ => &self.request_duration_ok,
        }
    }

    fn request_bytes(&self, status: &'static str) -> &metrics::Counter {
        match status {
            "error" => &self.request_bytes_error,
            _ => &self.request_bytes_ok,
        }
    }

    fn response_bytes(&self, status: &'static str) -> &metrics::Counter {
        match status {
            "error" => &self.response_bytes_error,
            _ => &self.response_bytes_ok,
        }
    }

    #[cfg(feature = "phase-timing")]
    fn phase_duration(&self, phase: &'static str) -> Option<&metrics::Histogram> {
        PHASES
            .iter()
            .position(|candidate| *candidate == phase)
            .map(|index| &self.phase_durations[index])
    }
}

#[cfg(feature = "phase-timing")]
const PHASES: [&str; 6] = [
    "socket_read",
    "decode",
    "request_body_read",
    "handler",
    "response_encode",
    "socket_write",
];

fn attributes_with_label(
    attributes: &[metrics::Label],
    label: metrics::Label,
) -> Vec<metrics::Label> {
    let mut combined = Vec::with_capacity(attributes.len() + 1);
    combined.extend_from_slice(attributes);
    combined.push(label);
    combined
}

impl<Observer> std::fmt::Debug for NacelleTelemetry<Observer> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacelleTelemetry")
            .field("config", &self.config)
            .field("observer", &std::any::type_name::<Observer>())
            .finish()
    }
}

impl Default for NacelleTelemetry<NoopObserver> {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleTelemetry<NoopObserver> {
    pub fn new() -> Self {
        Self {
            config: NacelleTelemetryConfig::default(),
            observer: NoopObserver,
        }
    }
}

impl<Observer> NacelleTelemetry<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    pub fn with_config(mut self, config: NacelleTelemetryConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_observer<Next>(self, observer: Next) -> NacelleTelemetry<Next>
    where
        Next: NacelleTelemetryObserver,
    {
        NacelleTelemetry {
            config: self.config,
            observer,
        }
    }

    pub fn with_additional_observer<Next>(
        self,
        observer: Next,
    ) -> NacelleTelemetry<CompositeObserver<Observer, Next>>
    where
        Next: NacelleTelemetryObserver,
    {
        NacelleTelemetry {
            config: self.config,
            observer: CompositeObserver::new(self.observer, observer),
        }
    }

    pub fn with_request_metrics(mut self, request_metrics: NacelleRequestMetricsConfig) -> Self {
        self.config.request_metrics = request_metrics;
        self
    }

    pub fn with_request_started_metrics(mut self, enabled: bool) -> Self {
        self.config.request_metrics.started = enabled;
        self
    }

    pub fn with_request_completed_metrics(mut self, enabled: bool) -> Self {
        self.config.request_metrics.completed = enabled;
        self
    }

    pub fn with_request_in_flight_metrics(mut self, enabled: bool) -> Self {
        self.config.request_metrics.in_flight = enabled;
        self
    }

    pub fn with_request_duration_metrics(mut self, enabled: bool) -> Self {
        self.config.request_metrics.duration_ms = enabled;
        self
    }

    pub fn with_byte_count_metrics(mut self, enabled: bool) -> Self {
        self.config.request_metrics.byte_counts = enabled;
        self
    }

    pub fn with_phase_duration_metrics(mut self, enabled: bool) -> Self {
        self.config.phase_duration_metrics = enabled;
        self
    }

    pub fn config(&self) -> NacelleTelemetryConfig {
        self.config
    }

    pub const fn metrics_enabled(&self) -> bool {
        true
    }

    pub fn request_metrics_enabled(&self) -> bool {
        self.config.request_metrics.enabled()
    }

    pub fn request_duration_metrics_enabled(&self) -> bool {
        self.config.request_metrics.duration_ms
    }

    pub fn phase_duration_metrics_enabled(&self) -> bool {
        cfg!(feature = "phase-timing") && self.config.phase_duration_metrics
    }

    pub const fn request_events_enabled(&self) -> bool {
        true
    }

    pub const fn observer_enabled(&self) -> bool {
        Observer::ENABLED
    }

    pub fn listener_configured(&self, transport: NacelleTransport, name: &str, addr: &str) {
        tracing::info!(
            target: "nacelle",
            transport = transport.as_str(),
            binding = name,
            addr,
            "listener configured"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ListenerConfigured,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
    }

    pub fn listener_failed(
        &self,
        transport: NacelleTransport,
        name: &str,
        addr: &str,
        error: &crate::error::NacelleError,
    ) {
        tracing::error!(
            target: "nacelle",
            transport = transport.as_str(),
            binding = name,
            addr,
            error = %error,
            "listener failed"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ListenerFailed,
            transport: Some(transport),
            reason: error_reason(error),
            count: 1,
        });
    }

    pub fn connection_opened(&self, transport: NacelleTransport) {
        tracing::debug!(
            target: "nacelle",
            transport = transport.as_str(),
            "connection opened"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ConnectionOpened,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
        metrics::counter!(
            "nacelle.connections.opened",
            "transport" => transport.as_str()
        )
        .increment(1);
    }

    pub fn connection_accepted(&self, context: &NacelleMetricsContext) {
        context.connection_accepted.increment(1);
        context.connection_active.increment(1.0);
    }

    pub fn connection_closed(&self, context: &NacelleMetricsContext, close_reason: &'static str) {
        metrics::counter!(
            "nacelle.connections.closed",
            attributes_with_label(
                context.connection_attributes.as_ref(),
                metrics::Label::from_static_parts("close_reason", close_reason),
            )
        )
        .increment(1);
        context.connection_active.decrement(1.0);
    }

    pub fn connection_rejected(&self, transport: NacelleTransport, reason: &'static str) {
        tracing::warn!(
            target: "nacelle",
            transport = transport.as_str(),
            reason,
            "connection rejected"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ConnectionRejected,
            transport: Some(transport),
            reason: Some(reason),
            count: 1,
        });
        metrics::counter!(
            "nacelle.rejections",
            "transport" => transport.as_str(),
            "reason" => reason
        )
        .increment(1);
    }

    pub fn request_rejected(&self, transport: NacelleTransport, reason: &'static str) {
        tracing::warn!(
            target: "nacelle",
            transport = transport.as_str(),
            reason,
            "request rejected"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::RequestRejected,
            transport: Some(transport),
            reason: Some(reason),
            count: 1,
        });
        metrics::counter!(
            "nacelle.rejections",
            "transport" => transport.as_str(),
            "reason" => reason
        )
        .increment(1);
    }

    pub fn request_started_with_context(&self, context: &NacelleMetricsContext) {
        if self.config.request_metrics.started {
            context.request_started.increment(1);
        }
        if self.config.request_metrics.in_flight {
            context.request_in_flight.increment(1.0);
        }
    }

    pub fn request_finished_with_context(
        &self,
        context: &NacelleMetricsContext,
        status: &'static str,
        request_bytes: usize,
        response_bytes: usize,
        elapsed: Duration,
    ) {
        let request_metrics = self.config.request_metrics;
        if request_metrics.completed {
            context.request_completed(status).increment(1);
        }
        if request_metrics.duration_ms {
            context
                .request_duration(status)
                .record(elapsed.as_secs_f64() * 1_000.0);
        }
        if request_metrics.byte_counts && request_bytes != 0 {
            context
                .request_bytes(status)
                .increment(request_bytes as u64);
        }
        if request_metrics.byte_counts && response_bytes != 0 {
            context
                .response_bytes(status)
                .increment(response_bytes as u64);
        }
        if request_metrics.in_flight {
            context.request_in_flight.decrement(1.0);
        }
    }

    pub fn request_completed(
        &self,
        transport: NacelleTransport,
        request_bytes: usize,
        response_bytes: usize,
        elapsed: Duration,
    ) {
        self.request_completed_inner(transport, request_bytes, response_bytes, elapsed, true);
    }

    #[doc(hidden)]
    pub fn request_completed_without_metrics(
        &self,
        transport: NacelleTransport,
        request_bytes: usize,
        response_bytes: usize,
        elapsed: Duration,
    ) {
        self.request_completed_inner(transport, request_bytes, response_bytes, elapsed, false);
    }

    fn request_completed_inner(
        &self,
        transport: NacelleTransport,
        request_bytes: usize,
        response_bytes: usize,
        elapsed: Duration,
        emit_metrics: bool,
    ) {
        tracing::debug!(
            target: "nacelle",
            transport = transport.as_str(),
            request_bytes,
            response_bytes,
            elapsed_us = elapsed.as_micros() as u64,
            "request completed"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::RequestCompleted,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
        let request_metrics = self.config.request_metrics;
        if emit_metrics && request_metrics.completed {
            metrics::counter!(
                "nacelle.requests.completed",
                "transport" => transport.as_str(),
                "status" => "ok"
            )
            .increment(1);
        }
        if emit_metrics && request_metrics.duration_ms {
            metrics::histogram!(
                "nacelle.request.duration_ms",
                "transport" => transport.as_str(),
                "status" => "ok"
            )
            .record(elapsed.as_secs_f64() * 1_000.0);
        }
        if emit_metrics && request_metrics.byte_counts {
            if request_bytes != 0 {
                metrics::counter!(
                    "nacelle.request.bytes",
                    "transport" => transport.as_str(),
                    "status" => "ok"
                )
                .increment(request_bytes as u64);
            }
            if response_bytes != 0 {
                metrics::counter!(
                    "nacelle.response.bytes",
                    "transport" => transport.as_str(),
                    "status" => "ok"
                )
                .increment(response_bytes as u64);
            }
        }
    }

    pub fn request_failed(
        &self,
        transport: NacelleTransport,
        elapsed: Duration,
        error: &crate::error::NacelleError,
    ) {
        self.request_failed_inner(transport, elapsed, error, true);
    }

    #[doc(hidden)]
    pub fn request_failed_without_metrics(
        &self,
        transport: NacelleTransport,
        elapsed: Duration,
        error: &crate::error::NacelleError,
    ) {
        self.request_failed_inner(transport, elapsed, error, false);
    }

    fn request_failed_inner(
        &self,
        transport: NacelleTransport,
        elapsed: Duration,
        error: &crate::error::NacelleError,
        emit_metrics: bool,
    ) {
        tracing::warn!(
            target: "nacelle",
            transport = transport.as_str(),
            elapsed_us = elapsed.as_micros() as u64,
            error = %error,
            "request failed"
        );
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::RequestFailed,
            transport: Some(transport),
            reason: error_reason(error),
            count: 1,
        });
        if emit_metrics {
            metrics::counter!(
                "nacelle.requests.failed",
                "transport" => transport.as_str()
            )
            .increment(1);
        }
        if emit_metrics && self.config.request_metrics.duration_ms {
            metrics::histogram!(
                "nacelle.request.duration_ms",
                "transport" => transport.as_str(),
                "status" => "error"
            )
            .record(elapsed.as_secs_f64() * 1_000.0);
        }
    }

    pub fn phase_duration(
        &self,
        context: &NacelleMetricsContext,
        phase: &'static str,
        elapsed: Duration,
    ) {
        #[cfg(feature = "phase-timing")]
        if self.phase_duration_metrics_enabled()
            && let Some(histogram) = context.phase_duration(phase)
        {
            histogram.record(elapsed.as_secs_f64() * 1_000.0);
        }
        #[cfg(not(feature = "phase-timing"))]
        let _ = (context, phase, elapsed);
    }

    pub fn operation_error(
        &self,
        context: &NacelleMetricsContext,
        phase: &'static str,
        error: &crate::error::NacelleError,
    ) {
        let mut attributes = context.request_attributes.to_vec();
        attributes.push(metrics::Label::from_static_parts("phase", phase));
        attributes.push(metrics::Label::from_static_parts(
            "error_kind",
            error_kind(error),
        ));
        metrics::counter!("nacelle.errors", attributes).increment(1);
        if let crate::error::NacelleError::ResourceLimit(limit) = error {
            let mut attributes = context.request_attributes.to_vec();
            attributes.push(metrics::Label::from_static_parts("limit", limit));
            attributes.push(metrics::Label::from_static_parts("phase", phase));
            metrics::counter!("nacelle.resource_limit.rejections", attributes).increment(1);
        }
    }

    pub fn timeout(&self, transport: NacelleTransport, operation: &'static str) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::Timeout,
            transport: Some(transport),
            reason: Some(operation),
            count: 1,
        });
        metrics::counter!(
            "nacelle.timeouts",
            "transport" => transport.as_str(),
            "operation" => operation
        )
        .increment(1);
    }

    pub fn shutdown_event(&self, kind: NacelleTelemetryEventKind, transport: NacelleTransport) {
        self.record(NacelleTelemetryEvent {
            kind,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
        metrics::counter!(
            "nacelle.shutdown_events",
            "transport" => transport.as_str(),
            "stage" => shutdown_stage(kind)
        )
        .increment(1);
    }

    pub fn shutdown_requested(&self) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ShutdownRequested,
            transport: None,
            reason: None,
            count: 1,
        });
        metrics::counter!(
            "nacelle.shutdown_events",
            "transport" => "host",
            "stage" => "requested"
        )
        .increment(1);
    }

    pub fn connections_aborted(&self, transport: NacelleTransport, count: usize) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ConnectionsAborted,
            transport: Some(transport),
            reason: None,
            count: count as u64,
        });
        metrics::counter!(
            "nacelle.connection_aborts",
            "transport" => transport.as_str()
        )
        .increment(count as u64);
    }

    pub fn response_body_bytes(&self, transport: NacelleTransport, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ResponseBodyBytes,
            transport: Some(transport),
            reason: None,
            count: bytes as u64,
        });
        if self.config.request_metrics.byte_counts {
            metrics::counter!(
                "nacelle.response.bytes",
                "transport" => transport.as_str()
            )
            .increment(bytes as u64);
        }
    }

    pub fn register_runtime_state(&self, state: crate::limits::NacelleRuntimeState) {
        let _ = state;
    }

    fn record(&self, event: NacelleTelemetryEvent) {
        self.observer.record(event);
    }
}

fn error_reason(error: &crate::error::NacelleError) -> Option<&'static str> {
    match error {
        crate::error::NacelleError::ResourceLimit(reason)
        | crate::error::NacelleError::Timeout(reason)
        | crate::error::NacelleError::InvalidFrame(reason) => Some(reason),
        crate::error::NacelleError::FrameTooLarge { .. } => Some("frame_too_large"),
        crate::error::NacelleError::UnexpectedEof => Some("unexpected_eof"),
        crate::error::NacelleError::ConnectionClosed => Some("connection_closed"),
        crate::error::NacelleError::MissingProtocol => Some("missing_protocol"),
        crate::error::NacelleError::Io(_) => Some("io"),
        crate::error::NacelleError::Protocol(_) => Some("protocol"),
        crate::error::NacelleError::Handler(_) => Some("handler"),
        crate::error::NacelleError::Join(_) => Some("join"),
    }
}

fn error_kind(error: &crate::error::NacelleError) -> &'static str {
    match error {
        crate::error::NacelleError::ResourceLimit(_) => "resource_limit",
        crate::error::NacelleError::Timeout(_) => "timeout",
        crate::error::NacelleError::InvalidFrame(_) => "invalid_frame",
        crate::error::NacelleError::FrameTooLarge { .. } => "frame_too_large",
        crate::error::NacelleError::UnexpectedEof => "unexpected_eof",
        crate::error::NacelleError::ConnectionClosed => "connection_closed",
        crate::error::NacelleError::MissingProtocol => "missing_protocol",
        crate::error::NacelleError::Io(_) => "io",
        crate::error::NacelleError::Protocol(_) => "protocol",
        crate::error::NacelleError::Handler(_) => "handler",
        crate::error::NacelleError::Join(_) => "join",
    }
}

fn shutdown_stage(kind: NacelleTelemetryEventKind) -> &'static str {
    match kind {
        NacelleTelemetryEventKind::ShutdownRequested => "requested",
        NacelleTelemetryEventKind::ListenerStoppedAccepting => "listener_stopped_accepting",
        NacelleTelemetryEventKind::DrainStarted => "drain_started",
        NacelleTelemetryEventKind::DrainCompleted => "drain_completed",
        NacelleTelemetryEventKind::DrainTimedOut => "drain_timed_out",
        NacelleTelemetryEventKind::ConnectionsAborted => "connections_aborted",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_observer_records_rejection_timeout_and_shutdown_events() {
        let observer = NacelleInMemoryObserver::new();
        let telemetry = NacelleTelemetry::new().with_observer(observer.clone());

        telemetry.connection_rejected(NacelleTransport::new("tcp"), "connections");
        telemetry.request_rejected(NacelleTransport::new("http"), "host");
        telemetry.timeout(NacelleTransport::new("tcp"), "request_body_read");
        telemetry.shutdown_requested();
        telemetry.shutdown_event(
            NacelleTelemetryEventKind::DrainCompleted,
            NacelleTransport::new("tcp"),
        );

        let events = observer.events();
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                NacelleTelemetryEventKind::ConnectionRejected,
                NacelleTelemetryEventKind::RequestRejected,
                NacelleTelemetryEventKind::Timeout,
                NacelleTelemetryEventKind::ShutdownRequested,
                NacelleTelemetryEventKind::DrainCompleted,
            ]
        );
        assert_eq!(events[0].reason, Some("connections"));
        assert_eq!(events[1].reason, Some("host"));
        assert_eq!(events[2].reason, Some("request_body_read"));
        assert_eq!(events[3].transport, None);
    }

    #[test]
    fn concrete_and_composite_observers_record_without_dynamic_adapter() {
        let first = NacelleInMemoryObserver::new();
        let second = NacelleInMemoryObserver::new();
        let telemetry = NacelleTelemetry::new()
            .with_observer(first.clone())
            .with_additional_observer(second.clone());

        telemetry.timeout(NacelleTransport::new("tcp"), "test");

        assert_eq!(first.events().len(), 1);
        assert_eq!(second.events().len(), 1);
        assert!(telemetry.request_events_enabled());
        const {
            assert!(!<Arc<NoopObserver> as NacelleTelemetryObserver>::ENABLED);
        }
    }

    #[test]
    fn request_duration_metrics_are_opt_in() {
        let telemetry = NacelleTelemetry::default();

        assert!(telemetry.config().request_metrics.started);
        assert!(telemetry.config().request_metrics.completed);
        assert!(!telemetry.config().request_metrics.in_flight);
        assert!(!telemetry.config().request_metrics.duration_ms);
        assert!(telemetry.config().request_metrics.byte_counts);
        assert!(!telemetry.config().phase_duration_metrics);
        assert!(!telemetry.request_duration_metrics_enabled());

        let telemetry = telemetry
            .with_request_started_metrics(false)
            .with_request_completed_metrics(false)
            .with_request_duration_metrics(true)
            .with_byte_count_metrics(false)
            .with_request_in_flight_metrics(true)
            .with_phase_duration_metrics(true);

        assert!(!telemetry.config().request_metrics.started);
        assert!(!telemetry.config().request_metrics.completed);
        assert!(telemetry.config().request_metrics.in_flight);
        assert!(telemetry.config().request_metrics.duration_ms);
        assert!(!telemetry.config().request_metrics.byte_counts);
        assert!(telemetry.config().phase_duration_metrics);
        assert!(telemetry.request_duration_metrics_enabled());
        assert_eq!(
            telemetry.phase_duration_metrics_enabled(),
            cfg!(feature = "phase-timing")
        );
    }

    #[test]
    fn request_duration_metrics_require_runtime_activation() {
        let duration_disabled = NacelleTelemetry::default().with_request_duration_metrics(false);
        assert!(!duration_disabled.request_duration_metrics_enabled());

        let enabled = NacelleTelemetry::default().with_request_duration_metrics(true);
        assert!(enabled.request_duration_metrics_enabled());
    }
}
