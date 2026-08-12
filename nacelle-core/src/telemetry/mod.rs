use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

mod sink;
pub use sink::{
    CompositeObserver, NacelleInMemoryObserver, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetryObserver, NacelleTransport, NoopObserver,
};

fn register_metric_descriptions() {
    metrics::describe_histogram!(
        "nacelle.request.duration",
        metrics::Unit::Seconds,
        "Time spent processing a request"
    );
    metrics::describe_histogram!(
        "nacelle.request.body.size",
        metrics::Unit::Bytes,
        "Request body size"
    );
    metrics::describe_histogram!(
        "nacelle.response.body.size",
        metrics::Unit::Bytes,
        "Response body size"
    );
}

#[derive(Clone)]
pub struct NacelleTelemetry<Observer = NoopObserver> {
    config: NacelleTelemetryConfig,
    observer: Observer,
}

/// Runtime switches for Nacelle-owned metric domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleTelemetryConfig {
    /// Global metric emission gate. Individual domain settings are preserved.
    pub metrics: bool,
    /// Connection lifecycle counters and gauges.
    pub connection_metrics: bool,
    /// Request counters, gauges, byte counts, and duration histograms.
    pub request_metrics: NacelleRequestMetricsConfig,
    /// Runtime permit, memory, shutdown, and abort metrics.
    pub runtime_metrics: bool,
    /// Rejection, timeout, request-failure, and operation-error counters.
    pub error_metrics: bool,
    /// TCP phase histograms when the `phase-timing` feature is also enabled.
    pub phase_duration_metrics: bool,
}

impl Default for NacelleTelemetryConfig {
    fn default() -> Self {
        Self {
            metrics: true,
            connection_metrics: true,
            request_metrics: NacelleRequestMetricsConfig::default(),
            runtime_metrics: true,
            error_metrics: true,
            phase_duration_metrics: false,
        }
    }
}

/// Independent request metric switches.
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
    connection_accepted: OnceLock<metrics::Counter>,
    connection_active: OnceLock<metrics::Gauge>,
    request_started: OnceLock<metrics::Counter>,
    request_in_flight: OnceLock<metrics::Gauge>,
    request_completed_ok: OnceLock<metrics::Counter>,
    request_completed_error: OnceLock<metrics::Counter>,
    request_duration_ok: OnceLock<metrics::Histogram>,
    request_duration_error: OnceLock<metrics::Histogram>,
    request_bytes_ok: OnceLock<metrics::Histogram>,
    request_bytes_error: OnceLock<metrics::Histogram>,
    response_bytes_ok: OnceLock<metrics::Histogram>,
    response_bytes_error: OnceLock<metrics::Histogram>,
    #[cfg(feature = "phase-timing")]
    phase_durations: [OnceLock<metrics::Histogram>; 6],
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
        Self {
            transport,
            listener,
            protocol,
            tls,
            connection_accepted: OnceLock::new(),
            connection_active: OnceLock::new(),
            request_started: OnceLock::new(),
            request_in_flight: OnceLock::new(),
            request_completed_ok: OnceLock::new(),
            request_completed_error: OnceLock::new(),
            request_duration_ok: OnceLock::new(),
            request_duration_error: OnceLock::new(),
            request_bytes_ok: OnceLock::new(),
            request_bytes_error: OnceLock::new(),
            response_bytes_ok: OnceLock::new(),
            response_bytes_error: OnceLock::new(),
            #[cfg(feature = "phase-timing")]
            phase_durations: std::array::from_fn(|_| OnceLock::new()),
            connection_attributes,
            request_attributes,
        }
    }

    fn connection_accepted(&self) -> &metrics::Counter {
        self.connection_accepted.get_or_init(|| {
            metrics::counter!(
                "nacelle.connection.accepted",
                self.connection_attributes.to_vec()
            )
        })
    }

    fn connection_active(&self) -> &metrics::Gauge {
        self.connection_active.get_or_init(|| {
            metrics::gauge!(
                "nacelle.connection.active",
                self.connection_attributes.to_vec()
            )
        })
    }

    fn request_started(&self) -> &metrics::Counter {
        self.request_started.get_or_init(|| {
            metrics::counter!("nacelle.request.started", self.request_attributes.to_vec())
        })
    }

    fn request_in_flight(&self) -> &metrics::Gauge {
        self.request_in_flight.get_or_init(|| {
            metrics::gauge!("nacelle.request.active", self.request_attributes.to_vec())
        })
    }

    fn request_completed(&self, status: &'static str) -> &metrics::Counter {
        let handle = match status {
            "error" => &self.request_completed_error,
            _ => &self.request_completed_ok,
        };
        handle.get_or_init(|| {
            metrics::counter!("nacelle.request.completed", self.status_attributes(status))
        })
    }

    fn request_duration(&self, status: &'static str) -> &metrics::Histogram {
        let handle = match status {
            "error" => &self.request_duration_error,
            _ => &self.request_duration_ok,
        };
        handle.get_or_init(|| {
            metrics::histogram!("nacelle.request.duration", self.status_attributes(status))
        })
    }

    fn request_bytes(&self, status: &'static str) -> &metrics::Histogram {
        let handle = match status {
            "error" => &self.request_bytes_error,
            _ => &self.request_bytes_ok,
        };
        handle.get_or_init(|| {
            metrics::histogram!("nacelle.request.body.size", self.status_attributes(status))
        })
    }

    fn response_bytes(&self, status: &'static str) -> &metrics::Histogram {
        let handle = match status {
            "error" => &self.response_bytes_error,
            _ => &self.response_bytes_ok,
        };
        handle.get_or_init(|| {
            metrics::histogram!("nacelle.response.body.size", self.status_attributes(status))
        })
    }

    fn status_attributes(&self, status: &'static str) -> Vec<metrics::Label> {
        let status = match status {
            "error" => "error",
            _ => "ok",
        };
        attributes_with_label(
            self.request_attributes.as_ref(),
            metrics::Label::from_static_parts("status", status),
        )
    }

    #[cfg(feature = "phase-timing")]
    fn phase_duration(&self, phase: &'static str) -> Option<&metrics::Histogram> {
        PHASES
            .iter()
            .position(|candidate| *candidate == phase)
            .map(|index| {
                self.phase_durations[index].get_or_init(|| {
                    metrics::histogram!(
                        "nacelle.phase.duration_ms",
                        attributes_with_label(
                            self.request_attributes.as_ref(),
                            metrics::Label::from_static_parts("phase", phase),
                        )
                    )
                })
            })
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
        register_metric_descriptions();
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

    /// Enable or disable all metrics without changing individual domain settings.
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.config.metrics = enabled;
        self
    }

    /// Enable or disable connection lifecycle metrics.
    pub fn with_connection_metrics(mut self, enabled: bool) -> Self {
        self.config.connection_metrics = enabled;
        self
    }

    /// Enable or disable runtime permit, memory, shutdown, and abort metrics.
    pub fn with_runtime_metrics(mut self, enabled: bool) -> Self {
        self.config.runtime_metrics = enabled;
        self
    }

    /// Enable or disable rejection, timeout, failure, and operation-error metrics.
    pub fn with_error_metrics(mut self, enabled: bool) -> Self {
        self.config.error_metrics = enabled;
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
        self.config.metrics
    }

    pub const fn connection_metrics_enabled(&self) -> bool {
        self.config.metrics && self.config.connection_metrics
    }

    pub const fn runtime_metrics_enabled(&self) -> bool {
        self.config.metrics && self.config.runtime_metrics
    }

    pub const fn error_metrics_enabled(&self) -> bool {
        self.config.metrics && self.config.error_metrics
    }

    pub fn context_metrics_enabled(&self) -> bool {
        self.connection_metrics_enabled()
            || self.request_metrics_enabled()
            || self.error_metrics_enabled()
            || self.phase_duration_metrics_enabled()
    }

    pub fn request_metrics_enabled(&self) -> bool {
        self.config.metrics && self.config.request_metrics.enabled()
    }

    pub fn request_duration_metrics_enabled(&self) -> bool {
        self.config.metrics && self.config.request_metrics.duration_ms
    }

    pub fn phase_duration_metrics_enabled(&self) -> bool {
        self.config.metrics && cfg!(feature = "phase-timing") && self.config.phase_duration_metrics
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
        if self.connection_metrics_enabled() {
            metrics::counter!(
                "nacelle.connection.opened",
                "transport" => transport.as_str()
            )
            .increment(1);
        }
    }

    pub fn connection_accepted(&self, context: &NacelleMetricsContext) {
        if self.connection_metrics_enabled() {
            context.connection_accepted().increment(1);
            context.connection_active().increment(1.0);
        }
    }

    pub fn connection_closed(&self, context: &NacelleMetricsContext, close_reason: &'static str) {
        if self.connection_metrics_enabled() {
            metrics::counter!(
                "nacelle.connection.closed",
                attributes_with_label(
                    context.connection_attributes.as_ref(),
                    metrics::Label::from_static_parts("close_reason", close_reason),
                )
            )
            .increment(1);
            context.connection_active().decrement(1.0);
        }
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
        if self.error_metrics_enabled() {
            metrics::counter!(
                "nacelle.connection.rejected",
                "transport" => transport.as_str(),
                "reason" => reason
            )
            .increment(1);
        }
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
        if self.error_metrics_enabled() {
            metrics::counter!(
                "nacelle.request.rejected",
                "transport" => transport.as_str(),
                "reason" => reason
            )
            .increment(1);
        }
    }

    pub fn request_started_with_context(&self, context: &NacelleMetricsContext) {
        if !self.config.metrics {
            return;
        }
        if self.config.request_metrics.started {
            context.request_started().increment(1);
        }
        if self.config.request_metrics.in_flight {
            context.request_in_flight().increment(1.0);
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
        if !self.config.metrics {
            return;
        }
        let request_metrics = self.config.request_metrics;
        if request_metrics.completed {
            context.request_completed(status).increment(1);
        }
        if request_metrics.duration_ms {
            context
                .request_duration(status)
                .record(elapsed.as_secs_f64());
        }
        if request_metrics.byte_counts && request_bytes != 0 {
            context.request_bytes(status).record(request_bytes as f64);
        }
        if request_metrics.byte_counts && response_bytes != 0 {
            context.response_bytes(status).record(response_bytes as f64);
        }
        if request_metrics.in_flight {
            context.request_in_flight().decrement(1.0);
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
        let emit_metrics = emit_metrics && self.config.metrics;
        if emit_metrics && request_metrics.completed {
            metrics::counter!(
                "nacelle.request.completed",
                "transport" => transport.as_str(),
                "status" => "ok"
            )
            .increment(1);
        }
        if emit_metrics && request_metrics.duration_ms {
            metrics::histogram!(
                "nacelle.request.duration",
                "transport" => transport.as_str(),
                "status" => "ok"
            )
            .record(elapsed.as_secs_f64());
        }
        if emit_metrics && request_metrics.byte_counts {
            if request_bytes != 0 {
                metrics::histogram!(
                    "nacelle.request.body.size",
                    "transport" => transport.as_str(),
                    "status" => "ok"
                )
                .record(request_bytes as f64);
            }
            if response_bytes != 0 {
                metrics::histogram!(
                    "nacelle.response.body.size",
                    "transport" => transport.as_str(),
                    "status" => "ok"
                )
                .record(response_bytes as f64);
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
        if emit_metrics && self.error_metrics_enabled() {
            metrics::counter!("nacelle.request.failed", "transport" => transport.as_str())
                .increment(1);
            if matches!(error, crate::error::NacelleError::Timeout(_)) {
                metrics::counter!(
                    "nacelle.request.timed_out",
                    "transport" => transport.as_str(),
                    "operation" => error_reason(error).expect("timeout has a reason")
                )
                .increment(1);
            }
        }
        if emit_metrics && self.config.metrics && self.config.request_metrics.duration_ms {
            metrics::histogram!(
                "nacelle.request.duration",
                "transport" => transport.as_str(),
                "status" => "error"
            )
            .record(elapsed.as_secs_f64());
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
        if !self.error_metrics_enabled() {
            return;
        }
        let mut attributes = context.request_attributes.to_vec();
        attributes.push(metrics::Label::from_static_parts("phase", phase));
        attributes.push(metrics::Label::from_static_parts(
            "error_kind",
            error_kind(error),
        ));
        metrics::counter!("nacelle.errors", attributes).increment(1);
        if let crate::error::NacelleError::ResourceLimit(limit) = error {
            let mut attributes = context.request_attributes.to_vec();
            attributes.push(metrics::Label::from_static_parts("limit", limit.as_str()));
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
        if self.error_metrics_enabled() {
            metrics::counter!(
                "nacelle.timeouts",
                "transport" => transport.as_str(),
                "operation" => operation
            )
            .increment(1);
        }
    }

    pub fn shutdown_event(&self, kind: NacelleTelemetryEventKind, transport: NacelleTransport) {
        self.record(NacelleTelemetryEvent {
            kind,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
        if self.runtime_metrics_enabled() {
            metrics::counter!(
                "nacelle.shutdown_events",
                "transport" => transport.as_str(),
                "stage" => shutdown_stage(kind)
            )
            .increment(1);
        }
    }

    pub fn shutdown_requested(&self) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ShutdownRequested,
            transport: None,
            reason: None,
            count: 1,
        });
        if self.runtime_metrics_enabled() {
            metrics::counter!(
                "nacelle.shutdown_events",
                "transport" => "host",
                "stage" => "requested"
            )
            .increment(1);
        }
    }

    pub fn connections_aborted(&self, transport: NacelleTransport, count: usize) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ConnectionsAborted,
            transport: Some(transport),
            reason: None,
            count: count as u64,
        });
        if self.runtime_metrics_enabled() {
            metrics::counter!(
                "nacelle.connection_aborts",
                "transport" => transport.as_str()
            )
            .increment(count as u64);
        }
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
        if self.config.metrics && self.config.request_metrics.byte_counts {
            metrics::histogram!(
                "nacelle.response.body.size",
                "transport" => transport.as_str()
            )
            .record(bytes as f64);
        }
    }

    pub fn register_runtime_state(&self, state: crate::limits::NacelleRuntimeState) {
        state.set_metrics_enabled(self.runtime_metrics_enabled());
    }

    fn record(&self, event: NacelleTelemetryEvent) {
        self.observer.record(event);
    }
}

fn error_reason(error: &crate::error::NacelleError) -> Option<&'static str> {
    match error {
        crate::error::NacelleError::ResourceLimit(reason) => Some(reason.as_str()),
        crate::error::NacelleError::Timeout(reason) => Some(reason.as_str()),
        crate::error::NacelleError::InvalidFrame(reason) => Some(reason),
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
    use std::collections::{HashMap, HashSet};

    use metrics::Unit;
    use metrics_util::debugging::DebugValue;
    use metrics_util::debugging::DebuggingRecorder;

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
        assert!(telemetry.config().metrics);
        assert!(telemetry.config().connection_metrics);
        assert!(telemetry.config().runtime_metrics);
        assert!(telemetry.config().error_metrics);
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

    #[test]
    fn metric_schema_uses_singular_names_and_base_units() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let telemetry = NacelleTelemetry::default().with_request_duration_metrics(true);
            let context = NacelleMetricsContext::new(
                NacelleTransport::new("tcp"),
                Arc::from("test"),
                "test",
                "none",
            );
            telemetry.connection_opened(NacelleTransport::new("tcp"));
            telemetry.connection_accepted(&context);
            telemetry.connection_closed(&context, "eof");
            telemetry.connection_rejected(NacelleTransport::new("tcp"), "connections");
            telemetry.request_rejected(NacelleTransport::new("tcp"), "requests");
            telemetry.request_started_with_context(&context);
            telemetry.request_finished_with_context(
                &context,
                "ok",
                4,
                8,
                Duration::from_millis(250),
            );
            telemetry.request_failed(
                NacelleTransport::new("tcp"),
                Duration::from_millis(500),
                &crate::error::NacelleError::Timeout(crate::error::NacelleTimeoutReason::Handler),
            );
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let names: HashSet<_> = snapshot
            .iter()
            .map(|(key, _, _, _)| key.key().name())
            .collect();
        for name in [
            "nacelle.connection.opened",
            "nacelle.connection.accepted",
            "nacelle.connection.active",
            "nacelle.connection.closed",
            "nacelle.connection.rejected",
            "nacelle.request.started",
            "nacelle.request.completed",
            "nacelle.request.rejected",
            "nacelle.request.timed_out",
            "nacelle.request.failed",
            "nacelle.request.duration",
            "nacelle.request.body.size",
            "nacelle.response.body.size",
        ] {
            assert!(names.contains(name), "missing metric {name}");
        }

        let units: HashMap<_, _> = snapshot
            .iter()
            .filter_map(|(key, unit, _, value)| {
                matches!(value, DebugValue::Histogram(_))
                    .then_some((key.key().name(), unit.as_ref()))
            })
            .collect();
        assert_eq!(units["nacelle.request.duration"], Some(&Unit::Seconds));
        assert_eq!(units["nacelle.request.body.size"], Some(&Unit::Bytes));
        assert_eq!(units["nacelle.response.body.size"], Some(&Unit::Bytes));
    }

    #[test]
    fn global_metric_switch_preserves_domains_and_observers() {
        let observer = NacelleInMemoryObserver::new();
        let telemetry = NacelleTelemetry::default()
            .with_request_duration_metrics(true)
            .with_metrics(false)
            .with_observer(observer.clone());

        assert!(!telemetry.metrics_enabled());
        assert!(!telemetry.connection_metrics_enabled());
        assert!(!telemetry.request_metrics_enabled());
        assert!(!telemetry.request_duration_metrics_enabled());
        assert!(!telemetry.runtime_metrics_enabled());
        assert!(!telemetry.error_metrics_enabled());
        assert!(telemetry.config().request_metrics.duration_ms);

        telemetry.timeout(NacelleTransport::new("tcp"), "test");
        assert_eq!(observer.events().len(), 1);

        let telemetry = telemetry.with_metrics(true);
        assert!(telemetry.connection_metrics_enabled());
        assert!(telemetry.request_duration_metrics_enabled());
        assert!(telemetry.runtime_metrics_enabled());
        assert!(telemetry.error_metrics_enabled());
    }

    #[test]
    fn global_metric_switch_suppresses_emission_but_not_observers() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let observer = NacelleInMemoryObserver::new();

        metrics::with_local_recorder(&recorder, || {
            let telemetry = NacelleTelemetry::default()
                .with_metrics(false)
                .with_observer(observer.clone());
            let context = NacelleMetricsContext::new(
                NacelleTransport::new("tcp"),
                Arc::from("test"),
                "test",
                "none",
            );
            telemetry.connection_opened(NacelleTransport::new("tcp"));
            telemetry.connection_accepted(&context);
            telemetry.connection_closed(&context, "eof");
            telemetry.connection_rejected(NacelleTransport::new("tcp"), "connections");
            telemetry.request_started_with_context(&context);
            telemetry.request_finished_with_context(&context, "ok", 4, 8, Duration::from_millis(1));
            telemetry.operation_error(
                &context,
                "decode",
                &crate::error::NacelleError::InvalidFrame("test"),
            );
            telemetry.timeout(NacelleTransport::new("tcp"), "tcp_read");
            telemetry.shutdown_requested();
            telemetry.request_completed(
                NacelleTransport::new("tcp"),
                4,
                8,
                Duration::from_millis(1),
            );
        });

        assert!(snapshotter.snapshot().into_vec().is_empty());
        assert_eq!(observer.events().len(), 5);
    }
}
