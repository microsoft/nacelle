use std::time::Duration;

use std::sync::Arc;
#[cfg(feature = "otel")]
use std::sync::Mutex;

mod sink;
pub use sink::{
    NacelleInMemoryTelemetrySink, NacelleTelemetryEvent, NacelleTelemetryEventKind,
    NacelleTelemetrySink, NacelleTransport,
};

#[cfg(feature = "otel")]
mod attributes;
#[cfg(feature = "otel")]
use attributes::{
    attributes_with_key_value, connection_attributes, error_attributes, phase_attributes,
    request_attributes, shutdown_stage,
};

#[derive(Clone)]
pub struct NacelleTelemetry {
    config: NacelleTelemetryConfig,
    sink: Option<Arc<dyn NacelleTelemetrySink>>,
    #[cfg(feature = "otel")]
    metrics: std::sync::Arc<OtelMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleTelemetryConfig {
    pub metrics: bool,
    pub request_metrics: NacelleRequestMetricsConfig,
    pub phase_duration_metrics: bool,
}

impl Default for NacelleTelemetryConfig {
    fn default() -> Self {
        Self {
            metrics: true,
            request_metrics: NacelleRequestMetricsConfig::default(),
            phase_duration_metrics: false,
        }
    }
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
    #[cfg(feature = "otel")]
    connection_attributes: Arc<[opentelemetry::KeyValue]>,
    #[cfg(feature = "otel")]
    request_attributes: Arc<[opentelemetry::KeyValue]>,
    #[cfg(feature = "otel")]
    request_ok_attributes: Arc<[opentelemetry::KeyValue]>,
    #[cfg(feature = "otel")]
    request_error_attributes: Arc<[opentelemetry::KeyValue]>,
}

impl NacelleMetricsContext {
    pub fn new(
        transport: NacelleTransport,
        listener: Arc<str>,
        protocol: &'static str,
        tls: &'static str,
    ) -> Self {
        #[cfg(feature = "otel")]
        let connection_attributes = connection_attributes(transport, &listener, tls);
        #[cfg(feature = "otel")]
        let request_attributes = request_attributes(connection_attributes.as_ref(), protocol);
        #[cfg(feature = "otel")]
        let request_ok_attributes = attributes_with_key_value(
            request_attributes.as_ref(),
            opentelemetry::KeyValue::new("status", "ok"),
        );
        #[cfg(feature = "otel")]
        let request_error_attributes = attributes_with_key_value(
            request_attributes.as_ref(),
            opentelemetry::KeyValue::new("status", "error"),
        );

        Self {
            transport,
            listener,
            protocol,
            tls,
            #[cfg(feature = "otel")]
            connection_attributes,
            #[cfg(feature = "otel")]
            request_attributes,
            #[cfg(feature = "otel")]
            request_ok_attributes,
            #[cfg(feature = "otel")]
            request_error_attributes,
        }
    }

    #[cfg(feature = "otel")]
    fn connection_attributes(&self) -> &[opentelemetry::KeyValue] {
        &self.connection_attributes
    }

    #[cfg(feature = "otel")]
    fn request_attributes(&self) -> &[opentelemetry::KeyValue] {
        &self.request_attributes
    }

    #[cfg(feature = "otel")]
    fn request_status_attributes(&self, status: &'static str) -> &[opentelemetry::KeyValue] {
        match status {
            "ok" => &self.request_ok_attributes,
            "error" => &self.request_error_attributes,
            _ => &self.request_attributes,
        }
    }
}

impl std::fmt::Debug for NacelleTelemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NacelleTelemetry")
            .field("config", &self.config)
            .field("has_sink", &self.sink.is_some())
            .finish()
    }
}

impl Default for NacelleTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleTelemetry {
    pub fn new() -> Self {
        Self {
            config: NacelleTelemetryConfig::default(),
            sink: None,
            #[cfg(feature = "otel")]
            metrics: std::sync::Arc::new(OtelMetrics::new()),
        }
    }

    pub fn with_config(mut self, config: NacelleTelemetryConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_sink(mut self, sink: Arc<dyn NacelleTelemetrySink>) -> Self {
        self.sink = Some(sink);
        self
    }

    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.config.metrics = enabled;
        self
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

    pub fn metrics_enabled(&self) -> bool {
        cfg!(feature = "otel") && self.config.metrics
    }

    pub fn request_metrics_enabled(&self) -> bool {
        self.metrics_enabled() && self.config.request_metrics.enabled()
    }

    pub fn request_duration_metrics_enabled(&self) -> bool {
        self.config.request_metrics.duration_ms
    }

    pub fn phase_duration_metrics_enabled(&self) -> bool {
        self.metrics_enabled() && self.config.phase_duration_metrics
    }

    pub fn request_events_enabled(&self) -> bool {
        self.sink.is_some() || self.metrics_enabled()
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
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.connection_count.add(
                1,
                &[opentelemetry::KeyValue::new(
                    "transport",
                    transport.as_str(),
                )],
            );
        }
    }

    pub fn connection_accepted(&self, context: &NacelleMetricsContext) {
        let _ = context;
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            let attributes = context.connection_attributes();
            self.metrics.connection_accepted.add(1, attributes);
            self.metrics.connection_active.add(1, attributes);
        }
    }

    pub fn connection_closed(&self, context: &NacelleMetricsContext, close_reason: &'static str) {
        let _ = (context, close_reason);
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            let mut attributes = context.connection_attributes().to_vec();
            attributes.push(opentelemetry::KeyValue::new("close_reason", close_reason));
            self.metrics.connection_closed.add(1, &attributes);
            self.metrics
                .connection_active
                .add(-1, context.connection_attributes());
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
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.rejection_count.add(
                1,
                &[
                    opentelemetry::KeyValue::new("transport", transport.as_str()),
                    opentelemetry::KeyValue::new("reason", reason),
                ],
            );
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
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.rejection_count.add(
                1,
                &[
                    opentelemetry::KeyValue::new("transport", transport.as_str()),
                    opentelemetry::KeyValue::new("reason", reason),
                ],
            );
        }
    }

    pub fn request_started_with_context(&self, context: &NacelleMetricsContext) {
        let _ = context;
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            let request_metrics = self.config.request_metrics;
            let attributes = context.request_attributes();
            if request_metrics.started {
                self.metrics.request_started.add(1, attributes);
            }
            if request_metrics.in_flight {
                self.metrics.request_in_flight.add(1, attributes);
            }
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
        let _ = (context, status, request_bytes, response_bytes, elapsed);
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            let request_metrics = self.config.request_metrics;
            let attributes = context.request_status_attributes(status);
            if request_metrics.completed {
                self.metrics.request_count.add(1, attributes);
            }
            if request_metrics.duration_ms {
                self.metrics
                    .request_duration_ms
                    .record(elapsed.as_secs_f64() * 1_000.0, attributes);
            }
            if request_metrics.byte_counts && request_bytes != 0 {
                self.metrics
                    .request_bytes
                    .add(request_bytes as u64, attributes);
            }
            if request_metrics.byte_counts && response_bytes != 0 {
                self.metrics
                    .response_bytes
                    .add(response_bytes as u64, attributes);
            }

            if request_metrics.in_flight {
                self.metrics
                    .request_in_flight
                    .add(-1, context.request_attributes());
            }
        }
    }

    pub fn request_completed(
        &self,
        transport: NacelleTransport,
        request_bytes: usize,
        response_bytes: usize,
        elapsed: Duration,
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
        #[cfg(feature = "otel")]
        {
            let attributes = [opentelemetry::KeyValue::new(
                "transport",
                transport.as_str(),
            )];
            let request_metrics = self.config.request_metrics;
            if self.config.metrics && request_metrics.completed {
                self.metrics.request_count.add(1, &attributes);
            }
            if self.config.metrics && request_metrics.duration_ms {
                self.metrics
                    .request_duration_ms
                    .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
            }
            if self.config.metrics && request_metrics.byte_counts {
                self.metrics
                    .request_bytes
                    .add(request_bytes as u64, &attributes);
                self.metrics
                    .response_bytes
                    .add(response_bytes as u64, &attributes);
            }
        }
    }

    pub fn request_failed(
        &self,
        transport: NacelleTransport,
        elapsed: Duration,
        error: &crate::error::NacelleError,
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
        #[cfg(feature = "otel")]
        {
            let attributes = [opentelemetry::KeyValue::new(
                "transport",
                transport.as_str(),
            )];
            if self.config.metrics {
                self.metrics.request_error_count.add(1, &attributes);
            }
            if self.config.metrics && self.config.request_metrics.duration_ms {
                self.metrics
                    .request_duration_ms
                    .record(elapsed.as_secs_f64() * 1_000.0, &attributes);
            }
        }
    }

    pub fn phase_duration(
        &self,
        context: &NacelleMetricsContext,
        phase: &'static str,
        elapsed: Duration,
    ) {
        let _ = (context, phase, elapsed);
        #[cfg(feature = "otel")]
        if self.phase_duration_metrics_enabled() {
            self.metrics.phase_duration_ms.record(
                elapsed.as_secs_f64() * 1_000.0,
                &phase_attributes(context, phase),
            );
        }
    }

    pub fn operation_error(
        &self,
        context: &NacelleMetricsContext,
        phase: &'static str,
        error: &crate::error::NacelleError,
    ) {
        let _ = (context, phase, error);
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics
                .errors
                .add(1, &error_attributes(context, phase, error));
            if let crate::error::NacelleError::ResourceLimit(limit) = error {
                let mut attributes = context.request_attributes().to_vec();
                attributes.push(opentelemetry::KeyValue::new("limit", *limit));
                attributes.push(opentelemetry::KeyValue::new("phase", phase));
                self.metrics.resource_limit_rejections.add(1, &attributes);
            }
        }
    }

    pub fn timeout(&self, transport: NacelleTransport, operation: &'static str) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::Timeout,
            transport: Some(transport),
            reason: Some(operation),
            count: 1,
        });
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.timeout_count.add(
                1,
                &[
                    opentelemetry::KeyValue::new("transport", transport.as_str()),
                    opentelemetry::KeyValue::new("operation", operation),
                ],
            );
        }
    }

    pub fn shutdown_event(&self, kind: NacelleTelemetryEventKind, transport: NacelleTransport) {
        self.record(NacelleTelemetryEvent {
            kind,
            transport: Some(transport),
            reason: None,
            count: 1,
        });
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.shutdown_event_count.add(
                1,
                &[
                    opentelemetry::KeyValue::new("transport", transport.as_str()),
                    opentelemetry::KeyValue::new("stage", shutdown_stage(kind)),
                ],
            );
        }
    }

    pub fn shutdown_requested(&self) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ShutdownRequested,
            transport: None,
            reason: None,
            count: 1,
        });
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.shutdown_event_count.add(
                1,
                &[
                    opentelemetry::KeyValue::new("transport", "host"),
                    opentelemetry::KeyValue::new("stage", "requested"),
                ],
            );
        }
    }

    pub fn connections_aborted(&self, transport: NacelleTransport, count: usize) {
        self.record(NacelleTelemetryEvent {
            kind: NacelleTelemetryEventKind::ConnectionsAborted,
            transport: Some(transport),
            reason: None,
            count: count as u64,
        });
        #[cfg(feature = "otel")]
        if self.metrics_enabled() {
            self.metrics.connection_abort_count.add(
                count as u64,
                &[opentelemetry::KeyValue::new(
                    "transport",
                    transport.as_str(),
                )],
            );
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
        #[cfg(feature = "otel")]
        if self.metrics_enabled() && self.config.request_metrics.byte_counts {
            self.metrics.response_bytes.add(
                bytes as u64,
                &[opentelemetry::KeyValue::new(
                    "transport",
                    transport.as_str(),
                )],
            );
        }
    }

    pub fn register_runtime_state(&self, state: crate::limits::NacelleRuntimeState) {
        #[cfg(feature = "otel")]
        self.metrics.register_runtime_state(state);
        #[cfg(not(feature = "otel"))]
        let _ = state;
    }

    fn record(&self, event: NacelleTelemetryEvent) {
        if let Some(sink) = &self.sink {
            sink.record(event);
        }
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

#[cfg(feature = "otel")]
#[derive(Debug)]
struct OtelMetrics {
    runtime_states: Arc<Mutex<Vec<crate::limits::NacelleRuntimeState>>>,
    runtime_state_gauges: Mutex<Vec<opentelemetry::metrics::ObservableGauge<u64>>>,
    connection_active: opentelemetry::metrics::UpDownCounter<i64>,
    connection_accepted: opentelemetry::metrics::Counter<u64>,
    connection_count: opentelemetry::metrics::Counter<u64>,
    connection_closed: opentelemetry::metrics::Counter<u64>,
    request_started: opentelemetry::metrics::Counter<u64>,
    request_in_flight: opentelemetry::metrics::UpDownCounter<i64>,
    request_count: opentelemetry::metrics::Counter<u64>,
    request_error_count: opentelemetry::metrics::Counter<u64>,
    rejection_count: opentelemetry::metrics::Counter<u64>,
    timeout_count: opentelemetry::metrics::Counter<u64>,
    shutdown_event_count: opentelemetry::metrics::Counter<u64>,
    connection_abort_count: opentelemetry::metrics::Counter<u64>,
    request_bytes: opentelemetry::metrics::Counter<u64>,
    response_bytes: opentelemetry::metrics::Counter<u64>,
    request_duration_ms: opentelemetry::metrics::Histogram<f64>,
    phase_duration_ms: opentelemetry::metrics::Histogram<f64>,
    errors: opentelemetry::metrics::Counter<u64>,
    resource_limit_rejections: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel")]
impl OtelMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("nacelle");
        Self {
            runtime_states: Arc::new(Mutex::new(Vec::new())),
            runtime_state_gauges: Mutex::new(Vec::new()),
            connection_active: meter
                .i64_up_down_counter("nacelle.connections.in_flight")
                .build(),
            connection_accepted: meter.u64_counter("nacelle.connections.accepted").build(),
            connection_count: meter.u64_counter("nacelle.connections.opened").build(),
            connection_closed: meter.u64_counter("nacelle.connections.closed").build(),
            request_started: meter.u64_counter("nacelle.requests.started").build(),
            request_in_flight: meter
                .i64_up_down_counter("nacelle.requests.in_flight")
                .build(),
            request_count: meter.u64_counter("nacelle.requests.completed").build(),
            request_error_count: meter.u64_counter("nacelle.requests.failed").build(),
            rejection_count: meter.u64_counter("nacelle.rejections").build(),
            timeout_count: meter.u64_counter("nacelle.timeouts").build(),
            shutdown_event_count: meter.u64_counter("nacelle.shutdown_events").build(),
            connection_abort_count: meter.u64_counter("nacelle.connection_aborts").build(),
            request_bytes: meter.u64_counter("nacelle.request.bytes").build(),
            response_bytes: meter.u64_counter("nacelle.response.bytes").build(),
            request_duration_ms: meter.f64_histogram("nacelle.request.duration_ms").build(),
            phase_duration_ms: meter.f64_histogram("nacelle.phase.duration_ms").build(),
            errors: meter.u64_counter("nacelle.errors").build(),
            resource_limit_rejections: meter
                .u64_counter("nacelle.resource_limit.rejections")
                .build(),
        }
    }

    fn register_runtime_state(&self, state: crate::limits::NacelleRuntimeState) {
        let mut states = self
            .runtime_states
            .lock()
            .expect("otel runtime state registry poisoned");
        if states
            .iter()
            .any(|registered| registered.shares_counters_with(&state))
        {
            return;
        }
        states.push(state);
        if states.len() != 1 {
            return;
        }
        drop(states);

        let meter = opentelemetry::global::meter("nacelle");
        let connections = self.runtime_states.clone();
        let requests = self.runtime_states.clone();
        let streaming = self.runtime_states.clone();
        let memory = self.runtime_states.clone();
        let gauges = vec![
            meter
                .u64_observable_gauge("nacelle.connections.active")
                .with_callback(move |observer| {
                    let total = connections
                        .lock()
                        .expect("otel runtime state registry poisoned")
                        .iter()
                        .map(crate::limits::NacelleRuntimeState::active_connections)
                        .sum::<usize>();
                    observer.observe(total as u64, &[])
                })
                .build(),
            meter
                .u64_observable_gauge("nacelle.requests.active")
                .with_callback(move |observer| {
                    let total = requests
                        .lock()
                        .expect("otel runtime state registry poisoned")
                        .iter()
                        .map(crate::limits::NacelleRuntimeState::active_requests)
                        .sum::<usize>();
                    observer.observe(total as u64, &[])
                })
                .build(),
            meter
                .u64_observable_gauge("nacelle.streaming_tasks.active")
                .with_callback(move |observer| {
                    let total = streaming
                        .lock()
                        .expect("otel runtime state registry poisoned")
                        .iter()
                        .map(crate::limits::NacelleRuntimeState::active_streaming_tasks)
                        .sum::<usize>();
                    observer.observe(total as u64, &[])
                })
                .build(),
            meter
                .u64_observable_gauge("nacelle.memory.used_bytes")
                .with_callback(move |observer| {
                    let states = memory.lock().expect("otel runtime state registry poisoned");
                    let mut identities = Vec::with_capacity(states.len());
                    let total = states
                        .iter()
                        .filter_map(|state| {
                            let identity = state.memory_identity();
                            if identities.contains(&identity) {
                                None
                            } else {
                                identities.push(identity);
                                Some(state.memory_used_bytes())
                            }
                        })
                        .sum::<usize>();
                    observer.observe(total as u64, &[])
                })
                .build(),
        ];
        self.runtime_state_gauges
            .lock()
            .expect("otel gauge registry poisoned")
            .extend(gauges);
    }

    #[cfg(test)]
    fn registered_runtime_state_count(&self) -> usize {
        self.runtime_states
            .lock()
            .expect("otel runtime state registry poisoned")
            .len()
    }

    #[cfg(test)]
    fn registered_memory_budget_count(&self) -> usize {
        let states = self
            .runtime_states
            .lock()
            .expect("otel runtime state registry poisoned");
        let mut identities = Vec::new();
        for state in states.iter() {
            let identity = state.memory_identity();
            if !identities.contains(&identity) {
                identities.push(identity);
            }
        }
        identities.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "otel")]
    #[test]
    fn telemetry_aggregates_worker_states_without_double_counting_memory() {
        let states = crate::limits::NacelleRuntimeState::partitioned([
            crate::limits::NacelleLimits::default().with_max_memory_bytes(10),
            crate::limits::NacelleLimits::default().with_max_memory_bytes(10),
        ])
        .expect("partitioned states should build");
        let telemetry = NacelleTelemetry::default();

        telemetry.register_runtime_state(states[0].clone());
        telemetry.register_runtime_state(states[1].clone());
        telemetry.register_runtime_state(states[0].clone());

        assert_eq!(telemetry.metrics.registered_runtime_state_count(), 2);
        assert_eq!(telemetry.metrics.registered_memory_budget_count(), 1);
    }

    #[test]
    fn in_memory_sink_records_rejection_timeout_and_shutdown_events() {
        let sink = Arc::new(NacelleInMemoryTelemetrySink::new());
        let telemetry = NacelleTelemetry::new().with_sink(sink.clone());

        telemetry.connection_rejected(NacelleTransport::new("tcp"), "connections");
        telemetry.request_rejected(NacelleTransport::new("http"), "host");
        telemetry.timeout(NacelleTransport::new("tcp"), "request_body_read");
        telemetry.shutdown_requested();
        telemetry.shutdown_event(
            NacelleTelemetryEventKind::DrainCompleted,
            NacelleTransport::new("tcp"),
        );

        let events = sink.events();
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
    fn request_duration_metrics_are_opt_in() {
        let telemetry = NacelleTelemetry::default();

        assert!(telemetry.config().metrics);
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
    }
}
