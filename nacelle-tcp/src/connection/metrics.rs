use std::time::{Duration, Instant};

use crate::protocol::Protocol;
use nacelle_core::error::NacelleError;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver, NacelleTransport,
};

#[derive(Debug, Clone, Copy)]
pub(super) struct TcpTelemetryPlan {
    pub(super) request_metrics: bool,
    pub(super) request_duration: bool,
    pub(super) phase_duration: bool,
    pub(super) observer: bool,
    pub(super) request_events: bool,
}

impl TcpTelemetryPlan {
    pub(super) fn new<Observer>(telemetry: &NacelleTelemetry<Observer>) -> Self
    where
        Observer: NacelleTelemetryObserver,
    {
        Self {
            request_metrics: telemetry.request_metrics_enabled(),
            request_duration: telemetry.request_duration_metrics_enabled(),
            phase_duration: telemetry.phase_duration_metrics_enabled(),
            observer: telemetry.observer_enabled(),
            request_events: telemetry.request_events_enabled(),
        }
    }
}

pub(super) fn tcp_metrics_context<P>(
    protocol: &P,
    connection: &NacelleConnectionMeta,
) -> NacelleMetricsContext
where
    P: Protocol,
{
    NacelleMetricsContext::new(
        connection.transport,
        connection.listener.clone(),
        protocol.name(),
        connection.tls_label(),
    )
}

pub(super) struct TcpPhaseTimer {
    #[cfg(feature = "phase-timing")]
    started: Option<Instant>,
}

pub(super) fn start_tcp_phase(_enabled: bool) -> TcpPhaseTimer {
    TcpPhaseTimer {
        #[cfg(feature = "phase-timing")]
        started: _enabled.then(Instant::now),
    }
}

pub(super) fn finish_tcp_phase<Observer>(
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase: &'static str,
    timer: TcpPhaseTimer,
) where
    Observer: NacelleTelemetryObserver,
{
    #[cfg(feature = "phase-timing")]
    if let (Some(started), Some(metrics_context)) = (timer.started, metrics_context) {
        telemetry.phase_duration(metrics_context, phase, started.elapsed());
    }
    #[cfg(not(feature = "phase-timing"))]
    let _ = (telemetry, metrics_context, phase, timer);
}

pub(super) fn record_tcp_error<Observer>(
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase: &'static str,
    error: &NacelleError,
) where
    Observer: NacelleTelemetryObserver,
{
    if let Some(metrics_context) = metrics_context {
        telemetry.operation_error(metrics_context, phase, error);
    }
}

pub(super) fn record_core_request_completed<Observer>(
    telemetry: &NacelleTelemetry<Observer>,
    enabled: bool,
    emit_metrics: bool,
    transport: NacelleTransport,
    request_bytes: usize,
    response_bytes: usize,
    started: Option<Instant>,
) where
    Observer: NacelleTelemetryObserver,
{
    if enabled {
        if emit_metrics {
            telemetry.request_completed(
                transport,
                request_bytes,
                response_bytes,
                elapsed_since(started),
            );
        } else {
            telemetry.request_completed_without_metrics(
                transport,
                request_bytes,
                response_bytes,
                elapsed_since(started),
            );
        }
    }
}

pub(super) fn record_core_request_failed<Observer>(
    telemetry: &NacelleTelemetry<Observer>,
    enabled: bool,
    emit_metrics: bool,
    transport: NacelleTransport,
    started: Option<Instant>,
    error: &NacelleError,
) where
    Observer: NacelleTelemetryObserver,
{
    if enabled {
        if emit_metrics {
            telemetry.request_failed(transport, elapsed_since(started), error);
        } else {
            telemetry.request_failed_without_metrics(transport, elapsed_since(started), error);
        }
    }
}

pub(super) fn tcp_close_reason(result: &Result<(), NacelleError>) -> &'static str {
    match result {
        Ok(()) => "eof",
        Err(NacelleError::Timeout(_)) => "timeout",
        Err(NacelleError::UnexpectedEof) => "unexpected_eof",
        Err(NacelleError::ConnectionClosed) => "connection_closed",
        Err(NacelleError::ResourceLimit(_)) => "resource_limit",
        Err(NacelleError::Io(_)) => "io",
        Err(NacelleError::Protocol(_)) => "protocol",
        Err(NacelleError::Handler(_)) => "handler",
        Err(NacelleError::Join(_)) => "join",
        Err(NacelleError::InvalidFrame(_)) | Err(NacelleError::FrameTooLarge { .. }) => {
            "invalid_frame"
        }
        Err(NacelleError::MissingProtocol) => "missing_protocol",
    }
}

fn elapsed_since(started: Option<Instant>) -> Duration {
    started.map_or(Duration::ZERO, |started| started.elapsed())
}

pub(super) struct TcpRequestMetricsGuard<'a, Observer: NacelleTelemetryObserver> {
    telemetry: &'a NacelleTelemetry<Observer>,
    context: Option<&'a NacelleMetricsContext>,
    request_bytes: usize,
    started: Option<Instant>,
    completed: bool,
}

impl<'a, Observer> TcpRequestMetricsGuard<'a, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    pub(super) fn new(
        telemetry: &'a NacelleTelemetry<Observer>,
        context: Option<&'a NacelleMetricsContext>,
        request_bytes: usize,
        started: Option<Instant>,
    ) -> Self {
        if let Some(context) = context {
            telemetry.request_started_with_context(context);
        }
        Self {
            telemetry,
            context,
            request_bytes,
            started,
            completed: false,
        }
    }

    pub(super) fn complete(&mut self, status: &'static str, response_bytes: usize) {
        if let Some(context) = self.context {
            self.telemetry.request_finished_with_context(
                context,
                status,
                self.request_bytes,
                response_bytes,
                elapsed_since(self.started),
            );
        }
        self.completed = true;
    }
}

impl<Observer> Drop for TcpRequestMetricsGuard<'_, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    fn drop(&mut self) {
        if !self.completed
            && let Some(context) = self.context
        {
            self.telemetry.request_finished_with_context(
                context,
                "error",
                self.request_bytes,
                0,
                elapsed_since(self.started),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nacelle_core::telemetry::{NacelleInMemoryObserver, NoopObserver};

    use super::*;

    #[test]
    fn default_plan_enables_facade_metrics_without_optional_timers() {
        let telemetry = NacelleTelemetry::default();
        let plan = TcpTelemetryPlan::new(&telemetry);

        assert!(plan.request_metrics);
        assert!(!plan.request_duration);
        assert!(!plan.phase_duration);
        assert!(!plan.observer);
        assert!(plan.request_events);
        const {
            assert!(!<Arc<NoopObserver> as NacelleTelemetryObserver>::ENABLED);
        }
    }

    #[test]
    fn observer_plan_preserves_events_without_metrics_context() {
        let telemetry = NacelleTelemetry::default().with_observer(NacelleInMemoryObserver::new());
        let plan = TcpTelemetryPlan::new(&telemetry);

        assert!(plan.request_metrics);
        assert!(plan.observer);
        assert!(plan.request_events);
    }

    #[test]
    fn phase_plan_requires_compile_time_and_runtime_opt_in() {
        let configured = NacelleTelemetry::default().with_phase_duration_metrics(true);
        assert_eq!(
            TcpTelemetryPlan::new(&configured).phase_duration,
            cfg!(feature = "phase-timing")
        );

        let runtime_disabled = configured.with_phase_duration_metrics(false);
        assert!(!TcpTelemetryPlan::new(&runtime_disabled).phase_duration);
    }

    #[test]
    fn phase_timer_storage_is_compiled_out_when_disabled() {
        #[cfg(feature = "phase-timing")]
        assert_ne!(std::mem::size_of::<TcpPhaseTimer>(), 0);
        #[cfg(not(feature = "phase-timing"))]
        assert_eq!(std::mem::size_of::<TcpPhaseTimer>(), 0);
    }
}
