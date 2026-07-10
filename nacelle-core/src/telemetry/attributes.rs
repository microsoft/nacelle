//! OpenTelemetry attribute builders for Nacelle metrics. Compiled only with the
//! `otel` feature. These helpers assemble low-cardinality `KeyValue` attribute
//! sets shared across connection, request, phase, and error metrics.

use std::sync::Arc;

use super::{NacelleMetricsContext, NacelleTelemetryEventKind, NacelleTransport};

pub(super) fn connection_attributes(
    transport: NacelleTransport,
    listener: &Arc<str>,
    tls: &'static str,
) -> Arc<[opentelemetry::KeyValue]> {
    Arc::from(
        vec![
            opentelemetry::KeyValue::new("listener", listener.as_ref().to_owned()),
            opentelemetry::KeyValue::new("transport", transport.as_str()),
            opentelemetry::KeyValue::new("tls", tls),
        ]
        .into_boxed_slice(),
    )
}

pub(super) fn request_attributes(
    connection_attributes: &[opentelemetry::KeyValue],
    protocol: &'static str,
) -> Arc<[opentelemetry::KeyValue]> {
    let mut attributes = connection_attributes.to_vec();
    attributes.push(opentelemetry::KeyValue::new("protocol", protocol));
    Arc::from(attributes.into_boxed_slice())
}

pub(super) fn attributes_with_key_value(
    attributes: &[opentelemetry::KeyValue],
    key_value: opentelemetry::KeyValue,
) -> Arc<[opentelemetry::KeyValue]> {
    let mut attributes = attributes.to_vec();
    attributes.push(key_value);
    Arc::from(attributes.into_boxed_slice())
}

pub(super) fn phase_attributes(
    context: &NacelleMetricsContext,
    phase: &'static str,
) -> Vec<opentelemetry::KeyValue> {
    let mut attributes =
        request_attributes(context.connection_attributes(), context.protocol).to_vec();
    attributes.push(opentelemetry::KeyValue::new("phase", phase));
    attributes
}

pub(super) fn error_attributes(
    context: &NacelleMetricsContext,
    phase: &'static str,
    error: &crate::error::NacelleError,
) -> Vec<opentelemetry::KeyValue> {
    let mut attributes = phase_attributes(context, phase);
    attributes.push(opentelemetry::KeyValue::new(
        "error_kind",
        error_kind(error),
    ));
    attributes
}

pub(super) fn error_kind(error: &crate::error::NacelleError) -> &'static str {
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

pub(super) fn shutdown_stage(kind: NacelleTelemetryEventKind) -> &'static str {
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
