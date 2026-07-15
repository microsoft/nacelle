use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReadError};
#[cfg(feature = "phase-timing")]
use std::pin::Pin;
#[cfg(feature = "phase-timing")]
use std::task::{Context, Poll};
#[cfg(feature = "phase-timing")]
use tokio::io::{AsyncRead, ReadBuf};

use crate::config::NacelleTcpConfig;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::{NacelleMemoryAllocation, NacelleRuntimeState};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::metrics::{finish_tcp_phase, start_tcp_phase};

#[cfg(feature = "phase-timing")]
pub(super) struct InstrumentedReader<'a, R, Observer: NacelleTelemetryObserver> {
    reader: R,
    telemetry: &'a NacelleTelemetry<Observer>,
    metrics_context: Option<&'a NacelleMetricsContext>,
    phase_duration_metrics: bool,
    read_timer: Option<super::metrics::TcpPhaseTimer>,
}

#[cfg(feature = "phase-timing")]
impl<'a, R, Observer> InstrumentedReader<'a, R, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    pub(super) const fn new(
        reader: R,
        telemetry: &'a NacelleTelemetry<Observer>,
        metrics_context: Option<&'a NacelleMetricsContext>,
        phase_duration_metrics: bool,
    ) -> Self {
        Self {
            reader,
            telemetry,
            metrics_context,
            phase_duration_metrics,
            read_timer: None,
        }
    }
}

#[cfg(feature = "phase-timing")]
impl<R, Observer> AsyncRead for InstrumentedReader<'_, R, Observer>
where
    R: AsyncRead + Unpin,
    Observer: NacelleTelemetryObserver,
{
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.phase_duration_metrics {
            return Pin::new(&mut this.reader).poll_read(context, buffer);
        }
        if this.read_timer.is_none() {
            this.read_timer = Some(start_tcp_phase(true));
        }
        let result = Pin::new(&mut this.reader).poll_read(context, buffer);
        if result.is_ready() {
            finish_tcp_phase(
                this.telemetry,
                this.metrics_context,
                "socket_read",
                this.read_timer
                    .take()
                    .expect("ready socket read should have a phase timer"),
            );
        }
        result
    }
}

pub(super) fn allocate_connection_buffers(
    config: &NacelleTcpConfig,
    runtime_state: &NacelleRuntimeState,
) -> Result<NacelleMemoryAllocation, NacelleError> {
    let bytes = config
        .read_buffer_capacity
        .saturating_add(config.response_buffer_capacity);
    runtime_state.allocate_memory(bytes)
}

pub(super) struct InstrumentedDecoder<'a, D, Observer: NacelleTelemetryObserver> {
    decoder: D,
    telemetry: &'a NacelleTelemetry<Observer>,
    metrics_context: Option<&'a NacelleMetricsContext>,
    phase_duration_metrics: bool,
}

impl<'a, D, Observer> InstrumentedDecoder<'a, D, Observer>
where
    Observer: NacelleTelemetryObserver,
{
    pub(super) const fn new(
        decoder: D,
        telemetry: &'a NacelleTelemetry<Observer>,
        metrics_context: Option<&'a NacelleMetricsContext>,
        phase_duration_metrics: bool,
    ) -> Self {
        Self {
            decoder,
            telemetry,
            metrics_context,
            phase_duration_metrics,
        }
    }
}

impl<D, Observer> MessageDecoder for InstrumentedDecoder<'_, D, Observer>
where
    D: MessageDecoder<Error = NacelleError>,
    Observer: NacelleTelemetryObserver,
{
    type Message = D::Message;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let decode_started = start_tcp_phase(self.phase_duration_metrics);
        let result = self.decoder.decode(input);
        finish_tcp_phase(
            self.telemetry,
            self.metrics_context,
            "decode",
            decode_started,
        );
        if let (Err(error), Some(metrics_context)) = (&result, self.metrics_context) {
            self.telemetry
                .operation_error(metrics_context, "decode", error);
        }
        result
    }

    fn decode_eof(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let decode_started = start_tcp_phase(self.phase_duration_metrics);
        let result = self.decoder.decode_eof(input);
        finish_tcp_phase(
            self.telemetry,
            self.metrics_context,
            "decode",
            decode_started,
        );
        if let (Err(error), Some(metrics_context)) = (&result, self.metrics_context) {
            self.telemetry
                .operation_error(metrics_context, "decode", error);
        }
        result
    }
}

pub(super) fn map_message_read_error(error: MessageReadError<NacelleError>) -> NacelleError {
    match error {
        MessageReadError::Io(error) => NacelleError::Io(error),
        MessageReadError::Decoder(error) => error,
        MessageReadError::UnexpectedEof { .. } => NacelleError::UnexpectedEof,
        MessageReadError::MessageWithoutProgress => {
            NacelleError::InvalidFrame("decoder returned a request without consuming input")
        }
        MessageReadError::ConsumedOnNeedMore { .. } => {
            NacelleError::InvalidFrame("decoder consumed input before requesting more data")
        }
    }
}
