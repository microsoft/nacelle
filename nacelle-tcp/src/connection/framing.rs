use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReadError};
#[cfg(feature = "phase-timing")]
use std::pin::Pin;
#[cfg(feature = "phase-timing")]
use std::task::{Context, Poll};
#[cfg(feature = "phase-timing")]
use tokio::io::{AsyncRead, ReadBuf};

use nacelle_core::error::NacelleError;
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

pub(super) struct MessageReadFailure {
    error: NacelleError,
    can_respond: bool,
}

impl MessageReadFailure {
    pub(super) fn transport(error: NacelleError) -> Self {
        let can_respond = matches!(error, NacelleError::Timeout(_));
        Self { error, can_respond }
    }

    pub(super) fn from_message_read(error: MessageReadError<NacelleError>) -> Self {
        let can_respond = !matches!(error, MessageReadError::Io(_));
        Self {
            error: map_message_read_error(error),
            can_respond,
        }
    }

    pub(super) const fn should_encode(&self) -> bool {
        self.can_respond
    }

    pub(super) fn into_error(self) -> NacelleError {
        self.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nacelle_core::error::NacelleTimeoutReason;

    #[test]
    fn timeout_can_respond_but_socket_io_failure_cannot() {
        let timeout =
            MessageReadFailure::transport(NacelleError::Timeout(NacelleTimeoutReason::Idle));
        assert!(timeout.should_encode());
        let io = MessageReadFailure::from_message_read(MessageReadError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "peer reset",
        )));
        assert!(!io.should_encode());
    }
}
