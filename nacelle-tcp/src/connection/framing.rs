use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReadError};

use crate::config::NacelleTcpConfig;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::{NacelleMemoryAllocation, NacelleRuntimeState};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry};

use super::metrics::{finish_tcp_phase, start_tcp_phase};

pub(super) fn allocate_connection_buffers(
    config: &NacelleTcpConfig,
    runtime_state: &NacelleRuntimeState,
) -> Result<NacelleMemoryAllocation, NacelleError> {
    let bytes = config
        .read_buffer_capacity
        .saturating_add(config.response_buffer_capacity);
    runtime_state.allocate_memory(bytes)
}

pub(super) struct InstrumentedDecoder<'a, D> {
    decoder: D,
    telemetry: &'a NacelleTelemetry,
    metrics_context: &'a NacelleMetricsContext,
}

impl<'a, D> InstrumentedDecoder<'a, D> {
    pub(super) const fn new(
        decoder: D,
        telemetry: &'a NacelleTelemetry,
        metrics_context: &'a NacelleMetricsContext,
    ) -> Self {
        Self {
            decoder,
            telemetry,
            metrics_context,
        }
    }
}

impl<D> MessageDecoder for InstrumentedDecoder<'_, D>
where
    D: MessageDecoder<Error = NacelleError>,
{
    type Message = D::Message;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let decode_started = start_tcp_phase(self.telemetry);
        let result = self.decoder.decode(input);
        finish_tcp_phase(
            self.telemetry,
            Some(self.metrics_context),
            "decode",
            decode_started,
        );
        if let Err(error) = &result {
            self.telemetry
                .operation_error(self.metrics_context, "decode", error);
        }
        result
    }

    fn decode_eof(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        let decode_started = start_tcp_phase(self.telemetry);
        let result = self.decoder.decode_eof(input);
        finish_tcp_phase(
            self.telemetry,
            Some(self.metrics_context),
            "decode",
            decode_started,
        );
        if let Err(error) = &result {
            self.telemetry
                .operation_error(self.metrics_context, "decode", error);
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
