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
use crate::config::TcpHeadReadLimits;

pub(super) const HEAD_LIMIT_EXCEEDED: &str = "request head exceeds configured byte limit";
pub(super) const READ_BUDGET_EXCEEDED: &str = "undecided input exceeds configured byte limit";

pub(super) struct HeadDecoder<D, const BOUNDED: bool> {
    decoder: D,
    max_head_bytes: usize,
    max_undecided_bytes: usize,
}

impl<D, const BOUNDED: bool> HeadDecoder<D, BOUNDED>
where
    D: MessageDecoder<Error = NacelleError>,
{
    pub(super) fn new(decoder: D, limits: Option<TcpHeadReadLimits>) -> Self {
        Self {
            decoder,
            max_head_bytes: limits.map_or(0, TcpHeadReadLimits::max_head_bytes),
            max_undecided_bytes: limits.map_or(0, TcpHeadReadLimits::max_undecided_bytes),
        }
    }

    fn decode_inner(
        &mut self,
        input: &mut BytesMut,
        eof: bool,
    ) -> Result<Option<D::Message>, NacelleError> {
        if !BOUNDED {
            return if eof {
                self.decoder.decode_eof(input)
            } else {
                self.decoder.decode(input)
            };
        }
        if input.len() > self.max_undecided_bytes {
            return Err(NacelleError::InvalidFrame(READ_BUDGET_EXCEEDED));
        }

        let truncated = input.len() > self.max_head_bytes;
        let tail = truncated.then(|| input.split_off(self.max_head_bytes));
        let before = input.len();
        // A truncated prefix is not an EOF boundary. Preserve the shared tail
        // even when decoding fails or violates the progress contract.
        let result = if eof && !truncated {
            self.decoder.decode_eof(input)
        } else {
            self.decoder.decode(input)
        };
        let after = input.len();
        if let Some(tail) = tail {
            input.unsplit(tail);
        }
        if matches!(result, Ok(None)) && before == after && before == self.max_head_bytes {
            return Err(NacelleError::InvalidFrame(HEAD_LIMIT_EXCEEDED));
        }
        result
    }
}

impl<D, const BOUNDED: bool> MessageDecoder for HeadDecoder<D, BOUNDED>
where
    D: MessageDecoder<Error = NacelleError>,
{
    type Message = D::Message;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        self.decode_inner(input, false)
    }

    fn decode_eof(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        self.decode_inner(input, true)
    }
}

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
        MessageReadError::ReadBudgetExceeded { .. } => {
            NacelleError::InvalidFrame(READ_BUDGET_EXCEEDED)
        }
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

    struct ProbeDecoder {
        head_len: usize,
        eof_only: bool,
        calls: usize,
        eof_calls: usize,
    }

    impl MessageDecoder for ProbeDecoder {
        type Message = bytes::Bytes;
        type Error = NacelleError;

        fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
            self.calls += 1;
            Ok((!self.eof_only && input.len() >= self.head_len)
                .then(|| input.split_to(self.head_len).freeze()))
        }

        fn decode_eof(
            &mut self,
            input: &mut BytesMut,
        ) -> Result<Option<Self::Message>, Self::Error> {
            self.eof_calls += 1;
            Ok((!input.is_empty()).then(|| input.split().freeze()))
        }
    }

    fn probe(head_len: usize, eof_only: bool) -> HeadDecoder<ProbeDecoder, true> {
        HeadDecoder::new(
            ProbeDecoder {
                head_len,
                eof_only,
                calls: 0,
                eof_calls: 0,
            },
            Some(TcpHeadReadLimits::new(4, 8).expect("limits")),
        )
    }

    #[test]
    fn bounded_decoder_retains_shared_tail_and_decoder_state() {
        let mut decoder = probe(4, false);
        let mut input = BytesMut::from(&b"headnext"[..]);
        let pointer = input.as_ptr();
        let first = decoder.decode(&mut input).expect("decode").expect("head");
        assert_eq!(&first[..], b"head");
        assert_eq!(first.as_ptr(), pointer);
        assert_eq!(&input[..], b"next");
        assert_eq!(input.as_ptr(), pointer.wrapping_add(4));
        let second = decoder.decode(&mut input).expect("decode").expect("next");
        assert_eq!(&second[..], b"next");
        assert_eq!(decoder.decoder.calls, 2);
    }

    #[test]
    fn bounded_decoder_preserves_capacity_without_a_split_tail() {
        let mut decoder = probe(4, false);
        let mut input = BytesMut::with_capacity(64 * 1024);
        let pointer = input.as_ptr();
        assert!(decoder.decode(&mut input).expect("empty decode").is_none());
        assert_eq!(input.capacity(), 64 * 1024);
        assert_eq!(input.as_ptr(), pointer);

        input.extend_from_slice(b"head");
        let head = decoder.decode(&mut input).expect("decode").expect("head");
        assert_eq!(head.as_ptr(), pointer);
        assert_eq!(input.capacity(), 64 * 1024 - 4);
        assert_eq!(input.as_ptr(), pointer.wrapping_add(4));
        assert!(decoder.decode(&mut input).expect("empty tail").is_none());
        assert_eq!(input.capacity(), 64 * 1024 - 4);
    }

    #[test]
    fn bounded_decoder_rejects_incomplete_heads_without_losing_tail() {
        let mut decoder = probe(5, false);
        let mut input = BytesMut::from(&b"longhead"[..]);
        assert!(matches!(
            decoder.decode(&mut input),
            Err(NacelleError::InvalidFrame(HEAD_LIMIT_EXCEEDED))
        ));
        assert_eq!(&input[..], b"longhead");
        input.extend_from_slice(b"x");
        assert!(matches!(
            decoder.decode(&mut input),
            Err(NacelleError::InvalidFrame(READ_BUDGET_EXCEEDED))
        ));
        assert_eq!(&input[..], b"longheadx");
        assert_eq!(decoder.decoder.calls, 1);
    }

    #[test]
    fn bounded_decoder_does_not_fabricate_eof_at_a_truncated_prefix() {
        let mut decoder = probe(4, true);
        let mut input = BytesMut::from(&b"headtail"[..]);
        assert!(matches!(
            decoder.decode_eof(&mut input),
            Err(NacelleError::InvalidFrame(HEAD_LIMIT_EXCEEDED))
        ));
        assert_eq!(decoder.decoder.eof_calls, 0);
        assert_eq!(&input[..], b"headtail");

        let mut input = BytesMut::from(&b"end"[..]);
        let message = decoder
            .decode_eof(&mut input)
            .expect("EOF decode")
            .expect("message");
        assert_eq!(&message[..], b"end");
        assert_eq!(decoder.decoder.eof_calls, 1);
    }

    #[tokio::test]
    async fn bounded_decoder_preserves_progress_contract_errors() {
        struct InvalidDecoder(bool);
        impl MessageDecoder for InvalidDecoder {
            type Message = ();
            type Error = NacelleError;
            fn decode(&mut self, input: &mut BytesMut) -> Result<Option<()>, NacelleError> {
                if self.0 {
                    drop(input.split_to(1));
                    Ok(None)
                } else {
                    Ok(Some(()))
                }
            }
        }
        for consume_on_wait in [false, true] {
            let decoder = HeadDecoder::<_, true>::new(
                InvalidDecoder(consume_on_wait),
                Some(TcpHeadReadLimits::new(4, 8).expect("limits")),
            );
            let mut reader = nacelle_codec::MessageReader::with_buffer(
                tokio::io::empty(),
                decoder,
                BytesMut::from(&b"headtail"[..]),
            );
            let error = reader
                .read_message_bounded(8)
                .await
                .expect_err("invalid decoder");
            if consume_on_wait {
                assert!(matches!(
                    error,
                    MessageReadError::ConsumedOnNeedMore {
                        before: 8,
                        after: 7
                    }
                ));
                assert_eq!(&reader.buffer()[..], b"eadtail");
            } else {
                assert!(matches!(error, MessageReadError::MessageWithoutProgress));
                assert_eq!(&reader.buffer()[..], b"headtail");
            }
        }
    }

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
