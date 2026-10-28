use bytes::BytesMut;
use tokio::io::AsyncWrite;

use crate::config::{NacelleTcpConfig, ResponseWritePolicy};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    FrameBuffer, Protocol, ResponseAbortReason, ResponseCompletionCallback,
    ResponseDeliveryOutcome, ResponseDeliveryPhase, TcpCompletion,
};
use nacelle_core::error::{NacelleError, NacelleResourceLimitReason};
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::io::{flush_with_timeout, write_all_tracked_with_timeout};
#[cfg(test)]
use super::metrics::TcpTelemetryPlan;
use super::metrics::{finish_tcp_phase, record_tcp_error, start_tcp_phase};

struct ResponseCompletionGuard {
    item: Option<ResponseCompletionCallback>,
}

impl ResponseCompletionGuard {
    fn new(item: ResponseCompletionCallback) -> Self {
        Self { item: Some(item) }
    }

    fn deliver(mut self, encoded_wire_bytes: usize, written_wire_bytes: usize) {
        if let Some(item) = self.item.take() {
            item(ResponseDeliveryOutcome::Delivered {
                encoded_wire_bytes,
                written_wire_bytes,
            });
        }
    }

    fn fail(
        mut self,
        phase: ResponseDeliveryPhase,
        encoded_wire_bytes: usize,
        written_wire_bytes: usize,
        error: &NacelleError,
    ) {
        if let Some(item) = self.item.take() {
            item(ResponseDeliveryOutcome::Failed {
                phase,
                encoded_wire_bytes,
                written_wire_bytes,
                error,
            });
        }
    }
}

impl Drop for ResponseCompletionGuard {
    fn drop(&mut self) {
        if let Some(item) = self.item.take() {
            item(ResponseDeliveryOutcome::Aborted {
                reason: ResponseAbortReason::Cancelled,
            });
        }
    }
}

pub(super) struct GuardedResponseCompletion<Response, ResponseContext> {
    guard: Option<ResponseCompletionGuard>,
    completion: TcpCompletion<Response, ResponseContext>,
}

impl<Response, ResponseContext> GuardedResponseCompletion<Response, ResponseContext> {
    pub(super) fn new(mut completion: TcpCompletion<Response, ResponseContext>) -> Self {
        let guard = completion
            .completion
            .take()
            .map(ResponseCompletionGuard::new);
        Self { guard, completion }
    }
}

#[derive(Debug)]
pub(super) struct ResponseDeliveryError {
    pub(super) error: NacelleError,
    pub(super) encoded_bytes: usize,
    pub(super) delivered_bytes: usize,
    kind: ResponseDeliveryErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseDeliveryErrorKind {
    ResponseBody,
    Encode,
    SocketWrite,
    SocketFlush,
}

#[derive(Debug)]
struct FrameDelivery {
    encoded_bytes: usize,
    delivered_bytes: usize,
}

struct PendingGrowth {
    pending: BytesMut,
}

pub(super) struct ResponseDelivery {
    pending: BytesMut,
    base_capacity: usize,
    policy: ResponseWritePolicy,
    interrupted: bool,
}

impl ResponseDelivery {
    pub(super) fn new(config: &NacelleTcpConfig) -> Self {
        Self {
            pending: BytesMut::with_capacity(config.response_buffer_capacity),
            base_capacity: config.response_buffer_capacity,
            policy: config.response_write_policy,
            interrupted: false,
        }
    }

    fn threshold(&self) -> usize {
        match self.policy {
            ResponseWritePolicy::Immediate => 1,
            ResponseWritePolicy::CoalesceBuffered => self.base_capacity.max(1),
            ResponseWritePolicy::FlushAtBytes(bytes) => bytes.max(1),
        }
    }

    fn should_flush(&self) -> bool {
        matches!(self.policy, ResponseWritePolicy::Immediate)
            || self.pending.len() >= self.threshold()
    }

    /// Whether bytes from prior responses remain buffered (coalesced).
    pub(super) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    pub(super) fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    pub(super) fn interrupt(&mut self) {
        self.pending.clear();
        self.interrupted = true;
    }

    pub(super) fn is_interrupted(&self) -> bool {
        self.interrupted
    }

    fn reset(&mut self) {
        if self.pending.capacity() > self.base_capacity {
            drop(std::mem::take(&mut self.pending));
            self.pending = BytesMut::with_capacity(self.base_capacity);
        } else {
            self.pending.clear();
        }
    }

    fn prepare_growth(&self, required_len: usize) -> Result<Option<PendingGrowth>, NacelleError> {
        if required_len <= self.pending.capacity() {
            return Ok(None);
        }
        let previous_capacity = self.pending.capacity();
        let maximum_capacity = isize::MAX as usize;
        if required_len > maximum_capacity {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ResponseFrameBytes,
            ));
        }
        let growth_capacity = previous_capacity
            .saturating_mul(2)
            .min(maximum_capacity)
            .max(required_len);
        Ok(Some(self.allocate_growth(growth_capacity)))
    }

    fn allocate_growth(&self, capacity: usize) -> PendingGrowth {
        let mut pending = BytesMut::with_capacity(capacity);
        pending.extend_from_slice(&self.pending);
        PendingGrowth { pending }
    }

    fn commit_growth(&mut self, growth: PendingGrowth) {
        let previous = std::mem::replace(&mut self.pending, growth.pending);
        drop(previous);
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_frame<W, E, Observer>(
        &mut self,
        writer: &mut W,
        tcp_limits: &NacelleTcpLimits,
        frame_capacity: usize,
        _runtime_state: &NacelleRuntimeState,
        telemetry: &NacelleTelemetry<Observer>,
        metrics_context: Option<&NacelleMetricsContext>,
        phase_duration_metrics: bool,
        encode: E,
    ) -> Result<FrameDelivery, ResponseDeliveryError>
    where
        W: AsyncWrite + Unpin,
        E: FnOnce(&mut FrameBuffer<'_>) -> Result<(), NacelleError>,
        Observer: NacelleTelemetryObserver,
    {
        let frame_start = self.pending.len();
        let required_len = frame_start.checked_add(frame_capacity).ok_or_else(|| {
            ResponseDeliveryError::before_delivery(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::ResponseFrameBytes,
            ))
        })?;
        let mut growth = self
            .prepare_growth(required_len)
            .map_err(ResponseDeliveryError::before_delivery)?;
        let encode_started = start_tcp_phase(phase_duration_metrics);
        let encode_result = {
            let pending = match &mut growth {
                Some(growth) => &mut growth.pending,
                None => &mut self.pending,
            };
            let mut frame = FrameBuffer::append_to(pending, frame_capacity);
            encode(&mut frame)
        };
        finish_tcp_phase(
            telemetry,
            metrics_context,
            "response_encode",
            encode_started,
        );
        if let Err(error) = encode_result {
            if growth.is_none() {
                self.pending.truncate(frame_start);
                if self.pending.is_empty() {
                    self.reset();
                }
            }
            return Err(ResponseDeliveryError::before_delivery(error));
        }
        if let Some(growth) = growth {
            self.commit_growth(growth);
        }
        let frame_len = self.pending.len().saturating_sub(frame_start);

        let delivered_bytes = if self.should_flush() {
            self.flush_with_current_frame(
                writer,
                tcp_limits,
                telemetry,
                metrics_context,
                phase_duration_metrics,
                frame_start,
                frame_len,
            )
            .await?
        } else {
            0
        };
        Ok(FrameDelivery {
            encoded_bytes: frame_len,
            delivered_bytes,
        })
    }

    pub(super) async fn flush<W, Observer>(
        &mut self,
        writer: &mut W,
        tcp_limits: &NacelleTcpLimits,
        telemetry: &NacelleTelemetry<Observer>,
        metrics_context: Option<&NacelleMetricsContext>,
        phase_duration_metrics: bool,
    ) -> Result<usize, ResponseDeliveryError>
    where
        W: AsyncWrite + Unpin,
        Observer: NacelleTelemetryObserver,
    {
        let pending_bytes = self.pending.len();
        let delivered_bytes = if self.pending.is_empty() {
            self.reset();
            0
        } else {
            self.write_pending(
                writer,
                tcp_limits,
                telemetry,
                metrics_context,
                phase_duration_metrics,
            )
            .await
            .map_err(|(error, written)| ResponseDeliveryError {
                error,
                encoded_bytes: pending_bytes,
                delivered_bytes: written,
                kind: ResponseDeliveryErrorKind::SocketWrite,
            })?
        };
        let flush_started = start_tcp_phase(phase_duration_metrics);
        let flush_result = flush_with_timeout(writer, tcp_limits).await;
        finish_tcp_phase(telemetry, metrics_context, "socket_write", flush_started);
        flush_result.map_err(|error| {
            record_tcp_error(telemetry, metrics_context, "socket_write", &error);
            ResponseDeliveryError {
                error,
                encoded_bytes: delivered_bytes,
                delivered_bytes,
                kind: ResponseDeliveryErrorKind::SocketFlush,
            }
        })?;
        Ok(delivered_bytes)
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_with_current_frame<W, Observer>(
        &mut self,
        writer: &mut W,
        tcp_limits: &NacelleTcpLimits,
        telemetry: &NacelleTelemetry<Observer>,
        metrics_context: Option<&NacelleMetricsContext>,
        phase_duration_metrics: bool,
        previous_bytes: usize,
        frame_len: usize,
    ) -> Result<usize, ResponseDeliveryError>
    where
        W: AsyncWrite + Unpin,
        Observer: NacelleTelemetryObserver,
    {
        self.write_pending(
            writer,
            tcp_limits,
            telemetry,
            metrics_context,
            phase_duration_metrics,
        )
        .await
        .map(|_| frame_len)
        .map_err(|(error, written)| ResponseDeliveryError {
            error,
            encoded_bytes: frame_len,
            delivered_bytes: written.saturating_sub(previous_bytes).min(frame_len),
            kind: ResponseDeliveryErrorKind::SocketWrite,
        })
    }

    async fn write_pending<W, Observer>(
        &mut self,
        writer: &mut W,
        tcp_limits: &NacelleTcpLimits,
        telemetry: &NacelleTelemetry<Observer>,
        metrics_context: Option<&NacelleMetricsContext>,
        phase_duration_metrics: bool,
    ) -> Result<usize, (NacelleError, usize)>
    where
        W: AsyncWrite + Unpin,
        Observer: NacelleTelemetryObserver,
    {
        let write_started = start_tcp_phase(phase_duration_metrics);
        let result = write_all_tracked_with_timeout(writer, &self.pending, tcp_limits).await;
        finish_tcp_phase(telemetry, metrics_context, "socket_write", write_started);
        if let Err((error, _)) = &result {
            record_tcp_error(telemetry, metrics_context, "socket_write", error);
        }
        self.reset();
        result
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn deliver_response<P, W, Observer>(
    protocol: &P,
    completion: GuardedResponseCompletion<P::Response, P::ResponseContext>,
    writer: &mut W,
    tcp_limits: &NacelleTcpLimits,
    delivery: &mut ResponseDelivery,
    runtime_state: &NacelleRuntimeState,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase_duration_metrics: bool,
) -> Result<usize, ResponseDeliveryError>
where
    P: Protocol,
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    let GuardedResponseCompletion { guard, completion } = completion;
    // Isolate callback-bearing responses from prior coalesced responses.
    if guard.is_some()
        && delivery.has_pending()
        && let Err(error) = delivery
            .flush(
                writer,
                tcp_limits,
                telemetry,
                metrics_context,
                phase_duration_metrics,
            )
            .await
    {
        if let Some(guard) = guard {
            guard.fail(error.phase_enum(), 0, 0, &error.error);
        }
        return Err(error);
    }
    let response_bytes = match encode_response_body(
        protocol,
        completion,
        writer,
        tcp_limits,
        delivery,
        runtime_state,
        telemetry,
        metrics_context,
        phase_duration_metrics,
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(error) => {
            if let Some(guard) = guard {
                guard.fail(
                    error.phase_enum(),
                    error.encoded_bytes,
                    error.delivered_bytes,
                    &error.error,
                );
            }
            if error.is_encode_failure() {
                record_tcp_error(telemetry, metrics_context, error.phase(), &error.error);
            }
            return Err(error);
        }
    };
    // Delivery callbacks require a write-and-flush boundary under every policy.
    if let Some(guard) = guard {
        match delivery
            .flush(
                writer,
                tcp_limits,
                telemetry,
                metrics_context,
                phase_duration_metrics,
            )
            .await
        {
            Ok(_) => guard.deliver(response_bytes, response_bytes),
            Err(error) => {
                let error = error.with_cumulative_final_flush(response_bytes);
                guard.fail(
                    error.phase_enum(),
                    error.encoded_bytes,
                    error.delivered_bytes,
                    &error.error,
                );
                return Err(error);
            }
        }
    }
    Ok(response_bytes)
}

pub(super) fn delivered_bytes_after_flush_failure(
    response_bytes: usize,
    pending_bytes: usize,
    delivered_bytes: usize,
) -> usize {
    let pending_current = pending_bytes.min(response_bytes);
    let pending_previous = pending_bytes.saturating_sub(pending_current);
    response_bytes
        .saturating_sub(pending_current)
        .saturating_add(
            delivered_bytes
                .saturating_sub(pending_previous)
                .min(pending_current),
        )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn encode_response_body<P, W, Observer>(
    protocol: &P,
    completion: TcpCompletion<P::Response, P::ResponseContext>,
    writer: &mut W,
    tcp_limits: &NacelleTcpLimits,
    delivery: &mut ResponseDelivery,
    runtime_state: &NacelleRuntimeState,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase_duration_metrics: bool,
) -> Result<usize, ResponseDeliveryError>
where
    P: Protocol,
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    let TcpCompletion {
        response,
        mut response_context,
        completion: _,
    } = completion;
    protocol.apply_response(&mut response_context, &response);

    let body = protocol.response_body(response);
    let mut response_body_bytes = 0_usize;
    let mut body = match body.try_into_single_chunk_or_empty() {
        Ok(Some(chunk)) => {
            validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state)
                .map_err(ResponseDeliveryError::before_delivery)?;
            let frame_capacity = response_frame_capacity(protocol, chunk.len())
                .map_err(ResponseDeliveryError::before_delivery)?;
            return delivery
                .stage_frame(
                    writer,
                    tcp_limits,
                    frame_capacity,
                    runtime_state,
                    telemetry,
                    metrics_context,
                    phase_duration_metrics,
                    |dst| {
                        protocol.encode_response_terminal_chunk(&mut response_context, chunk, dst)
                    },
                )
                .await
                .map(|delivery| delivery.encoded_bytes);
        }
        Ok(None) => {
            return delivery
                .stage_frame(
                    writer,
                    tcp_limits,
                    protocol.max_response_frame_overhead(),
                    runtime_state,
                    telemetry,
                    metrics_context,
                    phase_duration_metrics,
                    |dst| protocol.encode_response_end(&mut response_context, dst),
                )
                .await
                .map(|delivery| delivery.encoded_bytes);
        }
        Err(body) => body,
    };

    delivery
        .flush(
            writer,
            tcp_limits,
            telemetry,
            metrics_context,
            phase_duration_metrics,
        )
        .await?;
    let mut response_wire_bytes = 0_usize;
    let mut delivered_wire_bytes = 0_usize;
    loop {
        match delivery
            .flush(
                writer,
                tcp_limits,
                telemetry,
                metrics_context,
                phase_duration_metrics,
            )
            .await
        {
            Ok(written) => delivered_wire_bytes = delivered_wire_bytes.saturating_add(written),
            Err(error) => {
                return Err(error.with_previous(response_wire_bytes, delivered_wire_bytes));
            }
        }
        let Some(chunk) = body.next_chunk().await else {
            break;
        };
        let chunk = chunk.map_err(|error| ResponseDeliveryError {
            error,
            encoded_bytes: response_wire_bytes,
            delivered_bytes: delivered_wire_bytes,
            kind: ResponseDeliveryErrorKind::ResponseBody,
        })?;
        if chunk.is_empty() {
            continue;
        }
        validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state).map_err(
            |error| ResponseDeliveryError {
                error,
                encoded_bytes: response_wire_bytes,
                delivered_bytes: delivered_wire_bytes,
                kind: ResponseDeliveryErrorKind::Encode,
            },
        )?;
        let frame_capacity = response_frame_capacity(protocol, chunk.len()).map_err(|error| {
            ResponseDeliveryError {
                error,
                encoded_bytes: response_wire_bytes,
                delivered_bytes: delivered_wire_bytes,
                kind: ResponseDeliveryErrorKind::Encode,
            }
        })?;
        let frame_delivery = delivery
            .stage_frame(
                writer,
                tcp_limits,
                frame_capacity,
                runtime_state,
                telemetry,
                metrics_context,
                phase_duration_metrics,
                |dst| protocol.encode_response_chunk(&mut response_context, chunk, dst),
            )
            .await
            .map_err(|error| error.with_previous(response_wire_bytes, delivered_wire_bytes))?;
        response_wire_bytes = response_wire_bytes.saturating_add(frame_delivery.encoded_bytes);
        delivered_wire_bytes = delivered_wire_bytes.saturating_add(frame_delivery.delivered_bytes);
    }

    let frame_delivery = delivery
        .stage_frame(
            writer,
            tcp_limits,
            protocol.max_response_frame_overhead(),
            runtime_state,
            telemetry,
            metrics_context,
            phase_duration_metrics,
            |dst| protocol.encode_response_end(&mut response_context, dst),
        )
        .await
        .map_err(|error| error.with_previous(response_wire_bytes, delivered_wire_bytes))?;

    Ok(response_wire_bytes.saturating_add(frame_delivery.encoded_bytes))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_error<P, W, Observer>(
    writer: &mut W,
    protocol: &P,
    context: Option<P::ErrorContext>,
    error: &NacelleError,
    tcp_limits: &NacelleTcpLimits,
    delivery: &mut ResponseDelivery,
    runtime_state: &NacelleRuntimeState,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase_duration_metrics: bool,
) -> Result<usize, ResponseDeliveryError>
where
    P: Protocol,
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    match protocol.error_response(context.as_ref(), error) {
        Some(response) => {
            deliver_response(
                protocol,
                GuardedResponseCompletion::new(response),
                writer,
                tcp_limits,
                delivery,
                runtime_state,
                telemetry,
                metrics_context,
                phase_duration_metrics,
            )
            .await
        }
        None => Ok(0),
    }
}

fn response_frame_capacity<P>(protocol: &P, chunk_len: usize) -> Result<usize, NacelleError>
where
    P: Protocol,
{
    chunk_len
        .checked_add(protocol.max_response_frame_overhead())
        .ok_or(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::ResponseFrameBytes,
        ))
}

impl ResponseDeliveryError {
    fn before_delivery(error: NacelleError) -> Self {
        Self {
            error,
            encoded_bytes: 0,
            delivered_bytes: 0,
            kind: ResponseDeliveryErrorKind::Encode,
        }
    }

    fn with_previous(self, encoded_previous: usize, delivered_previous: usize) -> Self {
        Self {
            error: self.error,
            encoded_bytes: encoded_previous.saturating_add(self.encoded_bytes),
            delivered_bytes: delivered_previous.saturating_add(self.delivered_bytes),
            kind: self.kind,
        }
    }

    pub(super) fn with_cumulative_final_flush(mut self, response_bytes: usize) -> Self {
        self.delivered_bytes = delivered_bytes_after_flush_failure(
            response_bytes,
            self.encoded_bytes,
            self.delivered_bytes,
        );
        self.encoded_bytes = response_bytes;
        self
    }

    pub(super) fn phase(&self) -> &'static str {
        match self.kind {
            ResponseDeliveryErrorKind::ResponseBody | ResponseDeliveryErrorKind::Encode => {
                "response_encode"
            }
            ResponseDeliveryErrorKind::SocketWrite | ResponseDeliveryErrorKind::SocketFlush => {
                "socket_write"
            }
        }
    }

    pub(super) fn phase_enum(&self) -> ResponseDeliveryPhase {
        match self.kind {
            ResponseDeliveryErrorKind::ResponseBody => ResponseDeliveryPhase::ResponseBody,
            ResponseDeliveryErrorKind::Encode => ResponseDeliveryPhase::Encode,
            ResponseDeliveryErrorKind::SocketWrite => ResponseDeliveryPhase::SocketWrite,
            ResponseDeliveryErrorKind::SocketFlush => ResponseDeliveryPhase::SocketFlush,
        }
    }

    pub(super) fn is_encode_failure(&self) -> bool {
        matches!(
            self.kind,
            ResponseDeliveryErrorKind::ResponseBody | ResponseDeliveryErrorKind::Encode
        )
    }
}

fn validate_response_bytes(
    total: &mut usize,
    next_chunk_len: usize,
    runtime_state: &NacelleRuntimeState,
) -> Result<(), NacelleError> {
    let Some(next) = total.checked_add(next_chunk_len) else {
        return Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::ResponseBodyBytes,
        ));
    };
    if next > runtime_state.limits().max_response_body_bytes {
        return Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::ResponseBodyBytes,
        ));
    }
    *total = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use tokio::io::{AsyncWrite, sink};

    use super::*;

    struct FailingWriter;

    struct RecordingWriter(Arc<Mutex<Vec<u8>>>);

    struct PartialThenFail {
        bytes_before_failure: usize,
        written: usize,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.0
                .lock()
                .expect("recording writer poisoned")
                .extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("injected write failure")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for PartialThenFail {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.written >= self.bytes_before_failure {
                return Poll::Ready(Err(std::io::Error::other("injected write failure")));
            }
            let written = buf
                .len()
                .min(self.bytes_before_failure.saturating_sub(self.written));
            self.written = self.written.saturating_add(written);
            Poll::Ready(Ok(written))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn failed_encoder_after_growth_restores_base_capacity() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let mut writer = sink();

        let error = delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                16,
                &runtime_state,
                &telemetry,
                None,
                TcpTelemetryPlan::new(&telemetry).phase_duration,
                |frame| {
                    frame.extend_from_slice(b"encoded data")?;
                    Err(NacelleError::InvalidFrame("injected encoder failure"))
                },
            )
            .await
            .expect_err("encoding should fail");

        assert!(matches!(error.error, NacelleError::InvalidFrame(_)));
        assert_eq!(delivery.pending.capacity(), delivery.base_capacity);
        assert!(delivery.pending.is_empty());
    }

    #[tokio::test]
    async fn failed_encoder_truncates_only_current_coalesced_frame() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut writer = RecordingWriter(bytes.clone());

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                4,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"good"),
            )
            .await
            .expect("first frame should stage");
        let error = delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                4,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| {
                    frame.extend_from_slice(b"bad")?;
                    Err(NacelleError::InvalidFrame("injected encoder failure"))
                },
            )
            .await
            .expect_err("second frame should fail");
        assert!(matches!(
            error.error,
            NacelleError::InvalidFrame("injected encoder failure")
        ));

        delivery
            .flush(
                &mut writer,
                &NacelleTcpLimits::default(),
                &telemetry,
                None,
                phase_duration_metrics,
            )
            .await
            .expect("prior frame should flush");
        assert_eq!(&*bytes.lock().expect("recording writer poisoned"), b"good");
    }

    #[tokio::test]
    async fn coalesced_frames_preserve_payload_order() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut writer = RecordingWriter(bytes.clone());

        for payload in [b"first".as_slice(), b"-second".as_slice()] {
            delivery
                .stage_frame(
                    &mut writer,
                    &NacelleTcpLimits::default(),
                    payload.len(),
                    &runtime_state,
                    &telemetry,
                    None,
                    phase_duration_metrics,
                    |frame| frame.extend_from_slice(payload),
                )
                .await
                .expect("frame should stage");
        }
        delivery
            .flush(
                &mut writer,
                &NacelleTcpLimits::default(),
                &telemetry,
                None,
                phase_duration_metrics,
            )
            .await
            .expect("frames should flush");

        assert_eq!(
            &*bytes.lock().expect("recording writer poisoned"),
            b"first-second"
        );
    }

    #[tokio::test]
    async fn queued_flush_reports_only_current_frame_partial_bytes() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(8),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let limits = NacelleTcpLimits::default();
        let mut writer = PartialThenFail {
            bytes_before_failure: 6,
            written: 0,
        };

        delivery
            .stage_frame(
                &mut writer,
                &limits,
                4,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"prev"),
            )
            .await
            .expect("first frame should stage");
        let error = delivery
            .stage_frame(
                &mut writer,
                &limits,
                4,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"next"),
            )
            .await
            .expect_err("queued write should fail");

        assert_eq!(error.delivered_bytes, 2);
        assert_eq!(error.phase(), "socket_write");
    }

    #[test]
    fn final_flush_failure_counts_previously_delivered_stream_bytes() {
        let error = ResponseDeliveryError {
            error: NacelleError::InvalidFrame("test write failure"),
            encoded_bytes: 3,
            delivered_bytes: 1,
            kind: ResponseDeliveryErrorKind::SocketWrite,
        };
        let error = error.with_cumulative_final_flush(10);

        assert_eq!(error.encoded_bytes, 10);
        assert_eq!(error.delivered_bytes, 8);
    }

    #[test]
    fn final_flush_failure_counts_all_bytes_on_flush_error() {
        let error = ResponseDeliveryError {
            error: NacelleError::InvalidFrame("test flush failure"),
            encoded_bytes: 3,
            delivered_bytes: 3,
            kind: ResponseDeliveryErrorKind::SocketFlush,
        };
        let error = error.with_cumulative_final_flush(10);

        assert_eq!(error.encoded_bytes, 10);
        assert_eq!(error.delivered_bytes, 10);
    }

    #[tokio::test]
    async fn threshold_write_failure_is_classified_after_current_encoding() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::CoalesceBuffered,
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let limits = NacelleTcpLimits::default();
        let encoded = std::cell::Cell::new(false);

        delivery
            .stage_frame(
                &mut FailingWriter,
                &limits,
                4,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"prev"),
            )
            .await
            .expect("first frame should stage");
        let error = delivery
            .stage_frame(
                &mut FailingWriter,
                &limits,
                8,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| {
                    encoded.set(true);
                    frame.extend_from_slice(b"current")
                },
            )
            .await
            .expect_err("threshold write should fail");

        assert!(encoded.get());
        assert_eq!(error.delivered_bytes, 0);
        assert_eq!(error.phase(), "socket_write");
    }

    #[tokio::test]
    async fn non_divisible_threshold_flushes_after_crossing() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(10),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state = NacelleRuntimeState::default();
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let mut writer = RecordingWriter(bytes.clone());

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                6,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"first-"),
            )
            .await
            .expect("first frame should stage");
        assert!(bytes.lock().expect("recording writer poisoned").is_empty());
        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                6,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(b"second"),
            )
            .await
            .expect("crossing frame should flush");

        assert_eq!(
            &*bytes.lock().expect("recording writer poisoned"),
            b"first-second"
        );
    }
}
