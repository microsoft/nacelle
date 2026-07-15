use bytes::BytesMut;
use tokio::io::AsyncWrite;

use crate::config::{NacelleTcpConfig, ResponseWritePolicy};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{FrameBuffer, Protocol, TcpCompletion};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::{NacelleMemoryAllocation, NacelleRuntimeState};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::io::{flush_with_timeout, write_all_tracked_with_timeout};
#[cfg(test)]
use super::metrics::TcpTelemetryPlan;
use super::metrics::{finish_tcp_phase, record_tcp_error, start_tcp_phase};

#[derive(Debug)]
pub(super) struct ResponseDeliveryError {
    pub(super) error: NacelleError,
    pub(super) delivered_bytes: usize,
    kind: ResponseDeliveryErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseDeliveryErrorKind {
    Encode,
    Write,
}

#[derive(Debug)]
struct FrameDelivery {
    encoded_bytes: usize,
    delivered_bytes: usize,
}

struct PendingGrowth {
    pending: BytesMut,
    allocations: BufferAllocations,
}

struct BufferAllocations {
    requested: NacelleMemoryAllocation,
    excess: Option<NacelleMemoryAllocation>,
}

impl BufferAllocations {
    fn shrink_by(&mut self, mut bytes: usize) {
        let requested_release = self.requested.bytes().min(bytes);
        self.requested
            .shrink_to(self.requested.bytes().saturating_sub(requested_release));
        bytes = bytes.saturating_sub(requested_release);
        if let Some(excess) = &mut self.excess {
            excess.shrink_to(excess.bytes().saturating_sub(bytes));
        }
    }

    fn is_empty(&self) -> bool {
        self.requested.bytes() == 0
            && self
                .excess
                .as_ref()
                .is_none_or(|allocation| allocation.bytes() == 0)
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        self.requested.bytes().saturating_add(
            self.excess
                .as_ref()
                .map_or(0, NacelleMemoryAllocation::bytes),
        )
    }
}

pub(super) struct ResponseDelivery {
    pending: BytesMut,
    base_capacity: usize,
    policy: ResponseWritePolicy,
    overflow_allocations: Option<BufferAllocations>,
}

impl ResponseDelivery {
    pub(super) fn new(config: &NacelleTcpConfig) -> Self {
        Self {
            pending: BytesMut::with_capacity(config.response_buffer_capacity),
            base_capacity: config.response_buffer_capacity,
            policy: config.response_write_policy,
            overflow_allocations: None,
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

    fn reset(&mut self) {
        self.pending.clear();
        if self.pending.capacity() > self.base_capacity {
            self.pending = BytesMut::new();
            self.overflow_allocations = None;
            self.pending = BytesMut::with_capacity(self.base_capacity);
        } else {
            self.overflow_allocations = None;
        }
    }

    fn prepare_growth(
        &self,
        required_len: usize,
        runtime_state: &NacelleRuntimeState,
    ) -> Result<Option<PendingGrowth>, NacelleError> {
        if required_len <= self.pending.capacity() {
            return Ok(None);
        }
        let previous_capacity = self.pending.capacity();
        let maximum_capacity = isize::MAX as usize;
        if required_len > maximum_capacity {
            return Err(NacelleError::ResourceLimit("memory_bytes"));
        }
        let growth_capacity = previous_capacity
            .saturating_mul(2)
            .min(maximum_capacity)
            .max(required_len);
        match self.allocate_growth(growth_capacity, runtime_state) {
            Ok(growth) => Ok(Some(growth)),
            Err(NacelleError::ResourceLimit("memory_bytes")) if growth_capacity != required_len => {
                self.allocate_growth(required_len, runtime_state).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    fn allocate_growth(
        &self,
        capacity: usize,
        runtime_state: &NacelleRuntimeState,
    ) -> Result<PendingGrowth, NacelleError> {
        let allocation = runtime_state.allocate_memory(capacity)?;
        let mut pending = BytesMut::with_capacity(capacity);
        let excess_bytes = pending.capacity().saturating_sub(capacity);
        let excess = if excess_bytes == 0 {
            None
        } else {
            Some(runtime_state.allocate_memory(excess_bytes)?)
        };
        pending.extend_from_slice(&self.pending);
        Ok(PendingGrowth {
            pending,
            allocations: BufferAllocations {
                requested: allocation,
                excess,
            },
        })
    }

    fn commit_growth(&mut self, mut growth: PendingGrowth) {
        let previous = std::mem::replace(&mut self.pending, growth.pending);
        drop(previous);
        self.overflow_allocations = None;
        growth.allocations.shrink_by(self.base_capacity);
        if !growth.allocations.is_empty() {
            self.overflow_allocations = Some(growth.allocations);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_frame<W, E, Observer>(
        &mut self,
        writer: &mut W,
        tcp_limits: &NacelleTcpLimits,
        frame_capacity: usize,
        runtime_state: &NacelleRuntimeState,
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
                "response_frame_bytes",
            ))
        })?;
        let mut growth = self
            .prepare_growth(required_len, runtime_state)
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
                delivered_bytes: written,
                kind: ResponseDeliveryErrorKind::Write,
            })?
        };
        let flush_started = start_tcp_phase(phase_duration_metrics);
        let flush_result = flush_with_timeout(writer, tcp_limits).await;
        finish_tcp_phase(telemetry, metrics_context, "socket_write", flush_started);
        flush_result.map_err(|error| {
            record_tcp_error(telemetry, metrics_context, "socket_write", &error);
            ResponseDeliveryError {
                error,
                delivered_bytes,
                kind: ResponseDeliveryErrorKind::Write,
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
            delivered_bytes: written.saturating_sub(previous_bytes).min(frame_len),
            kind: ResponseDeliveryErrorKind::Write,
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
        .await
        .map_err(|error| ResponseDeliveryError {
            error: error.error,
            delivered_bytes: 0,
            kind: ResponseDeliveryErrorKind::Write,
        })?;
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
                return Err(ResponseDeliveryError {
                    error: error.error,
                    delivered_bytes: delivered_wire_bytes.saturating_add(error.delivered_bytes),
                    kind: ResponseDeliveryErrorKind::Write,
                });
            }
        }
        let Some(chunk) = body.next_chunk().await else {
            break;
        };
        let chunk = chunk.map_err(|error| ResponseDeliveryError {
            error,
            delivered_bytes: delivered_wire_bytes,
            kind: ResponseDeliveryErrorKind::Encode,
        })?;
        if chunk.is_empty() {
            continue;
        }
        validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state).map_err(
            |error| ResponseDeliveryError {
                error,
                delivered_bytes: delivered_wire_bytes,
                kind: ResponseDeliveryErrorKind::Encode,
            },
        )?;
        let frame_capacity = response_frame_capacity(protocol, chunk.len()).map_err(|error| {
            ResponseDeliveryError {
                error,
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
            .map_err(|error| error.with_previous(delivered_wire_bytes))?;
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
        .map_err(|error| error.with_previous(delivered_wire_bytes))?;

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
    delivery
        .stage_frame(
            writer,
            tcp_limits,
            delivery.base_capacity.max(128),
            runtime_state,
            telemetry,
            metrics_context,
            phase_duration_metrics,
            move |dst| protocol.encode_error(context.as_ref(), error, dst),
        )
        .await
        .map(|delivery| delivery.encoded_bytes)
}

fn response_frame_capacity<P>(protocol: &P, chunk_len: usize) -> Result<usize, NacelleError>
where
    P: Protocol,
{
    chunk_len
        .checked_add(protocol.max_response_frame_overhead())
        .ok_or(NacelleError::ResourceLimit("response_frame_bytes"))
}

impl ResponseDeliveryError {
    fn before_delivery(error: NacelleError) -> Self {
        Self {
            error,
            delivered_bytes: 0,
            kind: ResponseDeliveryErrorKind::Encode,
        }
    }

    fn with_previous(self, previous: usize) -> Self {
        Self {
            error: self.error,
            delivered_bytes: previous.saturating_add(self.delivered_bytes),
            kind: self.kind,
        }
    }

    pub(super) fn phase(&self) -> &'static str {
        match self.kind {
            ResponseDeliveryErrorKind::Encode => "response_encode",
            ResponseDeliveryErrorKind::Write => "socket_write",
        }
    }

    pub(super) fn is_encode_failure(&self) -> bool {
        self.kind == ResponseDeliveryErrorKind::Encode
    }
}

fn validate_response_bytes(
    total: &mut usize,
    next_chunk_len: usize,
    runtime_state: &NacelleRuntimeState,
) -> Result<(), NacelleError> {
    let Some(next) = total.checked_add(next_chunk_len) else {
        return Err(NacelleError::ResourceLimit("response_body_bytes"));
    };
    if next > runtime_state.limits().max_response_body_bytes {
        return Err(NacelleError::ResourceLimit("response_body_bytes"));
    }
    *total = next;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};

    use nacelle_core::limits::NacelleLimits;
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
    async fn oversized_staging_is_rejected_before_encoding() {
        let mut writer = sink();
        let limits = NacelleTcpLimits::default();
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(16));
        let _baseline_allocation = runtime_state
            .allocate_memory(8)
            .expect("baseline response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let encoded = std::cell::Cell::new(false);

        let result = delivery
            .stage_frame(
                &mut writer,
                &limits,
                32,
                &runtime_state,
                &telemetry,
                None,
                TcpTelemetryPlan::new(&telemetry).phase_duration,
                |frame| {
                    encoded.set(true);
                    frame.extend_from_slice(b"frame")
                },
            )
            .await;

        assert!(matches!(
            result,
            Err(ResponseDeliveryError {
                error: NacelleError::ResourceLimit("memory_bytes"),
                delivered_bytes: 0,
                ..
            })
        ));
        assert!(!encoded.get());
        assert_eq!(runtime_state.memory_used_bytes(), 8);
        assert_eq!(delivery.pending.capacity(), delivery.base_capacity);
        assert!(delivery.overflow_allocations.is_none());
    }

    #[tokio::test]
    async fn overflow_accounting_matches_actual_buffer_capacity() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(128));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let mut writer = sink();

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                17,
                &runtime_state,
                &telemetry,
                None,
                TcpTelemetryPlan::new(&telemetry).phase_duration,
                |frame| frame.extend_from_slice(b"123456789"),
            )
            .await
            .expect("frame should stage");

        assert_eq!(
            runtime_state.memory_used_bytes() - delivery.base_capacity,
            delivery.pending.capacity() - delivery.base_capacity
        );
    }

    #[tokio::test]
    async fn growth_authorizes_the_complete_replacement_allocation() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(16));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let mut writer = sink();

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                8,
                &runtime_state,
                &telemetry,
                None,
                TcpTelemetryPlan::new(&telemetry).phase_duration,
                |frame| frame.extend_from_slice(b"existing"),
            )
            .await
            .expect("base-capacity frame should stage");
        let encoded = std::cell::Cell::new(false);
        let error = delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                1,
                &runtime_state,
                &telemetry,
                None,
                TcpTelemetryPlan::new(&telemetry).phase_duration,
                |frame| {
                    encoded.set(true);
                    frame.extend_from_slice(b"x")
                },
            )
            .await
            .expect_err("replacement should exceed the transient memory budget");

        assert!(matches!(
            error.error,
            NacelleError::ResourceLimit("memory_bytes")
        ));
        assert!(!encoded.get());
        assert_eq!(&delivery.pending[..], b"existing");
        assert_eq!(runtime_state.memory_used_bytes(), 8);
    }

    #[tokio::test]
    async fn repeated_growth_keeps_fixed_exact_overflow_accounting() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(128),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(256));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let mut writer = sink();

        for payload in [b"123456789".as_slice(), b"abcdefghi".as_slice()] {
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

        let overflow = delivery
            .overflow_allocations
            .as_ref()
            .expect("grown buffer should retain overflow accounting");
        assert_eq!(
            overflow.bytes(),
            delivery.pending.capacity() - delivery.base_capacity
        );
        assert_eq!(
            runtime_state.memory_used_bytes(),
            delivery.pending.capacity()
        );
    }

    #[tokio::test]
    async fn failed_encoder_after_growth_restores_base_capacity() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(64));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
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
        assert_eq!(runtime_state.memory_used_bytes(), 8);
        assert_eq!(delivery.pending.capacity(), delivery.base_capacity);
        assert!(delivery.pending.is_empty());
    }

    #[tokio::test]
    async fn coalesced_overflow_remains_accounted_until_flush() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(64));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let mut writer = sink();

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                16,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(&[0_u8; 16]),
            )
            .await
            .expect("frame should stage");
        assert_eq!(runtime_state.memory_used_bytes(), 16);

        delivery
            .flush(
                &mut writer,
                &NacelleTcpLimits::default(),
                &telemetry,
                None,
                phase_duration_metrics,
            )
            .await
            .expect("pending bytes should flush");
        assert_eq!(runtime_state.memory_used_bytes(), 8);
    }

    #[tokio::test]
    async fn failed_coalesced_write_releases_overflow_accounting() {
        let config = NacelleTcpConfig {
            response_buffer_capacity: 8,
            response_write_policy: ResponseWritePolicy::FlushAtBytes(64),
            ..NacelleTcpConfig::default()
        };
        let mut delivery = ResponseDelivery::new(&config);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(64));
        let _baseline = runtime_state
            .allocate_memory(8)
            .expect("base response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let phase_duration_metrics = TcpTelemetryPlan::new(&telemetry).phase_duration;
        let mut writer = sink();

        delivery
            .stage_frame(
                &mut writer,
                &NacelleTcpLimits::default(),
                16,
                &runtime_state,
                &telemetry,
                None,
                phase_duration_metrics,
                |frame| frame.extend_from_slice(&[0_u8; 16]),
            )
            .await
            .expect("frame should stage");
        assert_eq!(runtime_state.memory_used_bytes(), 16);

        let error = delivery
            .flush(
                &mut FailingWriter,
                &NacelleTcpLimits::default(),
                &telemetry,
                None,
                phase_duration_metrics,
            )
            .await
            .expect_err("write should fail");
        assert!(matches!(error.error, NacelleError::Io(_)));
        assert_eq!(runtime_state.memory_used_bytes(), 8);
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
