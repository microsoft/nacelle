use bytes::BytesMut;
use tokio::io::AsyncWrite;

use crate::limits::NacelleTcpLimits;
use crate::protocol::{FrameBuffer, Protocol, TcpCompletion};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::io::write_all_tracked_with_timeout;
use super::metrics::{TcpTelemetryPlan, finish_tcp_phase, record_tcp_error, start_tcp_phase};

pub(super) struct ResponseDeliveryError {
    pub(super) error: NacelleError,
    pub(super) delivered_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn encode_response_body<P, W, Observer>(
    protocol: &P,
    completion: TcpCompletion<P::Response, P::ResponseContext>,
    writer: &mut W,
    tcp_limits: &NacelleTcpLimits,
    write_buf: &mut BytesMut,
    response_buffer_capacity: usize,
    runtime_state: &NacelleRuntimeState,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    telemetry_plan: TcpTelemetryPlan,
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
            return stage_and_write(
                writer,
                tcp_limits,
                write_buf,
                response_buffer_capacity,
                frame_capacity,
                runtime_state,
                telemetry,
                metrics_context,
                telemetry_plan,
                |dst| protocol.encode_response_terminal_chunk(&mut response_context, chunk, dst),
            )
            .await;
        }
        Ok(None) => {
            return stage_and_write(
                writer,
                tcp_limits,
                write_buf,
                response_buffer_capacity,
                protocol.max_response_frame_overhead(),
                runtime_state,
                telemetry,
                metrics_context,
                telemetry_plan,
                |dst| protocol.encode_response_end(&mut response_context, dst),
            )
            .await;
        }
        Err(body) => body,
    };

    let mut response_wire_bytes = 0_usize;
    while let Some(chunk) = body.next_chunk().await {
        let chunk = chunk.map_err(|error| ResponseDeliveryError {
            error,
            delivered_bytes: response_wire_bytes,
        })?;
        if chunk.is_empty() {
            continue;
        }
        validate_response_bytes(&mut response_body_bytes, chunk.len(), runtime_state).map_err(
            |error| ResponseDeliveryError {
                error,
                delivered_bytes: response_wire_bytes,
            },
        )?;
        let frame_capacity = response_frame_capacity(protocol, chunk.len()).map_err(|error| {
            ResponseDeliveryError {
                error,
                delivered_bytes: response_wire_bytes,
            }
        })?;
        let written = stage_and_write(
            writer,
            tcp_limits,
            write_buf,
            response_buffer_capacity,
            frame_capacity,
            runtime_state,
            telemetry,
            metrics_context,
            telemetry_plan,
            |dst| protocol.encode_response_chunk(&mut response_context, chunk, dst),
        )
        .await
        .map_err(|error| error.with_previous(response_wire_bytes))?;
        response_wire_bytes = response_wire_bytes.saturating_add(written);
    }

    let written = stage_and_write(
        writer,
        tcp_limits,
        write_buf,
        response_buffer_capacity,
        protocol.max_response_frame_overhead(),
        runtime_state,
        telemetry,
        metrics_context,
        telemetry_plan,
        |dst| protocol.encode_response_end(&mut response_context, dst),
    )
    .await
    .map_err(|error| error.with_previous(response_wire_bytes))?;

    Ok(response_wire_bytes.saturating_add(written))
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn write_error<P, W, Observer>(
    writer: &mut W,
    protocol: &P,
    context: Option<P::ErrorContext>,
    error: &NacelleError,
    buffer_capacity: usize,
    tcp_limits: &NacelleTcpLimits,
    write_buf: &mut BytesMut,
    runtime_state: &NacelleRuntimeState,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    telemetry_plan: TcpTelemetryPlan,
) -> Result<usize, ResponseDeliveryError>
where
    P: Protocol,
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    stage_and_write(
        writer,
        tcp_limits,
        write_buf,
        buffer_capacity,
        buffer_capacity.max(128),
        runtime_state,
        telemetry,
        metrics_context,
        telemetry_plan,
        |dst| protocol.encode_error(context.as_ref(), error, dst),
    )
    .await
}

fn response_frame_capacity<P>(protocol: &P, chunk_len: usize) -> Result<usize, NacelleError>
where
    P: Protocol,
{
    chunk_len
        .checked_add(protocol.max_response_frame_overhead())
        .ok_or(NacelleError::ResourceLimit("response_frame_bytes"))
}

#[allow(clippy::too_many_arguments)]
fn stage_and_write<'a, W, E, Observer>(
    writer: &'a mut W,
    tcp_limits: &'a NacelleTcpLimits,
    write_buf: &'a mut BytesMut,
    response_buffer_capacity: usize,
    frame_capacity: usize,
    runtime_state: &'a NacelleRuntimeState,
    telemetry: &'a NacelleTelemetry<Observer>,
    metrics_context: Option<&'a NacelleMetricsContext>,
    telemetry_plan: TcpTelemetryPlan,
    encode: E,
) -> impl Future<Output = Result<usize, ResponseDeliveryError>> + 'a
where
    W: AsyncWrite + Unpin + 'a,
    E: FnOnce(&mut FrameBuffer<'_>) -> Result<(), NacelleError>,
    Observer: NacelleTelemetryObserver,
{
    write_buf.clear();
    let staging_result = (|| {
        let staging_allocation = if frame_capacity > write_buf.capacity() {
            match runtime_state.allocate_memory(frame_capacity) {
                Ok(allocation) => {
                    *write_buf = BytesMut::with_capacity(frame_capacity);
                    Some(allocation)
                }
                Err(error) => return Err(ResponseDeliveryError::before_delivery(error)),
            }
        } else {
            None
        };
        let mut frame = FrameBuffer::new(write_buf, frame_capacity);
        if let Err(error) = encode(&mut frame) {
            write_buf.clear();
            return Err(ResponseDeliveryError::before_delivery(error));
        }
        Ok(staging_allocation)
    })();

    async move {
        let _staging_allocation = match staging_result {
            Ok(allocation) => allocation,
            Err(error) => {
                reset_write_buffer(write_buf, response_buffer_capacity);
                return Err(error);
            }
        };
        let written = write_buf.len();
        let write_started = start_tcp_phase(telemetry_plan.phase_duration);
        let result =
            write_all_tracked_with_timeout(writer, write_buf, tcp_limits, "tcp_write").await;
        finish_tcp_phase(telemetry, metrics_context, "socket_write", write_started);
        if let Err((error, _)) = &result {
            record_tcp_error(telemetry, metrics_context, "socket_write", error);
        }
        reset_write_buffer(write_buf, response_buffer_capacity);
        result
            .map(|_| written)
            .map_err(|(error, delivered_bytes)| ResponseDeliveryError {
                error,
                delivered_bytes,
            })
    }
}

impl ResponseDeliveryError {
    fn before_delivery(error: NacelleError) -> Self {
        Self {
            error,
            delivered_bytes: 0,
        }
    }

    fn with_previous(self, previous: usize) -> Self {
        Self {
            error: self.error,
            delivered_bytes: previous.saturating_add(self.delivered_bytes),
        }
    }
}

fn reset_write_buffer(write_buf: &mut BytesMut, response_buffer_capacity: usize) {
    write_buf.clear();
    if write_buf.capacity() > response_buffer_capacity {
        *write_buf = BytesMut::with_capacity(response_buffer_capacity);
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
    use nacelle_core::limits::NacelleLimits;
    use tokio::io::sink;

    use super::*;

    #[tokio::test]
    async fn oversized_staging_is_rejected_before_encoding() {
        let mut writer = sink();
        let limits = NacelleTcpLimits::default();
        let mut write_buf = BytesMut::with_capacity(8);
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(16));
        let _baseline_allocation = runtime_state
            .allocate_memory(8)
            .expect("baseline response buffer should fit");
        let telemetry = NacelleTelemetry::default();
        let encoded = std::cell::Cell::new(false);

        let result = stage_and_write(
            &mut writer,
            &limits,
            &mut write_buf,
            8,
            32,
            &runtime_state,
            &telemetry,
            None,
            TcpTelemetryPlan::new(&telemetry),
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
            })
        ));
        assert!(!encoded.get());
        assert_eq!(runtime_state.memory_used_bytes(), 8);
    }
}
