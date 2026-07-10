use bytes::BytesMut;

use crate::protocol::{DecodedRequest, Protocol};
use nacelle_core::config::NacelleConfig;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::{NacelleMemoryAllocation, NacelleRuntimeState};
use nacelle_core::request::RequestMetadata;
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry};

use super::metrics::{finish_tcp_phase, start_tcp_phase};

pub(super) fn allocate_connection_buffers(
    config: &NacelleConfig,
    runtime_state: &NacelleRuntimeState,
) -> Result<NacelleMemoryAllocation, NacelleError> {
    let bytes = config
        .read_buffer_capacity
        .saturating_add(config.response_buffer_capacity);
    runtime_state.allocate_memory(bytes)
}

pub(super) fn decode_next_request<Req, P>(
    protocol: &P,
    read_buf: &mut BytesMut,
    max_frame_len: usize,
    telemetry: &NacelleTelemetry,
    metrics_context: &NacelleMetricsContext,
) -> Result<Option<DecodedRequest<Req>>, NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
{
    let decode_started = start_tcp_phase(telemetry);
    let result = protocol.decode_head(read_buf, max_frame_len);
    finish_tcp_phase(telemetry, Some(metrics_context), "decode", decode_started);
    if let Err(error) = &result {
        telemetry.operation_error(metrics_context, "decode", error);
    }
    result
}
