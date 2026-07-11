use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::config::{NacelleTcpConfig, TcpRequestBodyMode};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{DecodedRequest, Protocol};
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::{
    NacelleBody, NacelleConnectionMeta, NacelleRequest, NacelleRequestMeta, TcpRequestMeta,
};
use nacelle_core::response::NacelleResponse;
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry};

use super::body::{buffered_request_body, pump_request_body, read_buffered_request_body};
use super::metrics::{
    TcpRequestMetricsGuard, finish_tcp_phase, record_core_request_completed,
    record_core_request_failed, record_tcp_error, start_tcp_phase,
};
use super::response::{encode_response_body, write_error};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_request<P, H, R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    protocol: &P,
    handler: &H,
    decoded: DecodedRequest<P::Request>,
    error_context: P::ErrorContext,
    config: &NacelleTcpConfig,
    telemetry: &NacelleTelemetry,
    runtime_state: &NacelleRuntimeState,
    tcp_limits: &NacelleTcpLimits,
    connection: &NacelleConnectionMeta,
    metrics_context: Option<&NacelleMetricsContext>,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: Handler,
    R: AsyncRead + Unpin + Send,
{
    let request = decoded.request;
    let detailed_request_metrics = telemetry.request_metrics_enabled();
    let core_request_events = telemetry.request_events_enabled() && !detailed_request_metrics;
    let core_request_duration_metrics =
        core_request_events && telemetry.request_duration_metrics_enabled();
    let request_started = (core_request_duration_metrics
        || telemetry.request_duration_metrics_enabled())
    .then(std::time::Instant::now);
    let request_bytes = 4 + 20 + decoded.body_len;
    let response_context = protocol.response_context(&request);
    let request_meta = protocol.request_meta(&request, decoded.body_len);
    let max_request_body_bytes = protocol.max_request_body_bytes(
        &request,
        connection,
        runtime_state.limits().max_request_body_bytes,
    );
    if decoded.body_len > max_request_body_bytes {
        let error = NacelleError::ResourceLimit("request_body_bytes");
        record_tcp_error(telemetry, metrics_context, "request_body_limit", &error);
        record_core_request_failed(
            telemetry,
            core_request_events,
            connection.transport,
            request_started,
            &error,
        );
        write_error::<P>(
            write_buf,
            protocol,
            Some(error_context),
            error,
            config.response_buffer_capacity,
        )?;
        return Err(NacelleError::ResourceLimit("request_body_bytes"));
    }
    let _request_permit = match runtime_state.acquire_request_tracked() {
        Ok(permit) => permit,
        Err(error) => {
            record_tcp_error(telemetry, metrics_context, "request_permit", &error);
            return Err(error);
        }
    };
    let mut request_metrics = TcpRequestMetricsGuard::new(
        telemetry,
        metrics_context.cloned(),
        request_bytes,
        request_started,
    );
    let outcome = if decoded.body_len <= read_buf.len() {
        let body_started = start_tcp_phase(telemetry);
        let body =
            buffered_request_body(read_buf, decoded.body_len, config.request_body_chunk_size);
        finish_tcp_phase(
            telemetry,
            metrics_context,
            "request_body_read",
            body_started,
        );
        execute_handler_with_metrics(
            handler,
            request,
            request_meta,
            body,
            runtime_state,
            connection,
            telemetry,
            metrics_context,
        )
        .await
    } else if config.request_body_mode == TcpRequestBodyMode::Buffered {
        let body_started = start_tcp_phase(telemetry);
        let body = read_buffered_request_body(
            reader,
            read_buf,
            decoded.body_len,
            runtime_state,
            tcp_limits,
        )
        .await
        .inspect_err(|error| {
            record_tcp_error(telemetry, metrics_context, "request_body_read", error)
        })?;
        finish_tcp_phase(
            telemetry,
            metrics_context,
            "request_body_read",
            body_started,
        );
        execute_handler_with_metrics(
            handler,
            request,
            request_meta,
            body,
            runtime_state,
            connection,
            telemetry,
            metrics_context,
        )
        .await
    } else {
        let _streaming_permit =
            runtime_state
                .acquire_streaming_task_tracked()
                .inspect_err(|error| {
                    record_tcp_error(telemetry, metrics_context, "streaming_task", error)
                })?;
        let _streaming_body_allocation = runtime_state
            .allocate_memory_with_timeout(
                decoded.body_len,
                runtime_state.limits().memory_allocation_timeout,
            )
            .await
            .inspect_err(|error| {
                record_tcp_error(telemetry, metrics_context, "streaming_memory", error)
            })?;
        let (body_tx, body_rx) = mpsc::channel(config.request_body_channel_capacity);
        let body = NacelleBody::new(body_rx, decoded.body_len);
        let h = handler.clone();
        let state = runtime_state.clone();
        let tcp_limits = *tcp_limits;
        let connection = connection.clone();
        let handler_telemetry = telemetry.clone();
        let handler_metrics_context = metrics_context.cloned();
        let handler_task = nacelle_core::runtime::spawn(async move {
            execute_handler_with_metrics(
                &h,
                request,
                request_meta,
                body,
                &state,
                &connection,
                &handler_telemetry,
                handler_metrics_context.as_ref(),
            )
            .await
        });

        let body_started = start_tcp_phase(telemetry);
        let pump_result = pump_request_body(
            reader,
            read_buf,
            decoded.body_len,
            &body_tx,
            config,
            &tcp_limits,
        )
        .await;
        finish_tcp_phase(
            telemetry,
            metrics_context,
            "request_body_read",
            body_started,
        );
        if let Err(error) = &pump_result {
            record_tcp_error(telemetry, metrics_context, "request_body_read", error);
        }
        drop(body_tx);
        let outcome = match handler_task.await {
            Ok(outcome) => outcome?,
            Err(error) => {
                let error = NacelleError::from(error);
                record_tcp_error(telemetry, metrics_context, "handler_join", &error);
                return Err(error);
            }
        };
        pump_result?;
        Ok(outcome)
    };

    match outcome {
        Ok(response) => {
            let prev_response_len = write_buf.len();
            let encode_started = start_tcp_phase(telemetry);
            let encode_result = encode_response_body::<P>(
                protocol,
                response_context,
                response,
                write_buf,
                runtime_state,
            )
            .await;
            finish_tcp_phase(
                telemetry,
                metrics_context,
                "response_encode",
                encode_started,
            );
            if let Err(error) = encode_result {
                record_tcp_error(telemetry, metrics_context, "response_encode", &error);
                return Err(error);
            }
            let response_bytes = write_buf.len().saturating_sub(prev_response_len);
            record_core_request_completed(
                telemetry,
                core_request_events,
                connection.transport,
                request_bytes,
                response_bytes,
                request_started,
            );
            request_metrics.complete("ok", response_bytes);
        }
        Err(error) => {
            let prev_response_len = write_buf.len();
            record_tcp_error(telemetry, metrics_context, "handler", &error);
            record_core_request_failed(
                telemetry,
                core_request_events,
                connection.transport,
                request_started,
                &error,
            );
            write_error::<P>(
                write_buf,
                protocol,
                Some(error_context),
                error,
                config.response_buffer_capacity,
            )?;
            let response_bytes = write_buf.len().saturating_sub(prev_response_len);
            record_core_request_completed(
                telemetry,
                core_request_events,
                connection.transport,
                request_bytes,
                response_bytes,
                request_started,
            );
            request_metrics.complete("error", response_bytes);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_handler_with_metrics<Req, H>(
    handler: &H,
    request: Req,
    request_meta: TcpRequestMeta,
    body: NacelleBody,
    runtime_state: &NacelleRuntimeState,
    connection: &NacelleConnectionMeta,
    telemetry: &NacelleTelemetry,
    metrics_context: Option<&NacelleMetricsContext>,
) -> Result<NacelleResponse, NacelleError>
where
    Req: Send + 'static,
    H: Handler,
{
    let handler_started = metrics_context.and_then(|_| start_tcp_phase(telemetry));
    let result = execute_handler(
        handler,
        request,
        request_meta,
        body,
        runtime_state,
        connection,
    )
    .await;
    finish_tcp_phase(telemetry, metrics_context, "handler", handler_started);
    result
}

async fn execute_handler<Req, H>(
    handler: &H,
    _request: Req,
    request_meta: TcpRequestMeta,
    body: NacelleBody,
    runtime_state: &NacelleRuntimeState,
    connection: &NacelleConnectionMeta,
) -> Result<NacelleResponse, NacelleError>
where
    Req: Send + 'static,
    H: Handler,
{
    let request = NacelleRequest {
        connection: connection.clone(),
        meta: NacelleRequestMeta::Tcp(request_meta),
        body,
    };
    let future = handler.call(request);
    if let Some(timeout) = runtime_state.limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout("handler"))?
    } else {
        future.await
    }
}
