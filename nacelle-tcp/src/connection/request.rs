use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::config::{NacelleTcpConfig, TcpRequestBodyMode};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    DecodedRequest, Protocol, TcpHandler, TcpOneWayHandler, TcpRequest, TcpResponder,
};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::{ConnectionContext, NoResponse, RequestContext, RequiredResponder};
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry};

use super::body::{buffered_request_body, pump_request_body, read_buffered_request_body};
use super::metrics::{
    TcpRequestMetricsGuard, finish_tcp_phase, record_core_request_completed,
    record_core_request_failed, record_tcp_error, start_tcp_phase,
};
use super::response::{encode_response_body, write_error};

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_request<P, H, R, W>(
    reader: &mut R,
    writer: &mut W,
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
    connection_context: &ConnectionContext<std::sync::Arc<P::ConnectionState>>,
    metrics_context: Option<&NacelleMetricsContext>,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin,
{
    let request = decoded.request;
    let detailed_request_metrics = telemetry.request_metrics_enabled();
    let core_request_events = telemetry.request_events_enabled() && !detailed_request_metrics;
    let core_request_duration_metrics =
        core_request_events && telemetry.request_duration_metrics_enabled();
    let request_started = (core_request_duration_metrics
        || telemetry.request_duration_metrics_enabled())
    .then(std::time::Instant::now);
    let request_bytes = protocol.request_wire_bytes(&request, decoded.body_len);
    let response_context = protocol.response_context(&request);
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
        if let Err(delivery_error) = write_error::<P, W>(
            writer,
            protocol,
            Some(error_context),
            &error,
            config.response_buffer_capacity,
            tcp_limits,
            write_buf,
            runtime_state,
            telemetry,
            metrics_context,
        )
        .await
        {
            return Err(delivery_error.error);
        }
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
            body,
            response_context,
            runtime_state,
            connection_context,
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
            body,
            response_context,
            runtime_state,
            connection_context,
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
        let handler_future = execute_handler_with_metrics(
            handler,
            request,
            body,
            response_context,
            runtime_state,
            connection_context,
            telemetry,
            metrics_context,
        );
        let pump_future = pump_request_body(
            reader,
            read_buf,
            decoded.body_len,
            body_tx,
            config,
            tcp_limits,
        );
        tokio::pin!(handler_future);
        tokio::pin!(pump_future);

        tokio::select! {
            biased;
            pump_result = &mut pump_future => {
                match pump_result {
                    Ok(()) => handler_future.await,
                    Err(error) => {
                        record_tcp_error(
                            telemetry,
                            metrics_context,
                            "request_body_read",
                            &error,
                        );
                        Err(error)
                    }
                }
            }
            handler_result = &mut handler_future => {
                match handler_result {
                    Ok(completion) => {
                        let body_started = start_tcp_phase(telemetry);
                        let pump_result = pump_future.await;
                        finish_tcp_phase(
                            telemetry,
                            metrics_context,
                            "request_body_read",
                            body_started,
                        );
                        if let Err(error) = &pump_result {
                            record_tcp_error(telemetry, metrics_context, "request_body_read", error);
                        }
                        pump_result?;
                        Ok(completion)
                    }
                    Err(error) => Err(error),
                }
            }
        }
    };

    match outcome {
        Ok(completion) => {
            let encode_started = start_tcp_phase(telemetry);
            let encode_result = encode_response_body::<P, W>(
                protocol,
                completion.into_inner(),
                writer,
                tcp_limits,
                write_buf,
                config.response_buffer_capacity,
                runtime_state,
                telemetry,
                metrics_context,
            )
            .await;
            finish_tcp_phase(
                telemetry,
                metrics_context,
                "response_encode",
                encode_started,
            );
            let response_bytes = match encode_result {
                Ok(response_bytes) => response_bytes,
                Err(delivery_error) => {
                    record_tcp_error(
                        telemetry,
                        metrics_context,
                        "response_encode",
                        &delivery_error.error,
                    );
                    request_metrics.complete("error", delivery_error.delivered_bytes);
                    return Err(delivery_error.error);
                }
            };
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
            record_tcp_error(telemetry, metrics_context, "handler", &error);
            record_core_request_failed(
                telemetry,
                core_request_events,
                connection.transport,
                request_started,
                &error,
            );
            let response_bytes = match write_error::<P, W>(
                writer,
                protocol,
                Some(error_context),
                &error,
                config.response_buffer_capacity,
                tcp_limits,
                write_buf,
                runtime_state,
                telemetry,
                metrics_context,
            )
            .await
            {
                Ok(response_bytes) => response_bytes,
                Err(delivery_error) => {
                    request_metrics.complete("error", delivery_error.delivered_bytes);
                    return Err(delivery_error.error);
                }
            };
            record_core_request_completed(
                telemetry,
                core_request_events,
                connection.transport,
                request_bytes,
                response_bytes,
                request_started,
            );
            request_metrics.complete("error", response_bytes);
            return Err(error);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_one_way<P, H, R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    protocol: &P,
    handler: &H,
    decoded: DecodedRequest<P::OneWayRequest>,
    config: &NacelleTcpConfig,
    telemetry: &NacelleTelemetry,
    runtime_state: &NacelleRuntimeState,
    tcp_limits: &NacelleTcpLimits,
    connection: &NacelleConnectionMeta,
    connection_context: &ConnectionContext<std::sync::Arc<P::ConnectionState>>,
    metrics_context: Option<&NacelleMetricsContext>,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpOneWayHandler<P>,
    R: AsyncRead + Unpin + Send,
{
    let request = decoded.request;
    let request_bytes = protocol.one_way_wire_bytes(&request, decoded.body_len);
    let request_started = telemetry
        .request_duration_metrics_enabled()
        .then(std::time::Instant::now);
    let max_request_body_bytes = protocol.max_one_way_body_bytes(
        &request,
        connection,
        runtime_state.limits().max_request_body_bytes,
    );
    if decoded.body_len > max_request_body_bytes {
        return Err(NacelleError::ResourceLimit("request_body_bytes"));
    }
    let _request_permit = runtime_state.acquire_request_tracked()?;
    let mut request_metrics = TcpRequestMetricsGuard::new(
        telemetry,
        metrics_context.cloned(),
        request_bytes,
        request_started,
    );

    let result = if decoded.body_len <= read_buf.len() {
        let body =
            buffered_request_body(read_buf, decoded.body_len, config.request_body_chunk_size);
        execute_one_way(handler, request, body, runtime_state, connection_context).await
    } else if config.request_body_mode == TcpRequestBodyMode::Buffered {
        let body = read_buffered_request_body(
            reader,
            read_buf,
            decoded.body_len,
            runtime_state,
            tcp_limits,
        )
        .await?;
        execute_one_way(handler, request, body, runtime_state, connection_context).await
    } else {
        let _streaming_permit = runtime_state.acquire_streaming_task_tracked()?;
        let _streaming_body_allocation = runtime_state
            .allocate_memory_with_timeout(
                decoded.body_len,
                runtime_state.limits().memory_allocation_timeout,
            )
            .await?;
        let (body_tx, body_rx) = mpsc::channel(config.request_body_channel_capacity);
        let body = NacelleBody::new(body_rx, decoded.body_len);
        let handler_future =
            execute_one_way(handler, request, body, runtime_state, connection_context);
        let pump_future = pump_request_body(
            reader,
            read_buf,
            decoded.body_len,
            body_tx,
            config,
            tcp_limits,
        );
        tokio::pin!(handler_future);
        tokio::pin!(pump_future);
        tokio::select! {
            biased;
            pump_result = &mut pump_future => {
                pump_result?;
                handler_future.await
            }
            handler_result = &mut handler_future => {
                let completion = handler_result?;
                pump_future.await?;
                Ok(completion)
            }
        }
    };

    match result {
        Ok(_completed) => {
            request_metrics.complete("ok", 0);
            record_core_request_completed(
                telemetry,
                telemetry.request_events_enabled(),
                connection.transport,
                request_bytes,
                0,
                request_started,
            );
            Ok(())
        }
        Err(error) => {
            request_metrics.complete("error", 0);
            record_core_request_failed(
                telemetry,
                telemetry.request_events_enabled(),
                connection.transport,
                request_started,
                &error,
            );
            Err(error)
        }
    }
}

async fn execute_one_way<P, H>(
    handler: &H,
    request: P::OneWayRequest,
    body: NacelleBody,
    runtime_state: &NacelleRuntimeState,
    connection_context: &ConnectionContext<std::sync::Arc<P::ConnectionState>>,
) -> Result<nacelle_core::pipeline::Completed, NacelleError>
where
    P: Protocol,
    H: TcpOneWayHandler<P>,
{
    let context = RequestContext::new(
        TcpRequest {
            head: request,
            body,
        },
        NoResponse,
        (),
        connection_context.clone(),
    );
    let future = handler.call(context);
    if let Some(timeout) = runtime_state.limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout("handler"))?
    } else {
        future.await
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_handler_with_metrics<P, H>(
    handler: &H,
    request: P::Request,
    body: NacelleBody,
    response_context: P::ResponseContext,
    runtime_state: &NacelleRuntimeState,
    connection_context: &ConnectionContext<std::sync::Arc<P::ConnectionState>>,
    telemetry: &NacelleTelemetry,
    metrics_context: Option<&NacelleMetricsContext>,
) -> Result<crate::protocol::TcpHandlerCompletion<P>, NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let handler_started = metrics_context.and_then(|_| start_tcp_phase(telemetry));
    let result = execute_handler(
        handler,
        request,
        body,
        response_context,
        runtime_state,
        connection_context,
    )
    .await;
    finish_tcp_phase(telemetry, metrics_context, "handler", handler_started);
    result
}

async fn execute_handler<P, H>(
    handler: &H,
    request: P::Request,
    body: NacelleBody,
    response_context: P::ResponseContext,
    runtime_state: &NacelleRuntimeState,
    connection_context: &ConnectionContext<std::sync::Arc<P::ConnectionState>>,
) -> Result<crate::protocol::TcpHandlerCompletion<P>, NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
{
    let context = RequestContext::new(
        TcpRequest {
            head: request,
            body,
        },
        RequiredResponder::new(TcpResponder::new(response_context)),
        (),
        connection_context.clone(),
    );
    let future = handler.call(context);
    if let Some(timeout) = runtime_state.limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout("handler"))?
    } else {
        future.await
    }
}
