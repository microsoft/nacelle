use bytes::BytesMut;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::config::{NacelleTcpConfig, TcpRequestBodyMode};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    DecodedRequest, LocalSerialTcpHandler, LocalTcpHandler, Protocol, SerialTcpHandler,
    SharedProtocol, TcpHandler, TcpHandlerCompletion, TcpRequest, TcpResponder,
};
use nacelle_core::error::{NacelleError, NacelleResourceLimitReason, NacelleTimeoutReason};
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::{
    ConnectionContext, ConnectionInfo, OptionalResponder, RequestContext,
};
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::body::{buffered_request_body, pump_request_body, read_buffered_request_body};
use super::metrics::{
    TcpRequestMetricsGuard, TcpTelemetryPlan, finish_tcp_phase, record_core_request_completed,
    record_core_request_failed, record_tcp_error, start_tcp_phase,
};
use super::response::{GuardedResponseCompletion, ResponseDelivery, deliver_response, write_error};

pub(super) trait ConnectionAccess<P>
where
    P: Protocol,
{
    type Connection;

    fn new_connection(&self, info: ConnectionInfo, state: P::ConnectionState) -> Self::Connection;

    fn info<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection ConnectionInfo;

    fn state<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection P::ConnectionState;
}

pub(super) trait RequestDispatch<P>: ConnectionAccess<P>
where
    P: Protocol,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Self::Connection,
        request: TcpRequest<P::Request>,
        responder: OptionalResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection;
}

pub(super) struct SharedRequestDispatch<H>(pub(super) Arc<H>);
pub(super) struct LocalRequestDispatch<H>(pub(super) Rc<H>);
pub(super) struct SerialRequestDispatch<H>(pub(super) Arc<H>);
pub(super) struct LocalSerialRequestDispatch<H>(pub(super) Rc<H>);

impl<P, H> ConnectionAccess<P> for SharedRequestDispatch<H>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
{
    type Connection = ConnectionContext<Arc<P::ConnectionState>>;

    fn new_connection(&self, info: ConnectionInfo, state: P::ConnectionState) -> Self::Connection {
        ConnectionContext::new(info, Arc::new(state))
    }

    fn info<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection ConnectionInfo {
        &connection.info
    }

    fn state<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection P::ConnectionState {
        connection.state.as_ref()
    }
}

impl<P, H> ConnectionAccess<P> for LocalRequestDispatch<H>
where
    P: Protocol,
    H: LocalTcpHandler<P>,
{
    type Connection = ConnectionContext<Arc<P::ConnectionState>>;

    fn new_connection(&self, info: ConnectionInfo, state: P::ConnectionState) -> Self::Connection {
        ConnectionContext::new(info, Arc::new(state))
    }

    fn info<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection ConnectionInfo {
        &connection.info
    }

    fn state<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection P::ConnectionState {
        connection.state.as_ref()
    }
}

impl<P, H> ConnectionAccess<P> for SerialRequestDispatch<H>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
{
    type Connection = ConnectionContext<P::ConnectionState>;

    fn new_connection(&self, info: ConnectionInfo, state: P::ConnectionState) -> Self::Connection {
        ConnectionContext::new(info, state)
    }

    fn info<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection ConnectionInfo {
        &connection.info
    }

    fn state<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection P::ConnectionState {
        &connection.state
    }
}

impl<P, H> ConnectionAccess<P> for LocalSerialRequestDispatch<H>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
{
    type Connection = ConnectionContext<P::ConnectionState>;

    fn new_connection(&self, info: ConnectionInfo, state: P::ConnectionState) -> Self::Connection {
        ConnectionContext::new(info, state)
    }

    fn info<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection ConnectionInfo {
        &connection.info
    }

    fn state<'connection>(
        &self,
        connection: &'connection Self::Connection,
    ) -> &'connection P::ConnectionState {
        &connection.state
    }
}

impl<P, H> RequestDispatch<P> for SharedRequestDispatch<H>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Self::Connection,
        request: TcpRequest<P::Request>,
        responder: OptionalResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        let context = RequestContext::without_app_state(request, responder, connection.clone());
        nacelle_core::pipeline::Handler::call(self.0.as_ref(), context)
    }
}

impl<P, H> RequestDispatch<P> for SerialRequestDispatch<H>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Self::Connection,
        request: TcpRequest<P::Request>,
        responder: OptionalResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        SerialTcpHandler::call(
            self.0.as_ref(),
            RequestContext::without_app_state(request, responder, connection),
        )
    }
}

#[allow(clippy::future_not_send)]
impl<P, H> RequestDispatch<P> for LocalRequestDispatch<H>
where
    P: Protocol,
    H: LocalTcpHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Self::Connection,
        request: TcpRequest<P::Request>,
        responder: OptionalResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        let context = RequestContext::without_app_state(request, responder, connection.clone());
        nacelle_core::pipeline::LocalHandler::call(self.0.as_ref(), context)
    }
}

#[allow(clippy::future_not_send)]
impl<P, H> RequestDispatch<P> for LocalSerialRequestDispatch<H>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Self::Connection,
        request: TcpRequest<P::Request>,
        responder: OptionalResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        LocalSerialTcpHandler::call(
            self.0.as_ref(),
            RequestContext::without_app_state(request, responder, connection),
        )
    }
}

fn use_buffered_body(body_len: usize, buffered_len: usize) -> bool {
    body_len <= buffered_len
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_request<P, D, R, W, Observer>(
    reader: &mut R,
    writer: &mut W,
    read_buf: &mut BytesMut,
    delivery: &mut ResponseDelivery,
    protocol: &P,
    handler: &D,
    decoded: DecodedRequest<P::Request>,
    error_context: P::ErrorContext,
    config: &NacelleTcpConfig,
    telemetry: &NacelleTelemetry<Observer>,
    runtime_state: &NacelleRuntimeState,
    tcp_limits: &NacelleTcpLimits,
    connection: &NacelleConnectionMeta,
    connection_context: &mut D::Connection,
    metrics_context: Option<&NacelleMetricsContext>,
    telemetry_plan: TcpTelemetryPlan,
) -> Result<(), NacelleError>
where
    P: Protocol,
    D: RequestDispatch<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let request = decoded.request;
    let detailed_request_metrics = telemetry_plan.request_metrics;
    let core_request_events = telemetry_plan.request_events;
    let request_started = telemetry_plan
        .request_duration
        .then(std::time::Instant::now);
    let request_bytes = protocol.request_wire_bytes(&request, decoded.body_len);
    let response_context = protocol.response_context(&request);
    let max_request_body_bytes = protocol.max_request_body_bytes(
        &request,
        handler.info(connection_context),
        handler.state(connection_context),
        runtime_state.startup_limits().max_request_body_bytes,
    );
    let mut request_metrics =
        TcpRequestMetricsGuard::new(telemetry, metrics_context, request_bytes, request_started);
    let mut _request_permit = None;
    let outcome = async {
        if decoded.body_len > max_request_body_bytes {
            let error = NacelleError::ResourceLimit(NacelleResourceLimitReason::RequestBodyBytes);
            record_tcp_error(telemetry, metrics_context, "request_body_limit", &error);
            return Err(error);
        }
        _request_permit = Some(match runtime_state.acquire_request_tracked() {
            Ok(permit) => permit,
            Err(error) => {
                record_tcp_error(telemetry, metrics_context, "request_permit", &error);
                return Err(error);
            }
        });
        let use_buffered_body = use_buffered_body(decoded.body_len, read_buf.len());
        if use_buffered_body {
            let body_started = start_tcp_phase(telemetry_plan.phase_duration);
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
                telemetry_plan.phase_duration,
            )
            .await
        } else if config.request_body_mode == TcpRequestBodyMode::Buffered {
            let body_started = start_tcp_phase(telemetry_plan.phase_duration);
            let body_result = read_buffered_request_body(
                reader,
                read_buf,
                decoded.body_len,
                config.head_read_limits.is_some(),
                tcp_limits,
            )
            .await
            .inspect_err(|error| {
                record_tcp_error(telemetry, metrics_context, "request_body_read", error)
            });
            finish_tcp_phase(
                telemetry,
                metrics_context,
                "request_body_read",
                body_started,
            );
            match body_result {
                Ok(body) => {
                    execute_handler_with_metrics(
                        handler,
                        request,
                        body,
                        response_context,
                        runtime_state,
                        connection_context,
                        telemetry,
                        metrics_context,
                        telemetry_plan.phase_duration,
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            let _streaming_permit =
                runtime_state
                    .acquire_streaming_task_tracked()
                    .inspect_err(|error| {
                        record_tcp_error(telemetry, metrics_context, "streaming_task", error)
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
                telemetry_plan.phase_duration,
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
                            let body_started = start_tcp_phase(telemetry_plan.phase_duration);
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
        }
    }
    .await;

    match outcome {
        Ok(completion) => {
            let response_bytes = if let Some(completion) = completion {
                match deliver_response::<P, W, Observer>(
                    protocol,
                    completion,
                    writer,
                    tcp_limits,
                    delivery,
                    runtime_state,
                    telemetry,
                    metrics_context,
                    telemetry_plan.phase_duration,
                )
                .await
                {
                    Ok(response_bytes) => response_bytes,
                    Err(delivery_error) => {
                        request_metrics.complete("error", delivery_error.delivered_bytes);
                        return Err(delivery_error.error);
                    }
                }
            } else {
                0
            };
            record_core_request_completed(
                telemetry,
                core_request_events,
                !detailed_request_metrics,
                connection.transport,
                request_bytes,
                response_bytes,
                request_started,
            );
            request_metrics.complete("ok", response_bytes);
        }
        Err(error) => {
            record_core_request_failed(
                telemetry,
                core_request_events,
                !detailed_request_metrics,
                connection.transport,
                request_started,
                &error,
            );
            let response_bytes = match write_error::<P, W, Observer>(
                writer,
                protocol,
                Some(error_context),
                &error,
                tcp_limits,
                delivery,
                runtime_state,
                telemetry,
                metrics_context,
                telemetry_plan.phase_duration,
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
                !detailed_request_metrics,
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
async fn execute_handler_with_metrics<P, D, Observer>(
    handler: &D,
    request: P::Request,
    body: NacelleBody,
    response_context: P::ResponseContext,
    runtime_state: &NacelleRuntimeState,
    connection_context: &mut D::Connection,
    telemetry: &NacelleTelemetry<Observer>,
    metrics_context: Option<&NacelleMetricsContext>,
    phase_duration_metrics: bool,
) -> Result<Option<GuardedResponseCompletion<P::Response, P::ResponseContext>>, NacelleError>
where
    P: Protocol,
    D: RequestDispatch<P>,
    Observer: NacelleTelemetryObserver,
{
    let handler_started = start_tcp_phase(phase_duration_metrics && metrics_context.is_some());
    let result = execute_handler(
        handler,
        request,
        body,
        response_context,
        runtime_state,
        connection_context,
    )
    .await;
    if let Err(error) = &result {
        record_tcp_error(telemetry, metrics_context, "handler", error);
    }
    finish_tcp_phase(telemetry, metrics_context, "handler", handler_started);
    result.map(|completion| {
        completion
            .into_response()
            .map(GuardedResponseCompletion::new)
    })
}

async fn execute_handler<P, D>(
    handler: &D,
    request: P::Request,
    body: NacelleBody,
    response_context: P::ResponseContext,
    runtime_state: &NacelleRuntimeState,
    connection_context: &mut D::Connection,
) -> Result<crate::protocol::TcpHandlerCompletion<P>, NacelleError>
where
    P: Protocol,
    D: RequestDispatch<P>,
{
    let future = handler.call(
        connection_context,
        TcpRequest {
            head: request,
            body,
        },
        OptionalResponder::new(TcpResponder::new(response_context)),
    );
    if let Some(timeout) = runtime_state.startup_limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(NacelleTimeoutReason::Handler))?
    } else {
        future.await
    }
}
