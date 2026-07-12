use bytes::BytesMut;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

use crate::config::{NacelleTcpConfig, TcpRequestBodyMode};
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    DecodedRequest, LocalSerialTcpHandler, LocalSerialTcpOneWayHandler, LocalTcpHandler,
    LocalTcpOneWayHandler, Protocol, SerialTcpHandler, SerialTcpOneWayHandler, SharedProtocol,
    TcpHandler, TcpHandlerCompletion, TcpOneWayHandler, TcpRequest, TcpResponder,
};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::{
    ConnectionContext, ConnectionInfo, NoResponse, RequestContext, RequiredResponder,
};
use nacelle_core::request::{NacelleBody, NacelleConnectionMeta};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};

use super::body::{buffered_request_body, pump_request_body, read_buffered_request_body};
use super::metrics::{
    TcpRequestMetricsGuard, TcpTelemetryPlan, finish_tcp_phase, record_core_request_completed,
    record_core_request_failed, record_tcp_error, start_tcp_phase,
};
use super::response::{ResponseDelivery, encode_response_body, write_error};

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
        responder: RequiredResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection;
}

pub(super) trait OneWayDispatch<P, Connection>
where
    P: Protocol,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut Connection,
        request: TcpRequest<P::OneWayRequest>,
    ) -> impl Future<Output = Result<nacelle_core::pipeline::Completed, NacelleError>> + 'connection;
}

pub(super) struct SharedRequestDispatch<H>(pub(super) Arc<H>);
pub(super) struct SharedOneWayDispatch<H>(pub(super) Arc<H>);
pub(super) struct LocalRequestDispatch<H>(pub(super) Rc<H>);
pub(super) struct LocalOneWayDispatch<H>(pub(super) Rc<H>);
pub(super) struct SerialRequestDispatch<H>(pub(super) Arc<H>);
pub(super) struct SerialOneWayDispatch<H>(pub(super) Arc<H>);
pub(super) struct LocalSerialRequestDispatch<H>(pub(super) Rc<H>);
pub(super) struct LocalSerialOneWayDispatch<H>(pub(super) Rc<H>);

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
        responder: RequiredResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        let context = RequestContext::new(request, responder, (), connection.clone());
        nacelle_core::pipeline::Handler::call(self.0.as_ref(), context)
    }
}

impl<P, H> OneWayDispatch<P, ConnectionContext<Arc<P::ConnectionState>>> for SharedOneWayDispatch<H>
where
    P: SharedProtocol,
    H: TcpOneWayHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut ConnectionContext<Arc<P::ConnectionState>>,
        request: TcpRequest<P::OneWayRequest>,
    ) -> impl Future<Output = Result<nacelle_core::pipeline::Completed, NacelleError>> + 'connection
    {
        let context = RequestContext::new(request, NoResponse, (), connection.clone());
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
        responder: RequiredResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        SerialTcpHandler::call(
            self.0.as_ref(),
            RequestContext::new(request, responder, (), connection),
        )
    }
}

impl<P, H> OneWayDispatch<P, ConnectionContext<P::ConnectionState>> for SerialOneWayDispatch<H>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpOneWayHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut ConnectionContext<P::ConnectionState>,
        request: TcpRequest<P::OneWayRequest>,
    ) -> impl Future<Output = Result<nacelle_core::pipeline::Completed, NacelleError>> + 'connection
    {
        SerialTcpOneWayHandler::call(
            self.0.as_ref(),
            RequestContext::new(request, NoResponse, (), connection),
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
        responder: RequiredResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        let context = RequestContext::new(request, responder, (), connection.clone());
        nacelle_core::pipeline::LocalHandler::call(self.0.as_ref(), context)
    }
}

#[allow(clippy::future_not_send)]
impl<P, H> OneWayDispatch<P, ConnectionContext<Arc<P::ConnectionState>>> for LocalOneWayDispatch<H>
where
    P: Protocol,
    H: LocalTcpOneWayHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut ConnectionContext<Arc<P::ConnectionState>>,
        request: TcpRequest<P::OneWayRequest>,
    ) -> impl Future<Output = Result<nacelle_core::pipeline::Completed, NacelleError>> + 'connection
    {
        let context = RequestContext::new(request, NoResponse, (), connection.clone());
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
        responder: RequiredResponder<TcpResponder<P::Response, P::ResponseContext>>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'connection {
        LocalSerialTcpHandler::call(
            self.0.as_ref(),
            RequestContext::new(request, responder, (), connection),
        )
    }
}

#[allow(clippy::future_not_send)]
impl<P, H> OneWayDispatch<P, ConnectionContext<P::ConnectionState>> for LocalSerialOneWayDispatch<H>
where
    P: Protocol,
    H: LocalSerialTcpOneWayHandler<P>,
{
    fn call<'connection>(
        &'connection self,
        connection: &'connection mut ConnectionContext<P::ConnectionState>,
        request: TcpRequest<P::OneWayRequest>,
    ) -> impl Future<Output = Result<nacelle_core::pipeline::Completed, NacelleError>> + 'connection
    {
        LocalSerialTcpOneWayHandler::call(
            self.0.as_ref(),
            RequestContext::new(request, NoResponse, (), connection),
        )
    }
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
    let core_request_events = telemetry_plan.observer;
    let request_started = telemetry_plan
        .request_duration
        .then(std::time::Instant::now);
    let request_bytes = protocol.request_wire_bytes(&request, decoded.body_len);
    let response_context = protocol.response_context(&request);
    let max_request_body_bytes = protocol.max_request_body_bytes(
        &request,
        handler.info(connection_context),
        handler.state(connection_context),
        runtime_state.limits().max_request_body_bytes,
    );
    if decoded.body_len > max_request_body_bytes {
        let error = NacelleError::ResourceLimit("request_body_bytes");
        record_tcp_error(telemetry, metrics_context, "request_body_limit", &error);
        record_core_request_failed(
            telemetry,
            core_request_events,
            !detailed_request_metrics,
            connection.transport,
            request_started,
            &error,
        );
        if let Err(delivery_error) = write_error::<P, W, Observer>(
            writer,
            protocol,
            Some(error_context),
            &error,
            tcp_limits,
            delivery,
            runtime_state,
            telemetry,
            metrics_context,
            telemetry_plan,
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
            telemetry_plan,
        )
        .await
    } else if config.request_body_mode == TcpRequestBodyMode::Buffered {
        let body_started = start_tcp_phase(telemetry_plan.phase_duration);
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
            telemetry_plan,
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
            telemetry_plan,
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
    };

    match outcome {
        Ok(completion) => {
            let encode_started = start_tcp_phase(telemetry_plan.phase_duration);
            let encode_result = encode_response_body::<P, W, Observer>(
                protocol,
                completion.into_inner(),
                writer,
                tcp_limits,
                delivery,
                runtime_state,
                telemetry,
                metrics_context,
                telemetry_plan,
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
                    if delivery_error.is_encode_failure() {
                        record_tcp_error(
                            telemetry,
                            metrics_context,
                            delivery_error.phase(),
                            &delivery_error.error,
                        );
                    }
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
            request_metrics.complete("ok", response_bytes);
        }
        Err(error) => {
            record_tcp_error(telemetry, metrics_context, "handler", &error);
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
                telemetry_plan,
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
pub(super) async fn run_one_way<P, A, D, R, Observer>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    protocol: &P,
    connection_access: &A,
    handler: &D,
    decoded: DecodedRequest<P::OneWayRequest>,
    config: &NacelleTcpConfig,
    telemetry: &NacelleTelemetry<Observer>,
    runtime_state: &NacelleRuntimeState,
    tcp_limits: &NacelleTcpLimits,
    connection: &NacelleConnectionMeta,
    connection_context: &mut A::Connection,
    metrics_context: Option<&NacelleMetricsContext>,
    telemetry_plan: TcpTelemetryPlan,
) -> Result<(), NacelleError>
where
    P: Protocol,
    A: ConnectionAccess<P>,
    D: OneWayDispatch<P, A::Connection>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
{
    let request = decoded.request;
    let request_bytes = protocol.one_way_wire_bytes(&request, decoded.body_len);
    let request_started = telemetry_plan
        .request_duration
        .then(std::time::Instant::now);
    let max_request_body_bytes = protocol.max_one_way_body_bytes(
        &request,
        connection_access.info(connection_context),
        connection_access.state(connection_context),
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
                telemetry_plan.request_events,
                metrics_context.is_none(),
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
                telemetry_plan.request_events,
                metrics_context.is_none(),
                connection.transport,
                request_started,
                &error,
            );
            Err(error)
        }
    }
}

async fn execute_one_way<P, D, Connection>(
    handler: &D,
    request: P::OneWayRequest,
    body: NacelleBody,
    runtime_state: &NacelleRuntimeState,
    connection_context: &mut Connection,
) -> Result<nacelle_core::pipeline::Completed, NacelleError>
where
    P: Protocol,
    D: OneWayDispatch<P, Connection>,
{
    let future = handler.call(
        connection_context,
        TcpRequest {
            head: request,
            body,
        },
    );
    if let Some(timeout) = runtime_state.limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout("handler"))?
    } else {
        future.await
    }
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
    telemetry_plan: TcpTelemetryPlan,
) -> Result<crate::protocol::TcpHandlerCompletion<P>, NacelleError>
where
    P: Protocol,
    D: RequestDispatch<P>,
    Observer: NacelleTelemetryObserver,
{
    let handler_started =
        metrics_context.and_then(|_| start_tcp_phase(telemetry_plan.phase_duration));
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
        RequiredResponder::new(TcpResponder::new(response_context)),
    );
    if let Some(timeout) = runtime_state.limits().handler_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout("handler"))?
    } else {
        future.await
    }
}
