//! Direct framed-connection serving primitives.
//!
//! These functions own the supplied I/O values for the lifetime of their
//! futures, except the explicitly borrowed serial helper. Cancellation drops
//! owned I/O and does not guarantee a graceful socket shutdown. The functions
//! enforce [`NacelleRuntimeState`] limits plus [`NacelleTcpConfig`] and
//! [`NacelleTcpLimits`]; callers of `without_connection_limit` variants must
//! already hold the corresponding connection permit.
//!
//! # Errors
//!
//! Serving returns [`NacelleError`] for connection admission, framing,
//! protocol, handler, body, socket, timeout, and shutdown failures.
//!
//! # Panics
//!
//! These futures must be polled inside a Tokio runtime. Nacelle does not
//! intentionally panic for peer or configuration input. Worker-local variants
//! must also run in their documented local execution context.
//!
//! # Example
//!
//! Run a listener that accepts sockets and passes each one directly to
//! [`crate::TcpServer::serve_io`]:
//!
//! ```text
//! cargo run -p nacelle-examples --bin direct_tcp
//! ```

use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use nacelle_codec::MessageReader;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::NacelleTcpConfig;
use crate::limits::NacelleTcpLimits;
use crate::policies::{
    DeliveryDispatch, LocalDispatch, LocalTcpPolicySet, NoTcpPolicies, SharedDispatch,
    SharedTcpPolicySet,
};
#[cfg(feature = "policies")]
use crate::policy::TcpLifecyclePolicy;
use crate::protocol::{
    LocalSerialTcpHandler, Protocol, SerialTcpHandler, SharedProtocol, TcpHandler,
};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::ConnectionInfo;
use nacelle_core::policy::{PolicyLease, Rejection};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver};

mod body;
#[doc(hidden)]
pub mod checkpoint;
mod framing;
mod io;
mod metrics;
mod request;
mod response;
#[cfg(test)]
mod tests;

use framing::{InstrumentedDecoder, map_message_read_error};
use io::read_message_with_timeout;
use io::shutdown_with_timeout;
use metrics::{TcpTelemetryPlan, record_tcp_error, tcp_metrics_context};
use request::{
    LocalRequestDispatch, LocalSerialRequestDispatch, RequestDispatch, SerialRequestDispatch,
    SharedRequestDispatch, run_request,
};
use response::{ResponseDelivery, write_error};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_shared_halves_with_policies<P, H, R, W, Observer, Policies>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
    policies: &Policies,
    acquire_connection: bool,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let _connection_permit = acquire_connection
        .then(|| runtime_state.acquire_connection_tracked())
        .transpose()?;
    policies
        .run(drive_connection_with_dispatch(
            reader,
            writer,
            protocol,
            SharedRequestDispatch(handler),
            SharedDispatch(policies),
            config,
            telemetry,
            runtime_state,
            tcp_limits,
            connection,
        ))
        .await
        .map_err(Rejection::into_error)?
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_shared_io_with_policies<P, H, IO, Observer, Policies>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
    policies: &Policies,
    acquire_connection: bool,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: SharedTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    IO: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (reader, writer) = tokio::io::split(&mut io);
    serve_shared_halves_with_policies(
        reader,
        writer,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
        policies,
        acquire_connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_serial_io_with_policies<P, H, IO, Observer, Policies>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
    policies: &Policies,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: SharedTcpPolicySet<P::Request, P::ConnectionState>,
    IO: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (reader, writer) = tokio::io::split(io);
    policies
        .run(drive_connection_with_dispatch(
            reader,
            writer,
            protocol,
            SerialRequestDispatch(handler),
            SharedDispatch(policies),
            config,
            telemetry,
            runtime_state,
            tcp_limits,
            connection,
        ))
        .await
        .map_err(Rejection::into_error)?
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_local_io_with_policies<P, H, IO, Observer, Policies>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
    policies: &Policies,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: crate::protocol::LocalTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: LocalTcpPolicySet<P::Request, Arc<P::ConnectionState>>,
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, writer) = tokio::io::split(&mut io);
    policies
        .run(drive_connection_with_dispatch(
            reader,
            writer,
            protocol,
            LocalRequestDispatch(handler),
            LocalDispatch(policies),
            config,
            telemetry,
            runtime_state,
            tcp_limits,
            connection,
        ))
        .await
        .map_err(Rejection::into_error)?
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_local_serial_io_with_policies<P, H, IO, Observer, Policies>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
    policies: &Policies,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    Policies: LocalTcpPolicySet<P::Request, P::ConnectionState>,
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, writer) = tokio::io::split(&mut io);
    policies
        .run(drive_connection_with_dispatch(
            reader,
            writer,
            protocol,
            LocalSerialRequestDispatch(handler),
            LocalDispatch(policies),
            config,
            telemetry,
            runtime_state,
            tcp_limits,
            connection,
        ))
        .await
        .map_err(Rejection::into_error)?
}

/// Drive one TCP framed connection.
pub async fn serve_connection<P, H, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_connection_with_connection_meta_and_tcp_state(
        reader,
        writer,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
}

/// Drive one TCP framed connection with caller-supplied connection metadata.
#[allow(clippy::too_many_arguments)]
pub async fn serve_connection_with_connection_meta<P, H, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_connection_with_connection_meta_and_tcp_state(
        reader,
        writer,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_connection_with_connection_meta_and_tcp_state<P, H, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    telemetry.register_runtime_state(runtime_state.clone());
    let _connection_permit = runtime_state.acquire_connection_tracked()?;
    drive_connection(
        reader,
        writer,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection<P, H, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SharedRequestDispatch(handler),
        SharedDispatch(&NoTcpPolicies),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection_with_dispatch<P, PO, D, R, W, Observer, Policies>(
    reader: R,
    mut writer: W,
    protocol: PO,
    handler: D,
    policies: Policies,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    mut connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    PO: Deref<Target = P>,
    D: RequestDispatch<P>,
    Policies: DeliveryDispatch<P::Request, D::Storage, W, Observer>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    telemetry.register_runtime_state(runtime_state.clone());
    #[cfg(feature = "policies")]
    let _transport_policy_permit = protocol
        .policy()
        .transport_ready(&connection)
        .await
        .map_err(|error| {
            crate::policies::reject_connection(
                protocol.deref(),
                &connection,
                &telemetry,
                "policy_transport_ready",
                error,
            )
        })?;
    let _ready_lease = policies.ready(&mut connection).await.map_err(|rejection| {
        crate::policies::reject_connection(
            protocol.deref(),
            &connection,
            &telemetry,
            "policy_transport_ready",
            rejection.into_error(),
        )
    })?;
    let mut response_delivery = ResponseDelivery::new(&config);
    let transport = connection.transport;
    let connection_info = ConnectionInfo::from(&connection);
    let connection_state = protocol.connection_state(&connection_info);
    let mut connection_context = handler.new_connection(connection_info, connection_state);
    let telemetry_plan = TcpTelemetryPlan::new(&telemetry);
    let connection_metrics = telemetry
        .context_metrics_enabled()
        .then(|| tcp_metrics_context(protocol.deref(), &connection));
    #[cfg(feature = "phase-timing")]
    let reader = framing::InstrumentedReader::new(
        reader,
        &telemetry,
        connection_metrics.as_ref(),
        telemetry_plan.phase_duration,
    );
    let decoder = InstrumentedDecoder::new(
        protocol.decoder(config.max_frame_len),
        &telemetry,
        connection_metrics.as_ref(),
        telemetry_plan.phase_duration,
    );
    let mut request_reader =
        MessageReader::with_capacity(reader, decoder, config.read_buffer_capacity);
    if let Some(connection_metrics) = &connection_metrics {
        telemetry.connection_accepted(connection_metrics);
    }
    telemetry.connection_opened(transport);
    let mut connection_guard =
        metrics::TcpConnectionMetricsGuard::new(&telemetry, connection_metrics.as_ref(), transport);

    #[cfg(feature = "policies")]
    let mut legacy_policy_permit = None;
    let mut request_policy_permit = None;
    let result: Result<(), NacelleError> = async {
        'conn: loop {
            #[cfg(feature = "buffer-rotation")]
            request_reader.rotate_empty_buffer(config.read_buffer_capacity);

            let read_result = {
                async {
                    #[cfg(feature = "policies")]
                    if legacy_policy_permit.is_none() {
                        let mut failure = None;
                        legacy_policy_permit = Some(
                            checkpoint::await_admission(
                                protocol.policy().before_request(
                                    handler.info(&connection_context),
                                    handler.state(&connection_context),
                                ),
                                checkpoint::Io {
                                    writer: &mut writer,
                                    delivery: &mut response_delivery,
                                    limits: &tcp_limits,
                                    telemetry: &telemetry,
                                    metrics: connection_metrics.as_ref(),
                                    phase_duration: telemetry_plan.phase_duration,
                                    failure: &mut failure,
                                },
                            )
                            .await
                            .map_err(|error| {
                                if failure.is_some() {
                                    framing::MessageReadFailure::delivery(error)
                                } else {
                                    framing::MessageReadFailure::policy(error)
                                }
                            })?,
                        );
                    }
                    if request_policy_permit.is_none() {
                        let mut failure = None;
                        request_policy_permit = Some(
                            policies
                                .before(
                                    handler.context_mut(&mut connection_context),
                                    checkpoint::Io {
                                        writer: &mut writer,
                                        delivery: &mut response_delivery,
                                        limits: &tcp_limits,
                                        telemetry: &telemetry,
                                        metrics: connection_metrics.as_ref(),
                                        phase_duration: telemetry_plan.phase_duration,
                                        failure: &mut failure,
                                    },
                                )
                                .await
                                .map_err(|error| {
                                    if failure.is_some() {
                                        framing::MessageReadFailure::delivery(error)
                                    } else {
                                        framing::MessageReadFailure::policy(error)
                                    }
                                })?,
                        );
                    }
                    let mut failure = None;
                    checkpoint::continue_input(
                        read_message_with_timeout(&mut request_reader, &tcp_limits),
                        checkpoint::Io {
                            writer: &mut writer,
                            delivery: &mut response_delivery,
                            limits: &tcp_limits,
                            telemetry: &telemetry,
                            metrics: connection_metrics.as_ref(),
                            phase_duration: telemetry_plan.phase_duration,
                            failure: &mut failure,
                        },
                        framing::MessageReadFailure::delivery,
                    )
                    .await
                }
                .await
            };
            let decoded = match read_result {
                Ok(Some(decoded)) => decoded,
                Ok(None) => break 'conn,
                Err(failure) => {
                    let should_encode =
                        failure.should_encode() && !response_delivery.is_interrupted();
                    let phase = failure.operation_phase();
                    let error = failure.into_error();
                    if let (Some(connection_metrics), Some(phase)) = (&connection_metrics, phase) {
                        telemetry.operation_error(connection_metrics, phase, &error);
                    }
                    if should_encode
                        && let Err(delivery_error) = write_error::<P, W, Observer>(
                            &mut writer,
                            protocol.deref(),
                            None,
                            &error,
                            &tcp_limits,
                            &mut response_delivery,
                            &runtime_state,
                            &telemetry,
                            connection_metrics.as_ref(),
                            telemetry_plan.phase_duration,
                        )
                        .await
                    {
                        return Err(delivery_error.error);
                    }
                    return Err(error);
                }
            };

            let mut decoded = Some(decoded);
            while let Some(request) = decoded {
                let (reader, read_buf) = request_reader.transport_and_buffer_mut();
                let error_context = protocol.error_context(&request.request);
                run_request(
                    reader,
                    &mut writer,
                    read_buf,
                    &mut response_delivery,
                    protocol.deref(),
                    &handler,
                    &policies,
                    request,
                    error_context,
                    &config,
                    &telemetry,
                    &runtime_state,
                    &tcp_limits,
                    &connection,
                    &mut connection_context,
                    connection_metrics.as_ref(),
                    telemetry_plan,
                    request_policy_permit
                        .as_ref()
                        .is_some_and(PolicyLease::requires_flush),
                    #[cfg(feature = "policies")]
                    legacy_policy_permit.as_ref().is_some_and(Option::is_some),
                )
                .await?;
                drop(request_policy_permit.take());
                #[cfg(feature = "policies")]
                drop(legacy_policy_permit.take());
                let mut can_respond = true;
                let buffered_result = {
                    async {
                        #[cfg(feature = "policies")]
                        if !request_reader.buffer().is_empty() {
                            let admission = protocol.policy().before_request(
                                handler.info(&connection_context),
                                handler.state(&connection_context),
                            );
                            let mut failure = None;
                            let result = checkpoint::await_admission(
                                admission,
                                checkpoint::Io {
                                    writer: &mut writer,
                                    delivery: &mut response_delivery,
                                    limits: &tcp_limits,
                                    telemetry: &telemetry,
                                    metrics: connection_metrics.as_ref(),
                                    phase_duration: telemetry_plan.phase_duration,
                                    failure: &mut failure,
                                },
                            )
                            .await;
                            if failure.is_some() {
                                can_respond = false;
                            }
                            legacy_policy_permit = Some(result.inspect_err(|error| {
                                if failure.is_none() {
                                    crate::policies::record_error(
                                        protocol.deref(),
                                        &connection,
                                        &telemetry,
                                        "policy_before_request",
                                        error,
                                    );
                                }
                            })?);
                        }
                        if !request_reader.buffer().is_empty() {
                            let mut failure = None;
                            let admitted = policies
                                .before(
                                    handler.context_mut(&mut connection_context),
                                    checkpoint::Io {
                                        writer: &mut writer,
                                        delivery: &mut response_delivery,
                                        limits: &tcp_limits,
                                        telemetry: &telemetry,
                                        metrics: connection_metrics.as_ref(),
                                        phase_duration: telemetry_plan.phase_duration,
                                        failure: &mut failure,
                                    },
                                )
                                .await;
                            if failure.is_some() {
                                can_respond = false;
                            }
                            request_policy_permit = Some(admitted?);
                        }
                        request_reader
                            .decode_buffered()
                            .map_err(map_message_read_error)
                    }
                    .await
                };
                decoded = match buffered_result {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        if response_delivery.checkpoint_flush_pending() {
                            response_delivery.interrupt();
                        }
                        can_respond &= !response_delivery.is_interrupted();
                        if !can_respond {
                            return Err(error);
                        }
                        if let Err(delivery_error) = write_error::<P, W, Observer>(
                            &mut writer,
                            protocol.deref(),
                            None,
                            &error,
                            &tcp_limits,
                            &mut response_delivery,
                            &runtime_state,
                            &telemetry,
                            connection_metrics.as_ref(),
                            telemetry_plan.phase_duration,
                        )
                        .await
                        {
                            return Err(delivery_error.error);
                        }
                        return Err(error);
                    }
                };
            }
            if !response_delivery.checkpoint_flush_pending() {
                response_delivery
                    .flush(
                        &mut writer,
                        &tcp_limits,
                        &telemetry,
                        connection_metrics.as_ref(),
                        telemetry_plan.phase_duration,
                    )
                    .await
                    .map_err(|error| error.error)?;
            }
        }

        Ok(())
    }
    .await;

    if response_delivery.is_interrupted() {
        // A cancelled writer operation must not be resumed during cleanup.
        connection_guard.finish(&result);
        return result;
    }

    let result = match response_delivery
        .flush(
            &mut writer,
            &tcp_limits,
            &telemetry,
            connection_metrics.as_ref(),
            telemetry_plan.phase_duration,
        )
        .await
    {
        Ok(_) => result,
        Err(error) => Err(error.error),
    };
    let shutdown_result = shutdown_with_timeout(&mut writer, &tcp_limits).await;
    if let Err(error) = &shutdown_result {
        record_tcp_error(
            &telemetry,
            connection_metrics.as_ref(),
            "socket_shutdown",
            error,
        );
    }
    let result = match result {
        Ok(()) => shutdown_result,
        Err(error) => Err(error),
    };
    connection_guard.finish(&result);
    result
}

/// Drive one worker-local TCP connection without taking another connection permit.
#[allow(clippy::too_many_arguments)]
pub async fn serve_local_stream_without_connection_limit<P, H, IO, Observer>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: crate::protocol::LocalTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalRequestDispatch(handler),
        LocalDispatch(&NoTcpPolicies),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

/// Drive one serial shared-runtime TCP connection without another connection permit.
///
/// The I/O value is borrowed so a caller that cancels this future regains the
/// stream and can apply its own finalization policy.
#[allow(clippy::too_many_arguments)]
pub async fn serve_serial_stream_without_connection_limit<P, H, IO, Observer>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (reader, writer) = tokio::io::split(io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SerialRequestDispatch(handler),
        SharedDispatch(&NoTcpPolicies),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

/// Drive one worker-local serial TCP connection without another connection permit.
#[allow(clippy::too_many_arguments)]
pub async fn serve_local_serial_stream_without_connection_limit<P, H, IO, Observer>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalSerialRequestDispatch(handler),
        LocalDispatch(&NoTcpPolicies),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

/// Drive one TCP framed connection using a single unsplit I/O object.
pub async fn serve_stream<P, H, IO, Observer>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
}

/// Drive one TCP framed connection using a single unsplit I/O object and caller-supplied metadata.
pub async fn serve_stream_with_connection_meta<P, H, IO, Observer>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_stream_with_connection_meta_and_tcp_state<P, H, IO, Observer>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    telemetry.register_runtime_state(runtime_state.clone());
    let _connection_permit = runtime_state.acquire_connection_tracked()?;
    serve_stream_inner(
        &mut io,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

/// Drive one TCP framed connection using a single unsplit I/O object.
pub async fn serve_stream_without_connection_limit<P, H, IO, Observer>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
}

/// Drive one TCP framed connection without taking a connection permit.
pub async fn serve_stream_without_connection_limit_with_connection_meta<P, H, IO, Observer>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_stream_without_connection_limit_with_connection_meta_and_tcp_state<
    P,
    H,
    IO,
    Observer,
>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_inner(
        &mut io,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_stream_inner<P, H, IO, Observer>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    drive_connection(
        reader,
        writer,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}
