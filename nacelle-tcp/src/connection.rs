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
use crate::protocol::{Protocol, SharedProtocol};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::ConnectionInfo;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver};

mod body;
mod framing;
mod io;
mod metrics;
mod request;
mod response;
#[cfg(test)]
pub(crate) mod tests;

use framing::{InstrumentedDecoder, map_message_read_error};
use io::read_message_with_timeout;
use io::shutdown_with_timeout;
use metrics::{TcpTelemetryPlan, record_tcp_error, tcp_close_reason, tcp_metrics_context};
use request::{
    LocalRequestDispatch, LocalSerialRequestDispatch, RequestDispatch, SerialRequestDispatch,
    SharedRequestDispatch, run_request,
};
use response::{ResponseDelivery, write_error};

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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SharedRequestDispatch(handler),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection_with_dispatch<P, PO, D, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: PO,
    handler: D,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    PO: Deref<Target = P>,
    D: RequestDispatch<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if handler.admission_state().is_some() && config.head_read_limits.is_none() {
        return Err(nacelle_core::NacelleConfigurationError::AdmissionReadBounds.into());
    }
    if config.head_read_limits.is_some() {
        drive_connection_with_head_reads::<P, PO, D, R, W, Observer, true>(
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
    } else {
        drive_connection_with_head_reads::<P, PO, D, R, W, Observer, false>(
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
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection_with_head_reads<P, PO, D, R, W, Observer, const BOUNDED: bool>(
    reader: R,
    mut writer: W,
    protocol: PO,
    handler: D,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    PO: Deref<Target = P>,
    D: RequestDispatch<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    telemetry.register_runtime_state(runtime_state.clone());
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
        framing::HeadDecoder::<_, BOUNDED>::new(
            protocol.decoder(config.max_frame_len),
            config.head_read_limits,
        ),
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

    let mut error_limits = tcp_limits;
    if let Some(admission) = handler.admission_state() {
        error_limits
            .write_timeout
            .get_or_insert(admission.config().timeout());
    }
    let result: Result<(), NacelleError> = async {
        'conn: loop {
            #[cfg(feature = "buffer-rotation")]
            request_reader.rotate_empty_buffer(config.read_buffer_capacity);

            let read_result = read_message_with_timeout::<_, _, BOUNDED>(
                &mut request_reader,
                &tcp_limits,
                config
                    .head_read_limits
                    .map_or(0, crate::TcpHeadReadLimits::max_undecided_bytes),
            )
            .await;
            let decoded = match read_result {
                Ok(Some(decoded)) => decoded,
                Ok(None) => break 'conn,
                Err(failure) => {
                    let should_encode = failure.should_encode();
                    let error = failure.into_error();
                    if let Some(connection_metrics) = &connection_metrics {
                        telemetry.operation_error(connection_metrics, "socket_read", &error);
                    }
                    if should_encode
                        && let Err(delivery_error) = write_error::<P, W, Observer>(
                            &mut writer,
                            protocol.deref(),
                            None,
                            &error,
                            &error_limits,
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
                )
                .await?;
                decoded = match request_reader.decode_buffered() {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        let error = map_message_read_error(error);
                        if let Err(delivery_error) = write_error::<P, W, Observer>(
                            &mut writer,
                            protocol.deref(),
                            None,
                            &error,
                            &error_limits,
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

        Ok(())
    }
    .await;

    let mut closing_limits = if result.is_err() {
        error_limits
    } else {
        tcp_limits
    };
    if result.is_err()
        && let Some(admission) = handler.admission_state()
    {
        closing_limits
            .shutdown_timeout
            .get_or_insert(admission.config().timeout());
    }
    let result = match response_delivery
        .flush(
            &mut writer,
            &closing_limits,
            &telemetry,
            connection_metrics.as_ref(),
            telemetry_plan.phase_duration,
        )
        .await
    {
        Ok(_) => result,
        Err(error) => Err(error.error),
    };
    let shutdown_result = shutdown_with_timeout(&mut writer, &closing_limits).await;
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
    telemetry.connection_closed(
        transport,
        connection_metrics.as_ref(),
        tcp_close_reason(&result),
    );
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
    H: crate::admission::LocalTcpService<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalRequestDispatch(handler),
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
    H: crate::admission::SerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (reader, writer) = tokio::io::split(io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SerialRequestDispatch(handler),
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
    H: crate::admission::LocalSerialTcpService<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalSerialRequestDispatch(handler),
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
    H: crate::admission::TcpService<P>,
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
