use std::convert::Infallible;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use nacelle_codec::MessageReader;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::NacelleTcpConfig;
use crate::limits::NacelleTcpLimits;
use crate::protocol::{
    DecodedMessage, LocalSerialTcpHandler, LocalSerialTcpOneWayHandler, NoOneWayHandler, Protocol,
    SerialTcpHandler, SerialTcpOneWayHandler, SharedProtocol, TcpHandler, TcpOneWayHandler,
};
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
mod tests;

use framing::{InstrumentedDecoder, allocate_connection_buffers, map_message_read_error};
use io::read_message_with_timeout;
use io::shutdown_with_timeout;
use metrics::{TcpTelemetryPlan, record_tcp_error, tcp_close_reason, tcp_metrics_context};
use request::{
    LocalOneWayDispatch, LocalRequestDispatch, LocalSerialOneWayDispatch,
    LocalSerialRequestDispatch, OneWayDispatch, RequestDispatch, SerialOneWayDispatch,
    SerialRequestDispatch, SharedOneWayDispatch, SharedRequestDispatch, run_one_way, run_request,
};
use response::ResponseDelivery;

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
    P: SharedProtocol<OneWayRequest = Infallible>,
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
        Arc::new(NoOneWayHandler::<P>::new()),
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
    P: SharedProtocol<OneWayRequest = Infallible>,
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
        Arc::new(NoOneWayHandler::<P>::new()),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_connection_with_connection_meta_and_tcp_state<P, H, OH, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let _connection_permit = runtime_state.acquire_connection_tracked()?;
    drive_connection(
        reader,
        writer,
        protocol,
        handler,
        one_way_handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection<P, H, OH, R, W, Observer>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SharedRequestDispatch(handler),
        SharedOneWayDispatch(one_way_handler),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn drive_connection_with_dispatch<P, PO, D, OD, R, W, Observer>(
    reader: R,
    mut writer: W,
    protocol: PO,
    handler: D,
    one_way_handler: OD,
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
    OD: OneWayDispatch<P, D::Connection>,
    Observer: NacelleTelemetryObserver,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let _buffer_allocation = allocate_connection_buffers(&config, &runtime_state)?;
    let mut response_delivery = ResponseDelivery::new(&config);
    let transport = connection.transport;
    let connection_info = ConnectionInfo::from(&connection);
    let connection_state = protocol.connection_state(&connection_info);
    let mut connection_context = handler.new_connection(connection_info, connection_state);
    let telemetry_plan = TcpTelemetryPlan::new(&telemetry);
    let connection_metrics = Some(tcp_metrics_context(protocol.deref(), &connection));
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

    let result: Result<(), NacelleError> = async {
        'conn: loop {
            #[cfg(feature = "buffer-rotation")]
            request_reader.rotate_empty_buffer(config.read_buffer_capacity);

            let read_result = read_message_with_timeout(&mut request_reader, &tcp_limits).await;
            let decoded = match read_result {
                Ok(Some(decoded)) => decoded,
                Ok(None) => break 'conn,
                Err(error) => {
                    if let Some(connection_metrics) = &connection_metrics {
                        telemetry.operation_error(connection_metrics, "socket_read", &error);
                    }
                    return Err(error);
                }
            };

            let mut decoded = Some(decoded);
            while let Some(message) = decoded {
                let (reader, read_buf) = request_reader.transport_and_buffer_mut();
                match message {
                    DecodedMessage::Request(request) => {
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
                    }
                    DecodedMessage::OneWay(request) => {
                        run_one_way(
                            reader,
                            read_buf,
                            protocol.deref(),
                            &handler,
                            &one_way_handler,
                            request,
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
                    }
                }
                decoded = request_reader
                    .decode_buffered()
                    .map_err(map_message_read_error)?;
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
    let result = match result {
        Ok(()) => {
            let shutdown_result = shutdown_with_timeout(&mut writer, &tcp_limits).await;
            if let Err(error) = &shutdown_result {
                record_tcp_error(
                    &telemetry,
                    connection_metrics.as_ref(),
                    "socket_shutdown",
                    error,
                );
            }
            shutdown_result
        }
        Err(error) => Err(error),
    };
    if let Some(connection_metrics) = &connection_metrics {
        telemetry.connection_closed(connection_metrics, tcp_close_reason(&result));
    }
    result
}

/// Drive one worker-local TCP connection without taking another connection permit.
#[allow(clippy::too_many_arguments)]
pub async fn serve_local_stream_without_connection_limit<P, H, OH, IO, Observer>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    one_way_handler: Rc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: crate::protocol::LocalTcpHandler<P>,
    OH: crate::protocol::LocalTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalRequestDispatch(handler),
        LocalOneWayDispatch(one_way_handler),
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

/// Drive one serial shared-runtime TCP connection without another connection permit.
#[allow(clippy::too_many_arguments)]
pub async fn serve_serial_stream_without_connection_limit<P, H, OH, IO, Observer>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
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
    OH: SerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        SerialRequestDispatch(handler),
        SerialOneWayDispatch(one_way_handler),
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
pub async fn serve_local_serial_stream_without_connection_limit<P, H, OH, IO, Observer>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    one_way_handler: Rc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P>,
    OH: LocalSerialTcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(&mut io);
    drive_connection_with_dispatch(
        reader,
        writer,
        protocol,
        LocalSerialRequestDispatch(handler),
        LocalSerialOneWayDispatch(one_way_handler),
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
    P: SharedProtocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        Arc::new(NoOneWayHandler::<P>::new()),
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
    P: SharedProtocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        Arc::new(NoOneWayHandler::<P>::new()),
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_stream_with_connection_meta_and_tcp_state<P, H, OH, IO, Observer>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _connection_permit = runtime_state.acquire_connection_tracked()?;
    serve_stream_inner(
        &mut io,
        protocol,
        handler,
        one_way_handler,
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
    P: SharedProtocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        Arc::new(NoOneWayHandler::<P>::new()),
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
    P: SharedProtocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        Arc::new(handler),
        Arc::new(NoOneWayHandler::<P>::new()),
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
    OH,
    IO,
    Observer,
>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_inner(
        &mut io,
        protocol,
        handler,
        one_way_handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_stream_inner<P, H, OH, IO, Observer>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    drive_connection(
        reader,
        writer,
        protocol,
        handler,
        one_way_handler,
        config,
        telemetry,
        runtime_state,
        tcp_limits,
        connection,
    )
    .await
}
