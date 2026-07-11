use std::convert::Infallible;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use bytes::BytesMut;
use nacelle_codec::MessageReader;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::config::NacelleTcpConfig;
use crate::limits::NacelleTcpLimits;
use crate::protocol::{DecodedMessage, NoOneWayHandler, Protocol, TcpHandler, TcpOneWayHandler};
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::pipeline::{ConnectionContext, ConnectionInfo};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::NacelleTelemetry;

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
use metrics::{finish_tcp_phase, start_tcp_phase, tcp_close_reason, tcp_metrics_context};
use request::{
    LocalOneWayDispatch, LocalRequestDispatch, OneWayDispatch, RequestDispatch,
    SharedOneWayDispatch, SharedRequestDispatch, run_one_way, run_request,
};

/// Drive one TCP framed connection.
pub async fn serve_connection<P, H, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
pub async fn serve_connection_with_connection_meta<P, H, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
pub(crate) async fn serve_connection_with_connection_meta_and_tcp_state<P, H, OH, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
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
async fn drive_connection<P, H, OH, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
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
async fn drive_connection_with_dispatch<P, PO, D, OD, R, W>(
    reader: R,
    mut writer: W,
    protocol: PO,
    handler: D,
    one_way_handler: OD,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    PO: Deref<Target = P>,
    D: RequestDispatch<P>,
    OD: OneWayDispatch<P>,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let _buffer_allocation = allocate_connection_buffers(&config, &runtime_state)?;
    let mut write_buf = BytesMut::with_capacity(config.response_buffer_capacity);
    let transport = connection.transport;
    let connection_context = ConnectionContext::new(
        ConnectionInfo::from(&connection),
        Arc::new(protocol.connection_state(&ConnectionInfo::from(&connection))),
    );
    let connection_metrics = tcp_metrics_context(protocol.deref(), &connection);
    let decoder = InstrumentedDecoder::new(
        protocol.decoder(config.max_frame_len),
        &telemetry,
        &connection_metrics,
    );
    let mut request_reader =
        MessageReader::with_capacity(reader, decoder, config.read_buffer_capacity);
    telemetry.connection_accepted(&connection_metrics);
    telemetry.connection_opened(transport);

    let result: Result<(), NacelleError> = async {
        'conn: loop {
            #[cfg(feature = "buffer-rotation")]
            request_reader.rotate_empty_buffer(config.read_buffer_capacity);

            let read_started = start_tcp_phase(&telemetry);
            let read_result =
                read_message_with_timeout(&mut request_reader, &tcp_limits, "tcp_read").await;
            finish_tcp_phase(
                &telemetry,
                Some(&connection_metrics),
                "socket_read",
                read_started,
            );
            let decoded = match read_result {
                Ok(Some(decoded)) => decoded,
                Ok(None) => break 'conn,
                Err(error) => {
                    telemetry.operation_error(&connection_metrics, "socket_read", &error);
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
                            &mut write_buf,
                            protocol.deref(),
                            &handler,
                            request,
                            error_context,
                            &config,
                            &telemetry,
                            &runtime_state,
                            &tcp_limits,
                            &connection,
                            &connection_context,
                            telemetry.metrics_enabled().then_some(&connection_metrics),
                        )
                        .await?;
                    }
                    DecodedMessage::OneWay(request) => {
                        run_one_way(
                            reader,
                            read_buf,
                            protocol.deref(),
                            &one_way_handler,
                            request,
                            &config,
                            &telemetry,
                            &runtime_state,
                            &tcp_limits,
                            &connection,
                            &connection_context,
                            telemetry.metrics_enabled().then_some(&connection_metrics),
                        )
                        .await?;
                    }
                }
                decoded = request_reader
                    .decode_buffered()
                    .map_err(map_message_read_error)?;
            }
        }

        Ok(())
    }
    .await;

    telemetry.connection_closed(&connection_metrics, tcp_close_reason(&result));
    result
}

/// Drive one worker-local TCP connection without taking another connection permit.
#[allow(clippy::too_many_arguments)]
pub async fn serve_local_stream_without_connection_limit<P, H, OH, IO>(
    mut io: IO,
    protocol: Rc<P>,
    handler: Rc<H>,
    one_way_handler: Rc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: crate::protocol::LocalTcpHandler<P>,
    OH: crate::protocol::LocalTcpOneWayHandler<P>,
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

/// Drive one TCP framed connection using a single unsplit I/O object.
pub async fn serve_stream<P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
pub async fn serve_stream_with_connection_meta<P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
pub(crate) async fn serve_stream_with_connection_meta_and_tcp_state<P, H, OH, IO>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
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
pub async fn serve_stream_without_connection_limit<P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
pub async fn serve_stream_without_connection_limit_with_connection_meta<P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol<OneWayRequest = Infallible>,
    H: TcpHandler<P>,
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
>(
    mut io: IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
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
async fn serve_stream_inner<P, H, OH, IO>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: Arc<H>,
    one_way_handler: Arc<OH>,
    config: NacelleTcpConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
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
