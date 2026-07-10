use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::limits::NacelleTcpLimits;
use crate::protocol::Protocol;
use nacelle_core::config::NacelleConfig;
use nacelle_core::error::NacelleError;
use nacelle_core::handler::Handler;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::{NacelleConnectionMeta, RequestMetadata};
use nacelle_core::telemetry::NacelleTelemetry;

mod body;
mod framing;
mod io;
mod metrics;
mod request;
mod response;
#[cfg(test)]
mod tests;

use framing::{allocate_connection_buffers, decode_next_request};
use io::{read_buf_with_timeout, write_all_with_timeout};
use metrics::{finish_tcp_phase, start_tcp_phase, tcp_close_reason, tcp_metrics_context};
use request::run_request;

/// Drive one TCP framed connection and coalesce completed responses into writes.
pub async fn serve_connection<Req, P, H, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_connection_with_connection_meta_and_tcp_state(
        reader,
        writer,
        protocol,
        handler,
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
pub async fn serve_connection_with_connection_meta<Req, P, H, R, W>(
    reader: R,
    writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    serve_connection_with_connection_meta_and_tcp_state(
        reader,
        writer,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_connection_with_connection_meta_and_tcp_state<Req, P, H, R, W>(
    mut reader: R,
    mut writer: W,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let _connection_permit = runtime_state.acquire_connection_tracked()?;
    let _buffer_allocation = allocate_connection_buffers(&config, &runtime_state)?;
    let mut read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
    let mut write_buf = BytesMut::with_capacity(config.response_buffer_capacity);
    let transport = connection.transport;
    let connection_metrics = tcp_metrics_context(protocol.as_ref(), &connection);
    telemetry.connection_accepted(&connection_metrics);
    telemetry.connection_opened(transport);

    let result: Result<(), NacelleError> = async {
        'conn: loop {
            if !write_buf.is_empty() {
                let write_started = start_tcp_phase(&telemetry);
                let write_result =
                    write_all_with_timeout(&mut writer, &write_buf, &tcp_limits, "tcp_write").await;
                finish_tcp_phase(
                    &telemetry,
                    Some(&connection_metrics),
                    "socket_write",
                    write_started,
                );
                if let Err(error) = write_result {
                    telemetry.operation_error(&connection_metrics, "socket_write", &error);
                    return Err(error);
                }
                write_buf.clear();
                if write_buf.capacity() > config.response_buffer_capacity {
                    write_buf = BytesMut::with_capacity(config.response_buffer_capacity);
                }
            }

            if read_buf.is_empty() && read_buf.capacity() > config.read_buffer_capacity {
                read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
            }

            let read_started = start_tcp_phase(&telemetry);
            let read_result =
                read_buf_with_timeout(&mut reader, &mut read_buf, &tcp_limits, "tcp_read").await;
            finish_tcp_phase(
                &telemetry,
                Some(&connection_metrics),
                "socket_read",
                read_started,
            );
            let bytes_read = match read_result {
                Ok(bytes_read) => bytes_read,
                Err(error) => {
                    telemetry.operation_error(&connection_metrics, "socket_read", &error);
                    return Err(error);
                }
            };
            if bytes_read == 0 {
                if read_buf.is_empty() {
                    break 'conn;
                }
                return Err(NacelleError::UnexpectedEof);
            }

            while let Some(decoded) = decode_next_request(
                protocol.as_ref(),
                &mut read_buf,
                config.max_frame_len,
                &telemetry,
                &connection_metrics,
            )? {
                let error_context = protocol.error_context(&decoded.request);
                run_request(
                    &mut reader,
                    &mut read_buf,
                    &mut write_buf,
                    protocol.as_ref(),
                    &handler,
                    decoded,
                    error_context,
                    &config,
                    &telemetry,
                    &runtime_state,
                    &tcp_limits,
                    &connection,
                    telemetry.metrics_enabled().then_some(&connection_metrics),
                )
                .await?;
            }
        }

        Ok(())
    }
    .await;

    if !write_buf.is_empty() {
        let write_started = start_tcp_phase(&telemetry);
        let final_write =
            write_all_with_timeout(&mut writer, &write_buf, &tcp_limits, "tcp_final_write").await;
        finish_tcp_phase(
            &telemetry,
            Some(&connection_metrics),
            "socket_write",
            write_started,
        );
        if let Err(error) = &final_write {
            telemetry.operation_error(&connection_metrics, "socket_write", error);
        }
    }

    telemetry.connection_closed(&connection_metrics, tcp_close_reason(&result));
    result
}

/// Drive one TCP framed connection using a single unsplit I/O object.
pub async fn serve_stream<Req, P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
}

/// Drive one TCP framed connection using a single unsplit I/O object and caller-supplied metadata.
pub async fn serve_stream_with_connection_meta<Req, P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_with_connection_meta_and_tcp_state(
        io,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        connection,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_stream_with_connection_meta_and_tcp_state<Req, P, H, IO>(
    mut io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
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
pub async fn serve_stream_without_connection_limit<Req, P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        handler,
        config,
        telemetry,
        runtime_state,
        NacelleTcpLimits::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
}

/// Drive one TCP framed connection without taking a connection permit.
pub async fn serve_stream_without_connection_limit_with_connection_meta<Req, P, H, IO>(
    io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_stream_without_connection_limit_with_connection_meta_and_tcp_state(
        io,
        protocol,
        handler,
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
    Req,
    P,
    H,
    IO,
>(
    mut io: IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
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
async fn serve_stream_inner<Req, P, H, IO>(
    io: &mut IO,
    protocol: Arc<P>,
    handler: H,
    config: NacelleConfig,
    telemetry: NacelleTelemetry,
    runtime_state: NacelleRuntimeState,
    tcp_limits: NacelleTcpLimits,
    connection: NacelleConnectionMeta,
) -> Result<(), NacelleError>
where
    Req: RequestMetadata + Send + 'static,
    P: Protocol<Req> + Send + Sync + 'static,
    H: Handler,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _buffer_allocation = allocate_connection_buffers(&config, &runtime_state)?;
    let mut read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
    let mut write_buf = BytesMut::with_capacity(config.response_buffer_capacity);
    let transport = connection.transport;
    let connection_metrics = tcp_metrics_context(protocol.as_ref(), &connection);
    telemetry.connection_accepted(&connection_metrics);
    telemetry.connection_opened(transport);

    let result: Result<(), NacelleError> = async {
        'conn: loop {
            if !write_buf.is_empty() {
                let write_started = start_tcp_phase(&telemetry);
                let write_result =
                    write_all_with_timeout(io, &write_buf, &tcp_limits, "tcp_write").await;
                finish_tcp_phase(
                    &telemetry,
                    Some(&connection_metrics),
                    "socket_write",
                    write_started,
                );
                if let Err(error) = write_result {
                    telemetry.operation_error(&connection_metrics, "socket_write", &error);
                    return Err(error);
                }
                write_buf.clear();
                if write_buf.capacity() > config.response_buffer_capacity {
                    write_buf = BytesMut::with_capacity(config.response_buffer_capacity);
                }
            }

            if read_buf.is_empty() && read_buf.capacity() > config.read_buffer_capacity {
                read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
            }

            let read_started = start_tcp_phase(&telemetry);
            let read_result =
                read_buf_with_timeout(io, &mut read_buf, &tcp_limits, "tcp_read").await;
            finish_tcp_phase(
                &telemetry,
                Some(&connection_metrics),
                "socket_read",
                read_started,
            );
            let bytes_read = match read_result {
                Ok(bytes_read) => bytes_read,
                Err(error) => {
                    telemetry.operation_error(&connection_metrics, "socket_read", &error);
                    return Err(error);
                }
            };
            if bytes_read == 0 {
                if read_buf.is_empty() {
                    break 'conn;
                }
                return Err(NacelleError::UnexpectedEof);
            }

            while let Some(decoded) = decode_next_request(
                protocol.as_ref(),
                &mut read_buf,
                config.max_frame_len,
                &telemetry,
                &connection_metrics,
            )? {
                let error_context = protocol.error_context(&decoded.request);
                run_request(
                    io,
                    &mut read_buf,
                    &mut write_buf,
                    protocol.as_ref(),
                    &handler,
                    decoded,
                    error_context,
                    &config,
                    &telemetry,
                    &runtime_state,
                    &tcp_limits,
                    &connection,
                    telemetry.metrics_enabled().then_some(&connection_metrics),
                )
                .await?;
            }
        }

        Ok(())
    }
    .await;

    if !write_buf.is_empty() {
        let write_started = start_tcp_phase(&telemetry);
        let final_write =
            write_all_with_timeout(io, &write_buf, &tcp_limits, "tcp_final_write").await;
        finish_tcp_phase(
            &telemetry,
            Some(&connection_metrics),
            "socket_write",
            write_started,
        );
        if let Err(error) = &final_write {
            telemetry.operation_error(&connection_metrics, "socket_write", error);
        }
    }

    telemetry.connection_closed(&connection_metrics, tcp_close_reason(&result));
    result
}
