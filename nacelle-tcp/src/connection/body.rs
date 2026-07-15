use bytes::{Bytes, BytesMut};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::config::NacelleTcpConfig;
use crate::limits::NacelleTcpLimits;
use nacelle_core::error::NacelleError;
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleBody;

use super::io::read_buf_with_timeout;

pub(super) async fn read_buffered_request_body<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    body_len: usize,
    runtime_state: &NacelleRuntimeState,
    tcp_limits: &NacelleTcpLimits,
) -> Result<NacelleBody, NacelleError>
where
    R: AsyncRead + Unpin,
{
    if body_len == 0 {
        return Ok(NacelleBody::empty());
    }

    let allocation = runtime_state
        .allocate_memory_with_timeout(body_len, runtime_state.limits().memory_allocation_timeout)
        .await?;
    let mut body = BytesMut::with_capacity(body_len);
    if !read_buf.is_empty() {
        let take = body_len.min(read_buf.len());
        body.extend_from_slice(&read_buf.split_to(take));
    }

    while body.len() < body_len {
        let bytes_read = read_buf_with_timeout(reader, &mut body, tcp_limits).await?;
        if bytes_read == 0 {
            return Err(NacelleError::UnexpectedEof);
        }
    }

    Ok(NacelleBody::from_single_chunk(body.freeze(), body_len).with_memory_allocation(allocation))
}

pub(super) async fn pump_request_body<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    body_len: usize,
    tx: mpsc::Sender<Result<Bytes, NacelleError>>,
    config: &NacelleTcpConfig,
    tcp_limits: &NacelleTcpLimits,
) -> Result<(), NacelleError>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = body_len;
    let mut receiver_open = true;

    while remaining > 0 && !read_buf.is_empty() {
        let take = remaining
            .min(read_buf.len())
            .min(config.request_body_chunk_size);
        let chunk = read_buf.split_to(take).freeze();
        remaining -= take;
        if receiver_open && tx.send(Ok(chunk)).await.is_err() {
            receiver_open = false;
        }
    }

    while remaining > 0 {
        let next_len = remaining.min(config.request_body_chunk_size);
        let mut chunk = BytesMut::with_capacity(next_len);
        let bytes_read = match read_buf_with_timeout(reader, &mut chunk, tcp_limits).await {
            Ok(bytes_read) => bytes_read,
            Err(error) => {
                if receiver_open {
                    let _ = tx.send(Err(body_read_error(&error))).await;
                }
                return Err(error);
            }
        };
        if bytes_read == 0 {
            if receiver_open {
                let _ = tx.send(Err(NacelleError::UnexpectedEof)).await;
            }
            return Err(NacelleError::UnexpectedEof);
        }

        remaining -= bytes_read;
        if receiver_open && tx.send(Ok(chunk.freeze())).await.is_err() {
            receiver_open = false;
        }
    }

    Ok(())
}

fn body_read_error(error: &NacelleError) -> NacelleError {
    match error {
        NacelleError::Timeout(name) => NacelleError::Timeout(name),
        NacelleError::Io(error) => {
            NacelleError::Io(std::io::Error::new(error.kind(), error.to_string()))
        }
        _ => NacelleError::Io(std::io::Error::other(error.to_string())),
    }
}

pub(super) fn buffered_request_body(
    read_buf: &mut BytesMut,
    body_len: usize,
    chunk_size: usize,
) -> NacelleBody {
    if body_len == 0 {
        return NacelleBody::empty();
    }

    if body_len <= chunk_size {
        let chunk = read_buf.split_to(body_len).freeze();
        return NacelleBody::from_single_chunk(chunk, body_len);
    }

    let mut remaining = body_len;
    let mut chunks = Vec::with_capacity(body_len.div_ceil(chunk_size));
    while remaining > 0 {
        let take = remaining.min(chunk_size);
        chunks.push(read_buf.split_to(take).freeze());
        remaining -= take;
    }

    NacelleBody::from_buffered(chunks, body_len)
}
