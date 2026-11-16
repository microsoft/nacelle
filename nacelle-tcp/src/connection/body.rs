use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

use crate::config::NacelleTcpConfig;
use crate::limits::NacelleTcpLimits;
use nacelle_core::error::{NacelleError, NacelleTimeoutReason};
use nacelle_core::request::NacelleBody;

use super::io::{read_buf_before_deadline, request_body_read_deadline};

pub(super) async fn read_buffered_request_body<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    body_len: usize,
    bounded_reads: bool,
    tcp_limits: &NacelleTcpLimits,
) -> Result<NacelleBody, NacelleError>
where
    R: AsyncRead + Unpin,
{
    if body_len == 0 {
        return Ok(NacelleBody::empty());
    }
    let deadline = request_body_read_deadline(tcp_limits);

    let mut body = BytesMut::with_capacity(body_len);
    if !read_buf.is_empty() {
        let take = body_len.min(read_buf.len());
        body.extend_from_slice(&read_buf.split_to(take));
    }

    while body.len() < body_len {
        let bytes_read = if bounded_reads {
            let remaining = body_len - body.len();
            read_buf_before_deadline(reader, &mut (&mut body).limit(remaining), deadline).await?
        } else {
            read_buf_before_deadline(reader, &mut body, deadline).await?
        };
        if bytes_read == 0 {
            return Err(NacelleError::UnexpectedEof);
        }
    }

    Ok(NacelleBody::from_single_chunk(body.freeze(), body_len))
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
    let future = pump_request_body_chunks(reader, read_buf, body_len, &tx, config);
    let result = if let Some(deadline) = request_body_read_deadline(tcp_limits) {
        tokio::time::timeout_at(deadline, future)
            .await
            .unwrap_or(Err(NacelleError::Timeout(
                NacelleTimeoutReason::RequestBodyRead,
            )))
    } else {
        future.await
    };
    if let Err(error) = &result {
        // The driver must receive the failure even if the body queue is full.
        let _ = tx.try_send(Err(body_read_error(error)));
    }
    result
}

async fn pump_request_body_chunks<R>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    body_len: usize,
    tx: &mpsc::Sender<Result<Bytes, NacelleError>>,
    config: &NacelleTcpConfig,
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
        let bytes_read = if config.head_read_limits.is_some() {
            reader.read_buf(&mut (&mut chunk).limit(next_len)).await?
        } else {
            reader.read_buf(&mut chunk).await?
        };
        if bytes_read == 0 {
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
        NacelleError::UnexpectedEof => NacelleError::UnexpectedEof,
        NacelleError::Timeout(reason) => NacelleError::Timeout(*reason),
        NacelleError::ResourceLimit(reason) => NacelleError::ResourceLimit(*reason),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn body_failure_does_not_wait_for_error_channel_capacity() {
        for timeout in [true, false] {
            for full in [true, false] {
                let mut reader = &b""[..];
                let mut buffer = if full {
                    BytesMut::from(&b"x"[..])
                } else {
                    BytesMut::new()
                };
                let (tx, mut rx) = mpsc::channel(1);
                let config = NacelleTcpConfig::default().with_request_body_chunk_size(1);
                let limits = if timeout {
                    NacelleTcpLimits::default()
                } else {
                    NacelleTcpLimits::default().without_read_timeout()
                };
                let started = tokio::time::Instant::now();

                let result = tokio::time::timeout(
                    Duration::from_secs(1),
                    pump_request_body(&mut reader, &mut buffer, 2, tx, &config, &limits),
                )
                .await
                .expect("body failure must reach the driver even when the channel is full");

                assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
                assert_eq!(started.elapsed(), Duration::ZERO);
                if full {
                    assert_eq!(rx.recv().await.unwrap().unwrap(), &b"x"[..]);
                } else {
                    assert!(matches!(
                        rx.recv().await,
                        Some(Err(NacelleError::UnexpectedEof))
                    ));
                }
                assert!(rx.recv().await.is_none());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn body_channel_progress_does_not_restart_deadline() {
        for buffered in [true, false] {
            for timeout in [true, false] {
                let mut reader = if buffered { &b""[..] } else { &b"abc"[..] };
                let mut buffer = if buffered {
                    BytesMut::from(&b"abc"[..])
                } else {
                    BytesMut::new()
                };
                let (tx, mut rx) = mpsc::channel(1);
                let config = NacelleTcpConfig::default().with_request_body_chunk_size(1);
                let limits = if timeout {
                    NacelleTcpLimits::default()
                } else {
                    NacelleTcpLimits::default().without_read_timeout()
                };
                let started = tokio::time::Instant::now();
                let pump = async {
                    let result =
                        pump_request_body(&mut reader, &mut buffer, 3, tx, &config, &limits).await;
                    (result, started.elapsed())
                };
                let consume = async {
                    let mut received = Vec::new();
                    loop {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        match rx.recv().await {
                            Some(chunk) => received.extend_from_slice(&chunk.unwrap()),
                            None => break,
                        }
                    }
                    received
                };

                let ((result, elapsed), received) = tokio::join!(pump, consume);

                if timeout {
                    assert!(matches!(
                        result,
                        Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
                    ));
                    assert_eq!(elapsed, Duration::from_secs(30));
                    assert_eq!(received, b"ab");
                } else {
                    result.expect("disabled deadline allows the consumer to drain slowly");
                    assert_eq!(elapsed, Duration::from_secs(40));
                    assert_eq!(received, b"abc");
                }
            }
        }
    }
}
