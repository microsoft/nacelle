use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReader};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::framing::map_message_read_error;
use crate::limits::NacelleTcpLimits;
use nacelle_core::error::NacelleError;

const TCP_READ_TIMEOUT: &str = "tcp_read";
const REQUEST_BODY_READ_TIMEOUT: &str = "request_body_read";
const TCP_WRITE_TIMEOUT: &str = "tcp_write";
const TCP_SHUTDOWN_TIMEOUT: &str = "tcp_shutdown";

pub(super) async fn read_message_with_timeout<R, D>(
    reader: &mut MessageReader<R, D>,
    tcp_limits: &NacelleTcpLimits,
) -> Result<Option<D::Message>, NacelleError>
where
    R: AsyncRead + Unpin,
    D: MessageDecoder<Error = NacelleError>,
{
    let future = reader.read_message();
    let result = if let Some(timeout) = tcp_limits.read_timeout.or(tcp_limits.idle_timeout) {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(TCP_READ_TIMEOUT))?
    } else {
        future.await
    };
    result.map_err(map_message_read_error)
}

pub(super) async fn read_buf_with_timeout<R>(
    reader: &mut R,
    buf: &mut BytesMut,
    tcp_limits: &NacelleTcpLimits,
) -> Result<usize, NacelleError>
where
    R: AsyncRead + Unpin,
{
    let future = reader.read_buf(buf);
    if let Some(timeout) = tcp_limits.read_timeout.or(tcp_limits.idle_timeout) {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(REQUEST_BODY_READ_TIMEOUT))?
            .map_err(NacelleError::from)
    } else {
        future.await.map_err(NacelleError::from)
    }
}

pub(super) async fn write_all_tracked_with_timeout<W>(
    writer: &mut W,
    buf: &[u8],
    tcp_limits: &NacelleTcpLimits,
) -> Result<usize, (NacelleError, usize)>
where
    W: AsyncWrite + Unpin,
{
    async fn write_loop<W>(
        writer: &mut W,
        mut buf: &[u8],
        written: &mut usize,
    ) -> Result<(), NacelleError>
    where
        W: AsyncWrite + Unpin,
    {
        while !buf.is_empty() {
            let bytes = writer.write(buf).await.map_err(NacelleError::from)?;
            if bytes == 0 {
                return Err(NacelleError::ConnectionClosed);
            }
            *written = written.saturating_add(bytes);
            buf = buf.get(bytes..).unwrap_or_default();
        }
        Ok(())
    }

    let mut written = 0_usize;
    if let Some(timeout) = tcp_limits.write_timeout {
        match tokio::time::timeout(timeout, write_loop(writer, buf, &mut written)).await {
            Ok(Ok(())) => Ok(written),
            Ok(Err(error)) => Err((error, written)),
            Err(_) => Err((NacelleError::Timeout(TCP_WRITE_TIMEOUT), written)),
        }
    } else {
        write_loop(writer, buf, &mut written)
            .await
            .map(|()| written)
            .map_err(|error| (error, written))
    }
}

pub(super) async fn shutdown_with_timeout<W>(
    writer: &mut W,
    tcp_limits: &NacelleTcpLimits,
) -> Result<(), NacelleError>
where
    W: AsyncWrite + Unpin,
{
    let future = writer.shutdown();
    if let Some(timeout) = tcp_limits.write_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(TCP_SHUTDOWN_TIMEOUT))?
            .map_err(NacelleError::from)
    } else {
        future.await.map_err(NacelleError::from)
    }
}

pub(super) async fn flush_with_timeout<W>(
    writer: &mut W,
    tcp_limits: &NacelleTcpLimits,
) -> Result<(), NacelleError>
where
    W: AsyncWrite + Unpin,
{
    let future = writer.flush();
    if let Some(timeout) = tcp_limits.write_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(TCP_WRITE_TIMEOUT))?
            .map_err(NacelleError::from)
    } else {
        future.await.map_err(NacelleError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use tokio::io::AsyncWrite;

    use super::*;

    struct PartialThenPending {
        wrote: bool,
    }

    struct ShutdownWriter {
        shutdown: Arc<AtomicBool>,
        pending: bool,
    }

    struct FlushWriter {
        flushed: Arc<AtomicBool>,
        pending: bool,
    }

    impl AsyncWrite for FlushWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            if self.pending {
                Poll::Pending
            } else {
                self.flushed.store(true, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for ShutdownWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            if self.pending {
                Poll::Pending
            } else {
                self.shutdown.store(true, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
        }
    }

    impl AsyncWrite for PartialThenPending {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.wrote {
                return Poll::Pending;
            }
            self.wrote = true;
            Poll::Ready(Ok(buf.len().min(3)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn whole_frame_timeout_preserves_partial_progress() {
        let mut writer = PartialThenPending { wrote: false };
        let limits = NacelleTcpLimits::default().with_write_timeout(Duration::from_millis(10));

        let result = write_all_tracked_with_timeout(&mut writer, b"abcdef", &limits).await;

        assert!(matches!(
            result,
            Err((NacelleError::Timeout(TCP_WRITE_TIMEOUT), 3))
        ));
    }

    #[tokio::test]
    async fn shutdown_completes_and_honors_write_timeout() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut writer = ShutdownWriter {
            shutdown: shutdown.clone(),
            pending: false,
        };
        shutdown_with_timeout(&mut writer, &NacelleTcpLimits::default())
            .await
            .expect("shutdown should complete");
        assert!(shutdown.load(Ordering::Relaxed));

        let mut pending_writer = ShutdownWriter {
            shutdown,
            pending: true,
        };
        let limits = NacelleTcpLimits::default().with_write_timeout(Duration::from_millis(10));
        let result = shutdown_with_timeout(&mut pending_writer, &limits).await;

        assert!(matches!(
            result,
            Err(NacelleError::Timeout(TCP_SHUTDOWN_TIMEOUT))
        ));
    }

    #[tokio::test]
    async fn flush_completes_and_honors_write_timeout() {
        let flushed = Arc::new(AtomicBool::new(false));
        let mut writer = FlushWriter {
            flushed: flushed.clone(),
            pending: false,
        };
        flush_with_timeout(&mut writer, &NacelleTcpLimits::default())
            .await
            .expect("flush should complete");
        assert!(flushed.load(Ordering::Relaxed));

        let mut pending_writer = FlushWriter {
            flushed,
            pending: true,
        };
        let limits = NacelleTcpLimits::default().with_write_timeout(Duration::from_millis(10));
        let result = flush_with_timeout(&mut pending_writer, &limits).await;

        assert!(matches!(
            result,
            Err(NacelleError::Timeout(TCP_WRITE_TIMEOUT))
        ));
    }
}
