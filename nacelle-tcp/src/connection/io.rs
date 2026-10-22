use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReader};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::framing::MessageReadFailure;
use crate::limits::NacelleTcpLimits;
use nacelle_core::error::{NacelleError, NacelleTimeoutReason};

const TCP_READ_TIMEOUT: NacelleTimeoutReason = NacelleTimeoutReason::TcpRead;
const REQUEST_BODY_READ_TIMEOUT: NacelleTimeoutReason = NacelleTimeoutReason::RequestBodyRead;
const TCP_WRITE_TIMEOUT: NacelleTimeoutReason = NacelleTimeoutReason::TcpWrite;
const TCP_SHUTDOWN_TIMEOUT: NacelleTimeoutReason = NacelleTimeoutReason::TcpShutdown;

pub(super) async fn read_message_with_timeout<R, D>(
    reader: &mut MessageReader<R, D>,
    tcp_limits: &NacelleTcpLimits,
) -> Result<Option<D::Message>, MessageReadFailure>
where
    R: AsyncRead + Unpin,
    D: MessageDecoder<Error = NacelleError>,
{
    if reader.buffer().is_empty() {
        reader
            .decode_buffered()
            .map_err(MessageReadFailure::from_message_read)?;
        let first_read = reader.read_more();
        let result = if let Some(timeout) = tcp_limits.idle_timeout {
            tokio::time::timeout(timeout, first_read)
                .await
                .map_err(|_| {
                    MessageReadFailure::transport(NacelleError::Timeout(NacelleTimeoutReason::Idle))
                })?
        } else {
            first_read.await
        };
        result.map_err(|error| MessageReadFailure::transport(NacelleError::from(error)))?;
    }

    let future = reader.read_message();
    let result = if let Some(timeout) = tcp_limits.read_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| MessageReadFailure::transport(NacelleError::Timeout(TCP_READ_TIMEOUT)))?
    } else {
        future.await
    };
    result.map_err(MessageReadFailure::from_message_read)
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
    if let Some(timeout) = tcp_limits.read_timeout {
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
    if let Some(timeout) = tcp_limits.shutdown_timeout {
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

    struct FixedDecoder(usize);

    impl MessageDecoder for FixedDecoder {
        type Message = bytes::Bytes;
        type Error = NacelleError;

        fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
            Ok((input.len() >= self.0).then(|| input.split_to(self.0).freeze()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_is_independent_of_read_timeout() {
        for limits in [
            NacelleTcpLimits::default(),
            NacelleTcpLimits::default().without_read_timeout(),
        ] {
            let (_client, stream) = tokio::io::duplex(64);
            let mut reader = MessageReader::new(stream, FixedDecoder(2));
            let started = tokio::time::Instant::now();

            let result = read_message_with_timeout(&mut reader, &limits).await;

            let failure = result.expect_err("silent connection should time out");
            assert!(!failure.should_encode());
            assert!(matches!(
                failure.into_error(),
                NacelleError::Timeout(NacelleTimeoutReason::Idle)
            ));
            assert_eq!(started.elapsed(), Duration::from_secs(120));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn read_deadline_starts_at_first_byte_and_idle_restarts_between_messages() {
        let (mut client, stream) = tokio::io::duplex(64);
        let mut reader = MessageReader::new(stream, FixedDecoder(2));
        let limits = NacelleTcpLimits::default();
        let started = tokio::time::Instant::now();
        let send = async {
            tokio::time::sleep(Duration::from_secs(110)).await;
            client.write_all(b"a").await.expect("first byte");
            tokio::time::sleep(Duration::from_secs(25)).await;
            client.write_all(b"b").await.expect("second byte");
        };

        let (result, ()) = tokio::join!(read_message_with_timeout(&mut reader, &limits), send);
        assert_eq!(result.ok().flatten().expect("message"), &b"ab"[..]);
        assert_eq!(started.elapsed(), Duration::from_secs(135));

        let failure = read_message_with_timeout(&mut reader, &limits)
            .await
            .expect_err("next idle wait should time out");
        assert!(matches!(
            failure.into_error(),
            NacelleError::Timeout(NacelleTimeoutReason::Idle)
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(255));
    }

    #[tokio::test(start_paused = true)]
    async fn trickled_bytes_do_not_restart_the_message_deadline() {
        let (mut client, stream) = tokio::io::duplex(64);
        let mut reader = MessageReader::new(stream, FixedDecoder(4));
        let limits = NacelleTcpLimits::default();
        let started = tokio::time::Instant::now();
        let send = async {
            tokio::time::sleep(Duration::from_secs(100)).await;
            client.write_all(b"a").await.expect("first byte");
            for byte in [b"b", b"c"] {
                tokio::time::sleep(Duration::from_secs(10)).await;
                client.write_all(byte).await.expect("trickled byte");
            }
        };

        let (result, ()) = tokio::join!(read_message_with_timeout(&mut reader, &limits), send);
        let failure = result.expect_err("incomplete message should time out");
        assert!(!failure.should_encode());
        assert!(matches!(
            failure.into_error(),
            NacelleError::Timeout(NacelleTimeoutReason::TcpRead)
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(130));
        assert_eq!(&reader.buffer()[..], b"abc");
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_partial_message_uses_only_the_read_deadline() {
        let (_client, stream) = tokio::io::duplex(64);
        let mut reader =
            MessageReader::with_buffer(stream, FixedDecoder(2), BytesMut::from(&b"a"[..]));
        let limits = NacelleTcpLimits::default().with_idle_timeout(Duration::from_secs(5));
        let started = tokio::time::Instant::now();

        let failure = read_message_with_timeout(&mut reader, &limits)
            .await
            .expect_err("partial timeout");
        assert!(matches!(
            failure.into_error(),
            NacelleError::Timeout(NacelleTimeoutReason::TcpRead)
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn disabling_idle_timeout_still_bounds_partial_messages() {
        let (mut client, stream) = tokio::io::duplex(64);
        let mut reader = MessageReader::new(stream, FixedDecoder(2));
        let limits = NacelleTcpLimits::default().without_idle_timeout();
        let started = tokio::time::Instant::now();
        let send = async {
            tokio::time::sleep(Duration::from_secs(150)).await;
            client.write_all(b"a").await.expect("first byte");
        };

        let (result, ()) = tokio::join!(read_message_with_timeout(&mut reader, &limits), send);
        let failure = result.expect_err("read timeout remains enabled");
        assert!(matches!(
            failure.into_error(),
            NacelleError::Timeout(NacelleTimeoutReason::TcpRead)
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(180));
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_read_timeout_does_not_fall_back_to_idle_timeout() {
        for limits in [
            NacelleTcpLimits::default().without_read_timeout(),
            NacelleTcpLimits::default()
                .without_read_timeout()
                .without_idle_timeout(),
        ] {
            let (mut client, stream) = tokio::io::duplex(64);
            let mut reader = MessageReader::new(stream, FixedDecoder(2));
            let started = tokio::time::Instant::now();
            let first_byte_delay = if limits.idle_timeout.is_some() {
                1
            } else {
                150
            };
            let send = async {
                tokio::time::sleep(Duration::from_secs(first_byte_delay)).await;
                client.write_all(b"a").await.expect("first byte");
                tokio::time::sleep(Duration::from_secs(150)).await;
                client.write_all(b"b").await.expect("second byte");
            };

            let (result, ()) = tokio::join!(read_message_with_timeout(&mut reader, &limits), send);
            assert_eq!(result.ok().flatten().expect("unbounded read"), &b"ab"[..]);
            assert_eq!(
                started.elapsed(),
                Duration::from_secs(first_byte_delay + 150)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_messages_and_eof_do_not_wait_for_a_deadline() {
        let (mut client, stream) = tokio::io::duplex(64);
        let mut reader =
            MessageReader::with_buffer(stream, FixedDecoder(2), BytesMut::from(&b"abcd"[..]));
        let limits = NacelleTcpLimits::default();
        let started = tokio::time::Instant::now();

        for expected in [b"ab", b"cd"] {
            let result = read_message_with_timeout(&mut reader, &limits).await;
            assert_eq!(
                result.ok().flatten().expect("buffered message"),
                &expected[..]
            );
        }
        client.shutdown().await.expect("shutdown");
        for _ in 0..2 {
            assert!(matches!(
                read_message_with_timeout(&mut reader, &limits).await,
                Ok(None)
            ));
        }
        assert_eq!(started.elapsed(), Duration::ZERO);

        let (mut client, stream) = tokio::io::duplex(64);
        let mut reader = MessageReader::new(stream, FixedDecoder(2));
        client.write_all(b"a").await.expect("partial message");
        client.shutdown().await.expect("shutdown");
        let failure = read_message_with_timeout(&mut reader, &limits)
            .await
            .expect_err("incomplete EOF");
        assert!(failure.should_encode());
        assert!(matches!(failure.into_error(), NacelleError::UnexpectedEof));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_input_decoder_contract_errors_do_not_wait_for_idle_timeout() {
        let (_client, stream) = tokio::io::duplex(64);
        let mut reader = MessageReader::new(stream, FixedDecoder(0));
        let limits = NacelleTcpLimits::default();
        let started = tokio::time::Instant::now();

        let failure = read_message_with_timeout(&mut reader, &limits)
            .await
            .expect_err("contract error");
        assert!(failure.should_encode());
        assert!(matches!(
            failure.into_error(),
            NacelleError::InvalidFrame("decoder returned a request without consuming input")
        ));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn body_reads_use_read_timeout_without_idle_fallback() {
        let (_client, mut stream) = tokio::io::duplex(64);
        let mut buffer = BytesMut::with_capacity(64);
        let limits = NacelleTcpLimits::default().with_idle_timeout(Duration::from_secs(5));
        let started = tokio::time::Instant::now();

        let result = read_buf_with_timeout(&mut stream, &mut buffer, &limits).await;
        assert!(matches!(
            result,
            Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(30));

        let limits = limits.without_read_timeout();
        let (mut client, mut stream) = tokio::io::duplex(64);
        let started = tokio::time::Instant::now();
        let send = async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            client.write_all(b"body").await.expect("body bytes");
        };
        let (result, ()) = tokio::join!(
            read_buf_with_timeout(&mut stream, &mut buffer, &limits),
            send
        );
        assert_eq!(result.expect("unbounded body read"), 4);
        assert_eq!(&buffer[..], b"body");
        assert_eq!(started.elapsed(), Duration::from_secs(60));
    }

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
    async fn shutdown_completes_and_honors_shutdown_timeout() {
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
        let limits = NacelleTcpLimits::default()
            .with_write_timeout(Duration::from_secs(1))
            .with_shutdown_timeout(Duration::from_millis(10));
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
