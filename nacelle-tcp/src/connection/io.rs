use bytes::BytesMut;
use nacelle_codec::{MessageDecoder, MessageReader};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::framing::map_message_read_error;
use crate::limits::NacelleTcpLimits;
use nacelle_core::error::NacelleError;

pub(super) async fn read_message_with_timeout<R, D>(
    reader: &mut MessageReader<R, D>,
    tcp_limits: &NacelleTcpLimits,
    name: &'static str,
) -> Result<Option<D::Message>, NacelleError>
where
    R: AsyncRead + Unpin,
    D: MessageDecoder<Error = NacelleError>,
{
    let future = reader.read_message();
    let result = if let Some(timeout) = tcp_limits.read_timeout.or(tcp_limits.idle_timeout) {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(name))?
    } else {
        future.await
    };
    result.map_err(map_message_read_error)
}

pub(super) async fn read_buf_with_timeout<R>(
    reader: &mut R,
    buf: &mut BytesMut,
    tcp_limits: &NacelleTcpLimits,
    name: &'static str,
) -> Result<usize, NacelleError>
where
    R: AsyncRead + Unpin,
{
    let future = reader.read_buf(buf);
    if let Some(timeout) = tcp_limits.read_timeout.or(tcp_limits.idle_timeout) {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(name))?
            .map_err(NacelleError::from)
    } else {
        future.await.map_err(NacelleError::from)
    }
}

pub(super) async fn write_all_with_timeout<W>(
    writer: &mut W,
    buf: &[u8],
    tcp_limits: &NacelleTcpLimits,
    name: &'static str,
) -> Result<(), NacelleError>
where
    W: AsyncWrite + Unpin,
{
    let future = writer.write_all(buf);
    if let Some(timeout) = tcp_limits.write_timeout {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(name))?
            .map_err(NacelleError::from)
    } else {
        future.await.map_err(NacelleError::from)
    }
}
