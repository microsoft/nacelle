//! Reference-protocol client and upstream wire exchange.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
/// Protocol adapter used by the proxy's inbound and upstream connections.
pub use nacelle_reference_protocol::LengthDelimitedProtocol as ProxyProtocol;
use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_ERROR, FRAME_FLAG_START};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::ProxyError;

const FRAME_HEADER_LEN: usize = 24;
const FRAME_FIELDS_LEN: usize = FRAME_HEADER_LEN - 4;
const KNOWN_FRAME_FLAGS: u32 = FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR;

#[derive(Debug, Clone, Copy)]
/// Configured client for one reference-protocol upstream exchange.
pub struct ReferenceProtocolClient {
    backend_addr: SocketAddr,
    connect_timeout: Duration,
    io_timeout: Duration,
    max_response_body_bytes: usize,
}

impl ReferenceProtocolClient {
    /// Create a client with explicit connection, exchange deadline, and response bounds.
    #[must_use]
    pub const fn new(
        backend_addr: SocketAddr,
        connect_timeout: Duration,
        io_timeout: Duration,
        max_response_body_bytes: usize,
    ) -> Self {
        Self {
            backend_addr,
            connect_timeout,
            io_timeout,
            max_response_body_bytes,
        }
    }

    /// Send one request and return its complete correlated response body.
    ///
    /// # Errors
    ///
    /// Returns a proxy error when connection or I/O fails, a timeout expires,
    /// the response violates the wire protocol, or its body exceeds the bound.
    pub async fn exchange(
        &self,
        request_id: u64,
        opcode: u64,
        body: &[u8],
    ) -> Result<Bytes, ProxyError> {
        let connect = tokio::time::timeout(
            self.connect_timeout,
            TcpStream::connect(self.backend_addr),
        )
        .await
        .map_err(|_| {
            ProxyError::upstream_connect(
                self.backend_addr,
                std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"),
            )
        })?;
        let mut stream =
            connect.map_err(|error| ProxyError::upstream_connect(self.backend_addr, error))?;

        let frame = ProxyProtocol
            .encode_request_frame(request_id, opcode, FRAME_FLAG_START | FRAME_FLAG_END, body)
            .map_err(|error| {
                ProxyError::upstream_protocol_source("could not encode upstream request", error)
            })?;
        tokio::time::timeout(self.io_timeout, async {
            stream
                .write_all(&frame)
                .await
                .map_err(|error| ProxyError::upstream_io("write", error))?;

            read_response(
                &mut stream,
                request_id,
                opcode,
                self.max_response_body_bytes,
            )
            .await
        })
        .await
        .map_err(|_| ProxyError::upstream_timeout("exchange"))?
    }
}

async fn read_response(
    stream: &mut TcpStream,
    expected_request_id: u64,
    expected_opcode: u64,
    max_body_bytes: usize,
) -> Result<Bytes, ProxyError> {
    let mut response = BytesMut::new();
    let mut first_frame = true;

    loop {
        let mut header = [0_u8; FRAME_HEADER_LEN];
        stream
            .read_exact(&mut header)
            .await
            .map_err(|error| ProxyError::upstream_io("read", error))?;

        let frame_len = u32::from_le_bytes(header[0..4].try_into().expect("fixed width")) as usize;
        if frame_len < FRAME_FIELDS_LEN {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream returned a truncated frame",
            ));
        }
        let request_id = u64::from_le_bytes(header[4..12].try_into().expect("fixed width"));
        let opcode = u64::from_le_bytes(header[12..20].try_into().expect("fixed width"));
        let flags = u32::from_le_bytes(header[20..24].try_into().expect("fixed width"));
        if flags & !KNOWN_FRAME_FLAGS != 0 {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream response used unknown frame flags",
            ));
        }
        if request_id != expected_request_id || opcode != expected_opcode {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream response did not match the request",
            ));
        }
        if first_frame && flags & FRAME_FLAG_START == 0 {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream response did not start a message",
            ));
        }
        if !first_frame && flags & FRAME_FLAG_START != 0 {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream response restarted a message",
            ));
        }
        if flags & FRAME_FLAG_ERROR != 0 {
            return Err(ProxyError::upstream_protocol(
                "proxy upstream returned an error frame",
            ));
        }

        let body_len = frame_len - FRAME_FIELDS_LEN;
        let next_len = response
            .len()
            .checked_add(body_len)
            .ok_or_else(|| ProxyError::response_too_large(usize::MAX, max_body_bytes))?;
        if next_len > max_body_bytes {
            return Err(ProxyError::response_too_large(next_len, max_body_bytes));
        }

        response.reserve(body_len);
        let start = response.len();
        response.put_bytes(0, body_len);
        let response_frame = response.get_mut(start..next_len).ok_or_else(|| {
            ProxyError::upstream_protocol("proxy response buffer length was inconsistent")
        })?;
        stream
            .read_exact(response_frame)
            .await
            .map_err(|error| ProxyError::upstream_io("read", error))?;
        if flags & FRAME_FLAG_END != 0 {
            return Ok(response.freeze());
        }
        first_frame = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProxyErrorKind;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn exchanges_a_request_and_reads_the_matching_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let backend_addr = listener.local_addr().expect("backend address");
        let backend = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut header = [0_u8; FRAME_HEADER_LEN];
            stream
                .read_exact(&mut header)
                .await
                .expect("request header");
            let frame_len =
                u32::from_le_bytes(header[0..4].try_into().expect("fixed width")) as usize;
            let mut body = vec![0_u8; frame_len - FRAME_FIELDS_LEN];
            stream.read_exact(&mut body).await.expect("request body");
            assert_eq!(body, b"hello");

            let response = ProxyProtocol
                .encode_request_frame(7, 42, FRAME_FLAG_START | FRAME_FLAG_END, b"proxied")
                .expect("response frame");
            stream.write_all(&response).await.expect("response");
        });
        let client = ReferenceProtocolClient::new(
            backend_addr,
            Duration::from_secs(1),
            Duration::from_secs(1),
            1024,
        );

        let response = client
            .exchange(7, 42, b"hello")
            .await
            .expect("proxied response");

        assert_eq!(response, Bytes::from_static(b"proxied"));
        backend.await.expect("backend task");
    }

    #[tokio::test]
    async fn rejects_unknown_response_flags() {
        let response = ProxyProtocol
            .encode_request_frame(7, 42, FRAME_FLAG_START | FRAME_FLAG_END | (1 << 31), b"")
            .expect("response frame");

        let error = exchange_with_response(response, 1024)
            .await
            .expect_err("unknown flag should be rejected");

        assert_eq!(error.kind(), ProxyErrorKind::UpstreamProtocol);
    }

    #[tokio::test]
    async fn rejects_a_repeated_start_flag() {
        let first = ProxyProtocol
            .encode_request_frame(7, 42, FRAME_FLAG_START, b"first")
            .expect("first response frame");
        let second = ProxyProtocol
            .encode_request_frame(7, 42, FRAME_FLAG_START | FRAME_FLAG_END, b"second")
            .expect("second response frame");
        let mut response = BytesMut::with_capacity(first.len() + second.len());
        response.extend_from_slice(&first);
        response.extend_from_slice(&second);

        let error = exchange_with_response(response.freeze(), 1024)
            .await
            .expect_err("repeated start should be rejected");

        assert_eq!(error.kind(), ProxyErrorKind::UpstreamProtocol);
    }

    #[tokio::test]
    async fn rejects_oversized_and_error_frames_before_reading_their_bodies() {
        let oversized = response_header(7, 42, FRAME_FLAG_START | FRAME_FLAG_END, 9);
        let oversized_error = exchange_with_response(oversized, 8)
            .await
            .expect_err("oversized response should be rejected");
        assert_eq!(oversized_error.kind(), ProxyErrorKind::ResponseTooLarge);

        let upstream_error = response_header(
            7,
            42,
            FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR,
            1024,
        );
        let protocol_error = exchange_with_response(upstream_error, 8)
            .await
            .expect_err("error response should be rejected from its header");
        assert_eq!(protocol_error.kind(), ProxyErrorKind::UpstreamProtocol);
    }

    async fn exchange_with_response(
        response: Bytes,
        max_response_body_bytes: usize,
    ) -> Result<Bytes, ProxyError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let backend_addr = listener.local_addr().expect("backend address");
        let backend = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut header = [0_u8; FRAME_HEADER_LEN];
            stream
                .read_exact(&mut header)
                .await
                .expect("request header");
            let frame_len =
                u32::from_le_bytes(header[0..4].try_into().expect("fixed width")) as usize;
            let mut body = vec![0_u8; frame_len - FRAME_FIELDS_LEN];
            stream.read_exact(&mut body).await.expect("request body");
            stream.write_all(&response).await.expect("response");
        });
        let client = ReferenceProtocolClient::new(
            backend_addr,
            Duration::from_secs(1),
            Duration::from_secs(1),
            max_response_body_bytes,
        );

        let result = client.exchange(7, 42, b"request").await;
        backend.await.expect("backend task");
        result
    }

    fn response_header(request_id: u64, opcode: u64, flags: u32, body_len: usize) -> Bytes {
        let frame_len = u32::try_from(FRAME_FIELDS_LEN + body_len).expect("frame length");
        let mut header = BytesMut::with_capacity(FRAME_HEADER_LEN);
        header.put_u32_le(frame_len);
        header.put_u64_le(request_id);
        header.put_u64_le(opcode);
        header.put_u32_le(flags);
        header.freeze()
    }
}
