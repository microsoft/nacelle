use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_core::telemetry::NacelleTelemetry;
use nacelle_tcp::{
    DecodedMessage, DecodedRequest, FrameBuffer, NacelleTcpConfig, Protocol, ResponseWritePolicy,
    TcpRequestContext, TcpResponse, TcpServer,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const REQUESTS: usize = 64;
const RESPONSE_BYTES: usize = 32;

struct Decoder;

impl MessageDecoder for Decoder {
    type Message = DecodedMessage<(), Infallible>;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if input.is_empty() {
            return Ok(None);
        }
        let _ = input.split_to(1);
        Ok(Some(DecodedMessage::Request(DecodedRequest {
            request: (),
            body_len: 0,
        })))
    }
}

#[derive(Clone, Copy)]
struct BenchProtocol;

impl Protocol for BenchProtocol {
    type Request = ();
    type OneWayRequest = Infallible;
    type Response = TcpResponse;
    type ConnectionState = ();
    type Decoder = Decoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
        Decoder
    }
    fn connection_state(&self, _connection: &ConnectionInfo) {}
    fn request_wire_bytes(&self, _request: &(), _body_len: usize) -> usize {
        1
    }
    fn one_way_wire_bytes(&self, request: &Infallible, _body_len: usize) -> usize {
        match *request {}
    }
    fn response_context(&self, _request: &()) {}
    fn error_context(&self, _request: &()) {}
    fn apply_response(&self, _context: &mut (), _response: &TcpResponse) {}
    fn max_response_frame_overhead(&self) -> usize {
        0
    }
    fn response_body(&self, response: TcpResponse) -> nacelle_core::NacelleBody {
        response.body
    }
    fn encode_response_chunk(
        &self,
        _context: &mut (),
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)
    }
    fn encode_response_terminal_chunk(
        &self,
        context: &mut (),
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }
    fn encode_response_end(
        &self,
        _context: &mut (),
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
    fn encode_error(
        &self,
        _context: Option<&()>,
        _error: &NacelleError,
        _dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        Ok(())
    }
}

struct BenchmarkIo {
    input: [u8; REQUESTS],
    position: usize,
    writes: Arc<AtomicUsize>,
}

impl AsyncRead for BenchmarkIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.position >= self.input.len() {
            return Poll::Ready(Ok(()));
        }
        let available = &self.input[self.position..];
        let take = available.len().min(buf.remaining());
        buf.put_slice(&available[..take]);
        self.position += take;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for BenchmarkIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

async fn run(policy: ResponseWritePolicy, response_buffer_capacity: usize, expected_writes: usize) {
    let writes = Arc::new(AtomicUsize::new(0));
    let io = BenchmarkIo {
        input: [0; REQUESTS],
        position: 0,
        writes: writes.clone(),
    };
    let server = TcpServer::<BenchProtocol>::builder()
        .protocol(BenchProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<BenchProtocol>| async move {
                context
                    .respond(TcpResponse::bytes(Bytes::from_static(&[0; RESPONSE_BYTES])))
                    .await
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_buffer_capacity(response_buffer_capacity)
                .with_response_write_policy(policy),
        )
        .telemetry(NacelleTelemetry::default().with_metrics(false))
        .build()
        .expect("benchmark server should build");

    server
        .serve_io(io)
        .await
        .expect("benchmark server should run");
    assert_eq!(writes.load(Ordering::Relaxed), expected_writes);
}

fn response_delivery(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("tcp_response_delivery_64x32_bytes");

    group.bench_function("immediate", |b| {
        b.to_async(&runtime)
            .iter(|| run(ResponseWritePolicy::Immediate, 2_048, REQUESTS));
    });
    group.bench_function("coalesce_buffered", |b| {
        b.to_async(&runtime)
            .iter(|| run(ResponseWritePolicy::CoalesceBuffered, 2_048, 1));
    });
    group.bench_function("flush_at_bytes_1024", |b| {
        b.to_async(&runtime)
            .iter(|| run(ResponseWritePolicy::FlushAtBytes(1_024), 1_024, 2));
    });
    group.bench_function("flush_at_bytes_2048_grows_from_1024", |b| {
        b.to_async(&runtime)
            .iter(|| run(ResponseWritePolicy::FlushAtBytes(2_048), 1_024, 1));
    });
    group.finish();
}

criterion_group!(benches, response_delivery);
criterion_main!(benches);
