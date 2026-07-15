use std::convert::Infallible;

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_core::telemetry::{NacelleTelemetry, NoopObserver};
use nacelle_tcp::{
    DecodedMessage, DecodedRequest, FrameBuffer, Protocol, TcpRequestContext, TcpResponse,
    TcpServer,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

async fn serve_one_request(telemetry: NacelleTelemetry<NoopObserver>) {
    let (mut client, server_io) = tokio::io::duplex(64);
    let server = TcpServer::<BenchProtocol>::builder()
        .protocol(BenchProtocol)
        .handler(handler_fn(
            |context: TcpRequestContext<BenchProtocol>| async move {
                context.respond(TcpResponse::bytes("x")).await
            },
        ))
        .telemetry(telemetry)
        .build()
        .expect("benchmark server should build");
    let server_task = tokio::spawn(async move { server.serve_io(server_io).await });

    client.write_all(&[0]).await.expect("benchmark write");
    let mut response = [0_u8; 1];
    client
        .read_exact(&mut response)
        .await
        .expect("benchmark response");
    client.shutdown().await.expect("benchmark shutdown");
    server_task
        .await
        .expect("benchmark server join")
        .expect("benchmark server result");
}

fn telemetry_paths(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("tcp_connection_request");

    group.bench_function("facade_no_recorder", |b| {
        b.to_async(&runtime).iter(|| async {
            serve_one_request(NacelleTelemetry::default()).await;
        });
    });
    #[cfg(feature = "phase-timing")]
    group.bench_function("phase_timing_enabled", |b| {
        b.to_async(&runtime).iter(|| async {
            serve_one_request(NacelleTelemetry::default().with_phase_duration_metrics(true)).await;
        });
    });
    group.finish();
}

criterion_group!(benches, telemetry_paths);
criterion_main!(benches);
