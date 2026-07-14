//! Loopback RTT benchmark for the proxy's one-connection-per-request upstream path.

use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nacelle_proxy::wire::{ProxyProtocol, ReferenceProtocolClient};
use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_START};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

const FRAME_HEADER_LEN: usize = 24;
const FRAME_FIELDS_LEN: usize = FRAME_HEADER_LEN - 4;
const PAYLOAD: [u8; 64] = [0xAB; 64];

fn simple_rtt(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let listener = runtime
        .block_on(TcpListener::bind("127.0.0.1:0"))
        .expect("benchmark listener");
    let backend_addr = listener.local_addr().expect("backend address");
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let backend = runtime.spawn(run_backend(listener, shutdown_receiver));
    let client = ReferenceProtocolClient::new(
        backend_addr,
        Duration::from_secs(1),
        Duration::from_secs(1),
        1024,
    );

    {
        let mut group = criterion.benchmark_group("proxy_upstream_simple_rtt");
        group.throughput(Throughput::Elements(1));
        group.warm_up_time(Duration::from_secs(1));
        group.measurement_time(Duration::from_secs(3));
        group.sample_size(20);
        group.bench_function("64_byte_loopback_connect_exchange", |bencher| {
            bencher.to_async(&runtime).iter(|| async {
                let response = client
                    .exchange(black_box(7), black_box(42), black_box(PAYLOAD.as_slice()))
                    .await
                    .expect("benchmark exchange");
                black_box(response);
            });
        });
        group.finish();
    }

    assert!(shutdown.send(()).is_ok(), "backend should still be running");
    runtime.block_on(backend).expect("backend task");
}

async fn run_backend(listener: TcpListener, mut shutdown: oneshot::Receiver<()>) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted.expect("benchmark accept");
                respond(stream).await;
            }
            _ = &mut shutdown => break,
        }
    }
}

async fn respond(mut stream: TcpStream) {
    let mut header = [0_u8; FRAME_HEADER_LEN];
    stream
        .read_exact(&mut header)
        .await
        .expect("request header");
    let frame_len = u32::from_le_bytes(header[0..4].try_into().expect("fixed width")) as usize;
    assert!(frame_len >= FRAME_FIELDS_LEN);
    let request_id = u64::from_le_bytes(header[4..12].try_into().expect("fixed width"));
    let opcode = u64::from_le_bytes(header[12..20].try_into().expect("fixed width"));
    let mut body = vec![0_u8; frame_len - FRAME_FIELDS_LEN];
    stream.read_exact(&mut body).await.expect("request body");

    let response = ProxyProtocol
        .encode_request_frame(request_id, opcode, FRAME_FLAG_START | FRAME_FLAG_END, &body)
        .expect("response frame");
    stream.write_all(&response).await.expect("response");
}

criterion_group!(benches, simple_rtt);
criterion_main!(benches);
