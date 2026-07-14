//! Socket-free benchmark for the proxy reference protocol's framing work.

use std::hint::black_box;

use bytes::BytesMut;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use nacelle::codec::MessageDecoder;
use nacelle::tcp::{DecodedMessage, Protocol};
use nacelle_proxy::wire::ProxyProtocol;
use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_START};

const FRAME_COUNT: usize = 64;
const FRAME_HEADER_LEN: usize = 24;
const PAYLOAD_LEN: usize = 256;
const MAX_FRAME_LEN: usize = 1024;
const PAYLOAD: [u8; PAYLOAD_LEN] = [0xAB; PAYLOAD_LEN];

fn protocol_efficiency(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("proxy_protocol_efficiency");
    group.throughput(Throughput::Bytes(
        u64::try_from(FRAME_COUNT * PAYLOAD_LEN).expect("benchmark byte count"),
    ));
    group.bench_function("64x256_byte_encode_decode", |bencher| {
        bencher.iter(|| {
            let mut encoded =
                BytesMut::with_capacity(FRAME_COUNT * (FRAME_HEADER_LEN + PAYLOAD_LEN));
            for request_id in 0..FRAME_COUNT {
                let frame = ProxyProtocol
                    .encode_request_frame(
                        u64::try_from(request_id).expect("benchmark request id"),
                        42,
                        FRAME_FLAG_START | FRAME_FLAG_END,
                        black_box(PAYLOAD.as_slice()),
                    )
                    .expect("benchmark encode");
                encoded.extend_from_slice(&frame);
            }

            let mut parser = ProxyProtocol.decoder(MAX_FRAME_LEN);
            for _ in 0..FRAME_COUNT {
                let message = parser
                    .decode(&mut encoded)
                    .expect("benchmark decode")
                    .expect("benchmark frame");
                let request = match message {
                    DecodedMessage::Request(request) => request,
                    DecodedMessage::OneWay(one_way) => match one_way.request {},
                };
                black_box(request.request);
                black_box(encoded.split_to(request.body_len));
            }
            assert!(encoded.is_empty());
        });
    });
    group.finish();
}

criterion_group!(benches, protocol_efficiency);
criterion_main!(benches);
