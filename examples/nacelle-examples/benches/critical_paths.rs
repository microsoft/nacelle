use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nacelle::codec::{MessageDecoder, MessageReader};
use nacelle::core::{
    NacelleInMemoryObserver, NacelleLimits, NacellePeerRateLimiter, NacelleRuntimeState,
    NacelleTelemetry, NacelleTransport,
};
use nacelle::tcp::{FrameBuffer, Protocol};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};
use std::hint::black_box;
use std::net::IpAddr;
use std::time::Duration;

const PIPELINE_REQUESTS: usize = 64;
const PIPELINE_BODY_LEN: usize = 32;

fn protocol_frame_benches(c: &mut Criterion) {
    let protocol = LengthDelimitedProtocol;
    let req = FrameRequest {
        request_id: 7,
        opcode: 42,
        flags: 0,
        body_len: 4096,
    };
    let small_body = [0xAB; 32];
    let large_body = vec![0xCD; 4096];
    let small_frame = protocol
        .encode_request_frame(7, 42, 0, &small_body)
        .expect("small frame should encode");
    let large_frame = protocol
        .encode_request_frame(7, 42, 0, &large_body)
        .expect("large frame should encode");
    let response_chunk = Bytes::copy_from_slice(&large_body);

    let mut group = c.benchmark_group("protocol_frames");
    group.bench_function("decoder_factory", |b| {
        b.iter(|| black_box(protocol.decoder(black_box(1024))))
    });
    group.bench_function("encode_request_small", |b| {
        b.iter(|| {
            black_box(
                protocol
                    .encode_request_frame(7, 42, 0, black_box(&small_body))
                    .expect("frame should encode"),
            )
        })
    });
    group.bench_function("encode_request_large", |b| {
        b.iter(|| {
            black_box(
                protocol
                    .encode_request_frame(7, 42, 0, black_box(large_body.as_slice()))
                    .expect("frame should encode"),
            )
        })
    });
    group.bench_function("decode_request_small", |b| {
        b.iter_batched(
            || (protocol.decoder(1024), BytesMut::from(small_frame.as_ref())),
            |(mut decoder, mut buf)| {
                let decoded = decoder
                    .decode(&mut buf)
                    .expect("decode should succeed")
                    .expect("head should be present");
                black_box(decoded)
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("decode_request_large", |b| {
        b.iter_batched(
            || (protocol.decoder(8192), BytesMut::from(large_frame.as_ref())),
            |(mut decoder, mut buf)| {
                let decoded = decoder
                    .decode(&mut buf)
                    .expect("decode should succeed")
                    .expect("head should be present");
                black_box(decoded)
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("decode_request_need_more", |b| {
        b.iter_batched(
            || (protocol.decoder(1024), BytesMut::from(&small_frame[..3])),
            |(mut decoder, mut buf)| {
                black_box(decoder.decode(&mut buf).expect("decode should succeed"))
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function(BenchmarkId::new("encode_response_chunk", "4k"), |b| {
        b.iter_batched(
            || {
                (
                    protocol.response_context(&req),
                    BytesMut::with_capacity(8192),
                )
            },
            |(mut context, mut dst)| {
                let frame_capacity = protocol.max_response_frame_overhead() + response_chunk.len();
                let mut frame = FrameBuffer::new(&mut dst, frame_capacity);
                protocol
                    .encode_response_chunk(&mut context, response_chunk.clone(), &mut frame)
                    .expect("response chunk should encode");
                black_box(dst)
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn protocol_pipeline_benches(c: &mut Criterion) {
    let protocol = LengthDelimitedProtocol;
    let body = [0xAB; PIPELINE_BODY_LEN];
    let mut encoded = BytesMut::with_capacity(PIPELINE_REQUESTS * (24 + PIPELINE_BODY_LEN));
    for request_id in 0..PIPELINE_REQUESTS {
        let frame = protocol
            .encode_request_frame(request_id as u64, 42, 0, &body)
            .expect("pipeline frame should encode");
        encoded.extend_from_slice(&frame);
    }

    let mut group = c.benchmark_group("protocol_pipeline_64x32_bytes");
    group.throughput(Throughput::Elements(PIPELINE_REQUESTS as u64));

    group.bench_function("decoder_direct", |b| {
        b.iter_batched(
            || (protocol.decoder(1024), encoded.clone()),
            |(mut decoder, mut input)| {
                for _ in 0..PIPELINE_REQUESTS {
                    let decoded = decoder
                        .decode(&mut input)
                        .expect("pipeline decode should succeed")
                        .expect("pipeline request should decode");
                    let body_len = decoded.body_len;
                    black_box(decoded);
                    black_box(input.split_to(body_len));
                }
                assert!(input.is_empty());
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("message_reader_buffered", |b| {
        b.iter_batched(
            || {
                MessageReader::with_buffer(
                    tokio::io::empty(),
                    protocol.decoder(1024),
                    encoded.clone(),
                )
            },
            |mut reader| {
                for _ in 0..PIPELINE_REQUESTS {
                    let decoded = reader
                        .decode_buffered()
                        .expect("buffered pipeline decode should succeed")
                        .expect("buffered pipeline request should decode");
                    let body_len = decoded.body_len;
                    black_box(decoded);
                    black_box(reader.buffer_mut().split_to(body_len));
                }
                assert!(reader.buffer().is_empty());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn runtime_limit_benches(c: &mut Criterion) {
    let state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_max_connections(128_000)
            .with_max_in_flight_requests(128_000)
            .with_max_streaming_tasks(128_000),
    );
    let peer_state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_max_connections(128_000)
            .with_max_connections_per_peer(128_000),
    );
    let peer_rate_state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_max_connections(128_000)
            .with_max_connection_opens_per_peer_per_second(usize::MAX),
    );
    let peer: IpAddr = "127.0.0.1".parse().expect("valid ip");

    let mut group = c.benchmark_group("runtime_limits");
    group.bench_function("request_permit_acquire_drop", |b| {
        b.iter(|| {
            let permit = black_box(&state)
                .acquire_request_tracked()
                .expect("request permit");
            black_box(&permit);
            drop(permit);
        })
    });
    group.bench_function("connection_permit_acquire_drop", |b| {
        b.iter(|| {
            let permit = black_box(&state)
                .acquire_connection_tracked()
                .expect("connection permit");
            black_box(&permit);
            drop(permit);
        })
    });
    group.bench_function("connection_per_peer_acquire_drop", |b| {
        b.iter(|| {
            let permit = black_box(&peer_state)
                .acquire_connection_for_peer(black_box(peer))
                .expect("peer connection permit");
            black_box(&permit);
            drop(permit);
        })
    });
    group.bench_function("connection_per_peer_rate_acquire_drop", |b| {
        b.iter(|| {
            let permit = black_box(&peer_rate_state)
                .acquire_connection_for_peer(black_box(peer))
                .expect("peer connection rate permit");
            black_box(&permit);
            drop(permit);
        })
    });
    group.finish();
}

fn peer_rate_limit_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("peer_rate_limit_distinct_peers");

    for peer_count in [1_usize, 1_000, 100_000] {
        let peers: Vec<IpAddr> = (0..peer_count)
            .map(|index| {
                let octets = (index as u32).to_be_bytes();
                std::net::Ipv4Addr::new(10, octets[1], octets[2], octets[3]).into()
            })
            .collect();
        group.throughput(Throughput::Elements(peer_count as u64));
        group.bench_with_input(
            BenchmarkId::new("initial_admission", peer_count),
            &peers,
            |b, peers| {
                b.iter_batched(
                    || NacellePeerRateLimiter::new(peers.len()),
                    |limiter| {
                        for peer in peers {
                            assert!(limiter.try_acquire(*peer, usize::MAX).is_allowed());
                        }
                        black_box(limiter);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn telemetry_benches(c: &mut Criterion) {
    let facade = NacelleTelemetry::default();
    let concrete = NacelleTelemetry::new().with_observer(NacelleInMemoryObserver::new());
    let elapsed = Duration::from_micros(250);

    let mut group = c.benchmark_group("telemetry");
    group.bench_function("connection_opened_no_recorder", |b| {
        b.iter(|| {
            black_box(&facade).connection_opened(black_box(NacelleTransport::new("tcp")));
        })
    });
    group.bench_function("request_completed_no_recorder", |b| {
        b.iter(|| {
            black_box(&facade).request_completed(
                black_box(NacelleTransport::new("tcp")),
                black_box(1024),
                black_box(64),
                black_box(elapsed),
            );
        })
    });
    group.bench_function("timeout_no_recorder", |b| {
        b.iter(|| {
            black_box(&facade).timeout(
                black_box(NacelleTransport::new("tcp")),
                black_box("request_body_read"),
            );
        })
    });
    group.bench_function("connection_opened_concrete_observer", |b| {
        b.iter(|| {
            black_box(&concrete).connection_opened(black_box(NacelleTransport::new("tcp")));
        })
    });
    group.finish();
}

fn policy_benches(c: &mut Criterion) {
    use nacelle::core::policy::{Admission, Chain, NoPolicy, RequestRate, check_fn};
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("ready policy benchmark must not wait"),
        }
    }

    let check = |value: &usize| {
        if *value > 0 {
            Ok(())
        } else {
            Err(nacelle::core::policy::Rejection::new(
                nacelle::core::policy::RejectionKind::Denied,
                "zero",
            ))
        }
    };
    let predicate = check_fn(check);
    let chain = Chain::new(predicate, predicate);
    let peers = Arc::new(NacellePeerRateLimiter::new(128));
    let rate = RequestRate::new(peers.clone(), usize::MAX);
    let peer = IpAddr::from([127, 0, 0, 1]);
    let mut group = c.benchmark_group("policy_admission");
    group.bench_function("direct_predicate", |b| {
        b.iter(|| black_box(&check)(&black_box(1_usize)).unwrap())
    });
    group.bench_function("empty", |b| {
        b.iter(|| {
            ready(Admission::enter(
                black_box(&NoPolicy),
                &mut black_box(1_usize),
            ))
            .unwrap();
        })
    });
    group.bench_function("predicate", |b| {
        b.iter(|| {
            ready(Admission::enter(
                black_box(&predicate),
                &mut black_box(1_usize),
            ))
            .unwrap();
        })
    });
    group.bench_function("two_predicates", |b| {
        b.iter(|| {
            ready(Admission::enter(black_box(&chain), &mut black_box(1_usize))).unwrap();
        })
    });
    group.bench_function("direct_peer_rate", |b| {
        b.iter(|| black_box(peers.try_acquire(black_box(peer), usize::MAX)))
    });
    group.bench_function("peer_rate_policy", |b| {
        b.iter(|| {
            ready(Admission::enter(
                black_box(&rate),
                &mut Some(black_box(peer)),
            ))
            .unwrap();
        })
    });
    group.finish();
}

criterion_group!(
    critical_paths,
    protocol_frame_benches,
    protocol_pipeline_benches,
    runtime_limit_benches,
    peer_rate_limit_benches,
    telemetry_benches,
    policy_benches
);
criterion_main!(critical_paths);
