use bytes::{Bytes, BytesMut};
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nacelle::{
    FrameRequest, LengthDelimitedProtocol, NacelleInMemoryTelemetrySink, NacelleLimits,
    NacelleRuntimeState, NacelleTelemetry, NacelleTransport, Protocol,
};
use std::hint::black_box;
use std::net::IpAddr;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const MEMORY_CONTENTION_CONNECTIONS: usize = 5_000;

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
    group.bench_function("decode_head_small", |b| {
        b.iter_batched(
            || BytesMut::from(small_frame.as_ref()),
            |mut buf| {
                black_box(
                    protocol
                        .decode_head(&mut buf, 1024)
                        .expect("decode should succeed")
                        .expect("head should be present"),
                )
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("decode_head_large", |b| {
        b.iter_batched(
            || BytesMut::from(large_frame.as_ref()),
            |mut buf| {
                black_box(
                    protocol
                        .decode_head(&mut buf, 8192)
                        .expect("decode should succeed")
                        .expect("head should be present"),
                )
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
                protocol
                    .encode_response_chunk(&mut context, response_chunk.clone(), &mut dst)
                    .expect("response chunk should encode");
                black_box(dst)
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
            .with_max_streaming_tasks(128_000)
            .with_max_memory_bytes(8 * 1024 * 1024 * 1024),
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
    let unbounded_memory_state =
        NacelleRuntimeState::new(NacelleLimits::default().with_max_memory_bytes(usize::MAX));
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
    group.bench_function("memory_allocate_drop_1k", |b| {
        b.iter(|| {
            let allocation = black_box(&state).allocate_memory(1024).expect("memory");
            black_box(&allocation);
            drop(allocation);
        })
    });
    group.bench_function("memory_allocate_drop_unbounded_1k", |b| {
        b.iter(|| {
            let allocation = black_box(&unbounded_memory_state)
                .allocate_memory(1024)
                .expect("unbounded memory");
            black_box(&allocation);
            drop(allocation);
        })
    });
    group.finish();
}

fn memory_contention_benches(c: &mut Criterion) {
    let bounded = Arc::new(NacelleRuntimeState::new(
        NacelleLimits::default().with_max_memory_bytes(8 * 1024 * 1024 * 1024),
    ));
    let unbounded = Arc::new(NacelleRuntimeState::new(
        NacelleLimits::default().with_max_memory_bytes(usize::MAX),
    ));

    let mut group = c.benchmark_group("memory_contention");
    group.throughput(Throughput::Elements(MEMORY_CONTENTION_CONNECTIONS as u64));
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(20);

    group.bench_function("bounded_1k_5000_connection_wave", |b| {
        let state = bounded.clone();
        b.iter_custom(|waves| contended_memory_allocation_waves(state.clone(), 1024, waves))
    });
    group.bench_function("bounded_64k_5000_connection_wave", |b| {
        let state = bounded.clone();
        b.iter_custom(|waves| contended_memory_allocation_waves(state.clone(), 64 * 1024, waves))
    });
    group.bench_function("unbounded_1k_5000_connection_wave", |b| {
        let state = unbounded.clone();
        b.iter_custom(|waves| contended_memory_allocation_waves(state.clone(), 1024, waves))
    });

    group.finish();
}

fn contended_memory_allocation_waves(
    state: Arc<NacelleRuntimeState>,
    bytes: usize,
    waves: u64,
) -> Duration {
    let workers = thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, MEMORY_CONTENTION_CONNECTIONS);
    let base_allocations = MEMORY_CONTENTION_CONNECTIONS / workers;
    let extra_allocations = MEMORY_CONTENTION_CONNECTIONS % workers;
    let ready = Arc::new(Barrier::new(workers + 1));
    let start = Arc::new(Barrier::new(workers + 1));

    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let state = state.clone();
            let ready = ready.clone();
            let start = start.clone();
            let allocations_per_wave = base_allocations + usize::from(worker < extra_allocations);

            thread::spawn(move || {
                ready.wait();
                start.wait();

                for _ in 0..waves {
                    let mut allocations = Vec::with_capacity(allocations_per_wave);
                    for _ in 0..allocations_per_wave {
                        allocations.push(
                            state
                                .allocate_memory(bytes)
                                .expect("contended memory allocation"),
                        );
                    }
                    black_box(allocations.len());
                    drop(allocations);
                }
            })
        })
        .collect();

    ready.wait();
    let elapsed = Instant::now();
    start.wait();

    for handle in handles {
        handle.join().expect("memory contention worker panicked");
    }

    elapsed.elapsed()
}

fn telemetry_benches(c: &mut Criterion) {
    let disabled = NacelleTelemetry::default();
    let sink = Arc::new(NacelleInMemoryTelemetrySink::new());
    let enabled = NacelleTelemetry::new().with_sink(sink);
    let elapsed = Duration::from_micros(250);

    let mut group = c.benchmark_group("telemetry");
    group.bench_function("connection_opened_disabled", |b| {
        b.iter(|| {
            black_box(&disabled).connection_opened(black_box(NacelleTransport::new("tcp")));
        })
    });
    group.bench_function("request_completed_disabled", |b| {
        b.iter(|| {
            black_box(&disabled).request_completed(
                black_box(NacelleTransport::new("tcp")),
                black_box(1024),
                black_box(64),
                black_box(elapsed),
            );
        })
    });
    group.bench_function("timeout_disabled", |b| {
        b.iter(|| {
            black_box(&disabled).timeout(
                black_box(NacelleTransport::new("tcp")),
                black_box("request_body_read"),
            );
        })
    });
    group.bench_function("connection_opened_in_memory_sink", |b| {
        b.iter(|| {
            black_box(&enabled).connection_opened(black_box(NacelleTransport::new("tcp")));
        })
    });
    group.finish();
}

criterion_group!(
    critical_paths,
    protocol_frame_benches,
    runtime_limit_benches,
    memory_contention_benches,
    telemetry_benches
);
criterion_main!(critical_paths);
