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

#[cfg(feature = "http")]
fn http_connection_benches(c: &mut Criterion) {
    use nacelle::core::{lifecycle::NacelleShutdown, pipeline::handler_fn};
    use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn round_trip(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.expect("connect HTTP client");
        stream.write_all(request).await.expect("write HTTP request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read HTTP response");
        response
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("HTTP benchmark runtime");
    let (shutdown, token) = NacelleShutdown::pair();
    let (addr, task) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HTTP benchmark listener");
        let addr = listener.local_addr().expect("HTTP listener address");
        let server = HyperServer::new(handler_fn(
            |mut context: HttpRequestContext<()>| async move {
                let mut body = BytesMut::new();
                while let Some(chunk) = context.request_mut().next_body_chunk().await {
                    body.extend_from_slice(&chunk?);
                }
                context
                    .respond(HttpResponse::bytes(http::StatusCode::OK, body.freeze()))
                    .await
            },
        ));
        let task =
            tokio::spawn(async move { server.serve_listener_with_shutdown(listener, token).await });
        (addr, task)
    });

    let mut group = c.benchmark_group("http_connection_round_trip");
    for body_len in [0, 64 * 1024] {
        let body = vec![0xAB; body_len];
        let mut request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {body_len}\r\nConnection: close\r\n\r\n"
        )
        .into_bytes();
        request.extend_from_slice(&body);
        let response = runtime.block_on(round_trip(addr, &request));
        assert!(response.starts_with(b"HTTP/1.1 200 "));
        // The response adapter adds chunked framing around the non-ASCII payload.
        assert_eq!(
            response.iter().filter(|&&byte| byte == 0xAB).count(),
            body_len
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("buffered_echo", body_len),
            &request,
            |b, request| b.to_async(&runtime).iter(|| round_trip(addr, request)),
        );
    }
    group.finish();

    shutdown.shutdown();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("HTTP benchmark should shut down")
            .expect("HTTP server task should join")
            .expect("HTTP server should exit cleanly");
    });
}

criterion_group!(
    critical_paths,
    protocol_frame_benches,
    protocol_pipeline_benches,
    runtime_limit_benches,
    peer_rate_limit_benches,
    telemetry_benches
);
#[cfg(feature = "http")]
criterion_group!(http_paths, http_connection_benches);
#[cfg(feature = "http")]
criterion_main!(critical_paths, http_paths);
#[cfg(not(feature = "http"))]
criterion_main!(critical_paths);
