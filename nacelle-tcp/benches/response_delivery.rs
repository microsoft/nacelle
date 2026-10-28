use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nacelle_codec::MessageDecoder;
use nacelle_core::error::NacelleError;
use nacelle_core::pipeline::{ConnectionInfo, handler_fn};
use nacelle_core::policy::{Admission, Hold, Rejection, check_fn};
use nacelle_core::telemetry::NacelleTelemetry;
use nacelle_tcp::policies::{NoTcpPolicies, SharedTcpPolicySet, TcpHead, TcpPolicies};
use nacelle_tcp::{
    DecodedRequest, FrameBuffer, NacelleTcpConfig, Protocol, ResponseWritePolicy,
    TcpRequestContext, TcpResponse, TcpServer,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const RESPONSE_BYTES: usize = 32;
const RESPONSE_BUFFER_CAPACITY: usize = 2 * 1024;
const PIPELINE_DEPTHS: [usize; 3] = [1, 8, 32];
const POOLED_CONNECTIONS: usize = 8;
const POOLED_ROUNDS: usize = 8;
const REQUEST_BYTES: [u8; 64] = [0; 64];

struct Decoder;

impl MessageDecoder for Decoder {
    type Message = DecodedRequest<()>;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if input.is_empty() {
            return Ok(None);
        }
        let _ = input.split_to(1);
        Ok(Some(DecodedRequest {
            request: (),
            body_len: 0,
        }))
    }
}

#[derive(Clone, Copy)]
struct BenchProtocol;

impl Protocol for BenchProtocol {
    type Request = ();
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
}

#[derive(Default)]
struct WriteStats {
    writes: AtomicUsize,
    max_write_bytes: AtomicUsize,
}

struct BenchmarkIo {
    remaining_requests: usize,
    readable_requests: usize,
    current_window_requests: usize,
    delivered_window_bytes: usize,
    pipeline_depth: usize,
    read_waker: Option<Waker>,
    stats: Arc<WriteStats>,
}

impl BenchmarkIo {
    fn new(total_requests: usize, pipeline_depth: usize, stats: Arc<WriteStats>) -> Self {
        let readable_requests = total_requests.min(pipeline_depth);
        Self {
            remaining_requests: total_requests.saturating_sub(readable_requests),
            readable_requests,
            current_window_requests: readable_requests,
            delivered_window_bytes: 0,
            pipeline_depth,
            read_waker: None,
            stats,
        }
    }
}

impl AsyncRead for BenchmarkIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.readable_requests == 0 && self.remaining_requests == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.readable_requests == 0 {
            self.read_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let take = self.readable_requests.min(buf.remaining());
        buf.put_slice(&REQUEST_BYTES[..take]);
        self.readable_requests -= take;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for BenchmarkIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.stats
            .max_write_bytes
            .fetch_max(buf.len(), Ordering::Relaxed);
        self.delivered_window_bytes = self.delivered_window_bytes.saturating_add(buf.len());
        let expected_window_bytes = self.current_window_requests.saturating_mul(RESPONSE_BYTES);
        if expected_window_bytes != 0 && self.delivered_window_bytes >= expected_window_bytes {
            let next_window = self.remaining_requests.min(self.pipeline_depth);
            self.remaining_requests = self.remaining_requests.saturating_sub(next_window);
            self.readable_requests = next_window;
            self.current_window_requests = next_window;
            self.delivered_window_bytes = 0;
            if let Some(waker) = self.read_waker.take() {
                waker.wake();
            }
        }
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn server(
    policy: ResponseWritePolicy,
    response_buffer_capacity: usize,
) -> TcpServer<BenchProtocol, impl nacelle_tcp::TcpHandler<BenchProtocol>> {
    TcpServer::<BenchProtocol>::builder()
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
        .telemetry(NacelleTelemetry::default())
        .build()
        .expect("benchmark server should build")
}

async fn run_workload(
    policy: ResponseWritePolicy,
    response_buffer_capacity: usize,
    pipeline_depth: usize,
    connections: usize,
    rounds: usize,
    expected_writes: usize,
    expected_max_write_bytes: usize,
) {
    let stats = Arc::new(WriteStats::default());
    let server = server(policy, response_buffer_capacity);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..connections {
        let io = BenchmarkIo::new(
            pipeline_depth.saturating_mul(rounds),
            pipeline_depth,
            stats.clone(),
        );
        let server = server.clone();
        tasks.spawn(async move { server.serve_io(io).await });
    }
    while let Some(result) = tasks.join_next().await {
        result
            .expect("benchmark task should join")
            .expect("benchmark server should run");
    }

    assert_eq!(stats.writes.load(Ordering::Relaxed), expected_writes);
    assert_eq!(
        stats.max_write_bytes.load(Ordering::Relaxed),
        expected_max_write_bytes
    );
}

fn response_delivery(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("benchmark runtime");
    let mut group = c.benchmark_group("tcp_response_delivery_64x32_bytes");

    group.bench_function("immediate", |b| {
        b.to_async(&runtime).iter(|| {
            run_workload(
                ResponseWritePolicy::Immediate,
                RESPONSE_BUFFER_CAPACITY,
                64,
                1,
                1,
                64,
                RESPONSE_BYTES,
            )
        });
    });
    group.bench_function("coalesce_buffered", |b| {
        b.to_async(&runtime).iter(|| {
            run_workload(
                ResponseWritePolicy::CoalesceBuffered,
                RESPONSE_BUFFER_CAPACITY,
                64,
                1,
                1,
                1,
                64 * RESPONSE_BYTES,
            )
        });
    });
    group.bench_function("flush_at_bytes_1024", |b| {
        b.to_async(&runtime).iter(|| {
            run_workload(
                ResponseWritePolicy::FlushAtBytes(1_024),
                1_024,
                64,
                1,
                1,
                2,
                1_024,
            )
        });
    });
    group.bench_function("flush_at_bytes_2048_grows_from_1024", |b| {
        b.to_async(&runtime).iter(|| {
            run_workload(
                ResponseWritePolicy::FlushAtBytes(2_048),
                1_024,
                64,
                1,
                1,
                1,
                2_048,
            )
        });
    });
    group.finish();

    let mut same_socket = c.benchmark_group("tcp_response_delivery_same_socket_32_byte_response");
    for pipeline_depth in PIPELINE_DEPTHS {
        same_socket.throughput(Throughput::Elements(pipeline_depth as u64));
        for (name, policy) in [
            ("immediate", ResponseWritePolicy::Immediate),
            ("coalesce_buffered", ResponseWritePolicy::CoalesceBuffered),
        ] {
            let expected_writes = if matches!(policy, ResponseWritePolicy::Immediate) {
                pipeline_depth
            } else {
                1
            };
            let expected_max_write = if matches!(policy, ResponseWritePolicy::Immediate) {
                RESPONSE_BYTES
            } else {
                pipeline_depth * RESPONSE_BYTES
            };
            same_socket.bench_with_input(
                BenchmarkId::new(name, pipeline_depth),
                &pipeline_depth,
                |b, &pipeline_depth| {
                    b.to_async(&runtime).iter(|| {
                        run_workload(
                            policy,
                            RESPONSE_BUFFER_CAPACITY,
                            pipeline_depth,
                            1,
                            1,
                            expected_writes,
                            expected_max_write,
                        )
                    });
                },
            );
        }
    }
    same_socket.finish();

    let mut pooled = c.benchmark_group("tcp_response_delivery_pool_8_connections_8_rounds");
    for pipeline_depth in PIPELINE_DEPTHS {
        let requests = POOLED_CONNECTIONS * POOLED_ROUNDS * pipeline_depth;
        pooled.throughput(Throughput::Elements(requests as u64));
        for (name, policy) in [
            ("immediate", ResponseWritePolicy::Immediate),
            ("coalesce_buffered", ResponseWritePolicy::CoalesceBuffered),
        ] {
            let writes_per_window = if matches!(policy, ResponseWritePolicy::Immediate) {
                pipeline_depth
            } else {
                1
            };
            let expected_max_write = if matches!(policy, ResponseWritePolicy::Immediate) {
                RESPONSE_BYTES
            } else {
                pipeline_depth * RESPONSE_BYTES
            };
            pooled.bench_with_input(
                BenchmarkId::new(name, pipeline_depth),
                &pipeline_depth,
                |b, &pipeline_depth| {
                    b.to_async(&runtime).iter(|| {
                        run_workload(
                            policy,
                            RESPONSE_BUFFER_CAPACITY,
                            pipeline_depth,
                            POOLED_CONNECTIONS,
                            POOLED_ROUNDS,
                            POOLED_CONNECTIONS * POOLED_ROUNDS * writes_per_window,
                            expected_max_write,
                        )
                    });
                },
            );
        }
    }
    pooled.finish();

    let mut policies = c.benchmark_group("tcp_static_policies_pipeline_64");
    policies.throughput(Throughput::Elements(64));
    macro_rules! profile {
        ($name:literal, $policies:expr, $writes:expr) => {
            policies.bench_function($name, |b| {
                b.to_async(&runtime)
                    .iter(|| run_static_workload($policies, $writes));
            });
        };
    }
    policies.bench_function("default", |b| {
        b.to_async(&runtime).iter(|| async {
            let stats = Arc::new(WriteStats::default());
            let server = server(
                ResponseWritePolicy::CoalesceBuffered,
                RESPONSE_BUFFER_CAPACITY,
            );
            server
                .serve_io(BenchmarkIo::new(64, 64, stats.clone()))
                .await
                .expect("static-policy benchmark should run");
            assert_eq!(stats.writes.load(Ordering::Relaxed), 1);
        });
    });
    profile!("attached_noop", NoTcpPolicies, 1);
    profile!("empty", TcpPolicies::new(), 1);
    profile!(
        "predicate",
        TcpPolicies::new().on_request_head(check_fn(allow_head)),
        1
    );
    profile!(
        "two_predicates",
        TcpPolicies::new()
            .on_request_head(check_fn(allow_head))
            .on_request_head(check_fn(allow_head)),
        1
    );
    profile!(
        "head_resource",
        TcpPolicies::new().on_request_head(BenchLease),
        64
    );
    profile!(
        "handler_resource",
        TcpPolicies::new().around_handler(BenchLease),
        1
    );
    policies.finish();
}

fn allow_head(_: &TcpHead<'_, (), Arc<()>>) -> Result<(), Rejection> {
    Ok(())
}

#[derive(Clone, Copy)]
struct BenchLease;

impl<I: ?Sized> Admission<I> for BenchLease {
    type Lease = Hold<()>;

    fn enter<'a>(
        &'a self,
        _: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        std::future::ready(Ok(Hold::new(())))
    }
}

async fn run_static_workload<Policies>(policies: Policies, expected_writes: usize)
where
    Policies: SharedTcpPolicySet<(), Arc<()>>,
{
    let stats = Arc::new(WriteStats::default());
    let server = server(
        ResponseWritePolicy::CoalesceBuffered,
        RESPONSE_BUFFER_CAPACITY,
    )
    .with_policies(policies);
    server
        .serve_io(BenchmarkIo::new(64, 64, stats.clone()))
        .await
        .expect("static-policy benchmark should run");
    assert_eq!(stats.writes.load(Ordering::Relaxed), expected_writes);
}

criterion_group!(benches, response_delivery);
criterion_main!(benches);
