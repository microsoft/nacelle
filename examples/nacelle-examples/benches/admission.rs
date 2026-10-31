use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{Admission, AdmissionConfig, AdmissionRejection, AllowAll, NacelleBody};
use nacelle::http::{HttpRequestContext, HttpResponse, HttpService, HyperServer, StatusCode};
use nacelle::tcp::{
    NacelleTcpConfig, ResponseWritePolicy, TcpHeadReadLimits, TcpRequestContext, TcpResponse,
    TcpServer, TcpService,
};
use nacelle_reference_protocol::LengthDelimitedProtocol;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct Capacity(Arc<Semaphore>);

impl<Context> Admission<Context> for Capacity {
    type Permit = OwnedSemaphorePermit;

    fn admit(
        &self,
        _context: Context,
    ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + Send {
        let semaphore = self.0.clone();
        async move {
            semaphore
                .acquire_owned()
                .await
                .map_err(|_| AdmissionRejection::Unavailable)
        }
    }
}

fn admission_config() -> AdmissionConfig {
    AdmissionConfig::new(Duration::from_secs(30), 64).unwrap()
}

fn tcp_handler() -> impl nacelle::tcp::TcpHandler<LengthDelimitedProtocol> {
    handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                chunk?;
            }
            // All cases yield so four connections exercise the capacity-one queue.
            tokio::task::yield_now().await;
            context
                .respond(TcpResponse::bytes(Bytes::from_static(&[0xAB; 32])))
                .await
        },
    )
}

fn tcp_case<H: TcpService<LengthDelimitedProtocol>>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    runtime: &tokio::runtime::Runtime,
    name: &str,
    server: TcpServer<LengthDelimitedProtocol, H>,
    input: &Bytes,
) {
    let io = || tokio::io::join(Cursor::new(input.clone()), tokio::io::sink());
    group.bench_function(name, |b| {
        b.to_async(runtime).iter(|| async {
            let (a, b, c, d) = tokio::join!(
                server.serve_io(io()),
                server.serve_io(io()),
                server.serve_io(io()),
                server.serve_io(io()),
            );
            a.unwrap();
            b.unwrap();
            c.unwrap();
            d.unwrap();
        })
    });
}

fn tcp(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut input = BytesMut::new();
    for id in 0..64 {
        input.extend_from_slice(
            &LengthDelimitedProtocol
                .encode_request_frame(id, 1, 0, &[0; 32])
                .unwrap(),
        );
    }
    let input = input.freeze();
    for policy in [
        ResponseWritePolicy::Immediate,
        ResponseWritePolicy::CoalesceBuffered,
    ] {
        let label = if matches!(policy, ResponseWritePolicy::Immediate) {
            "immediate"
        } else {
            "coalesced"
        };
        let mut group = c.benchmark_group(format!("admission_tcp_4x64/{label}"));
        group.throughput(Throughput::Elements(256));
        let config = NacelleTcpConfig::default().with_response_write_policy(policy);
        tcp_case(
            &mut group,
            &runtime,
            "default",
            TcpServer::<LengthDelimitedProtocol>::builder()
                .protocol(LengthDelimitedProtocol)
                .tcp_config(config.clone())
                .handler(tcp_handler())
                .build()
                .unwrap(),
            &input,
        );
        let config = config.with_head_read_limits(TcpHeadReadLimits::new(24, 4096).unwrap());
        let builder = || {
            TcpServer::<LengthDelimitedProtocol>::builder()
                .protocol(LengthDelimitedProtocol)
                .tcp_config(config.clone())
                .handler(tcp_handler())
        };
        tcp_case(
            &mut group,
            &runtime,
            "bounded_no_policy",
            builder().build().unwrap(),
            &input,
        );
        tcp_case(
            &mut group,
            &runtime,
            "ready",
            builder()
                .admission(AllowAll, admission_config())
                .unwrap()
                .build()
                .unwrap(),
            &input,
        );
        for capacity in [4, 1] {
            tcp_case(
                &mut group,
                &runtime,
                &format!("semaphore_{capacity}"),
                builder()
                    .admission(
                        Capacity(Arc::new(Semaphore::new(capacity))),
                        admission_config(),
                    )
                    .unwrap()
                    .build()
                    .unwrap(),
                &input,
            );
        }
        group.finish();
    }
}

fn http_handler() -> impl nacelle::http::HttpHandler<()> {
    handler_fn(|mut context: HttpRequestContext<()>| async move {
        let mut received = 0;
        while let Some(chunk) = context.request_mut().next_body_chunk().await {
            received += chunk?.len();
        }
        tokio::task::yield_now().await;
        let body = if received == 0 {
            NacelleBody::empty()
        } else {
            let (sender, body) = NacelleBody::channel(4);
            tokio::spawn(async move {
                for _ in 0..4 {
                    if sender
                        .send(Ok(Bytes::from_static(&[0xAB; 16 * 1024])))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            body
        };
        context
            .respond(HttpResponse::new(
                StatusCode::OK,
                http::HeaderMap::new(),
                body,
            ))
            .await
    })
}

fn http_case<H: HttpService<()>>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    runtime: &tokio::runtime::Runtime,
    name: &str,
    server: HyperServer<H>,
) {
    let (shutdown, token) = nacelle::core::NacelleShutdown::pair();
    let (address, task) = runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(server.serve_listener_with_shutdown(listener, token));
        (address, task)
    });
    for body_bytes in [0, 64 * 1024] {
        let mut request = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {body_bytes}\r\nConnection: close\r\n\r\n",
        ).into_bytes();
        request.extend_from_slice(&vec![0xAB; body_bytes]);
        let request = request.as_slice();
        let round_trip = || async {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket.write_all(request).await.unwrap();
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200 "));
            assert_eq!(
                response.iter().filter(|&&byte| byte == 0xAB).count(),
                body_bytes
            );
        };
        runtime.block_on(round_trip());
        group.bench_with_input(BenchmarkId::new(name, body_bytes), &(), |b, ()| {
            b.to_async(runtime).iter(|| async {
                tokio::join!(round_trip(), round_trip(), round_trip(), round_trip());
            });
        });
    }
    shutdown.shutdown();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    });
}

fn http(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("admission_http_4_connections");
    group.throughput(Throughput::Elements(4));
    http_case(
        &mut group,
        &runtime,
        "default",
        HyperServer::new(http_handler()),
    );
    http_case(
        &mut group,
        &runtime,
        "ready",
        HyperServer::new(http_handler()).with_admission(AllowAll, admission_config()),
    );
    for capacity in [4, 1] {
        http_case(
            &mut group,
            &runtime,
            &format!("semaphore_{capacity}"),
            HyperServer::new(http_handler()).with_admission(
                Capacity(Arc::new(Semaphore::new(capacity))),
                admission_config(),
            ),
        );
    }
    group.finish();
}

criterion_group!(benches, tcp, http);
criterion_main!(benches);
