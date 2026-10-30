use std::net::SocketAddr;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput};
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{
    NacelleCloseOutcome, NacelleConnectionTracking, NacelleError, NacelleLimits,
    NacelleRetireOutcome, NacelleRuntimeState, NacelleShutdown, NacelleTelemetry,
};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer, StatusCode};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

type Client = BufReader<TcpStream>;

struct Server {
    address: SocketAddr,
    state: NacelleRuntimeState,
    shutdown: NacelleShutdown,
    task: tokio::task::JoinHandle<Result<(), NacelleError>>,
}

impl Server {
    async fn start(tracked: bool, capacity: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut limits = NacelleLimits::default();
        if tracked {
            limits =
                limits.with_connection_tracking(NacelleConnectionTracking::new(capacity).unwrap());
        }
        let state = NacelleRuntimeState::new(limits);
        state.control().set_max_connections(Some(capacity)).unwrap();
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            let mut response =
                HttpResponse::bytes(StatusCode::OK, &b"0123456789abcdef0123456789abcdef"[..]);
            response.headers.insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_static("32"),
            );
            context.respond(response).await
        }))
        .with_runtime_state(state.clone())
        .unwrap()
        .with_telemetry(NacelleTelemetry::default().with_metrics(false));
        let (shutdown, token) = NacelleShutdown::pair();
        let task = tokio::spawn(server.serve_listener_with_shutdown(listener, token));
        Self {
            address,
            state,
            shutdown,
            task,
        }
    }

    async fn connect(&self) -> Client {
        let client = TcpStream::connect(self.address).await.unwrap();
        client.set_nodelay(true).unwrap();
        BufReader::new(client)
    }

    async fn empty(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.state.active_connections() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection cleanup");
    }

    async fn stop(self) {
        self.shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(self.state.active_connections(), 0);
    }
}

async fn request(client: &mut Client, close: bool) {
    let request = if close {
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".as_slice()
    } else {
        b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".as_slice()
    };
    client.get_mut().write_all(request).await.unwrap();
    let mut line = Vec::with_capacity(128);
    client.read_until(b'\n', &mut line).await.unwrap();
    assert!(line.starts_with(b"HTTP/1.1 200 "));
    loop {
        line.clear();
        assert!(client.read_until(b'\n', &mut line).await.unwrap() > 0);
        if line == b"\r\n" {
            break;
        }
    }
    let mut body = [0; 32];
    client.read_exact(&mut body).await.unwrap();
    assert_eq!(&body, b"0123456789abcdef0123456789abcdef");
    if close {
        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    }
}

fn profiles(c: &mut Criterion, runtime: &tokio::runtime::Runtime) {
    let mut group = c.benchmark_group("http_tracking");
    for tracked in [false, true] {
        let server = runtime.block_on(Server::start(tracked, 4096));
        for count in [1, 64] {
            group.throughput(Throughput::Elements(count));
            group.bench_with_input(
                BenchmarkId::new(if tracked { "tracked" } else { "untracked" }, count),
                &count,
                |b, &count| {
                    b.to_async(runtime).iter(|| async {
                        let mut client = server.connect().await;
                        for i in 0..count {
                            request(&mut client, i + 1 == count).await;
                        }
                    })
                },
            );
        }
        runtime.block_on(server.stop());
    }
    group.finish();

    let server = runtime.block_on(Server::start(true, 1));
    let mut group = c.benchmark_group("http_retirement");
    for graceful in [false, true] {
        group.bench_function(if graceful { "graceful" } else { "forced" }, |b| {
            b.iter_custom(|iterations| {
                runtime.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        let mut client = server.connect().await;
                        request(&mut client, false).await;
                        let control = server.state.control();
                        let id = control.connections(1).unwrap()[0].connection.connection_id;
                        let started = Instant::now();
                        if graceful {
                            assert_eq!(
                                control.retire_connection(id, Duration::from_secs(1)),
                                NacelleRetireOutcome::Selected
                            );
                        } else {
                            assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
                        }
                        assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
                        server.empty().await;
                        elapsed += started.elapsed();
                    }
                    elapsed
                })
            });
        });
    }
    group.finish();
    runtime.block_on(server.stop());
}

#[cfg(target_os = "linux")]
async fn memory(tracked: bool, count: usize) {
    fn rss_kib() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
    assert!((1..=16_384).contains(&count));
    let server = Server::start(tracked, count).await;
    let mut warm = server.connect().await;
    request(&mut warm, true).await;
    drop(warm);
    server.empty().await;
    let mut slots: Vec<_> = (0..count).map(|i| (i + 1, None)).collect();
    let before = rss_kib();
    for (_, slot) in &mut slots {
        let mut client = server.connect().await;
        request(&mut client, false).await;
        *slot = Some(client);
    }
    assert_eq!(server.state.active_connections(), count);
    let live = rss_kib();
    for (_, slot) in &mut slots {
        *slot = None;
    }
    server.empty().await;
    let retired = rss_kib();
    println!(
        "tracking={tracked} connections={count} before_kib={before} live_kib={live} retired_kib={retired} bytes_per_pair={:.1}",
        live.saturating_sub(before) as f64 * 1024.0 / count as f64
    );
    std::hint::black_box(slots);
    server.stop().await;
}

async fn latency(tracked: bool, count: usize) {
    assert!((1..=1_000_000).contains(&count));
    let server = Server::start(tracked, 1).await;
    let mut client = server.connect().await;
    for _ in 0..100 {
        request(&mut client, false).await;
    }
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let started = Instant::now();
        request(&mut client, false).await;
        samples.push(started.elapsed().as_nanos());
    }
    drop(client);
    server.empty().await;
    samples.sort_unstable();
    println!(
        "tracking={tracked} requests={count} p50_ns={} p99_ns={}",
        samples[count / 2],
        samples[(count * 99).div_ceil(100) - 1]
    );
    server.stop().await;
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let tracked = match std::env::var("NACELLE_HTTP_TRACKING").as_deref() {
        Ok("1") => true,
        Ok("0") | Err(_) => false,
        _ => panic!("NACELLE_HTTP_TRACKING must be 0 or 1"),
    };
    #[cfg(target_os = "linux")]
    if let Ok(count) = std::env::var("NACELLE_HTTP_MEMORY_CONNECTIONS") {
        runtime.block_on(memory(tracked, count.parse().expect("connection count")));
        return;
    }
    if let Ok(count) = std::env::var("NACELLE_HTTP_LATENCY_REQUESTS") {
        runtime.block_on(latency(tracked, count.parse().expect("request count")));
        return;
    }
    let mut criterion = Criterion::default().configure_from_args();
    profiles(&mut criterion, &runtime);
    criterion.final_summary();
}
