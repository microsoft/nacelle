use std::hint::black_box;
use std::io::Cursor;
use std::sync::mpsc;

use bytes::BytesMut;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use nacelle::core::pipeline::handler_fn;
use nacelle::core::tracking::NacelleConnectionRegistration;
use nacelle::core::{
    NacelleConnectionMeta, NacelleConnectionTracking, NacelleLimits, NacelleRuntimeControl,
    NacelleRuntimeState, NacelleTelemetry,
};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

fn state(tracked: bool, capacity: usize) -> NacelleRuntimeState {
    let mut limits = NacelleLimits::default();
    if tracked {
        limits = limits.with_connection_tracking(NacelleConnectionTracking::new(capacity).unwrap());
    }
    let state = NacelleRuntimeState::new(limits);
    state.control().set_max_connections(None).unwrap();
    state
}

fn pool(count: usize) -> (Vec<NacelleConnectionRegistration>, NacelleRuntimeControl) {
    let state = state(true, count);
    let entries = (0..count)
        .map(|_| {
            let metadata = NacelleConnectionMeta::tcp(None, None);
            let entry = state.register_connection(&metadata).unwrap();
            let activity = entry.activity().unwrap();
            activity.established(&metadata);
            activity.idle();
            entry
        })
        .collect();
    (entries, state.control())
}

fn registry(c: &mut Criterion) {
    let mut churn = c.benchmark_group("registry_churn");
    churn.throughput(Throughput::Elements(1));
    for tracked in [false, true] {
        let state = state(tracked, 256);
        let metadata = NacelleConnectionMeta::tcp(None, None);
        churn.bench_function(if tracked { "tracked" } else { "untracked" }, |b| {
            b.iter(|| drop(black_box(state.register_connection(&metadata).unwrap())));
        });
    }
    churn.finish();

    let (_entries, control) = pool(256);
    control.set_max_connections(Some(256)).unwrap();
    c.bench_function("registry_idle_scan_256", |b| {
        b.iter(|| black_box(control.reclaim_idle(256)));
    });
    c.bench_function("registry_reclaim_128_of_256", |b| {
        b.iter_batched_ref(
            || {
                let pool = pool(256);
                pool.1.set_max_connections(Some(128)).unwrap();
                pool
            },
            |(_, control)| black_box(control.reclaim_idle(128)),
            BatchSize::PerIteration,
        );
    });

    // Persistent workers exclude thread startup. Dispatch and replies remain measured.
    let (done, replies) = mpsc::channel();
    let mut senders = Vec::new();
    let mut threads = Vec::new();
    for _ in 0..4 {
        let (send, receive) = mpsc::channel::<NacelleRuntimeControl>();
        let done = done.clone();
        senders.push(send);
        threads.push(std::thread::spawn(move || {
            while let Ok(control) = receive.recv() {
                done.send(control.reclaim_idle(32)).unwrap();
            }
        }));
    }
    c.bench_function("registry_concurrent_4x32_of_256", |b| {
        b.iter_batched_ref(
            || {
                let pool = pool(256);
                pool.1.set_max_connections(Some(128)).unwrap();
                pool
            },
            |(_, control)| {
                for sender in &senders {
                    sender.send(control.clone()).unwrap();
                }
                for _ in 0..4 {
                    black_box(replies.recv().unwrap());
                }
            },
            BatchSize::PerIteration,
        );
    });
    drop(senders);
    for thread in threads {
        thread.join().unwrap();
    }
}

fn tcp(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("tracking_tcp");
    for count in [1, 64] {
        let mut input = BytesMut::new();
        for id in 0..count {
            input.extend_from_slice(
                &LengthDelimitedProtocol
                    .encode_request_frame(id, 1, 0, &[0; 32])
                    .unwrap(),
            );
        }
        let input = input.freeze();
        group.throughput(Throughput::Elements(count));
        for tracked in [false, true] {
            let server = TcpServer::<LengthDelimitedProtocol>::builder()
                .protocol(LengthDelimitedProtocol)
                .runtime_state(state(tracked, 1))
                .unwrap()
                .handler(handler_fn(
                    |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                        while let Some(chunk) = context.request_mut().body.next_chunk().await {
                            chunk?;
                        }
                        context.respond(TcpResponse::bytes(&b"ok"[..])).await
                    },
                ))
                .build()
                .unwrap()
                .with_telemetry(NacelleTelemetry::default().with_metrics(false));
            group.bench_with_input(
                BenchmarkId::new(if tracked { "tracked" } else { "untracked" }, count),
                &input,
                |b, input| {
                    b.to_async(&runtime).iter(|| async {
                        server
                            .serve_io(tokio::io::join(
                                Cursor::new(input.clone()),
                                tokio::io::sink(),
                            ))
                            .await
                            .unwrap();
                    })
                },
            );
        }
    }
    group.finish();
}

#[cfg(target_os = "linux")]
fn memory(count: usize) {
    fn rss_kib() -> usize {
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }
    let state = state(true, count);
    // Nonzero sentinels touch each storage page before the registry observation.
    let mut slots: Vec<(usize, Option<NacelleConnectionRegistration>)> =
        (0..count).map(|i| (i + 1, None)).collect();
    let before = rss_kib();
    for (_, slot) in &mut slots {
        *slot = Some(
            state
                .register_connection(&NacelleConnectionMeta::tcp(None, None))
                .unwrap(),
        );
    }
    let live = rss_kib();
    for (_, slot) in &mut slots {
        *slot = None;
    }
    let retired = rss_kib();
    println!(
        "entries={count} before_kib={before} live_kib={live} retired_kib={retired} bytes_per_live_entry={:.1}",
        live.saturating_sub(before) as f64 * 1024.0 / count as f64
    );
    black_box(slots);
}

fn main() {
    #[cfg(target_os = "linux")]
    if let Ok(count) = std::env::var("NACELLE_TRACKING_MEMORY_ENTRIES") {
        memory(count.parse().expect("positive entry count"));
        return;
    }
    let mut criterion = Criterion::default().configure_from_args();
    registry(&mut criterion);
    tcp(&mut criterion);
    criterion.final_summary();
}
