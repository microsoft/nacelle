#[cfg(all(feature = "mimalloc-allocator", not(feature = "allocation-counting")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
#[cfg(all(feature = "mimalloc-allocator", feature = "allocation-counting"))]
#[global_allocator]
static ALLOC: nacelle_stress_common::CountingAllocator<mimalloc::MiMalloc> =
    nacelle_stress_common::CountingAllocator::new(mimalloc::MiMalloc);
#[cfg(all(not(feature = "mimalloc-allocator"), feature = "allocation-counting"))]
#[global_allocator]
static ALLOC: nacelle_stress_common::CountingAllocator<std::alloc::System> =
    nacelle_stress_common::CountingAllocator::new(std::alloc::System);

mod recorder;
use nacelle_stress_server::shared;
use recorder::{ByteTotals, ConsoleRecorder};
use shared::{
    ResponseWriteMode, ServerConfig, StressServer, build_runtime_state,
    build_server_with_runtime_state, configure_allocator, parse_args, print_config,
};

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use metrics_util::debugging::{DebugValue, Snapshotter};
#[cfg(feature = "tls-self-signed")]
use nacelle::core::NacelleTimeoutReason;
use nacelle::core::telemetry::NacelleTelemetryObserver;
use nacelle::core::{NacelleError, NacelleTelemetry};
#[cfg(feature = "tls-self-signed")]
use nacelle::rustls::NacelleTlsConfig;
use nacelle::tcp::TcpHandler;
use nacelle_reference_protocol::LengthDelimitedProtocol;
use nacelle_stress_common::make_tcp_socket;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::watch;

#[cfg(feature = "tls-self-signed")]
type StressTlsConfig = NacelleTlsConfig;
#[cfg(not(feature = "tls-self-signed"))]
type StressTlsConfig = ();

#[cfg(feature = "tls-self-signed")]
fn clone_tls_config(tls_config: &Option<StressTlsConfig>) -> Option<StressTlsConfig> {
    tls_config.clone()
}

#[cfg(not(feature = "tls-self-signed"))]
fn clone_tls_config(tls_config: &Option<StressTlsConfig>) -> Option<StressTlsConfig> {
    *tls_config
}

/// Creates a server-side TCP socket ready for `bind()` + `listen()`.
///
/// When `reuseport` is true and the platform is Linux, `SO_REUSEPORT` is set so
/// that multiple listeners can share the same port with kernel-level load
/// balancing.
fn make_server_socket(
    addr: &SocketAddr,
    #[cfg_attr(not(target_os = "linux"), allow(unused_variables))] reuseport: bool,
) -> Result<TcpSocket, std::io::Error> {
    let socket = make_tcp_socket(addr)?;
    #[cfg(target_os = "linux")]
    if reuseport {
        socket.set_reuseport(true)?;
    }
    Ok(socket)
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

async fn run_server<H, Observer>(
    listener: TcpListener,
    server: StressServer<H, Observer>,
    tls_config: Option<StressTlsConfig>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), NacelleError>
where
    H: TcpHandler<LengthDelimitedProtocol>,
    Observer: NacelleTelemetryObserver,
{
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                let server = server.clone();
                let tls_config = clone_tls_config(&tls_config);
                tokio::spawn(async move {
                    let _ = serve_accepted_stream(server, stream, tls_config).await;
                });
            }
        }
    }
    Ok(())
}

async fn serve_accepted_stream<H, Observer>(
    server: StressServer<H, Observer>,
    mut stream: TcpStream,
    tls_config: Option<StressTlsConfig>,
) -> Result<(), NacelleError>
where
    H: TcpHandler<LengthDelimitedProtocol>,
    Observer: NacelleTelemetryObserver,
{
    #[cfg(feature = "tls-self-signed")]
    if let Some(tls_config) = tls_config {
        let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
        let tls_stream =
            tokio::time::timeout(tls_config.handshake_timeout(), acceptor.accept(stream))
                .await
                .map_err(|_| NacelleError::Timeout(NacelleTimeoutReason::TlsHandshake))??;
        return server.serve_io(tls_stream).await;
    }

    #[cfg(not(feature = "tls-self-signed"))]
    let _ = tls_config;

    match server {
        StressServer::Shared(server) => {
            let (reader, writer) = stream.split();
            server.serve_halves(reader, writer).await
        }
        StressServer::Serial(server) => {
            let (reader, writer) = stream.split();
            server.serve_halves(reader, writer).await
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn validate_worker_affinity(cpus: Option<&[usize]>) -> Result<(), NacelleError> {
    let Some(cpus) = cpus else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        let available = core_affinity::get_core_ids()
            .ok_or_else(|| std::io::Error::other("unable to discover available worker CPUs"))?;
        for cpu in cpus {
            if !available.iter().any(|core| core.id == *cpu) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("worker CPU {cpu} is outside the process affinity mask"),
                )
                .into());
            }
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpus;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "explicit worker CPU affinity requires Linux",
        )
        .into())
    }
}

fn pin_worker(cpu: Option<usize>) -> Result<(), NacelleError> {
    let Some(cpu) = cpu else {
        return Ok(());
    };
    #[cfg(target_os = "linux")]
    {
        if core_affinity::set_for_current(core_affinity::CoreId { id: cpu }) {
            Ok(())
        } else {
            Err(std::io::Error::other(format!("unable to pin listener worker to CPU {cpu}")).into())
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = cpu;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "explicit worker CPU affinity requires Linux",
        )
        .into())
    }
}

fn spawn_server_thread<H, Observer, Factory>(
    listener: TcpListener,
    tls_config: Option<StressTlsConfig>,
    shutdown: watch::Receiver<bool>,
    cpu: Option<usize>,
    factory: Factory,
    ready: mpsc::Sender<Result<(), String>>,
) -> thread::JoinHandle<Result<(), NacelleError>>
where
    H: TcpHandler<LengthDelimitedProtocol>,
    Observer: NacelleTelemetryObserver,
    Factory: FnOnce() -> Result<StressServer<H, Observer>, NacelleError> + Send + 'static,
{
    thread::spawn(move || {
        let startup = (|| {
            pin_worker(cpu)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(NacelleError::from)?;
            let server = factory()?;
            Ok::<_, NacelleError>((runtime, server))
        })();
        let (runtime, server) = match startup {
            Ok(started) => {
                let _ = ready.send(Ok(()));
                started
            }
            Err(error) => {
                let _ = ready.send(Err(error.to_string()));
                return Err(error);
            }
        };
        drop(ready);
        runtime.block_on(run_server(listener, server, tls_config, shutdown))
    })
}

fn build_tls_config(
    config: &shared::ServerConfig,
) -> Result<Option<StressTlsConfig>, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "tls-self-signed")]
    {
        if config.tls_self_signed {
            let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])?;
            return Ok(Some(generated.tls_config));
        }
        Ok(None)
    }

    #[cfg(not(feature = "tls-self-signed"))]
    {
        if config.tls_self_signed {
            return Err("nacelle-stress-server was built without tls-self-signed support".into());
        }
        Ok(None)
    }
}

const METRICS_EXPORT_INTERVAL: Duration = Duration::from_secs(5);

fn bytes_to_mib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes_f = bytes as f64;
    if bytes_f >= GIB {
        format!("{:.2} GiB", bytes_f / GIB)
    } else if bytes_f >= MIB {
        format!("{:.2} MiB", bytes_f / MIB)
    } else if bytes_f >= KIB {
        format!("{:.2} KiB", bytes_f / KIB)
    } else {
        format!("{bytes} B")
    }
}

// ---------------------------------------------------------------------------
// Metrics console export
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct StressByteMetrics {
    byte_counts: bool,
}

impl From<&shared::ServerConfig> for StressByteMetrics {
    fn from(config: &shared::ServerConfig) -> Self {
        Self {
            byte_counts: config.byte_metrics,
        }
    }
}

#[derive(Debug)]
struct StressMetricsConsole {
    snapshotter: Snapshotter,
    byte_totals: ByteTotals,
    byte_metrics: StressByteMetrics,
    active_connections: i64,
    active_connection_delta: i64,
    active_requests: i64,
    active_streaming_tasks: i64,
    #[cfg(feature = "allocation-counting")]
    previous_allocations: nacelle_stress_common::AllocationSnapshot,
}

impl StressMetricsConsole {
    fn install(
        config: &shared::ServerConfig,
    ) -> Result<Self, metrics::SetRecorderError<ConsoleRecorder>> {
        let recorder = ConsoleRecorder::new();
        let snapshotter = recorder.snapshotter();
        let byte_totals = recorder.byte_totals();
        metrics::set_global_recorder(recorder)?;
        Ok(Self {
            snapshotter,
            byte_totals,
            byte_metrics: StressByteMetrics::from(config),
            active_connections: 0,
            active_connection_delta: 0,
            active_requests: 0,
            active_streaming_tasks: 0,
            #[cfg(feature = "allocation-counting")]
            previous_allocations: nacelle_stress_common::allocation_snapshot(),
        })
    }
}

#[derive(Debug, Default)]
struct StressMetricsSnapshot {
    active_connections: u64,
    active_connection_delta: i64,
    active_requests: u64,
    active_streaming_tasks: u64,
    accepted_connections: u64,
    closed_connections: u64,
    started_requests: u64,
    completed_requests: u64,
    ok_requests: u64,
    failed_requests: u64,
    operation_errors: u64,
    resource_limit_rejections: u64,
    request_bytes: u64,
    response_bytes: u64,
    phase_durations: BTreeMap<String, PhaseDurationSnapshot>,
    #[cfg(feature = "allocation-counting")]
    allocations: nacelle_stress_common::AllocationSnapshot,
}

#[derive(Debug, Default)]
struct PhaseDurationSnapshot {
    count: u64,
    sum_ms: f64,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
}

impl PhaseDurationSnapshot {
    fn add(&mut self, value_ms: f64) {
        self.count = self.count.saturating_add(1);
        self.sum_ms += value_ms;
        self.min_ms = Some(
            self.min_ms
                .map_or(value_ms, |current| current.min(value_ms)),
        );
        self.max_ms = Some(
            self.max_ms
                .map_or(value_ms, |current| current.max(value_ms)),
        );
    }
}

impl StressMetricsConsole {
    fn snapshot(&mut self) -> StressMetricsSnapshot {
        #[cfg(feature = "allocation-counting")]
        let allocations = {
            let current = nacelle_stress_common::allocation_snapshot();
            let delta = current.delta_since(self.previous_allocations);
            self.previous_allocations = current;
            delta
        };
        let (request_bytes, response_bytes) = self.byte_totals.take();
        let mut snapshot = StressMetricsSnapshot {
            request_bytes,
            response_bytes,
            #[cfg(feature = "allocation-counting")]
            allocations,
            ..StressMetricsSnapshot::default()
        };
        for (key, _, _, value) in self.snapshotter.snapshot().into_vec() {
            let metric_name = key.key().name();
            match value {
                DebugValue::Counter(value) => match metric_name {
                    "server.connection.accepted" => {
                        snapshot.accepted_connections += value;
                    }
                    "server.connection.closed" => snapshot.closed_connections += value,
                    "server.request.started" => snapshot.started_requests += value,
                    "server.request.completed" => {
                        snapshot.completed_requests += value;
                        if metric_has_label(key.key(), "status", "ok") {
                            snapshot.ok_requests += value;
                        } else if metric_has_label(key.key(), "status", "error") {
                            snapshot.failed_requests += value;
                        }
                    }
                    "server.request.failed" => snapshot.failed_requests += value,
                    "server.errors" => snapshot.operation_errors += value,
                    "server.resource_limit.rejections" => {
                        snapshot.resource_limit_rejections += value;
                    }
                    _ => {}
                },
                DebugValue::Gauge(value) => {
                    let delta = value.into_inner() as i64;
                    match metric_name {
                        "server.connection.active" if metric_has_key(key.key(), "listener") => {
                            self.active_connection_delta =
                                self.active_connection_delta.saturating_add(delta);
                        }
                        "server.connection.active" => {
                            self.active_connections = self.active_connections.saturating_add(delta);
                        }
                        "server.request.active" if !metric_has_key(key.key(), "protocol") => {
                            self.active_requests = self.active_requests.saturating_add(delta);
                        }
                        "server.streaming_task.active" => {
                            self.active_streaming_tasks =
                                self.active_streaming_tasks.saturating_add(delta);
                        }
                        _ => {}
                    }
                }
                DebugValue::Histogram(values) if metric_name == "server.phase.duration_ms" => {
                    let Some(phase) = key
                        .key()
                        .labels()
                        .find(|label| label.key() == "phase")
                        .map(|label| label.value().to_owned())
                    else {
                        continue;
                    };
                    let duration = snapshot.phase_durations.entry(phase).or_default();
                    for value in values {
                        duration.add(value.into_inner());
                    }
                }
                DebugValue::Histogram(_) => {}
            }
        }
        snapshot.active_connections = self.active_connections.max(0) as u64;
        snapshot.active_connection_delta = self.active_connection_delta;
        snapshot.active_requests = self.active_requests.max(0) as u64;
        snapshot.active_streaming_tasks = self.active_streaming_tasks.max(0) as u64;
        snapshot
    }
}

fn metric_has_label(key: &metrics::Key, label_key: &str, label_value: &str) -> bool {
    key.labels()
        .any(|label| label.key() == label_key && label.value() == label_value)
}

fn metric_has_key(key: &metrics::Key, label_key: &str) -> bool {
    key.labels().any(|label| label.key() == label_key)
}

async fn run_metrics_console(
    mut console: StressMetricsConsole,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(METRICS_EXPORT_INTERVAL);
    interval.tick().await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    print_metrics_snapshot(console.snapshot(), console.byte_metrics);
                    break;
                }
            }
            _ = interval.tick() => {
                print_metrics_snapshot(console.snapshot(), console.byte_metrics);
            }
        }
    }
}

fn print_metrics_snapshot(snapshot: StressMetricsSnapshot, byte_metrics: StressByteMetrics) {
    let interval_secs = METRICS_EXPORT_INTERVAL.as_secs_f64();
    println!("nacelle-stress-server metrics window={interval_secs:.1}s");
    println!(
        "  connections  active={} delta={} accepted={} ({:.2}/s) closed={} ({:.2}/s)",
        snapshot.active_connections,
        snapshot.active_connection_delta,
        snapshot.accepted_connections,
        snapshot.accepted_connections as f64 / interval_secs,
        snapshot.closed_connections,
        snapshot.closed_connections as f64 / interval_secs,
    );
    println!(
        "  requests     active={} started={} ({:.2}/s) completed={} ({:.2}/s) ok={} failed={} ({:.2}/s) operation_errors={} resource_limit_rejections={}",
        snapshot.active_requests,
        snapshot.started_requests,
        snapshot.started_requests as f64 / interval_secs,
        snapshot.completed_requests,
        snapshot.completed_requests as f64 / interval_secs,
        snapshot.ok_requests,
        snapshot.failed_requests,
        snapshot.failed_requests as f64 / interval_secs,
        snapshot.operation_errors,
        snapshot.resource_limit_rejections,
    );
    if byte_metrics.byte_counts {
        println!(
            "  bytes        request={} ({:.2} MiB/s) response={} ({:.2} MiB/s)",
            format_bytes(snapshot.request_bytes),
            bytes_to_mib(snapshot.request_bytes) / interval_secs,
            format_bytes(snapshot.response_bytes),
            bytes_to_mib(snapshot.response_bytes) / interval_secs,
        );
    } else {
        println!("  bytes        byte_metrics=off");
    }
    println!(
        "  runtime      streaming_tasks_active={}",
        snapshot.active_streaming_tasks,
    );
    #[cfg(feature = "allocation-counting")]
    {
        let allocations = snapshot.allocations;
        println!(
            "  allocations  instrumented=rust-global-allocator successful={} realloc={} requested_bytes={} frees={}",
            allocations.successful_allocations(),
            allocations.realloc_calls,
            allocations.requested_bytes,
            allocations.free_calls,
        );
        if snapshot.completed_requests > 0 {
            println!(
                "  allocation_cost calls_per_request={:.6} requested_bytes_per_request={:.6}",
                allocations.successful_allocations() as f64 / snapshot.completed_requests as f64,
                allocations.requested_bytes as f64 / snapshot.completed_requests as f64,
            );
        }
    }
    for (phase, duration) in snapshot.phase_durations {
        let mean_ms = if duration.count == 0 {
            0.0
        } else {
            duration.sum_ms / duration.count as f64
        };
        println!(
            "  phase        name={phase} count={} mean_ms={mean_ms:.6} min_ms={:.6} max_ms={:.6}",
            duration.count,
            duration.min_ms.unwrap_or_default(),
            duration.max_ms.unwrap_or_default(),
        );
    }
}

async fn wait_for_shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Arc::new(parse_args(std::env::args().skip(1), "tokio")?);
    validate_worker_affinity(config.worker_cpus.as_deref())?;
    let single_loop = (!cfg!(target_os = "linux") || config.server_threads <= 1)
        && !config.worker_local_response
        && config.worker_cpus.is_none()
        && config.response_write_mode == ResponseWriteMode::Immediate;
    let mut runtime = if single_loop {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut runtime = tokio::runtime::Builder::new_multi_thread();
        runtime.worker_threads(1);
        runtime
    };
    runtime.enable_all().build()?.block_on(run(config))
}

async fn run(config: Arc<ServerConfig>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    configure_allocator(config.low_memory);

    let metrics_console = StressMetricsConsole::install(&config)?;

    let telemetry = NacelleTelemetry::default()
        .with_byte_count_metrics(config.byte_metrics)
        .with_phase_duration_metrics(config.phase_duration_metrics);
    let runtime_state = build_runtime_state(&config)?;
    let server = if config.worker_local_response {
        None
    } else {
        Some(build_server_with_runtime_state(
            &config,
            runtime_state.clone(),
            telemetry.clone(),
        )?)
    };
    let tls_config = build_tls_config(&config)?;

    #[cfg(not(target_os = "linux"))]
    let n_server_threads = {
        if config.server_threads > 1 {
            eprintln!(
                "note: --server-threads > 1 requires Linux SO_REUSEPORT; using 1 server thread"
            );
        }
        1_usize
    };
    #[cfg(target_os = "linux")]
    let n_server_threads = config.server_threads.max(1);

    print_config(&config, "tokio", n_server_threads);

    let use_reuseport = n_server_threads > 1;
    let first_socket = make_server_socket(&config.bind, use_reuseport)?;
    first_socket.bind(config.bind)?;
    let first_listener = first_socket.listen(1024)?;
    let listen_addr = first_listener.local_addr()?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (metrics_shutdown_tx, metrics_shutdown_rx) = watch::channel(false);
    let metrics_task = tokio::spawn(run_metrics_console(metrics_console, metrics_shutdown_rx));

    if n_server_threads == 1 && !config.worker_local_response && config.worker_cpus.is_none() {
        println!(
            "nacelle-stress-server listening on {} (threads=1 transport={})",
            listen_addr,
            if config.tls_self_signed {
                "tcp-tls"
            } else {
                "tcp"
            },
        );
        let server = server.ok_or("shared server was not initialized")?;
        let server_task = tokio::spawn(run_server(first_listener, server, tls_config, shutdown_rx));
        wait_for_shutdown_signal().await?;
        eprintln!("\nshutting down...");
        let _ = shutdown_tx.send(true);
        server_task.await??;
        let _ = metrics_shutdown_tx.send(true);
        metrics_task.await?;
        return Ok(());
    }

    let mut listeners = Vec::with_capacity(n_server_threads);
    listeners.push(first_listener);
    for _ in 1..n_server_threads {
        let socket = make_server_socket(&listen_addr, true)?;
        socket.bind(listen_addr)?;
        listeners.push(socket.listen(1024)?);
    }
    let mut server_tasks = Vec::with_capacity(n_server_threads);
    let (ready_tx, ready_rx) = mpsc::channel();
    for (index, listener) in listeners.into_iter().enumerate() {
        let server = server.clone();
        let config = config.clone();
        let state = runtime_state.clone();
        let telemetry = telemetry.clone();
        let cpu = config.worker_cpus.as_ref().map(|cpus| cpus[index]);
        server_tasks.push(spawn_server_thread(
            listener,
            clone_tls_config(&tls_config),
            shutdown_rx.clone(),
            cpu,
            move || match server {
                Some(server) => Ok(server),
                None => build_server_with_runtime_state(&config, state, telemetry),
            },
            ready_tx.clone(),
        ));
    }
    drop(ready_tx);
    for _ in 0..n_server_threads {
        let error = match ready_rx.recv() {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(_) => "listener worker stopped before completing startup".to_owned(),
        };
        let _ = shutdown_tx.send(true);
        for task in server_tasks {
            let _ = task.join();
        }
        let _ = metrics_shutdown_tx.send(true);
        metrics_task.await?;
        return Err(error.into());
    }
    println!(
        "nacelle-stress-server listening on {} (threads={} transport={})",
        listen_addr,
        n_server_threads,
        if config.tls_self_signed {
            "tcp-tls"
        } else {
            "tcp"
        },
    );

    wait_for_shutdown_signal().await?;
    eprintln!("\nshutting down...");
    let _ = shutdown_tx.send(true);
    for task in server_tasks {
        task.join()
            .map_err(|_| "server thread panicked")?
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
    }
    let _ = metrics_shutdown_tx.send(true);
    metrics_task.await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[cfg(all(test, target_os = "linux"))]
mod worker_tests {
    use super::*;

    #[tokio::test]
    async fn worker_is_pinned_before_server_initialization() {
        let before = core_affinity::get_core_ids().expect("available CPUs");
        let cpu = before[0].id;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let config = shared::ServerConfig::default();
        let state = build_runtime_state(&config).expect("runtime state");
        let (shutdown, receiver) = watch::channel(false);
        let (ready, started) = mpsc::channel();
        let worker = spawn_server_thread(
            listener,
            None,
            receiver,
            Some(cpu),
            move || {
                let current = core_affinity::get_core_ids().expect("worker CPUs");
                assert_eq!(current.len(), 1);
                assert_eq!(current[0].id, cpu);
                build_server_with_runtime_state(&config, state, NacelleTelemetry::default())
            },
            ready,
        );
        started
            .recv_timeout(Duration::from_secs(3))
            .expect("worker startup")
            .expect("worker initialization");
        shutdown.send(true).expect("shutdown");
        worker.join().expect("worker join").expect("worker result");
        let after = core_affinity::get_core_ids().expect("parent CPUs");
        assert_eq!(
            before.iter().map(|core| core.id).collect::<Vec<_>>(),
            after.iter().map(|core| core.id).collect::<Vec<_>>()
        );
        assert!(validate_worker_affinity(Some(&[usize::MAX])).is_err());
    }

    #[tokio::test]
    async fn worker_reports_initialization_failure_before_serving() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let config = shared::ServerConfig::default();
        let state = build_runtime_state(&config).expect("runtime state");
        let (_, receiver) = watch::channel(false);
        let (ready, started) = mpsc::channel();
        let worker = spawn_server_thread(
            listener,
            None,
            receiver,
            None,
            move || {
                let server =
                    build_server_with_runtime_state(&config, state, NacelleTelemetry::default())?;
                Err::<(), NacelleError>(
                    std::io::Error::other("synthetic worker initialization failure").into(),
                )
                .map(|()| server)
            },
            ready,
        );
        let error = started
            .recv_timeout(Duration::from_secs(3))
            .expect("startup result")
            .expect_err("startup should fail");
        assert!(error.contains("synthetic worker initialization failure"));
        assert!(worker.join().expect("worker join").is_err());
    }
}
