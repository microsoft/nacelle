#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[path = "shared.rs"]
mod shared;
use shared::{build_server, configure_allocator, parse_args, print_config};

use std::net::SocketAddr;
#[cfg(feature = "otel")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
#[cfg(feature = "otel")]
use std::time::Duration;

#[cfg(feature = "tls-self-signed")]
use nacelle::NacelleTlsConfig;
use nacelle::{Handler, NacelleError, NacelleTelemetry, TcpServer};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};
use nacelle_stress_common::make_tcp_socket;
#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry_sdk::Resource;
#[cfg(feature = "otel")]
use opentelemetry_sdk::error::OTelSdkResult;
#[cfg(feature = "otel")]
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics, Sum};
#[cfg(feature = "otel")]
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
#[cfg(feature = "otel")]
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
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

async fn run_server<H>(
    listener: TcpListener,
    server: TcpServer<FrameRequest, LengthDelimitedProtocol, H>,
    tls_config: Option<StressTlsConfig>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), NacelleError>
where
    H: Handler,
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

async fn serve_accepted_stream<H>(
    server: TcpServer<FrameRequest, LengthDelimitedProtocol, H>,
    stream: TcpStream,
    tls_config: Option<StressTlsConfig>,
) -> Result<(), NacelleError>
where
    H: Handler,
{
    #[cfg(feature = "tls-self-signed")]
    if let Some(tls_config) = tls_config {
        let acceptor = tokio_rustls::TlsAcceptor::from(tls_config.server_config());
        let tls_stream =
            tokio::time::timeout(tls_config.handshake_timeout(), acceptor.accept(stream))
                .await
                .map_err(|_| NacelleError::Timeout("tls_handshake"))??;
        return server.serve_io(tls_stream).await;
    }

    #[cfg(not(feature = "tls-self-signed"))]
    let _ = tls_config;

    server.serve_io(stream).await
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn spawn_server_thread<H>(
    listener: TcpListener,
    server: TcpServer<FrameRequest, LengthDelimitedProtocol, H>,
    tls_config: Option<StressTlsConfig>,
    shutdown: watch::Receiver<bool>,
) -> thread::JoinHandle<Result<(), NacelleError>>
where
    H: Handler,
{
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NacelleError::from)?;
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

#[cfg(feature = "otel")]
const OTEL_EXPORT_INTERVAL: Duration = Duration::from_secs(5);

#[cfg(feature = "otel")]
fn bytes_to_mib(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

#[cfg(feature = "otel")]
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
// OpenTelemetry console export
// ---------------------------------------------------------------------------

#[cfg(feature = "otel")]
fn init_otel_console_exporter(config: &shared::ServerConfig) -> SdkMeterProvider {
    let exporter = StressConsoleMetricExporter::new(StressOtelByteMetrics::from(config));
    let reader = PeriodicReader::builder(exporter)
        .with_interval(OTEL_EXPORT_INTERVAL)
        .build();
    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            Resource::builder()
                .with_service_name("nacelle-stress-server")
                .build(),
        )
        .build();
    global::set_meter_provider(provider.clone());
    provider
}

#[cfg(not(feature = "otel"))]
#[derive(Debug)]
struct DisabledOtelConsoleExporter;

#[cfg(not(feature = "otel"))]
fn init_otel_console_exporter(_config: &shared::ServerConfig) -> DisabledOtelConsoleExporter {
    DisabledOtelConsoleExporter
}

#[cfg(feature = "otel")]
#[derive(Debug, Clone, Copy)]
struct StressOtelByteMetrics {
    byte_counts: bool,
}

#[cfg(feature = "otel")]
impl From<&shared::ServerConfig> for StressOtelByteMetrics {
    fn from(config: &shared::ServerConfig) -> Self {
        Self {
            byte_counts: config.byte_metrics,
        }
    }
}

#[cfg(feature = "otel")]
#[derive(Debug)]
struct StressConsoleMetricExporter {
    shutdown: AtomicBool,
    byte_metrics: StressOtelByteMetrics,
}

#[cfg(feature = "otel")]
impl StressConsoleMetricExporter {
    fn new(byte_metrics: StressOtelByteMetrics) -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            byte_metrics,
        }
    }
}

#[cfg(feature = "otel")]
impl PushMetricExporter for StressConsoleMetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown);
        }

        print_otel_snapshot(
            StressOtelSnapshot::from_resource_metrics(metrics),
            self.byte_metrics,
        );
        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        self.shutdown.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        Temporality::Delta
    }
}

#[cfg(feature = "otel")]
#[derive(Debug, Default)]
struct StressOtelSnapshot {
    active_connections: u64,
    active_connection_delta: i64,
    active_requests: u64,
    active_streaming_tasks: u64,
    memory_used_bytes: u64,
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
}

#[cfg(feature = "otel")]
impl StressOtelSnapshot {
    fn from_resource_metrics(metrics: &ResourceMetrics) -> Self {
        let mut snapshot = Self::default();
        for scope in metrics.scope_metrics() {
            for metric in scope.metrics() {
                match metric.name() {
                    "nacelle.connections.active" => {
                        snapshot.active_connections += gauge_u64(metric.data());
                    }
                    "nacelle.requests.active" => {
                        snapshot.active_requests += gauge_u64(metric.data());
                    }
                    "nacelle.streaming_tasks.active" => {
                        snapshot.active_streaming_tasks += gauge_u64(metric.data());
                    }
                    "nacelle.memory.used_bytes" => {
                        snapshot.memory_used_bytes += gauge_u64(metric.data());
                    }
                    "nacelle.connections.in_flight" => {
                        snapshot.active_connection_delta += sum_i64(metric.data());
                    }
                    "nacelle.connections.accepted" => {
                        snapshot.accepted_connections += sum_u64(metric.data());
                    }
                    "nacelle.connections.closed" => {
                        snapshot.closed_connections += sum_u64(metric.data());
                    }
                    "nacelle.requests.started" => {
                        snapshot.started_requests += sum_u64(metric.data());
                    }
                    "nacelle.requests.completed" => {
                        snapshot.completed_requests += sum_u64(metric.data());
                        snapshot.ok_requests +=
                            sum_u64_with_attribute(metric.data(), "status", "ok");
                        snapshot.failed_requests +=
                            sum_u64_with_attribute(metric.data(), "status", "error");
                    }
                    "nacelle.errors" => {
                        snapshot.operation_errors += sum_u64(metric.data());
                    }
                    "nacelle.resource_limit.rejections" => {
                        snapshot.resource_limit_rejections += sum_u64(metric.data());
                    }
                    "nacelle.request.bytes" => {
                        snapshot.request_bytes += sum_u64(metric.data());
                    }
                    "nacelle.response.bytes" => {
                        snapshot.response_bytes += sum_u64(metric.data());
                    }
                    _ => {}
                }
            }
        }
        snapshot
    }
}

#[cfg(feature = "otel")]
fn print_otel_snapshot(snapshot: StressOtelSnapshot, byte_metrics: StressOtelByteMetrics) {
    let interval_secs = OTEL_EXPORT_INTERVAL.as_secs_f64();
    println!("nacelle-stress-server otel window={interval_secs:.1}s");
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
        "  runtime      streaming_tasks_active={} memory_used={}",
        snapshot.active_streaming_tasks,
        format_bytes(snapshot.memory_used_bytes),
    );
}

#[cfg(feature = "otel")]
fn gauge_u64(metrics: &AggregatedMetrics) -> u64 {
    match metrics {
        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
            gauge.data_points().map(|point| point.value()).sum()
        }
        _ => 0,
    }
}

#[cfg(feature = "otel")]
fn sum_u64(metrics: &AggregatedMetrics) -> u64 {
    match metrics {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
            sum.data_points().map(|point| point.value()).sum()
        }
        _ => 0,
    }
}

#[cfg(feature = "otel")]
fn sum_i64(metrics: &AggregatedMetrics) -> i64 {
    match metrics {
        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
            sum.data_points().map(|point| point.value()).sum()
        }
        _ => 0,
    }
}

#[cfg(feature = "otel")]
fn sum_u64_with_attribute(metrics: &AggregatedMetrics, key: &str, value: &str) -> u64 {
    match metrics {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum_points_with_attribute(sum, key, value),
        _ => 0,
    }
}

#[cfg(feature = "otel")]
fn sum_points_with_attribute(sum: &Sum<u64>, key: &str, value: &str) -> u64 {
    sum.data_points()
        .filter(|point| {
            point
                .attributes()
                .any(|attribute| attribute.key.as_str() == key && attribute.value.as_str() == value)
        })
        .map(|point| point.value())
        .sum()
}

#[cfg(feature = "otel")]
fn shutdown_otel_console_exporter(provider: &SdkMeterProvider) -> OTelSdkResult {
    provider.shutdown()
}

#[cfg(not(feature = "otel"))]
fn shutdown_otel_console_exporter(
    _provider: &DisabledOtelConsoleExporter,
) -> Result<(), std::convert::Infallible> {
    Ok(())
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = parse_args(std::env::args().skip(1), "tokio")?;
    configure_allocator(config.low_memory);

    #[cfg(feature = "otel")]
    let meter_provider = init_otel_console_exporter(&config);
    #[cfg(not(feature = "otel"))]
    let meter_provider = init_otel_console_exporter(&config);

    let telemetry = NacelleTelemetry::default().with_byte_count_metrics(config.byte_metrics);
    let server = build_server(&config)?.with_telemetry(telemetry);
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

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    if n_server_threads == 1 {
        let server_task = tokio::spawn(run_server(first_listener, server, tls_config, shutdown_rx));
        wait_for_shutdown_signal().await?;
        eprintln!("\nshutting down...");
        let _ = shutdown_tx.send(true);
        server_task.await??;
        shutdown_otel_console_exporter(&meter_provider)?;
        return Ok(());
    }

    let mut server_tasks = Vec::with_capacity(n_server_threads);
    server_tasks.push(spawn_server_thread(
        first_listener,
        server.clone(),
        clone_tls_config(&tls_config),
        shutdown_rx.clone(),
    ));
    for _ in 1..n_server_threads {
        let socket = make_server_socket(&listen_addr, true)?;
        socket.bind(listen_addr)?;
        let listener = socket.listen(1024)?;
        server_tasks.push(spawn_server_thread(
            listener,
            server.clone(),
            clone_tls_config(&tls_config),
            shutdown_rx.clone(),
        ));
    }

    wait_for_shutdown_signal().await?;
    eprintln!("\nshutting down...");
    let _ = shutdown_tx.send(true);
    for task in server_tasks {
        task.join()
            .map_err(|_| "server thread panicked")?
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?;
    }
    shutdown_otel_console_exporter(&meter_provider)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------
