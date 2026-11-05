// Shared service/config logic for the Tokio stress server.

use std::net::SocketAddr;
#[cfg(feature = "mimalloc-allocator")]
use std::os::raw::c_long;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::telemetry::{NacelleTelemetryObserver, NoopObserver};
use nacelle::core::{NacelleError, NacelleLimits, NacelleRuntimeState, NacelleTelemetry};
use nacelle::tcp::{
    NacelleTcpConfig, NacelleTcpLimits, ResponseWritePolicy, SerialTcpHandler,
    SerialTcpRequestContext, SerialTcpServer, TcpHandler, TcpHeadReadLimits, TcpRequestContext,
    TcpResponse, TcpServer,
};
use nacelle_reference_protocol::LengthDelimitedProtocol;
use nacelle_stress_common::STRESS_OPCODE;
use serde::Deserialize;

const DEFAULT_CONFIG_PATH: &str = "config.toml";
static DEFAULT_RESPONSE_PAYLOAD: [u8; 64] = [0x5A; 64];

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum HandlerMode {
    #[default]
    Shared,
    Serial,
}

impl FromStr for HandlerMode {
    type Err = std::io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "shared" => Ok(Self::Shared),
            "serial" => Ok(Self::Serial),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid handler mode {value:?}; expected shared or serial"),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ResponseWriteMode {
    #[default]
    Immediate,
    CoalesceBuffered,
}

impl FromStr for ResponseWriteMode {
    type Err = std::io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "immediate" => Ok(Self::Immediate),
            "coalesce-buffered" => Ok(Self::CoalesceBuffered),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "invalid response write mode {value:?}; expected immediate or coalesce-buffered"
                ),
            )),
        }
    }
}

impl From<ResponseWriteMode> for ResponseWritePolicy {
    fn from(mode: ResponseWriteMode) -> Self {
        match mode {
            ResponseWriteMode::Immediate => Self::Immediate,
            ResponseWriteMode::CoalesceBuffered => Self::CoalesceBuffered,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub config_sources: Vec<String>,
    pub bind: SocketAddr,
    pub server_threads: usize,
    /// Optional ordered logical CPU IDs, one per effective listener worker.
    pub worker_cpus: Option<Vec<usize>>,
    pub handler_mode: HandlerMode,
    pub handler_timeout_disabled: bool,
    pub tcp_timeouts_disabled: bool,
    pub response_bytes: usize,
    /// Allocate a response template on each listener worker instead of sharing one.
    pub worker_local_response: bool,
    pub response_write_mode: ResponseWriteMode,
    pub read_buffer_capacity: usize,
    pub response_buffer_capacity: usize,
    pub request_body_chunk_size: usize,
    pub request_body_channel_capacity: usize,
    pub head_read_limits: Option<TcpHeadReadLimits>,
    pub low_memory: bool,
    pub byte_metrics: bool,
    pub phase_duration_metrics: bool,
    pub tls_self_signed: bool,
    pub limits: NacelleLimits,
    pub connection_limits: nacelle::core::NacelleConnectionLimits,
    pub tcp_limits: NacelleTcpLimits,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            config_sources: vec!["code defaults".to_string()],
            bind: SocketAddr::from(([127, 0, 0, 1], 7878)),
            server_threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            worker_cpus: None,
            handler_mode: HandlerMode::Shared,
            handler_timeout_disabled: false,
            tcp_timeouts_disabled: false,
            response_bytes: 64,
            worker_local_response: false,
            response_write_mode: ResponseWriteMode::Immediate,
            read_buffer_capacity: 64 * 1024,
            response_buffer_capacity: 16 * 1024,
            request_body_chunk_size: 16 * 1024,
            request_body_channel_capacity: 8,
            head_read_limits: None,
            low_memory: false,
            byte_metrics: true,
            phase_duration_metrics: false,
            tls_self_signed: false,
            limits: NacelleLimits::default(),
            connection_limits: nacelle::core::NacelleConnectionLimits::default(),
            tcp_limits: NacelleTcpLimits::default(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerConfigFile {
    bind: Option<SocketAddr>,
    server_threads: Option<usize>,
    worker_cpus: Option<Vec<usize>>,
    handler_mode: Option<HandlerMode>,
    response_bytes: Option<usize>,
    worker_local_response: Option<bool>,
    response_write_mode: Option<ResponseWriteMode>,
    read_buffer_capacity: Option<usize>,
    response_buffer_capacity: Option<usize>,
    request_body_chunk_size: Option<usize>,
    request_body_channel_capacity: Option<usize>,
    head_read_limits: Option<HeadReadLimitsConfigFile>,
    low_memory: Option<bool>,
    byte_metrics: Option<bool>,
    phase_duration_metrics: Option<bool>,
    tls_self_signed: Option<bool>,
    limits: Option<LimitsConfigFile>,
    connection_limits: Option<ConnectionLimitsConfigFile>,
    tcp_limits: Option<TcpLimitsConfigFile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsConfigFile {
    max_connections_per_peer: Option<usize>,
    connection_rate_limit_table_capacity: Option<usize>,
    max_in_flight_requests: Option<usize>,
    max_streaming_tasks: Option<usize>,
    max_request_body_bytes: Option<usize>,
    max_response_body_bytes: Option<usize>,
    handler_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionLimitsConfigFile {
    max_connections: Option<usize>,
    max_connection_opens_per_peer_per_second: Option<usize>,
    unlimited_connections: Option<bool>,
    disable_connection_open_rate: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TcpLimitsConfigFile {
    read_timeout_ms: Option<u64>,
    write_timeout_ms: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
    idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadReadLimitsConfigFile {
    max_head_bytes: usize,
    max_undecided_bytes: usize,
}

impl ServerConfig {
    fn apply_file(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        let config = std::fs::read_to_string(path)?;
        let file = toml::from_str::<ServerConfigFile>(&config)?;
        self.apply_config_file(file)?;
        self.config_sources.push(format!("toml {}", path.display()));
        Ok(())
    }

    fn apply_config_file(
        &mut self,
        file: ServerConfigFile,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let head_read_limits = file
            .head_read_limits
            .as_ref()
            .map(|limits| TcpHeadReadLimits::new(limits.max_head_bytes, limits.max_undecided_bytes))
            .transpose()?;
        if let Some(limits) = &file.connection_limits {
            if limits.max_connections.is_some() && limits.unlimited_connections == Some(true) {
                return Err("choose max_connections or unlimited_connections, not both".into());
            }
            if limits.max_connection_opens_per_peer_per_second.is_some()
                && limits.disable_connection_open_rate == Some(true)
            {
                return Err(
                    "choose a connection-open rate or disable_connection_open_rate, not both"
                        .into(),
                );
            }
        }
        if let Some(bind) = file.bind {
            self.bind = bind;
        }
        if let Some(server_threads) = file.server_threads {
            self.server_threads = server_threads;
        }
        if let Some(worker_cpus) = file.worker_cpus {
            self.worker_cpus = Some(worker_cpus);
        }
        if let Some(handler_mode) = file.handler_mode {
            self.handler_mode = handler_mode;
        }
        if let Some(response_bytes) = file.response_bytes {
            self.response_bytes = response_bytes;
        }
        if let Some(worker_local_response) = file.worker_local_response {
            self.worker_local_response = worker_local_response;
        }
        if let Some(response_write_mode) = file.response_write_mode {
            self.response_write_mode = response_write_mode;
        }
        if let Some(read_buffer_capacity) = file.read_buffer_capacity {
            self.read_buffer_capacity = read_buffer_capacity;
        }
        if let Some(response_buffer_capacity) = file.response_buffer_capacity {
            self.response_buffer_capacity = response_buffer_capacity;
        }
        if let Some(request_body_chunk_size) = file.request_body_chunk_size {
            self.request_body_chunk_size = request_body_chunk_size;
        }
        if let Some(request_body_channel_capacity) = file.request_body_channel_capacity {
            self.request_body_channel_capacity = request_body_channel_capacity;
        }
        if let Some(limits) = head_read_limits {
            self.head_read_limits = Some(limits);
        }
        if let Some(low_memory) = file.low_memory {
            self.low_memory = low_memory;
        }
        if let Some(byte_metrics) = file.byte_metrics {
            self.byte_metrics = byte_metrics;
        }
        if let Some(phase_duration_metrics) = file.phase_duration_metrics {
            self.phase_duration_metrics = phase_duration_metrics;
        }
        if let Some(tls_self_signed) = file.tls_self_signed {
            self.tls_self_signed = tls_self_signed;
        }
        if let Some(limits) = file.limits {
            self.apply_limits_file(limits);
        }
        if let Some(limits) = file.connection_limits {
            if let Some(max) = limits.max_connections {
                self.connection_limits.max_connections = Some(max);
            }
            if limits.unlimited_connections == Some(true) {
                self.connection_limits.max_connections = None;
            }
            if let Some(rate) = limits.max_connection_opens_per_peer_per_second {
                self.connection_limits
                    .max_connection_opens_per_peer_per_second = Some(rate);
            }
            if limits.disable_connection_open_rate == Some(true) {
                self.connection_limits
                    .max_connection_opens_per_peer_per_second = None;
            }
        }
        if let Some(tcp_limits) = file.tcp_limits {
            self.apply_tcp_limits_file(tcp_limits);
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.server_threads == 0 {
            return Err("--server-threads must be greater than zero".into());
        }
        let effective_workers = if cfg!(target_os = "linux") {
            self.server_threads.max(1)
        } else {
            1
        };
        if let Some(cpus) = &self.worker_cpus {
            if !cfg!(target_os = "linux") {
                return Err("--worker-cpus requires Linux".into());
            }
            if cpus.is_empty() {
                return Err("--worker-cpus must contain at least one CPU ID".into());
            }
            let mut unique = cpus.clone();
            unique.sort_unstable();
            if unique.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err("--worker-cpus must not contain duplicate CPU IDs".into());
            }
            if cpus.len() != effective_workers {
                return Err(format!(
                    "--worker-cpus must contain {effective_workers} CPU IDs, one per listener worker"
                )
                .into());
            }
        }
        let template_count = if self.worker_local_response {
            effective_workers
        } else {
            1
        };
        if self.response_bytes.checked_mul(template_count).is_none() {
            return Err("total response template byte budget overflows usize".into());
        }
        if self.phase_duration_metrics && !cfg!(feature = "phase-timing") {
            return Err(
                "phase duration metrics require the nacelle-stress-server phase-timing feature"
                    .into(),
            );
        }
        Ok(())
    }

    fn apply_limits_file(&mut self, file: LimitsConfigFile) {
        if let Some(max_connections_per_peer) = file.max_connections_per_peer {
            self.limits.max_connections_per_peer = Some(max_connections_per_peer.max(1));
        }
        if let Some(capacity) = file.connection_rate_limit_table_capacity {
            self.limits.connection_rate_limit_table_capacity = capacity;
        }
        if let Some(max_in_flight_requests) = file.max_in_flight_requests {
            self.limits.max_in_flight_requests = max_in_flight_requests.max(1);
        }
        if let Some(max_streaming_tasks) = file.max_streaming_tasks {
            self.limits.max_streaming_tasks = max_streaming_tasks.max(1);
        }
        if let Some(max_request_body_bytes) = file.max_request_body_bytes {
            self.limits.max_request_body_bytes = max_request_body_bytes;
        }
        if let Some(max_response_body_bytes) = file.max_response_body_bytes {
            self.limits.max_response_body_bytes = max_response_body_bytes;
        }
        if let Some(handler_timeout_ms) = file.handler_timeout_ms {
            self.limits.handler_timeout = Some(Duration::from_millis(handler_timeout_ms));
        }
    }

    fn apply_tcp_limits_file(&mut self, file: TcpLimitsConfigFile) {
        if let Some(read_timeout_ms) = file.read_timeout_ms {
            self.tcp_limits.read_timeout = Some(Duration::from_millis(read_timeout_ms));
        }
        if let Some(write_timeout_ms) = file.write_timeout_ms {
            self.tcp_limits.write_timeout = Some(Duration::from_millis(write_timeout_ms));
        }
        if let Some(shutdown_timeout_ms) = file.shutdown_timeout_ms {
            self.tcp_limits.shutdown_timeout = Some(Duration::from_millis(shutdown_timeout_ms));
        }
        if let Some(idle_timeout_ms) = file.idle_timeout_ms {
            self.tcp_limits.idle_timeout = Some(Duration::from_millis(idle_timeout_ms));
        }
    }

    fn disable_handler_timeout(&mut self) {
        self.handler_timeout_disabled = true;
        self.limits.handler_timeout = None;
    }

    fn disable_tcp_timeouts(&mut self) {
        self.tcp_timeouts_disabled = true;
        self.tcp_limits.read_timeout = None;
        self.tcp_limits.write_timeout = None;
        self.tcp_limits.shutdown_timeout = None;
        self.tcp_limits.idle_timeout = None;
    }

    fn disable_timeouts(&mut self) {
        self.disable_handler_timeout();
        self.disable_tcp_timeouts();
    }
}

/// mimalloc v2 option constants not yet exposed by `libmimalloc-sys`.
/// Values match the `mi_option_e` enum in mimalloc v2 `mimalloc/types.h`.
/// These are stable within v2 and `libmimalloc-sys` always compiles v2 unless
/// the `v3` feature is explicitly enabled.
#[cfg(feature = "mimalloc-allocator")]
const MI_OPTION_ARENA_EAGER_COMMIT: libmimalloc_sys::mi_option_t = 4;
#[cfg(feature = "mimalloc-allocator")]
const MI_OPTION_PURGE_DELAY: libmimalloc_sys::mi_option_t = 15;

/// Configures mimalloc for minimal OS memory retention when `low_memory` is
/// true.  Call this once at the start of `main()`, before spawning threads.
///
/// The same effect can be achieved without recompiling by setting environment
/// variables before launching the server:
///   `MIMALLOC_PURGE_DELAY=0 MIMALLOC_ARENA_EAGER_COMMIT=0 ./server`
#[cfg(feature = "mimalloc-allocator")]
pub fn configure_allocator(low_memory: bool) {
    if !low_memory {
        return;
    }
    unsafe {
        // Return freed pages to the OS immediately instead of holding them for
        // a default grace period (~100 ms).  Reduces RSS after traffic spikes.
        libmimalloc_sys::mi_option_set(MI_OPTION_PURGE_DELAY, 0 as c_long);
        // Do not pre-commit arena memory up-front; commit only as needed.
        libmimalloc_sys::mi_option_set(MI_OPTION_ARENA_EAGER_COMMIT, 0 as c_long);
    }
}

#[cfg(not(feature = "mimalloc-allocator"))]
pub fn configure_allocator(low_memory: bool) {
    assert!(
        !low_memory,
        "low-memory mode requires the mimalloc-allocator feature"
    );
}

#[derive(Clone)]
pub struct SerialEchoHandler {
    response_payload: Bytes,
}

impl SerialTcpHandler<LengthDelimitedProtocol> for SerialEchoHandler {
    fn call<'connection>(
        &'connection self,
        mut context: SerialTcpRequestContext<'connection, LengthDelimitedProtocol>,
    ) -> impl Future<
        Output = Result<nacelle::tcp::TcpHandlerCompletion<LengthDelimitedProtocol>, NacelleError>,
    > + Send
    + 'connection {
        let response_payload = self.response_payload.clone();
        async move {
            let opcode = context.request().head.opcode;
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                let _ = chunk?;
            }
            if opcode != STRESS_OPCODE {
                return Err(NacelleError::handler(std::io::Error::other(format!(
                    "unknown opcode {}",
                    opcode
                ))));
            }
            context.respond(TcpResponse::bytes(response_payload)).await
        }
    }
}

pub enum StressServer<H, Observer = NoopObserver> {
    Shared(TcpServer<LengthDelimitedProtocol, H, Observer>),
    Serial(
        SerialTcpServer<
            LengthDelimitedProtocol,
            nacelle::tcp::admission::SerialHandlerAdapter<SerialEchoHandler>,
            Observer,
        >,
    ),
}

impl<H, Observer> Clone for StressServer<H, Observer>
where
    H: TcpHandler<LengthDelimitedProtocol>,
    Observer: NacelleTelemetryObserver,
{
    fn clone(&self) -> Self {
        match self {
            Self::Shared(server) => Self::Shared(server.clone()),
            Self::Serial(server) => Self::Serial(server.clone()),
        }
    }
}

impl<H, Observer> StressServer<H, Observer>
where
    H: TcpHandler<LengthDelimitedProtocol>,
    Observer: NacelleTelemetryObserver,
{
    pub fn with_telemetry<Next>(self, telemetry: NacelleTelemetry<Next>) -> StressServer<H, Next>
    where
        Next: NacelleTelemetryObserver,
    {
        match self {
            Self::Shared(server) => StressServer::Shared(server.with_telemetry(telemetry)),
            Self::Serial(server) => StressServer::Serial(server.with_telemetry(telemetry)),
        }
    }

    pub async fn serve_io<IO>(&self, io: IO) -> Result<(), NacelleError>
    where
        IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        match self {
            Self::Shared(server) => server.serve_io(io).await,
            Self::Serial(server) => server.serve_io(io).await,
        }
    }
}

pub fn build_server(
    config: &ServerConfig,
) -> Result<StressServer<impl TcpHandler<LengthDelimitedProtocol> + use<>>, NacelleError> {
    build_server_with_runtime_state(
        config,
        build_runtime_state(config)?,
        NacelleTelemetry::default(),
    )
}

/// Construct process-wide startup limits and initial live connection controls.
///
/// Call once before building listener workers; workers must share clones of the
/// returned state rather than reconstructing or partitioning its budgets.
pub fn build_runtime_state(config: &ServerConfig) -> Result<NacelleRuntimeState, NacelleError> {
    let runtime_state = NacelleRuntimeState::new(config.limits.clone());
    runtime_state
        .control()
        .set_max_connections(config.connection_limits.max_connections)?;
    runtime_state.control().set_connection_open_rate(
        config
            .connection_limits
            .max_connection_opens_per_peer_per_second,
    )?;
    Ok(runtime_state)
}

/// Build one response template and server around an already configured state.
///
/// Worker-local mode calls this once per listener worker, after optional CPU
/// pinning and before serving. Shared-template mode calls it once and clones
/// the server. No live thresholds or rate-table history are reset.
///
/// Shared templates matching the immutable default payload use static storage.
/// Custom sizes and explicit worker-local templates retain owned allocations.
///
/// Use this function for both paths when their returned server types must
/// match; [`build_server`] exposes a separate opaque handler type.
pub fn build_server_with_runtime_state<Observer>(
    config: &ServerConfig,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
) -> Result<
    StressServer<impl TcpHandler<LengthDelimitedProtocol> + use<Observer>, Observer>,
    NacelleError,
>
where
    Observer: NacelleTelemetryObserver,
{
    let response_payload = if config.response_bytes == DEFAULT_RESPONSE_PAYLOAD.len()
        && !config.worker_local_response
    {
        Bytes::from_static(&DEFAULT_RESPONSE_PAYLOAD)
    } else {
        Bytes::from(vec![0x5A; config.response_bytes])
    };
    build_server_with_payload(config, runtime_state, telemetry, response_payload)
}

fn build_server_with_payload<Observer>(
    config: &ServerConfig,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
    response_payload: Bytes,
) -> Result<
    StressServer<impl TcpHandler<LengthDelimitedProtocol> + use<Observer>, Observer>,
    NacelleError,
>
where
    Observer: NacelleTelemetryObserver,
{
    let mut tcp_config = NacelleTcpConfig::default()
        .with_read_buffer_capacity(config.read_buffer_capacity)
        .with_response_buffer_capacity(config.response_buffer_capacity)
        .with_request_body_chunk_size(config.request_body_chunk_size)
        .with_request_body_channel_capacity(config.request_body_channel_capacity)
        .with_response_write_policy(config.response_write_mode.into());
    tcp_config.head_read_limits = config.head_read_limits;
    let shared_response_payload = response_payload.clone();
    match config.handler_mode {
        HandlerMode::Shared => Ok(StressServer::Shared(
            TcpServer::<LengthDelimitedProtocol>::builder()
                .protocol(LengthDelimitedProtocol)
                .tcp_config(tcp_config)
                .telemetry(telemetry)
                .runtime_state(runtime_state)?
                .tcp_limits(config.tcp_limits)
                .handler(handler_fn(
                    move |mut context: TcpRequestContext<LengthDelimitedProtocol>| {
                        let response_payload = shared_response_payload.clone();
                        async move {
                            let opcode = context.request().head.opcode;
                            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                                let _ = chunk?;
                            }
                            if opcode != STRESS_OPCODE {
                                return Err(NacelleError::handler(std::io::Error::other(format!(
                                    "unknown opcode {}",
                                    opcode
                                ))));
                            }
                            context.respond(TcpResponse::bytes(response_payload)).await
                        }
                    },
                ))
                .build()?,
        )),
        HandlerMode::Serial => Ok(StressServer::Serial(
            SerialTcpServer::new(
                LengthDelimitedProtocol,
                SerialEchoHandler { response_payload },
            )
            .with_tcp_config(tcp_config)
            .with_telemetry(telemetry)
            .with_runtime_state(runtime_state)?
            .with_tcp_limits(config.tcp_limits),
        )),
    }
}

pub fn parse_args(
    args: impl IntoIterator<Item = String>,
    runtime: &str,
) -> Result<ServerConfig, Box<dyn std::error::Error + Send + Sync>> {
    parse_args_with_default_config(args, runtime, Path::new(DEFAULT_CONFIG_PATH))
}

fn parse_args_with_default_config(
    args: impl IntoIterator<Item = String>,
    runtime: &str,
    default_config_path: &Path,
) -> Result<ServerConfig, Box<dyn std::error::Error + Send + Sync>> {
    let args = args.into_iter().collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help(runtime);
        std::process::exit(0);
    }

    let mut config = ServerConfig::default();
    if default_config_path.exists() {
        config.apply_file(default_config_path)?;
    }
    apply_config_args(&mut config, &args, runtime)?;
    let cli_overrides = has_cli_overrides(&args);
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print_help(runtime);
                std::process::exit(0);
            }
            "--bind" => {
                config.bind = parse_value(&arg, args.next())?;
            }
            "--config" => {
                let _ = args
                    .next()
                    .ok_or_else(|| format!("missing value for {arg}"))?;
            }
            "--server-threads" => {
                config.server_threads = parse_value(&arg, args.next())?;
            }
            "--worker-cpus" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("missing value for {arg}"))?;
                config.worker_cpus = Some(
                    value
                        .split(',')
                        .map(|cpu| cpu.trim().parse::<usize>())
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            "--handler-mode" => {
                config.handler_mode = parse_value(&arg, args.next())?;
            }
            "--disable-timeouts" => {
                config.disable_timeouts();
            }
            "--disable-handler-timeout" => {
                config.disable_handler_timeout();
            }
            "--disable-tcp-timeouts" => {
                config.disable_tcp_timeouts();
            }
            "--response-bytes" => {
                config.response_bytes = parse_value(&arg, args.next())?;
            }
            "--worker-local-response" => {
                config.worker_local_response = true;
            }
            "--response-write-mode" => {
                config.response_write_mode = parse_value(&arg, args.next())?;
            }
            "--read-buffer" => {
                config.read_buffer_capacity = parse_value(&arg, args.next())?;
            }
            "--response-buffer" => {
                config.response_buffer_capacity = parse_value(&arg, args.next())?;
            }
            "--request-body-chunk-size" => {
                config.request_body_chunk_size = parse_value(&arg, args.next())?;
            }
            "--request-body-channel-capacity" => {
                config.request_body_channel_capacity = parse_value(&arg, args.next())?;
            }
            "--low-memory" => {
                config.low_memory = true;
            }
            "--byte-metrics" => {
                config.byte_metrics = true;
            }
            "--no-byte-metrics" => {
                config.byte_metrics = false;
            }
            "--phase-duration-metrics" => {
                config.phase_duration_metrics = true;
            }
            "--tls-self-signed" => {
                config.tls_self_signed = true;
            }
            other => {
                return Err(format!("unknown argument: {other}").into());
            }
        }
    }

    config.validate()?;

    if cli_overrides {
        config.config_sources.push("cli args".to_string());
    }

    Ok(config)
}

pub fn print_config(config: &ServerConfig, runtime: &str, actual_server_threads: usize) {
    println!("nacelle-stress-server effective config:");
    println!("  runtime: {runtime}");
    println!("  head_read_limits: {:?}", config.head_read_limits);
    println!("  config_sources: {}", config.config_sources.join(" -> "));
    println!("  bind: {}", config.bind);
    println!("  server_threads: {}", config.server_threads);
    println!("  actual_server_threads: {actual_server_threads}");
    println!("  worker_cpus: {:?}", config.worker_cpus);
    println!("  handler_mode: {:?}", config.handler_mode);
    println!(
        "  handler_timeout_disabled: {}",
        config.handler_timeout_disabled
    );
    println!("  tcp_timeouts_disabled: {}", config.tcp_timeouts_disabled);
    println!("  response_bytes: {}", config.response_bytes);
    println!("  worker_local_response: {}", config.worker_local_response);
    let template_count = if config.worker_local_response {
        actual_server_threads
    } else {
        1
    };
    println!(
        "  response_template_bytes: {}",
        config
            .response_bytes
            .checked_mul(template_count)
            .map_or_else(
                || "overflow (invalid configuration)".to_string(),
                |bytes| bytes.to_string()
            )
    );
    println!("  response_write_mode: {:?}", config.response_write_mode);
    println!("  read_buffer_capacity: {}", config.read_buffer_capacity);
    println!(
        "  response_buffer_capacity: {}",
        config.response_buffer_capacity
    );
    println!(
        "  request_body_chunk_size: {}",
        config.request_body_chunk_size
    );
    println!(
        "  request_body_channel_capacity: {}",
        config.request_body_channel_capacity
    );
    println!("  low_memory: {}", config.low_memory);
    println!("  byte_metrics: {}", config.byte_metrics);
    println!(
        "  phase_duration_metrics: {}",
        config.phase_duration_metrics
    );
    println!("  tls_self_signed: {}", config.tls_self_signed);
    println!("  connection_limits:");
    println!(
        "    max_connections: {}",
        config
            .connection_limits
            .max_connections
            .map(|max| max.to_string())
            .unwrap_or_else(|| "unlimited".to_string())
    );
    println!(
        "    max_connection_opens_per_peer_per_second: {}",
        config
            .connection_limits
            .max_connection_opens_per_peer_per_second
            .map(|max| max.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    );
    println!("  limits:");
    println!(
        "    max_connections_per_peer: {}",
        config
            .limits
            .max_connections_per_peer
            .map(|max| max.to_string())
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "    connection_rate_limit_table_capacity: {}",
        config.limits.connection_rate_limit_table_capacity
    );
    println!(
        "    max_in_flight_requests: {}",
        config.limits.max_in_flight_requests
    );
    println!(
        "    max_streaming_tasks: {}",
        config.limits.max_streaming_tasks
    );
    println!(
        "    max_request_body_bytes: {}",
        config.limits.max_request_body_bytes
    );
    println!(
        "    max_response_body_bytes: {}",
        config.limits.max_response_body_bytes
    );
    println!(
        "    handler_timeout_ms: {}",
        format_duration_ms(config.limits.handler_timeout)
    );
    println!("  tcp_limits:");
    println!(
        "    read_timeout_ms: {}",
        format_duration_ms(config.tcp_limits.read_timeout)
    );
    println!(
        "    write_timeout_ms: {}",
        format_duration_ms(config.tcp_limits.write_timeout)
    );
    println!(
        "    shutdown_timeout_ms: {}",
        format_duration_ms(config.tcp_limits.shutdown_timeout)
    );
    println!(
        "    idle_timeout_ms: {}",
        format_duration_ms(config.tcp_limits.idle_timeout)
    );
}

fn has_cli_overrides(args: &[String]) -> bool {
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--config" => index += 2,
            "--help" | "-h" => index += 1,
            _ => return true,
        }
    }
    false
}

fn format_duration_ms(duration: Option<Duration>) -> String {
    match duration {
        Some(duration) => duration.as_millis().to_string(),
        None => "null".to_string(),
    }
}

fn apply_config_args(
    config: &mut ServerConfig,
    args: &[String],
    runtime: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--help" | "-h" => {
                print_help(runtime);
                std::process::exit(0);
            }
            "--config" => {
                let path = args
                    .get(index + 1)
                    .ok_or_else(|| "missing value for --config".to_string())?;
                config.apply_file(path)?;
                index += 2;
            }
            _ => {
                index += 1;
            }
        }
    }
    Ok(())
}

fn parse_value<T>(
    flag: &str,
    value: Option<String>,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = value.ok_or_else(|| format!("missing value for {flag}"))?;
    Ok(value.parse::<T>()?)
}

pub fn print_help(runtime: &str) {
    println!(
        "Nacelle stress server ({runtime})\n\
         \n\
         Runs a standalone nacelle server that can be targeted by an external\n\
         load generator (e.g. nacelle-stress-test with --bind pointing here).\n\
         \n\
         Usage:\n\
           cargo run --release --bin stress-{runtime} -- [options]\n\
         \n\
         Options:\n\
           --bind <addr>                             Listen address (default 127.0.0.1:7878)\n\
           --config <path>                           Load TOML config before applying CLI flags\n\
           --server-threads <count>                  Threads (default: logical CPUs)\n\
           --worker-cpus <id,id,...>                 Linux CPU IDs, one per listener worker; no pinning by default\n\
           --handler-mode <shared|serial>            Connection-state handler mode (default: shared)\n\
           --disable-timeouts                        Disable handler, allocation, read, write, shutdown, and idle\n\
                                                     timeouts for diagnostic comparisons only.\n\
           --disable-handler-timeout                 Disable only the handler timeout for diagnostics.\n\
           --disable-tcp-timeouts                    Disable read, write, shutdown, and idle timeouts for diagnostics.\n\
           --response-bytes <bytes>                  Response payload bytes per request (default 64)\n\
           --worker-local-response                   Allocate one response template per listener worker;\n\
                                                     payload storage is workers * response-bytes instead\n\
                                                     of one shared template. Global limits stay shared.\n\
           --response-write-mode <mode>              immediate (default) or coalesce-buffered\n\
           --read-buffer <bytes>                     Read buffer capacity (default 65536)\n\
           --response-buffer <bytes>                 Response encode buffer capacity (default 16384)\n\
           --request-body-chunk-size <bytes>         Request body chunk size (default 16384)\n\
           --request-body-channel-capacity <count>   Request body channel capacity (default 8)\n\
           --low-memory                              Configure mimalloc to return freed pages to the\n\
                                                     OS immediately (purge_delay=0, no eager arena\n\
                                                     commit).  Reduces RSS on constrained systems at\n\
                                                     the cost of slightly higher allocation latency.\n\
                                                     Equivalent env vars (no recompile required):\n\
                                                       MIMALLOC_PURGE_DELAY=0\n\
                                                       MIMALLOC_ARENA_EAGER_COMMIT=0\n\
           --byte-metrics                            Enable request/response byte counters.\n\
           --no-byte-metrics                         Disable request/response byte counters.\n\
           --phase-duration-metrics                  Enable diagnostic TCP phase histograms; requires\n\
                                                     the phase-timing Cargo feature.\n\
           --tls-self-signed                         Serve TCP over TLS with an ephemeral\n\
                                                     self-signed certificate. The stress server\n\
                                                     default build includes this capability, but\n\
                                                     plain TCP remains the runtime default.\n\
         \n\
         Config:\n\
           If ./config.toml exists, it is loaded automatically. Explicit\n\
           --config files are applied after ./config.toml, and CLI flags are\n\
           applied last.\n\
        "
    );
}

#[cfg(test)]
mod tests {
    use nacelle::core::NacelleResourceLimitReason;
    use nacelle::core::telemetry::{NacelleInMemoryObserver, NacelleTelemetryEventKind};
    use nacelle_reference_protocol::{FRAME_FLAG_END, FRAME_FLAG_ERROR, FRAME_FLAG_START};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[test]
    fn worker_modes_apply_toml_overlays_and_cli() {
        let missing = Path::new("missing-default-config.toml");
        let defaults = parse_args_with_default_config([], "tokio", missing).expect("defaults");
        assert!(!defaults.worker_local_response);
        assert_eq!(defaults.worker_cpus, None);

        let config = parse_args_with_default_config(
            ["--worker-local-response".to_string()],
            "tokio",
            missing,
        )
        .expect("worker-local response mode");
        assert!(config.worker_local_response);
        assert_eq!(config.config_sources.last().unwrap(), "cli args");
        let mut overlay = ServerConfig::default();
        overlay
            .apply_config_file(
                toml::from_str::<ServerConfigFile>("worker_local_response = true").expect("TOML"),
            )
            .expect("worker-local response overlay");
        overlay
            .apply_config_file(ServerConfigFile::default())
            .expect("empty overlay");
        assert!(overlay.worker_local_response);
        overlay
            .apply_config_file(
                toml::from_str::<ServerConfigFile>("worker_local_response = false").expect("TOML"),
            )
            .expect("reset overlay");
        assert!(!overlay.worker_local_response);
    }

    #[test]
    fn worker_cpu_configuration_rejects_invalid_lists() {
        let missing = Path::new("missing-default-config.toml");
        for cpus in ["", "0,0", "0", "-1,2", "0,", "0,,2", "0-2"] {
            assert!(
                parse_args_with_default_config(
                    [
                        "--server-threads".to_string(),
                        "2".to_string(),
                        "--worker-cpus".to_string(),
                        cpus.to_string(),
                    ],
                    "tokio",
                    missing,
                )
                .is_err()
            );
        }
        for cpus in ["[]", "[0, 0]", "[0]"] {
            let mut config = ServerConfig::default();
            config
                .apply_config_file(
                    toml::from_str::<ServerConfigFile>(&format!(
                        "server_threads = 2\nworker_cpus = {cpus}"
                    ))
                    .expect("TOML"),
                )
                .expect("overlay");
            assert!(config.validate().is_err());
        }
        #[cfg(not(target_os = "linux"))]
        assert!(
            parse_args_with_default_config(
                [
                    "--server-threads".to_string(),
                    "8".to_string(),
                    "--worker-cpus".to_string(),
                    "0".to_string(),
                ],
                "tokio",
                missing,
            )
            .expect_err("affinity requires Linux")
            .to_string()
            .contains("requires Linux")
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_cpu_configuration_validates_after_final_overrides() {
        let config = parse_args_with_default_config(
            [
                "--server-threads".to_string(),
                "2".to_string(),
                "--worker-cpus".to_string(),
                "4, 0,2".to_string(),
                "--server-threads".to_string(),
                "3".to_string(),
            ],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .expect("final worker count");
        assert_eq!(config.worker_cpus, Some(vec![4, 0, 2]));

        let mut overlay = ServerConfig::default();
        for input in [
            "server_threads = 3\nworker_cpus = [4, 0, 2]",
            "server_threads = 2",
        ] {
            overlay
                .apply_config_file(toml::from_str::<ServerConfigFile>(input).expect("TOML"))
                .expect("overlay");
        }
        assert!(overlay.validate().is_err());
        overlay
            .apply_config_file(
                toml::from_str::<ServerConfigFile>("worker_cpus = [4, 0]").expect("TOML"),
            )
            .expect("final CPU overlay");
        overlay.validate().expect("final configuration");
        assert_eq!(overlay.worker_cpus, Some(vec![4, 0]));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_template_budget_rejects_overflow_only_when_opted_in() {
        let args = [
            "--server-threads".to_string(),
            "2".to_string(),
            "--response-bytes".to_string(),
            (usize::MAX / 2 + 1).to_string(),
        ];
        let missing = Path::new("missing-default-config.toml");
        parse_args_with_default_config(args.clone(), "tokio", missing)
            .expect("one shared template does not multiply storage");
        let error = parse_args_with_default_config(
            args.into_iter()
                .chain(["--worker-local-response".to_string()]),
            "tokio",
            missing,
        )
        .expect_err("worker template budget overflows");
        assert!(error.to_string().contains("template byte budget"));
    }

    #[test]
    fn worker_servers_share_runtime_budgets_and_live_controls() {
        for handler_mode in [HandlerMode::Shared, HandlerMode::Serial] {
            let mut config = ServerConfig {
                handler_mode,
                worker_local_response: true,
                ..ServerConfig::default()
            };
            config.connection_limits.max_connections = Some(2);
            config.limits.max_in_flight_requests = 1;
            config.limits.max_streaming_tasks = 1;
            config.limits.max_connections_per_peer = Some(1);
            let state = build_runtime_state(&config).expect("runtime state");
            let telemetry = NacelleTelemetry::default().with_runtime_metrics(false);
            let server_a = {
                let owned_config = config.clone();
                build_server_with_runtime_state(&owned_config, state.clone(), telemetry.clone())
                    .expect("first worker")
            };
            state
                .control()
                .set_max_connections(Some(1))
                .expect("live cap");
            let server_b =
                build_server_with_runtime_state(&config, state.clone(), telemetry.clone())
                    .expect("second worker");
            let state_a = match &server_a {
                StressServer::Shared(server) => {
                    assert!(!server.telemetry().runtime_metrics_enabled());
                    server.runtime_state()
                }
                StressServer::Serial(server) => {
                    assert!(!server.telemetry().runtime_metrics_enabled());
                    server.runtime_state()
                }
            };
            let state_b = match &server_b {
                StressServer::Shared(server) => server.runtime_state(),
                StressServer::Serial(server) => server.runtime_state(),
            };
            let mut identity = state_a.clone();
            identity
                .replace(state_b.clone())
                .expect("workers install the identical shared state");
            assert_eq!(state_b.control().snapshot().max_connections, Some(1));

            let connection = state_a.acquire_connection().expect("first connection");
            assert!(matches!(
                state_b.acquire_connection(),
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::Connections
                ))
            ));
            drop(connection);
            drop(state_b.acquire_connection().expect("released connection"));
            let request = state_a.acquire_request().expect("first request");
            assert!(matches!(
                state_b.acquire_request(),
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::InFlightRequests
                ))
            ));
            drop(request);
            drop(state_b.acquire_request().expect("released request"));
            let streaming = state_a
                .acquire_streaming_task()
                .expect("first streaming task");
            assert!(matches!(
                state_b.acquire_streaming_task(),
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::StreamingTasks
                ))
            ));
            drop(streaming);
            drop(
                state_b
                    .acquire_streaming_task()
                    .expect("released streaming task"),
            );

            state_a
                .control()
                .set_max_connections(Some(2))
                .expect("raise live cap");
            let peer = "127.0.0.1".parse().expect("peer");
            let connection = state_a
                .acquire_connection_for_peer(peer)
                .expect("first peer connection");
            assert!(matches!(
                state_b.acquire_connection_for_peer(peer),
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::PeerConnections
                ))
            ));
            drop(connection);
            drop(
                state_b
                    .acquire_connection_for_peer(peer)
                    .expect("released peer connection"),
            );
            assert_eq!(state.active_connections(), 0);
            assert_eq!(state.active_requests(), 0);
            assert_eq!(state.active_streaming_tasks(), 0);
        }
    }

    #[test]
    fn worker_server_rebuild_preserves_connection_rate_history() {
        for handler_mode in [HandlerMode::Shared, HandlerMode::Serial] {
            let mut config = ServerConfig {
                handler_mode,
                ..ServerConfig::default()
            };
            config.limits.connection_rate_limit_table_capacity = 1;
            config
                .connection_limits
                .max_connection_opens_per_peer_per_second = Some(1);
            let state = build_runtime_state(&config).expect("runtime state");
            let server_a = build_server_with_runtime_state(
                &config,
                state.clone(),
                NacelleTelemetry::default(),
            )
            .expect("first worker");
            let state_a = match &server_a {
                StressServer::Shared(server) => server.runtime_state(),
                StressServer::Serial(server) => server.runtime_state(),
            };
            let peer = "127.0.0.1".parse().expect("peer");
            drop(
                state_a
                    .acquire_connection_for_peer(peer)
                    .expect("first peer open"),
            );
            state
                .control()
                .set_connection_open_rate(None)
                .expect("disable rate");
            let server_b = build_server_with_runtime_state(
                &config,
                state.clone(),
                NacelleTelemetry::default(),
            )
            .expect("second worker");
            let state_b = match &server_b {
                StressServer::Shared(server) => server.runtime_state(),
                StressServer::Serial(server) => server.runtime_state(),
            };
            assert_eq!(
                state_b
                    .control()
                    .snapshot()
                    .max_connection_opens_per_peer_per_second,
                None
            );
            state_b
                .control()
                .set_connection_open_rate(Some(1))
                .expect("reenable rate without clearing history");
            assert!(matches!(
                state_b.acquire_connection_for_peer(peer),
                Err(NacelleError::ResourceLimit(
                    NacelleResourceLimitReason::PeerConnectionRate
                ))
            ));
            assert_eq!(state.active_connections(), 0);
        }
    }

    #[test]
    fn worker_response_templates_share_within_workers_but_not_across_workers() {
        for handler_mode in [HandlerMode::Shared, HandlerMode::Serial] {
            let config = ServerConfig {
                handler_mode,
                response_bytes: 257,
                worker_local_response: true,
                ..ServerConfig::default()
            };
            let state = build_runtime_state(&config).expect("runtime state");
            let payload_a = Bytes::from(vec![0x5A; config.response_bytes]);
            let payload_b = Bytes::from(vec![0x5A; config.response_bytes]);
            let server_a = build_server_with_payload(
                &config,
                state.clone(),
                NacelleTelemetry::default(),
                payload_a.clone(),
            )
            .expect("first worker");
            let server_b = build_server_with_payload(
                &config,
                state,
                NacelleTelemetry::default(),
                payload_b.clone(),
            )
            .expect("second worker");
            assert_eq!(payload_a, payload_b);
            assert_ne!(payload_a.as_ptr(), payload_b.as_ptr());
            assert!(!payload_a.is_unique());
            assert!(!payload_b.is_unique());
            let clone = server_a.clone();
            drop(server_a);
            assert!(!payload_a.is_unique());
            drop(clone);
            assert!(payload_a.is_unique());
            assert!(!payload_b.is_unique());
            drop(server_b);
            assert!(payload_b.is_unique());
        }
    }

    #[tokio::test]
    async fn worker_servers_preserve_fragmented_request_and_response_wire_behavior() {
        for handler_mode in [HandlerMode::Shared, HandlerMode::Serial] {
            for response_write_mode in [
                ResponseWriteMode::Immediate,
                ResponseWriteMode::CoalesceBuffered,
            ] {
                for (response_bytes, worker_local_response) in [
                    (0, false),
                    (0, true),
                    (64, false),
                    (64, true),
                    (257, false),
                    (257, true),
                ] {
                    let config = ServerConfig {
                        handler_mode,
                        response_write_mode,
                        response_bytes,
                        worker_local_response,
                        ..ServerConfig::default()
                    };
                    let state = build_runtime_state(&config).expect("runtime state");
                    let observer = NacelleInMemoryObserver::new();
                    let server = build_server_with_runtime_state(
                        &config,
                        state.clone(),
                        NacelleTelemetry::default().with_observer(observer.clone()),
                    )
                    .expect("worker server");
                    let (mut client, server_io) = tokio::io::duplex(32);
                    let client = async {
                        for request_id in [7, 8] {
                            let request = LengthDelimitedProtocol
                                .encode_request_frame(
                                    request_id,
                                    STRESS_OPCODE,
                                    FRAME_FLAG_START | FRAME_FLAG_END,
                                    b"fragmented-body",
                                )
                                .expect("request frame");
                            client.write_all(&request[..7]).await.expect("partial head");
                            tokio::task::yield_now().await;
                            client
                                .write_all(&request[7..])
                                .await
                                .expect("request remainder");
                            let expected = LengthDelimitedProtocol
                                .encode_request_frame(
                                    request_id,
                                    STRESS_OPCODE,
                                    FRAME_FLAG_START | FRAME_FLAG_END,
                                    &vec![0x5A; response_bytes],
                                )
                                .expect("response frame");
                            let mut response = vec![0; expected.len()];
                            client.read_exact(&mut response).await.expect("response");
                            assert_eq!(response.as_slice(), expected.as_ref());
                        }
                        let opcode = STRESS_OPCODE + 1;
                        let request = LengthDelimitedProtocol
                            .encode_request_frame(9, opcode, 0, b"invalid")
                            .expect("invalid opcode frame");
                        client.write_all(&request).await.expect("invalid request");
                        let mut error_wire = Vec::new();
                        client
                            .read_to_end(&mut error_wire)
                            .await
                            .expect("error response");
                        let error = NacelleError::handler(std::io::Error::other(format!(
                            "unknown opcode {opcode}"
                        )));
                        let expected = LengthDelimitedProtocol
                            .encode_request_frame(
                                9,
                                opcode,
                                FRAME_FLAG_START | FRAME_FLAG_END | FRAME_FLAG_ERROR,
                                error.to_string().as_bytes(),
                            )
                            .expect("error frame");
                        assert_eq!(error_wire.as_slice(), expected.as_ref());
                    };
                    let (result, ()) = tokio::time::timeout(Duration::from_secs(2), async {
                        tokio::join!(server.serve_io(server_io), client)
                    })
                    .await
                    .expect("responses must arrive without another request");
                    assert!(matches!(result, Err(NacelleError::Handler(_))));
                    let events = observer.events();
                    assert_eq!(
                        events
                            .iter()
                            .filter(
                                |event| event.kind == NacelleTelemetryEventKind::RequestCompleted
                            )
                            .count(),
                        3
                    );
                    assert_eq!(
                        events
                            .iter()
                            .filter(|event| event.kind == NacelleTelemetryEventKind::RequestFailed)
                            .count(),
                        1
                    );
                    assert_eq!(state.active_connections(), 0);
                    assert_eq!(state.active_requests(), 0);
                    assert_eq!(state.active_streaming_tasks(), 0);
                }
            }
        }
    }

    #[test]
    fn head_read_limits_validate_before_applying_an_overlay() {
        let mut config = ServerConfig::default();
        let valid = toml::from_str::<ServerConfigFile>(
            "[head_read_limits]\nmax_head_bytes = 24\nmax_undecided_bytes = 4096",
        )
        .expect("TOML");
        config.apply_config_file(valid).expect("valid limits");
        let expected = TcpHeadReadLimits::new(24, 4096).expect("limits");
        assert_eq!(config.head_read_limits, Some(expected));
        config
            .apply_config_file(ServerConfigFile::default())
            .expect("empty overlay");
        assert_eq!(config.head_read_limits, Some(expected));
        let invalid = toml::from_str::<ServerConfigFile>(
            "response_bytes = 1\n[head_read_limits]\nmax_head_bytes = 24\nmax_undecided_bytes = 23",
        )
        .expect("TOML");
        assert!(config.apply_config_file(invalid).is_err());
        assert_eq!(config.head_read_limits, Some(expected));
        assert_eq!(
            config.response_bytes,
            ServerConfig::default().response_bytes
        );
        assert!(
            toml::from_str::<ServerConfigFile>("[head_read_limits]\nmax_head_bytes = 24",).is_err()
        );
    }

    #[test]
    fn connection_limit_overlays_preserve_zero_and_reject_conflicts() {
        let mut config = ServerConfig::default();
        for invalid in [
            "[connection_limits]\nmax_connections = 1\nunlimited_connections = true",
            "[connection_limits]\nmax_connection_opens_per_peer_per_second = 1\ndisable_connection_open_rate = true",
        ] {
            let file = toml::from_str::<ServerConfigFile>(invalid).expect("TOML");
            assert!(config.apply_config_file(file).is_err());
        }
        assert_eq!(
            config.connection_limits,
            nacelle::core::NacelleConnectionLimits::default()
        );
        let zero = toml::from_str::<ServerConfigFile>(
            "[connection_limits]\nmax_connections = 0\nmax_connection_opens_per_peer_per_second = 0",
        ).expect("TOML");
        config.apply_config_file(zero).expect("zero thresholds");
        assert_eq!(config.connection_limits.max_connections, Some(0));
        assert_eq!(
            config
                .connection_limits
                .max_connection_opens_per_peer_per_second,
            Some(0)
        );
        let disabled = toml::from_str::<ServerConfigFile>(
            "[connection_limits]\nunlimited_connections = true\ndisable_connection_open_rate = true",
        ).expect("TOML");
        config
            .apply_config_file(disabled)
            .expect("disable thresholds");
        assert_eq!(config.connection_limits.max_connections, None);
        assert_eq!(
            config
                .connection_limits
                .max_connection_opens_per_peer_per_second,
            None
        );
        assert!(toml::from_str::<ServerConfigFile>("[limits]\nmax_connections = 1").is_err());
    }

    #[cfg(not(feature = "mimalloc-allocator"))]
    #[test]
    #[should_panic(expected = "low-memory mode requires the mimalloc-allocator feature")]
    fn low_memory_requires_mimalloc_allocator() {
        configure_allocator(true);
    }

    #[test]
    fn toml_config_applies_limits() {
        let toml = r#"
bind = "127.0.0.1:9000"
server_threads = 4
handler_mode = "serial"
response_bytes = 256
response_write_mode = "coalesce-buffered"
read_buffer_capacity = 4096
response_buffer_capacity = 2048
request_body_chunk_size = 1024
request_body_channel_capacity = 2
low_memory = true
byte_metrics = true
phase_duration_metrics = false
tls_self_signed = true

[connection_limits]
max_connections = 128000
max_connection_opens_per_peer_per_second = 2048

[limits]
max_connections_per_peer = 4096
max_in_flight_requests = 64000
max_streaming_tasks = 8192
max_request_body_bytes = 16777216
max_response_body_bytes = 15728640
handler_timeout_ms = 60000

[tcp_limits]
read_timeout_ms = 30000
write_timeout_ms = 30000
shutdown_timeout_ms = 30000
idle_timeout_ms = 120000
"#;
        let file = toml::from_str::<ServerConfigFile>(toml).unwrap();
        let mut config = ServerConfig::default();
        config.apply_config_file(file).expect("configuration");

        assert_eq!(config.bind, "127.0.0.1:9000".parse().unwrap());
        assert_eq!(config.server_threads, 4);
        assert_eq!(config.handler_mode, HandlerMode::Serial);
        assert_eq!(config.response_bytes, 256);
        assert_eq!(
            config.response_write_mode,
            ResponseWriteMode::CoalesceBuffered
        );
        assert_eq!(config.read_buffer_capacity, 4096);
        assert_eq!(config.response_buffer_capacity, 2048);
        assert_eq!(config.request_body_chunk_size, 1024);
        assert_eq!(config.request_body_channel_capacity, 2);
        assert!(config.low_memory);
        assert!(config.byte_metrics);
        assert!(!config.phase_duration_metrics);
        assert!(config.tls_self_signed);
        assert_eq!(config.connection_limits.max_connections, Some(128_000));
        assert_eq!(config.limits.max_connections_per_peer, Some(4_096));
        assert_eq!(
            config
                .connection_limits
                .max_connection_opens_per_peer_per_second,
            Some(2_048)
        );
        assert_eq!(config.limits.max_in_flight_requests, 64_000);
        assert_eq!(config.limits.max_streaming_tasks, 8_192);
        assert_eq!(config.limits.max_request_body_bytes, 16_777_216);
        assert_eq!(config.limits.max_response_body_bytes, 15_728_640);
        assert_eq!(config.limits.handler_timeout, Some(Duration::from_secs(60)));
        assert_eq!(
            config.tcp_limits.read_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            config.tcp_limits.write_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            config.tcp_limits.shutdown_timeout,
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            config.tcp_limits.idle_timeout,
            Some(Duration::from_secs(120))
        );
    }

    #[test]
    fn toml_rejects_removed_memory_limit() {
        let error = toml::from_str::<ServerConfigFile>("[limits]\nmax_memory_bytes = 536870912\n")
            .expect_err("memory limit should be unavailable");

        assert!(
            error
                .to_string()
                .contains("unknown field `max_memory_bytes`")
        );
    }

    #[test]
    fn cli_selects_serial_handler_mode() {
        let config = parse_args_with_default_config(
            ["--handler-mode".to_string(), "serial".to_string()],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .unwrap();

        assert_eq!(config.handler_mode, HandlerMode::Serial);
    }

    #[test]
    fn cli_selects_coalesced_response_writes() {
        let config = parse_args_with_default_config(
            [
                "--response-write-mode".to_string(),
                "coalesce-buffered".to_string(),
            ],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .unwrap();

        assert_eq!(
            config.response_write_mode,
            ResponseWriteMode::CoalesceBuffered
        );
    }

    #[test]
    fn phase_duration_metrics_require_compile_time_support() {
        let result = parse_args_with_default_config(
            ["--phase-duration-metrics".to_string()],
            "tokio",
            Path::new("missing-default-config.toml"),
        );

        #[cfg(feature = "phase-timing")]
        assert!(result.unwrap().phase_duration_metrics);
        #[cfg(not(feature = "phase-timing"))]
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("phase-timing feature")
        );
    }

    #[test]
    fn cli_disables_all_timeouts_after_config_files() {
        let config = parse_args_with_default_config(
            ["--disable-timeouts".to_string()],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .unwrap();

        assert!(config.handler_timeout_disabled);
        assert!(config.tcp_timeouts_disabled);
        assert_eq!(config.limits.handler_timeout, None);
        assert_eq!(config.tcp_limits.read_timeout, None);
        assert_eq!(config.tcp_limits.write_timeout, None);
        assert_eq!(config.tcp_limits.idle_timeout, None);
    }

    #[test]
    fn cli_disables_handler_and_tcp_timeouts_independently() {
        let handler_only = parse_args_with_default_config(
            ["--disable-handler-timeout".to_string()],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .unwrap();
        assert!(handler_only.handler_timeout_disabled);
        assert!(!handler_only.tcp_timeouts_disabled);
        assert_eq!(handler_only.limits.handler_timeout, None);
        assert!(handler_only.tcp_limits.read_timeout.is_some());

        let tcp_only = parse_args_with_default_config(
            ["--disable-tcp-timeouts".to_string()],
            "tokio",
            Path::new("missing-default-config.toml"),
        )
        .unwrap();
        assert!(!tcp_only.handler_timeout_disabled);
        assert!(tcp_only.tcp_timeouts_disabled);
        assert!(tcp_only.limits.handler_timeout.is_some());
        assert_eq!(tcp_only.tcp_limits.read_timeout, None);
        assert_eq!(tcp_only.tcp_limits.write_timeout, None);
        assert_eq!(tcp_only.tcp_limits.idle_timeout, None);
    }

    #[test]
    fn cli_overrides_toml_after_config_load() {
        let path = std::env::temp_dir().join(format!(
            "nacelle-stress-server-test-{}.toml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "server_threads = 2\nresponse_bytes = 128\n[connection_limits]\nmax_connections = 1500\n",
        )
        .unwrap();

        let config = parse_args(
            [
                "--config".to_string(),
                path.to_string_lossy().into_owned(),
                "--server-threads".to_string(),
                "8".to_string(),
            ],
            "tokio",
        )
        .unwrap();

        let _ = std::fs::remove_file(&path);
        assert_eq!(config.server_threads, 8);
        assert_eq!(config.response_bytes, 128);
        assert_eq!(config.connection_limits.max_connections, Some(1_500));
    }

    #[test]
    fn cli_enables_self_signed_tls() {
        let config = parse_args(["--tls-self-signed".to_string()], "tokio").unwrap();
        assert!(config.tls_self_signed);
        assert_eq!(config.config_sources.last().unwrap(), "cli args");
    }

    #[test]
    fn missing_toml_values_keep_code_defaults() {
        let file =
            toml::from_str::<ServerConfigFile>("[connection_limits]\nmax_connections = 1500\n")
                .unwrap();
        let mut config = ServerConfig::default();
        let default_response_bytes = config.response_bytes;
        let default_response_body_bytes = config.limits.max_response_body_bytes;
        let default_handler_timeout = config.limits.handler_timeout;

        config.apply_config_file(file).expect("configuration");

        assert_eq!(config.response_bytes, default_response_bytes);
        assert_eq!(config.connection_limits.max_connections, Some(1_500));
        assert_eq!(
            config.limits.max_response_body_bytes,
            default_response_body_bytes
        );
        assert_eq!(config.limits.handler_timeout, default_handler_timeout);
    }

    #[test]
    fn default_config_toml_loads_when_present() {
        let path = std::env::temp_dir().join(format!(
            "nacelle-stress-server-default-config-{}.toml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "response_bytes = 512\n[connection_limits]\nmax_connections = 1500\n",
        )
        .unwrap();

        let config = parse_args_with_default_config([], "tokio", &path).unwrap();

        let _ = std::fs::remove_file(&path);
        assert_eq!(config.response_bytes, 512);
        assert_eq!(config.connection_limits.max_connections, Some(1_500));
        assert_eq!(
            config.config_sources,
            vec![
                "code defaults".to_string(),
                format!("toml {}", path.display())
            ]
        );
    }

    #[test]
    fn explicit_config_and_cli_override_default_config_toml() {
        let base_path = std::env::temp_dir().join(format!(
            "nacelle-stress-server-base-config-{}.toml",
            std::process::id()
        ));
        let explicit_path = std::env::temp_dir().join(format!(
            "nacelle-stress-server-explicit-config-{}.toml",
            std::process::id()
        ));
        std::fs::write(
            &base_path,
            "response_bytes = 128\n[connection_limits]\nmax_connections = 1500\n",
        )
        .unwrap();
        std::fs::write(
            &explicit_path,
            "response_bytes = 256\n[connection_limits]\nmax_connections = 32000\n",
        )
        .unwrap();

        let config = parse_args_with_default_config(
            [
                "--config".to_string(),
                explicit_path.to_string_lossy().into_owned(),
                "--response-bytes".to_string(),
                "1024".to_string(),
            ],
            "tokio",
            &base_path,
        )
        .unwrap();

        let _ = std::fs::remove_file(base_path);
        let _ = std::fs::remove_file(explicit_path);
        assert_eq!(config.response_bytes, 1024);
        assert_eq!(config.connection_limits.max_connections, Some(32_000));
        assert_eq!(config.config_sources.last().unwrap(), "cli args");
    }

    #[test]
    fn explicit_config_without_cli_override_does_not_record_cli_source() {
        let path = std::env::temp_dir().join(format!(
            "nacelle-stress-server-explicit-only-config-{}.toml",
            std::process::id()
        ));
        std::fs::write(&path, "response_bytes = 256\n").unwrap();

        let config = parse_args_with_default_config(
            ["--config".to_string(), path.to_string_lossy().into_owned()],
            "tokio",
            Path::new("nacelle-stress-server-test-missing-default-config.toml"),
        )
        .unwrap();

        let _ = std::fs::remove_file(&path);
        assert_eq!(config.response_bytes, 256);
        assert_eq!(
            config.config_sources,
            vec![
                "code defaults".to_string(),
                format!("toml {}", path.display())
            ]
        );
    }
}
