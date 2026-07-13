use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleShutdown, NacelleShutdownToken};
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
#[cfg(any(feature = "tcp", feature = "http"))]
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};
#[cfg(all(feature = "openssl", feature = "tcp"))]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(all(feature = "rustls", any(feature = "tcp", feature = "http")))]
use nacelle_core::tls::NacelleTlsConfig;

#[cfg(feature = "http")]
use nacelle_http::{
    LocalHttpConnectionStateFactory, LocalHttpHandler, LocalHttpSharedState, LocalHyperServer,
};
#[cfg(feature = "tcp")]
use nacelle_tcp::options::NacelleTcpOptions;
#[cfg(feature = "tcp")]
use nacelle_tcp::protocol::{LocalTcpHandler, LocalTcpOneWayHandler, Protocol};
#[cfg(feature = "tcp")]
use nacelle_tcp::server::LocalTcpServer;
#[cfg(feature = "tcp")]
use nacelle_tcp::{LocalSerialTcpHandler, LocalSerialTcpOneWayHandler, LocalSerialTcpServer};

/// Application runtime topology.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeMode {
    /// Portable Tokio multi-thread runtime used by [`crate::NacelleApp`].
    #[default]
    Shared,
    /// Explicit worker-local runtime topology.
    ThreadPerCore,
}

/// One logical worker selected for thread-per-core execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Worker {
    /// Stable zero-based position in the configured worker set.
    pub index: usize,
    /// Operating-system logical CPU identifier used for optional affinity.
    pub core_id: usize,
}

/// Explicit worker selection for thread-per-core execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerSet {
    core_ids: Vec<usize>,
}

impl WorkerSet {
    /// Select every logical CPU reported by the affinity provider.
    pub fn all() -> Result<Self, NacelleError> {
        let core_ids: Vec<_> = core_affinity::get_core_ids()
            .ok_or(NacelleError::ResourceLimit("worker_discovery"))?
            .into_iter()
            .map(|core| core.id)
            .collect();
        Self::explicit(core_ids)
    }

    /// Select the first `count` logical CPUs.
    pub fn first(count: usize) -> Result<Self, NacelleError> {
        if count == 0 {
            return Err(NacelleError::ResourceLimit("worker_count"));
        }
        let all = Self::all()?;
        if count > all.len() {
            return Err(NacelleError::ResourceLimit("worker_count"));
        }
        Self::explicit(all.core_ids.into_iter().take(count))
    }

    /// Select explicit operating-system logical CPU identifiers.
    pub fn explicit(core_ids: impl IntoIterator<Item = usize>) -> Result<Self, NacelleError> {
        let core_ids: Vec<_> = core_ids.into_iter().collect();
        if core_ids.is_empty() {
            return Err(NacelleError::ResourceLimit("worker_count"));
        }
        let unique: HashSet<_> = core_ids.iter().copied().collect();
        if unique.len() != core_ids.len() {
            return Err(NacelleError::ResourceLimit("worker_duplicate"));
        }
        let available =
            core_affinity::get_core_ids().ok_or(NacelleError::ResourceLimit("worker_discovery"))?;
        if core_ids
            .iter()
            .any(|requested| !available.iter().any(|core| core.id == *requested))
        {
            return Err(NacelleError::ResourceLimit("worker_core"));
        }
        Ok(Self { core_ids })
    }

    /// Number of selected workers.
    pub fn len(&self) -> usize {
        self.core_ids.len()
    }

    /// Whether no workers are selected.
    pub fn is_empty(&self) -> bool {
        self.core_ids.is_empty()
    }

    fn workers(&self) -> impl Iterator<Item = Worker> + '_ {
        self.core_ids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, core_id)| Worker { index, core_id })
    }
}

/// Explicit thread-per-core runtime configuration.
#[derive(Debug, Clone)]
pub struct ThreadPerCoreConfig {
    workers: WorkerSet,
    pin_workers: bool,
}

/// Resource accounting policy selected once for a thread-per-core runtime.
#[derive(Debug, Clone)]
pub enum ThreadPerCoreLimits {
    /// Existing process-wide counters and memory accounting shared by workers.
    Global(NacelleRuntimeState),
    /// Worker-local counters with one shared process-wide hard memory ceiling.
    Worker(Vec<NacelleRuntimeState>),
}

impl ThreadPerCoreLimits {
    /// Use one existing process-wide runtime state on every worker.
    pub const fn global(state: NacelleRuntimeState) -> Self {
        Self::Global(state)
    }

    /// Partition capacity deterministically across worker-local runtime states.
    ///
    /// Finite connection, request, streaming, and per-peer capacities are split
    /// by quotient and remainder in configured worker order. Body limits and
    /// timeout policy remain identical on every worker. Memory is not
    /// partitioned: all states share `limits.max_memory_bytes` as a hard
    /// process-wide ceiling.
    pub fn worker(limits: NacelleLimits, worker_count: usize) -> Result<Self, NacelleError> {
        if worker_count == 0 {
            return Err(NacelleError::ResourceLimit("worker_count"));
        }
        if limits.max_connection_opens_per_peer_per_second.is_some()
            && limits.connection_rate_limit_table_capacity < worker_count
        {
            return Err(NacelleError::ResourceLimit(
                "connection_rate_limit_table_capacity",
            ));
        }
        let partitions: Vec<_> = (0..worker_count)
            .map(|worker| partition_limits(&limits, worker_count, worker))
            .collect();
        Ok(Self::Worker(NacelleRuntimeState::partitioned(partitions)?))
    }

    /// Resolve the runtime state for one configured worker.
    pub fn state_for(&self, worker: Worker) -> Result<NacelleRuntimeState, NacelleError> {
        match self {
            Self::Global(state) => Ok(state.clone()),
            Self::Worker(states) => states
                .get(worker.index)
                .cloned()
                .ok_or(NacelleError::ResourceLimit("worker_index")),
        }
    }
}

fn partition_limits(limits: &NacelleLimits, workers: usize, worker: usize) -> NacelleLimits {
    let mut partition = limits.clone();
    partition.max_connections = partition_capacity(limits.max_connections, workers, worker);
    partition.max_in_flight_requests =
        partition_capacity(limits.max_in_flight_requests, workers, worker);
    partition.max_streaming_tasks = partition_capacity(limits.max_streaming_tasks, workers, worker);
    partition.max_connections_per_peer = limits
        .max_connections_per_peer
        .map(|limit| partition_capacity(limit, workers, worker));
    partition.max_connection_opens_per_peer_per_second = limits
        .max_connection_opens_per_peer_per_second
        .map(|limit| partition_capacity(limit, workers, worker));
    if limits.max_connection_opens_per_peer_per_second.is_some() {
        partition.connection_rate_limit_table_capacity =
            partition_capacity(limits.connection_rate_limit_table_capacity, workers, worker);
    }
    partition
}

fn partition_capacity(total: usize, workers: usize, worker: usize) -> usize {
    let base = total / workers;
    base + usize::from(worker < total % workers)
}

impl ThreadPerCoreConfig {
    /// Configure an explicit worker set.
    pub const fn new(workers: WorkerSet) -> Self {
        Self {
            workers,
            pin_workers: false,
        }
    }

    /// Enable or disable CPU affinity for worker threads.
    pub const fn with_cpu_affinity(mut self, enabled: bool) -> Self {
        self.pin_workers = enabled;
        self
    }

    /// Selected workers.
    pub const fn workers(&self) -> &WorkerSet {
        &self.workers
    }

    /// Whether workers are pinned before their pipeline factory runs.
    pub const fn cpu_affinity_enabled(&self) -> bool {
        self.pin_workers
    }

    /// Validate the requested topology on the current platform.
    pub fn validate(&self) -> Result<(), NacelleError> {
        #[cfg(not(target_os = "linux"))]
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_unsupported_platform",
        ));

        #[cfg(target_os = "linux")]
        {
            if self.workers.is_empty() {
                return Err(NacelleError::ResourceLimit("worker_count"));
            }
            Ok(())
        }
    }
}

/// Per-worker context supplied after optional CPU pinning.
pub struct WorkerContext {
    /// Worker identity.
    pub worker: Worker,
    /// Cooperative process-wide shutdown token.
    pub shutdown: NacelleShutdownToken,
}

impl WorkerContext {
    /// Offload blocking work and resume on this worker before using its result.
    ///
    /// The closure and result must be `Send` because they cross to Tokio's
    /// blocking pool. Awaiting the returned future does not move the caller's
    /// local task; completion is observed again on the originating `LocalSet`.
    pub async fn offload_blocking<Work, Output>(&self, work: Work) -> Result<Output, NacelleError>
    where
        Work: FnOnce() -> Output + Send + 'static,
        Output: Send + 'static,
    {
        tokio::task::spawn_blocking(work)
            .await
            .map_err(NacelleError::from)
    }
}

/// Run one current-thread Tokio runtime and `LocalSet` per configured worker.
///
/// The factory runs on the owning worker after optional affinity is applied and
/// may construct `!Send` state. Its future remains on that worker until
/// completion. The first startup/runtime failure requests global shutdown;
/// every worker is joined before the first failure is returned.
pub fn run_thread_per_core<Factory, WorkerFuture>(
    config: ThreadPerCoreConfig,
    factory: Factory,
) -> Result<(), NacelleError>
where
    Factory: Fn(WorkerContext) -> Result<WorkerFuture, NacelleError> + Clone + Send + 'static,
    WorkerFuture: Future<Output = Result<(), NacelleError>> + 'static,
{
    run_thread_per_core_with_shutdown(config, NacelleShutdown::new(), factory)
}

/// Run thread-per-core workers with a caller-owned shutdown source.
pub fn run_thread_per_core_with_shutdown<Factory, WorkerFuture>(
    config: ThreadPerCoreConfig,
    shutdown: NacelleShutdown,
    factory: Factory,
) -> Result<(), NacelleError>
where
    Factory: Fn(WorkerContext) -> Result<WorkerFuture, NacelleError> + Clone + Send + 'static,
    WorkerFuture: Future<Output = Result<(), NacelleError>> + 'static,
{
    run_thread_per_core_with_spawner(config, shutdown, factory, |worker, task| {
        thread::Builder::new()
            .name(format!("nacelle-worker-{}", worker.index))
            .spawn(task)
    })
}

fn run_thread_per_core_with_spawner<Factory, WorkerFuture, Spawn>(
    config: ThreadPerCoreConfig,
    shutdown: NacelleShutdown,
    factory: Factory,
    mut spawn: Spawn,
) -> Result<(), NacelleError>
where
    Factory: Fn(WorkerContext) -> Result<WorkerFuture, NacelleError> + Clone + Send + 'static,
    WorkerFuture: Future<Output = Result<(), NacelleError>> + 'static,
    Spawn: FnMut(
        Worker,
        Box<dyn FnOnce() -> Result<(), NacelleError> + Send>,
    ) -> std::io::Result<thread::JoinHandle<Result<(), NacelleError>>>,
{
    config.validate()?;
    let (startup_tx, startup_rx) = mpsc::channel();
    let mut threads = Vec::with_capacity(config.workers.len());
    let mut first_error = None;

    for worker in config.workers.workers() {
        let factory = factory.clone();
        let worker_shutdown = shutdown.clone();
        let startup_tx = startup_tx.clone();
        let pin_workers = config.pin_workers;
        let task: Box<dyn FnOnce() -> Result<(), NacelleError> + Send> = Box::new(move || {
            let setup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if pin_workers
                    && !core_affinity::set_for_current(core_affinity::CoreId { id: worker.core_id })
                {
                    return Err(NacelleError::ResourceLimit("worker_affinity"));
                }

                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(NacelleError::from)?;
                let local = tokio::task::LocalSet::new();
                let future = {
                    let _guard = runtime.enter();
                    factory(WorkerContext {
                        worker,
                        shutdown: worker_shutdown.token(),
                    })?
                };
                Ok((runtime, local, future))
            }));

            let (runtime, local, future) = match setup {
                Ok(Ok(setup)) => setup,
                Ok(Err(error)) => {
                    worker_shutdown.shutdown();
                    let _ = startup_tx.send(Err(error));
                    return Ok(());
                }
                Err(_) => {
                    worker_shutdown.shutdown();
                    let _ = startup_tx.send(Err(NacelleError::ResourceLimit("worker_panic")));
                    return Ok(());
                }
            };
            startup_tx
                .send(Ok(worker.index))
                .map_err(|_| NacelleError::ConnectionClosed)?;
            drop(startup_tx);

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime.block_on(local.run_until(future))
            }));

            let result = match result {
                Ok(result) => result,
                Err(_) => Err(NacelleError::ResourceLimit("worker_panic")),
            };
            if result.is_err() {
                worker_shutdown.shutdown();
            }
            result
        });
        match spawn(worker, task) {
            Ok(thread) => threads.push(thread),
            Err(error) => {
                first_error = Some(NacelleError::from(error));
                shutdown.shutdown();
                break;
            }
        }
    }
    drop(startup_tx);

    for _ in 0..threads.len() {
        match startup_rx.recv() {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    shutdown.shutdown();
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(NacelleError::ConnectionClosed);
                    shutdown.shutdown();
                }
                break;
            }
        }
    }

    for worker in threads {
        match worker.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_error.is_none() {
                    first_error = Some(error);
                    shutdown.shutdown();
                }
            }
            Err(_) => {
                if first_error.is_none() {
                    first_error = Some(NacelleError::ResourceLimit("worker_panic"));
                    shutdown.shutdown();
                }
            }
        }
    }

    first_error.map_or(Ok(()), Err)
}

/// Bind a Linux TCP listener with mandatory `SO_REUSEPORT`.
///
/// Every thread-per-core worker binds the same address and accepts directly on
/// its owning runtime. Unsupported platforms return an explicit error.
pub fn bind_reuse_port_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener, NacelleError> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = addr;
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_unsupported_platform",
        ));
    }

    #[cfg(target_os = "linux")]
    {
        let domain = if addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&socket2::SockAddr::from(addr))?;
        socket.listen(1024)?;
        let listener: std::net::TcpListener = socket.into();
        Ok(tokio::net::TcpListener::from_std(listener)?)
    }
}

/// Configuration for one Linux worker-local plain TCP runtime.
#[cfg(feature = "tcp")]
#[derive(Debug, Clone)]
pub struct LocalTcpRuntimeConfig<Observer = NoopObserver> {
    runtime: ThreadPerCoreConfig,
    shutdown: NacelleShutdown,
    limits: ThreadPerCoreLimits,
    telemetry: NacelleTelemetry<Observer>,
    addr: SocketAddr,
    tcp_options: NacelleTcpOptions,
    drain_timeout: std::time::Duration,
}

/// Configuration for one Linux worker-local plain HTTP runtime.
#[cfg(feature = "http")]
#[derive(Debug, Clone)]
pub struct LocalHttpRuntimeConfig<Observer = NoopObserver> {
    runtime: ThreadPerCoreConfig,
    shutdown: NacelleShutdown,
    limits: ThreadPerCoreLimits,
    telemetry: NacelleTelemetry<Observer>,
    addr: SocketAddr,
    drain_timeout: std::time::Duration,
}

#[cfg(feature = "http")]
impl LocalHttpRuntimeConfig<NoopObserver> {
    /// Construct local HTTP runtime configuration with global accounting.
    pub fn new(
        runtime: ThreadPerCoreConfig,
        addr: SocketAddr,
        runtime_state: NacelleRuntimeState,
    ) -> Self {
        Self {
            runtime,
            shutdown: NacelleShutdown::new(),
            limits: ThreadPerCoreLimits::global(runtime_state),
            telemetry: NacelleTelemetry::default(),
            addr,
            drain_timeout: std::time::Duration::from_secs(30),
        }
    }
}

#[cfg(feature = "http")]
impl<Observer> LocalHttpRuntimeConfig<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    /// Use a caller-owned shutdown source.
    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Select global or partitioned worker resource accounting.
    pub fn with_limits(mut self, limits: ThreadPerCoreLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Set telemetry shared by all workers.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> LocalHttpRuntimeConfig<Next>
    where
        Next: NacelleTelemetryObserver,
    {
        LocalHttpRuntimeConfig {
            runtime: self.runtime,
            shutdown: self.shutdown,
            limits: self.limits,
            telemetry,
            addr: self.addr,
            drain_timeout: self.drain_timeout,
        }
    }

    /// Set the local connection drain deadline.
    pub fn with_drain_timeout(mut self, drain_timeout: std::time::Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }
}

#[cfg(feature = "tcp")]
impl LocalTcpRuntimeConfig<NoopObserver> {
    /// Construct local TCP runtime configuration with global accounting.
    pub fn new(
        runtime: ThreadPerCoreConfig,
        addr: SocketAddr,
        runtime_state: NacelleRuntimeState,
    ) -> Self {
        Self {
            runtime,
            shutdown: NacelleShutdown::new(),
            limits: ThreadPerCoreLimits::global(runtime_state),
            telemetry: NacelleTelemetry::default(),
            addr,
            tcp_options: NacelleTcpOptions::default(),
            drain_timeout: std::time::Duration::from_secs(30),
        }
    }
}

#[cfg(feature = "tcp")]
impl<Observer> LocalTcpRuntimeConfig<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    /// Use a caller-owned shutdown source.
    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Select global or partitioned worker resource accounting.
    pub fn with_limits(mut self, limits: ThreadPerCoreLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Set telemetry shared by all workers.
    pub fn with_telemetry<Next>(
        self,
        telemetry: NacelleTelemetry<Next>,
    ) -> LocalTcpRuntimeConfig<Next>
    where
        Next: NacelleTelemetryObserver,
    {
        LocalTcpRuntimeConfig {
            runtime: self.runtime,
            shutdown: self.shutdown,
            limits: self.limits,
            telemetry,
            addr: self.addr,
            tcp_options: self.tcp_options,
            drain_timeout: self.drain_timeout,
        }
    }

    /// Set accepted TCP stream options.
    pub fn with_tcp_options(mut self, tcp_options: NacelleTcpOptions) -> Self {
        self.tcp_options = tcp_options;
        self
    }

    /// Set the local connection drain deadline.
    pub fn with_drain_timeout(mut self, drain_timeout: std::time::Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }
}

/// Run one worker-local TCP listener stack per configured worker.
///
/// The server factory executes on each worker after optional CPU affinity is
/// applied. Every worker binds `addr` with mandatory Linux `SO_REUSEPORT`, owns
/// its protocol and `!Send` handlers, and processes accepted connections only
/// through `spawn_local` on that worker.
#[cfg(feature = "tcp")]
pub fn run_local_tcp_thread_per_core<P, H, OH, Observer, ServerObserver, Factory>(
    config: LocalTcpRuntimeConfig<Observer>,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalTcpServer<P, H, OH, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let tcp_options = config.tcp_options;
    let drain_timeout = config.drain_timeout;
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = std::rc::Rc::new(
            server_factory(context.worker)?
                .with_runtime_context(telemetry.clone(), limits.state_for(context.worker)?),
        );
        let tcp_options = tcp_options.clone();
        Ok(async move {
            nacelle_tcp::runtime::serve_local_tcp_listener(
                server,
                listener,
                tcp_options,
                context.shutdown,
                nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
            )
            .await
        })
    })
}

/// Run one worker-local serial TCP listener stack per configured worker.
#[cfg(feature = "tcp")]
pub fn run_local_serial_tcp_thread_per_core<P, H, OH, Observer, ServerObserver, Factory>(
    config: LocalTcpRuntimeConfig<Observer>,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P> + 'static,
    OH: LocalSerialTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalSerialTcpServer<P, H, OH, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let tcp_options = config.tcp_options;
    let drain_timeout = config.drain_timeout;
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = std::rc::Rc::new(
            server_factory(context.worker)?
                .with_runtime_context(telemetry.clone(), limits.state_for(context.worker)?),
        );
        let tcp_options = tcp_options.clone();
        Ok(async move {
            nacelle_tcp::runtime::serve_local_serial_tcp_listener(
                server,
                listener,
                tcp_options,
                context.shutdown,
                nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
            )
            .await
        })
    })
}

/// Run one worker-local Rustls TCP listener stack per configured worker.
#[cfg(all(feature = "tcp", feature = "rustls"))]
pub fn run_local_tcp_tls_thread_per_core<P, H, OH, Observer, ServerObserver, Factory>(
    config: LocalTcpRuntimeConfig<Observer>,
    tls_config: NacelleTlsConfig,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalTcpServer<P, H, OH, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let tcp_options = config.tcp_options;
    let drain_timeout = config.drain_timeout;
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = std::rc::Rc::new(
            server_factory(context.worker)?
                .with_runtime_context(telemetry.clone(), limits.state_for(context.worker)?),
        );
        let tcp_options = tcp_options.clone();
        let tls_config = tls_config.clone();
        Ok(async move {
            nacelle_tcp::runtime::serve_local_tcp_tls_listener(
                server,
                listener,
                tcp_options,
                tls_config,
                context.shutdown,
                nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
            )
            .await
        })
    })
}

/// Run one worker-local required-OpenSSL TCP listener stack per configured worker.
#[cfg(all(feature = "tcp", feature = "openssl"))]
pub fn run_local_tcp_openssl_thread_per_core<P, H, OH, Observer, ServerObserver, Factory>(
    config: LocalTcpRuntimeConfig<Observer>,
    tls_config: NacelleOpenSslConfig,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalTcpHandler<P> + 'static,
    OH: LocalTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalTcpServer<P, H, OH, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let tcp_options = config.tcp_options;
    let drain_timeout = config.drain_timeout;
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = std::rc::Rc::new(
            server_factory(context.worker)?
                .with_runtime_context(telemetry.clone(), limits.state_for(context.worker)?),
        );
        let tcp_options = tcp_options.clone();
        let tls_config = tls_config.clone();
        Ok(async move {
            nacelle_tcp::runtime::serve_local_tcp_openssl_listener(
                server,
                listener,
                tcp_options,
                tls_config,
                context.shutdown,
                nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
            )
            .await
        })
    })
}

/// Run one worker-local serial required-OpenSSL listener per configured worker.
#[cfg(all(feature = "tcp", feature = "openssl"))]
pub fn run_local_serial_tcp_openssl_thread_per_core<P, H, OH, Observer, ServerObserver, Factory>(
    config: LocalTcpRuntimeConfig<Observer>,
    tls_config: NacelleOpenSslConfig,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P> + 'static,
    OH: LocalSerialTcpOneWayHandler<P> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalSerialTcpServer<P, H, OH, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let tcp_options = config.tcp_options;
    let drain_timeout = config.drain_timeout;
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = std::rc::Rc::new(
            server_factory(context.worker)?
                .with_runtime_context(telemetry.clone(), limits.state_for(context.worker)?),
        );
        let tcp_options = tcp_options.clone();
        let tls_config = tls_config.clone();
        Ok(async move {
            nacelle_tcp::runtime::serve_local_serial_tcp_openssl_listener(
                server,
                listener,
                tcp_options,
                tls_config,
                context.shutdown,
                nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
            )
            .await
        })
    })
}

/// Run one worker-local HTTP/1 listener stack per configured worker.
#[cfg(feature = "http")]
pub fn run_local_http_thread_per_core<H, F, Observer, ServerObserver, Factory>(
    config: LocalHttpRuntimeConfig<Observer>,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    F: LocalHttpConnectionStateFactory,
    H: LocalHttpHandler<F::State> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalHyperServer<H, F, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let drain_timeout = config.drain_timeout;
    let shared_state = LocalHttpSharedState::default();
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = server_factory(context.worker)?.with_runtime_context(
            telemetry.clone(),
            limits.state_for(context.worker)?,
            shared_state.clone(),
        );
        Ok(async move {
            server
                .serve_listener(
                    listener,
                    context.shutdown,
                    nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
                )
                .await
        })
    })
}

/// Run one worker-local Rustls HTTP/1 listener stack per configured worker.
#[cfg(all(feature = "http", feature = "rustls"))]
pub fn run_local_http_tls_thread_per_core<H, F, Observer, ServerObserver, Factory>(
    config: LocalHttpRuntimeConfig<Observer>,
    tls_config: NacelleTlsConfig,
    server_factory: Factory,
) -> Result<(), NacelleError>
where
    F: LocalHttpConnectionStateFactory,
    H: LocalHttpHandler<F::State> + 'static,
    Observer: NacelleTelemetryObserver,
    ServerObserver: NacelleTelemetryObserver,
    Factory: Fn(Worker) -> Result<LocalHyperServer<H, F, ServerObserver>, NacelleError>
        + Clone
        + Send
        + 'static,
{
    if config.runtime.workers().len() > 1 && config.addr.port() == 0 {
        return Err(NacelleError::ResourceLimit(
            "thread_per_core_ephemeral_port",
        ));
    }
    let runtime = config.runtime;
    let shutdown = config.shutdown;
    let limits = config.limits;
    let telemetry = config.telemetry;
    let addr = config.addr;
    let drain_timeout = config.drain_timeout;
    let shared_state = LocalHttpSharedState::default();
    run_thread_per_core_with_shutdown(runtime, shutdown, move |context| {
        let listener = bind_reuse_port_listener(addr)?;
        let server = server_factory(context.worker)?.with_runtime_context(
            telemetry.clone(),
            limits.state_for(context.worker)?,
            shared_state.clone(),
        );
        let tls_config = tls_config.clone();
        Ok(async move {
            server
                .serve_tls_listener(
                    listener,
                    tls_config,
                    context.shutdown,
                    nacelle_core::lifecycle::NacelleDrainDeadline::new(drain_timeout),
                )
                .await
        })
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    #[cfg(all(target_os = "linux", feature = "tcp"))]
    use std::cell::Cell;

    use super::*;

    #[cfg(all(target_os = "linux", feature = "tcp"))]
    use bytes::{Bytes, BytesMut};
    #[cfg(all(target_os = "linux", feature = "tcp"))]
    use nacelle_codec::MessageDecoder;
    #[cfg(all(target_os = "linux", feature = "tcp"))]
    use nacelle_core::pipeline::LocalHandler;
    #[cfg(all(target_os = "linux", feature = "http"))]
    use nacelle_core::pipeline::LocalHandler as LocalPipelineHandler;
    #[cfg(all(target_os = "linux", feature = "http"))]
    use nacelle_http::{
        HttpHandlerCompletion, HttpResponse, LocalHttpConnectionStateFactory,
        LocalHttpRequestContext, LocalHyperServer,
    };
    #[cfg(all(target_os = "linux", feature = "tcp"))]
    use nacelle_tcp::{
        DecodedMessage, DecodedRequest, FrameBuffer, LocalSerialTcpHandler, LocalSerialTcpServer,
        LocalTcpServer, Protocol, SerialTcpRequestContext, TcpHandlerCompletion, TcpRequestContext,
        TcpResponse,
    };

    #[test]
    fn shared_runtime_is_the_default_mode() {
        assert_eq!(RuntimeMode::default(), RuntimeMode::Shared);
    }

    #[test]
    fn worker_set_rejects_empty_and_duplicate_workers() {
        assert!(matches!(
            WorkerSet::explicit([]),
            Err(NacelleError::ResourceLimit("worker_count"))
        ));
        let core = core_affinity::get_core_ids()
            .and_then(|cores| cores.first().copied())
            .expect("test requires one logical CPU");
        assert!(matches!(
            WorkerSet::explicit([core.id, core.id]),
            Err(NacelleError::ResourceLimit("worker_duplicate"))
        ));
    }

    #[test]
    fn explicit_worker_set_preserves_caller_order() {
        let available = core_affinity::get_core_ids().expect("logical CPUs should be discoverable");
        if available.len() < 2 {
            return;
        }
        let workers = WorkerSet::explicit([available[1].id, available[0].id])
            .expect("worker set should be valid");
        let selected: Vec<_> = workers.workers().map(|worker| worker.core_id).collect();

        assert_eq!(selected, [available[1].id, available[0].id]);
    }

    #[test]
    fn worker_limits_partition_capacity_and_share_hard_memory_ceiling() {
        let limits = NacelleLimits::default()
            .with_max_connections(5)
            .with_max_in_flight_requests(7)
            .with_max_streaming_tasks(4)
            .with_max_memory_bytes(10);
        let policy = ThreadPerCoreLimits::worker(limits, 2).expect("worker limits should build");
        let first = policy
            .state_for(Worker {
                index: 0,
                core_id: 0,
            })
            .expect("first worker state");
        let second = policy
            .state_for(Worker {
                index: 1,
                core_id: 1,
            })
            .expect("second worker state");

        assert_eq!(first.limits().max_connections, 3);
        assert_eq!(second.limits().max_connections, 2);
        assert_eq!(first.limits().max_in_flight_requests, 4);
        assert_eq!(second.limits().max_in_flight_requests, 3);
        assert_eq!(first.limits().max_streaming_tasks, 2);
        assert_eq!(second.limits().max_streaming_tasks, 2);

        let first_request = first.acquire_request().expect("first worker request");
        let second_request = second.acquire_request().expect("second worker request");
        assert_eq!(first.active_requests(), 1);
        assert_eq!(second.active_requests(), 1);

        let first_memory = first.allocate_memory(6).expect("first memory allocation");
        assert!(matches!(
            second.allocate_memory(5),
            Err(NacelleError::ResourceLimit("memory_bytes"))
        ));
        assert_eq!(first.memory_used_bytes(), 6);
        assert_eq!(second.memory_used_bytes(), 6);

        drop(first_memory);
        drop(first_request);
        drop(second_request);
    }

    #[test]
    fn worker_limits_require_one_rate_limit_slot_per_worker() {
        let limits = NacelleLimits::default()
            .with_max_connection_opens_per_peer_per_second(1)
            .with_connection_rate_limit_table_capacity(1);

        assert!(matches!(
            ThreadPerCoreLimits::worker(limits, 2),
            Err(NacelleError::ResourceLimit(
                "connection_rate_limit_table_capacity"
            ))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_runtime_supports_local_state_and_future() {
        let workers = WorkerSet::first(1).expect("one worker should be available");
        run_thread_per_core(ThreadPerCoreConfig::new(workers), |_context| {
            let state = Rc::new(RefCell::new(0_u64));
            Ok(async move {
                *state.borrow_mut() += 1;
                assert_eq!(*state.borrow(), 1);
                Ok(())
            })
        })
        .expect("local worker should complete");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn blocking_offload_resumes_on_originating_worker() {
        let workers = WorkerSet::first(1).expect("one worker should be available");
        run_thread_per_core(ThreadPerCoreConfig::new(workers), |context| {
            let worker_thread = thread::current().id();
            Ok(async move {
                let blocking_thread = context.offload_blocking(|| thread::current().id()).await?;
                assert_ne!(blocking_thread, worker_thread);
                assert_eq!(thread::current().id(), worker_thread);
                Ok(())
            })
        })
        .expect("blocking offload should complete");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn reuse_port_listeners_can_bind_the_same_address() {
        let first = bind_reuse_port_listener("127.0.0.1:0".parse().expect("valid address"))
            .expect("first listener should bind");
        let addr = first.local_addr().expect("listener should have address");
        let second = bind_reuse_port_listener(addr).expect("second listener should share port");

        assert_eq!(
            second.local_addr().expect("listener should have address"),
            addr
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn worker_error_is_returned_after_all_workers_join() {
        let workers = WorkerSet::first(1).expect("one worker should be available");
        let result = run_thread_per_core(ThreadPerCoreConfig::new(workers), |_context| {
            Ok(async { Err(NacelleError::ResourceLimit("worker_test")) })
        });

        assert!(matches!(
            result,
            Err(NacelleError::ResourceLimit("worker_test"))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn startup_failure_rolls_back_initialized_workers() {
        let available = core_affinity::get_core_ids().expect("logical CPUs should be discoverable");
        if available.len() < 2 {
            return;
        }
        let workers = WorkerSet::explicit([available[0].id, available[1].id])
            .expect("two workers should be valid");
        let result = run_thread_per_core(ThreadPerCoreConfig::new(workers), |context| {
            if context.worker.index == 1 {
                return Err(NacelleError::ResourceLimit("worker_startup_test"));
            }
            Ok(async move {
                let mut shutdown = context.shutdown;
                assert!(shutdown.changed().await);
                Ok(())
            })
        });

        assert!(result.is_err());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn panic_before_readiness_does_not_deadlock_startup() {
        let available = core_affinity::get_core_ids().expect("logical CPUs should be discoverable");
        if available.len() < 2 {
            return;
        }
        let workers = WorkerSet::explicit([available[0].id, available[1].id])
            .expect("two workers should be valid");
        let result = run_thread_per_core(ThreadPerCoreConfig::new(workers), |context| {
            if context.worker.index == 1 {
                panic!("startup panic test");
            }
            Ok(async move {
                let mut shutdown = context.shutdown;
                assert!(shutdown.changed().await);
                Ok(())
            })
        });

        assert!(matches!(
            result,
            Err(NacelleError::ResourceLimit("worker_panic"))
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn partial_thread_spawn_failure_shuts_down_and_joins_started_workers() {
        let available = core_affinity::get_core_ids().expect("logical CPUs should be discoverable");
        if available.len() < 2 {
            return;
        }
        let workers = WorkerSet::explicit([available[0].id, available[1].id])
            .expect("two workers should be valid");
        let shutdown = NacelleShutdown::new();
        let (joined_tx, joined_rx) = mpsc::channel();
        let mut spawn_count = 0_usize;
        let result = run_thread_per_core_with_spawner(
            ThreadPerCoreConfig::new(workers),
            shutdown,
            move |context| {
                let joined_tx = joined_tx.clone();
                Ok(async move {
                    let mut shutdown = context.shutdown;
                    assert!(shutdown.changed().await);
                    joined_tx.send(context.worker.index).expect("join observer");
                    Ok(())
                })
            },
            move |worker, task| {
                let current = spawn_count;
                spawn_count += 1;
                if current == 1 {
                    return Err(std::io::Error::other("injected spawn failure"));
                }
                thread::Builder::new()
                    .name(format!("test-worker-{}", worker.index))
                    .spawn(task)
            },
        );

        assert!(matches!(result, Err(NacelleError::Io(_))));
        assert_eq!(joined_rx.recv().expect("started worker should join"), 0);
    }

    #[cfg(all(target_os = "linux", feature = "tcp"))]
    #[test]
    fn local_tcp_worker_supports_shared_and_serial_non_send_state() {
        #[derive(Clone)]
        struct TestProtocol;

        struct TestDecoder;

        impl MessageDecoder for TestDecoder {
            type Message = DecodedMessage<(), std::convert::Infallible>;
            type Error = NacelleError;

            fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
                if src.len() < 4 {
                    return Ok(None);
                }
                let body_len = u32::from_le_bytes(src[..4].try_into().expect("length prefix"));
                let body_len = usize::try_from(body_len).expect("u32 should fit usize");
                if src.len() < 4 + body_len {
                    return Ok(None);
                }
                let _ = src.split_to(4);
                Ok(Some(DecodedMessage::Request(DecodedRequest {
                    request: (),
                    body_len,
                })))
            }
        }

        impl Protocol for TestProtocol {
            type Request = ();
            type OneWayRequest = std::convert::Infallible;
            type Response = TcpResponse;
            type ConnectionState = Rc<Cell<usize>>;
            type Decoder = TestDecoder;
            type ResponseContext = ();
            type ErrorContext = ();

            fn decoder(&self, _max_frame_len: usize) -> Self::Decoder {
                TestDecoder
            }

            fn connection_state(
                &self,
                _connection: &nacelle_core::pipeline::ConnectionInfo,
            ) -> Self::ConnectionState {
                Rc::new(Cell::new(0))
            }

            fn request_wire_bytes(&self, _request: &(), body_len: usize) -> usize {
                4 + body_len
            }

            fn one_way_wire_bytes(
                &self,
                request: &std::convert::Infallible,
                _body_len: usize,
            ) -> usize {
                match *request {}
            }

            fn response_context(&self, _request: &()) {}

            fn error_context(&self, _request: &()) {}

            fn apply_response(&self, _context: &mut (), _response: &TcpResponse) {}

            fn max_response_frame_overhead(&self) -> usize {
                4
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
                dst.put_u32_le(u32::try_from(chunk.len()).expect("test chunk fits u32"))?;
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
                dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                dst.put_u32_le(0)
            }

            fn encode_error(
                &self,
                _context: Option<&()>,
                _error: &NacelleError,
                dst: &mut FrameBuffer<'_>,
            ) -> Result<(), NacelleError> {
                dst.put_u32_le(0)
            }
        }

        struct LocalStateHandler {
            requests: Rc<RefCell<usize>>,
        }

        impl LocalHandler<TcpRequestContext<TestProtocol>> for LocalStateHandler {
            type Completion = TcpHandlerCompletion<TestProtocol>;
            type Error = NacelleError;

            async fn call(
                &self,
                mut context: TcpRequestContext<TestProtocol>,
            ) -> Result<Self::Completion, Self::Error> {
                *self.requests.borrow_mut() += 1;
                let chunk = context
                    .request_mut()
                    .body
                    .next_chunk()
                    .await
                    .transpose()?
                    .unwrap_or_default();
                context.respond(TcpResponse::bytes(chunk)).await
            }
        }

        struct LocalSerialStateHandler;

        impl LocalSerialTcpHandler<TestProtocol> for LocalSerialStateHandler {
            async fn call<'connection>(
                &'connection self,
                mut context: SerialTcpRequestContext<'connection, TestProtocol>,
            ) -> Result<TcpHandlerCompletion<TestProtocol>, NacelleError> {
                let state = context.connection_mut().state.clone();
                state.set(state.get() + 1);
                let chunk = context
                    .request_mut()
                    .body
                    .next_chunk()
                    .await
                    .transpose()?
                    .unwrap_or_default();
                context.respond(TcpResponse::bytes(chunk)).await
            }
        }

        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe should bind");
        let addr = probe.local_addr().expect("probe address");
        drop(probe);
        let workers = WorkerSet::first(1).expect("one worker should be available");
        let shutdown = NacelleShutdown::new();
        let client_shutdown = shutdown.clone();
        let client = thread::spawn(move || {
            let mut stream = loop {
                match std::net::TcpStream::connect(addr) {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        thread::yield_now();
                    }
                    Err(error) => panic!("client connect failed: {error}"),
                }
            };
            use std::io::{Read, Write};
            stream
                .write_all(&[4, 0, 0, 0, b'p', b'i', b'n', b'g'])
                .expect("request should write");
            let mut response = [0_u8; 8];
            stream
                .read_exact(&mut response)
                .expect("response should read");
            assert_eq!(&response, &[4, 0, 0, 0, b'p', b'i', b'n', b'g']);
            client_shutdown.shutdown();
        });

        let result = run_local_tcp_thread_per_core(
            LocalTcpRuntimeConfig::new(
                ThreadPerCoreConfig::new(workers),
                addr,
                NacelleRuntimeState::default(),
            )
            .with_shutdown(shutdown)
            .with_limits(
                ThreadPerCoreLimits::worker(NacelleLimits::default(), 1)
                    .expect("worker limits should build"),
            )
            .with_drain_timeout(std::time::Duration::from_secs(1)),
            |_worker| {
                Ok(LocalTcpServer::new(
                    TestProtocol,
                    LocalStateHandler {
                        requests: Rc::new(RefCell::new(0)),
                    },
                ))
            },
        );
        client.join().expect("client thread should join");
        result.expect("worker-local TCP runtime should shut down cleanly");

        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe should bind");
        let serial_addr = probe.local_addr().expect("probe address");
        drop(probe);
        let serial_workers = WorkerSet::first(1).expect("one worker should be available");
        let serial_shutdown = NacelleShutdown::new();
        let client_shutdown = serial_shutdown.clone();
        let serial_client = thread::spawn(move || {
            let mut stream = loop {
                match std::net::TcpStream::connect(serial_addr) {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        thread::yield_now();
                    }
                    Err(error) => panic!("client connect failed: {error}"),
                }
            };
            use std::io::{Read, Write};
            stream
                .write_all(&[4, 0, 0, 0, b'p', b'o', b'n', b'g'])
                .expect("request should write");
            let mut response = [0_u8; 8];
            stream
                .read_exact(&mut response)
                .expect("response should read");
            assert_eq!(&response, &[4, 0, 0, 0, b'p', b'o', b'n', b'g']);
            client_shutdown.shutdown();
        });

        let serial_result = run_local_serial_tcp_thread_per_core(
            LocalTcpRuntimeConfig::new(
                ThreadPerCoreConfig::new(serial_workers),
                serial_addr,
                NacelleRuntimeState::default(),
            )
            .with_shutdown(serial_shutdown)
            .with_limits(
                ThreadPerCoreLimits::worker(NacelleLimits::default(), 1)
                    .expect("worker limits should build"),
            )
            .with_drain_timeout(std::time::Duration::from_secs(1)),
            |_worker| {
                Ok(LocalSerialTcpServer::new(
                    TestProtocol,
                    LocalSerialStateHandler,
                ))
            },
        );
        serial_client.join().expect("client thread should join");
        serial_result.expect("worker-local serial TCP runtime should shut down cleanly");
    }

    #[cfg(all(target_os = "linux", feature = "http"))]
    #[test]
    fn local_http_worker_reuses_non_send_state_across_keep_alive_requests() {
        struct LocalConnectionState {
            requests: Rc<RefCell<usize>>,
        }

        struct LocalStateFactory;

        impl LocalHttpConnectionStateFactory for LocalStateFactory {
            type State = LocalConnectionState;

            fn create(&self, _connection: &nacelle_core::pipeline::ConnectionInfo) -> Self::State {
                LocalConnectionState {
                    requests: Rc::new(RefCell::new(0)),
                }
            }
        }

        struct LocalHttpHandlerState {
            total_requests: Rc<RefCell<usize>>,
        }

        impl LocalPipelineHandler<LocalHttpRequestContext<LocalConnectionState>> for LocalHttpHandlerState {
            type Completion = HttpHandlerCompletion;
            type Error = NacelleError;

            async fn call(
                &self,
                mut context: LocalHttpRequestContext<LocalConnectionState>,
            ) -> Result<Self::Completion, Self::Error> {
                *self.total_requests.borrow_mut() += 1;
                let connection_request = {
                    let mut requests = context.connection().state.requests.borrow_mut();
                    *requests += 1;
                    *requests
                };
                let mut body_bytes = 0;
                while let Some(chunk) = context.request_mut().next_body_chunk().await {
                    body_bytes += chunk?.len();
                }
                context
                    .respond(HttpResponse::bytes(
                        nacelle_http::StatusCode::OK,
                        format!("{connection_request}:{body_bytes}"),
                    ))
                    .await
            }
        }

        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe should bind");
        let addr = probe.local_addr().expect("probe address");
        drop(probe);
        let workers = WorkerSet::first(1).expect("one worker should be available");
        let shutdown = NacelleShutdown::new();
        let client_shutdown = shutdown.clone();
        let client = thread::spawn(move || {
            let mut stream = loop {
                match std::net::TcpStream::connect(addr) {
                    Ok(stream) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                        thread::yield_now();
                    }
                    Err(error) => panic!("client connect failed: {error}"),
                }
            };
            use std::io::{Read, Write};
            stream
                .write_all(
                                        b"POST /one HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nabc\
                                            POST /two HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\ndefg",
                )
                .expect("requests should write");
            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .expect("responses should read");
            let response = String::from_utf8(response).expect("response should be utf8");
            assert_eq!(response.matches("HTTP/1.1 200 OK").count(), 2);
            assert!(response.contains("\r\n3\r\n1:3"));
            assert!(response.contains("\r\n3\r\n2:4"));
            client_shutdown.shutdown();
        });

        let result = run_local_http_thread_per_core(
            LocalHttpRuntimeConfig::new(
                ThreadPerCoreConfig::new(workers),
                addr,
                NacelleRuntimeState::default(),
            )
            .with_shutdown(shutdown)
            .with_drain_timeout(std::time::Duration::from_secs(1)),
            |_worker| {
                Ok(LocalHyperServer::new(LocalHttpHandlerState {
                    total_requests: Rc::new(RefCell::new(0)),
                })
                .with_connection_state_factory(LocalStateFactory))
            },
        );
        client.join().expect("client thread should join");
        result.expect("worker-local HTTP runtime should shut down cleanly");
    }
}
