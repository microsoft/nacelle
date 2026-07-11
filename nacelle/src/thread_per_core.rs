use std::future::Future;
use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleShutdown, NacelleShutdownToken};

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
        let mut core_ids: Vec<_> = core_ids.into_iter().collect();
        if core_ids.is_empty() {
            return Err(NacelleError::ResourceLimit("worker_count"));
        }
        core_ids.sort_unstable();
        if core_ids.windows(2).any(|pair| pair[0] == pair[1]) {
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
    config.validate()?;
    let shutdown = NacelleShutdown::new();
    let (startup_tx, startup_rx) = mpsc::channel();
    let mut threads = Vec::with_capacity(config.workers.len());

    for worker in config.workers.workers() {
        let factory = factory.clone();
        let worker_shutdown = shutdown.clone();
        let startup_tx = startup_tx.clone();
        let pin_workers = config.pin_workers;
        let thread = thread::Builder::new()
            .name(format!("nacelle-worker-{}", worker.index))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if pin_workers
                        && !core_affinity::set_for_current(core_affinity::CoreId {
                            id: worker.core_id,
                        })
                    {
                        let error = NacelleError::ResourceLimit("worker_affinity");
                        worker_shutdown.shutdown();
                        let _ = startup_tx.send(Err(error));
                        return Ok(());
                    }

                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(NacelleError::from)?;
                    let local = tokio::task::LocalSet::new();
                    let future = {
                        let _guard = runtime.enter();
                        match factory(WorkerContext {
                            worker,
                            shutdown: worker_shutdown.token(),
                        }) {
                            Ok(future) => future,
                            Err(error) => {
                                worker_shutdown.shutdown();
                                let _ = startup_tx.send(Err(error));
                                return Ok(());
                            }
                        }
                    };
                    startup_tx
                        .send(Ok(worker.index))
                        .map_err(|_| NacelleError::ConnectionClosed)?;
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
            })
            .map_err(NacelleError::from)?;
        threads.push(thread);
    }
    drop(startup_tx);

    let mut first_error = None;
    for _ in 0..config.workers.len() {
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

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
}
