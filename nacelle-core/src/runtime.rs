//! Tokio runtime helpers.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

pub use tokio::task::JoinError;

/// A join handle whose output is always `Result<T, JoinError>`.
pub struct JoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Spawn a `Send + 'static` future onto Tokio.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    JoinHandle(tokio::spawn(future))
}

/// The runtime topology a listener is serving on.
///
/// Nacelle parallelises connection handling by spawning onto the ambient
/// runtime, so a listener started on a current-thread runtime is capped to a
/// single core no matter how many cores the host has. This type makes that
/// property observable.
///
/// Thread-per-core hosts are the exception: they intentionally run one
/// current-thread runtime per worker, so the ambient handle reports one worker
/// even though the process spans many cores. Such hosts call
/// [`declare_worker_topology`] before starting workers, and this type then
/// reports the declared process-wide worker count instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NacelleRuntimeTopology {
    flavor: &'static str,
    workers: usize,
}

impl NacelleRuntimeTopology {
    /// Describe the topology the caller is serving on, if any.
    ///
    /// Returns `None` outside a Tokio runtime.
    pub fn current() -> Option<Self> {
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let declared = DECLARED_WORKERS.load(Ordering::Relaxed);
        if declared > 0 {
            return Some(Self {
                flavor: "thread_per_core",
                workers: declared,
            });
        }
        let flavor = match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::CurrentThread => "current_thread",
            tokio::runtime::RuntimeFlavor::MultiThread => "multi_thread",
            _ => "unknown",
        };
        Some(Self {
            flavor,
            workers: handle.metrics().num_workers(),
        })
    }

    /// The runtime flavor as a stable, low-cardinality label.
    pub fn flavor(&self) -> &'static str {
        self.flavor
    }

    /// The number of workers serving the process.
    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Whether the process can only ever use one core.
    pub fn is_single_worker(&self) -> bool {
        self.workers <= 1
    }
}

/// Declared process-wide worker count; `0` means "not declared".
static DECLARED_WORKERS: AtomicUsize = AtomicUsize::new(0);
static TOPOLOGY_REPORTED: AtomicBool = AtomicBool::new(false);

/// Declare the process-wide worker count for hosts that run one runtime per
/// worker.
///
/// Thread-per-core hosts give each worker its own current-thread runtime, so
/// the ambient Tokio handle reports a single worker and would otherwise be
/// diagnosed as a single-core misconfiguration. Call this once, before starting
/// workers, so [`NacelleRuntimeTopology`] and `server.runtime.workers` describe
/// the process rather than one worker's runtime.
pub fn declare_worker_topology(workers: usize) {
    DECLARED_WORKERS.store(workers.max(1), Ordering::Relaxed);
}

/// Report the runtime topology once per process when a listener starts.
///
/// Emits one `INFO` line describing the topology, a `WARN` when the process is
/// capped to a single worker, and publishes the worker count as the
/// `server.runtime.workers` gauge so single-core misconfiguration is visible in
/// dashboards rather than only in a profiler.
///
/// `runtime_metrics_enabled` carries the caller's telemetry policy: the gauge is
/// a runtime-domain metric and is suppressed when that domain is disabled. Log
/// output is not a metric and is always emitted.
pub fn report_runtime_topology(transport: &'static str, runtime_metrics_enabled: bool) {
    let Some(topology) = NacelleRuntimeTopology::current() else {
        return;
    };
    // Report and publish under one decision so the gauge can never disagree
    // with the logged topology in a process with several listeners.
    if TOPOLOGY_REPORTED.swap(true, Ordering::Relaxed) {
        return;
    }
    if runtime_metrics_enabled {
        metrics::gauge!("server.runtime.workers").set(topology.workers as f64);
    }
    tracing::info!(
        target: "nacelle",
        transport,
        runtime = topology.flavor,
        workers = topology.workers,
        "listener started"
    );
    if topology.is_single_worker() {
        tracing::warn!(
            target: "nacelle",
            transport,
            runtime = topology.flavor,
            workers = topology.workers,
            "runtime has a single worker; throughput is capped to one core"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `DECLARED_WORKERS` is process-wide, so tests that touch it must not run
    /// concurrently with tests that read the ambient runtime.
    static DECLARED_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_runtime_reports_single_worker() {
        let _guard = DECLARED_GUARD.lock().expect("guard");
        let topology = NacelleRuntimeTopology::current().expect("runtime");
        assert_eq!(topology.flavor(), "current_thread");
        assert!(topology.is_single_worker());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_thread_runtime_reports_worker_count() {
        let _guard = DECLARED_GUARD.lock().expect("guard");
        let topology = NacelleRuntimeTopology::current().expect("runtime");
        assert_eq!(topology.flavor(), "multi_thread");
        assert_eq!(topology.workers(), 2);
        assert!(!topology.is_single_worker());
    }

    #[test]
    fn topology_is_absent_outside_a_runtime() {
        assert!(NacelleRuntimeTopology::current().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn declared_thread_per_core_topology_is_not_single_worker() {
        let _guard = DECLARED_GUARD.lock().expect("guard");
        declare_worker_topology(8);
        let topology = NacelleRuntimeTopology::current().expect("runtime");
        // Each thread-per-core worker owns a current-thread runtime; without the
        // declaration this would be misreported as a one-core deployment.
        assert_eq!(topology.flavor(), "thread_per_core");
        assert_eq!(topology.workers(), 8);
        assert!(!topology.is_single_worker());
        DECLARED_WORKERS.store(0, Ordering::Relaxed);
    }
}
