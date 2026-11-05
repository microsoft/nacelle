use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

const BYTE_SHARDS: usize = 64;
static NEXT_BYTE_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static BYTE_SHARD: usize = NEXT_BYTE_SHARD.fetch_add(1, Ordering::Relaxed) % BYTE_SHARDS;
}

pub(crate) struct ConsoleRecorder {
    inner: DebuggingRecorder,
    bytes: ByteTotals,
}

impl ConsoleRecorder {
    pub(crate) fn new() -> Self {
        Self {
            inner: DebuggingRecorder::new(),
            bytes: ByteTotals::default(),
        }
    }

    pub(crate) fn snapshotter(&self) -> Snapshotter {
        self.inner.snapshotter()
    }

    pub(crate) fn byte_totals(&self) -> ByteTotals {
        self.bytes.clone()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ByteTotals {
    request: Arc<ByteSums>,
    response: Arc<ByteSums>,
}

impl ByteTotals {
    pub(crate) fn take(&self) -> (u64, u64) {
        // Each shard has its own interval cutoff; there is no cross-shard instant.
        (self.request.take(), self.response.take())
    }
}

#[derive(Debug)]
struct ByteSums([Arc<ByteSum>; BYTE_SHARDS]);

impl Default for ByteSums {
    fn default() -> Self {
        Self(std::array::from_fn(|_| Arc::new(ByteSum::default())))
    }
}

impl ByteSums {
    fn take(&self) -> u64 {
        self.0.iter().fold(0_u64, |total, shard| {
            total.saturating_add(shard.0.swap(0, Ordering::Relaxed))
        })
    }
}

#[derive(Debug, Default)]
#[repr(align(64))]
struct ByteSum(AtomicU64);

impl HistogramFn for ByteSum {
    fn record(&self, value: f64) {
        self.record_many(value, 1);
    }

    fn record_many(&self, value: f64, count: usize) {
        let bytes = (value as u64).saturating_mul(count as u64);
        if bytes == 0 {
            return;
        }
        let _ = self
            .0
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(bytes))
            });
    }
}

impl Recorder for ConsoleRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.inner.register_counter(key, metadata)
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.register_gauge(key, metadata)
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        // The console displays only cross-label byte totals, not distributions.
        match key.name() {
            "server.request.body.size" => {
                BYTE_SHARD.with(|shard| Histogram::from_arc(self.bytes.request.0[*shard].clone()))
            }
            "server.response.body.size" => {
                BYTE_SHARD.with(|shard| Histogram::from_arc(self.bytes.response.0[*shard].clone()))
            }
            _ => self.inner.register_histogram(key, metadata),
        }
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::DebugValue;

    use super::*;

    #[test]
    fn byte_totals_merge_labels_without_retaining_samples() {
        let recorder = ConsoleRecorder::new();
        let totals = recorder.byte_totals();
        metrics::with_local_recorder(&recorder, || {
            metrics::histogram!("server.request.body.size", "status" => "ok")
                .record_many(256.0, 1000);
            metrics::histogram!("server.request.body.size", "status" => "error").record(17.0);
            metrics::histogram!("server.response.body.size", "protocol" => "test")
                .record_many(64.0, 1000);
        });
        assert_eq!(totals.take(), (256_017, 64_000));
        assert_eq!(totals.take(), (0, 0));
        assert!(recorder.snapshotter().snapshot().into_vec().is_empty());
    }

    #[test]
    fn other_metrics_keep_the_debugging_recorders_semantics() {
        let recorder = ConsoleRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("server.request.completed", "status" => "ok").increment(3);
            metrics::gauge!("server.request.active").set(2.0);
            metrics::histogram!("server.phase.duration_ms", "phase" => "handler").record(4.0);
        });
        let snapshot = recorder.snapshotter().snapshot().into_vec();
        assert_eq!(snapshot.len(), 3);
        for (key, _, _, value) in snapshot {
            match key.key().name() {
                "server.request.completed" => assert_eq!(value, DebugValue::Counter(3)),
                "server.request.active" => assert_eq!(value, DebugValue::Gauge(2.0.into())),
                "server.phase.duration_ms" => {
                    assert_eq!(value, DebugValue::Histogram(vec![4.0.into()]));
                }
                _ => panic!("unexpected metric"),
            }
        }
    }

    #[test]
    fn zero_contributions_preserve_byte_conversion() {
        let recorder = ConsoleRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let request = metrics::histogram!("server.request.body.size");
            for value in [0.0, -7.0, f64::NAN] {
                request.record(value);
            }
            request.record_many(19.0, 0);
            request.record_many(f64::INFINITY, 0);
            request.record(4.9);
            request.record_many(2.1, 3);
        });
        assert_eq!(recorder.byte_totals().take(), (10, 0));
        assert!(recorder.snapshotter().snapshot().into_vec().is_empty());
    }

    #[test]
    fn colliding_shards_preserve_bytes_across_successive_concurrent_takes() {
        fn drain_rounds(total: &ByteTotals, barrier: &std::sync::Barrier) -> (u64, u64) {
            let mut request = 0;
            let mut response = 0;
            for _ in 0..8 {
                barrier.wait();
                let (request_bytes, response_bytes) = total.take();
                request += request_bytes;
                response += response_bytes;
                barrier.wait();
                let (request_bytes, response_bytes) = total.take();
                request += request_bytes;
                response += response_bytes;
            }
            (request, response)
        }

        let total = ByteTotals::default();
        let barrier = std::sync::Barrier::new(6);
        let (request, response) = std::thread::scope(|scope| {
            for worker in 0..4 {
                let request = Histogram::from_arc(total.request.0[worker % 2].clone());
                let response = Histogram::from_arc(total.response.0[worker].clone());
                let barrier = &barrier;
                scope.spawn(move || {
                    for _ in 0..8 {
                        barrier.wait();
                        for _ in 0..2000 {
                            request.record(3.0);
                            response.record(5.0);
                        }
                        barrier.wait();
                    }
                });
            }
            let second = scope.spawn(|| drain_rounds(&total, &barrier));
            let (request, response) = drain_rounds(&total, &barrier);
            let (second_request, second_response) = second.join().expect("snapshot thread");
            (request + second_request, response + second_response)
        });
        assert_eq!((request, response), (192_000, 320_000));
        assert_eq!(total.take(), (0, 0));
    }

    #[test]
    fn per_shard_and_merged_totals_saturate() {
        let total = ByteTotals::default();
        total.request.0[0].record_many(f64::INFINITY, 2);
        total.request.0[0].record(1.0);
        total.response.0[0].record((1_u64 << 63) as f64);
        total.response.0[1].record((1_u64 << 63) as f64);
        assert_eq!(total.take(), (u64::MAX, u64::MAX));
        assert_eq!(total.take(), (0, 0));
        total.request.0[0].record(1.0);
        total.response.0[1].record(2.0);
        assert_eq!(total.take(), (1, 2));
    }

    #[test]
    fn thread_exit_retains_pending_bytes_and_registered_handles() {
        let recorder = ConsoleRecorder::new();
        let (request, response) = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    metrics::with_local_recorder(&recorder, || {
                        let request = metrics::histogram!("server.request.body.size");
                        let response = metrics::histogram!("server.response.body.size");
                        request.record(11.0);
                        response.record(13.0);
                        (request, response)
                    })
                })
                .join()
                .expect("recording thread")
        });
        assert_eq!(recorder.byte_totals().take(), (11, 13));
        request.record(17.0);
        response.record(19.0);
        assert_eq!(recorder.byte_totals().take(), (17, 19));
        assert_eq!(recorder.byte_totals().take(), (0, 0));
    }
}
