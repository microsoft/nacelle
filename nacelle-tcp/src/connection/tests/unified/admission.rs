use super::*;
use crate::admission::{SerialTcpAdmissionContext, TcpService};
use nacelle_core::{
    Admission, AdmissionConfig, AdmissionContext, AdmissionRejection, LocalAdmission,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[test]
fn rejection_metrics_do_not_control_permit_or_observer_cleanup() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use nacelle_core::{NacelleInMemoryObserver, NacelleTelemetryEventKind};

    for (metrics, errors) in [(false, true), (true, true), (true, false)] {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let observer = NacelleInMemoryObserver::new();
        let runtime = NacelleRuntimeState::default();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let server = server(
                        Capacity::new(1),
                        CustomProtocol::default(),
                        config(2),
                        runtime.clone(),
                        Arc::default(),
                        1,
                    )
                    .with_telemetry(
                        NacelleTelemetry::default()
                            .with_metrics(metrics)
                            .with_error_metrics(errors)
                            .with_connection_metrics(false)
                            .with_runtime_metrics(false)
                            .with_observer(observer.clone()),
                    );
                    assert!(matches!(
                        server.serve_io(io(b"R\x04bodyA\0")).await,
                        Err(NacelleError::AdmissionRejected(
                            AdmissionRejection::Forbidden
                        ))
                    ));
                });
        });
        assert_released(&runtime);
        for kind in [
            NacelleTelemetryEventKind::RequestRejected,
            NacelleTelemetryEventKind::RequestFailed,
        ] {
            let events = observer.events();
            let events: Vec<_> = events.iter().filter(|event| event.kind == kind).collect();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].reason, Some("admission_forbidden"));
        }
        let snapshot = snapshotter.snapshot().into_vec();
        for name in ["server.request.rejected", "server.request.failed"] {
            let values: Vec<_> = snapshot
                .iter()
                .filter(|(key, _, _, _)| key.key().name() == name)
                .map(|(_, _, _, value)| value)
                .collect();
            assert_eq!(
                values,
                if metrics && errors {
                    vec![&DebugValue::Counter(1)]
                } else {
                    vec![]
                }
            );
        }
    }
}

#[test]
fn admission_timeout_keeps_error_counters_with_detailed_request_metrics() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap()
            .block_on(async {
                let server = server(
                    Capacity::new(0),
                    CustomProtocol::default(),
                    config(2),
                    NacelleRuntimeState::default(),
                    Arc::default(),
                    1,
                )
                .with_telemetry(
                    NacelleTelemetry::default()
                        .with_connection_metrics(false)
                        .with_runtime_metrics(false),
                );
                let mut future = Box::pin(server.serve_io(io(b"A\0")));
                pending(future.as_mut()).await;
                tokio::time::advance(Duration::from_secs(1)).await;
                assert!(matches!(
                    future.await,
                    Err(NacelleError::Timeout(NacelleTimeoutReason::Admission))
                ));
            });
    });

    let snapshot = snapshotter.snapshot().into_vec();
    for name in ["server.request.failed", "server.request.timed_out"] {
        let values: Vec<_> = snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .map(|(_, _, _, value)| value)
            .collect();
        assert_eq!(values, vec![&DebugValue::Counter(1)]);
    }
}

struct CapacityPermit {
    _permit: Option<OwnedSemaphorePermit>,
    drops: Arc<AtomicUsize>,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Capacity {
    semaphore: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    order: Arc<std::sync::Mutex<Vec<u8>>>,
    wait_on: Option<u8>,
}

impl Capacity {
    fn new(permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            calls: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            order: Arc::new(std::sync::Mutex::new(Vec::new())),
            wait_on: None,
        }
    }
}

impl<'a, State: Send + 'static> Admission<AdmissionContext<'a, u8, State>> for Capacity {
    type Permit = CapacityPermit;

    async fn admit(
        &self,
        context: AdmissionContext<'a, u8, State>,
    ) -> Result<Self::Permit, AdmissionRejection> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if *context.head() == b'R' {
            return Err(AdmissionRejection::Forbidden);
        }
        let permit = if self.wait_on.is_none_or(|head| head == *context.head()) {
            Some(
                self.semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| AdmissionRejection::Unavailable)?,
            )
        } else {
            None
        };
        self.order.lock().expect("order").push(*context.head());
        Ok(CapacityPermit {
            _permit: permit,
            drops: self.drops.clone(),
        })
    }
}

fn config(budget: usize) -> NacelleTcpConfig {
    NacelleTcpConfig::default().with_head_read_limits(TcpHeadReadLimits::new(2, budget).unwrap())
}

fn server(
    rule: Capacity,
    protocol: CustomProtocol,
    config: NacelleTcpConfig,
    runtime: NacelleRuntimeState,
    calls: Arc<AtomicUsize>,
    pending: usize,
) -> TcpServer<CustomProtocol, impl TcpService<CustomProtocol, Permit = CapacityPermit>> {
    TcpServer::<CustomProtocol>::builder()
        .protocol(protocol)
        .tcp_config(config)
        .runtime_state(runtime)
        .unwrap()
        .handler(handler_fn(
            move |mut context: TcpRequestContext<CustomProtocol>| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if context.request().head == b'F' {
                        return Err(NacelleError::handler("test handler failure"));
                    }
                    if context.request().head == b'S' {
                        return Ok(context.complete());
                    }
                    while let Some(chunk) = context.request_mut().body.next_chunk().await {
                        chunk?;
                    }
                    let tag = context.request().head;
                    context
                        .respond(CustomResponse {
                            tag,
                            body: NacelleBody::bytes("ok"),
                        })
                        .await
                }
            },
        ))
        .admission(
            rule,
            AdmissionConfig::new(Duration::from_secs(1), pending).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
}

fn io(input: &[u8]) -> RecordingIo {
    RecordingIo {
        input: input.to_vec(),
        position: 0,
        writes: Arc::default(),
    }
}

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

#[tokio::test]
async fn pending_admission_bounds_reads_and_waiters_without_connection_cap() {
    let rule = Capacity::new(0);
    let runtime =
        NacelleRuntimeState::new(NacelleLimits::default().with_max_in_flight_requests(usize::MAX));
    runtime.control().set_max_connections(None).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let server = server(
        rule.clone(),
        CustomProtocol::default(),
        config(2),
        runtime.clone(),
        calls.clone(),
        1,
    );
    let reads = Arc::default();
    let first = server.serve_io(CountingIo {
        inner: io(b"A\x04bodyR\0"),
        reads: Arc::clone(&reads),
    });
    let mut first = Box::pin(first);
    pending(first.as_mut()).await;
    pending(first.as_mut()).await;
    assert_eq!(rule.calls.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
    assert_eq!(
        reads.lock().unwrap().iter().map(|(_, n)| n).sum::<usize>(),
        2
    );

    assert!(matches!(
        server.serve_io(io(b"B\0")).await,
        Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::AdmissionWaiters
        ))
    ));
    assert_eq!(rule.calls.load(Ordering::SeqCst), 1);
    drop(first);
    assert_released(&runtime);
    rule.semaphore.add_permits(1);
    server.serve_io(io(b"A\0")).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    assert_eq!(rule.semaphore.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn admission_timeout_and_ready_rejection_close_without_body_or_next_head() {
    for reject in [false, true] {
        let rule = Capacity::new(0);
        let protocol = CustomProtocol::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = NacelleRuntimeState::default();
        let server = server(
            rule.clone(),
            protocol.clone(),
            config(2),
            runtime.clone(),
            calls.clone(),
            1,
        );
        let reads = Arc::default();
        let input = if reject {
            b"R\x04bodyA\0"
        } else {
            b"A\x04bodyR\0"
        };
        let inner = io(input);
        let writes = inner.writes.clone();
        let result = server
            .serve_io(CountingIo {
                inner,
                reads: Arc::clone(&reads),
            })
            .await;
        if reject {
            assert!(matches!(
                result,
                Err(NacelleError::AdmissionRejected(
                    AdmissionRejection::Forbidden
                ))
            ));
            assert_error_wire(
                &protocol,
                Some(b'R'),
                &writes.lock().unwrap().concat(),
                b"[Rcustom:admission_forbidden]",
            );
        } else {
            assert!(matches!(
                result,
                Err(NacelleError::Timeout(NacelleTimeoutReason::Admission))
            ));
            assert_error_wire(
                &protocol,
                Some(b'A'),
                &writes.lock().unwrap().concat(),
                b"[Acustom:admission]",
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(rule.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            reads.lock().unwrap().iter().map(|(_, n)| n).sum::<usize>(),
            2
        );
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn semaphore_fifo_retains_original_future_and_cancellation_removes_waiter() {
    let rule = Capacity::new(0);
    let runtime = NacelleRuntimeState::default();
    let server = server(
        rule.clone(),
        CustomProtocol::default(),
        config(2),
        runtime.clone(),
        Arc::default(),
        3,
    );
    let mut first = Box::pin(server.serve_io(io(b"A\0")));
    let mut cancelled = Box::pin(server.serve_io(io(b"B\0")));
    let mut last = Box::pin(server.serve_io(io(b"C\0")));
    pending(first.as_mut()).await;
    pending(cancelled.as_mut()).await;
    pending(last.as_mut()).await;
    drop(cancelled);
    rule.semaphore.add_permits(1);
    first.await.unwrap();
    last.await.unwrap();
    assert_eq!(*rule.order.lock().unwrap(), b"AC");
    assert_eq!(rule.calls.load(Ordering::SeqCst), 3);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 2);
    assert_eq!(rule.semaphore.available_permits(), 1);
    assert_released(&runtime);
}

#[tokio::test]
async fn permit_covers_early_silent_completion_body_resolution_and_cancellation() {
    for cancel in [false, true] {
        let rule = Capacity::new(1);
        let runtime = NacelleRuntimeState::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let server = server(
            rule.clone(),
            CustomProtocol::default(),
            config(2).with_request_body_mode(TcpRequestBodyMode::Streaming),
            runtime.clone(),
            calls.clone(),
            1,
        );
        let (mut client, stream) = tokio::io::duplex(64);
        client.write_all(b"S\x02").await.unwrap();
        let mut future = Box::pin(server.serve_io(stream));
        pending(future.as_mut()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(rule.semaphore.available_permits(), 0);
        assert_eq!(rule.drops.load(Ordering::SeqCst), 0);
        if cancel {
            drop(future);
        } else {
            client.write_all(b"ok").await.unwrap();
            client.shutdown().await.unwrap();
            future.await.unwrap();
        }
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert_eq!(rule.semaphore.available_permits(), 1);
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn admitted_handler_and_body_failures_release_capacity_once() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        for input in [&b"F\0"[..], &b"A\x02x"[..]] {
            let rule = Capacity::new(1);
            let runtime = NacelleRuntimeState::default();
            let server = server(
                rule.clone(),
                CustomProtocol::default(),
                config(2).with_request_body_mode(mode),
                runtime.clone(),
                Arc::default(),
                1,
            );
            assert!(server.serve_io(io(input)).await.is_err());
            assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
            assert_eq!(rule.semaphore.available_permits(), 1);
            assert_released(&runtime);
        }
    }
}

struct FlushGate {
    inner: RecordingIo,
    open: Arc<AtomicBool>,
    flushes: Arc<AtomicUsize>,
}

impl AsyncRead for FlushGate {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for FlushGate {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let take = bytes.len().min(1);
        Pin::new(&mut self.inner).poll_write(cx, &bytes[..take])
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct WriteGate {
    inner: FlushGate,
    remaining: usize,
}

struct PartialWriteThenFailIo {
    inner: RecordingIo,
    wrote: bool,
}

impl AsyncRead for PartialWriteThenFailIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PartialWriteThenFailIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.wrote {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "test response write failure",
            )));
        }
        self.wrote = true;
        Pin::new(&mut self.inner).poll_write(cx, &bytes[..1])
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[test]
fn rejected_request_does_not_count_prior_response_bytes() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let protocol = CustomProtocol {
        error_outcomes: Some(outcomes.clone()),
        ..CustomProtocol::default()
    };
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let server = server(
                    Capacity::new(1),
                    protocol.clone(),
                    config(4).with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                    NacelleRuntimeState::default(),
                    Arc::default(),
                    1,
                )
                .with_telemetry(
                    NacelleTelemetry::default()
                        .with_connection_metrics(false)
                        .with_runtime_metrics(false),
                );
                let result = server
                    .serve_io(PartialWriteThenFailIo {
                        inner: io(b"A\0R\0"),
                        wrote: false,
                    })
                    .await;
                assert!(matches!(
                    result,
                    Err(NacelleError::Io(ref error))
                        if error.kind() == std::io::ErrorKind::BrokenPipe
                ));
            });
    });

    assert_eq!(
        *outcomes.lock().expect("outcome recorder poisoned"),
        vec![CapturedOutcome::Failed {
            phase: ResponseDeliveryPhase::SocketWrite,
            encoded_wire_bytes: 0,
            written_wire_bytes: 0,
        }]
    );
    let snapshot = snapshotter.snapshot().into_vec();
    let values: Vec<_> = snapshot
        .iter()
        .filter(|(key, _, _, _)| {
            key.key().name() == "server.response.body.size"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "status" && label.value() == "error")
        })
        .map(|(_, _, _, value)| value)
        .collect();
    assert_eq!(values, vec![&DebugValue::Histogram(vec![0.0.into()])]);
}

impl AsyncRead for WriteGate {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WriteGate {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.remaining == 0 {
            return Poll::Pending;
        }
        let take = bytes.len().min(self.remaining);
        let result = Pin::new(&mut self.inner).poll_write(cx, &bytes[..take]);
        if let Poll::Ready(Ok(written)) = result {
            self.remaining -= written;
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test(start_paused = true)]
async fn head_error_delivery_bounds_disabled_timeouts_and_preserves_explicit_timeouts() {
    for (input, head_bytes, handled, expected) in [
        (&b"\xff"[..], 2, 0, &b"[-custom:malformed]"[..]),
        (&b"A\0\xff"[..], 2, 1, &b"[Aok][-custom:malformed]"[..]),
        (&b"A\0"[..], 1, 0, &b"[-custom:malformed]"[..]),
    ] {
        for policy in [
            ResponseWritePolicy::Immediate,
            ResponseWritePolicy::CoalesceBuffered,
        ] {
            for flush_only in [false, true] {
                for write_timeout in [None, Some(Duration::from_secs(3))] {
                    let rule = Capacity::new(1);
                    let protocol = CustomProtocol::default();
                    let runtime = NacelleRuntimeState::default();
                    let calls = Arc::default();
                    let mut limits = NacelleTcpLimits::default()
                        .without_write_timeout()
                        .without_shutdown_timeout();
                    limits.write_timeout = write_timeout;
                    let server = server(
                        rule.clone(),
                        protocol.clone(),
                        NacelleTcpConfig::default()
                            .with_head_read_limits(TcpHeadReadLimits::new(head_bytes, 4).unwrap())
                            .with_response_write_policy(policy),
                        runtime.clone(),
                        Arc::clone(&calls),
                        1,
                    )
                    .with_tcp_limits(limits);
                    let inner = io(input);
                    let writes = inner.writes.clone();
                    let started = tokio::time::Instant::now();
                    let result = tokio::time::timeout(
                        Duration::from_secs(4),
                        server.serve_io(WriteGate {
                            inner: FlushGate {
                                inner,
                                open: Arc::new(AtomicBool::new(!flush_only)),
                                flushes: Arc::default(),
                            },
                            remaining: if flush_only {
                                usize::MAX
                            } else if policy == ResponseWritePolicy::Immediate && handled == 1 {
                                b"[Aok][".len()
                            } else {
                                1
                            },
                        }),
                    )
                    .await
                    .expect("head-error delivery must have a finite deadline");
                    assert!(matches!(
                        result,
                        Err(NacelleError::Timeout(NacelleTimeoutReason::TcpWrite))
                    ));
                    assert_eq!(
                        started.elapsed(),
                        write_timeout.unwrap_or(Duration::from_secs(1))
                    );
                    assert_eq!(
                        writes.lock().unwrap().concat(),
                        if flush_only {
                            expected
                        } else if policy == ResponseWritePolicy::Immediate && handled == 1 {
                            &b"[Aok]["[..]
                        } else {
                            b"["
                        }
                    );
                    assert_eq!(*protocol.errors.lock().unwrap(), vec![None]);
                    assert_eq!(calls.load(Ordering::SeqCst), handled);
                    assert_eq!(rule.calls.load(Ordering::SeqCst), handled);
                    assert_released(&runtime);
                }
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn admitted_normal_response_does_not_gain_a_disabled_write_deadline() {
    let rule = Capacity::new(1);
    let runtime = NacelleRuntimeState::default();
    let server = server(
        rule,
        CustomProtocol::default(),
        config(2),
        runtime.clone(),
        Arc::default(),
        1,
    )
    .with_tcp_limits(NacelleTcpLimits::default().without_write_timeout());
    let mut future = Box::pin(server.serve_io(WriteGate {
        inner: FlushGate {
            inner: io(b"A\0"),
            open: Arc::new(AtomicBool::new(true)),
            flushes: Arc::default(),
        },
        remaining: 1,
    }));
    pending(future.as_mut()).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    pending(future.as_mut()).await;
    drop(future);
    assert_released(&runtime);
}

#[test]
fn cancelled_pre_admission_delivery_records_one_socket_write_timeout() {
    use metrics_util::debugging::DebugValue;

    for flush_only in [false, true] {
        for (metrics, errors) in [(false, true), (true, true), (true, false)] {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let observer = NacelleInMemoryObserver::new();
            let runtime = NacelleRuntimeState::default();
            metrics::with_local_recorder(&recorder, || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .unwrap()
                    .block_on(async {
                        let mut rule = Capacity::new(0);
                        rule.wait_on = Some(b'B');
                        let protocol = CustomProtocol::default();
                        let calls = Arc::default();
                        let server = server(
                            rule.clone(),
                            protocol.clone(),
                            config(4)
                                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                            runtime.clone(),
                            Arc::clone(&calls),
                            1,
                        )
                        .with_telemetry(
                            NacelleTelemetry::default()
                                .with_metrics(metrics)
                                .with_error_metrics(errors)
                                .with_observer(observer.clone()),
                        );
                        let inner = io(b"A\0B\0");
                        let writes = inner.writes.clone();
                        let flushes = Arc::new(AtomicUsize::new(0));
                        let mut future = Box::pin(server.serve_io(WriteGate {
                            inner: FlushGate {
                                inner,
                                open: Arc::new(AtomicBool::new(!flush_only)),
                                flushes: flushes.clone(),
                            },
                            remaining: if flush_only { usize::MAX } else { 1 },
                        }));
                        pending(future.as_mut()).await;
                        tokio::time::advance(Duration::from_secs(1)).await;
                        assert!(matches!(
                            future.await,
                            Err(NacelleError::Timeout(NacelleTimeoutReason::Admission))
                        ));
                        assert_eq!(
                            writes.lock().unwrap().concat(),
                            if flush_only { &b"[Aok]"[..] } else { b"[" }
                        );
                        assert!(protocol.errors.lock().unwrap().is_empty());
                        assert_eq!(calls.load(Ordering::SeqCst), 1);
                        assert_eq!(rule.calls.load(Ordering::SeqCst), 2);
                        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
                    });
            });
            assert_released(&runtime);
            assert!(!observer.events().iter().any(|event| matches!(
                event.kind,
                NacelleTelemetryEventKind::RequestRejected
                    | NacelleTelemetryEventKind::RequestFailed
            )));
            let snapshot = snapshotter.snapshot().into_vec();
            let failures: Vec<_> = snapshot
                .iter()
                .filter(|(key, _, _, _)| key.key().name() == "server.errors")
                .collect();
            if metrics && errors {
                assert_eq!(failures.len(), 1);
                let (key, _, _, value) = failures[0];
                assert_eq!(*value, DebugValue::Counter(1));
                let labels: Vec<_> = key
                    .key()
                    .labels()
                    .map(|label| (label.key(), label.value()))
                    .collect();
                assert!(labels.contains(&("phase", "socket_write")));
                assert!(labels.contains(&("error_kind", "timeout")));
            } else {
                assert!(failures.is_empty());
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn pending_policy_flushes_prior_output_and_underlying_writer_within_deadline() {
    for timeout in [false, true] {
        let mut rule = Capacity::new(0);
        rule.wait_on = Some(b'B');
        let protocol = CustomProtocol::default();
        let runtime = NacelleRuntimeState::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let server = server(
            rule.clone(),
            protocol.clone(),
            config(4).with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            runtime.clone(),
            calls.clone(),
            1,
        );
        let inner = io(b"A\0B\0");
        let writes = inner.writes.clone();
        let open = Arc::new(AtomicBool::new(false));
        let flushes = Arc::new(AtomicUsize::new(0));
        let mut future = Box::pin(server.serve_io(FlushGate {
            inner,
            open: open.clone(),
            flushes: flushes.clone(),
        }));
        pending(future.as_mut()).await;
        assert_eq!(writes.lock().unwrap().concat(), b"[Aok]");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert!(flushes.load(Ordering::SeqCst) > 0);
        rule.semaphore.add_permits(1);
        pending(future.as_mut()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        if timeout {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(matches!(
                future.await,
                Err(NacelleError::Timeout(NacelleTimeoutReason::Admission))
            ));
            assert!(protocol.errors.lock().unwrap().is_empty());
            assert_eq!(writes.lock().unwrap().concat(), b"[Aok]");
            assert_eq!(rule.semaphore.available_permits(), 1);
        } else {
            open.store(true, Ordering::SeqCst);
            future.await.unwrap();
            assert_eq!(writes.lock().unwrap().concat(), b"[Aok][Bok]");
            assert_eq!(rule.drops.load(Ordering::SeqCst), 2);
        }
        assert_eq!(rule.calls.load(Ordering::SeqCst), 2);
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn ready_policies_preserve_coalescing_and_release_before_final_flush() {
    let rule = Capacity::new(1);
    let runtime = NacelleRuntimeState::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let server = server(
        rule.clone(),
        CustomProtocol::default(),
        config(4).with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        runtime.clone(),
        calls.clone(),
        1,
    );
    let open = Arc::new(AtomicBool::new(false));
    let mut future = Box::pin(server.serve_io(FlushGate {
        inner: io(b"A\0B\0"),
        open: open.clone(),
        flushes: Arc::default(),
    }));
    pending(future.as_mut()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 2);
    assert_eq!(rule.semaphore.available_permits(), 1);
    open.store(true, Ordering::SeqCst);
    future.await.unwrap();
    assert_released(&runtime);
}

#[tokio::test(start_paused = true)]
async fn ready_rejection_preserves_prior_output_and_bounds_disabled_write_timeouts() {
    let rule = Capacity::new(1);
    let runtime = NacelleRuntimeState::default();
    let calls = Arc::default();
    let server = server(
        rule.clone(),
        CustomProtocol::default(),
        config(6).with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        runtime.clone(),
        Arc::clone(&calls),
        1,
    )
    .with_tcp_limits(
        NacelleTcpLimits::default()
            .without_write_timeout()
            .without_shutdown_timeout(),
    );
    let inner = io(b"A\0R\0C\0");
    let writes = inner.writes.clone();
    let future = server.serve_io(FlushGate {
        inner,
        open: Arc::new(AtomicBool::new(false)),
        flushes: Arc::default(),
    });
    let result = tokio::time::timeout(Duration::from_secs(2), future)
        .await
        .expect("admission supplies a finite error-delivery fallback");
    assert!(result.is_err());
    assert_eq!(
        writes.lock().unwrap().concat(),
        b"[Aok][Rcustom:admission_forbidden]"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(rule.calls.load(Ordering::SeqCst), 2);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    assert_released(&runtime);
}

#[tokio::test]
async fn response_production_releases_permit_before_completion_callback_flush() {
    for cancel in [false, true] {
        let rule = Capacity::new(1);
        let callbacks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(CustomProtocol::default())
            .tcp_config(config(2))
            .handler(handler_fn({
                let callbacks = callbacks.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    let callbacks = callbacks.clone();
                    async move {
                        context
                            .respond(CustomResponse {
                                tag: b'A',
                                body: NacelleBody::bytes("body"),
                            })
                            .await
                            .map(|done| {
                                done.map(|response| {
                                    response.with_completion_item(move |outcome| {
                                        callbacks.lock().unwrap().push(capture_outcome(outcome));
                                    })
                                })
                            })
                    }
                }
            }))
            .admission(
                rule.clone(),
                AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let open = Arc::new(AtomicBool::new(false));
        let mut future = Box::pin(server.serve_io(FlushGate {
            inner: io(b"A\0"),
            open: open.clone(),
            flushes: Arc::default(),
        }));
        pending(future.as_mut()).await;
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert!(callbacks.lock().unwrap().is_empty());
        if cancel {
            drop(future);
        } else {
            open.store(true, Ordering::SeqCst);
            future.await.unwrap();
        }
        let events = callbacks.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            (&events[0], cancel),
            (CapturedOutcome::Delivered { .. }, false) | (CapturedOutcome::Aborted { .. }, true)
        ));
        assert_eq!(rule.semaphore.available_permits(), 1);
    }
}

#[tokio::test]
async fn streaming_response_holds_permit_until_production_ends_or_is_cancelled() {
    for cancel in [false, true] {
        let rule = Capacity::new(1);
        let (tx, body) = NacelleBody::channel(1);
        let body = std::sync::Mutex::new(Some(body));
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(CustomProtocol::default())
            .tcp_config(config(2))
            .handler(handler_fn(
                move |context: TcpRequestContext<CustomProtocol>| {
                    let body = body.lock().unwrap().take().unwrap();
                    async move { context.respond(CustomResponse { tag: b'A', body }).await }
                },
            ))
            .admission(
                rule.clone(),
                AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let mut future = Box::pin(server.serve_io(io(b"A\0")));
        pending(future.as_mut()).await;
        assert_eq!(rule.drops.load(Ordering::SeqCst), 0);
        assert_eq!(rule.semaphore.available_permits(), 0);
        if cancel {
            drop(future);
        } else {
            tx.send(Ok(Bytes::from_static(b"body"))).await.unwrap();
            drop(tx);
            future.await.unwrap();
        }
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert_eq!(rule.semaphore.available_permits(), 1);
    }
}

struct LocalRule(Rc<std::cell::Cell<usize>>);

impl<'a, State> LocalAdmission<AdmissionContext<'a, u8, State>> for LocalRule {
    type Permit = ();

    async fn admit(
        &self,
        context: AdmissionContext<'a, u8, State>,
    ) -> Result<(), AdmissionRejection> {
        tokio::task::yield_now().await;
        self.0
            .set(self.0.get() + usize::from(*context.head() == b'S'));
        Ok(())
    }
}

#[tokio::test]
async fn local_and_serial_servers_dispatch_one_borrowed_policy() {
    let shared = Capacity::new(1);
    SerialTcpServer::new(CustomProtocol::default(), CustomSerialHandler)
        .with_tcp_config(config(2))
        .with_admission(
            shared.clone(),
            AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
        )
        .unwrap()
        .serve_io(io(b"S\0"))
        .await
        .unwrap();
    assert_eq!(shared.drops.load(Ordering::SeqCst), 1);
    let calls = Rc::new(std::cell::Cell::new(0));
    LocalSerialTcpServer::new(CustomProtocol::default(), CustomSerialHandler)
        .with_tcp_config(config(2))
        .with_admission(
            LocalRule(calls.clone()),
            AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
        )
        .unwrap()
        .serve_io(io(b"S\0"))
        .await
        .unwrap();
    let server = crate::LocalTcpServer::new(
        CustomProtocol::default(),
        local_handler_fn(|context: TcpRequestContext<CustomProtocol>| async move {
            Ok(context.complete())
        }),
    )
    .with_tcp_config(config(2))
    .with_admission(
        LocalRule(calls.clone()),
        AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
    )
    .unwrap();
    super::super::super::serve_local_stream(
        io(b"S\0"),
        server.protocol(),
        server.handler(),
        server.config(),
        server.telemetry(),
        server.runtime_state(),
        server.tcp_limits(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
    .unwrap();
    assert_eq!(calls.get(), 2);
}

struct SerialBorrow;

#[derive(Default)]
struct SerialAppState {
    policy_calls: AtomicUsize,
    handler_calls: AtomicUsize,
}

struct StateOnlySerialHandler;

impl SerialTcpHandler<CustomProtocol, SerialAppState> for StateOnlySerialHandler {
    async fn call<'a>(
        &'a self,
        context: SerialTcpRequestContext<'a, CustomProtocol, SerialAppState>,
    ) -> Result<TcpHandlerCompletion<CustomProtocol>, NacelleError> {
        assert_eq!(context.app_state().policy_calls.load(Ordering::SeqCst), 1);
        context
            .app_state()
            .handler_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(context.complete())
    }
}

impl LocalSerialTcpHandler<CustomProtocol, SerialAppState> for StateOnlySerialHandler {
    async fn call<'a>(
        &'a self,
        context: SerialTcpRequestContext<'a, CustomProtocol, SerialAppState>,
    ) -> Result<TcpHandlerCompletion<CustomProtocol>, NacelleError> {
        SerialTcpHandler::call(self, context).await
    }
}

struct SerialStateRule;

impl<'a> Admission<SerialTcpAdmissionContext<'a, CustomProtocol, SerialAppState>>
    for SerialStateRule
{
    type Permit = ();

    async fn admit(
        &self,
        context: SerialTcpAdmissionContext<'a, CustomProtocol, SerialAppState>,
    ) -> Result<(), AdmissionRejection> {
        assert_eq!(*context.head(), b'S');
        context
            .app_state()
            .policy_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct LocalStateRule;

impl<'a, State> LocalAdmission<AdmissionContext<'a, u8, State, SerialAppState>> for LocalStateRule {
    type Permit = ();

    async fn admit(
        &self,
        context: AdmissionContext<'a, u8, State, SerialAppState>,
    ) -> Result<(), AdmissionRejection> {
        assert_eq!(*context.head(), b'S');
        context
            .app_state()
            .policy_calls
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn state_only_local_handler_configures_admission_before_binding_app_state() {
    let state = Arc::new(SerialAppState::default());
    let runtime = NacelleRuntimeState::default();
    let server = crate::LocalTcpServer::new(
        CustomProtocol::default(),
        local_handler_fn(
            |context: TcpRequestContext<CustomProtocol, SerialAppState>| async move {
                assert_eq!(context.app_state().policy_calls.load(Ordering::SeqCst), 1);
                context
                    .app_state()
                    .handler_calls
                    .fetch_add(1, Ordering::SeqCst);
                Ok(context.complete())
            },
        ),
    )
    .with_tcp_config(config(2))
    .with_tcp_limits(NacelleTcpLimits::default())
    .with_admission(
        LocalStateRule,
        AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
    )
    .unwrap()
    .with_app_state(state.clone())
    .with_runtime_state(runtime.clone())
    .unwrap();
    super::super::super::serve_local_stream(
        io(b"S\0"),
        server.protocol(),
        server.handler(),
        server.config(),
        server.telemetry(),
        server.runtime_state(),
        server.tcp_limits(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
    .unwrap();

    assert_eq!(state.policy_calls.load(Ordering::SeqCst), 1);
    assert_eq!(state.handler_calls.load(Ordering::SeqCst), 1);
    assert_released(&runtime);
}

#[tokio::test]
async fn state_only_serial_handlers_configure_admission_before_binding_app_state() {
    for local in [false, true] {
        let state = Arc::new(SerialAppState::default());
        let admission = AdmissionConfig::new(Duration::from_secs(1), 1).unwrap();
        let runtime = NacelleRuntimeState::default();
        if local {
            LocalSerialTcpServer::new(CustomProtocol::default(), StateOnlySerialHandler)
                .with_tcp_config(config(2))
                .with_tcp_limits(NacelleTcpLimits::default())
                .with_admission(SerialStateRule, admission)
                .unwrap()
                .with_app_state(state.clone())
                .with_runtime_state(runtime.clone())
                .unwrap()
                .serve_io(io(b"S\0"))
                .await
                .unwrap();
        } else {
            SerialTcpServer::new(CustomProtocol::default(), StateOnlySerialHandler)
                .with_tcp_config(config(2))
                .with_tcp_limits(NacelleTcpLimits::default())
                .with_admission(SerialStateRule, admission)
                .unwrap()
                .with_app_state(state.clone())
                .with_runtime_state(runtime.clone())
                .unwrap()
                .serve_io(io(b"S\0"))
                .await
                .unwrap();
        }
        assert_eq!(state.policy_calls.load(Ordering::SeqCst), 1);
        assert_eq!(state.handler_calls.load(Ordering::SeqCst), 1);
        assert_released(&runtime);
    }
}

impl<'a> Admission<SerialTcpAdmissionContext<'a, SerialCounterProtocol>> for SerialBorrow {
    type Permit = ();

    async fn admit(
        &self,
        context: SerialTcpAdmissionContext<'a, SerialCounterProtocol>,
    ) -> Result<(), AdmissionRejection> {
        tokio::task::yield_now().await;
        assert_eq!(context.connection().state.count, 0);
        Ok(())
    }
}

#[tokio::test]
async fn serial_admission_borrows_send_only_connection_state() {
    SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
        .with_tcp_config(config(2))
        .with_admission(
            SerialBorrow,
            AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
        )
        .unwrap()
        .serve_io(io(&[0]))
        .await
        .unwrap();
}
