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
                            .with_request_started_metrics(false)
                            .with_request_completed_metrics(false)
                            .with_byte_count_metrics(false)
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
    super::super::super::serve_local_stream_without_connection_limit(
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
