use super::*;
use nacelle_core::{
    Admission, AdmissionConfig, AdmissionRejection, NacelleCloseOutcome,
    NacelleConnectionActivityState, NacelleConnectionCloseReason, NacelleConnectionLookup,
    NacelleInMemoryObserver, NacelleReclaimOutcome, NacelleRuntimeControl,
    NacelleTelemetryEventKind,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

fn selected(control: &NacelleRuntimeControl) -> usize {
    match control.reclaim_idle(16) {
        NacelleReclaimOutcome::Observed { selected, .. } => selected,
        NacelleReclaimOutcome::TrackingDisabled => panic!("tracking enabled"),
    }
}

fn state(control: &NacelleRuntimeControl, id: u64) -> NacelleConnectionActivityState {
    match control.connection(id) {
        NacelleConnectionLookup::Found(snapshot) => snapshot.activity,
        _ => panic!("registered connection"),
    }
}

async fn pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
            .await
            .is_pending()
    );
}

#[derive(Default)]
struct Probe {
    connections_at_drop: AtomicUsize,
    flushes: AtomicUsize,
    shutdowns: AtomicUsize,
    reads: AtomicUsize,
    block_flush: AtomicBool,
    block_response_flush: AtomicBool,
    block_shutdown: AtomicBool,
    write_budget: std::sync::Mutex<Option<usize>>,
    written: std::sync::Mutex<Vec<u8>>,
}

struct ProbedIo<IO> {
    io: IO,
    probe: Arc<Probe>,
    after_input: Option<Box<dyn FnOnce() + Send>>,
    drop_state: Option<NacelleRuntimeState>,
}

impl<IO> Drop for ProbedIo<IO> {
    fn drop(&mut self) {
        if let Some(state) = &self.drop_state {
            self.probe
                .connections_at_drop
                .store(state.active_connections(), Ordering::SeqCst);
        }
    }
}

impl<IO: AsyncRead + Unpin> AsyncRead for ProbedIo<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.probe
                .reads
                .fetch_add(buf.filled().len() - before, Ordering::SeqCst);
            if let Some(callback) = self.after_input.take() {
                callback();
            }
        }
        result
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for ProbedIo<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let budget = *self.probe.write_budget.lock().unwrap();
        if budget == Some(0) {
            return Poll::Pending;
        }
        let bytes = &bytes[..budget.unwrap_or(bytes.len()).min(bytes.len()).min(1)];
        let result = Pin::new(&mut self.io).poll_write(cx, bytes);
        if let Poll::Ready(Ok(count)) = result {
            self.probe
                .written
                .lock()
                .unwrap()
                .extend_from_slice(&bytes[..count]);
            if let Some(budget) = &mut *self.probe.write_budget.lock().unwrap() {
                *budget -= count;
            }
        }
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.probe.flushes.fetch_add(1, Ordering::SeqCst);
        if self.probe.block_flush.load(Ordering::SeqCst)
            || (self.probe.block_response_flush.load(Ordering::SeqCst)
                && !self.probe.written.lock().unwrap().is_empty())
        {
            return Poll::Pending;
        }
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.probe.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.probe.block_shutdown.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

fn io(io: tokio::io::DuplexStream, probe: Arc<Probe>) -> ProbedIo<tokio::io::DuplexStream> {
    ProbedIo {
        io,
        probe,
        after_input: None,
        drop_state: None,
    }
}

struct Capacity(Arc<Semaphore>);

impl<'a> Admission<crate::TcpAdmissionContext<'a, CustomProtocol>> for Capacity {
    type Permit = OwnedSemaphorePermit;
    async fn admit(
        &self,
        _: crate::TcpAdmissionContext<'a, CustomProtocol>,
    ) -> Result<Self::Permit, AdmissionRejection> {
        self.0
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionRejection::Unavailable)
    }
}

#[tokio::test]
async fn input_and_reclaim_compete_before_handler_dispatch() {
    for reclaim_first in [false, true] {
        let runtime = tracked_runtime(1);
        let control = runtime.control();
        let connection = NacelleConnectionMeta::tcp(None, None);
        let id = connection.connection_id;
        let calls = Arc::new(AtomicUsize::new(0));
        let protocol = CustomProtocol::default();
        let server =
            TcpServer::<CustomProtocol>::builder()
                .protocol(protocol.clone())
                .runtime_state(runtime.clone())
                .unwrap()
                .handler(handler_fn({
                    let calls = calls.clone();
                    let control = control.clone();
                    move |_: TcpRequestContext<CustomProtocol>| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(selected(&control), 0);
                        std::future::pending::<
                            Result<TcpHandlerCompletion<CustomProtocol>, NacelleError>,
                        >()
                    }
                }))
                .build()
                .unwrap();
        let (mut client, stream) = tokio::io::duplex(128);
        let mut stream = io(stream, Arc::default());
        if reclaim_first {
            stream.after_input = Some(Box::new({
                let control = control.clone();
                move || assert_eq!(selected(&control), 1)
            }));
        }
        let future = server.serve_io_with_connection_meta(stream, connection);
        tokio::pin!(future);
        pending(future.as_mut()).await;
        assert_eq!(state(&control, id), NacelleConnectionActivityState::Idle);
        control.set_max_connections(Some(0)).unwrap();
        client.write_all(b"A\0").await.unwrap();
        if !reclaim_first {
            pending(future.as_mut()).await;
            assert_eq!(state(&control, id), NacelleConnectionActivityState::Active);
            assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
        }
        assert!(matches!(
            future.await,
            Err(NacelleError::ConnectionTerminated(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(!reclaim_first));
        assert!(protocol.errors.lock().unwrap().is_empty());
        assert_released(&runtime);
        assert!(control.connections(1).unwrap().is_empty());
    }
}

#[tokio::test]
async fn partial_heads_pending_admission_and_unresolved_bodies_are_active() {
    for input in [b"A".as_slice(), b"A\0".as_slice(), b"A\x05".as_slice()] {
        let runtime = tracked_runtime(2);
        let control = runtime.control();
        let semaphore = Arc::new(Semaphore::new(usize::from(
            input.len() == 2 && input[1] != 0,
        )));
        let calls = Arc::new(AtomicUsize::new(0));
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(CustomProtocol::default())
            .runtime_state(runtime.clone())
            .unwrap()
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_head_read_limits(TcpHeadReadLimits::new(2, 2).unwrap())
                    .with_request_body_mode(TcpRequestBodyMode::Streaming),
            )
            .handler(handler_fn({
                let calls = calls.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(context.complete()) }
                }
            }))
            .admission(
                Capacity(semaphore.clone()),
                AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
        let (mut client, stream) = tokio::io::duplex(128);
        let probe = Arc::new(Probe::default());
        let future = server.serve_io(io(stream, probe.clone()));
        tokio::pin!(future);
        pending(future.as_mut()).await;
        let id = control.connections(1).unwrap()[0].connection.connection_id;
        client.write_all(input).await.unwrap();
        pending(future.as_mut()).await;
        assert_eq!(state(&control, id), NacelleConnectionActivityState::Active);
        control.set_max_connections(Some(0)).unwrap();
        assert_eq!(selected(&control), 0);
        assert_eq!(probe.reads.load(Ordering::SeqCst), input.len());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            usize::from(input.len() == 2 && input[1] != 0)
        );
        assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
        assert_eq!(runtime.active_connections(), 1);
        assert!(future.await.is_err());
        assert_released(&runtime);
        if input == b"A\0" {
            control.set_max_connections(None).unwrap();
            let (mut client, stream) = tokio::io::duplex(128);
            client.write_all(input).await.unwrap();
            let next = server.serve_io(stream);
            tokio::pin!(next);
            pending(next.as_mut()).await;
            let id = control.connections(1).unwrap()[0].connection.connection_id;
            assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
            assert!(matches!(
                next.await,
                Err(NacelleError::ConnectionTerminated(_))
            ));
        }
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn forced_partial_output_aborts_callback_without_replaying_or_shutdown() {
    for policy in [
        ResponseWritePolicy::Immediate,
        ResponseWritePolicy::CoalesceBuffered,
    ] {
        let runtime = tracked_runtime(1);
        let control = runtime.control();
        let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observer = NacelleInMemoryObserver::new();
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(CustomProtocol::default())
            .runtime_state(runtime.clone())
            .unwrap()
            .tcp_config(NacelleTcpConfig::default().with_response_write_policy(policy))
            .tcp_limits(
                NacelleTcpLimits::default()
                    .without_write_timeout()
                    .without_shutdown_timeout(),
            )
            .handler(handler_fn({
                let outcomes = outcomes.clone();
                move |context: TcpRequestContext<CustomProtocol>| {
                    let outcomes = outcomes.clone();
                    async move {
                        context
                            .respond(CustomResponse {
                                tag: b'A',
                                body: NacelleBody::bytes("ok"),
                            })
                            .await
                            .map(|completion| {
                                completion.map(|completion| {
                                    completion.with_completion_item(move |outcome| {
                                        outcomes.lock().unwrap().push(capture_outcome(outcome));
                                    })
                                })
                            })
                    }
                }
            }))
            .build()
            .unwrap()
            .with_telemetry(
                NacelleTelemetry::default()
                    .with_metrics(false)
                    .with_observer(observer.clone()),
            );
        let probe = Arc::new(Probe::default());
        *probe.write_budget.lock().unwrap() = Some(1);
        let (mut client, stream) = tokio::io::duplex(128);
        client.write_all(b"A\0").await.unwrap();
        let mut stream = io(stream, probe.clone());
        stream.drop_state = Some(runtime.clone());
        let future = server.serve_io(stream);
        tokio::pin!(future);
        pending(future.as_mut()).await;
        let id = control.connections(1).unwrap()[0].connection.connection_id;
        control.set_max_connections(Some(0)).unwrap();
        assert_eq!(selected(&control), 0);
        assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
        let flushes = probe.flushes.load(Ordering::SeqCst);
        assert!(future.await.is_err());
        assert_eq!(probe.connections_at_drop.load(Ordering::SeqCst), 1);
        assert_eq!(*probe.written.lock().unwrap(), b"[");
        assert_eq!(probe.flushes.load(Ordering::SeqCst), flushes);
        assert_eq!(probe.shutdowns.load(Ordering::SeqCst), 0);
        assert_eq!(
            *outcomes.lock().unwrap(),
            vec![CapturedOutcome::Aborted {
                reason: ResponseAbortReason::Cancelled
            }]
        );
        let events = observer.events();
        let closes: Vec<_> = events
            .iter()
            .filter(|event| event.kind == NacelleTelemetryEventKind::ConnectionClosed)
            .collect();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].reason, Some("administrative_close"));
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn empty_frame_queue_is_not_idle_until_underlying_flush_completes() {
    for initial_flush in [false, true] {
        let runtime = tracked_runtime(1);
        let control = runtime.control();
        let server = TcpServer::<CustomProtocol>::builder()
            .protocol(CustomProtocol::default())
            .runtime_state(runtime.clone())
            .unwrap()
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .handler(handler_fn(
                |context: TcpRequestContext<CustomProtocol>| async move {
                    context
                        .respond(CustomResponse {
                            tag: b'A',
                            body: NacelleBody::bytes("ok"),
                        })
                        .await
                },
            ))
            .build()
            .unwrap();
        let probe = Arc::new(Probe::default());
        probe.block_flush.store(initial_flush, Ordering::SeqCst);
        probe
            .block_response_flush
            .store(!initial_flush, Ordering::SeqCst);
        let (mut client, stream) = tokio::io::duplex(128);
        if !initial_flush {
            client.write_all(b"A\0").await.unwrap();
        }
        let future = server.serve_io(io(stream, probe.clone()));
        tokio::pin!(future);
        pending(future.as_mut()).await;
        let id = control.connections(1).unwrap()[0].connection.connection_id;
        control.set_max_connections(Some(0)).unwrap();
        assert_eq!(state(&control, id), NacelleConnectionActivityState::Active);
        assert_eq!(selected(&control), 0);
        if !initial_flush {
            assert_eq!(*probe.written.lock().unwrap(), b"[Aok]");
        }
        probe.block_flush.store(false, Ordering::SeqCst);
        probe.block_response_flush.store(false, Ordering::SeqCst);
        pending(future.as_mut()).await;
        assert_eq!(state(&control, id), NacelleConnectionActivityState::Idle);
        assert_eq!(selected(&control), 1);
        assert!(matches!(
            future.await,
            Err(NacelleError::ConnectionTerminated(
                NacelleConnectionCloseReason::IdleReclaim
            ))
        ));
        assert_released(&runtime);
    }
}

#[tokio::test]
async fn streaming_response_retains_activity_and_application_permit_until_abort() {
    let runtime = tracked_runtime(1);
    let control = runtime.control();
    let capacity = Arc::new(Semaphore::new(1));
    let (sender, body) = NacelleBody::channel(1);
    let body = Arc::new(std::sync::Mutex::new(Some(body)));
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(CustomProtocol::default())
        .runtime_state(runtime.clone())
        .unwrap()
        .tcp_config(
            NacelleTcpConfig::default()
                .with_head_read_limits(TcpHeadReadLimits::new(2, 2).unwrap()),
        )
        .handler(handler_fn(
            move |context: TcpRequestContext<CustomProtocol>| {
                let body = body.lock().unwrap().take().unwrap();
                async move { context.respond(CustomResponse { tag: b'A', body }).await }
            },
        ))
        .admission(
            Capacity(capacity.clone()),
            AdmissionConfig::new(Duration::from_secs(1), 1).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
    let (mut client, stream) = tokio::io::duplex(128);
    client.write_all(b"A\0").await.unwrap();
    sender.send(Ok(Bytes::from_static(b"x"))).await.unwrap();
    let future = server.serve_io(stream);
    tokio::pin!(future);
    pending(future.as_mut()).await;
    let id = control.connections(1).unwrap()[0].connection.connection_id;
    control.set_max_connections(Some(0)).unwrap();
    assert_eq!(selected(&control), 0);
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
    assert!(future.await.is_err());
    assert_eq!(capacity.available_permits(), 1);
    assert!(sender.is_closed());
    assert_released(&runtime);
}

#[tokio::test]
async fn forced_close_interrupts_pending_normal_shutdown() {
    let runtime = tracked_runtime(1);
    let control = runtime.control();
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(CustomProtocol::default())
        .runtime_state(runtime.clone())
        .unwrap()
        .handler(handler_fn(
            |context: TcpRequestContext<CustomProtocol>| async move { Ok(context.complete()) },
        ))
        .build()
        .unwrap();
    let probe = Arc::new(Probe::default());
    probe.block_shutdown.store(true, Ordering::SeqCst);
    let (mut client, stream) = tokio::io::duplex(128);
    client.shutdown().await.unwrap();
    let future = server.serve_io(io(stream, probe.clone()));
    tokio::pin!(future);
    pending(future.as_mut()).await;
    let id = control.connections(1).unwrap()[0].connection.connection_id;
    assert_eq!(state(&control, id), NacelleConnectionActivityState::Closing);
    assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
    let shutdowns = probe.shutdowns.load(Ordering::SeqCst);
    assert!(matches!(
        future.await,
        Err(NacelleError::ConnectionTerminated(
            NacelleConnectionCloseReason::Forced
        ))
    ));
    assert_eq!(probe.shutdowns.load(Ordering::SeqCst), shutdowns);
    assert_released(&runtime);
}

#[tokio::test]
async fn direct_registration_rejects_full_and_duplicate_ids_and_cleans_up_abort() {
    let runtime = tracked_runtime(1);
    let control = runtime.control();
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(CustomProtocol::default())
        .runtime_state(runtime.clone())
        .unwrap()
        .handler(handler_fn(
            |context: TcpRequestContext<CustomProtocol>| async move { Ok(context.complete()) },
        ))
        .build()
        .unwrap();
    let connection = NacelleConnectionMeta::tcp(None, None);
    let (client, stream) = tokio::io::duplex(128);
    let mut first = Box::pin(server.serve_io_with_connection_meta(stream, connection.clone()));
    pending(first.as_mut()).await;
    for duplicate in [true, false] {
        let probe = Arc::new(Probe::default());
        let (_client, stream) = tokio::io::duplex(128);
        let metadata = if duplicate {
            connection.clone()
        } else {
            NacelleConnectionMeta::tcp(None, None)
        };
        let error = server
            .serve_io_with_connection_meta(io(stream, probe.clone()), metadata)
            .await
            .unwrap_err();
        assert!(matches!(error, NacelleError::ResourceLimit(reason)
            if reason == if duplicate { NacelleResourceLimitReason::DuplicateConnectionId } else { NacelleResourceLimitReason::ConnectionRegistry }));
        assert_eq!(probe.reads.load(Ordering::SeqCst), 0);
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_connections(), 1);
    }
    drop(first);
    drop(client);
    assert_released(&runtime);
    assert!(control.connections(1).unwrap().is_empty());
}

#[tokio::test]
async fn borrowed_serial_cancellation_returns_io_and_releases_registration() {
    let runtime = tracked_runtime(1);
    let control = runtime.control();
    let connection = NacelleConnectionMeta::tcp(None, None);
    let (client, mut stream) = tokio::io::duplex(128);
    let mut future = Box::pin(serve_serial_stream(
        &mut stream,
        Arc::new(SerialCounterProtocol),
        Arc::new(crate::admission::SerialHandlerAdapter(SerialCounterHandler)),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        runtime.clone(),
        NacelleTcpLimits::default(),
        connection.clone(),
    ));
    pending(future.as_mut()).await;
    assert_eq!(
        control.close_connection(connection.connection_id),
        NacelleCloseOutcome::Selected
    );
    assert!(future.await.is_err());
    assert_released(&runtime);
    assert!(control.connections(1).unwrap().is_empty());
    // The borrowed transport remains the caller's responsibility, not a closed socket.
    drop(client);
    stream.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn unix_shared_and_serial_listeners_register_and_close_owned_streams() {
    for serial in [false, true] {
        let runtime = tracked_runtime(1);
        let control = runtime.control();
        let path = std::env::temp_dir().join(format!(
            "nacelle-tracking-{}-{}.sock",
            std::process::id(),
            NacelleConnectionMeta::tcp(None, None).connection_id,
        ));
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let shutdown = NacelleShutdown::new();
        let task = tokio::spawn({
            let runtime = runtime.clone();
            let path = path.clone();
            let token = shutdown.token();
            async move {
                if serial {
                    let server = SerialTcpServer::new(SerialCounterProtocol, SerialCounterHandler)
                        .with_runtime_state(runtime)
                        .unwrap();
                    crate::runtime::serve_serial_unix_listener_with_shutdown_deadline(
                        Arc::new(server),
                        listener,
                        Some(path),
                        token,
                        NacelleDrainDeadline::new(Duration::from_secs(1)),
                    )
                    .await
                } else {
                    let server = TcpServer::<CustomProtocol>::builder()
                        .protocol(CustomProtocol::default())
                        .runtime_state(runtime)
                        .unwrap()
                        .handler(handler_fn(
                            |context: TcpRequestContext<CustomProtocol>| async move {
                                Ok(context.complete())
                            },
                        ))
                        .build()
                        .unwrap();
                    crate::runtime::serve_unix_listener_with_shutdown_deadline(
                        Arc::new(server),
                        listener,
                        Some(path),
                        token,
                        NacelleDrainDeadline::new(Duration::from_secs(1)),
                    )
                    .await
                }
            }
        });
        let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
        let snapshot = registered(&runtime).await;
        assert_eq!(snapshot.connection.local_path.as_ref(), Some(&path));
        assert_eq!(
            control.close_connection(snapshot.connection.connection_id),
            NacelleCloseOutcome::Selected
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), client.read(&mut [0]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        shutdown.shutdown();
        task.await.unwrap().unwrap();
        assert_released(&runtime);
        assert!(control.connections(1).unwrap().is_empty());
        std::fs::remove_file(path).unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn idle_timeout_error_delivery_is_active_until_cancelled() {
    let runtime = tracked_runtime(1);
    let control = runtime.control();
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let protocol = CustomProtocol {
        error_outcomes: Some(outcomes.clone()),
        ..CustomProtocol::default()
    };
    let server = TcpServer::<CustomProtocol>::builder()
        .protocol(protocol)
        .runtime_state(runtime.clone())
        .unwrap()
        .tcp_limits(
            NacelleTcpLimits::default()
                .with_idle_timeout(Duration::from_secs(1))
                .without_write_timeout(),
        )
        .handler(handler_fn(
            |context: TcpRequestContext<CustomProtocol>| async move { Ok(context.complete()) },
        ))
        .build()
        .unwrap();
    let probe = Arc::new(Probe::default());
    *probe.write_budget.lock().unwrap() = Some(1);
    let (_client, stream) = tokio::io::duplex(128);
    let future = server.serve_io(io(stream, probe.clone()));
    tokio::pin!(future);
    pending(future.as_mut()).await;
    let id = control.connections(1).unwrap()[0].connection.connection_id;
    tokio::time::advance(Duration::from_secs(1)).await;
    pending(future.as_mut()).await;
    assert_eq!(*probe.written.lock().unwrap(), b"[");
    assert_eq!(state(&control, id), NacelleConnectionActivityState::Active);
    control.set_max_connections(Some(0)).unwrap();
    assert_eq!(selected(&control), 0);
    assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
    assert!(future.await.is_err());
    assert_eq!(
        *outcomes.lock().unwrap(),
        vec![CapturedOutcome::Aborted {
            reason: ResponseAbortReason::Cancelled,
        }]
    );
    assert_eq!(*probe.written.lock().unwrap(), b"[");
    assert_released(&runtime);
}

#[test]
fn administrative_close_metrics_have_one_owner_and_respect_switches() {
    use metrics_util::debugging::DebugValue;
    for (metrics, connections) in [(false, true), (true, false), (true, true)] {
        let runtime = tracked_runtime(1);
        let observer = NacelleInMemoryObserver::new();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let server = TcpServer::<CustomProtocol>::builder()
                        .protocol(CustomProtocol::default())
                        .runtime_state(runtime.clone())
                        .unwrap()
                        .handler(handler_fn(
                            |context: TcpRequestContext<CustomProtocol>| async move {
                                Ok(context.complete())
                            },
                        ))
                        .build()
                        .unwrap()
                        .with_telemetry(
                            NacelleTelemetry::default()
                                .with_metrics(metrics)
                                .with_connection_metrics(connections)
                                .with_observer(observer.clone()),
                        );
                    let (_client, stream) = tokio::io::duplex(128);
                    let future = server.serve_io(stream);
                    tokio::pin!(future);
                    pending(future.as_mut()).await;
                    let control = runtime.control();
                    let id = control.connections(1).unwrap()[0].connection.connection_id;
                    assert_eq!(control.close_connection(id), NacelleCloseOutcome::Selected);
                    assert!(future.await.is_err());
                });
        });
        assert_released(&runtime);
        let events = observer.events();
        let closes: Vec<_> = events
            .iter()
            .filter(|event| event.kind == NacelleTelemetryEventKind::ConnectionClosed)
            .collect();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].reason, Some("administrative_close"));
        let snapshot = snapshotter.snapshot().into_vec();
        for name in ["server.connection.accepted", "server.connection.closed"] {
            let values: Vec<_> = snapshot
                .iter()
                .filter(|(key, _, _, _)| key.key().name() == name)
                .map(|(_, _, _, value)| value)
                .collect();
            assert_eq!(
                values,
                if metrics && connections {
                    vec![&DebugValue::Counter(1)]
                } else {
                    vec![]
                }
            );
        }
        for (key, _, _, value) in &snapshot {
            assert!(
                key.key()
                    .labels()
                    .all(|label| label.key() != "connection_id")
            );
            if key.key().name() == "server.connection.active" {
                assert_eq!(value, &DebugValue::Gauge(0.0.into()));
            }
        }
    }
}
