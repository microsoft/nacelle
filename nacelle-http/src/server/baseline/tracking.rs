use super::*;
use crate::HttpAdmissionContext;
use nacelle_core::{
    Admission, NacelleCloseOutcome, NacelleConnectionActivityState, NacelleConnectionCloseReason,
    NacelleConnectionLookup, NacelleConnectionTracking, NacelleLimits, NacelleReclaimOutcome,
    NacelleRetireOutcome,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

fn state() -> NacelleRuntimeState {
    NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_connection_tracking(NacelleConnectionTracking::new(4).unwrap()),
    )
}

fn run<H, Observer>(
    server: Arc<HyperServer<H, NoHttpConnectionState, Observer>>,
    mut io: ObservedIo,
) -> (impl Future<Output = Result<(), NacelleError>>, u64)
where
    H: HttpService<()>,
    Observer: NacelleTelemetryObserver,
{
    let metadata = NacelleConnectionMeta::http(None);
    let id = metadata.connection_id;
    io.drop_state = Some(server.runtime.runtime_state.clone());
    let registration = register_http_connection(&server.runtime.runtime_state, &metadata).unwrap();
    (
        run_http_connection(server, std::future::ready(Ok((io, metadata))), registration),
        id,
    )
}

fn clean(state: &NacelleRuntimeState, observed: &Observations) {
    assert!(observed.dropped.load(Ordering::Relaxed));
    assert_eq!(observed.connections_at_drop.load(Ordering::Relaxed), 1);
    assert_eq!(state.active_connections(), 0);
    assert_eq!(state.active_requests(), 0);
    assert_eq!(state.active_streaming_tasks(), 0);
    assert!(state.control().connections(4).unwrap().is_empty());
}

fn closed(observer: &NacelleInMemoryObserver, reason: &'static str) {
    let events = observer.events();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == NacelleTelemetryEventKind::ConnectionOpened)
            .count(),
        1
    );
    let closes: Vec<_> = events
        .iter()
        .filter(|e| e.kind == NacelleTelemetryEventKind::ConnectionClosed)
        .collect();
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].reason, Some(reason));
}

#[derive(Clone)]
struct Capacity(Arc<Semaphore>);

impl<'a> Admission<HttpAdmissionContext<'a>> for Capacity {
    type Permit = OwnedSemaphorePermit;

    async fn admit(&self, _: HttpAdmissionContext<'a>) -> Result<Self::Permit, AdmissionRejection> {
        self.0
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionRejection::Unavailable)
    }
}

#[tokio::test]
async fn idle_and_partial_heads_explicitly_skip_reclaim() {
    for input in [
        "",
        "GET / HTTP/1.1\r\nHo",
        "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
    ] {
        let state = state();
        let observer = NacelleInMemoryObserver::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let server = Arc::new(
            HyperServer::new(handler_fn({
                let calls = calls.clone();
                move |context: HttpRequestContext<()>| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
                }
            }))
            .with_runtime_state(state.clone())
            .unwrap()
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let (io, observed) = ObservedIo::new(input);
        let (future, id) = run(server, io);
        tokio::pin!(future);
        assert_pending(future.as_mut());
        let NacelleConnectionLookup::Found(snapshot) = state.control().connection(id) else {
            panic!("tracked");
        };
        assert_eq!(snapshot.activity, NacelleConnectionActivityState::Active);
        assert!(!snapshot.capabilities.idle_reclaim);
        assert!(snapshot.capabilities.graceful_retirement);
        state.control().set_max_connections(Some(0)).unwrap();
        let NacelleReclaimOutcome::Observed {
            selected,
            unfilled,
            partitions,
        } = state.control().reclaim_idle(4)
        else {
            panic!("tracked");
        };
        assert_eq!((selected, unfilled, partitions[0].unsupported), (0, 1, 1));
        assert!(!observed.dropped.load(Ordering::Relaxed));
        assert_eq!(
            state.control().close_connection(id),
            NacelleCloseOutcome::Selected
        );
        assert!(matches!(
            future.await,
            Err(NacelleError::ConnectionTerminated(
                NacelleConnectionCloseReason::Forced
            ))
        ));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(input.ends_with("\r\n\r\n"))
        );
        clean(&state, &observed);
        closed(&observer, "administrative_close");
    }
}

#[tokio::test(start_paused = true)]
async fn listener_drain_deadline_releases_pending_delivery_with_or_without_tracking() {
    for tracked in [false, true] {
        let state = if tracked {
            state()
        } else {
            NacelleRuntimeState::default()
        };
        let observer = NacelleInMemoryObserver::new();
        let telemetry = NacelleTelemetry::default().with_observer(observer.clone());
        let capacity = Arc::new(Semaphore::new(1));
        let (sender, body) = NacelleBody::channel(1);
        let body = Mutex::new(Some(body));
        let server = Arc::new(
            HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
                let body = body.lock().unwrap().take().unwrap();
                async move {
                    context
                        .respond(HttpResponse::new(
                            StatusCode::OK,
                            http::HeaderMap::new(),
                            body,
                        ))
                        .await
                }
            }))
            .with_admission(
                Capacity(capacity.clone()),
                AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
            )
            .with_runtime_state(state.clone())
            .unwrap()
            .with_telemetry(telemetry.clone()),
        );
        let (io, observed) = ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
        let (future, _) = run(server, io);
        let mut future = Box::pin(future);
        assert_pending(future.as_mut());
        assert_eq!(capacity.available_permits(), 0);
        let mut connections = tokio::task::JoinSet::new();
        connections.spawn(future);
        drain_http_connection_tasks(connections, Duration::from_secs(1), telemetry).await;
        assert!(sender.is_closed());
        assert_eq!(capacity.available_permits(), 1);
        assert!(observed.dropped.load(Ordering::Relaxed));
        assert_eq!(observed.connections_at_drop.load(Ordering::Relaxed), 1);
        assert_eq!(state.active_connections(), 0);
        assert_eq!(state.active_requests(), 0);
        assert_eq!(state.active_streaming_tasks(), 0);
        if tracked {
            assert!(state.control().connections(4).unwrap().is_empty());
        }
        closed(&observer, "cancelled");
        assert_eq!(
            observer
                .events()
                .iter()
                .filter(|e| e.kind == NacelleTelemetryEventKind::DrainTimedOut)
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn graceful_retirement_drains_body_and_flush_but_not_buffered_next_request() {
    let state = state();
    let observer = NacelleInMemoryObserver::new();
    let capacity = Arc::new(Semaphore::new(1));
    let (sender, body) = NacelleBody::channel(1);
    let body = Mutex::new(Some(body));
    let server = Arc::new(
        HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
            let body = body
                .lock()
                .unwrap()
                .take()
                .expect("next request must not dispatch");
            async move {
                context
                    .respond(HttpResponse::new(
                        StatusCode::OK,
                        http::HeaderMap::new(),
                        body,
                    ))
                    .await
            }
        }))
        .with_admission(
            Capacity(capacity.clone()),
            AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
        )
        .with_runtime_state(state.clone())
        .unwrap()
        .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
    );
    let (io, observed) = ObservedIo::new(
        "GET /one HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    let (future, id) = run(server, io);
    tokio::pin!(future);
    assert_pending(future.as_mut());
    assert_eq!(state.active_requests(), 0);
    assert_eq!(capacity.available_permits(), 0);
    assert_eq!(
        state
            .control()
            .retire_connection(id, Duration::from_secs(5)),
        NacelleRetireOutcome::Selected
    );
    assert_pending(future.as_mut());
    assert_eq!(state.active_connections(), 1);
    sender.try_send(Ok(Bytes::from_static(b"drained"))).unwrap();
    drop(sender);
    observed.block_flush.store(true, Ordering::Relaxed);
    assert_pending(future.as_mut());
    assert_eq!(capacity.available_permits(), 1);
    assert!(observed.flushes.load(Ordering::Relaxed) > 0);
    assert!(!observed.dropped.load(Ordering::Relaxed));
    assert_eq!(state.active_connections(), 1);
    assert_eq!(
        state
            .control()
            .retire_connection(id, Duration::from_secs(10)),
        NacelleRetireOutcome::AlreadyClosing
    );
    observed.block_flush.store(false, Ordering::Relaxed);
    future.await.unwrap();
    let output = observed.output.lock().unwrap();
    assert!(output.windows(7).any(|bytes| bytes == b"drained"));
    assert_eq!(
        output
            .windows(9)
            .filter(|bytes| *bytes == b"HTTP/1.1 ")
            .count(),
        1
    );
    clean(&state, &observed);
    closed(&observer, "administrative_retire");
}

#[tokio::test(start_paused = true)]
async fn retirement_deadline_cancels_pending_body_or_flush_without_replaying_output() {
    for pending_body in [false, true] {
        let state = state();
        let observer = NacelleInMemoryObserver::new();
        let capacity = Arc::new(Semaphore::new(1));
        let (sender, body) = NacelleBody::channel(1);
        let body = Mutex::new(Some(body));
        let server = Arc::new(
            HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
                let body = body.lock().unwrap().take().unwrap();
                async move {
                    context
                        .respond(HttpResponse::new(
                            StatusCode::OK,
                            http::HeaderMap::new(),
                            body,
                        ))
                        .await
                }
            }))
            .with_admission(
                Capacity(capacity.clone()),
                AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
            )
            .with_runtime_state(state.clone())
            .unwrap()
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let (io, observed) = ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
        let (future, id) = run(server, io);
        tokio::pin!(future);
        assert_pending(future.as_mut());
        sender.try_send(Ok(Bytes::from_static(b"prefix"))).unwrap();
        let sender = pending_body.then_some(sender);
        observed.block_flush.store(!pending_body, Ordering::Relaxed);
        assert_pending(future.as_mut());
        assert_eq!(
            state
                .control()
                .retire_connection(id, Duration::from_secs(2)),
            NacelleRetireOutcome::Selected
        );
        assert_pending(future.as_mut());
        let prefix = observed.output.lock().unwrap().clone();
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(matches!(
            future.await,
            Err(NacelleError::ConnectionTerminated(
                NacelleConnectionCloseReason::RetirementDeadline
            ))
        ));
        assert_eq!(*observed.output.lock().unwrap(), prefix);
        assert_eq!(observed.shutdowns.load(Ordering::Relaxed), 0);
        assert_eq!(capacity.available_permits(), 1);
        if let Some(sender) = sender {
            assert!(sender.try_send(Ok(Bytes::new())).is_err());
        }
        clean(&state, &observed);
        closed(&observer, "administrative_retire_timeout");
        assert_eq!(
            observer
                .events()
                .iter()
                .filter(|e| e.kind == NacelleTelemetryEventKind::Timeout
                    && e.reason == Some("administrative_retire"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn retirement_does_not_admit_expect_body_and_force_releases_original_waiter() {
    let state = state();
    let capacity = Arc::new(Semaphore::new(0));
    let server = Arc::new(
        HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            context.respond(HttpResponse::empty(StatusCode::OK)).await
        }))
        .with_admission(
            Capacity(capacity.clone()),
            AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
        )
        .with_runtime_state(state.clone())
        .unwrap(),
    );
    let (io, observed) = ObservedIo::new(
        "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n",
    );
    let (future, id) = run(server.clone(), io);
    tokio::pin!(future);
    assert_pending(future.as_mut());
    assert_eq!(state.active_requests(), 1);
    assert_eq!(state.active_streaming_tasks(), 0);
    let admission = server.handler.admission_state().unwrap();
    assert!(admission.acquire().is_err());
    assert_eq!(
        state
            .control()
            .retire_connection(id, Duration::from_secs(5)),
        NacelleRetireOutcome::Selected
    );
    assert_pending(future.as_mut());
    assert!(observed.output.lock().unwrap().is_empty());
    assert_eq!(state.active_streaming_tasks(), 0);
    assert_eq!(
        state.control().close_connection(id),
        NacelleCloseOutcome::Selected
    );
    assert!(matches!(
        future.await,
        Err(NacelleError::ConnectionTerminated(
            NacelleConnectionCloseReason::Forced
        ))
    ));
    assert!(admission.acquire().is_ok());
    capacity.add_permits(1);
    assert_eq!(capacity.available_permits(), 1);
    clean(&state, &observed);
}

#[tokio::test(start_paused = true)]
async fn terminal_reset_age_and_write_timeout_release_registration_and_leases() {
    for failure in ["reset", "age", "write", "body"] {
        let state = state();
        let observer = NacelleInMemoryObserver::new();
        let server = Arc::new(
            HyperServer::new(handler_fn(
                |mut context: HttpRequestContext<()>| async move {
                    while let Some(chunk) = context.request_mut().next_body_chunk().await {
                        chunk?;
                    }
                    context
                        .respond(HttpResponse::bytes(StatusCode::OK, "response"))
                        .await
                },
            ))
            .with_runtime_state(state.clone())
            .unwrap()
            .with_http_limits(
                NacelleHttpLimits::default()
                    .with_max_connection_age(Duration::from_secs(if failure == "age" {
                        1
                    } else {
                        60
                    }))
                    .with_response_write_timeout(Duration::from_secs(1))
                    .with_request_body_read_timeout(Duration::from_secs(1)),
            )
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let input = match failure {
            "write" => "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
            "body" => "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\n",
            _ => "",
        };
        let (io, observed) = ObservedIo::new(input);
        observed
            .block_write
            .store(failure == "write", Ordering::Relaxed);
        let (future, _) = run(server, io);
        tokio::pin!(future);
        assert_pending(future.as_mut());
        if failure == "reset" {
            observed.reset.store(true, Ordering::Relaxed);
        } else {
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        let result = future.await;
        match failure {
            "age" => assert!(matches!(
                result,
                Err(NacelleError::Timeout(
                    NacelleTimeoutReason::HttpMaxConnectionAge
                ))
            )),
            "write" => assert!(result.is_err()),
            "body" => assert!(
                observed
                    .output
                    .lock()
                    .unwrap()
                    .starts_with(b"HTTP/1.1 500 ")
            ),
            _ => {}
        }
        clean(&state, &observed);
        assert_eq!(
            observer
                .events()
                .iter()
                .filter(|e| e.kind == NacelleTelemetryEventKind::ConnectionClosed)
                .count(),
            1
        );
    }
}

#[test]
fn tracking_close_ownership_preserves_http_metric_gates_and_labels() {
    for (metrics, connections) in [(true, true), (true, false), (false, true)] {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let observer = NacelleInMemoryObserver::new();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let state = state();
                    let telemetry = NacelleTelemetry::default()
                        .with_metrics(metrics)
                        .with_connection_metrics(connections)
                        .with_observer(observer.clone());
                    let server = Arc::new(
                        HyperServer::new(handler_fn(
                            |_context: HttpRequestContext<()>| async move { panic!("no request") },
                        ))
                        .with_runtime_state(state.clone())
                        .unwrap()
                        .with_telemetry(telemetry),
                    );
                    let (io, _) = ObservedIo::new("");
                    let (future, id) = run(server, io);
                    assert_eq!(
                        state.control().close_connection(id),
                        NacelleCloseOutcome::Selected
                    );
                    assert!(future.await.is_err());
                });
        });
        closed(&observer, "administrative_close");
        let snapshot = snapshotter.snapshot().into_vec();
        let opened: Vec<_> = snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == "server.connection.opened")
            .collect();
        assert_eq!(opened.len(), usize::from(metrics && connections));
        if let Some((key, _, _, value)) = opened.first() {
            assert_eq!(value, &DebugValue::Counter(1));
            assert_eq!(
                key.key()
                    .labels()
                    .map(|l| (l.key(), l.value()))
                    .collect::<Vec<_>>(),
                vec![("transport", "http")]
            );
        }
        assert!(
            snapshot
                .iter()
                .all(|(key, _, _, _)| key.key().name() != "server.connection.closed")
        );
        assert!(
            snapshot
                .iter()
                .all(|(key, _, _, _)| key.key().labels().all(|l| l.key() != "connection_id"))
        );
        if !metrics {
            assert!(snapshot.is_empty());
        }
    }
}

#[tokio::test]
async fn local_non_send_handler_and_permit_remain_owned_through_retirement() {
    struct LocalRule(Rc<()>);
    impl<'a> nacelle_core::LocalAdmission<crate::LocalHttpAdmissionContext<'a>> for LocalRule {
        type Permit = Rc<()>;
        async fn admit(
            &self,
            _: crate::LocalHttpAdmissionContext<'a>,
        ) -> Result<Self::Permit, AdmissionRejection> {
            Ok(self.0.clone())
        }
    }
    let state = state();
    let permit = Rc::new(());
    let (sender, body) = NacelleBody::channel(1);
    let body = Rc::new(std::cell::RefCell::new(Some(body)));
    let server = Rc::new(
        LocalHyperServer::new(nacelle_core::pipeline::local_handler_fn({
            let body = body.clone();
            move |context: LocalHttpRequestContext<()>| {
                let body = body.borrow_mut().take().unwrap();
                async move {
                    context
                        .respond(HttpResponse::new(
                            StatusCode::OK,
                            http::HeaderMap::new(),
                            body,
                        ))
                        .await
                }
            }
        }))
        .with_admission(
            LocalRule(permit.clone()),
            AdmissionConfig::new(Duration::from_secs(30), 1).unwrap(),
        )
        .with_runtime_context(
            NacelleTelemetry::default(),
            state.clone(),
            LocalHttpSharedState::default(),
        )
        .unwrap(),
    );
    let metadata = NacelleConnectionMeta::http(None);
    let id = metadata.connection_id;
    let registration = register_http_connection(&state, &metadata).unwrap();
    let (mut io, observed) = ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    io.drop_state = Some(state.clone());
    let future =
        run_local_http_connection(server, std::future::ready(Ok((io, metadata))), registration);
    tokio::pin!(future);
    assert_pending(future.as_mut());
    assert_eq!(Rc::strong_count(&permit), 3);
    assert_eq!(
        state
            .control()
            .retire_connection(id, Duration::from_secs(5)),
        NacelleRetireOutcome::Selected
    );
    assert_pending(future.as_mut());
    drop(sender);
    future.await.unwrap();
    assert_eq!(Rc::strong_count(&permit), 1);
    clean(&state, &observed);
}

#[cfg(feature = "tls-self-signed")]
#[tokio::test(start_paused = true)]
async fn tls_setup_is_registered_and_bounded_by_administrative_deadline() {
    use tokio::io::AsyncReadExt;
    for outcome in ["force", "retire", "peer", "timeout"] {
        let state = state();
        let observer = NacelleInMemoryObserver::new();
        let server = Arc::new(
            HyperServer::new(handler_fn(|_context: HttpRequestContext<()>| async move {
                panic!("no TLS request")
            }))
            .with_runtime_state(state.clone())
            .unwrap()
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(address), listener.accept());
        let mut client = Some(client.unwrap());
        let (stream, peer) = accepted.unwrap();
        let metadata = NacelleConnectionMeta::http_socket(Some(peer), Some(address))
            .with_listener("tls-setup");
        let id = metadata.connection_id;
        let registration = register_http_connection(&state, &metadata).unwrap();
        let generated = NacelleTlsConfig::self_signed(["localhost"]).unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(generated.tls_config.server_config());
        let setup = accept_http_tls(
            stream,
            metadata,
            acceptor,
            Duration::from_secs(10),
            server.runtime.telemetry.clone(),
        );
        let future = run_http_connection(server, setup, registration);
        tokio::pin!(future);
        assert_pending(future.as_mut());
        let NacelleConnectionLookup::Found(snapshot) = state.control().connection(id) else {
            panic!("setup registered");
        };
        assert_eq!(snapshot.activity, NacelleConnectionActivityState::Setup);
        assert!(snapshot.connection.tls.is_none());
        match outcome {
            "force" => {
                assert_eq!(
                    state.control().close_connection(id),
                    NacelleCloseOutcome::Selected
                );
            }
            "retire" => {
                assert_eq!(
                    state
                        .control()
                        .retire_connection(id, Duration::from_secs(1)),
                    NacelleRetireOutcome::Selected
                );
                assert_pending(future.as_mut());
                tokio::time::advance(Duration::from_secs(1)).await;
            }
            "peer" => {
                client.take();
            }
            "timeout" => tokio::time::advance(Duration::from_secs(10)).await,
            _ => unreachable!(),
        }
        let result = future.await;
        let reason = match outcome {
            "force" => {
                assert!(matches!(
                    result,
                    Err(NacelleError::ConnectionTerminated(
                        NacelleConnectionCloseReason::Forced
                    ))
                ));
                "administrative_close"
            }
            "retire" => {
                assert!(matches!(
                    result,
                    Err(NacelleError::ConnectionTerminated(
                        NacelleConnectionCloseReason::RetirementDeadline
                    ))
                ));
                "administrative_retire_timeout"
            }
            "timeout" => {
                assert!(matches!(
                    result,
                    Err(NacelleError::Timeout(NacelleTimeoutReason::TlsHandshake))
                ));
                "timeout"
            }
            _ => {
                assert!(result.is_err());
                "protocol"
            }
        };
        assert_eq!(state.active_connections(), 0);
        assert!(state.control().connections(4).unwrap().is_empty());
        closed(&observer, reason);
        if let Some(mut client) = client {
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        }
    }
}
#[tokio::test]
async fn unpolled_drop_and_task_abort_close_once_and_release_owned_io_first() {
    for polled in [false, true] {
        let state = state();
        let observer = NacelleInMemoryObserver::new();
        let server = Arc::new(
            HyperServer::new(handler_fn(|_context: HttpRequestContext<()>| async move {
                panic!("no request")
            }))
            .with_runtime_state(state.clone())
            .unwrap()
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let (io, observed) = ObservedIo::new("");
        let (future, _) = run(server, io);
        if polled {
            let mut future = Box::pin(future);
            assert_pending(future.as_mut());
            let task = tokio::spawn(future);
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            drop(future);
        }
        clean(&state, &observed);
        closed(&observer, "cancelled");
    }
}
