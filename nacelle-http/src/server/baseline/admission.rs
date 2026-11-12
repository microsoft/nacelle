use super::*;
use crate::admission::{HttpAdmissionContext, LocalHttpAdmissionContext};
use nacelle_core::{Admission, LocalAdmission, NacelleLimits};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

struct AccessLogSubscriber {
    reasons: Arc<Mutex<Vec<String>>>,
}

impl Subscriber for AccessLogSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "nacelle::access"
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        struct ReasonVisitor<'a> {
            reasons: &'a Mutex<Vec<String>>,
        }

        impl Visit for ReasonVisitor<'_> {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "reason" {
                    self.reasons
                        .lock()
                        .expect("access reason lock")
                        .push(format!("{value:?}"));
                }
            }

            fn record_str(&mut self, field: &Field, value: &str) {
                if field.name() == "reason" {
                    self.reasons
                        .lock()
                        .expect("access reason lock")
                        .push(value.to_owned());
                }
            }
        }

        event.record(&mut ReasonVisitor {
            reasons: self.reasons.as_ref(),
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[test]
fn admission_error_metrics_and_observers_have_independent_exact_ownership() {
    for (metrics, errors) in [(false, true), (true, true), (true, false)] {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let observer = NacelleInMemoryObserver::new();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut rule = Capacity::new(1);
                    rule.reject = Some(AdmissionRejection::Forbidden);
                    let telemetry = NacelleTelemetry::default()
                        .with_metrics(metrics)
                        .with_error_metrics(errors)
                        .with_connection_metrics(false)
                        .with_runtime_metrics(false)
                        .with_request_started_metrics(false)
                        .with_request_completed_metrics(false)
                        .with_byte_count_metrics(false)
                        .with_observer(observer.clone());
                    let server = Arc::new(server(rule, Arc::default()).with_telemetry(telemetry));
                    let (io, _) = ObservedIo::new(
                        "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nbody",
                    );
                    run(server, io).await.unwrap();
                });
        });
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

struct Permit {
    _capacity: OwnedSemaphorePermit,
    drops: Arc<AtomicUsize>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct Capacity {
    semaphore: Arc<Semaphore>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    reject: Option<AdmissionRejection>,
}

impl Capacity {
    fn new(capacity: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity)),
            calls: Arc::default(),
            drops: Arc::default(),
            reject: None,
        }
    }
}

impl<'a> Admission<HttpAdmissionContext<'a>> for Capacity {
    type Permit = Permit;

    async fn admit(&self, context: HttpAdmissionContext<'a>) -> Result<Permit, AdmissionRejection> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let capacity = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionRejection::Unavailable)?;
        assert_eq!(context.head().headers.get("host").unwrap(), "localhost");
        if let Some(rejection) = self.reject {
            return Err(rejection);
        }
        Ok(Permit {
            _capacity: capacity,
            drops: self.drops.clone(),
        })
    }
}

fn config() -> AdmissionConfig {
    AdmissionConfig::new(Duration::from_millis(20), 1).unwrap()
}

fn server(
    rule: Capacity,
    calls: Arc<AtomicUsize>,
) -> HyperServer<impl HttpService<(), Permit = Permit>> {
    HyperServer::new(handler_fn(move |mut context: HttpRequestContext<()>| {
        calls.fetch_add(1, Ordering::SeqCst);
        async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                chunk?;
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        }
    }))
    .with_admission(rule, config())
}

fn run<H, Observer>(
    server: Arc<HyperServer<H, NoHttpConnectionState, Observer>>,
    io: ObservedIo,
) -> impl Future<Output = Result<(), NacelleError>>
where
    H: HttpService<()>,
    Observer: NacelleTelemetryObserver,
{
    let metadata = NacelleConnectionMeta::http(None);
    let registration = register_http_connection(&server.runtime.runtime_state, &metadata).unwrap();
    run_http_connection(server, std::future::ready(Ok((io, metadata))), registration)
}

#[tokio::test]
async fn pending_policy_bounds_waiters_without_body_work_or_additional_reads() {
    for chunked in [false, true] {
        let rule = Capacity::new(0);
        let calls = Arc::default();
        let runtime = NacelleRuntimeState::new(
            NacelleLimits::default().with_max_in_flight_requests(usize::MAX),
        );
        runtime.control().set_max_connections(None).unwrap();
        let server = Arc::new(
            server(rule.clone(), Arc::clone(&calls))
                .with_runtime_state(runtime.clone())
                .unwrap(),
        );
        let length = if chunked {
            "Transfer-Encoding: chunked"
        } else {
            "Content-Length: 65536"
        };
        let head = format!("POST / HTTP/1.1\r\nHost: localhost\r\n{length}\r\n\r\n");
        let (io, observed) = ObservedIo::new(format!("{head}{}", "a".repeat(65_536)));
        let mut first = Box::pin(run(server.clone(), io));
        assert_pending(first.as_mut());
        let read = observed.read_bytes.load(Ordering::Relaxed);
        assert!(read > head.len() && read <= 8192);
        assert_pending(first.as_mut());
        assert_eq!(observed.read_bytes.load(Ordering::Relaxed), read);
        assert_eq!(runtime.active_requests(), 1);
        assert_eq!(runtime.active_streaming_tasks(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let (io, observed) = ObservedIo::new(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nbodyGET /next HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        run(server.clone(), io).await.unwrap();
        assert!(
            observed
                .output
                .lock()
                .unwrap()
                .starts_with(b"HTTP/1.1 503 ")
        );
        assert_eq!(rule.calls.load(Ordering::SeqCst), 1);
        drop(first);
        assert_eq!(runtime.active_requests(), 0);
        rule.semaphore.add_permits(1);
        let (io, _) =
            ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        run(server, io).await.unwrap();
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert_eq!(rule.semaphore.available_permits(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn expect_continue_rejections_and_deadline_close_without_dispatch() {
    for (reject, available, status) in [
        (Some(AdmissionRejection::Forbidden), 1, "403"),
        (Some(AdmissionRejection::RateLimited), 1, "429"),
        (Some(AdmissionRejection::Unavailable), 1, "503"),
        (None, 0, "503"),
    ] {
        let mut rule = Capacity::new(available);
        rule.reject = reject;
        let calls = Arc::default();
        let observer = NacelleInMemoryObserver::new();
        let server = Arc::new(
            server(rule.clone(), Arc::clone(&calls))
                .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone()))
                .with_http_policy(NacelleHttpPolicy::new().with_default_security_headers()),
        );
        let runtime = server.runtime.runtime_state.clone();
        let (io, observed) = ObservedIo::new(
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\nbodyGET /next HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        run(server, io).await.unwrap();
        let output = String::from_utf8(observed.output.lock().unwrap().clone()).unwrap();
        assert!(output.starts_with(&format!("HTTP/1.1 {status} ")));
        assert!(output.contains("connection: close\r\n"));
        assert!(output.contains("x-content-type-options: nosniff\r\n"));
        assert!(!output.contains("100 Continue"));
        assert_eq!(output.matches("HTTP/1.1 ").count(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_requests(), 0);
        assert_eq!(runtime.active_streaming_tasks(), 0);
        for kind in [
            NacelleTelemetryEventKind::RequestRejected,
            NacelleTelemetryEventKind::RequestFailed,
        ] {
            assert_eq!(
                observer
                    .events()
                    .iter()
                    .filter(|event| event.kind == kind)
                    .count(),
                1
            );
        }
        assert_eq!(rule.semaphore.available_permits(), available);
    }
}

#[tokio::test]
async fn expect_continue_waits_for_allowance_and_cancellation_releases_the_permit() {
    let rule = Capacity::new(0);
    let calls = Arc::default();
    let server = Arc::new(server(rule.clone(), Arc::clone(&calls)));
    let state = server.runtime.runtime_state.clone();
    let (io, observed) = ObservedIo::new(
        "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n",
    );
    let mut connection = Box::pin(run(server, io));
    assert_pending(connection.as_mut());
    assert!(observed.output.lock().unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    rule.semaphore.add_permits(1);
    assert_pending(connection.as_mut());
    assert_pending(connection.as_mut());
    assert!(
        observed
            .output
            .lock()
            .unwrap()
            .starts_with(b"HTTP/1.1 100 Continue\r\n\r\n")
    );
    assert_eq!(rule.calls.load(Ordering::SeqCst), 1);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 0);
    drop(connection);
    assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    assert_eq!(state.active_requests(), 0);
    assert_eq!(state.active_streaming_tasks(), 0);
}

#[tokio::test]
async fn response_production_owns_permit_independently_of_request_gauge_and_flush() {
    for finish in [false, true] {
        let rule = Capacity::new(1);
        let (tx, body) = NacelleBody::channel(1);
        let body = Mutex::new(Some(body));
        let observer = NacelleInMemoryObserver::new();
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
            .with_admission(rule.clone(), config())
            .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
        );
        let state = server.runtime.runtime_state.clone();
        let (io, observed) =
            ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        let mut connection = Box::pin(run(server, io));
        assert_pending(connection.as_mut());
        assert_eq!(state.active_requests(), 0);
        assert_eq!(rule.drops.load(Ordering::SeqCst), 0);
        if finish {
            tx.try_send(Ok(Bytes::from_static(b"body"))).unwrap();
            drop(tx);
            observed.block_flush.store(true, Ordering::Relaxed);
            assert_pending(connection.as_mut());
            assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                observer
                    .events()
                    .iter()
                    .filter(|event| event.kind == NacelleTelemetryEventKind::ResponseBodyBytes)
                    .map(|event| event.count)
                    .collect::<Vec<_>>(),
                vec![4]
            );
            observed.block_flush.store(false, Ordering::Relaxed);
            connection.await.unwrap();
        } else {
            drop(connection);
        }
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        assert_eq!(rule.semaphore.available_permits(), 1);
    }
}

#[tokio::test]
async fn head_empty_and_final_length_frame_release_without_an_extra_eof_poll() {
    for (method, status, payload) in [
        ("HEAD", StatusCode::OK, "body"),
        ("GET", StatusCode::NO_CONTENT, ""),
        ("GET", StatusCode::OK, "body"),
    ] {
        let rule = Capacity::new(1);
        let server = Arc::new(
            HyperServer::new(handler_fn(
                move |context: HttpRequestContext<()>| async move {
                    let mut response = HttpResponse::bytes(status, payload);
                    response.headers.insert(
                        http::header::CONTENT_LENGTH,
                        http::HeaderValue::from(payload.len()),
                    );
                    context.respond(response).await
                },
            ))
            .with_admission(rule.clone(), config()),
        );
        let (io, observed) = ObservedIo::new(format!(
            "{method} / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        ));
        observed.block_flush.store(true, Ordering::Relaxed);
        let mut connection = Box::pin(run(server, io));
        assert_pending(connection.as_mut());
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
        observed.block_flush.store(false, Ordering::Relaxed);
        connection.await.unwrap();
        assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn write_deadline_drops_pending_response_production_and_its_permit() {
    let rule = Capacity::new(1);
    let (_tx, body) = NacelleBody::channel(1);
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
        .with_admission(rule.clone(), config())
        .with_http_limits(
            NacelleHttpLimits::default().with_response_write_timeout(Duration::from_millis(10)),
        ),
    );
    let (io, observed) = ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    observed.block_write.store(true, Ordering::Relaxed);
    let mut connection = Box::pin(run(server, io));
    assert_pending(connection.as_mut());
    assert_eq!(rule.drops.load(Ordering::SeqCst), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), connection)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    assert_eq!(rule.semaphore.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn full_body_channel_cannot_block_producer_failure_or_permit_cleanup() {
    let rule = Capacity::new(1);
    let observer = NacelleInMemoryObserver::new();
    let runtime = NacelleRuntimeState::new(NacelleLimits::default().with_max_request_body_bytes(8));
    let server = Arc::new(
        HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            std::future::pending::<()>().await;
            context.respond(HttpResponse::empty(StatusCode::OK)).await
        }))
        .with_admission(rule.clone(), config())
        .with_runtime_state(runtime.clone())
        .unwrap()
        .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone())),
    );
    let (io, observed) = ObservedIo::new(format!(
        "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n{}0\r\n\r\n",
        "1\r\na\r\n".repeat(9),
    ));
    tokio::time::timeout(Duration::from_millis(100), run(server, io))
        .await
        .unwrap()
        .unwrap();
    assert!(
        observed
            .output
            .lock()
            .unwrap()
            .starts_with(b"HTTP/1.1 413 ")
    );
    assert_eq!(rule.drops.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
    assert_eq!(
        observer
            .events()
            .iter()
            .filter(
                |event| event.kind == NacelleTelemetryEventKind::RequestFailed
                    && event.reason == Some("request_body_bytes")
            )
            .count(),
        1
    );
}

#[test]
fn streamed_body_limit_access_log_uses_resource_reason() {
    let reasons = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::with_default(
        AccessLogSubscriber {
            reasons: reasons.clone(),
        },
        || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let runtime = NacelleRuntimeState::new(
                        NacelleLimits::default().with_max_request_body_bytes(8),
                    );
                    let server = Arc::new(
                        HyperServer::new(handler_fn(
                            |context: HttpRequestContext<()>| async move {
                                std::future::pending::<()>().await;
                                context.respond(HttpResponse::empty(StatusCode::OK)).await
                            },
                        ))
                        .with_access_log(true)
                        .with_runtime_state(runtime)
                        .unwrap(),
                    );
                    let (io, _) = ObservedIo::new(format!(
                        "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n{}0\r\n\r\n",
                        "1\r\na\r\n".repeat(9),
                    ));
                    tokio::time::timeout(Duration::from_millis(100), run(server, io))
                        .await
                        .unwrap()
                        .unwrap();
                });
        },
    );

    assert_eq!(
        *reasons.lock().expect("access reason lock"),
        vec!["request_body_bytes"]
    );
}

#[tokio::test]
async fn parser_limits_and_declared_body_reject_before_custom_admission() {
    for (input, headers, body_limit, status) in [
        (
            "GET / HTTP/1.1\r\nHost: localhost\r\nX-Extra: value\r\n\r\n",
            Some(1),
            1024,
            "431",
        ),
        (
            "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nbody",
            None,
            1,
            "413",
        ),
    ] {
        let rule = Capacity::new(0);
        let server = Arc::new(
            server(rule.clone(), Arc::default())
                .with_http_limits(
                    NacelleHttpLimits::default()
                        .with_parser_limits(crate::HttpParserLimits::new(8192, headers).unwrap()),
                )
                .with_runtime_state(NacelleRuntimeState::new(
                    NacelleLimits::default().with_max_request_body_bytes(body_limit),
                ))
                .unwrap(),
        );
        let (io, observed) = ObservedIo::new(input);
        let _ = run(server, io).await;
        assert!(
            String::from_utf8_lossy(&observed.output.lock().unwrap())
                .starts_with(&format!("HTTP/1.1 {status} "))
        );
        assert_eq!(rule.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn local_policy_and_permit_can_borrow_rc_state_across_await() {
    struct LocalRule(Rc<std::cell::Cell<usize>>);
    impl<'a> LocalAdmission<LocalHttpAdmissionContext<'a>> for LocalRule {
        type Permit = Rc<std::cell::Cell<usize>>;
        async fn admit(
            &self,
            context: LocalHttpAdmissionContext<'a>,
        ) -> Result<Self::Permit, AdmissionRejection> {
            tokio::task::yield_now().await;
            assert_eq!(context.head().method, Method::GET);
            self.0.set(self.0.get() + 1);
            Ok(self.0.clone())
        }
    }
    let calls = Rc::new(std::cell::Cell::new(0));
    let server = Rc::new(
        LocalHyperServer::new(nacelle_core::pipeline::local_handler_fn(
            |context: LocalHttpRequestContext<()>| async move {
                context.respond(HttpResponse::empty(StatusCode::OK)).await
            },
        ))
        .with_admission(LocalRule(calls.clone()), config()),
    );
    let metadata = NacelleConnectionMeta::http(None);
    let registration = register_http_connection(&server.runtime.runtime_state, &metadata).unwrap();
    let (io, _) = ObservedIo::new("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    run_local_http_connection(server, std::future::ready(Ok((io, metadata))), registration)
        .await
        .unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(Rc::strong_count(&calls), 1);
}
