use super::*;
use crate::policies::TcpTransport;
use metrics_util::debugging::DebugValue;
use nacelle_core::telemetry::NacelleTransport;

struct ForbiddenState;

impl Default for ForbiddenState {
    fn default() -> Self {
        panic!("connection admission must precede protocol state creation")
    }
}

type RejectedProtocol = StaticProtocol<ForbiddenState>;

#[derive(Clone)]
struct ConnectionAdmission {
    rejection: Option<Rejection>,
    active: Arc<AtomicUsize>,
}

struct ConnectionLease(Arc<AtomicUsize>);

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ConnectionAdmission {
    fn admit(&self) -> Result<Hold<ConnectionLease>, Rejection> {
        if let Some(rejection) = self.rejection {
            return Err(rejection);
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(Hold::new(ConnectionLease(self.active.clone())))
    }
}

impl Admission<TcpTransport<'_>> for ConnectionAdmission {
    type Lease = Hold<ConnectionLease>;

    async fn enter<'a>(&'a self, _: &'a mut TcpTransport<'_>) -> Result<Self::Lease, Rejection> {
        self.admit()
    }
}

impl LocalAdmission<TcpTransport<'_>> for ConnectionAdmission {
    type Lease = Hold<ConnectionLease>;

    async fn enter<'a>(&'a self, _: &'a mut TcpTransport<'_>) -> Result<Self::Lease, Rejection> {
        self.admit()
    }
}

fn policies<Request: Send + 'static, Storage: Send + 'static>(
    stage: &'static str,
    rejection: Rejection,
    active: Arc<AtomicUsize>,
) -> impl SharedTcpPolicySet<Request, Storage> + LocalTcpPolicySet<Request, Storage> {
    let lease = ConnectionAdmission {
        rejection: None,
        active,
    };
    let reject = ConnectionAdmission {
        rejection: Some(rejection),
        ..lease.clone()
    };
    TcpPolicies::new()
        .on_accepted(lease.clone())
        .on_accepted(if stage == "policy_accepted" {
            reject.clone()
        } else {
            lease.clone()
        })
        .on_transport_ready(lease.clone())
        .on_transport_ready(if stage == "policy_transport_ready" {
            reject
        } else {
            lease
        })
}

struct NeverDispatch;

impl SerialTcpHandler<RejectedProtocol> for NeverDispatch {
    async fn call<'a>(
        &'a self,
        _: SerialTcpRequestContext<'a, RejectedProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<RejectedProtocol>, NacelleError> {
        panic!("connection admission must prevent dispatch")
    }
}

impl LocalSerialTcpHandler<RejectedProtocol> for NeverDispatch {
    async fn call<'a>(
        &'a self,
        _: SerialTcpRequestContext<'a, RejectedProtocol>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<RejectedProtocol>, NacelleError> {
        panic!("connection admission must prevent dispatch")
    }
}

#[derive(Clone, Copy, Debug)]
enum Dispatch {
    Shared,
    Serial,
    Local,
    LocalSerial,
}

async fn reject_direct(
    dispatch: Dispatch,
    rejection: Rejection,
    active: Arc<AtomicUsize>,
    telemetry: NacelleTelemetry<NacelleInMemoryObserver>,
    runtime: NacelleRuntimeState,
) {
    let stage = "policy_transport_ready";
    let (stream, writes) = io(&[1, 0]);
    let result = match dispatch {
        Dispatch::Shared => {
            TcpServer::<RejectedProtocol>::builder()
                .protocol(StaticProtocol(PhantomData))
                .handler(handler_fn(
                    |_: TcpRequestContext<RejectedProtocol>| async move {
                        panic!("connection admission must prevent dispatch")
                    },
                ))
                .runtime_state(runtime.clone())
                .telemetry(telemetry)
                .policies(policies(stage, rejection, active))
                .build()
                .unwrap()
                .serve_io(stream)
                .await
        }
        Dispatch::Serial => {
            SerialTcpServer::new(StaticProtocol(PhantomData), NeverDispatch)
                .with_runtime_state(runtime.clone())
                .with_telemetry(telemetry)
                .with_policies(policies(stage, rejection, active))
                .serve_io(stream)
                .await
        }
        Dispatch::Local => {
            LocalTcpServer::new(
                StaticProtocol(PhantomData),
                local_handler_fn(|_: TcpRequestContext<RejectedProtocol>| async move {
                    panic!("connection admission must prevent dispatch")
                }),
            )
            .with_runtime_state(runtime.clone())
            .with_telemetry(telemetry)
            .with_policies(policies(stage, rejection, active))
            .serve_io(stream)
            .await
        }
        Dispatch::LocalSerial => {
            LocalSerialTcpServer::new(StaticProtocol(PhantomData), NeverDispatch)
                .with_runtime_state(runtime.clone())
                .with_telemetry(telemetry)
                .with_policies(policies(stage, rejection, active))
                .serve_io(stream)
                .await
        }
    };
    match (rejection.kind(), result) {
        (RejectionKind::Timeout, Err(NacelleError::Timeout(reason))) => {
            assert_eq!(reason.as_str(), rejection.reason());
        }
        (_, Err(NacelleError::ResourceLimit(reason))) => {
            assert_eq!(reason.as_str(), rejection.reason());
        }
        (_, result) => panic!("unexpected admission result: {result:?}"),
    }
    assert!(writes.lock().unwrap().is_empty());
}

async fn reject_listener(
    dispatch: Dispatch,
    stage: &'static str,
    rejection: Rejection,
    active: Arc<AtomicUsize>,
    telemetry: NacelleTelemetry<NacelleInMemoryObserver>,
    runtime: NacelleRuntimeState,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, token) = NacelleShutdown::pair();
    let deadline = NacelleDrainDeadline::new(Duration::from_secs(2));
    let task = match dispatch {
        Dispatch::Shared => {
            let server = TcpServer::<RejectedProtocol>::builder()
                .protocol(StaticProtocol(PhantomData))
                .handler(handler_fn(
                    |_: TcpRequestContext<RejectedProtocol>| async move {
                        panic!("connection admission must prevent dispatch")
                    },
                ))
                .runtime_state(runtime.clone())
                .telemetry(telemetry)
                .policies(policies(stage, rejection, active))
                .build()
                .unwrap();
            tokio::spawn(crate::runtime::serve_tcp_listener_with_shutdown_deadline(
                Arc::new(server),
                listener,
                token,
                deadline,
            ))
        }
        Dispatch::Serial => {
            let server = SerialTcpServer::new(StaticProtocol(PhantomData), NeverDispatch)
                .with_runtime_state(runtime.clone())
                .with_telemetry(telemetry)
                .with_policies(policies(stage, rejection, active));
            tokio::spawn(
                crate::runtime::serve_serial_tcp_listener_with_options_and_shutdown_deadline(
                    Arc::new(server),
                    listener,
                    crate::NacelleTcpOptions::default(),
                    token,
                    deadline,
                ),
            )
        }
        Dispatch::Local => {
            let server = LocalTcpServer::new(
                StaticProtocol(PhantomData),
                local_handler_fn(|_: TcpRequestContext<RejectedProtocol>| async move {
                    panic!("connection admission must prevent dispatch")
                }),
            )
            .with_runtime_state(runtime.clone())
            .with_telemetry(telemetry)
            .with_policies(policies(stage, rejection, active));
            tokio::task::spawn_local(crate::runtime::serve_local_tcp_listener(
                Rc::new(server),
                listener,
                crate::NacelleTcpOptions::default(),
                token,
                deadline,
            ))
        }
        Dispatch::LocalSerial => {
            let server = LocalSerialTcpServer::new(StaticProtocol(PhantomData), NeverDispatch)
                .with_runtime_state(runtime.clone())
                .with_telemetry(telemetry)
                .with_policies(policies(stage, rejection, active));
            tokio::task::spawn_local(crate::runtime::serve_local_serial_tcp_listener(
                Rc::new(server),
                listener,
                crate::NacelleTcpOptions::default(),
                token,
                deadline,
            ))
        }
    };
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert!(response.is_empty());
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

fn assert_rejection_telemetry(
    observer: &NacelleInMemoryObserver,
    recorder: &DebuggingRecorder,
    stage: &'static str,
    kind: RejectionKind,
    metrics_enabled: bool,
) {
    let events = observer.events();
    let rejected: Vec<_> = events
        .iter()
        .filter(|event| event.kind == NacelleTelemetryEventKind::ConnectionRejected)
        .collect();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].reason, Some("policy"));
    assert_eq!(rejected[0].transport, Some(NacelleTransport::new("tcp")));
    assert_eq!(rejected[0].count, 1);
    assert!(!events.iter().any(|event| matches!(
        event.kind,
        NacelleTelemetryEventKind::ConnectionOpened
            | NacelleTelemetryEventKind::ConnectionClosed
            | NacelleTelemetryEventKind::RequestCompleted
            | NacelleTelemetryEventKind::RequestFailed
    )));

    let snapshot = recorder.snapshotter().snapshot().into_vec();
    if !metrics_enabled {
        assert!(snapshot.is_empty());
        return;
    }
    for (name, expected) in [
        ("server.connection.rejected", 1),
        ("server.errors", 1),
        (
            "server.resource_limit.rejections",
            usize::from(kind != RejectionKind::Timeout),
        ),
    ] {
        let metrics: Vec<_> = snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .collect();
        assert_eq!(metrics.len(), expected, "metric {name}");
        for (key, _, _, value) in metrics {
            assert_eq!(*value, DebugValue::Counter(1), "metric {name}");
            assert!(key.key().labels().any(|label| {
                if name == "server.connection.rejected" {
                    label.key() == "reason" && label.value() == "policy"
                } else {
                    label.key() == "phase" && label.value() == stage
                }
            }));
            if name == "server.resource_limit.rejections" {
                assert!(
                    key.key().labels().any(|label| {
                        label.key() == "limit" && label.value() == "test_admission"
                    })
                );
            }
            if name == "server.errors" {
                let expected_kind = if kind == RejectionKind::Timeout {
                    "timeout"
                } else {
                    "resource_limit"
                };
                assert!(key.key().labels().any(|label| {
                    label.key() == "error_kind" && label.value() == expected_kind
                }));
            }
        }
    }
}

#[test]
fn successful_connection_policies_do_not_report_rejection() {
    let recorder = DebuggingRecorder::new();
    let observer = NacelleInMemoryObserver::new();
    let active = Arc::new(AtomicUsize::new(0));
    metrics::with_local_recorder(&recorder, || {
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                |context: TcpRequestContext<StaticProtocol>| async move {
                    context.respond(TcpResponse::bytes("ok")).await
                },
            ))
            .telemetry(NacelleTelemetry::new().with_observer(observer.clone()))
            .policies(policies(
                "none",
                Rejection::new(RejectionKind::Denied, "test_admission"),
                active.clone(),
            ))
            .build()
            .unwrap();
        let (stream, writes) = io(&[1, 0]);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(server.serve_io(stream))
            .unwrap();
        assert_eq!(*writes.lock().unwrap(), [b"ok".to_vec()]);
    });
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(
        !observer
            .events()
            .iter()
            .any(|event| { event.kind == NacelleTelemetryEventKind::ConnectionRejected })
    );
    assert!(
        !recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .iter()
            .any(|(key, _, _, _)| matches!(
                key.key().name(),
                "server.connection.rejected" | "server.errors" | "server.resource_limit.rejections"
            ))
    );
}

#[test]
fn connection_policy_rejections_report_once_with_or_without_metrics() {
    for dispatch in [
        Dispatch::Shared,
        Dispatch::Serial,
        Dispatch::Local,
        Dispatch::LocalSerial,
    ] {
        for listener in [false, true] {
            for stage in ["policy_accepted", "policy_transport_ready"] {
                if !listener && stage == "policy_accepted" {
                    continue;
                }
                for kind in [
                    RejectionKind::RateLimited,
                    RejectionKind::Overloaded,
                    RejectionKind::Denied,
                    RejectionKind::Closed,
                    RejectionKind::Timeout,
                ] {
                    for metrics_enabled in [false, true] {
                        let recorder = DebuggingRecorder::new();
                        let observer = NacelleInMemoryObserver::new();
                        let runtime = NacelleRuntimeState::default();
                        let active = Arc::new(AtomicUsize::new(0));
                        metrics::with_local_recorder(&recorder, || {
                            let telemetry = NacelleTelemetry::new()
                                .with_metrics(metrics_enabled)
                                .with_observer(observer.clone());
                            let rejection = Rejection::new(kind, "test_admission");
                            tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .unwrap()
                                .block_on(tokio::task::LocalSet::new().run_until(async {
                                    let rejection = async {
                                        if listener {
                                            reject_listener(
                                                dispatch,
                                                stage,
                                                rejection,
                                                active.clone(),
                                                telemetry,
                                                runtime.clone(),
                                            )
                                            .await;
                                        } else {
                                            reject_direct(
                                                dispatch,
                                                rejection,
                                                active.clone(),
                                                telemetry,
                                                runtime.clone(),
                                            )
                                            .await;
                                        }
                                    };
                                    tokio::time::timeout(Duration::from_secs(2), rejection)
                                        .await
                                        .expect("admission rejection must finish");
                                }));
                        });
                        assert_rejection_telemetry(
                            &observer,
                            &recorder,
                            stage,
                            kind,
                            metrics_enabled,
                        );
                        assert_eq!(active.load(Ordering::SeqCst), 0);
                        assert_eq!(runtime.active_connections(), 0);
                        assert_eq!(runtime.active_requests(), 0);
                        assert_eq!(runtime.active_streaming_tasks(), 0);
                    }
                }
            }
        }
    }
}
