use super::*;
use crate::policy::{TcpLifecyclePolicy, WithTcpPolicy};
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct PolicyState {
    events: std::sync::Mutex<Vec<&'static str>>,
    active: AtomicUsize,
    changed: Notify,
    tls_ready: AtomicBool,
}

impl PolicyState {
    async fn wait_for(&self, stage: &'static str, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|event| **event == stage)
                    .count()
                    >= count
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("policy checkpoint should be reached");
    }
}

struct Lease(Arc<PolicyState>);

impl Drop for Lease {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct RecordingPolicy {
    state: Arc<PolicyState>,
    reject: Option<&'static str>,
    gate: Option<(&'static str, Arc<Semaphore>)>,
}

impl RecordingPolicy {
    async fn enter(&self, stage: &'static str) -> Result<Option<Lease>, NacelleError> {
        self.state.events.lock().unwrap().push(stage);
        self.state.changed.notify_waiters();
        if self.reject == Some(stage) {
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Other(stage),
            ));
        }
        if let Some((gated_stage, gate)) = &self.gate
            && *gated_stage == stage
        {
            gate.acquire()
                .await
                .expect("test gate should remain open")
                .forget();
        }
        self.state.active.fetch_add(1, Ordering::SeqCst);
        Ok(Some(Lease(self.state.clone())))
    }
}

impl<Request, State> TcpLifecyclePolicy<Request, State> for RecordingPolicy {
    type Permit = Lease;

    fn accepted(
        &self,
        connection: &NacelleConnectionMeta,
    ) -> impl std::future::Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        assert_eq!(&*connection.listener, "policy-test");
        assert!(connection.tls.is_none());
        self.enter("accepted")
    }

    fn transport_ready(
        &self,
        connection: &NacelleConnectionMeta,
    ) -> impl std::future::Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        self.state
            .tls_ready
            .store(connection.tls.is_some(), Ordering::SeqCst);
        self.enter("ready")
    }

    fn before_request(
        &self,
        _connection: &ConnectionInfo,
        _state: &State,
    ) -> impl std::future::Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        self.enter("before")
    }

    fn request_head(
        &self,
        _request: &Request,
        _body_len: usize,
        _connection: &ConnectionInfo,
        _state: &State,
    ) -> impl std::future::Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        self.enter("head")
    }

    fn body_complete(
        &self,
        _connection: &NacelleConnectionMeta,
        _body_len: usize,
    ) -> impl std::future::Future<Output = Result<Option<Self::Permit>, NacelleError>> + Send {
        self.enter("body")
    }
}

type PolicyProtocol = WithTcpPolicy<PhaseProtocol, RecordingPolicy>;

struct RejectBefore {
    admitted: usize,
    calls: AtomicUsize,
    timeout: bool,
}

impl TcpLifecyclePolicy<PhaseRequest, AuthState> for RejectBefore {
    type Permit = ();

    async fn before_request(
        &self,
        _: &ConnectionInfo,
        _: &AuthState,
    ) -> Result<Option<()>, NacelleError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.admitted {
            return Ok(None);
        }
        Err(if self.timeout {
            NacelleError::Timeout(NacelleTimeoutReason::Other("test_before"))
        } else {
            NacelleError::ResourceLimit(NacelleResourceLimitReason::Other("test_before"))
        })
    }
}

#[test]
fn legacy_pre_head_rejections_record_only_the_policy_phase() {
    use metrics_util::debugging::DebugValue;

    #[derive(Clone, Copy, Debug)]
    enum Metrics {
        Default,
        ErrorOnly,
        ErrorsDisabled,
        Disabled,
    }

    type P = WithTcpPolicy<PhaseProtocol, RejectBefore>;
    for admitted in [0, 1] {
        for timeout in [false, true] {
            for mode in [
                Metrics::Default,
                Metrics::ErrorOnly,
                Metrics::ErrorsDisabled,
                Metrics::Disabled,
            ] {
                let recorder = DebuggingRecorder::new();
                let handled = Arc::new(AtomicUsize::new(0));
                let handler_calls = handled.clone();
                let runtime = NacelleRuntimeState::default();
                metrics::with_local_recorder(&recorder, || {
                    let telemetry = match mode {
                        Metrics::Default => NacelleTelemetry::new(),
                        Metrics::ErrorOnly => NacelleTelemetry::new()
                            .with_connection_metrics(false)
                            .with_runtime_metrics(false)
                            .with_request_metrics(NacelleRequestMetricsConfig {
                                started: false,
                                completed: false,
                                in_flight: false,
                                duration_ms: false,
                                byte_counts: false,
                            }),
                        Metrics::ErrorsDisabled => {
                            NacelleTelemetry::new().with_error_metrics(false)
                        }
                        Metrics::Disabled => NacelleTelemetry::new().with_metrics(false),
                    };
                    let server = TcpServer::<P>::builder()
                        .protocol(
                            PhaseProtocol {
                                authenticated: true,
                                request_wire_bytes: None,
                                encoder_writes_then_errors: false,
                            }
                            .with_policy(RejectBefore {
                                admitted,
                                calls: AtomicUsize::new(0),
                                timeout,
                            }),
                        )
                        .handler(handler_fn(move |context: TcpRequestContext<P>| {
                            handler_calls.fetch_add(1, Ordering::SeqCst);
                            async move { context.respond(TcpResponse::bytes("ok")).await }
                        }))
                        .telemetry(telemetry)
                        .runtime_state(runtime.clone())
                        .build()
                        .unwrap();
                    let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let io = RecordingIo {
                        input: vec![0, 0],
                        position: 0,
                        writes: writes.clone(),
                    };
                    let result = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(server.serve_io(io));
                    let error = result.expect_err("before-request policy must reject");
                    assert!(matches!(
                        (&error, timeout),
                        (
                            NacelleError::Timeout(NacelleTimeoutReason::Other("test_before")),
                            true
                        ) | (
                            NacelleError::ResourceLimit(NacelleResourceLimitReason::Other(
                                "test_before"
                            )),
                            false
                        )
                    ));
                    let mut expected = b"ok".repeat(admitted);
                    expected.extend_from_slice(error.to_string().as_bytes());
                    assert_eq!(writes.lock().unwrap().concat(), expected);
                });
                assert_eq!(handled.load(Ordering::SeqCst), admitted);
                assert_eq!(runtime.active_connections(), 0);
                assert_eq!(runtime.active_requests(), 0);
                let snapshot = recorder.snapshotter().snapshot().into_vec();
                let enabled = matches!(mode, Metrics::Default | Metrics::ErrorOnly);
                for (name, expected) in [
                    ("server.errors", usize::from(enabled)),
                    (
                        "server.resource_limit.rejections",
                        usize::from(enabled && !timeout),
                    ),
                ] {
                    let counters: Vec<_> = snapshot
                        .iter()
                        .filter(|(key, _, _, _)| key.key().name() == name)
                        .collect();
                    assert_eq!(
                        counters.len(),
                        expected,
                        "{name}, admitted={admitted}, timeout={timeout}, mode={mode:?}"
                    );
                    for (key, _, _, value) in counters {
                        assert_eq!(*value, DebugValue::Counter(1));
                        assert!(key.key().labels().any(|label| {
                            label.key() == "phase" && label.value() == "policy_before_request"
                        }));
                    }
                }
            }
        }
    }
}

struct WaitLater {
    body: bool,
    calls: AtomicUsize,
    gate: Arc<Semaphore>,
}

impl WaitLater {
    async fn enter(&self, body: bool) -> Result<Option<()>, NacelleError> {
        if self.body == body && self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.gate.acquire().await.unwrap().forget();
        }
        Ok(None)
    }
}

impl TcpLifecyclePolicy<PhaseRequest, AuthState> for WaitLater {
    type Permit = ();

    fn request_head(
        &self,
        _: &PhaseRequest,
        _: usize,
        _: &ConnectionInfo,
        _: &AuthState,
    ) -> impl Future<Output = Result<Option<()>, NacelleError>> + Send {
        self.enter(false)
    }

    fn body_complete(
        &self,
        _: &NacelleConnectionMeta,
        _: usize,
    ) -> impl Future<Output = Result<Option<()>, NacelleError>> + Send {
        self.enter(true)
    }
}

#[tokio::test]
async fn pending_legacy_head_and_body_hooks_flush_prior_responses() {
    type P = WithTcpPolicy<PhaseProtocol, WaitLater>;
    for body in [false, true] {
        let gate = Arc::new(Semaphore::new(0));
        let server = TcpServer::<P>::builder()
            .protocol(
                PhaseProtocol {
                    authenticated: true,
                    request_wire_bytes: None,
                    encoder_writes_then_errors: false,
                }
                .with_policy(WaitLater {
                    body,
                    calls: AtomicUsize::new(0),
                    gate: gate.clone(),
                }),
            )
            .handler(handler_fn(|context: TcpRequestContext<P>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            }))
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .build()
            .unwrap();
        let (mut client, stream) = tokio::io::duplex(128);
        let task = tokio::spawn(async move { server.serve_io(stream).await });
        client.write_all(&[0, 0]).await.unwrap();
        let mut response = [0; 2];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"ok");
        gate.add_permits(1);
        client.shutdown().await.unwrap();
        client.read_exact(&mut response).await.unwrap();
        task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn server_registration_runs_legacy_then_new_hooks_once() {
    use crate::{TcpHead, TcpPolicies, TcpTransport};
    use nacelle_core::policy::check_fn;

    let legacy = RecordingPolicy::default();
    let state = legacy.state.clone();
    let socket = state.clone();
    let accepted = state.clone();
    let ready = state.clone();
    let before = state.clone();
    let head = state.clone();
    let around = state.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, token) = NacelleShutdown::pair();
    let server = TcpServer::<PolicyProtocol>::builder()
        .protocol(protocol(legacy))
        .handler(handler_fn(
            |context: TcpRequestContext<PolicyProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .listener_label("policy-test")
        .policies(
            TcpPolicies::new()
                .on_socket(check_fn(move |_: &NacelleConnectionMeta| {
                    socket.events.lock().unwrap().push("socket");
                    Ok(())
                }))
                .on_accepted(check_fn(move |view: &TcpTransport<'_>| {
                    assert!(view.connection().tls.is_none());
                    accepted.events.lock().unwrap().push("new_accepted");
                    Ok(())
                }))
                .on_transport_ready(check_fn(move |view: &TcpTransport<'_>| {
                    assert_eq!(&*view.connection().listener, "policy-test");
                    ready.events.lock().unwrap().push("new_ready");
                    Ok(())
                }))
                .before_request(check_fn(
                    move |_: &crate::TcpConnection<'_, Arc<AuthState>>| {
                        before.events.lock().unwrap().push("new_before");
                        Ok(())
                    },
                ))
                .on_request_head(check_fn(
                    move |_: &TcpHead<'_, PhaseRequest, Arc<AuthState>>| {
                        head.events.lock().unwrap().push("new_head");
                        Ok(())
                    },
                ))
                .around_handler(check_fn(
                    move |_: &TcpHead<'_, PhaseRequest, Arc<AuthState>>| {
                        around.events.lock().unwrap().push("new_handler");
                        Ok(())
                    },
                )),
        )
        .build()
        .unwrap();
    let task = tokio::spawn(crate::runtime::serve_tcp_listener_with_shutdown_deadline(
        Arc::new(server),
        listener,
        token,
        NacelleDrainDeadline::new(Duration::from_secs(2)),
    ));
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    client.write_all(&[0]).await.unwrap();
    let mut response = [0; 2];
    client.read_exact(&mut response).await.unwrap();
    client.shutdown().await.unwrap();
    let mut remainder = Vec::new();
    client.read_to_end(&mut remainder).await.unwrap();
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(
        *state.events.lock().unwrap(),
        [
            "socket",
            "accepted",
            "new_accepted",
            "ready",
            "new_ready",
            "before",
            "new_before",
            "head",
            "new_head",
            "body",
            "new_handler",
            "before",
            "new_before",
        ]
    );
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

fn protocol(policy: RecordingPolicy) -> PolicyProtocol {
    PhaseProtocol {
        authenticated: true,
        request_wire_bytes: None,
        encoder_writes_then_errors: false,
    }
    .with_policy(policy)
}

struct ObservedIo<IO> {
    inner: IO,
    read_bytes: Arc<AtomicUsize>,
    state: Arc<PolicyState>,
    write_leases: Arc<std::sync::Mutex<Vec<usize>>>,
}

impl<IO: AsyncRead + Unpin> AsyncRead for ObservedIo<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buffer);
        self.read_bytes
            .fetch_add(buffer.filled().len() - before, Ordering::SeqCst);
        result
    }
}

impl<IO: AsyncWrite + Unpin> AsyncWrite for ObservedIo<IO> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.write_leases
            .lock()
            .unwrap()
            .push(self.state.active.load(Ordering::SeqCst));
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn fragmented_head_retains_before_request_permit_without_readmission() {
    type FragmentedProtocol = WithTcpPolicy<MixedProtocol, RecordingPolicy>;
    let policy = RecordingPolicy::default();
    let state = policy.state.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let write_leases = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (mut client, io) = tokio::io::duplex(64);
    let task = tokio::spawn(serve_stream_with_connection_meta(
        ObservedIo {
            inner: io,
            read_bytes: reads.clone(),
            state: state.clone(),
            write_leases: write_leases.clone(),
        },
        Arc::new(MixedProtocol.with_policy(policy)),
        handler_fn(
            |context: TcpRequestContext<FragmentedProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));

    client.write_all(&[2]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while reads.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the incomplete head should be read");
    assert_eq!(*state.events.lock().unwrap(), ["ready", "before"]);
    assert_eq!(state.active.load(Ordering::SeqCst), 2);
    assert!(!task.is_finished());

    client.write_all(&[0]).await.unwrap();
    let mut response = [0; 2];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"ok");
    client.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        *state.events.lock().unwrap(),
        ["ready", "before", "head", "body", "before"]
    );
    assert_eq!(*write_leases.lock().unwrap(), [4]);
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn listener_drain_deadline_cancels_policy_waits_and_releases_permits() {
    for (acquired, stage) in ["accepted", "ready", "before", "head", "body"]
        .into_iter()
        .enumerate()
    {
        let gate = Arc::new(Semaphore::new(0));
        let policy = RecordingPolicy {
            gate: Some((stage, gate.clone())),
            ..Default::default()
        };
        let state = policy.state.clone();
        let runtime_state = NacelleRuntimeState::default();
        let observer = NacelleInMemoryObserver::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, token) = NacelleShutdown::pair();
        let server = TcpServer::<PolicyProtocol>::builder()
            .protocol(protocol(policy))
            .handler(handler_fn(
                |_context: TcpRequestContext<PolicyProtocol>| async move {
                    panic!("the policy wait must prevent handler dispatch")
                },
            ))
            .listener_label("policy-test")
            .policies(
                crate::TcpPolicies::new().on_socket(nacelle_core::policy::check_fn(
                    |meta: &NacelleConnectionMeta| {
                        assert_eq!(&*meta.listener, "policy-test");
                        Ok(())
                    },
                )),
            )
            .build()
            .unwrap()
            .with_runtime_state(runtime_state.clone())
            .with_telemetry(NacelleTelemetry::new().with_observer(observer.clone()));
        let task = tokio::spawn(crate::runtime::serve_tcp_listener_with_shutdown_deadline(
            Arc::new(server),
            listener,
            token,
            NacelleDrainDeadline::new(Duration::ZERO),
        ));
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        if matches!(stage, "head" | "body") {
            client.write_all(&[0]).await.unwrap();
        }
        state.wait_for(stage, 1).await;
        assert_eq!(state.active.load(Ordering::SeqCst), acquired);
        assert_eq!(runtime_state.active_connections(), 1);

        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the listener should abort waits at the drain deadline")
            .unwrap()
            .unwrap();
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime_state.active_connections(), 0);
        assert_eq!(runtime_state.active_requests(), 0);
        for kind in [
            NacelleTelemetryEventKind::DrainTimedOut,
            NacelleTelemetryEventKind::ConnectionsAborted,
        ] {
            assert!(observer.events().iter().any(|event| event.kind == kind));
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn unix_policies_preserve_hook_order_and_release_permits_after_rejection() {
    static NEXT_SOCKET: AtomicUsize = AtomicUsize::new(0);
    for reject in [Some("accepted"), Some("ready"), Some("head"), None] {
        let socket = std::env::temp_dir().join(format!(
            "nacelle-policy-{}-{}.sock",
            std::process::id(),
            NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let policy = RecordingPolicy {
            reject,
            ..Default::default()
        };
        let state = policy.state.clone();
        let runtime_state = NacelleRuntimeState::default();
        let observer = NacelleInMemoryObserver::new();
        let (shutdown, token) = NacelleShutdown::pair();
        let server = TcpServer::<PolicyProtocol>::builder()
            .protocol(protocol(policy))
            .handler(handler_fn(
                |context: TcpRequestContext<PolicyProtocol>| async move {
                    context.respond(TcpResponse::bytes("ok")).await
                },
            ))
            .listener_label("policy-test")
            .policies(
                crate::TcpPolicies::new().on_socket(nacelle_core::policy::check_fn(
                    |meta: &NacelleConnectionMeta| {
                        assert_eq!(&*meta.listener, "policy-test");
                        Ok(())
                    },
                )),
            )
            .build()
            .unwrap()
            .with_runtime_state(runtime_state.clone())
            .with_telemetry(NacelleTelemetry::new().with_observer(observer.clone()));
        let task = tokio::spawn(crate::runtime::serve_unix_listener_with_shutdown_deadline(
            Arc::new(server),
            listener,
            Some(socket.clone()),
            token,
            NacelleDrainDeadline::new(Duration::from_secs(1)),
        ));
        let mut client = tokio::net::UnixStream::connect(&socket).await.unwrap();
        if matches!(reject, Some("head") | None) {
            client.write_all(&[0]).await.unwrap();
            client.shutdown().await.unwrap();
        }
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        let expected_response = match reject {
            Some("head") => NacelleError::ResourceLimit(NacelleResourceLimitReason::Other("head"))
                .to_string()
                .into_bytes(),
            Some(_) => Vec::new(),
            None => b"ok".to_vec(),
        };
        assert_eq!(response, expected_response);
        shutdown.shutdown();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        std::fs::remove_file(socket).unwrap();
        let expected_events: &[&str] = match reject {
            Some("accepted") => &["accepted"],
            Some("ready") => &["accepted", "ready"],
            Some("head") => &["accepted", "ready", "before", "head"],
            _ => &["accepted", "ready", "before", "head", "body", "before"],
        };
        assert_eq!(*state.events.lock().unwrap(), expected_events);
        assert!(!state.tls_ready.load(Ordering::SeqCst));
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime_state.active_connections(), 0);
        assert_eq!(runtime_state.active_requests(), 0);
        let rejected: Vec<_> = observer
            .events()
            .into_iter()
            .filter(|event| event.kind == NacelleTelemetryEventKind::ConnectionRejected)
            .collect();
        assert_eq!(
            rejected.len(),
            usize::from(matches!(reject, Some("accepted" | "ready")))
        );
        for event in rejected {
            assert_eq!(event.reason, Some("policy"));
            assert_eq!(event.count, 1);
        }
    }
}

#[cfg(feature = "openssl")]
#[tokio::test]
async fn openssl_policies_cover_handshake_order_plaintext_detection_and_cleanup() {
    #[cfg(not(feature = "experimental-openssl-detection"))]
    let modes = ["required"];
    #[cfg(feature = "experimental-openssl-detection")]
    let modes = ["required", "optional_tls", "optional_plain"];

    for mode in modes {
        let config = crate::runtime::test_open_ssl_config();
        for reject in [Some("accepted"), Some("ready"), None] {
            let policy = RecordingPolicy {
                reject,
                ..Default::default()
            };
            let state = policy.state.clone();
            let runtime_state = NacelleRuntimeState::default();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, token) = NacelleShutdown::pair();
            let server =
                Arc::new(
                    TcpServer::<PolicyProtocol>::builder()
                        .protocol(protocol(policy))
                        .handler(handler_fn(
                            |context: TcpRequestContext<PolicyProtocol>| async move {
                                context.respond(TcpResponse::bytes("ok")).await
                            },
                        ))
                        .listener_label("policy-test")
                        .policies(crate::TcpPolicies::new().on_socket(
                            nacelle_core::policy::check_fn(|meta: &NacelleConnectionMeta| {
                                assert_eq!(&*meta.listener, "policy-test");
                                Ok(())
                            }),
                        ))
                        .build()
                        .unwrap()
                        .with_runtime_state(runtime_state.clone()),
                );
            let config = config.clone();
            let task = tokio::spawn(async move {
                #[cfg(feature = "experimental-openssl-detection")]
                if mode != "required" {
                    return crate::runtime::serve_tcp_optional_openssl_listener_with_options_and_shutdown_deadline(
                        server,
                        listener,
                        config,
                        crate::NacelleTcpOptions::default(),
                        crate::NacelleTlsDetectionOptions::default(),
                        token,
                        NacelleDrainDeadline::new(Duration::ZERO),
                    )
                    .await;
                }
                crate::runtime::serve_tcp_openssl_listener_with_shutdown_deadline(
                    server,
                    listener,
                    config,
                    token,
                    NacelleDrainDeadline::new(Duration::ZERO),
                )
                .await
            });
            let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
            if reject == Some("accepted") {
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(2), client.read(&mut [0]))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
            } else if mode == "optional_plain" {
                client.write_all(&[0]).await.unwrap();
                state.wait_for("ready", 1).await;
                if reject.is_none() {
                    let mut response = [0; 2];
                    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(&response, b"ok");
                }
            } else {
                let mut connector =
                    openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
                connector.set_verify(openssl::ssl::SslVerifyMode::NONE);
                let ssl = connector
                    .build()
                    .configure()
                    .unwrap()
                    .verify_hostname(false)
                    .into_ssl("localhost")
                    .unwrap();
                let mut client = tokio_openssl::SslStream::new(ssl, client).unwrap();
                tokio::time::timeout(Duration::from_secs(2), Pin::new(&mut client).connect())
                    .await
                    .unwrap()
                    .unwrap();
                state.wait_for("ready", 1).await;
                if reject.is_none() {
                    client.write_all(&[0]).await.unwrap();
                    let mut response = [0; 2];
                    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(&response, b"ok");
                }
            }
            if reject.is_none() {
                state.wait_for("before", 2).await;
            }
            shutdown.shutdown();
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let expected_events: &[&str] = match reject {
                Some("accepted") => &["accepted"],
                Some(_) => &["accepted", "ready"],
                None => &["accepted", "ready", "before", "head", "body", "before"],
            };
            assert_eq!(*state.events.lock().unwrap(), expected_events);
            assert_eq!(
                state.tls_ready.load(Ordering::SeqCst),
                reject != Some("accepted") && mode != "optional_plain"
            );
            assert_eq!(state.active.load(Ordering::SeqCst), 0);
            assert_eq!(runtime_state.active_connections(), 0);
            assert_eq!(runtime_state.active_requests(), 0);
        }
    }
}

#[tokio::test]
async fn lifecycle_order_and_permits_cover_coalesced_response_delivery() {
    let policy = RecordingPolicy::default();
    let state = policy.state.clone();
    let write_leases = Arc::new(std::sync::Mutex::new(Vec::new()));
    let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let io = ObservedIo {
        inner: RecordingIo {
            input: vec![2, b'a', b'b', 0],
            position: 0,
            writes: writes.clone(),
        },
        read_bytes: Arc::new(AtomicUsize::new(0)),
        state: state.clone(),
        write_leases: write_leases.clone(),
    };
    serve_stream_with_connection_meta(
        io,
        Arc::new(protocol(policy)),
        handler_fn(
            |mut context: TcpRequestContext<PolicyProtocol>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    chunk?;
                }
                context.respond(TcpResponse::bytes("ok")).await
            },
        ),
        NacelleTcpConfig::default()
            .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await
    .expect("connection should complete");
    assert_eq!(
        *state.events.lock().unwrap(),
        [
            "ready", "before", "head", "body", "before", "head", "body", "before"
        ]
    );
    assert_eq!(*write_leases.lock().unwrap(), [4, 4]);
    assert_eq!(*writes.lock().unwrap(), [b"ok".to_vec(), b"ok".to_vec()]);
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

struct SelectivePolicy {
    state: Arc<PolicyState>,
    connection_only: bool,
}

impl TcpLifecyclePolicy<PhaseRequest, AuthState> for SelectivePolicy {
    type Permit = Lease;

    async fn transport_ready(
        &self,
        _connection: &NacelleConnectionMeta,
    ) -> Result<Option<Lease>, NacelleError> {
        Ok(self.connection_only.then(|| {
            self.state.active.fetch_add(1, Ordering::SeqCst);
            Lease(self.state.clone())
        }))
    }

    async fn before_request(
        &self,
        _connection: &ConnectionInfo,
        _state: &AuthState,
    ) -> Result<Option<Lease>, NacelleError> {
        Ok((!self.connection_only).then(|| {
            self.state.active.fetch_add(1, Ordering::SeqCst);
            Lease(self.state.clone())
        }))
    }
}

#[tokio::test]
async fn only_acquired_request_permits_require_delivery_barriers() {
    type SelectiveProtocol = WithTcpPolicy<PhaseProtocol, SelectivePolicy>;
    for connection_only in [false, true] {
        let state = Arc::new(PolicyState::default());
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let write_leases = Arc::new(std::sync::Mutex::new(Vec::new()));
        let protocol = PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        }
        .with_policy(SelectivePolicy {
            state: state.clone(),
            connection_only,
        });
        serve_stream_with_connection_meta(
            ObservedIo {
                inner: RecordingIo {
                    input: vec![0, 0],
                    position: 0,
                    writes: writes.clone(),
                },
                read_bytes: Default::default(),
                state: state.clone(),
                write_leases: write_leases.clone(),
            },
            Arc::new(protocol),
            handler_fn(|context: TcpRequestContext<SelectiveProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            }),
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            NacelleTelemetry::default(),
            NacelleRuntimeState::default(),
            NacelleConnectionMeta::tcp(None, None),
        )
        .await
        .unwrap();
        assert_eq!(
            *writes.lock().unwrap(),
            if connection_only {
                vec![b"okok".to_vec()]
            } else {
                vec![b"ok".to_vec(), b"ok".to_vec()]
            },
        );
        assert!(
            write_leases
                .lock()
                .unwrap()
                .iter()
                .all(|leases| *leases == 1)
        );
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn response_encoding_failure_releases_all_policy_permits() {
    let policy = RecordingPolicy::default();
    let state = policy.state.clone();
    let protocol = PhaseProtocol {
        authenticated: true,
        request_wire_bytes: None,
        encoder_writes_then_errors: true,
    }
    .with_policy(policy);
    let result = serve_stream_with_connection_meta(
        RecordingIo {
            input: vec![0],
            position: 0,
            writes: Default::default(),
        },
        Arc::new(protocol),
        handler_fn(|context: TcpRequestContext<PolicyProtocol>| async move {
            context.respond(TcpResponse::bytes("ok")).await
        }),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    )
    .await;
    assert!(matches!(
        result,
        Err(NacelleError::InvalidFrame("test response encoder"))
    ));
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lifecycle_rejection_uses_existing_error_delivery_and_releases_permits() {
    for stage in ["ready", "before", "head", "body"] {
        let policy = RecordingPolicy {
            reject: Some(stage),
            ..Default::default()
        };
        let state = policy.state.clone();
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let result = serve_stream_with_connection_meta(
            RecordingIo {
                input: vec![0],
                position: 0,
                writes: writes.clone(),
            },
            Arc::new(protocol(policy)),
            handler_fn({
                let calls = calls.clone();
                move |context: TcpRequestContext<PolicyProtocol>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(context.complete()) }
                }
            }),
            NacelleTcpConfig::default(),
            NacelleTelemetry::default(),
            NacelleRuntimeState::default(),
            NacelleConnectionMeta::tcp(None, None),
        )
        .await;
        assert!(matches!(result, Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::Other(reason)
        )) if reason == stage));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(writes.lock().unwrap().is_empty(), stage == "ready");
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn policy_wait_blocks_reads_and_cancellation_releases_connection_permit() {
    let gate = Arc::new(Semaphore::new(0));
    let policy = RecordingPolicy {
        gate: Some(("before", gate)),
        ..Default::default()
    };
    let state = policy.state.clone();
    let reads = Arc::new(AtomicUsize::new(0));
    let (mut client, server) = tokio::io::duplex(64);
    client.write_all(&[1, b'a']).await.unwrap();
    let task = tokio::spawn(serve_stream_with_connection_meta(
        ObservedIo {
            inner: server,
            read_bytes: reads.clone(),
            state: state.clone(),
            write_leases: Default::default(),
        },
        Arc::new(protocol(policy)),
        handler_fn(
            |context: TcpRequestContext<PolicyProtocol>| async move { Ok(context.complete()) },
        ),
        NacelleTcpConfig::default(),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));
    state.wait_for("before", 1).await;
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(state.active.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn head_policy_wait_prevents_further_body_reads_in_both_modes() {
    for mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
        let gate = Arc::new(Semaphore::new(0));
        let policy = RecordingPolicy {
            gate: Some(("head", gate.clone())),
            ..Default::default()
        };
        let state = policy.state.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let (mut client, server) = tokio::io::duplex(64);
        let task = tokio::spawn(serve_stream_with_connection_meta(
            ObservedIo {
                inner: server,
                read_bytes: reads.clone(),
                state: state.clone(),
                write_leases: Default::default(),
            },
            Arc::new(protocol(policy)),
            handler_fn(
                |mut context: TcpRequestContext<PolicyProtocol>| async move {
                    let mut body = Vec::new();
                    while let Some(chunk) = context.request_mut().body.next_chunk().await {
                        body.extend_from_slice(&chunk?);
                    }
                    assert_eq!(body, b"abcd");
                    context.respond(TcpResponse::bytes("ok")).await
                },
            ),
            NacelleTcpConfig::default().with_request_body_mode(mode),
            NacelleTelemetry::default(),
            NacelleRuntimeState::default(),
            NacelleConnectionMeta::tcp(None, None),
        ));
        client.write_all(&[4]).await.unwrap();
        state.wait_for("head", 1).await;
        client.write_all(b"abcd").await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        gate.add_permits(1);
        let mut response = [0; 2];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"ok");
        client.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(
            state
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|s| **s == "body")
                .count(),
            1
        );
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
    }
}

struct WaitForSecondRequest {
    calls: AtomicUsize,
    entered: Arc<Notify>,
    gate: Arc<Semaphore>,
}

impl TcpLifecyclePolicy<PhaseRequest, AuthState> for WaitForSecondRequest {
    type Permit = ();

    async fn before_request(
        &self,
        _connection: &ConnectionInfo,
        _state: &AuthState,
    ) -> Result<Option<()>, NacelleError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
        }
        Ok(None)
    }
}

#[tokio::test]
async fn waiting_for_pipelined_admission_flushes_previous_response() {
    let entered = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    let policy = WaitForSecondRequest {
        calls: AtomicUsize::new(0),
        entered: entered.clone(),
        gate: gate.clone(),
    };
    type GatedProtocol = WithTcpPolicy<PhaseProtocol, WaitForSecondRequest>;
    let protocol = PhaseProtocol {
        authenticated: true,
        request_wire_bytes: None,
        encoder_writes_then_errors: false,
    }
    .with_policy(policy);
    let (mut client, server) = tokio::io::duplex(64);
    client.write_all(&[0, 0]).await.unwrap();
    let task = tokio::spawn(serve_stream_with_connection_meta(
        server,
        Arc::new(protocol),
        handler_fn(|context: TcpRequestContext<GatedProtocol>| async move {
            context.respond(TcpResponse::bytes("ok")).await
        }),
        NacelleTcpConfig::default()
            .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    let mut response = [0; 2];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response, b"ok");
    gate.add_permits(1);
    client.read_exact(&mut response).await.unwrap();
    client.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn accepted_policy_rejects_without_entering_transport_or_request_pipeline() {
    let policy = RecordingPolicy {
        reject: Some("accepted"),
        ..Default::default()
    };
    let state = policy.state.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, token) = NacelleShutdown::pair();
    let server = TcpServer::<PolicyProtocol>::builder()
        .protocol(protocol(policy))
        .handler(handler_fn(
            |context: TcpRequestContext<PolicyProtocol>| async move { Ok(context.complete()) },
        ))
        .listener_label("policy-test")
        .build()
        .unwrap();
    let task = tokio::spawn(crate::runtime::serve_tcp_listener_with_shutdown_deadline(
        Arc::new(server),
        listener,
        token,
        NacelleDrainDeadline::new(Duration::from_secs(1)),
    ));
    let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
    state.wait_for("accepted", 1).await;
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    shutdown.shutdown();
    task.await.unwrap().unwrap();
    assert_eq!(*state.events.lock().unwrap(), ["accepted"]);
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_streaming_body_does_not_visit_body_complete() {
    let policy = RecordingPolicy::default();
    let state = policy.state.clone();
    let (mut client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(serve_stream_with_connection_meta(
        server,
        Arc::new(protocol(policy)),
        handler_fn(
            |mut context: TcpRequestContext<PolicyProtocol>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    chunk?;
                }
                Ok(context.complete())
            },
        ),
        NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Streaming),
        NacelleTelemetry::default(),
        NacelleRuntimeState::default(),
        NacelleConnectionMeta::tcp(None, None),
    ));
    client.write_all(&[4]).await.unwrap();
    state.wait_for("head", 1).await;
    client.shutdown().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
    assert_eq!(*state.events.lock().unwrap(), ["ready", "before", "head"]);
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
}

struct LocalPolicyHandler;

impl LocalSerialTcpHandler<WithTcpPolicy<LocalOnlyProtocol, RecordingPolicy>>
    for LocalPolicyHandler
{
    async fn call<'connection>(
        &'connection self,
        context: SerialTcpRequestContext<
            'connection,
            WithTcpPolicy<LocalOnlyProtocol, RecordingPolicy>,
        >,
    ) -> Result<
        crate::protocol::TcpHandlerCompletion<WithTcpPolicy<LocalOnlyProtocol, RecordingPolicy>>,
        NacelleError,
    > {
        let state = context.connection().state.clone();
        let count = state.get();
        tokio::task::yield_now().await;
        state.set(count + 1);
        context.respond(TcpResponse::bytes(count.to_string())).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn policies_preserve_worker_local_non_send_serial_state() {
    let policy = RecordingPolicy::default();
    let state = policy.state.clone();
    let server =
        LocalSerialTcpServer::new(LocalOnlyProtocol.with_policy(policy), LocalPolicyHandler);
    let (mut client, io) = tokio::io::duplex(64);
    let send = async {
        client.write_all(&[0, 0]).await.unwrap();
        let mut response = [0; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"01");
        client.shutdown().await.unwrap();
    };
    let (result, ()) = tokio::join!(server.serve_io(io), send);
    result.unwrap();
    assert_eq!(state.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        state
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|s| **s == "head")
            .count(),
        2
    );
}

#[cfg(feature = "tls-self-signed")]
#[tokio::test]
async fn tls_reload_during_accepted_admission_applies_to_the_handshake() {
    let original = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).unwrap();
    let replacement = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).unwrap();
    let certificate = crate::runtime::test_certificate(replacement.certificate_pem.as_bytes());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));
    let gate = Arc::new(Semaphore::new(0));
    let policy = RecordingPolicy {
        gate: Some(("accepted", gate.clone())),
        ..Default::default()
    };
    let state = policy.state.clone();
    let server = TcpServer::<PolicyProtocol>::builder()
        .protocol(protocol(policy))
        .handler(handler_fn(
            |context: TcpRequestContext<PolicyProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .listener_label("policy-test")
        .build()
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = original.tls_config.clone();
    let (shutdown, token) = NacelleShutdown::pair();
    let task = tokio::spawn(
        crate::runtime::serve_tcp_tls_listener_with_shutdown_deadline(
            Arc::new(server),
            listener,
            original.tls_config,
            token,
            NacelleDrainDeadline::new(Duration::ZERO),
        ),
    );
    let stream = tokio::net::TcpStream::connect(address).await.unwrap();
    state.wait_for("accepted", 1).await;
    config.replace_server_config_arc(replacement.tls_config.server_config());
    gate.add_permits(1);
    let mut client = connector
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            stream,
        )
        .await
        .expect("handshake must trust the replacement certificate");
    client.write_all(&[0]).await.unwrap();
    let mut response = [0; 2];
    client.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"ok");
    shutdown.shutdown();
    task.await.unwrap().unwrap();
}

#[cfg(feature = "tls-self-signed")]
#[tokio::test]
async fn tls_policies_run_before_handshake_and_after_negotiated_metadata() {
    for reject in ["accepted", "ready"] {
        let generated = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).unwrap();
        let certificate = crate::runtime::test_certificate(generated.certificate_pem.as_bytes());
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));
        let policy = RecordingPolicy {
            reject: Some(reject),
            ..Default::default()
        };
        let state = policy.state.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, token) = NacelleShutdown::pair();
        let server = TcpServer::<PolicyProtocol>::builder()
            .protocol(protocol(policy))
            .handler(handler_fn(
                |context: TcpRequestContext<PolicyProtocol>| async move { Ok(context.complete()) },
            ))
            .listener_label("policy-test")
            .build()
            .unwrap();
        let task = tokio::spawn(
            crate::runtime::serve_tcp_tls_listener_with_shutdown_deadline(
                Arc::new(server),
                listener,
                generated.tls_config,
                token,
                NacelleDrainDeadline::new(Duration::from_secs(1)),
            ),
        );
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        if reject == "accepted" {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), client.read(&mut [0]))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            assert!(!state.tls_ready.load(Ordering::SeqCst));
        } else {
            let _tls = tokio::time::timeout(
                Duration::from_secs(2),
                connector.connect("localhost".try_into().unwrap(), client),
            )
            .await
            .unwrap()
            .unwrap();
            state.wait_for("ready", 1).await;
            assert!(state.tls_ready.load(Ordering::SeqCst));
        }
        shutdown.shutdown();
        task.await.unwrap().unwrap();
        assert_eq!(state.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            *state.events.lock().unwrap(),
            if reject == "accepted" {
                vec!["accepted"]
            } else {
                vec!["accepted", "ready"]
            }
        );
    }
}
