use super::*;
use std::cell::Cell;
use std::marker::PhantomData;

use crate::policies::{LocalTcpPolicySet, SharedTcpPolicySet, TcpHead, TcpPolicies};
use crate::server::LocalTcpServer;
use nacelle_core::policy::{
    Admission, Cancellation, Hold, LocalAdmission, Rejection, RejectionKind, bounded_wait,
    check_fn, local_check_fn,
};
use tokio::sync::Semaphore;

mod telemetry;

struct StaticProtocol<State = ()>(PhantomData<fn() -> State>);
struct StaticDecoder;

struct AsyncHead;

impl<'view, Request: Send, Storage: Send> Admission<TcpHead<'view, Request, Storage>>
    for AsyncHead
{
    type Lease = ();

    async fn enter<'a>(
        &'a self,
        input: &'a mut TcpHead<'view, Request, Storage>,
    ) -> Result<(), Rejection> {
        tokio::task::yield_now().await;
        assert_eq!(input.body_len(), 0);
        Ok(())
    }
}

impl MessageDecoder for StaticDecoder {
    type Message = DecodedRequest<Cell<u8>>;
    type Error = NacelleError;

    fn decode(&mut self, input: &mut BytesMut) -> Result<Option<Self::Message>, Self::Error> {
        if input.len() < 2 {
            return Ok(None);
        }
        let bytes = input.split_to(2);
        Ok(Some(DecodedRequest {
            request: Cell::new(bytes[0]),
            body_len: usize::from(bytes[1]),
        }))
    }
}

impl<State: Default + 'static> Protocol for StaticProtocol<State> {
    type Request = Cell<u8>;
    type Response = TcpResponse;
    type ConnectionState = State;
    type Decoder = StaticDecoder;
    type ResponseContext = ();
    type ErrorContext = ();

    fn decoder(&self, _: usize) -> StaticDecoder {
        StaticDecoder
    }
    fn connection_state(&self, _: &ConnectionInfo) -> State {
        State::default()
    }
    fn request_wire_bytes(&self, _: &Cell<u8>, body_len: usize) -> usize {
        2 + body_len
    }
    fn response_context(&self, _: &Cell<u8>) {}
    fn error_context(&self, _: &Cell<u8>) {}
    fn apply_response(&self, _: &mut (), _: &TcpResponse) {}
    fn max_response_frame_overhead(&self) -> usize {
        0
    }
    fn response_body(&self, response: TcpResponse) -> NacelleBody {
        response.body
    }
    fn encode_response_chunk(
        &self,
        _: &mut (),
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        dst.extend_from_slice(&chunk)
    }
    fn encode_response_terminal_chunk(
        &self,
        context: &mut (),
        chunk: Bytes,
        dst: &mut FrameBuffer<'_>,
    ) -> Result<(), NacelleError> {
        self.encode_response_chunk(context, chunk, dst)
    }
    fn encode_response_end(&self, _: &mut (), _: &mut FrameBuffer<'_>) -> Result<(), NacelleError> {
        Ok(())
    }
}

pub(super) fn io(input: &[u8]) -> (RecordingIo, Arc<std::sync::Mutex<Vec<Vec<u8>>>>) {
    let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
    (
        RecordingIo {
            input: input.to_vec(),
            position: 0,
            writes: writes.clone(),
        },
        writes,
    )
}

#[tokio::test]
async fn additive_predicates_preserve_non_sync_heads_and_handler_types() {
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let first = events.clone();
    let second = events.clone();
    let handler_events = events.clone();
    let policies = TcpPolicies::new()
        .on_request_head(check_fn(move |head: &TcpHead<'_, Cell<u8>, Arc<()>>| {
            first.lock().unwrap().push((1, head.request().get()));
            Ok(())
        }))
        .on_request_head(check_fn(move |head: &TcpHead<'_, Cell<u8>, Arc<()>>| {
            second.lock().unwrap().push((2, head.request().get()));
            Ok(())
        }))
        .on_request_head(AsyncHead);
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            move |context: TcpRequestContext<StaticProtocol>| {
                let events = handler_events.clone();
                async move {
                    events
                        .lock()
                        .unwrap()
                        .push((3, context.request().head.get()));
                    context.respond(TcpResponse::bytes("ok")).await
                }
            },
        ))
        .policies(policies)
        .build()
        .unwrap();
    let (stream, writes) = io(&[7, 0, 9, 0]);
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    task.await.unwrap().unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        [(1, 7), (2, 7), (3, 7), (1, 9), (2, 9), (3, 9)]
    );
    assert_eq!(*writes.lock().unwrap(), [b"ok".to_vec(), b"ok".to_vec()]);
}

fn shared_bootstrap() -> impl SharedTcpPolicySet<Cell<u8>, Arc<()>> {
    TcpPolicies::new().on_request_head(AsyncHead)
}

fn local_bootstrap() -> impl LocalTcpPolicySet<PhaseRequest, Rc<Cell<usize>>> {
    TcpPolicies::new().on_request_head(LocalAsyncHead)
}

#[tokio::test]
async fn opaque_policy_factories_attach_to_builders_and_servers() {
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |context: TcpRequestContext<StaticProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .policies(shared_bootstrap())
        .build()
        .unwrap();
    drop(server.serve_tcp("127.0.0.1:0".parse().unwrap()));
    drop(crate::runtime::serve_tcp::<_, _, _>(
        Arc::new(server.clone()),
        "127.0.0.1:0".parse().unwrap(),
    ));
    server.serve_io(io(&[1, 0]).0).await.unwrap();

    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |context: TcpRequestContext<StaticProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .build()
        .unwrap()
        .with_policies(shared_bootstrap());
    drop(server.serve_tcp("127.0.0.1:0".parse().unwrap()));
    server.serve_io(io(&[1, 0]).0).await.unwrap();

    let local = LocalSerialTcpServer::new(LocalOnlyProtocol, LocalOnlyHandler)
        .with_policies(local_bootstrap());
    local.serve_io(io(&[0]).0).await.unwrap();
}

struct SerialCellHandler;

impl SerialTcpHandler<StaticProtocol<Cell<usize>>> for SerialCellHandler {
    async fn call<'a>(
        &'a self,
        context: SerialTcpRequestContext<'a, StaticProtocol<Cell<usize>>>,
    ) -> Result<crate::protocol::TcpHandlerCompletion<StaticProtocol<Cell<usize>>>, NacelleError>
    {
        let count = context.connection().state.get();
        context.connection().state.set(count + 1);
        tokio::task::yield_now().await;
        context.respond(TcpResponse::bytes("ok")).await
    }
}

#[tokio::test]
async fn serial_policy_composition_does_not_require_sync_state() {
    let server = SerialTcpServer::new(
        StaticProtocol(PhantomData::<fn() -> Cell<usize>>),
        SerialCellHandler,
    )
    .with_policies(
        TcpPolicies::new()
            .before_request(check_fn(|view: &crate::TcpConnection<'_, Cell<usize>>| {
                assert!(view.state().get() <= 2);
                Ok(())
            }))
            .on_request_head(check_fn(|head: &TcpHead<'_, Cell<u8>, Cell<usize>>| {
                assert_eq!(usize::from(head.request().get()), head.state().get() + 1);
                Ok(())
            }))
            .on_request_head(check_fn(|head: &TcpHead<'_, Cell<u8>, Cell<usize>>| {
                assert_eq!(head.body_len(), 0);
                Ok(())
            }))
            .on_request_head(AsyncHead),
    );
    let (stream, _) = io(&[1, 0, 2, 0]);
    tokio::spawn(async move { server.serve_io(stream).await })
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn local_policy_composition_retains_rc_connection_and_policy_state() {
    let seen = Rc::new(Cell::new(0));
    let counter = seen.clone();
    let server = LocalSerialTcpServer::new(LocalOnlyProtocol, LocalOnlyHandler).with_policies(
        TcpPolicies::new()
            .on_request_head(local_check_fn(
                move |head: &TcpHead<'_, PhaseRequest, Rc<Cell<usize>>>| {
                    assert_eq!(head.state().get(), counter.get());
                    counter.set(counter.get() + 1);
                    Ok(())
                },
            ))
            .on_request_head(local_check_fn(
                |head: &TcpHead<'_, PhaseRequest, Rc<Cell<usize>>>| {
                    assert_eq!(head.body_len(), 0);
                    Ok(())
                },
            ))
            .on_request_head(LocalAsyncHead),
    );
    let (stream, writes) = io(&[0, 0]);
    server.serve_io(stream).await.unwrap();
    assert_eq!(seen.get(), 2);
    assert_eq!(*writes.lock().unwrap(), [b"0".to_vec(), b"1".to_vec()]);

    let local = LocalTcpServer::new(
        LocalOnlyProtocol,
        local_handler_fn(|context: TcpRequestContext<LocalOnlyProtocol>| async move {
            context.respond(TcpResponse::bytes("ok")).await
        }),
    )
    .with_policies(TcpPolicies::new().on_request_head(local_check_fn(
        |head: &TcpHead<'_, PhaseRequest, Arc<Rc<Cell<usize>>>>| {
            assert_eq!(head.state().get(), 0);
            Ok(())
        },
    )));
    let (stream, _) = io(&[0]);
    local.serve_io(stream).await.unwrap();
}

struct LocalAsyncHead;

impl<'view> LocalAdmission<TcpHead<'view, PhaseRequest, Rc<Cell<usize>>>> for LocalAsyncHead {
    type Lease = ();

    #[allow(clippy::future_not_send)]
    async fn enter<'a>(
        &'a self,
        input: &'a mut TcpHead<'view, PhaseRequest, Rc<Cell<usize>>>,
    ) -> Result<(), Rejection> {
        tokio::task::yield_now().await;
        assert!(input.state().get() < 2);
        Ok(())
    }
}

struct ActiveLease(Arc<AtomicUsize>);

impl Drop for ActiveLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct ResourcePolicy(Arc<AtomicUsize>);

impl<I: ?Sized> Admission<I> for ResourcePolicy {
    type Lease = Hold<ActiveLease>;

    fn enter<'a>(
        &'a self,
        _: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Hold::new(ActiveLease(self.0.clone()))))
    }
}

#[tokio::test]
async fn decision_only_checkpoints_preserve_batching_but_resources_flush() {
    let active = Arc::new(AtomicUsize::new(0));
    let decision = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |context: TcpRequestContext<StaticProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .policies(
            TcpPolicies::new()
                .before_request(check_fn(|_: &crate::TcpConnection<'_, Arc<()>>| Ok(())))
                .on_request_head(check_fn(|_: &TcpHead<'_, Cell<u8>, Arc<()>>| Ok(())))
                .around_handler(check_fn(|_: &TcpHead<'_, Cell<u8>, Arc<()>>| Ok(()))),
        )
        .build()
        .unwrap();
    let (stream, writes) = io(&[1, 0, 2, 0]);
    decision.serve_io(stream).await.unwrap();
    assert_eq!(*writes.lock().unwrap(), [b"okok".to_vec()]);

    let resource = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |context: TcpRequestContext<StaticProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .policies(TcpPolicies::new().on_request_head(ResourcePolicy(active.clone())))
        .build()
        .unwrap();
    let (stream, writes) = io(&[1, 0, 2, 0]);
    resource.serve_io(stream).await.unwrap();
    assert_eq!(*writes.lock().unwrap(), [b"ok".to_vec(), b"ok".to_vec()]);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[derive(Clone)]
struct WaitingPolicy {
    entries: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

impl<I: ?Sized> Admission<I> for WaitingPolicy {
    type Lease = Hold<tokio::sync::OwnedSemaphorePermit>;

    fn enter<'a>(
        &'a self,
        _: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        self.entries.fetch_add(1, Ordering::SeqCst);
        async {
            self.gate
                .clone()
                .acquire_owned()
                .await
                .map(Hold::new)
                .map_err(|_| Rejection::new(RejectionKind::Closed, "test_gate"))
        }
    }
}

async fn wait_for(counter: &AtomicUsize, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::SeqCst) != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_interrupts_wait_once_and_releases_connection_leases() {
    let observer = NacelleInMemoryObserver::new();
    let active = Arc::new(AtomicUsize::new(0));
    let entries = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let cancellation = Cancellation::new();
    let runtime = NacelleRuntimeState::default();
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |_: TcpRequestContext<StaticProtocol>| async move {
                panic!("waiting policy prevents handler")
            },
        ))
        .runtime_state(runtime.clone())
        .telemetry(NacelleTelemetry::new().with_observer(observer.clone()))
        .policies(
            TcpPolicies::new()
                .on_transport_ready(ResourcePolicy(active.clone()))
                .on_request_head(ResourcePolicy(active.clone()))
                .on_request_head(bounded_wait(
                    WaitingPolicy {
                        entries: entries.clone(),
                        gate: gate.clone(),
                    },
                    Duration::from_secs(30),
                    1,
                ))
                .with_cancellation(cancellation.clone()),
        )
        .build()
        .unwrap();
    let (stream, _) = io(&[1, 0]);
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    wait_for(&entries, 1).await;
    assert_eq!(active.load(Ordering::SeqCst), 2);
    cancellation.cancel();
    assert!(matches!(
        task.await.unwrap(),
        Err(NacelleError::ResourceLimit(_))
    ));
    assert_eq!(entries.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.active_connections(), 0);
    assert_eq!(runtime.active_requests(), 0);
    let closed = observer
        .events()
        .into_iter()
        .filter(|event| event.kind == NacelleTelemetryEventKind::ConnectionClosed)
        .collect::<Vec<_>>();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].reason, Some("cancelled"));
    gate.add_permits(1);
    assert_eq!(gate.available_permits(), 1);
}

#[tokio::test]
async fn fragmented_head_retains_new_before_admission() {
    let entries = Arc::new(AtomicUsize::new(0));
    let before = entries.clone();
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |context: TcpRequestContext<StaticProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .policies(TcpPolicies::new().before_request(check_fn(
            move |_: &crate::TcpConnection<'_, Arc<()>>| {
                before.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )))
        .build()
        .unwrap();
    let (mut client, stream) = tokio::io::duplex(64);
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    client.write_all(&[1]).await.unwrap();
    wait_for(&entries, 1).await;
    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(entries.load(Ordering::SeqCst), 1);
    client.write_all(&[0]).await.unwrap();
    let mut response = [0; 2];
    client.read_exact(&mut response).await.unwrap();
    client.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(entries.load(Ordering::SeqCst), 2);
}

#[derive(Clone)]
struct WaitOnSecond {
    enabled: bool,
    entries: Arc<AtomicUsize>,
    gate: Arc<Semaphore>,
}

impl<I: ?Sized + Send> Admission<I> for WaitOnSecond {
    type Lease = ();

    async fn enter<'a>(&'a self, _: &'a mut I) -> Result<(), Rejection> {
        if self.enabled && self.entries.fetch_add(1, Ordering::SeqCst) == 1 {
            self.gate.acquire().await.unwrap().forget();
        }
        Ok(())
    }
}

#[tokio::test]
async fn pending_admission_flushes_prior_response_without_restarting() {
    for checkpoint in ["before", "head", "handler"] {
        let entries = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let policy = |stage| WaitOnSecond {
            enabled: checkpoint == stage,
            entries: entries.clone(),
            gate: gate.clone(),
        };
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                |context: TcpRequestContext<StaticProtocol>| async move {
                    context.respond(TcpResponse::bytes("ok")).await
                },
            ))
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .policies(
                TcpPolicies::new()
                    .before_request(policy("before"))
                    .on_request_head(policy("head"))
                    .around_handler(policy("handler")),
            )
            .build()
            .unwrap();
        let (mut client, stream) = tokio::io::duplex(64);
        let task = tokio::spawn(async move { server.serve_io(stream).await });
        client.write_all(&[1, 0, 2, 0]).await.unwrap();
        wait_for(&entries, 2).await;
        let mut response = [0; 2];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&response, b"ok");
        assert_eq!(entries.load(Ordering::SeqCst), 2);
        gate.add_permits(1);
        client.read_exact(&mut response).await.unwrap();
        client.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        assert_eq!(
            entries.load(Ordering::SeqCst),
            if checkpoint == "before" { 3 } else { 2 }
        );
    }
}

#[tokio::test]
async fn handler_resource_ends_before_response_delivery_and_keeps_batching() {
    let active = Arc::new(AtomicUsize::new(0));
    let handler_active = active.clone();
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            move |context: TcpRequestContext<StaticProtocol>| {
                let active = handler_active.clone();
                async move {
                    assert_eq!(active.load(Ordering::SeqCst), 1);
                    context
                        .respond(TcpResponse::new(NacelleBody::bytes("ok")))
                        .await
                }
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .policies(TcpPolicies::new().around_handler(ResourcePolicy(active.clone())))
        .build()
        .unwrap();
    let (stream, writes) = io(&[1, 0, 2, 0]);
    server.serve_io(stream).await.unwrap();
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(*writes.lock().unwrap(), [b"okok".to_vec()]);
}

#[tokio::test(start_paused = true)]
async fn policy_timeout_releases_framework_budgets_and_pending_wait() {
    let entries = Arc::new(AtomicUsize::new(0));
    let runtime = NacelleRuntimeState::default();
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |_: TcpRequestContext<StaticProtocol>| async move {
                panic!("waiting policy prevents handler")
            },
        ))
        .runtime_state(runtime.clone())
        .policies(TcpPolicies::new().on_request_head(bounded_wait(
            WaitingPolicy {
                entries: entries.clone(),
                gate: Arc::new(Semaphore::new(0)),
            },
            Duration::from_secs(5),
            1,
        )))
        .build()
        .unwrap();
    let (stream, _) = io(&[1, 0]);
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    wait_for(&entries, 1).await;
    assert_eq!(runtime.active_connections(), 1);
    assert_eq!(runtime.active_requests(), 1);
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(matches!(task.await.unwrap(), Err(NacelleError::Timeout(_))));
    assert_eq!(runtime.active_connections(), 0);
    assert_eq!(runtime.active_requests(), 0);
}

struct FailPolicyFlushIo {
    reader: RecordingIo,
    writes: Arc<AtomicUsize>,
}

impl AsyncRead for FailPolicyFlushIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for FailPolicyFlushIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "test write failure",
        )))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn failed_flush_before_policy_wait_does_not_attempt_an_error_response() {
    let writes = Arc::new(AtomicUsize::new(0));
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn(
            |context: TcpRequestContext<PhaseProtocol>| async move {
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .policies(TcpPolicies::new().on_request_head(WaitOnSecond {
            enabled: true,
            entries: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Semaphore::new(0)),
        }))
        .build()
        .unwrap();
    let (reader, _) = io(&[0, 0]);
    let result = server
        .serve_io(FailPolicyFlushIo {
            reader,
            writes: writes.clone(),
        })
        .await;
    assert!(matches!(result, Err(NacelleError::Io(_))));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

struct PendingWrites<IO>(IO);

async fn assert_full_streaming_channel_body_failure(body_timeout: bool) {
    let active = Arc::new(AtomicUsize::new(0));
    let entries = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let runtime = NacelleRuntimeState::default();
    let server = TcpServer::<StaticProtocol>::builder()
        .protocol(StaticProtocol(PhantomData))
        .handler(handler_fn(
            |_: TcpRequestContext<StaticProtocol>| async move {
                panic!("waiting policy prevents handler")
            },
        ))
        .runtime_state(runtime.clone())
        .tcp_limits(
            crate::limits::NacelleTcpLimits::default().with_read_timeout(Duration::from_secs(1)),
        )
        .tcp_config(
            NacelleTcpConfig::default()
                .with_request_body_mode(TcpRequestBodyMode::Streaming)
                .with_request_body_chunk_size(1)
                .with_request_body_channel_capacity(1),
        )
        .policies(
            TcpPolicies::new()
                .on_transport_ready(ResourcePolicy(active.clone()))
                .on_request_head(ResourcePolicy(active.clone()))
                .around_handler(ResourcePolicy(active.clone()))
                .around_handler(WaitingPolicy {
                    entries: entries.clone(),
                    gate: gate.clone(),
                }),
        )
        .build()
        .unwrap();
    let (mut client, stream) = tokio::io::duplex(16);
    client.write_all(&[1, 2, b'x']).await.unwrap();
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    wait_for(&entries, 1).await;
    assert_eq!(active.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.active_connections(), 1);
    assert_eq!(runtime.active_requests(), 1);
    assert_eq!(runtime.active_streaming_tasks(), 1);

    let started = tokio::time::Instant::now();
    if !body_timeout {
        client.shutdown().await.unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("body failure must cancel admission despite a full body channel")
        .expect("connection task");
    if body_timeout {
        assert!(matches!(
            result,
            Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
        ));
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    } else {
        assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
        assert_eq!(started.elapsed(), Duration::ZERO);
    }
    assert_eq!(entries.load(Ordering::SeqCst), 1);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.active_connections(), 0);
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
    gate.add_permits(1);
    assert_eq!(gate.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn streaming_body_eof_cancels_admission_with_a_full_body_channel() {
    assert_full_streaming_channel_body_failure(false).await;
}

#[tokio::test(start_paused = true)]
async fn streaming_body_timeout_cancels_admission_with_a_full_body_channel() {
    assert_full_streaming_channel_body_failure(true).await;
}

struct StreamingBodyFailureIo {
    headers_read: bool,
    body_failed: bool,
    body_timeout: bool,
    body_waker: Option<std::task::Waker>,
    output: Arc<std::sync::Mutex<Vec<u8>>>,
    flushes: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
}

impl AsyncRead for StreamingBodyFailureIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.headers_read {
            buf.put_slice(&[1, 0, 2, 1]);
            self.headers_read = true;
            return Poll::Ready(Ok(()));
        }
        if self.output.lock().unwrap().is_empty() {
            self.body_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if self.body_timeout {
            return Poll::Pending;
        }
        self.body_failed = true;
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for StreamingBodyFailureIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let first = self.output.lock().unwrap().is_empty();
        if !first && !self.body_failed {
            return Poll::Pending;
        }
        let written = if first {
            bytes.len().min(2)
        } else {
            bytes.len()
        };
        self.output
            .lock()
            .unwrap()
            .extend_from_slice(&bytes[..written]);
        if let Some(waker) = self.body_waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn streaming_body_failure_does_not_replay_a_partially_written_response() {
    for (waiting, body_timeout) in [(true, false), (true, true), (false, false), (false, true)] {
        let active = Arc::new(AtomicUsize::new(0));
        let output = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let handlers = Arc::new(AtomicUsize::new(0));
        let called = handlers.clone();
        let runtime = NacelleRuntimeState::new(NacelleLimits::default().without_handler_timeout());
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                move |context: TcpRequestContext<StaticProtocol>| {
                    called.fetch_add(1, Ordering::SeqCst);
                    async move { context.respond(TcpResponse::bytes("prev")).await }
                },
            ))
            .runtime_state(runtime.clone())
            .tcp_limits(
                crate::limits::NacelleTcpLimits::default()
                    .with_read_timeout(Duration::from_secs(1))
                    .without_write_timeout()
                    .without_shutdown_timeout(),
            )
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_request_body_mode(TcpRequestBodyMode::Streaming)
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .policies(
                TcpPolicies::new()
                    .on_transport_ready(ResourcePolicy(active.clone()))
                    .around_handler(WaitOnSecond {
                        enabled: waiting,
                        entries: Arc::new(AtomicUsize::new(0)),
                        gate: Arc::new(Semaphore::new(0)),
                    }),
            )
            .build()
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            server.serve_io(StreamingBodyFailureIo {
                headers_read: false,
                body_failed: false,
                body_timeout,
                body_waker: None,
                output: output.clone(),
                flushes: flushes.clone(),
                shutdowns: shutdowns.clone(),
            }),
        )
        .await
        .expect("body failure must cancel pending delivery and finish the connection");
        if body_timeout {
            assert!(matches!(
                result,
                Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
            ));
        } else {
            assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
        }
        assert_eq!(&*output.lock().unwrap(), b"pr");
        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
        assert_eq!(handlers.load(Ordering::SeqCst), if waiting { 1 } else { 2 });
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.active_requests(), 0);
    }
}

struct YieldingAdmission(bool);

impl<I: ?Sized + Send> Admission<I> for YieldingAdmission {
    type Lease = ();

    async fn enter<'a>(&'a self, _: &'a mut I) -> Result<(), Rejection> {
        if self.0 {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn streaming_body_and_response_delivery_make_full_duplex_progress() {
    for (write_policy, write_capacity, attached, waiting) in [
        (ResponseWritePolicy::CoalesceBuffered, 0, false, false),
        (ResponseWritePolicy::CoalesceBuffered, 0, true, false),
        (ResponseWritePolicy::CoalesceBuffered, 0, true, true),
        (ResponseWritePolicy::Immediate, 32, false, false),
        (ResponseWritePolicy::Immediate, 32, true, false),
        (ResponseWritePolicy::Immediate, 32, true, true),
        (ResponseWritePolicy::CoalesceBuffered, 32, true, true),
        (ResponseWritePolicy::FlushAtBytes(16), 32, true, true),
    ] {
        let active = Arc::new(AtomicUsize::new(0));
        let handler_active = active.clone();
        let handled = Arc::new(AtomicUsize::new(0));
        let handler_done = handled.clone();
        let runtime = NacelleRuntimeState::new(NacelleLimits::default().without_handler_timeout());
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                move |mut context: TcpRequestContext<StaticProtocol>| {
                    let active = handler_active.clone();
                    let done = handler_done.clone();
                    async move {
                        if context.request().head.get() == 1 {
                            return context
                                .respond(TcpResponse::bytes(Bytes::from_static(&[b'p'; 16])))
                                .await;
                        }
                        let mut received = 0;
                        while let Some(chunk) = context.request_mut().body.next_chunk().await {
                            received += chunk?.len();
                        }
                        assert_eq!(received, 64);
                        assert_eq!(active.load(Ordering::SeqCst), if attached { 2 } else { 0 });
                        done.store(1, Ordering::SeqCst);
                        context.respond(TcpResponse::bytes("ok")).await
                    }
                },
            ))
            .runtime_state(runtime.clone())
            .tcp_limits(crate::limits::NacelleTcpLimits::default().without_write_timeout())
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_request_body_mode(TcpRequestBodyMode::Streaming)
                    .with_request_body_chunk_size(1)
                    .with_request_body_channel_capacity(1)
                    .with_response_write_policy(write_policy),
            )
            .build()
            .unwrap();
        let (mut client, stream) = tokio::io::duplex(8);
        let stream = tokio::io::BufStream::with_capacity(8, write_capacity, stream);
        client.write_all(&[1, 0, 2, 64]).await.unwrap();
        let policy_active = active.clone();
        let task = tokio::spawn(async move {
            if attached {
                server
                    .with_policies(
                        TcpPolicies::new()
                            .on_transport_ready(ResourcePolicy(policy_active.clone()))
                            .around_handler(YieldingAdmission(waiting))
                            .around_handler(ResourcePolicy(policy_active)),
                    )
                    .serve_io(stream)
                    .await
            } else {
                server.serve_io(stream).await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            client.write_all(&[b'x'; 64]).await.unwrap();
            wait_for(&handled, 1).await;
            assert_eq!(active.load(Ordering::SeqCst), if attached { 1 } else { 0 });
            client.shutdown().await.unwrap();
            let mut responses = Vec::new();
            client.read_to_end(&mut responses).await.unwrap();
            let mut expected = vec![b'p'; 16];
            expected.extend_from_slice(b"ok");
            assert_eq!(responses, expected);
            task.await.unwrap().unwrap();
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "response delivery blocked body consumption: {write_policy:?}, \
                 write_capacity={write_capacity}, attached={attached}, waiting={waiting}"
            )
        });
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.active_requests(), 0);
    }
}

struct PendingFlushIo {
    reader: RecordingIo,
    shutdowns: Arc<AtomicUsize>,
}

struct YieldOnSecond {
    enabled: bool,
    entries: Arc<AtomicUsize>,
}

impl<I: ?Sized + Send> Admission<I> for YieldOnSecond {
    type Lease = ();

    async fn enter<'a>(&'a self, _: &'a mut I) -> Result<(), Rejection> {
        let second = self.entries.fetch_add(1, Ordering::SeqCst) == 1;
        if self.enabled && second {
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

struct CheckpointCase {
    body_mode: TcpRequestBodyMode,
    write_policy: ResponseWritePolicy,
    write_capacity: usize,
    fragmented_head: bool,
}

async fn assert_checkpoint_full_duplex<P, Policies>(
    protocol: P,
    policies: Policies,
    case: CheckpointCase,
    before_entries: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
) where
    P: crate::protocol::SharedProtocol<
            Request = Cell<u8>,
            Response = TcpResponse,
            ConnectionState = (),
            ResponseContext = (),
        >,
    Policies: SharedTcpPolicySet<Cell<u8>, Arc<()>> + Send + Sync,
{
    let CheckpointCase {
        body_mode,
        write_policy,
        write_capacity,
        fragmented_head,
    } = case;
    let handler_active = active.clone();
    let handled = Arc::new(AtomicUsize::new(0));
    let handler_done = handled.clone();
    let runtime = NacelleRuntimeState::new(NacelleLimits::default().without_handler_timeout());
    let server = TcpServer::<P>::builder()
        .protocol(protocol)
        .handler(handler_fn(move |mut context: TcpRequestContext<P>| {
            let active = handler_active.clone();
            let done = handler_done.clone();
            async move {
                if context.request().head.get() == 1 {
                    return context
                        .respond(TcpResponse::bytes(Bytes::from_static(&[b'p'; 16])))
                        .await;
                }
                let mut received = 0;
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    received += chunk?.len();
                }
                assert_eq!(received, 64);
                assert_eq!(active.load(Ordering::SeqCst), 2);
                done.store(1, Ordering::SeqCst);
                context.respond(TcpResponse::bytes("ok")).await
            }
        }))
        .runtime_state(runtime.clone())
        .tcp_limits(crate::limits::NacelleTcpLimits::default().without_write_timeout())
        .tcp_config(
            NacelleTcpConfig::default()
                .with_request_body_mode(body_mode)
                .with_request_body_chunk_size(1)
                .with_request_body_channel_capacity(1)
                .with_response_write_policy(write_policy),
        )
        .policies(policies)
        .build()
        .unwrap();
    let (mut client, stream) = tokio::io::duplex(8);
    let stream = tokio::io::BufStream::with_capacity(8, write_capacity, stream);
    let heads = if fragmented_head {
        &b"\x01\x00\x02"[..]
    } else {
        &b"\x01\x00\x02\x40"[..]
    };
    client.write_all(heads).await.unwrap();
    let task = tokio::spawn(async move { server.serve_io(stream).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        if fragmented_head {
            wait_for(&before_entries, 2).await;
            client.write_all(&[64]).await.unwrap();
        }
        client.write_all(&[b'x'; 64]).await.unwrap();
        wait_for(&handled, 1).await;
        client.shutdown().await.unwrap();
        let mut responses = Vec::new();
        client.read_to_end(&mut responses).await.unwrap();
        let mut expected = vec![b'p'; 16];
        expected.extend_from_slice(b"ok");
        assert_eq!(responses, expected);
        task.await.unwrap().unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "admitted checkpoint blocked body input: {body_mode:?}, {write_policy:?}, \
             write_capacity={write_capacity}, fragmented_head={fragmented_head}"
        )
    });
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.active_connections(), 0);
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
    assert_eq!(before_entries.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn successful_static_checkpoints_preserve_full_duplex_progress() {
    for checkpoint in ["before", "head"] {
        for body_mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
            for (write_policy, write_capacity, fragmented_head) in [
                (ResponseWritePolicy::CoalesceBuffered, 0, false),
                (ResponseWritePolicy::Immediate, 32, false),
                (ResponseWritePolicy::CoalesceBuffered, 0, true),
                (ResponseWritePolicy::Immediate, 32, true),
            ] {
                if fragmented_head && checkpoint != "before" {
                    continue;
                }
                let active = Arc::new(AtomicUsize::new(0));
                let before_entries = Arc::new(AtomicUsize::new(0));
                let policy = |stage| YieldOnSecond {
                    enabled: checkpoint == stage,
                    entries: if stage == "before" {
                        before_entries.clone()
                    } else {
                        Arc::new(AtomicUsize::new(0))
                    },
                };
                assert_checkpoint_full_duplex(
                    StaticProtocol(PhantomData),
                    TcpPolicies::new()
                        .on_transport_ready(ResourcePolicy(active.clone()))
                        .before_request(policy("before"))
                        .on_request_head(policy("head"))
                        .around_handler(ResourcePolicy(active.clone())),
                    CheckpointCase {
                        body_mode,
                        write_policy,
                        write_capacity,
                        fragmented_head,
                    },
                    before_entries,
                    active,
                )
                .await;
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn successful_checkpoint_then_body_failure_does_not_restart_delivery() {
    for checkpoint in ["before", "head"] {
        for body_mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
            for body_timeout in [false, true] {
                let active = Arc::new(AtomicUsize::new(0));
                let output = Arc::new(std::sync::Mutex::new(Vec::new()));
                let flushes = Arc::new(AtomicUsize::new(0));
                let shutdowns = Arc::new(AtomicUsize::new(0));
                let runtime = NacelleRuntimeState::default();
                let policy = |stage| YieldOnSecond {
                    enabled: checkpoint == stage,
                    entries: Arc::new(AtomicUsize::new(0)),
                };
                let server = TcpServer::<StaticProtocol>::builder()
                    .protocol(StaticProtocol(PhantomData))
                    .handler(handler_fn(
                        |context: TcpRequestContext<StaticProtocol>| async move {
                            context.respond(TcpResponse::bytes("prev")).await
                        },
                    ))
                    .runtime_state(runtime.clone())
                    .tcp_limits(
                        crate::limits::NacelleTcpLimits::default()
                            .with_read_timeout(Duration::from_secs(1))
                            .without_write_timeout()
                            .without_shutdown_timeout(),
                    )
                    .tcp_config(
                        NacelleTcpConfig::default()
                            .with_request_body_mode(body_mode)
                            .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                    )
                    .policies(
                        TcpPolicies::new()
                            .on_transport_ready(ResourcePolicy(active.clone()))
                            .before_request(policy("before"))
                            .on_request_head(policy("head")),
                    )
                    .build()
                    .unwrap();
                let result = tokio::time::timeout(
                    Duration::from_secs(2),
                    server.serve_io(StreamingBodyFailureIo {
                        headers_read: false,
                        body_failed: false,
                        body_timeout,
                        body_waker: None,
                        output: output.clone(),
                        flushes: flushes.clone(),
                        shutdowns: shutdowns.clone(),
                    }),
                )
                .await
                .expect("body failure must terminate the saved delivery operation");
                if body_timeout {
                    assert!(matches!(
                        result,
                        Err(NacelleError::Timeout(NacelleTimeoutReason::RequestBodyRead))
                    ));
                } else {
                    assert!(matches!(result, Err(NacelleError::UnexpectedEof)));
                }
                assert_eq!(&*output.lock().unwrap(), b"pr");
                assert_eq!(flushes.load(Ordering::SeqCst), 0);
                assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
                assert_eq!(active.load(Ordering::SeqCst), 0);
                assert_eq!(runtime.active_connections(), 0);
                assert_eq!(runtime.active_requests(), 0);
                assert_eq!(runtime.active_streaming_tasks(), 0);
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn successful_pre_head_wait_keeps_read_deadline_and_skips_interrupted_cleanup() {
    struct BlockedIo(Arc<AtomicUsize>);

    impl AsyncRead for BlockedIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for BlockedIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    let writes_or_shutdowns = Arc::new(AtomicUsize::new(0));
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn(
            |_: TcpRequestContext<PhaseProtocol>| async move {
                panic!("a silent peer must not reach the handler")
            },
        ))
        .tcp_limits(
            crate::limits::NacelleTcpLimits::default()
                .with_idle_timeout(Duration::from_secs(1))
                .without_write_timeout()
                .without_shutdown_timeout(),
        )
        .policies(TcpPolicies::new().before_request(YieldingAdmission(true)))
        .build()
        .unwrap();
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        server.serve_io(BlockedIo(writes_or_shutdowns.clone())),
    )
    .await
    .expect("a saved flush must not block the configured read deadline");
    assert!(matches!(
        result,
        Err(NacelleError::Timeout(NacelleTimeoutReason::Idle))
    ));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(writes_or_shutdowns.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn successful_pre_head_wait_then_decode_failure_skips_interrupted_cleanup() {
    let shutdowns = Arc::new(AtomicUsize::new(0));
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn(
            |context: TcpRequestContext<PhaseProtocol>| async move {
                context.respond(TcpResponse::bytes("prev")).await
            },
        ))
        .tcp_limits(
            crate::limits::NacelleTcpLimits::default()
                .without_write_timeout()
                .without_shutdown_timeout(),
        )
        .tcp_config(
            NacelleTcpConfig::default()
                .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
        )
        .policies(TcpPolicies::new().before_request(YieldOnSecond {
            enabled: true,
            entries: Arc::new(AtomicUsize::new(0)),
        }))
        .build()
        .unwrap();
    let (reader, writes) = io(&[0, u8::MAX]);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        server.serve_io(PendingFlushIo {
            reader,
            shutdowns: shutdowns.clone(),
        }),
    )
    .await
    .expect("decode failure must terminate the saved delivery operation");
    assert!(matches!(result, Err(NacelleError::InvalidFrame(_))));
    assert_eq!(*writes.lock().unwrap(), [b"prev".to_vec()]);
    assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "policies")]
struct YieldingLegacyCheckpoint {
    before: YieldOnSecond,
    head: YieldOnSecond,
    body: YieldOnSecond,
}

#[cfg(feature = "policies")]
impl crate::policy::TcpLifecyclePolicy<Cell<u8>, ()> for YieldingLegacyCheckpoint {
    type Permit = ();

    async fn before_request(&self, _: &ConnectionInfo, _: &()) -> Result<Option<()>, NacelleError> {
        Admission::enter(&self.before, &mut ())
            .await
            .map_err(Rejection::into_error)?;
        Ok(None)
    }

    fn request_head(
        &self,
        _: &Cell<u8>,
        _: usize,
        _: &ConnectionInfo,
        _: &(),
    ) -> impl Future<Output = Result<Option<()>, NacelleError>> + Send {
        let admission = &self.head;
        async move {
            Admission::enter(admission, &mut ())
                .await
                .map_err(Rejection::into_error)?;
            Ok(None)
        }
    }

    async fn body_complete(
        &self,
        _: &NacelleConnectionMeta,
        _: usize,
    ) -> Result<Option<()>, NacelleError> {
        Admission::enter(&self.body, &mut ())
            .await
            .map_err(Rejection::into_error)?;
        Ok(None)
    }
}

#[cfg(feature = "policies")]
struct RejectDuringLegacyFlush {
    buffered: bool,
    entries: AtomicUsize,
}

#[cfg(feature = "policies")]
impl crate::policy::TcpLifecyclePolicy<Cell<u8>, ()> for RejectDuringLegacyFlush {
    type Permit = ();

    async fn before_request(&self, _: &ConnectionInfo, _: &()) -> Result<Option<()>, NacelleError> {
        if self.entries.fetch_add(1, Ordering::SeqCst) == usize::from(self.buffered) {
            tokio::time::sleep(Duration::from_secs(1)).await;
            return Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::Other("test_rejection"),
            ));
        }
        Ok(None)
    }
}

#[cfg(feature = "policies")]
fn assert_single_operation_phase(recorder: &DebuggingRecorder, phase: &str) {
    let errors: Vec<_> = recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, _)| key.key().name() == "server.errors")
        .collect();
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0]
            .0
            .key()
            .labels()
            .any(|label| { label.key() == "phase" && label.value() == phase })
    );
    assert_eq!(errors[0].3, metrics_util::debugging::DebugValue::Counter(1));
}

#[cfg(feature = "policies")]
#[test]
fn legacy_rejection_during_blocked_flush_keeps_policy_error_origin() {
    type P = crate::policy::WithTcpPolicy<StaticProtocol, RejectDuringLegacyFlush>;
    for buffered in [false, true] {
        let recorder = DebuggingRecorder::new();
        let shutdowns = Arc::new(AtomicUsize::new(0));
        metrics::with_local_recorder(&recorder, || {
            let server = TcpServer::<P>::builder()
                .protocol(
                    StaticProtocol(PhantomData).with_policy(RejectDuringLegacyFlush {
                        buffered,
                        entries: AtomicUsize::new(0),
                    }),
                )
                .handler(handler_fn(|context: TcpRequestContext<P>| async move {
                    context.respond(TcpResponse::bytes("prev")).await
                }))
                .tcp_limits(
                    crate::limits::NacelleTcpLimits::default()
                        .without_write_timeout()
                        .without_shutdown_timeout(),
                )
                .tcp_config(
                    NacelleTcpConfig::default()
                        .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                )
                .build()
                .unwrap();
            let (reader, writes) = io(&[1, 0, 2, 0]);
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        server.serve_io(PendingFlushIo {
                            reader,
                            shutdowns: shutdowns.clone(),
                        }),
                    )
                    .await
                });
            assert!(matches!(result, Ok(Err(NacelleError::ResourceLimit(_)))));
            assert_eq!(writes.lock().unwrap().len(), usize::from(buffered));
        });
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
        assert_single_operation_phase(&recorder, "policy_before_request");
    }
}

#[cfg(feature = "policies")]
#[test]
fn buffered_legacy_delivery_failure_is_not_a_policy_error() {
    type P = crate::policy::WithTcpPolicy<StaticProtocol, YieldingLegacyCheckpoint>;
    let recorder = DebuggingRecorder::new();
    metrics::with_local_recorder(&recorder, || {
        let policy = |enabled| YieldOnSecond {
            enabled,
            entries: Arc::new(AtomicUsize::new(0)),
        };
        let server = TcpServer::<P>::builder()
            .protocol(
                StaticProtocol(PhantomData).with_policy(YieldingLegacyCheckpoint {
                    before: policy(true),
                    head: policy(false),
                    body: policy(false),
                }),
            )
            .handler(handler_fn(|context: TcpRequestContext<P>| async move {
                context.respond(TcpResponse::bytes("prev")).await
            }))
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .build()
            .unwrap();
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(server.serve_io(DeliveryFailureIo {
                reader: io(&[1, 0, 2, 0]).0,
                remaining: 2,
                fail_flush: false,
            }));
        assert!(matches!(result, Err(NacelleError::Io(_))));
    });
    assert_single_operation_phase(&recorder, "socket_write");
}

#[cfg(feature = "policies")]
#[tokio::test(start_paused = true)]
async fn successful_legacy_checkpoints_preserve_full_duplex_progress() {
    for checkpoint in ["before", "head", "body"] {
        for body_mode in [TcpRequestBodyMode::Buffered, TcpRequestBodyMode::Streaming] {
            for (write_policy, write_capacity, fragmented_head) in [
                (ResponseWritePolicy::CoalesceBuffered, 0, false),
                (ResponseWritePolicy::Immediate, 32, false),
                (ResponseWritePolicy::CoalesceBuffered, 0, true),
                (ResponseWritePolicy::Immediate, 32, true),
            ] {
                if fragmented_head && checkpoint != "before" {
                    continue;
                }
                let active = Arc::new(AtomicUsize::new(0));
                let before_entries = Arc::new(AtomicUsize::new(0));
                let policy = |stage| YieldOnSecond {
                    enabled: checkpoint == stage,
                    entries: if stage == "before" {
                        before_entries.clone()
                    } else {
                        Arc::new(AtomicUsize::new(0))
                    },
                };
                assert_checkpoint_full_duplex(
                    StaticProtocol::<()>(PhantomData).with_policy(YieldingLegacyCheckpoint {
                        before: policy("before"),
                        head: policy("head"),
                        body: policy("body"),
                    }),
                    TcpPolicies::new()
                        .on_transport_ready(ResourcePolicy(active.clone()))
                        .around_handler(ResourcePolicy(active.clone())),
                    CheckpointCase {
                        body_mode,
                        write_policy,
                        write_capacity,
                        fragmented_head,
                    },
                    before_entries,
                    active,
                )
                .await;
            }
        }
    }
}

impl AsyncRead for PendingFlushIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for PendingFlushIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.reader).poll_write(cx, bytes)
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.shutdowns.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn interrupted_admission_flush_skips_connection_cleanup() {
    for checkpoint in ["before", "head", "handler"] {
        let active = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let runtime = NacelleRuntimeState::default();
        let policy = |stage| {
            bounded_wait(
                WaitOnSecond {
                    enabled: checkpoint == stage,
                    entries: Arc::new(AtomicUsize::new(0)),
                    gate: Arc::new(Semaphore::new(0)),
                },
                Duration::from_secs(1),
                1,
            )
        };
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                |context: TcpRequestContext<StaticProtocol>| async move {
                    context.respond(TcpResponse::bytes("ok")).await
                },
            ))
            .runtime_state(runtime.clone())
            .tcp_limits(
                crate::limits::NacelleTcpLimits::default()
                    .without_write_timeout()
                    .without_shutdown_timeout(),
            )
            .tcp_config(
                NacelleTcpConfig::default()
                    .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
            )
            .policies(
                TcpPolicies::new()
                    .on_transport_ready(ResourcePolicy(active.clone()))
                    .before_request(policy("before"))
                    .on_request_head(policy("head"))
                    .around_handler(policy("handler")),
            )
            .build()
            .unwrap();
        let (reader, writes) = io(&[1, 0, 2, 0]);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            server.serve_io(PendingFlushIo {
                reader,
                shutdowns: shutdowns.clone(),
            }),
        )
        .await
        .expect("admission timeout must not restart a blocked flush during cleanup");
        assert!(matches!(result, Err(NacelleError::Timeout(_))));
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(*writes.lock().unwrap(), [b"ok".to_vec()]);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.active_requests(), 0);
    }
}

pub(super) struct DeliveryFailureIo {
    pub(super) reader: RecordingIo,
    pub(super) remaining: usize,
    pub(super) fail_flush: bool,
}

impl AsyncRead for DeliveryFailureIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for DeliveryFailureIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.remaining == 0 {
            return Poll::Ready(Err(std::io::Error::other("test write failure")));
        }
        let written = bytes.len().min(self.remaining);
        self.remaining -= written;
        Pin::new(&mut self.reader).poll_write(cx, &bytes[..written])
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.fail_flush {
            Poll::Ready(Err(std::io::Error::other("test flush failure")))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

pub(super) fn assert_error_response_size(recorder: &DebuggingRecorder, expected: f64) {
    let (_, _, _, value) = recorder
        .snapshotter()
        .snapshot()
        .into_vec()
        .into_iter()
        .find(|(key, _, _, _)| {
            key.key().name() == "server.response.body.size"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "status" && label.value() == "error")
        })
        .expect("failed request must record its response size");
    assert_eq!(
        value,
        metrics_util::debugging::DebugValue::Histogram(vec![expected.into()])
    );
}

#[test]
fn policy_barrier_failure_counts_current_response_bytes() {
    for (write_policy, remaining, fail_flush, expected) in [
        (ResponseWritePolicy::Immediate, usize::MAX, true, 4.0),
        (ResponseWritePolicy::CoalesceBuffered, 2, false, 2.0),
        (ResponseWritePolicy::CoalesceBuffered, usize::MAX, true, 4.0),
    ] {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let server = TcpServer::<StaticProtocol>::builder()
                .protocol(StaticProtocol(PhantomData))
                .handler(handler_fn(
                    |context: TcpRequestContext<StaticProtocol>| async move {
                        context.respond(TcpResponse::bytes("next")).await
                    },
                ))
                .tcp_config(NacelleTcpConfig::default().with_response_write_policy(write_policy))
                .policies(
                    TcpPolicies::new()
                        .on_request_head(ResourcePolicy(Arc::new(AtomicUsize::new(0)))),
                )
                .build()
                .unwrap();
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server.serve_io(DeliveryFailureIo {
                    reader: io(&[1, 0]).0,
                    remaining,
                    fail_flush,
                }));
            assert!(matches!(result, Err(NacelleError::Io(_))));
        });
        assert_error_response_size(&recorder, expected);
    }
}

#[test]
fn policy_wait_flush_failure_excludes_previous_response_bytes() {
    for checkpoint in ["head", "handler"] {
        let recorder = DebuggingRecorder::new();
        let handlers = Arc::new(AtomicUsize::new(0));
        metrics::with_local_recorder(&recorder, || {
            let called = handlers.clone();
            let policy = |stage| WaitOnSecond {
                enabled: checkpoint == stage,
                entries: Arc::new(AtomicUsize::new(0)),
                gate: Arc::new(Semaphore::new(0)),
            };
            let server = TcpServer::<PhaseProtocol>::builder()
                .protocol(PhaseProtocol {
                    authenticated: true,
                    request_wire_bytes: None,
                    encoder_writes_then_errors: false,
                })
                .handler(handler_fn(
                    move |context: TcpRequestContext<PhaseProtocol>| {
                        called.fetch_add(1, Ordering::SeqCst);
                        async move { context.respond(TcpResponse::bytes("prev")).await }
                    },
                ))
                .tcp_config(
                    NacelleTcpConfig::default()
                        .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                )
                .policies(
                    TcpPolicies::new()
                        .on_request_head(policy("head"))
                        .around_handler(policy("handler")),
                )
                .build()
                .unwrap();
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server.serve_io(DeliveryFailureIo {
                    reader: io(&[0, 0]).0,
                    remaining: 2,
                    fail_flush: false,
                }));
            assert!(matches!(result, Err(NacelleError::Io(_))));
        });
        assert_eq!(handlers.load(Ordering::SeqCst), 1);
        assert_error_response_size(&recorder, 0.0);
    }
}

#[test]
fn rejection_flush_failure_without_error_response_excludes_previous_bytes() {
    for checkpoint in ["head", "handler"] {
        for (remaining, fail_flush) in [(2, false), (usize::MAX, true)] {
            let recorder = DebuggingRecorder::new();
            let handlers = Arc::new(AtomicUsize::new(0));
            metrics::with_local_recorder(&recorder, || {
                let called = handlers.clone();
                let policy = |stage| {
                    check_fn(move |head: &TcpHead<'_, Cell<u8>, Arc<()>>| {
                        if checkpoint == stage && head.request().get() == 2 {
                            Err(Rejection::new(RejectionKind::Denied, "test_rejection"))
                        } else {
                            Ok(())
                        }
                    })
                };
                let server = TcpServer::<StaticProtocol>::builder()
                    .protocol(StaticProtocol(PhantomData))
                    .handler(handler_fn(
                        move |context: TcpRequestContext<StaticProtocol>| {
                            called.fetch_add(1, Ordering::SeqCst);
                            async move { context.respond(TcpResponse::bytes("prev")).await }
                        },
                    ))
                    .tcp_config(
                        NacelleTcpConfig::default()
                            .with_response_write_policy(ResponseWritePolicy::CoalesceBuffered),
                    )
                    .policies(
                        TcpPolicies::new()
                            .on_request_head(policy("head"))
                            .around_handler(policy("handler")),
                    )
                    .build()
                    .unwrap();
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(server.serve_io(DeliveryFailureIo {
                        reader: io(&[1, 0, 2, 0]).0,
                        remaining,
                        fail_flush,
                    }));
                assert!(matches!(result, Err(NacelleError::Io(_))));
            });
            assert_eq!(handlers.load(Ordering::SeqCst), 1);
            assert_error_response_size(&recorder, 0.0);
        }
    }
}

#[test]
fn rejection_flush_failure_with_error_response_counts_current_bytes() {
    let response_bytes = Rejection::new(RejectionKind::Denied, "test_rejection")
        .into_error()
        .to_string()
        .len() as f64;
    for checkpoint in ["head", "handler"] {
        for (write_policy, remaining, fail_flush, expected) in [
            (ResponseWritePolicy::CoalesceBuffered, 2, false, 0.0),
            (ResponseWritePolicy::CoalesceBuffered, 6, false, 2.0),
            (
                ResponseWritePolicy::CoalesceBuffered,
                usize::MAX,
                true,
                response_bytes,
            ),
            (
                ResponseWritePolicy::Immediate,
                usize::MAX,
                true,
                response_bytes,
            ),
        ] {
            let recorder = DebuggingRecorder::new();
            let handlers = Arc::new(AtomicUsize::new(0));
            metrics::with_local_recorder(&recorder, || {
                let called = handlers.clone();
                let policy = |stage| {
                    let entries = AtomicUsize::new(0);
                    check_fn(move |_: &TcpHead<'_, PhaseRequest, Arc<AuthState>>| {
                        if checkpoint == stage && entries.fetch_add(1, Ordering::SeqCst) == 1 {
                            Err(Rejection::new(RejectionKind::Denied, "test_rejection"))
                        } else {
                            Ok(())
                        }
                    })
                };
                let server = TcpServer::<PhaseProtocol>::builder()
                    .protocol(PhaseProtocol {
                        authenticated: true,
                        request_wire_bytes: None,
                        encoder_writes_then_errors: false,
                    })
                    .handler(handler_fn(
                        move |context: TcpRequestContext<PhaseProtocol>| {
                            called.fetch_add(1, Ordering::SeqCst);
                            async move { context.respond(TcpResponse::bytes("prev")).await }
                        },
                    ))
                    .tcp_config(
                        NacelleTcpConfig::default().with_response_write_policy(write_policy),
                    )
                    .policies(
                        TcpPolicies::new()
                            .on_request_head(policy("head"))
                            .around_handler(policy("handler")),
                    )
                    .build()
                    .unwrap();
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(server.serve_io(DeliveryFailureIo {
                        reader: io(&[0, 0]).0,
                        remaining,
                        fail_flush,
                    }));
                assert!(matches!(result, Err(NacelleError::Io(_))));
            });
            assert_eq!(handlers.load(Ordering::SeqCst), 1);
            assert_error_response_size(&recorder, expected);
        }
    }
}

#[cfg(feature = "policies")]
struct LegacyLeaseOnSecond(AtomicUsize);

#[cfg(feature = "policies")]
impl crate::policy::TcpLifecyclePolicy<PhaseRequest, AuthState> for LegacyLeaseOnSecond {
    type Permit = ();

    async fn request_head(
        &self,
        _: &PhaseRequest,
        _: usize,
        _: &ConnectionInfo,
        _: &AuthState,
    ) -> Result<Option<()>, NacelleError> {
        Ok((self.0.fetch_add(1, Ordering::SeqCst) == 1).then_some(()))
    }
}

#[cfg(feature = "policies")]
#[test]
fn legacy_policy_barrier_failure_counts_current_response_bytes() {
    type P = crate::policy::WithTcpPolicy<PhaseProtocol, LegacyLeaseOnSecond>;
    for (write_policy, remaining, fail_flush, expected) in [
        (ResponseWritePolicy::Immediate, usize::MAX, true, 4.0),
        (ResponseWritePolicy::CoalesceBuffered, 2, false, 0.0),
        (ResponseWritePolicy::CoalesceBuffered, 6, false, 2.0),
        (ResponseWritePolicy::CoalesceBuffered, usize::MAX, true, 4.0),
    ] {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let server = TcpServer::<P>::builder()
                .protocol(
                    PhaseProtocol {
                        authenticated: true,
                        request_wire_bytes: None,
                        encoder_writes_then_errors: false,
                    }
                    .with_policy(LegacyLeaseOnSecond(AtomicUsize::new(0))),
                )
                .handler(handler_fn(|context: TcpRequestContext<P>| async move {
                    context.respond(TcpResponse::bytes("next")).await
                }))
                .tcp_config(NacelleTcpConfig::default().with_response_write_policy(write_policy))
                .build()
                .unwrap();
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(server.serve_io(DeliveryFailureIo {
                    reader: io(&[0, 0]).0,
                    remaining,
                    fail_flush,
                }));
            assert!(matches!(result, Err(NacelleError::Io(_))));
        });
        assert_error_response_size(&recorder, expected);
    }
}

#[tokio::test(start_paused = true)]
async fn policy_deadline_is_polled_during_blocked_flush() {
    let mut writer = PendingWrites(tokio::io::empty());
    let mut delivery = super::super::response::ResponseDelivery::new(&NacelleTcpConfig::default());
    let mut failure = None;
    let limits = crate::limits::NacelleTcpLimits::default().without_write_timeout();
    let telemetry = NacelleTelemetry::default();
    let started = tokio::time::Instant::now();
    let policy = bounded_wait(
        WaitingPolicy {
            entries: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Semaphore::new(0)),
        },
        Duration::from_secs(1),
        1,
    );
    let result = super::super::checkpoint::await_admission(
        async {
            Admission::enter(&policy, &mut ())
                .await
                .map_err(Rejection::into_error)
        },
        super::super::checkpoint::Io {
            writer: &mut writer,
            delivery: &mut delivery,
            limits: &limits,
            telemetry: &telemetry,
            metrics: None,
            phase_duration: false,
            failure: &mut failure,
        },
    )
    .await;
    assert!(matches!(result, Err(NacelleError::Timeout(_))));
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert!(
        failure.is_none(),
        "policy rejection is not a delivery failure"
    );
    assert!(delivery.is_interrupted());
    let mut input = ();
    let next = Admission::enter(&policy, &mut input);
    tokio::pin!(next);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx)))
            .await
            .is_pending(),
        "timed-out admission must release its waiter slot"
    );
}

#[tokio::test]
async fn initial_before_policy_flush_failure_does_not_encode_error() {
    let encoded = Arc::new(AtomicUsize::new(0));
    let encoded_errors = encoded.clone();
    let server = TcpServer::<PhaseProtocol>::builder()
        .protocol(PhaseProtocol {
            authenticated: true,
            request_wire_bytes: None,
            encoder_writes_then_errors: false,
        })
        .handler(handler_fn(
            |context: TcpRequestContext<PhaseProtocol>| async move {
                context.respond(TcpResponse::bytes("unexpected")).await
            },
        ))
        .policies(TcpPolicies::new().before_request(WaitingPolicy {
            entries: Arc::new(AtomicUsize::new(0)),
            gate: Arc::new(Semaphore::new(0)),
        }))
        .build()
        .unwrap();
    struct FailingFlush {
        writes: Arc<AtomicUsize>,
    }
    impl AsyncRead for FailingFlush {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }
    impl AsyncWrite for FailingFlush {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("flush failed")))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let result = server
        .serve_io(FailingFlush {
            writes: encoded_errors,
        })
        .await;
    assert!(matches!(result, Err(NacelleError::Io(_))));
    assert_eq!(encoded.load(Ordering::SeqCst), 0);
}

impl<IO: AsyncRead + Unpin> AsyncRead for PendingWrites<IO> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<IO: Unpin> AsyncWrite for PendingWrites<IO> {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

#[tokio::test]
async fn cancellation_interrupts_reads_handlers_and_writes_with_lease_cleanup() {
    for phase in ["read", "body", "handler", "write"] {
        let block_handler = phase == "handler";
        let cancellation = Cancellation::new();
        let active = Arc::new(AtomicUsize::new(0));
        let handlers = Arc::new(AtomicUsize::new(0));
        let called = handlers.clone();
        let runtime = NacelleRuntimeState::default();
        let server = TcpServer::<StaticProtocol>::builder()
            .protocol(StaticProtocol(PhantomData))
            .handler(handler_fn(
                move |context: TcpRequestContext<StaticProtocol>| {
                    let called = called.clone();
                    async move {
                        called.fetch_add(1, Ordering::SeqCst);
                        if block_handler {
                            std::future::pending::<()>().await;
                        }
                        context.respond(TcpResponse::bytes("ok")).await
                    }
                },
            ))
            .runtime_state(runtime.clone())
            .tcp_config(
                NacelleTcpConfig::default().with_request_body_mode(TcpRequestBodyMode::Buffered),
            )
            .policies(
                TcpPolicies::new()
                    .on_transport_ready(ResourcePolicy(active.clone()))
                    .on_request_head(ResourcePolicy(active.clone()))
                    .with_cancellation(cancellation.clone()),
            )
            .build()
            .unwrap();
        let (mut client, stream) = tokio::io::duplex(64);
        let task = tokio::spawn(async move { server.serve_io(PendingWrites(stream)).await });
        if phase != "read" {
            client
                .write_all(&[1, if phase == "body" { 2 } else { 0 }])
                .await
                .unwrap();
        }
        wait_for(&active, if phase == "read" { 1 } else { 2 }).await;
        if phase == "handler" || phase == "write" {
            wait_for(&handlers, 1).await;
        }
        cancellation.cancel();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(NacelleError::ResourceLimit(_))));
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.active_requests(), 0);
    }
}
