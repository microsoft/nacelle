use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http::StatusCode;
use nacelle_core::NacellePeerRateLimiter;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdown};
use nacelle_core::pipeline::{handler_fn, local_handler_fn};
use nacelle_core::policy::{
    Admission, Cancellation, Hold, LocalAdmission, Rejection, RejectionKind, RequestRate, Shared,
    SocketAdmission, check_fn, local_check_fn, map_input,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{
    HttpConnectionStateFactory, HttpHandler, HttpRequestContext, HttpResponse, HyperServer,
    LocalHttpRequestContext, LocalHyperServer, NacelleHttpPolicy,
};

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

async fn exchange<H, F, Observer, P>(
    server: HyperServer<H, F, Observer, P>,
    request: &[u8],
) -> String
where
    F: HttpConnectionStateFactory,
    H: HttpHandler<F::State>,
    Observer: nacelle_core::NacelleTelemetryObserver,
    P: HttpPolicySet<F::State>,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let addr = listener.local_addr().expect("address");
    let (shutdown, token) = NacelleShutdown::pair();
    let task = tokio::spawn(server.serve_listener_with_shutdown(listener, token));
    let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
    client.write_all(request).await.expect("write");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("response deadline")
        .expect("read");
    shutdown.shutdown();
    task.await.expect("join").expect("server");
    String::from_utf8(response).expect("HTTP text")
}

fn ok_handler() -> impl HttpHandler<()> {
    handler_fn(|context: HttpRequestContext<()>| async move {
        context
            .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
            .await
    })
}

struct OrderedFactory(Arc<Mutex<Vec<&'static str>>>);

impl HttpConnectionStateFactory for OrderedFactory {
    type State = ();

    fn create(&self, _: &ConnectionInfo) {
        self.0.lock().expect("log").push("state");
    }
}

#[tokio::test]
async fn checkpoints_append_and_preserve_handler_context() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let socket = log.clone();
    let accepted = log.clone();
    let ready = log.clone();
    let head1 = log.clone();
    let head2 = log.clone();
    let handler1 = log.clone();
    let handler2 = log.clone();
    let handler_log = log.clone();
    let policies = HttpPolicies::new()
        .on_socket(check_fn(move |_: &HttpSocket| {
            socket.lock().expect("log").push("socket");
            Ok(())
        }))
        .on_accepted(check_fn(move |_: &HttpConnection<'_>| {
            accepted.lock().expect("log").push("accepted");
            Ok(())
        }))
        .on_ready(check_fn(move |_: &HttpConnection<'_>| {
            ready.lock().expect("log").push("ready");
            Ok(())
        }))
        .on_request_head(check_fn(move |_: &HttpRequestHead<'_>| {
            head1.lock().expect("log").push("head1");
            Ok(())
        }))
        .on_request_head(check_fn(move |_: &HttpRequestHead<'_>| {
            head2.lock().expect("log").push("head2");
            Ok(())
        }))
        .around_handler(check_fn(move |_: &HttpHandlerInput<'_>| {
            handler1.lock().expect("log").push("handler1");
            Ok(())
        }))
        .around_handler(check_fn(move |_: &HttpHandlerInput<'_>| {
            handler2.lock().expect("log").push("handler2");
            Ok(())
        }));
    let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
        handler_log.lock().expect("log").push("handler");
        async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
    }))
    .with_connection_state_factory(OrderedFactory(log.clone()))
    .with_policies(policies);

    assert!(exchange(server, REQUEST).await.starts_with("HTTP/1.1 200"));
    assert_eq!(
        *log.lock().expect("log"),
        [
            "socket", "accepted", "ready", "state", "head1", "head2", "handler1", "handler2",
            "handler",
        ]
    );
}

struct GateState {
    allow_head: bool,
    allow_handler: bool,
    checks: AtomicUsize,
}

struct GateFactory {
    allow_head: bool,
    allow_handler: bool,
}

impl HttpConnectionStateFactory for GateFactory {
    type State = GateState;

    fn create(&self, _: &ConnectionInfo) -> GateState {
        GateState {
            allow_head: self.allow_head,
            allow_handler: self.allow_handler,
            checks: AtomicUsize::new(0),
        }
    }
}

fn shared_bootstrap_policies() -> impl HttpPolicySet<GateState> {
    HttpPolicies::new()
        .on_request_head(check_fn(|head: &HttpRequestHead<'_, GateState>| {
            assert_eq!(head.state().checks.fetch_add(1, Ordering::Relaxed), 0);
            if head.state().allow_head {
                Ok(())
            } else {
                Err(Rejection::new(RejectionKind::Denied, "head_state"))
            }
        }))
        .around_handler(check_fn(|input: &HttpHandlerInput<'_, GateState>| {
            assert_eq!(input.state().checks.fetch_add(1, Ordering::Relaxed), 1);
            if input.state().allow_handler {
                Ok(())
            } else {
                Err(Rejection::new(RejectionKind::Denied, "handler_state"))
            }
        }))
}

#[tokio::test]
async fn opaque_bootstrap_policies_preserve_typed_state_and_context() {
    for (allow_head, allow_handler, status) in [
        (false, true, "403"),
        (true, false, "403"),
        (true, true, "200"),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<GateState>| {
            assert_eq!(context.connection().state.checks.load(Ordering::Relaxed), 2);
            handler_calls.fetch_add(1, Ordering::Relaxed);
            async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
        }))
        .with_connection_state_factory(GateFactory {
            allow_head,
            allow_handler,
        })
        .with_policies(shared_bootstrap_policies());
        assert!(
            exchange(server, REQUEST)
                .await
                .starts_with(&format!("HTTP/1.1 {status} "))
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            usize::from(allow_head && allow_handler)
        );
    }
}

#[tokio::test]
async fn exclusive_input_composition_does_not_require_state_to_be_sync() {
    type Head<'a> = HttpRequestHead<'a, Cell<usize>, Box<Cell<usize>>>;
    type HandlerInput<'a> = HttpHandlerInput<'a, Cell<usize>, Box<Cell<usize>>>;
    fn assert_send<T: Send>(_: &T) {}
    let mut connection = nacelle_core::pipeline::ConnectionContext::new(
        ConnectionInfo::from(&nacelle_core::NacelleConnectionMeta::http_socket(
            None, None,
        )),
        Box::new(Cell::new(0)),
    );
    let (mut parts, ()) = http::Request::new(()).into_parts();
    let mut head = Head {
        parts: &mut parts,
        connection: &mut connection,
        peer_ip: None,
        body_len: Some(0),
        state_type: std::marker::PhantomData,
    };
    let policy = Chain::new(
        check_fn(|input: &Head<'_>| {
            input.state().set(1);
            Ok(())
        }),
        check_fn(|input: &Head<'_>| {
            assert_eq!(input.state().get(), 1);
            Ok(())
        }),
    );
    let admission = policy.enter(&mut head);
    assert_send(&admission);
    admission.await.expect("head admission");
    let mut request = HttpRequest::new(
        http::Method::GET,
        http::Uri::from_static("/"),
        http::HeaderMap::new(),
        None,
        nacelle_core::NacelleBody::empty(),
    );
    let mut input = HandlerInput {
        request: &mut request,
        connection: &mut connection,
        state_type: std::marker::PhantomData,
    };
    let policy = Chain::new(
        check_fn(|input: &HandlerInput<'_>| {
            input.state().set(2);
            Ok(())
        }),
        check_fn(|input: &HandlerInput<'_>| {
            assert_eq!(input.state().get(), 2);
            Ok(())
        }),
    );
    let admission = policy.enter(&mut input);
    assert_send(&admission);
    admission.await.expect("handler admission");
}

struct RecordedDrop(Arc<Mutex<Vec<&'static str>>>, &'static str);

impl Drop for RecordedDrop {
    fn drop(&mut self) {
        self.0.lock().expect("log").push(self.1);
    }
}

struct Reservation {
    log: Arc<Mutex<Vec<&'static str>>>,
    entered: &'static str,
    dropped: &'static str,
}

impl Reservation {
    fn reserve(&self) -> Hold<RecordedDrop> {
        self.log.lock().expect("log").push(self.entered);
        Hold::new(RecordedDrop(self.log.clone(), self.dropped))
    }
}

impl<I: ?Sized> Admission<I> for Reservation {
    type Lease = Hold<RecordedDrop>;

    fn enter<'a>(
        &'a self,
        _: &'a mut I,
    ) -> impl Future<Output = Result<Self::Lease, Rejection>> + Send + 'a {
        std::future::ready(Ok(self.reserve()))
    }
}

impl<I: ?Sized> SocketAdmission<I> for Reservation {
    type Lease = Hold<RecordedDrop>;

    fn admit(&self, _: &I) -> Result<Self::Lease, Rejection> {
        Ok(self.reserve())
    }
}

#[tokio::test]
async fn scoped_leases_release_in_reverse_acquisition_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let reserve = |entered, dropped| Reservation {
        log: log.clone(),
        entered,
        dropped,
    };
    let policies = HttpPolicies::new()
        .on_socket(reserve("socket_enter", "socket_drop"))
        .on_accepted(reserve("accepted_enter", "accepted_drop"))
        .on_ready(reserve("ready_enter", "ready_drop"))
        .on_request_head(reserve("head1_enter", "head1_drop"))
        .on_request_head(reserve("head2_enter", "head2_drop"))
        .around_handler(reserve("handler1_enter", "handler1_drop"))
        .around_handler(reserve("handler2_enter", "handler2_drop"));
    let response = exchange(
        HyperServer::new(ok_handler()).with_policies(policies),
        REQUEST,
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert_eq!(
        *log.lock().expect("log"),
        [
            "socket_enter",
            "accepted_enter",
            "ready_enter",
            "head1_enter",
            "head2_enter",
            "handler1_enter",
            "handler2_enter",
            "handler2_drop",
            "handler1_drop",
            "head2_drop",
            "head1_drop",
            "ready_drop",
            "accepted_drop",
            "socket_drop",
        ]
    );
}

#[tokio::test]
async fn typed_rejections_close_unread_requests_and_keep_security_headers() {
    for (kind, status) in [
        (RejectionKind::RateLimited, 429),
        (RejectionKind::Overloaded, 503),
        (RejectionKind::Denied, 403),
        (RejectionKind::Timeout, 503),
        (RejectionKind::Closed, 503),
    ] {
        let observer = nacelle_core::NacelleInMemoryObserver::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
            handler_calls.fetch_add(1, Ordering::Relaxed);
            async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
        }))
        .with_telemetry(nacelle_core::NacelleTelemetry::new().with_observer(observer.clone()))
        .with_http_policy(NacelleHttpPolicy::new().with_default_security_headers())
        .with_policies(HttpPolicies::new().on_request_head(check_fn(
            move |_: &HttpRequestHead<'_>| Err(Rejection::new(kind, "test_denied")),
        )));
        let response = exchange(server,
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ).await;
        assert!(response.starts_with(&format!("HTTP/1.1 {status} ")));
        assert!(
            response
                .to_ascii_lowercase()
                .contains("connection: close\r\n")
        );
        assert!(response.contains("x-content-type-options: nosniff\r\n"));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(response.matches("HTTP/1.1").count(), 1);
        assert!(observer.events().iter().any(|event| {
            event.kind == nacelle_core::NacelleTelemetryEventKind::RequestRejected
                && event.reason == Some("test_denied")
        }));
    }
}

#[tokio::test]
async fn mandatory_body_bound_precedes_custom_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let policy_calls = calls.clone();
    let server = HyperServer::new(ok_handler())
        .with_runtime_state(nacelle_core::NacelleRuntimeState::new(
            nacelle_core::NacelleLimits::default().with_max_request_body_bytes(4),
        ))
        .with_policies(HttpPolicies::new().on_request_head(check_fn(
            move |_: &HttpRequestHead<'_>| {
                policy_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
        )));
    let response = exchange(
        server,
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 413"));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn request_body_errors_release_head_and_handler_leases() {
    for request in [
        b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n".as_slice(),
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\nConnection: close\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nZ\r\ninvalid\r\n",
    ] {
        let drops = Arc::new(AtomicUsize::new(0));
        let handler_drops = Arc::new(AtomicUsize::new(0));
        let saw_error = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let handler_error = saw_error.clone();
        let runtime = nacelle_core::NacelleRuntimeState::new(
            nacelle_core::NacelleLimits::default().with_max_request_body_bytes(4),
        );
        let server = HyperServer::new(handler_fn(move |mut context: HttpRequestContext<()>| {
            let handler_error = handler_error.clone();
            async move {
                while let Some(chunk) = context.request_mut().next_body_chunk().await {
                    if let Err(error) = chunk {
                        handler_error.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                }
                context.respond(HttpResponse::empty(StatusCode::OK)).await
            }
        }))
        .with_runtime_state(runtime.clone())
        .with_http_limits(crate::NacelleHttpLimits::default()
            .with_request_body_read_timeout(Duration::from_millis(20)))
        .with_policies(HttpPolicies::new()
            .on_request_head(ReserveHead(drops.clone()))
            .around_handler(ReserveHandler(handler_drops.clone())));
        let response = exchange(server, request).await;
        assert!(response.starts_with("HTTP/1.1 500"));
        assert!(saw_error.load(Ordering::Relaxed));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(handler_drops.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.active_requests(), 0);
        assert_eq!(runtime.active_streaming_tasks(), 0);
    }
}

fn effective_peer(input: &HttpRequestHead<'_>) -> Option<IpAddr> {
    input.peer_ip()
}

#[tokio::test]
async fn head_projection_uses_trusted_proxy_identity_and_shared_rate_state() {
    let limiter = Arc::new(NacellePeerRateLimiter::new(32));
    let rate = RequestRate::new(limiter.clone(), 1);
    let policy =
        NacelleHttpPolicy::new().with_trusted_proxy_ips(["127.0.0.1".parse().expect("proxy IP")]);
    let server = HyperServer::new(ok_handler())
        .with_http_policy(policy)
        .with_policies(HttpPolicies::new().on_request_head(map_input(effective_peer, rate)));
    let response = exchange(server,
        b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Forwarded-For: 198.51.100.42\r\nConnection: close\r\n\r\n",
    ).await;
    assert!(response.starts_with("HTTP/1.1 200"));
    let rate = RequestRate::new(limiter, 1);
    let rejection = rate
        .enter(&mut Some("198.51.100.42".parse().expect("client IP")))
        .await
        .expect_err("HTTP consumed shared budget");
    assert_eq!(rejection.kind(), RejectionKind::RateLimited);
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct ReserveHead(Arc<AtomicUsize>);

impl Admission<HttpRequestHead<'_>> for ReserveHead {
    type Lease = Hold<DropCount>;

    async fn enter<'a>(&'a self, _: &'a mut HttpRequestHead<'_>) -> Result<Self::Lease, Rejection> {
        Ok(Hold::new(DropCount(self.0.clone())))
    }
}

struct ReserveHandler(Arc<AtomicUsize>);

impl Admission<HttpHandlerInput<'_>> for ReserveHandler {
    type Lease = Hold<DropCount>;

    async fn enter<'a>(
        &'a self,
        _: &'a mut HttpHandlerInput<'_>,
    ) -> Result<Self::Lease, Rejection> {
        Ok(Hold::new(DropCount(self.0.clone())))
    }
}

#[tokio::test]
async fn cancellation_interrupts_stalled_handler_and_response_body() {
    for response_body in [false, true] {
        let cancellation = Cancellation::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let handler_drops = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = started.clone();
        let (body_tx, body) = nacelle_core::NacelleBody::channel(1);
        let body = Arc::new(Mutex::new(Some(body)));
        let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
            let started = handler_started.clone();
            let body = body.lock().expect("body").take().expect("one request");
            async move {
                started.notify_one();
                if !response_body {
                    std::future::pending::<()>().await;
                }
                context
                    .respond(HttpResponse::new(StatusCode::OK, HeaderMap::new(), body))
                    .await
            }
        }))
        .with_policies(
            HttpPolicies::new()
                .on_request_head(ReserveHead(drops.clone()))
                .around_handler(ReserveHandler(handler_drops.clone()))
                .with_cancellation(cancellation.clone()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let (shutdown, token) = NacelleShutdown::pair();
        let task = tokio::spawn(server.serve_listener_with_shutdown(listener, token));
        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        client.write_all(REQUEST).await.expect("write");
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("handler started");
        if response_body {
            body_tx
                .send(Ok(bytes::Bytes::from_static(b"first")))
                .await
                .expect("body chunk");
            let mut first = [0; 1024];
            let count = tokio::time::timeout(Duration::from_secs(2), client.read(&mut first))
                .await
                .expect("headers deadline")
                .expect("headers");
            assert!(count > 0);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        assert_eq!(
            handler_drops.load(Ordering::Relaxed),
            usize::from(response_body)
        );
        cancellation.cancel();
        let mut remaining = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut remaining))
            .await
            .expect("cancelled connection closes");
        shutdown.shutdown();
        task.await.expect("join").expect("server");
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(handler_drops.load(Ordering::Relaxed), 1);
    }
}

struct LocalStateFactory(Rc<Cell<usize>>);

impl crate::LocalHttpConnectionStateFactory for LocalStateFactory {
    type State = Rc<Cell<usize>>;

    fn create(&self, _: &ConnectionInfo) -> Self::State {
        self.0.clone()
    }
}

struct LocalReservation(Rc<Cell<usize>>);
struct LocalLease(Rc<Cell<usize>>);

impl Drop for LocalLease {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

impl<State> LocalAdmission<LocalHttpRequestHead<'_, State>> for LocalReservation {
    type Lease = Hold<LocalLease>;

    async fn enter<'a>(
        &'a self,
        _: &'a mut LocalHttpRequestHead<'_, State>,
    ) -> Result<Self::Lease, Rejection> {
        tokio::task::yield_now().await;
        Ok(Hold::new(LocalLease(self.0.clone())))
    }
}

fn local_bootstrap_policies(
    calls: Rc<Cell<usize>>,
    released: Rc<Cell<usize>>,
) -> impl LocalHttpPolicySet<Rc<Cell<usize>>> {
    HttpPolicies::new()
        .on_local_accepted(Shared(check_fn(|_: &HttpConnection<'_>| Ok(()))))
        .on_local_request_head(local_check_fn(
            move |input: &LocalHttpRequestHead<'_, Rc<Cell<usize>>>| {
                assert!(Rc::ptr_eq(input.state(), &calls));
                assert_eq!(input.state().get(), 0);
                calls.set(calls.get() + 1);
                Ok(())
            },
        ))
        .on_local_request_head(LocalReservation(released))
        .around_local_handler(local_check_fn(
            |input: &LocalHttpHandlerInput<'_, Rc<Cell<usize>>>| {
                if input.state().get() == 1 {
                    Ok(())
                } else {
                    Err(Rejection::new(RejectionKind::Denied, "local_state"))
                }
            },
        ))
}

#[tokio::test(flavor = "current_thread")]
async fn opaque_local_bootstrap_policies_preserve_non_send_consumers() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let calls = Rc::new(Cell::new(0));
            let released = Rc::new(Cell::new(0));
            let server = LocalHyperServer::new(local_handler_fn(
                move |context: LocalHttpRequestContext<Rc<Cell<usize>>>| {
                    let state = context.connection().state.as_ref();
                    state.set(state.get() + 1);
                    async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
                },
            ))
            .with_connection_state_factory(LocalStateFactory(calls.clone()))
            .with_policies(local_bootstrap_policies(calls.clone(), released.clone()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let addr = listener.local_addr().expect("address");
            let (shutdown, token) = NacelleShutdown::pair();
            let task = tokio::task::spawn_local(server.serve_listener(
                listener,
                token,
                NacelleDrainDeadline::default(),
            ));
            let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
            client.write_all(REQUEST).await.expect("write");
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.expect("read");
            assert!(response.starts_with(b"HTTP/1.1 200"));
            shutdown.shutdown();
            task.await.expect("join").expect("server");
            assert_eq!(calls.get(), 2);
            assert_eq!(released.get(), 1);
        })
        .await;
}

struct WaitingHead(Arc<tokio::sync::Notify>, Arc<AtomicUsize>);

impl Admission<HttpRequestHead<'_>> for WaitingHead {
    type Lease = ();

    async fn enter<'a>(&'a self, _: &'a mut HttpRequestHead<'_>) -> Result<(), Rejection> {
        let _pending = DropCount(self.1.clone());
        self.0.notify_one();
        std::future::pending().await
    }
}

#[tokio::test]
async fn cancellation_interrupts_head_admission_and_releases_runtime_capacity() {
    let cancellation = Cancellation::new();
    let started = Arc::new(tokio::sync::Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let runtime = nacelle_core::NacelleRuntimeState::default();
    let server = HyperServer::new(ok_handler())
        .with_runtime_state(runtime.clone())
        .with_policies(
            HttpPolicies::new()
                .on_request_head(WaitingHead(started.clone(), drops.clone()))
                .with_cancellation(cancellation.clone()),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let addr = listener.local_addr().expect("address");
    let (shutdown, token) = NacelleShutdown::pair();
    let task = tokio::spawn(server.serve_listener_with_shutdown(listener, token));
    let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
    client.write_all(REQUEST).await.expect("request");
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("admission");
    assert_eq!(runtime.active_requests(), 1);
    cancellation.cancel();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("cancelled connection");
    shutdown.shutdown();
    task.await.expect("join").expect("server");
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_connections(), 0);
}

#[cfg(feature = "tls-self-signed")]
#[tokio::test]
async fn accepted_rejection_precedes_tls_handshake() {
    let accepted_calls = Arc::new(AtomicUsize::new(0));
    let accepted = accepted_calls.clone();
    let ready_calls = Arc::new(AtomicUsize::new(0));
    let ready = ready_calls.clone();
    let server = HyperServer::new(ok_handler()).with_policies(
        HttpPolicies::new()
            .on_accepted(check_fn(move |input: &HttpConnection<'_>| {
                assert!(input.connection().tls.is_none());
                accepted.fetch_add(1, Ordering::Relaxed);
                Err(Rejection::new(RejectionKind::Denied, "tls_admission"))
            }))
            .on_ready(check_fn(move |_: &HttpConnection<'_>| {
                ready.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let addr = listener.local_addr().expect("address");
    let config = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).expect("TLS");
    let (shutdown, token) = NacelleShutdown::pair();
    let task =
        tokio::spawn(server.serve_tls_listener_with_shutdown(listener, config.tls_config, token));
    let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut response = [0; 1];
    let count = tokio::time::timeout(Duration::from_secs(2), client.read(&mut response))
        .await
        .expect("pre-handshake rejection")
        .expect("close");
    assert_eq!(count, 0);
    shutdown.shutdown();
    task.await.expect("join").expect("server");
    assert_eq!(accepted_calls.load(Ordering::Relaxed), 1);
    assert_eq!(ready_calls.load(Ordering::Relaxed), 0);
}

#[cfg(feature = "tls-self-signed")]
struct TlsAdmission {
    started: Arc<tokio::sync::Notify>,
    drops: Arc<AtomicUsize>,
    wait: bool,
}

#[cfg(feature = "tls-self-signed")]
impl Admission<HttpConnection<'_>> for TlsAdmission {
    type Lease = Hold<DropCount>;

    async fn enter<'a>(&'a self, _: &'a mut HttpConnection<'_>) -> Result<Self::Lease, Rejection> {
        let lease = Hold::new(DropCount(self.drops.clone()));
        self.started.notify_one();
        if self.wait {
            std::future::pending::<()>().await;
        }
        Ok(lease)
    }
}

#[cfg(feature = "tls-self-signed")]
#[tokio::test]
async fn cancellation_interrupts_accepted_admission_and_tls_handshake_waits() {
    for wait in [false, true] {
        let cancellation = Cancellation::new();
        let started = Arc::new(tokio::sync::Notify::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let ready_calls = Arc::new(AtomicUsize::new(0));
        let ready = ready_calls.clone();
        let runtime = nacelle_core::NacelleRuntimeState::default();
        let server = HyperServer::new(ok_handler())
            .with_runtime_state(runtime.clone())
            .with_policies(
                HttpPolicies::new()
                    .on_accepted(TlsAdmission {
                        started: started.clone(),
                        drops: drops.clone(),
                        wait,
                    })
                    .on_ready(check_fn(move |_: &HttpConnection<'_>| {
                        ready.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }))
                    .with_cancellation(cancellation.clone()),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let addr = listener.local_addr().expect("address");
        let config = nacelle_rustls::NacelleTlsConfig::self_signed(["localhost"]).expect("TLS");
        let (shutdown, token) = NacelleShutdown::pair();
        let task = tokio::spawn(server.serve_tls_listener_with_shutdown(
            listener,
            config.tls_config,
            token,
        ));
        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        tokio::time::timeout(Duration::from_secs(2), started.notified())
            .await
            .expect("accepted");
        assert_eq!(runtime.active_connections(), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        cancellation.cancel();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("TLS cancellation deadline")
            .expect("closed socket");
        assert!(response.is_empty());
        shutdown.shutdown();
        task.await.expect("join").expect("server");
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(ready_calls.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.active_connections(), 0);
    }
}

#[test]
fn empty_sets_have_no_storage_or_allocation_handle() {
    assert_eq!(std::mem::size_of::<NoHttpPolicies>(), 0);
    assert_eq!(std::mem::size_of::<HttpPolicies>(), 0);
    let _server = HyperServer::new(()).with_policies(HttpPolicies::new());
}
