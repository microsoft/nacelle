use std::mem::size_of_val;
use std::sync::atomic::AtomicBool;

use nacelle_core::pipeline::handler_fn;
use nacelle_core::policy::{Admission, Cancellation, Hold, check_fn};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};

use super::*;
use crate::HttpPolicies;

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

fn duplex_http<H: HttpHandler<()>, Policies: HttpPolicySet>(
    server: HyperServer<H, NoHttpConnectionState, NoopObserver, Policies>,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<Result<(), hyper::Error>>,
) {
    let server = Arc::new(server);
    let connection = ConnectionContext::new(
        ConnectionInfo::from(&NacelleConnectionMeta::http_socket(
            Some("127.0.0.1:12345".parse().expect("peer")),
            None,
        )),
        Arc::new(()),
    );
    let service = service_fn(move |request| {
        let server = server.clone();
        let connection = connection.clone();
        async move { server.handle(request, &connection).await }
    });
    let (stream, client) = tokio::io::duplex(4096);
    let task = tokio::spawn(async move {
        http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
    });
    (client, task)
}

#[tokio::test]
async fn policy_rejection_closes_http10_head_keep_alive() {
    for reject_at_handler in [false, true] {
        let rejections = Arc::new(AtomicUsize::new(0));
        let head_rejections = rejections.clone();
        let handler_rejections = rejections.clone();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = handler_calls.clone();
        let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
            observed_calls.fetch_add(1, Ordering::Relaxed);
            async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
        }))
        .with_policies(
            HttpPolicies::new()
                .on_request_head(check_fn(move |_: &HttpRequestHead<'_>| {
                    if reject_at_handler {
                        Ok(())
                    } else {
                        head_rejections.fetch_add(1, Ordering::Relaxed);
                        Err(Rejection::new(RejectionKind::RateLimited, "head_rate"))
                    }
                }))
                .around_handler(check_fn(move |_: &HttpHandlerInput<'_>| {
                    handler_rejections.fetch_add(1, Ordering::Relaxed);
                    Err(Rejection::new(RejectionKind::RateLimited, "handler_rate"))
                })),
        );
        let (mut client, task) = duplex_http(server);
        client
            .write_all(
                b"HEAD / HTTP/1.0\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n\
                  GET / HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("requests");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("rejection must close")
            .expect("response");
        task.await.expect("join").expect("connection");
        let response = String::from_utf8(response).expect("HTTP response");
        assert!(response.starts_with("HTTP/1.0 429"), "{response}");
        assert!(!response.contains("keep-alive"), "{response}");
        assert_eq!(rejections.load(Ordering::Relaxed), 1);
        assert_eq!(handler_calls.load(Ordering::Relaxed), 0);
    }
}

async fn legacy_error_connection_reuse(bodyless: bool) {
    for failure in ["handler", "request_limit", "edge", "peer_rate"] {
        let runtime = NacelleRuntimeState::new(
            nacelle_core::NacelleLimits::default().with_max_in_flight_requests(1),
        );
        let permit = (failure == "request_limit")
            .then(|| runtime.acquire_request().expect("reserve request"));
        let policy = match failure {
            "edge" => NacelleHttpPolicy::new().with_max_uri_len(4),
            "peer_rate" => NacelleHttpPolicy::new().with_max_requests_per_peer_per_second(1),
            _ => NacelleHttpPolicy::default(),
        };
        let server = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
            if context.request().uri.path() == "/fail" {
                Err(NacelleError::protocol("handler failure"))
            } else {
                context.respond(HttpResponse::empty(StatusCode::OK)).await
            }
        }))
        .with_runtime_state(runtime.clone())
        .with_http_policy(policy);
        if failure == "peer_rate" {
            assert!(
                server
                    .runtime
                    .allow_peer_request("127.0.0.1".parse().expect("peer"))
                    .is_allowed()
            );
        }
        let (mut client, task) = duplex_http(server);
        let request = if bodyless {
            b"GET /fail HTTP/1.1\r\nHost: localhost\r\n\r\n\
              GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                .as_slice()
        } else {
            b"POST /fail HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1\r\n\r\n".as_slice()
        };
        client.write_all(request).await.expect("requests");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("error response must finish")
            .expect("response");
        task.await.expect("join").expect("connection");
        let response = String::from_utf8(response).expect("HTTP response");
        let expected_status = match failure {
            "handler" => "HTTP/1.1 500",
            "request_limit" => "HTTP/1.1 503",
            "edge" => "HTTP/1.1 414",
            "peer_rate" => "HTTP/1.1 429",
            _ => unreachable!("known failure"),
        };
        assert!(
            response.starts_with(expected_status),
            "{failure}: {response}"
        );
        assert_eq!(
            response.matches("HTTP/1.1 ").count(),
            if bodyless { 2 } else { 1 },
            "{failure}: {response}"
        );
        if bodyless && matches!(failure, "handler" | "edge") {
            assert!(response.contains("HTTP/1.1 200"), "{failure}: {response}");
        }
        if !bodyless {
            assert!(
                response.contains("connection: close"),
                "{failure}: {response}"
            );
        }
        drop(permit);
        assert_eq!(runtime.active_requests(), 0);
        assert_eq!(runtime.active_streaming_tasks(), 0);
    }
}

#[tokio::test]
async fn legacy_bodyless_errors_preserve_connection_reuse() {
    legacy_error_connection_reuse(true).await;
}

#[tokio::test]
async fn legacy_errors_close_connections_with_unread_bodies() {
    legacy_error_connection_reuse(false).await;
}

#[tokio::test]
async fn handler_errors_preserve_connection_reuse_after_body_eof() {
    for body in [
        "Content-Length: 1\r\n\r\nx",
        "Transfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n",
        "Transfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n1\r\nx\r\n0\r\nX-End: done\r\n\r\n",
    ] {
        let server = HyperServer::new(handler_fn(
            |mut context: HttpRequestContext<()>| async move {
                if context.request().uri.path() == "/fail" {
                    while let Some(chunk) = context.request_mut().next_body_chunk().await {
                        let _ = chunk?;
                    }
                    Err(NacelleError::protocol("handler failure"))
                } else {
                    context.respond(HttpResponse::empty(StatusCode::OK)).await
                }
            },
        ));
        let (mut client, task) = duplex_http(server);
        let request = format!(
            "POST /fail HTTP/1.1\r\nHost: localhost\r\n{body}\
             GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        );
        client
            .write_all(request.as_bytes())
            .await
            .expect("requests");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
            .await
            .expect("responses must finish")
            .expect("responses");
        task.await.expect("join").expect("connection");
        let response = String::from_utf8(response).expect("HTTP responses");
        assert!(response.starts_with("HTTP/1.1 500"), "{response}");
        assert_eq!(response.matches("HTTP/1.1 ").count(), 2, "{response}");
        assert!(response.contains("HTTP/1.1 200"), "{response}");
    }
}

#[tokio::test]
async fn handler_errors_close_connections_after_body_pump_failure() {
    let runtime = NacelleRuntimeState::new(
        nacelle_core::NacelleLimits::default().with_max_request_body_bytes(1),
    );
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                let _ = chunk?;
            }
            context.respond(HttpResponse::empty(StatusCode::OK)).await
        },
    ))
    .with_runtime_state(runtime.clone());
    let (mut client, task) = duplex_http(server);
    client
        .write_all(
            b"POST /fail HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n\
              2\r\nab\r\n0\r\n\r\n\
              GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("requests");
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("error response must finish")
        .expect("response");
    task.await.expect("join").expect("connection");
    let response = String::from_utf8(response).expect("HTTP response");
    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    assert_eq!(response.matches("HTTP/1.1 ").count(), 1, "{response}");
    assert!(response.contains("connection: close"), "{response}");
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
}

struct WaitingHandlerAdmission {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    drops: Arc<AtomicUsize>,
}

impl Admission<HttpHandlerInput<'_>> for WaitingHandlerAdmission {
    type Lease = Hold<DropCount>;

    async fn enter<'a>(
        &'a self,
        _: &'a mut HttpHandlerInput<'_>,
    ) -> Result<Self::Lease, Rejection> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(Hold::new(DropCount(self.drops.clone())))
    }
}

async fn waiting_handler_admission(request: &[u8], check_deadline: bool) {
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let runtime = NacelleRuntimeState::new(
        nacelle_core::NacelleLimits::default().with_handler_timeout(Duration::from_millis(30)),
    );
    let handler_drops = drops.clone();
    let server = HyperServer::new(handler_fn(move |mut context: HttpRequestContext<()>| {
        let handler_drops = handler_drops.clone();
        async move {
            assert_eq!(handler_drops.load(Ordering::Relaxed), 0);
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                let _ = chunk?;
            }
            context.respond(HttpResponse::empty(StatusCode::OK)).await
        }
    }))
    .with_runtime_state(runtime.clone())
    .with_policies(HttpPolicies::new().around_handler(WaitingHandlerAdmission {
        entered: entered.clone(),
        release: release.clone(),
        drops: drops.clone(),
    }));
    let (mut client, task) = duplex_http(server);
    client.write_all(request).await.expect("request");
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("handler admission must start");
    if check_deadline {
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert!(
            !task.is_finished(),
            "admission must not use handler timeout"
        );
    } else {
        tokio::task::yield_now().await;
        assert_eq!(
            runtime.active_streaming_tasks(),
            0,
            "body pump must not start during admission"
        );
    }
    assert_eq!(runtime.active_requests(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    release.notify_one();
    if !check_deadline {
        client.write_all(b"abc").await.expect("request body");
    }
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("response deadline")
        .expect("response");
    task.await.expect("join").expect("connection");
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.active_requests(), 0);
    assert_eq!(runtime.active_streaming_tasks(), 0);
}

#[tokio::test]
async fn handler_admission_wait_is_outside_handler_deadline() {
    waiting_handler_admission(REQUEST, true).await;
}

#[tokio::test]
async fn handler_admission_precedes_body_pump() {
    waiting_handler_admission(
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nConnection: close\r\n\r\n",
        false,
    )
    .await;
}

struct LocalHandlerReservation(Rc<std::cell::Cell<usize>>);

struct LocalDropCount(Rc<std::cell::Cell<usize>>);

impl Drop for LocalDropCount {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

impl nacelle_core::policy::LocalAdmission<LocalHttpHandlerInput<'_>> for LocalHandlerReservation {
    type Lease = Hold<LocalDropCount>;

    async fn enter<'a>(
        &'a self,
        _: &'a mut LocalHttpHandlerInput<'_>,
    ) -> Result<Self::Lease, Rejection> {
        tokio::time::sleep(Duration::from_millis(90)).await;
        Ok(Hold::new(LocalDropCount(self.0.clone())))
    }
}

#[tokio::test]
async fn local_handler_admission_retains_non_send_lease_only_until_handler_ends() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let drops = Rc::new(std::cell::Cell::new(0));
            let handler_drops = drops.clone();
            let runtime = NacelleRuntimeState::new(
                nacelle_core::NacelleLimits::default()
                    .with_handler_timeout(Duration::from_millis(30)),
            );
            let (sender, body) = nacelle_core::NacelleBody::channel(1);
            let body = Rc::new(std::cell::RefCell::new(Some(body)));
            let server = Rc::new(
                LocalHyperServer::new(nacelle_core::pipeline::local_handler_fn(
                    move |context: LocalHttpRequestContext<()>| {
                        assert_eq!(handler_drops.get(), 0);
                        let body = body.borrow_mut().take().expect("one handler call");
                        async move {
                            context
                                .respond(HttpResponse::new(
                                    StatusCode::OK,
                                    http::HeaderMap::new(),
                                    body,
                                ))
                                .await
                        }
                    },
                ))
                .with_runtime_context(
                    NacelleTelemetry::default(),
                    runtime.clone(),
                    LocalHttpSharedState::default(),
                )
                .with_policies(
                    HttpPolicies::new()
                        .around_local_handler(LocalHandlerReservation(drops.clone())),
                ),
            );
            let (stream, mut client) = tokio::io::duplex(4096);
            let task = tokio::task::spawn_local(async move {
                run_ready_local_http_connection(
                    &server,
                    stream,
                    NacelleConnectionMeta::http_socket(None, None),
                )
                .await
            });
            client.write_all(REQUEST).await.expect("request");
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !response.ends_with(b"\r\n\r\n") {
                    response.push(client.read_u8().await.expect("response header"));
                }
            })
            .await
            .expect("headers must arrive after admission");
            assert!(response.starts_with(b"HTTP/1.1 200"));
            assert_eq!(drops.get(), 1, "handler lease must not cover response body");
            assert_eq!(runtime.active_requests(), 0);
            sender
                .send(Ok(bytes::Bytes::from_static(b"body")))
                .await
                .expect("response chunk");
            drop(sender);
            client.read_to_end(&mut response).await.expect("response");
            task.await.expect("join").expect("connection");
            assert_eq!(drops.get(), 1);
        })
        .await;
}

async fn request_future_layout<Policies: HttpPolicySet>(policies: Policies) -> (usize, usize) {
    let base = HyperServer::new(handler_fn(|context: HttpRequestContext<()>| async move {
        context.respond(HttpResponse::empty(StatusCode::OK)).await
    }));
    let server = Arc::new(HyperServer {
        handler: base.handler,
        connection_state_factory: base.connection_state_factory,
        runtime: base.runtime,
        policies,
    });
    let connection = ConnectionContext::new(
        ConnectionInfo::from(&NacelleConnectionMeta::http_socket(None, None)),
        Arc::new(()),
    );
    let mut dispatch = SharedHttpDispatch {
        handler: server.handler.clone(),
        connection: connection.clone(),
        policies: &server.policies,
    };
    let mut request = HttpRequest::new(
        Method::GET,
        http::Uri::from_static("/"),
        http::HeaderMap::new(),
        None,
        nacelle_core::NacelleBody::empty(),
    );
    let handler_lease =
        HttpDispatch::<(), NoopObserver>::admit_handler(&mut dispatch, &mut request)
            .await
            .expect("handler admission");
    let handler_bytes = size_of_val(&HttpDispatch::<(), NoopObserver>::call(
        &mut dispatch,
        request,
        handler_lease,
    ));
    let request_bytes = Arc::new(AtomicUsize::new(0));
    let observed_bytes = request_bytes.clone();
    let service = service_fn(move |request| {
        let server = server.clone();
        let connection = connection.clone();
        let observed_bytes = observed_bytes.clone();
        async move {
            let request = server.handle(request, &connection);
            observed_bytes.store(size_of_val(&request), Ordering::Relaxed);
            request.await
        }
    });
    let (stream, mut client) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        http1::Builder::new()
            .serve_connection(TokioIo::new(stream), service)
            .await
    });
    client.write_all(REQUEST).await.expect("request");
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.expect("response");
    task.await.expect("join").expect("connection");
    assert!(response.starts_with(b"HTTP/1.1 200"));
    (request_bytes.load(Ordering::Relaxed), handler_bytes)
}

#[tokio::test]
async fn empty_and_ready_request_future_layout() {
    let default = request_future_layout(NoHttpPolicies).await;
    let empty = request_future_layout(HttpPolicies::new()).await;
    let ready = request_future_layout(HttpPolicies::new().on_request_head(check_fn(
        |head: &HttpRequestHead<'_>| {
            if head.method() == Method::GET {
                Ok(())
            } else {
                Err(Rejection::new(RejectionKind::Denied, "layout_method"))
            }
        },
    )))
    .await;
    let ready_handler = request_future_layout(HttpPolicies::new().around_handler(check_fn(
        |input: &HttpHandlerInput<'_>| {
            if input.request().method == Method::GET {
                Ok(())
            } else {
                Err(Rejection::new(RejectionKind::Denied, "layout_method"))
            }
        },
    )))
    .await;
    println!(
        "HTTP future bytes (request, handler): default={default:?}, empty={empty:?}, ready_head={ready:?}, ready_handler={ready_handler:?}"
    );
    // Compare storage on the same target; this is not an allocation or timing claim.
    assert_eq!(
        empty, default,
        "attaching an empty set must not grow request futures"
    );
    assert_eq!(
        ready, default,
        "a ready head check must not grow request futures"
    );
    assert_eq!(
        ready_handler, default,
        "a ready handler check must not grow request futures"
    );
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

struct HeadReservation(Arc<AtomicUsize>);

impl Admission<HttpRequestHead<'_>> for HeadReservation {
    type Lease = Hold<DropCount>;

    async fn enter<'a>(&'a self, _: &'a mut HttpRequestHead<'_>) -> Result<Self::Lease, Rejection> {
        Ok(Hold::new(DropCount(self.0.clone())))
    }
}

struct StalledWriteIo {
    inner: tokio::io::DuplexStream,
    stalled: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
    written: usize,
}

impl Drop for StalledWriteIo {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}

impl AsyncRead for StalledWriteIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl AsyncWrite for StalledWriteIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.written == 8 {
            self.stalled.notify_one();
            return Poll::Pending;
        }
        let count = buffer.len().min(8 - self.written);
        match Pin::new(&mut self.inner).poll_write(cx, &buffer[..count]) {
            Poll::Ready(Ok(count)) => {
                self.written += count;
                Poll::Ready(Ok(count))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn cancellation_interrupts_a_committed_stalled_response_write() {
    let cancellation = Cancellation::new();
    let lease_drops = Arc::new(AtomicUsize::new(0));
    let io_dropped = Arc::new(AtomicBool::new(false));
    let stalled = Arc::new(tokio::sync::Notify::new());
    let runtime = NacelleRuntimeState::default();
    let (sender, body) = nacelle_core::NacelleBody::channel(1);
    sender
        .send(Ok(bytes::Bytes::from_static(b"response")))
        .await
        .expect("body chunk");
    let body = Arc::new(std::sync::Mutex::new(Some(body)));
    let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
        let body = body.lock().expect("body").take().expect("one request");
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
    .with_runtime_state(runtime.clone())
    .with_policies(
        HttpPolicies::new()
            .on_request_head(HeadReservation(lease_drops.clone()))
            .with_cancellation(cancellation.clone()),
    );
    let peer = "127.0.0.1:12345".parse::<SocketAddr>().expect("peer");
    let permit = runtime
        .acquire_connection_for_peer(peer.ip())
        .expect("permit");
    let (stream, mut client) = tokio::io::duplex(1024);
    let stream = StalledWriteIo {
        inner: stream,
        stalled: stalled.clone(),
        dropped: io_dropped.clone(),
        written: 0,
    };
    let task = tokio::spawn(run_http_connection(
        Arc::new(server),
        move |connection| async move { Ok((stream, connection)) },
        NacelleConnectionMeta::http_socket(Some(peer), None),
        permit,
        (),
    ));
    client.write_all(REQUEST).await.expect("request");
    tokio::time::timeout(Duration::from_secs(2), stalled.notified())
        .await
        .expect("write must reach pending");
    assert_eq!(lease_drops.load(Ordering::Relaxed), 0);
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("cancellation deadline")
        .expect("join");
    assert!(matches!(
        result,
        Err(NacelleError::ResourceLimit(
            NacelleResourceLimitReason::Other("policy_cancelled")
        ))
    ));
    let mut response = Vec::new();
    client
        .read_to_end(&mut response)
        .await
        .expect("closed stream");
    assert_eq!(response, b"HTTP/1.1");
    assert!(io_dropped.load(Ordering::Relaxed));
    assert_eq!(lease_drops.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.active_connections(), 0);
    assert_eq!(runtime.active_requests(), 0);
    assert!(
        sender.is_closed(),
        "cancellation must drop the pending response body"
    );
}
