use std::mem::size_of_val;
use std::sync::atomic::AtomicBool;

use nacelle_core::pipeline::handler_fn;
use nacelle_core::policy::{Admission, Cancellation, Hold, check_fn};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};

use super::*;
use crate::HttpPolicies;

const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

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
    let request = HttpRequest::new(
        Method::GET,
        http::Uri::from_static("/"),
        http::HeaderMap::new(),
        None,
        nacelle_core::NacelleBody::empty(),
    );
    let handler_bytes = size_of_val(&HttpDispatch::<(), NoopObserver>::call(
        &mut dispatch,
        request,
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
