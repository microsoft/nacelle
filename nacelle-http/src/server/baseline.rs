use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::task::Waker;

use bytes::{Buf, Bytes};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use nacelle_core::pipeline::handler_fn;
use nacelle_core::{NacelleBody, NacelleInMemoryObserver};
use tokio::io::ReadBuf;

use super::*;

mod admission;

#[derive(Default)]
struct Observations {
    read_bytes: AtomicUsize,
    output: Mutex<Vec<u8>>,
    block_write: AtomicBool,
    block_flush: AtomicBool,
}

struct ObservedIo {
    input: Bytes,
    observations: Arc<Observations>,
}

impl ObservedIo {
    fn new(input: impl Into<Bytes>) -> (Self, Arc<Observations>) {
        let observations = Arc::new(Observations::default());
        (
            Self {
                input: input.into(),
                observations: observations.clone(),
            },
            observations,
        )
    }
}

impl AsyncRead for ObservedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.input.is_empty() {
            return Poll::Pending;
        }
        let count = buffer.remaining().min(self.input.len());
        buffer.put_slice(&self.input[..count]);
        self.input.advance(count);
        self.observations
            .read_bytes
            .fetch_add(count, Ordering::Relaxed);
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ObservedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.observations.block_write.load(Ordering::Relaxed) {
            return Poll::Pending;
        }
        self.observations
            .output
            .lock()
            .expect("output lock")
            .extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.observations.block_flush.load(Ordering::Relaxed) {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// These probes manually repoll after changing input/gates; no executor wake is required.
fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(
        future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

fn connection_context() -> ConnectionContext<Arc<()>> {
    ConnectionContext::new(
        ConnectionInfo::from(&NacelleConnectionMeta::http(None)),
        Arc::new(()),
    )
}

#[tokio::test]
async fn pending_head_gate_exposes_parser_read_ahead_without_dispatch() {
    for (maximum, padding) in [(None, 0), (Some(8192), 0), (Some(10_000), 12_000)] {
        let head = format!(
            "POST / HTTP/1.1\r\nHost: localhost\r\nX-Padding: {}\r\nContent-Length: 65536\r\n\r\n",
            "a".repeat(padding)
        );
        let (io, observed) = ObservedIo::new(format!("{head}{}", "a".repeat(65_536)));
        let heads = Arc::new(AtomicUsize::new(0));
        let server = Arc::new(HyperServer::new(handler_fn(
            |_context: HttpRequestContext<()>| async move {
                panic!("pending head gate must not dispatch the handler")
            },
        )));
        let service = service_fn({
            let heads = heads.clone();
            move |request| {
                heads.fetch_add(1, Ordering::Relaxed);
                let server = server.clone();
                async move {
                    std::future::pending::<()>().await;
                    server.handle(request, &connection_context()).await
                }
            }
        });
        let mut builder = http1::Builder::new();
        builder.timer(TokioTimer::new());
        if let Some(maximum) = maximum {
            builder.max_buf_size(maximum);
        }
        let connection = builder.serve_connection(TimeoutIo::new(TokioIo::new(io), None), service);
        tokio::pin!(connection);
        assert_pending(connection.as_mut());
        assert_eq!(heads.load(Ordering::Relaxed), 1);
        let read = observed.read_bytes.load(Ordering::Relaxed);
        assert!(read > head.len(), "parser read includes body bytes");
        if padding == 0 {
            assert!(read <= 8192, "small head uses the initial read buffer");
        } else {
            assert!(read > 10_000, "parser setting is not a hard read allowance");
        }
        assert_pending(connection.as_mut());
        assert_eq!(observed.read_bytes.load(Ordering::Relaxed), read);
        assert!(observed.output.lock().expect("output lock").is_empty());
    }
}

#[tokio::test]
async fn parser_limits_reject_before_nacelle_dispatch() {
    for header_count_limit in [false, true] {
        let request = if header_count_limit {
            "GET / HTTP/1.1\r\nHost: localhost\r\nX-Extra: value\r\n\r\n".to_owned()
        } else {
            format!(
                "GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: {}\r\n\r\n",
                "a".repeat(9000)
            )
        };
        let (io, observed) = ObservedIo::new(request);
        let server = HyperServer::new(handler_fn(|_context: HttpRequestContext<()>| async move {
            panic!("parser rejection must not dispatch the handler")
        }));
        let context = connection_context();
        let service = service_fn(|request| server.handle(request, &context));
        let mut builder = http1::Builder::new();
        builder.timer(TokioTimer::new()).max_buf_size(8192);
        if header_count_limit {
            builder.max_headers(1);
        }
        let result = builder
            .serve_connection(TimeoutIo::new(TokioIo::new(io), None), service)
            .await;
        assert!(result.is_err());
        assert!(
            observed
                .output
                .lock()
                .expect("output lock")
                .starts_with(b"HTTP/1.1 431 ")
        );
    }
}

#[tokio::test]
async fn expect_continue_is_deferred_until_nacelle_polls_the_body() {
    let (io, observed) = ObservedIo::new(Bytes::from_static(
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n",
    ));
    let allow = AtomicBool::new(false);
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                let _ = chunk?;
            }
            context.respond(HttpResponse::empty(StatusCode::OK)).await
        },
    ));
    let context = connection_context();
    let service = service_fn(|request| async {
        std::future::poll_fn(|_cx| {
            if allow.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        server.handle(request, &context).await
    });
    let connection = http1::Builder::new()
        .timer(TokioTimer::new())
        .serve_connection(TimeoutIo::new(TokioIo::new(io), None), service);
    tokio::pin!(connection);
    assert_pending(connection.as_mut());
    assert!(observed.output.lock().expect("output lock").is_empty());
    allow.store(true, Ordering::Relaxed);
    assert_pending(connection.as_mut());
    assert_pending(connection.as_mut());
    assert!(
        observed
            .output
            .lock()
            .expect("output lock")
            .starts_with(b"HTTP/1.1 100 Continue\r\n\r\n")
    );
}

#[tokio::test]
async fn rejected_expect_request_closes_even_with_a_buffered_next_request() {
    for buffered_body in [false, true] {
        let head =
            "POST / HTTP/1.1\r\nHost: denied\r\nContent-Length: 4\r\nExpect: 100-continue\r\n\r\n";
        let suffix = if buffered_body {
            "bodyGET /next HTTP/1.1\r\nHost: allowed\r\nConnection: close\r\n\r\n"
        } else {
            ""
        };
        let (io, observed) = ObservedIo::new(format!("{head}{suffix}"));
        let calls = Arc::new(AtomicUsize::new(0));
        let server = Arc::new(
            HyperServer::new(handler_fn({
                let calls = calls.clone();
                move |context: HttpRequestContext<()>| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    assert_eq!(context.request().uri.path(), "/next");
                    async move { context.respond(HttpResponse::empty(StatusCode::OK)).await }
                }
            }))
            .with_http_policy(NacelleHttpPolicy::new().with_allowed_hosts(["allowed"])),
        );
        let state = server.runtime.runtime_state.clone();
        run_http_connection(
            server,
            io,
            NacelleConnectionMeta::http(None),
            state.acquire_connection().expect("connection capacity"),
        )
        .await
        .expect("connection completes normally");
        let output = observed.output.lock().expect("output lock");
        assert!(output.starts_with(b"HTTP/1.1 421 "));
        assert!(!output.windows(12).any(|part| part == b"100 Continue"));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            output
                .windows(9)
                .filter(|part| *part == b"HTTP/1.1 ")
                .count(),
            1
        );
        assert_eq!(state.active_connections(), 0);
        assert_eq!(state.active_requests(), 0);
    }
}

#[tokio::test]
async fn graceful_shutdown_waits_for_response_production_and_underlying_flush() {
    let (io, observed) = ObservedIo::new(Bytes::from_static(
        b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
    ));
    let (sender, body) = NacelleBody::channel(1);
    let body = Mutex::new(Some(body));
    let observer = NacelleInMemoryObserver::new();
    let server = HyperServer::new(handler_fn(move |context: HttpRequestContext<()>| {
        let body = body.lock().expect("body lock").take().expect("one request");
        async move {
            context
                .respond(HttpResponse::new(
                    StatusCode::OK,
                    hyper::HeaderMap::new(),
                    body,
                ))
                .await
        }
    }))
    .with_telemetry(NacelleTelemetry::default().with_observer(observer.clone()));
    let context = connection_context();
    let service = service_fn(|request| server.handle(request, &context));
    let connection = http1::Builder::new()
        .timer(TokioTimer::new())
        .serve_connection(TimeoutIo::new(TokioIo::new(io), None), service);
    tokio::pin!(connection);
    assert_pending(connection.as_mut());
    assert_eq!(server.runtime.runtime_state.active_requests(), 0);
    assert!(
        observer
            .events()
            .iter()
            .any(|event| event.kind == NacelleTelemetryEventKind::RequestCompleted)
    );
    connection.as_mut().graceful_shutdown();
    assert_pending(connection.as_mut());
    sender
        .try_send(Ok(Bytes::from_static(b"hello")))
        .expect("body capacity");
    drop(sender);
    observed.block_flush.store(true, Ordering::Relaxed);
    assert_pending(connection.as_mut());
    assert!(
        observed
            .output
            .lock()
            .expect("output lock")
            .windows(5)
            .any(|bytes| bytes == b"hello")
    );
    observed.block_flush.store(false, Ordering::Relaxed);
    connection.await.expect("graceful completion");
    assert_eq!(
        observer
            .events()
            .iter()
            .filter(|event| event.kind == NacelleTelemetryEventKind::ResponseBodyBytes)
            .map(|event| event.count)
            .collect::<Vec<_>>(),
        vec![5]
    );
}

#[test]
fn http_request_and_response_metrics_keep_distinct_completion_boundaries() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                let (io, _) = ObservedIo::new(Bytes::from_static(
                    b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc",
                ));
                let server = Arc::new(HyperServer::new(handler_fn(
                    |mut context: HttpRequestContext<()>| async move {
                        while let Some(chunk) = context.request_mut().next_body_chunk().await {
                            let _ = chunk?;
                        }
                        context.respond(HttpResponse::bytes(StatusCode::OK, "hello")).await
                    },
                )));
                let permit = server.runtime.runtime_state.acquire_connection().expect("capacity");
                run_http_connection(server, io, NacelleConnectionMeta::http(None), permit).await.expect("request");
            });
    });
    let snapshot = snapshotter.snapshot().into_vec();
    for (name, value, labels) in [
        (
            "server.request.completed",
            DebugValue::Counter(1),
            vec![("status", "ok"), ("transport", "http")],
        ),
        (
            "server.request.body.size",
            DebugValue::Histogram(vec![3.0.into()]),
            vec![("status", "ok"), ("transport", "http")],
        ),
        (
            "server.response.body.size",
            DebugValue::Histogram(vec![5.0.into()]),
            vec![("transport", "http")],
        ),
    ] {
        let matches: Vec<_> = snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .collect();
        assert_eq!(matches.len(), 1, "{name}");
        let (key, _, _, actual) = matches[0];
        assert_eq!(*actual, value, "{name}");
        let mut actual_labels: Vec<_> = key
            .key()
            .labels()
            .map(|label| (label.key(), label.value()))
            .collect();
        actual_labels.sort_unstable();
        assert_eq!(actual_labels, labels, "{name}");
    }
    assert!(snapshot.iter().all(|(key, _, _, _)| !matches!(
        key.key().name(),
        "server.request.duration" | "server.request.started"
    )));
}
