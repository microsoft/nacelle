//! HTTP body conversion between Hyper's `Incoming` and Nacelle's
//! `NacelleBody`, plus response serialization.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use http_body_util::{BodyExt, StreamBody};
use hyper::Response;
use hyper::body::{Frame, Incoming};

use crate::limits::NacelleHttpLimits;
use crate::pipeline::HttpResponse;
use crate::policy::{NacelleHttpPolicy, apply_security_headers};
use nacelle_core::error::{NacelleError, NacelleResourceLimitReason, NacelleTimeoutReason};
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleBody;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NacelleTransport};

type RequestBodySender = tokio::sync::mpsc::Sender<Result<Bytes, NacelleError>>;

pub(crate) type HttpBody<Observer, Lease = ()> = StreamBody<HttpBodyStream<Observer, Lease>>;

/// The returned pump reports whether the incoming body reached EOF without failure.
pub(crate) fn incoming_to_body<Observer>(
    incoming: Incoming,
    body_len_hint: Option<usize>,
    request_body_bytes: &AtomicUsize,
    runtime_state: NacelleRuntimeState,
    http_limits: NacelleHttpLimits,
    telemetry: NacelleTelemetry<Observer>,
) -> (NacelleBody, impl Future<Output = bool>)
where
    Observer: NacelleTelemetryObserver,
{
    let (tx, body) = if incoming_body_is_empty(body_len_hint) {
        (None, NacelleBody::empty())
    } else {
        let (tx, body) = NacelleBody::channel_with_remaining(8, body_len_hint.unwrap_or_default());
        (Some(tx), body)
    };
    let pump = async move {
        if let Some(tx) = tx {
            pump_incoming_body(
                incoming,
                body_len_hint,
                request_body_bytes,
                runtime_state,
                http_limits,
                telemetry,
                tx,
            )
            .await
        } else {
            true
        }
    };
    (body, pump)
}

#[allow(clippy::too_many_arguments)]
async fn pump_incoming_body<Observer>(
    mut incoming: Incoming,
    body_len_hint: Option<usize>,
    request_body_bytes: &AtomicUsize,
    runtime_state: NacelleRuntimeState,
    http_limits: NacelleHttpLimits,
    telemetry: NacelleTelemetry<Observer>,
    tx: RequestBodySender,
) -> bool
where
    Observer: NacelleTelemetryObserver,
{
    if let Some(body_len_hint) = body_len_hint
        && body_len_hint > runtime_state.limits().max_request_body_bytes
    {
        let _ = tx
            .send(Err(NacelleError::ResourceLimit(
                NacelleResourceLimitReason::RequestBodyBytes,
            )))
            .await;
        return false;
    }
    let _streaming_permit = match runtime_state.acquire_streaming_task_tracked() {
        Ok(permit) => permit,
        Err(error) => {
            let _ = tx.send(Err(error)).await;
            return false;
        }
    };
    let mut body_bytes = 0_usize;
    loop {
        let frame = {
            let next = incoming.frame();
            if let Some(timeout) = http_limits.request_body_read_timeout {
                match tokio::time::timeout(timeout, next).await {
                    Ok(frame) => frame,
                    Err(_) => {
                        telemetry.timeout(NacelleTransport::new("http"), "http_body_read");
                        let _ = tx
                            .send(Err(NacelleError::Timeout(
                                NacelleTimeoutReason::HttpBodyRead,
                            )))
                            .await;
                        break;
                    }
                }
            } else {
                next.await
            }
        };
        let Some(frame) = frame else {
            return true;
        };
        match frame {
            Ok(frame) => {
                if let Some(data) = frame.data_ref() {
                    let Some(next) = body_bytes.checked_add(data.len()) else {
                        let _ = tx
                            .send(Err(NacelleError::ResourceLimit(
                                NacelleResourceLimitReason::RequestBodyBytes,
                            )))
                            .await;
                        break;
                    };
                    if next > runtime_state.limits().max_request_body_bytes {
                        let _ = tx
                            .send(Err(NacelleError::ResourceLimit(
                                NacelleResourceLimitReason::RequestBodyBytes,
                            )))
                            .await;
                        break;
                    }
                    body_bytes = next;
                    request_body_bytes.fetch_add(data.len(), Ordering::Relaxed);
                    let data = data.clone();
                    if tx.send(Ok(data)).await.is_err() {
                        break;
                    }
                }
            }
            Err(error) => {
                let _ = tx.send(Err(NacelleError::protocol(error))).await;
                break;
            }
        }
    }
    false
}

const fn incoming_body_is_empty(body_len_hint: Option<usize>) -> bool {
    matches!(body_len_hint, Some(0))
}

#[cfg(test)]
pub(crate) fn response_to_http<Observer>(
    response: HttpResponse,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
    policy: &NacelleHttpPolicy,
) -> Result<Response<HttpBody<Observer>>, NacelleError>
where
    Observer: NacelleTelemetryObserver,
{
    response_to_http_with_lease(response, runtime_state, telemetry, policy, None::<()>)
}

pub(crate) fn response_to_http_with_lease<Observer, Lease>(
    response: HttpResponse,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
    policy: &NacelleHttpPolicy,
    lease: Option<Lease>,
) -> Result<Response<HttpBody<Observer, Lease>>, NacelleError>
where
    Observer: NacelleTelemetryObserver,
{
    let mut builder = Response::builder().status(response.status);
    let Some(builder_headers) = builder.headers_mut() else {
        return Err(NacelleError::protocol("failed to build response headers"));
    };
    *builder_headers = response.headers;
    apply_security_headers(builder_headers, policy);
    builder
        .body(nacelle_body_to_http(
            response.body,
            runtime_state,
            telemetry,
            lease,
        ))
        .map_err(NacelleError::protocol)
}

fn nacelle_body_to_http<Observer, Lease>(
    body: NacelleBody,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
    lease: Option<Lease>,
) -> HttpBody<Observer, Lease>
where
    Observer: NacelleTelemetryObserver,
{
    StreamBody::new(HttpBodyStream {
        body,
        runtime_state,
        telemetry,
        response_body_bytes: 0,
        response_body_metric_pending: true,
        lease,
    })
}

pub(crate) struct HttpBodyStream<Observer: NacelleTelemetryObserver, Lease = ()> {
    body: NacelleBody,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry<Observer>,
    response_body_bytes: usize,
    response_body_metric_pending: bool,
    lease: Option<Lease>,
}

// Only the Unpin body is polled through Pin; the owned lease is never pinned.
impl<Observer: NacelleTelemetryObserver, Lease> Unpin for HttpBodyStream<Observer, Lease> {}

impl<Observer, Lease> Stream for HttpBodyStream<Observer, Lease>
where
    Observer: NacelleTelemetryObserver,
{
    type Item = Result<Frame<Bytes>, NacelleError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.body).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let Some(next) = this.response_body_bytes.checked_add(chunk.len()) else {
                    this.response_body_metric_pending = false;
                    this.lease.take();
                    return Poll::Ready(Some(Err(NacelleError::ResourceLimit(
                        NacelleResourceLimitReason::ResponseBodyBytes,
                    ))));
                };
                if next > this.runtime_state.limits().max_response_body_bytes {
                    this.response_body_metric_pending = false;
                    this.lease.take();
                    return Poll::Ready(Some(Err(NacelleError::ResourceLimit(
                        NacelleResourceLimitReason::ResponseBodyBytes,
                    ))));
                }
                this.response_body_bytes = next;
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Some(Err(error))) => {
                this.response_body_metric_pending = false;
                this.lease.take();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.lease.take();
                if this.response_body_metric_pending {
                    this.response_body_metric_pending = false;
                    this.telemetry.response_body_bytes(
                        NacelleTransport::new("http"),
                        this.response_body_bytes,
                    );
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use http::StatusCode;
    use http_body_util::BodyExt;
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{incoming_body_is_empty, response_to_http};
    use crate::pipeline::HttpResponse;
    use crate::policy::NacelleHttpPolicy;
    use nacelle_core::error::{NacelleError, NacelleResourceLimitReason};
    use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
    use nacelle_core::telemetry::{
        NacelleInMemoryObserver, NacelleTelemetry, NacelleTelemetryEventKind,
    };

    struct LeaseDrop(Rc<Cell<usize>>);

    impl Drop for LeaseDrop {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[tokio::test]
    async fn response_lease_ends_at_eof_error_or_drop_without_send_bound() {
        for outcome in ["eof", "error", "drop"] {
            let drops = Rc::new(Cell::new(0));
            let (sender, body) = nacelle_core::NacelleBody::channel(1);
            let response = super::response_to_http_with_lease(
                HttpResponse::new(http::StatusCode::OK, http::HeaderMap::new(), body),
                NacelleRuntimeState::default(),
                NacelleTelemetry::default(),
                &NacelleHttpPolicy::default(),
                Some(nacelle_core::policy::Hold::new(LeaseDrop(drops.clone()))),
            )
            .expect("response");
            let mut body = response.into_body();
            if outcome == "eof" {
                sender
                    .send(Ok(bytes::Bytes::from_static(b"data")))
                    .await
                    .expect("chunk");
                drop(sender);
                assert!(body.frame().await.expect("frame").is_ok());
                assert_eq!(drops.get(), 0);
                assert!(body.frame().await.is_none());
            } else if outcome == "error" {
                sender
                    .send(Err(NacelleError::ConnectionClosed))
                    .await
                    .expect("error");
                assert!(body.frame().await.expect("frame").is_err());
            } else {
                drop(body);
            }
            assert_eq!(drops.get(), 1, "{outcome}");
        }
    }

    #[test]
    fn only_exact_zero_body_hint_skips_body_pump() {
        assert!(incoming_body_is_empty(Some(0)));
        assert!(!incoming_body_is_empty(Some(1)));
        assert!(!incoming_body_is_empty(None));
    }

    #[tokio::test]
    async fn oversized_response_chunk_fails_before_emitting_bytes() {
        let runtime_state =
            NacelleRuntimeState::new(NacelleLimits::default().with_max_response_body_bytes(4));
        let observer = NacelleInMemoryObserver::new();
        let response = response_to_http(
            HttpResponse::bytes(StatusCode::OK, "hello"),
            runtime_state,
            NacelleTelemetry::new().with_observer(observer.clone()),
            &NacelleHttpPolicy::default(),
        )
        .expect("response should build");
        let mut body = response.into_body();

        let error = body
            .frame()
            .await
            .expect("response should yield an error frame")
            .expect_err("oversized response should fail before emitting bytes");
        assert!(matches!(
            error,
            NacelleError::ResourceLimit(NacelleResourceLimitReason::ResponseBodyBytes)
        ));
        assert!(body.frame().await.is_none());
        assert!(
            observer
                .events()
                .iter()
                .all(|event| event.kind != NacelleTelemetryEventKind::ResponseBodyBytes)
        );
    }

    #[tokio::test]
    async fn completed_empty_response_records_zero_bytes_once() {
        let observer = NacelleInMemoryObserver::new();
        let response = response_to_http(
            HttpResponse::empty(StatusCode::NO_CONTENT),
            NacelleRuntimeState::default(),
            NacelleTelemetry::new().with_observer(observer.clone()),
            &NacelleHttpPolicy::default(),
        )
        .expect("response should build");

        response
            .into_body()
            .collect()
            .await
            .expect("empty response should complete");

        let events = observer.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == NacelleTelemetryEventKind::ResponseBodyBytes)
                .map(|event| event.count)
                .collect::<Vec<_>>(),
            vec![0]
        );
    }

    #[tokio::test]
    async fn cancelled_response_does_not_record_partial_bytes() {
        let observer = NacelleInMemoryObserver::new();
        let response = response_to_http(
            HttpResponse::bytes(StatusCode::OK, "partial"),
            NacelleRuntimeState::default(),
            NacelleTelemetry::new().with_observer(observer.clone()),
            &NacelleHttpPolicy::default(),
        )
        .expect("response should build");
        let mut body = response.into_body();

        body.frame()
            .await
            .expect("response should have one frame")
            .expect("response frame should succeed");
        drop(body);

        assert!(
            observer
                .events()
                .iter()
                .all(|event| event.kind != NacelleTelemetryEventKind::ResponseBodyBytes)
        );
    }
}
