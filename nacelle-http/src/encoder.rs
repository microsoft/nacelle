//! HTTP body conversion between Hyper's `Incoming`/`BoxBody` and Nacelle's
//! `NacelleBody`, plus response serialization with memory accounting.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::body::{Frame, Incoming};
use hyper::{Response, StatusCode};

use crate::limits::NacelleHttpLimits;
use crate::policy::{NacelleHttpPolicy, apply_security_headers};
use nacelle_core::error::{BoxError, NacelleError};
use nacelle_core::limits::NacelleRuntimeState;
use nacelle_core::request::NacelleBody;
use nacelle_core::response::{NacelleResponse, NacelleResponseMeta};
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTransport};

pub(crate) type HttpBody = BoxBody<Bytes, BoxError>;

pub(crate) fn incoming_to_body(
    mut incoming: Incoming,
    body_len_hint: Option<usize>,
    request_body_bytes: Arc<AtomicUsize>,
    runtime_state: NacelleRuntimeState,
    http_limits: NacelleHttpLimits,
    telemetry: NacelleTelemetry,
) -> NacelleBody {
    let (tx, body) = NacelleBody::channel(8);
    tokio::spawn(async move {
        if let Some(body_len_hint) = body_len_hint
            && body_len_hint > runtime_state.limits().max_request_body_bytes
        {
            let _ = tx
                .send(Err(NacelleError::ResourceLimit("request_body_bytes")))
                .await;
            return;
        }
        let _body_allocation = match body_len_hint {
            Some(bytes) => match runtime_state
                .allocate_memory_with_timeout(
                    bytes,
                    runtime_state.limits().memory_allocation_timeout,
                )
                .await
            {
                Ok(allocation) => Some(allocation),
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            },
            None => None,
        };
        let _streaming_permit = match runtime_state.acquire_streaming_task_tracked() {
            Ok(permit) => permit,
            Err(error) => {
                let _ = tx.send(Err(error)).await;
                return;
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
                            let _ = tx.send(Err(NacelleError::Timeout("http_body_read"))).await;
                            break;
                        }
                    }
                } else {
                    next.await
                }
            };
            let Some(frame) = frame else {
                break;
            };
            match frame {
                Ok(frame) => {
                    if let Some(data) = frame.data_ref() {
                        let Some(next) = body_bytes.checked_add(data.len()) else {
                            let _ = tx
                                .send(Err(NacelleError::ResourceLimit("request_body_bytes")))
                                .await;
                            break;
                        };
                        if next > runtime_state.limits().max_request_body_bytes {
                            let _ = tx
                                .send(Err(NacelleError::ResourceLimit("request_body_bytes")))
                                .await;
                            break;
                        }
                        body_bytes = next;
                        request_body_bytes.fetch_add(data.len(), Ordering::Relaxed);
                        if tx.send(Ok(data.clone())).await.is_err() {
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
    });
    body
}

pub(crate) fn response_to_http(
    response: NacelleResponse,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry,
    policy: &NacelleHttpPolicy,
) -> Result<Response<HttpBody>, NacelleError> {
    let (status, headers) = match response.meta {
        NacelleResponseMeta::Http(meta) => (meta.status, meta.headers),
        NacelleResponseMeta::Tcp(_) => (StatusCode::OK, http::HeaderMap::new()),
    };

    let mut builder = Response::builder().status(status);
    let Some(builder_headers) = builder.headers_mut() else {
        return Err(NacelleError::protocol("failed to build response headers"));
    };
    *builder_headers = headers;
    apply_security_headers(builder_headers, policy);
    builder
        .body(nacelle_body_to_http(
            response.body,
            runtime_state,
            telemetry,
        ))
        .map_err(NacelleError::protocol)
}

fn nacelle_body_to_http(
    body: NacelleBody,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry,
) -> HttpBody {
    StreamBody::new(HttpBodyStream {
        body,
        runtime_state,
        telemetry,
        response_body_bytes: 0,
    })
    .map_err(|error| -> BoxError { Box::new(error) })
    .boxed()
}

struct HttpBodyStream {
    body: NacelleBody,
    runtime_state: NacelleRuntimeState,
    telemetry: NacelleTelemetry,
    response_body_bytes: usize,
}

impl Stream for HttpBodyStream {
    type Item = Result<Frame<Bytes>, NacelleError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.body).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let Some(next) = this.response_body_bytes.checked_add(chunk.len()) else {
                    return Poll::Ready(Some(Err(NacelleError::ResourceLimit(
                        "response_body_bytes",
                    ))));
                };
                if next > this.runtime_state.limits().max_response_body_bytes {
                    return Poll::Ready(Some(Err(NacelleError::ResourceLimit(
                        "response_body_bytes",
                    ))));
                }
                this.response_body_bytes = next;
                Poll::Ready(Some(Ok(Frame::data(chunk))))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for HttpBodyStream {
    fn drop(&mut self) {
        self.telemetry
            .response_body_bytes(NacelleTransport::new("http"), self.response_body_bytes);
    }
}

#[allow(dead_code)]
fn empty_body() -> HttpBody {
    Full::new(Bytes::new())
        .map_err(|never: Infallible| match never {})
        .boxed()
}
