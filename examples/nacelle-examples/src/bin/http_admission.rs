//! Route admission using the same typed application state as the handler.
//! Unadmitted bodies are not polled, including `Expect: 100-continue` requests.

use std::sync::Arc;
use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{Admission, AdmissionConfig, AdmissionRejection, NacelleError};
use nacelle::http::{
    HttpAdmissionContext, HttpParserLimits, HttpRequestContext, HttpResponse, HyperServer,
    NacelleHttpLimits, StatusCode,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct AppState {
    capacity: Arc<Semaphore>,
}

struct Capacity;

impl<'a> Admission<HttpAdmissionContext<'a, (), AppState>> for Capacity {
    type Permit = OwnedSemaphorePermit;

    async fn admit(
        &self,
        context: HttpAdmissionContext<'a, (), AppState>,
    ) -> Result<Self::Permit, AdmissionRejection> {
        if context.head().uri.path() != "/work" {
            return Err(AdmissionRejection::Forbidden);
        }
        context
            .app_state()
            .capacity
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionRejection::Unavailable)
    }
}

#[tokio::main]
async fn main() -> Result<(), NacelleError> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .map_err(NacelleError::protocol)?;
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<(), AppState>| async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                chunk?;
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "admitted"))
                .await
        },
    ))
    .with_admission(Capacity, AdmissionConfig::new(Duration::from_secs(1), 64)?)
    .with_http_limits(NacelleHttpLimits::default().with_parser_limits(
        HttpParserLimits::new(16 * 1024, None).map_err(NacelleError::protocol)?,
    ));
    NacelleApp::with_state(AppState {
        capacity: Arc::new(Semaphore::new(8)),
    })
    .with_ctrl_c_shutdown()
    .http("admission", address, server)
    .run()
    .await
}
