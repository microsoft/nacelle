//! Typed capacity admission and tracked HTTP control in one small server.
//! Ctrl-C pauses connections and requests deadline-bounded HTTP retirement.

use std::sync::Arc;
use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{
    Admission, AdmissionConfig, AdmissionRejection, NacelleConnectionTracking, NacelleError,
    NacelleLimits, NacelleRuntimeState, NacelleShutdown,
};
use nacelle::http::{
    HttpAdmissionContext, HttpRequestContext, HttpResponse, HyperServer, StatusCode,
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
    const CAPACITY: usize = 128;
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .map_err(NacelleError::protocol)?;
    let runtime_state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_connection_tracking(NacelleConnectionTracking::new(CAPACITY)?),
    );
    let shutdown = NacelleShutdown::new();
    let app = NacelleApp::with_state(AppState {
        capacity: Arc::new(Semaphore::new(8)),
    })
    .with_runtime_state(runtime_state)?
    .with_shutdown(shutdown.clone())
    .with_shutdown_drain_timeout(Duration::from_secs(6));
    let control = app.runtime_control();
    control.set_max_connections(Some(CAPACITY))?;
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<(), AppState>| async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                chunk?;
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        },
    ))
    .with_admission(Capacity, AdmissionConfig::new(Duration::from_secs(1), 64)?);
    let serving = app.http("controlled-http", address, server).run();
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            control.set_max_connections(Some(0))?;
            println!("Idle capability: {:?}", control.reclaim_idle(CAPACITY));
            for snapshot in control.connections(CAPACITY).unwrap_or_default() {
                println!("Retirement selection: {:?}", control.retire_connection(
                    snapshot.connection.connection_id, Duration::from_secs(5),
                ));
            }
            shutdown.shutdown();
            serving.await
        }
    }
}
