//! Ctrl-C pauses admission and requests deadline-bounded HTTP retirement.
//! Idle reclaim is unsupported; deadline expiry cancels remaining delivery.

use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{
    NacelleConnectionTracking, NacelleError, NacelleLimits, NacelleRuntimeState, NacelleShutdown,
};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer, StatusCode};

#[tokio::main]
async fn main() -> Result<(), NacelleError> {
    const CAPACITY: usize = 128;
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .map_err(NacelleError::protocol)?;
    let state = NacelleRuntimeState::new(
        NacelleLimits::default()
            .with_connection_tracking(NacelleConnectionTracking::new(CAPACITY)?),
    );
    let shutdown = NacelleShutdown::new();
    let app = NacelleApp::new()
        .with_runtime_state(state)?
        .with_shutdown(shutdown.clone())
        .with_shutdown_drain_timeout(Duration::from_secs(6));
    let control = app.runtime_control();
    control.set_max_connections(Some(CAPACITY))?;
    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                chunk?;
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, "ok"))
                .await
        },
    ));
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
