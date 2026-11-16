//! Typed capacity admission and tracked TCP control in one small server.
//! Ctrl-C pauses connections, reclaims idle work, then forces active work closed.

use std::sync::Arc;
use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{
    Admission, AdmissionConfig, AdmissionRejection, NacelleConnectionTracking, NacelleError,
    NacelleLimits, NacelleRuntimeState, NacelleShutdown,
};
use nacelle::tcp::{TcpAdmissionContext, TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct AppState {
    capacity: Arc<Semaphore>,
}

struct Capacity;

impl<'a> Admission<TcpAdmissionContext<'a, LengthDelimitedProtocol, AppState>> for Capacity {
    type Permit = OwnedSemaphorePermit;

    async fn admit(
        &self,
        context: TcpAdmissionContext<'a, LengthDelimitedProtocol, AppState>,
    ) -> Result<Self::Permit, AdmissionRejection> {
        if context.head().opcode != 1 {
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
    .with_shutdown(shutdown.clone());
    let control = app.runtime_control();
    control.set_max_connections(Some(CAPACITY))?;
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(
            |mut context: TcpRequestContext<LengthDelimitedProtocol, AppState>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    chunk?;
                }
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
        .admission(Capacity, AdmissionConfig::new(Duration::from_secs(1), 64)?)?
        .build()?;
    let serving = app.tcp("controlled", address, server).run();
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            control.set_max_connections(Some(0))?;
            println!("Idle selection: {:?}", control.reclaim_idle(CAPACITY));
            for snapshot in control.connections(CAPACITY).unwrap_or_default() {
                println!("Forced selection: {:?}", control.close_connection(snapshot.connection.connection_id));
            }
            shutdown.shutdown();
            serving.await
        }
    }
}
