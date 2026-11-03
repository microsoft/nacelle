//! Ctrl-C pauses admission, reclaims idle TCP connections, then cancels active work.
//! This demonstrates forced retirement, not graceful response delivery.

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{
    NacelleConnectionTracking, NacelleError, NacelleLimits, NacelleRuntimeState, NacelleShutdown,
};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

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
        .with_shutdown(shutdown.clone());
    let control = app.runtime_control();
    control.set_max_connections(Some(CAPACITY))?;
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(
            |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    chunk?;
                }
                context.respond(TcpResponse::bytes("ok")).await
            },
        ))
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
