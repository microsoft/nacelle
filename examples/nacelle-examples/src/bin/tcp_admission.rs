//! Admit opcode 1 using shared application capacity; reject other heads.
//! Waiting does not read a body, and timeout cancellation removes the waiter.

use std::sync::Arc;
use std::time::Duration;

use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{Admission, AdmissionConfig, AdmissionRejection, NacelleError};
use nacelle::tcp::{
    NacelleTcpConfig, TcpAdmissionContext, TcpHeadReadLimits, TcpRequestContext, TcpResponse,
    TcpServer,
};
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

fn server(
    timeout: Duration,
) -> Result<
    TcpServer<
        LengthDelimitedProtocol,
        impl nacelle::tcp::TcpService<LengthDelimitedProtocol, AppState>,
    >,
    NacelleError,
> {
    TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .tcp_config(NacelleTcpConfig::default().with_head_read_limits(
            TcpHeadReadLimits::new(24, 4096).map_err(NacelleError::protocol)?,
        ))
        .handler(handler_fn(
            |mut context: TcpRequestContext<LengthDelimitedProtocol, AppState>| async move {
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    let _ = chunk?;
                }
                context.respond(TcpResponse::bytes("admitted")).await
            },
        ))
        .admission(Capacity, AdmissionConfig::new(timeout, 64)?)?
        .build()
}

#[tokio::main]
async fn main() -> Result<(), NacelleError> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .map_err(NacelleError::protocol)?;
    NacelleApp::with_state(AppState {
        capacity: Arc::new(Semaphore::new(8)),
    })
    .with_ctrl_c_shutdown()
    .tcp("admission", address, server(Duration::from_secs(1))?)
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn timeout_removes_waiter_and_later_request_acquires_capacity() {
        let capacity = Arc::new(Semaphore::new(0));
        let server = server(Duration::from_millis(10))
            .unwrap()
            .with_app_state(Arc::new(AppState {
                capacity: capacity.clone(),
            }));
        for expected_timeout in [true, false] {
            let request = LengthDelimitedProtocol
                .encode_request_frame(1, 1, 0, b"")
                .unwrap();
            let (mut client, io) = tokio::io::duplex(1024);
            let peer = async {
                client.write_all(&request).await.unwrap();
                client.shutdown().await.unwrap();
                let mut bytes = Vec::new();
                client.read_to_end(&mut bytes).await.unwrap();
                assert!(!bytes.is_empty());
            };
            let (result, ()) = tokio::join!(server.serve_io(io), peer);
            if expected_timeout {
                assert!(matches!(
                    result,
                    Err(NacelleError::Timeout(
                        nacelle::core::NacelleTimeoutReason::Admission
                    ))
                ));
                capacity.add_permits(1);
            } else {
                result.unwrap();
                assert_eq!(capacity.available_permits(), 1);
            }
        }
    }
}
