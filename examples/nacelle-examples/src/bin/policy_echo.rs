use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::policy::{
    Admission, Hold, Rejection, RejectionKind, RequestRate, bounded_wait, check_fn, map_input,
};
use nacelle::core::{NacelleError, NacellePeerRateLimiter};
use nacelle::http::{HttpPolicies, HttpRequestContext, HttpRequestHead, HttpResponse, HyperServer};
use nacelle::tcp::policies::{TcpHead, TcpPolicies};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct BackendCapacity(Arc<Semaphore>);

impl<Input: ?Sized + Send> Admission<Input> for BackendCapacity {
    type Lease = Hold<OwnedSemaphorePermit>;

    async fn enter<'a>(&'a self, _: &'a mut Input) -> Result<Self::Lease, Rejection> {
        self.0
            .clone()
            .acquire_owned()
            .await
            .map(Hold::new)
            .map_err(|_| Rejection::new(RejectionKind::Closed, "backend_closed"))
    }
}

fn tcp_peer(input: &TcpHead<'_, FrameRequest, Arc<()>>) -> Option<IpAddr> {
    input.connection().peer_ip
}

fn http_peer(input: &HttpRequestHead<'_>) -> Option<IpAddr> {
    input.peer_ip()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let tcp_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    let http_addr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:8081".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let rate = RequestRate::new(Arc::new(NacellePeerRateLimiter::new(16_384)), 100);
    let backend = Arc::new(bounded_wait(
        BackendCapacity(Arc::new(Semaphore::new(32))),
        Duration::from_millis(100),
        128,
    ));
    let tcp_policies = TcpPolicies::new()
        .on_request_head(check_fn(|input: &TcpHead<'_, FrameRequest, Arc<()>>| {
            if input.request().opcode == 1 {
                Ok(())
            } else {
                Err(Rejection::new(RejectionKind::Denied, "unknown_opcode"))
            }
        }))
        .on_request_head(map_input(tcp_peer, rate.clone()))
        .around_handler(backend.clone());
    let http_policies = HttpPolicies::new()
        .on_request_head(map_input(http_peer, rate))
        .around_handler(backend);

    let tcp_handler = handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
            let mut body = BytesMut::new();
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                body.extend_from_slice(&chunk?);
            }
            context.respond(TcpResponse::bytes(body.freeze())).await
        },
    );
    let http_handler = handler_fn(|mut context: HttpRequestContext<()>| async move {
        let mut body = BytesMut::new();
        while let Some(chunk) = context.request_mut().next_body_chunk().await {
            body.extend_from_slice(&chunk?);
        }
        context
            .respond(HttpResponse::bytes(http::StatusCode::OK, body.freeze()))
            .await
    });
    let tcp = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(tcp_handler)
        .policies(tcp_policies)
        .build()?;
    let http = HyperServer::new(http_handler).with_policies(http_policies);

    println!("policy TCP echo listening on {tcp_addr}");
    println!("policy HTTP echo listening on {http_addr}");
    NacelleApp::new()
        .with_ctrl_c_shutdown()
        .tcp("tcp-policy-echo", tcp_addr, tcp)
        .http("http-policy-echo", http_addr, http)
        .run()
        .await
}
