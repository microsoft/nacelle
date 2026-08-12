use bytes::BytesMut;
use nacelle::core::pipeline::handler_fn;
use nacelle::prelude::*;
use nacelle::runtime::NacelleHost;
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let handler = handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
            let mut echoed = BytesMut::new();
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                echoed.extend_from_slice(&chunk?);
            }
            context.respond(TcpResponse::bytes(echoed.freeze())).await
        },
    );
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler)
        .build()?;

    let mut host = NacelleHost::new();
    host.enable_tcp("manual-echo", addr, server);
    println!("manual-host echo server listening on {addr}");
    host.wait().await
}
