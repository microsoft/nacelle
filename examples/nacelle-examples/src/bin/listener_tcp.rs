use std::sync::Arc;

use bytes::BytesMut;
use nacelle::core::pipeline::handler_fn;
use nacelle::prelude::*;
use nacelle::tcp::runtime::serve_tcp;
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr: std::net::SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(
            |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
                let mut echoed = BytesMut::new();
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    echoed.extend_from_slice(&chunk?);
                }
                context.respond(TcpResponse::bytes(echoed.freeze())).await
            },
        ))
        .build()?;

    println!("listener-helper TCP echo server listening on {addr}");
    serve_tcp(Arc::new(server), addr).await
}
