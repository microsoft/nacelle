use bytes::BytesMut;
use nacelle::core::pipeline::handler_fn;
use nacelle::prelude::*;
use nacelle::tcp::{TcpRequestContext, TcpResponse};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let protocols = NacelleProtocols::new().tcp("echo", addr, LengthDelimitedProtocol);
    let app = NacelleApp::new(handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
            let opcode = context.request().head.opcode;
            let mut echoed = BytesMut::new();
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                echoed.extend_from_slice(&chunk?);
            }
            if opcode != 1 {
                return Err(NacelleError::handler(std::io::Error::other(format!(
                    "unknown opcode {}",
                    opcode
                ))));
            }
            context.respond(TcpResponse::bytes(echoed.freeze())).await
        },
    ))
    .with_ctrl_c_shutdown();

    println!("nacelle echo server listening on {addr}");
    app.serve(protocols).await
}
