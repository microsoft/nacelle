use bytes::BytesMut;
use nacelle::core::pipeline::handler_fn;
use nacelle::prelude::*;
use nacelle::tcp::{NacelleUnixSocketOptions, TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/nacelle-echo.sock".to_string());
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

    println!("Unix echo server listening on {path}");
    server
        .serve_unix_with_options(
            path,
            NacelleUnixSocketOptions::new().with_unlink_stale_path(true),
        )
        .await
}
