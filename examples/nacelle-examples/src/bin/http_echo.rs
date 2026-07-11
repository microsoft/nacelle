use bytes::BytesMut;
use http::StatusCode;
use nacelle::NacelleApp;
use nacelle::core::NacelleError;
use nacelle::core::pipeline::handler_fn;
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            let mut echoed = BytesMut::new();
            while let Some(chunk) = context.request_mut().body.next_chunk().await {
                echoed.extend_from_slice(&chunk?);
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, echoed.freeze()))
                .await
        },
    ));

    println!("nacelle HTTP echo server listening on {addr}");
    NacelleApp::new()
        .with_ctrl_c_shutdown()
        .http("http-echo", addr, server)
        .run()
        .await
}
