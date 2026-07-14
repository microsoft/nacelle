use bytes::BytesMut;
use http::StatusCode;
use nacelle::NacelleApp;
use nacelle::core::NacelleError;
use nacelle::core::pipeline::handler_fn;
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};
use nacelle::rustls::NacelleTlsConfig;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8443".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])
        .map_err(NacelleError::protocol)?;
    println!("nacelle HTTPS echo server listening on {addr}");
    println!("self-signed certificate:\n{}", generated.certificate_pem);

    let server = HyperServer::new(handler_fn(
        |mut context: HttpRequestContext<()>| async move {
            let mut echoed = BytesMut::new();
            while let Some(chunk) = context.request_mut().next_body_chunk().await {
                echoed.extend_from_slice(&chunk?);
            }
            context
                .respond(HttpResponse::bytes(StatusCode::OK, echoed.freeze()))
                .await
        },
    ));

    NacelleApp::new()
        .with_ctrl_c_shutdown()
        .http_tls("https-echo", addr, server, generated.tls_config)
        .run()
        .await
}
