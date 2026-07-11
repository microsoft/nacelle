use std::sync::Arc;

use bytes::BytesMut;
use http::StatusCode;
use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{NacelleError, NacelleTelemetry};
use nacelle::http::{HttpRequestContext, HttpResponse, HyperServer};
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[derive(Debug)]
struct AppState {
    response_prefix: &'static [u8],
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

    let app = Arc::new(AppState {
        response_prefix: b"",
    });
    let telemetry = NacelleTelemetry::default();
    let tcp_handler = handler_fn({
        let app = app.clone();
        move |mut context: TcpRequestContext<LengthDelimitedProtocol>| {
            let app = app.clone();
            async move {
                let opcode = context.request().head.opcode;
                if opcode != 1 {
                    while let Some(chunk) = context.request_mut().body.next_chunk().await {
                        let _ = chunk?;
                    }
                    return Err(NacelleError::handler(std::io::Error::other(format!(
                        "unknown opcode {opcode}"
                    ))));
                }

                let mut echoed = BytesMut::new();
                echoed.extend_from_slice(app.response_prefix);
                while let Some(chunk) = context.request_mut().body.next_chunk().await {
                    echoed.extend_from_slice(&chunk?);
                }

                context.respond(TcpResponse::bytes(echoed.freeze())).await
            }
        }
    });
    let http_handler = handler_fn({
        let app = app.clone();
        move |mut context: HttpRequestContext<()>| {
            let app = app.clone();
            async move {
                let mut echoed = BytesMut::new();
                echoed.extend_from_slice(app.response_prefix);
                while let Some(chunk) = context.request_mut().next_body_chunk().await {
                    echoed.extend_from_slice(&chunk?);
                }

                context
                    .respond(HttpResponse::bytes(StatusCode::OK, echoed.freeze()))
                    .await
            }
        }
    });

    let tcp_server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(tcp_handler)
        .build()?;
    let http_server = HyperServer::new(http_handler);

    println!("nacelle TCP echo listening on {tcp_addr}");
    println!("nacelle HTTP echo listening on {http_addr}");

    NacelleApp::with_telemetry(telemetry)
        .tcp("tcp-echo", tcp_addr, tcp_server)
        .http("http-echo", http_addr, http_server)
        .run()
        .await
}
