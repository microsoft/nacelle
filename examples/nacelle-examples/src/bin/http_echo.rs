use bytes::BytesMut;
use http::StatusCode;
use nacelle::{HyperServer, NacelleError, NacelleRequest, NacelleResponse, handler_fn};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let server = HyperServer::new(handler_fn(|mut request: NacelleRequest| async move {
        let mut echoed = BytesMut::new();
        while let Some(chunk) = request.body.next_chunk().await {
            echoed.extend_from_slice(&chunk?);
        }
        Ok(NacelleResponse::http_bytes(StatusCode::OK, echoed.freeze()))
    }));

    println!("nacelle HTTP echo server listening on {addr}");
    server.serve(addr).await
}
