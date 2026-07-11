use bytes::BytesMut;
use nacelle::{
    NacelleError, NacelleRequest, NacelleResponse, NacelleTlsConfig, TcpServer, handler_fn,
};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8443".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;

    let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])?;
    let server = TcpServer::<LengthDelimitedProtocol>::builder()
        .protocol(LengthDelimitedProtocol)
        .handler(handler_fn(|mut request: NacelleRequest| async move {
            let opcode = request.tcp_opcode().unwrap_or_default();
            let mut echoed = BytesMut::new();
            while let Some(chunk) = request.body.next_chunk().await {
                echoed.extend_from_slice(&chunk?);
            }
            if opcode != 1 {
                return Err(NacelleError::handler(std::io::Error::other(format!(
                    "unknown opcode {}",
                    opcode
                ))));
            }
            Ok(NacelleResponse::tcp_bytes(echoed.freeze()))
        }))
        .build()?;

    println!("nacelle TLS echo server listening on {addr}");
    server.serve_tcp_tls(addr, generated.tls_config).await
}
