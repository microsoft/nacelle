use std::io;

use bytes::BytesMut;
use nacelle::NacelleApp;
use nacelle::core::NacelleError;
use nacelle::core::pipeline::handler_fn;
use nacelle::openssl::NacelleOpenSslConfig;
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), NacelleError> {
    let mut args = std::env::args().skip(1);
    let certificate_path = required_arg(args.next(), "certificate PEM path")?;
    let private_key_path = required_arg(args.next(), "private-key PEM path")?;
    let addr: std::net::SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8443".to_string())
        .parse()
        .map_err(NacelleError::protocol)?;
    let tls_config = NacelleOpenSslConfig::from_pem_files(certificate_path, private_key_path)?;
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

    println!("OpenSSL echo server listening on {addr}");
    NacelleApp::new()
        .with_ctrl_c_shutdown()
        .tcp_openssl("openssl-echo", addr, server, tls_config)
        .run()
        .await
}

fn required_arg(value: Option<String>, name: &'static str) -> Result<String, NacelleError> {
    value.ok_or_else(|| {
        NacelleError::handler(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing {name}"),
        ))
    })
}
