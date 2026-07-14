# Serve TCP with self-signed TLS

Use `tls-self-signed` for local load tests and auto-deploy flows that need a
certificate immediately. It implies the `rustls` provider.

```rust
use nacelle::core::pipeline::handler_fn;
use nacelle::core::NacelleError;
use nacelle::rustls::NacelleTlsConfig;
use nacelle::tcp::{TcpRequestContext, TcpResponse, TcpServer};
use nacelle_reference_protocol::LengthDelimitedProtocol;

let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])?;
let server = TcpServer::<LengthDelimitedProtocol>::builder()
    .protocol(LengthDelimitedProtocol)
    .handler(handler_fn(
        |mut context: TcpRequestContext<LengthDelimitedProtocol>| async move {
        let mut response = Vec::new();
        while let Some(chunk) = context.request_mut().body.next_chunk().await {
            response.extend_from_slice(&chunk?);
        }
        context.respond(TcpResponse::bytes(response)).await
    }))
    .build()?;

server
    .serve_tcp_tls("127.0.0.1:8443".parse()?, generated.tls_config)
    .await?;
# Ok::<(), NacelleError>(())
```

Self-signed certificates are for local and automated test flows. Public edge
deployments should use managed certificate material and a documented rotation
process.

For OpenSSL-backed TCP TLS, enable `openssl` and use
`NacelleOpenSslConfig::from_pem_files(...)` with `serve_tcp_openssl(...)`. Use
`openssl-vendored` only when the build machine has the tooling needed to compile
OpenSSL from source. The `openssl` feature enables provider-neutral `tls`
without selecting Rustls.
