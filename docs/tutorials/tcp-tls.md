# Serve TCP with self-signed TLS

Use `tls-self-signed` for local load tests and auto-deploy flows that need a
certificate immediately. It implies the `rustls` provider.

```rust
use nacelle::{
    NacelleTlsConfig, TcpServer, handler_fn,
};
use nacelle_reference_protocol::{FrameRequest, LengthDelimitedProtocol};

let generated = NacelleTlsConfig::self_signed(["localhost", "127.0.0.1"])?;
let server = TcpServer::<FrameRequest, ()>::builder()
    .protocol(LengthDelimitedProtocol)
    .handler(handler_fn(|request| async move {
        Ok(nacelle::NacelleResponse::tcp(request.body))
    }))
    .build()?;

server
    .serve_tcp_tls("127.0.0.1:8443".parse()?, generated.tls_config)
    .await?;
# Ok::<(), nacelle::NacelleError>(())
```

Self-signed certificates are for local and automated test flows. Public edge
deployments should use managed certificate material and a documented rotation
process.

For OpenSSL-backed TCP TLS, enable `openssl` and use
`NacelleOpenSslConfig::from_pem_files(...)` with `serve_tcp_openssl(...)`. Use
`openssl-vendored` only when the build machine has the tooling needed to compile
OpenSSL from source. The `openssl` feature enables provider-neutral `tls`
without selecting Rustls.
