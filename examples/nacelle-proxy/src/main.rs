//! TLS TCP proxy example with file-driven configuration and certificate reloads.

use nacelle_proxy::{ProxyApp, ProxyError};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), ProxyError> {
    ProxyApp::from_env().await?.run().await
}
