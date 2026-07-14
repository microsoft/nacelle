//! Proxy application composition, lifecycle, and Nacelle runtime assembly.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{ConfigurationSnapshot, initial_tls_config, load_snapshot};
use crate::reload::{ReloadPolicy, watch_configuration};
use crate::wire::ProxyProtocol;
use crate::{ProxyError, ProxyService};
use nacelle::NacelleApp;
use nacelle::core::pipeline::handler_fn;
use nacelle::core::{NacelleError, NacelleLimits, NacelleShutdown, NacelleTlsConfig};
use nacelle::tcp::{NacelleTcpLimits, TcpRequestContext, TcpServer};

const DEFAULT_CONFIG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml");

/// Configured proxy application and its runtime-reloadable state.
#[derive(Debug)]
pub struct ProxyApp {
    config_path: PathBuf,
    initial: ConfigurationSnapshot,
    tls_config: NacelleTlsConfig,
    service: Arc<ProxyService>,
}

impl ProxyApp {
    /// Load the proxy from the first command-line argument or the example path.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or TLS material cannot be loaded.
    pub async fn from_env() -> Result<Self, ProxyError> {
        let config_path = std::env::args()
            .nth(1)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        Self::from_config_file(config_path).await
    }

    /// Load the proxy from a TOML configuration file.
    ///
    /// # Errors
    ///
    /// Returns an error when configuration or TLS material cannot be loaded.
    pub async fn from_config_file(config_path: impl Into<PathBuf>) -> Result<Self, ProxyError> {
        let config_path = config_path.into();
        let initial = load_snapshot(&config_path).await?;
        let tls_config = initial_tls_config(&initial)?;
        let service = Arc::new(ProxyService::new(
            initial.file.runtime_configuration(),
            tls_config.clone(),
        )?);

        Ok(Self {
            config_path,
            initial,
            tls_config,
            service,
        })
    }

    /// Run the configured proxy until shutdown or listener failure.
    ///
    /// # Errors
    ///
    /// Returns a Nacelle runtime, listener, handler, or watcher task error.
    pub async fn run(self) -> Result<(), ProxyError> {
        let startup = self.initial.file.startup_configuration();
        let listen_addr = startup.listen_addr;
        let handler = handler_fn({
            let service = self.service.clone();
            move |context: TcpRequestContext<ProxyProtocol>| {
                let service = service.clone();
                async move { service.handle(context).await.map_err(NacelleError::handler) }
            }
        });
        let server = TcpServer::<ProxyProtocol>::builder()
            .protocol(ProxyProtocol)
            .handler(handler)
            .tcp_limits(
                NacelleTcpLimits::default()
                    .with_read_timeout(Duration::from_secs(30))
                    .with_write_timeout(Duration::from_secs(30)),
            )
            .build()
            .map_err(ProxyError::runtime)?;

        let shutdown = NacelleShutdown::new();
        let reload_policy = ReloadPolicy::new(startup);
        let watcher = tokio::spawn(watch_configuration(
            self.config_path,
            self.initial,
            reload_policy,
            self.service,
            shutdown.token(),
        ));

        println!("nacelle proxy listening on {listen_addr}");
        let app = NacelleApp::new()
            .with_limits(
                NacelleLimits::default()
                    .with_max_connections(startup.max_connections)
                    .with_max_in_flight_requests(startup.max_in_flight_requests)
                    .with_max_connections_per_peer(startup.max_connections_per_peer)
                    .with_max_memory_bytes(startup.max_memory_bytes)
                    .with_max_request_body_bytes(startup.max_request_body_bytes)
                    .with_max_response_body_bytes(startup.max_response_body_bytes)
                    .with_handler_timeout(startup.handler_timeout),
            )
            .with_shutdown(shutdown.clone())
            .with_ctrl_c_shutdown()
            .tcp_tls("proxy", listen_addr, server, self.tls_config)
            .run();
        supervise_application(app, watcher, shutdown).await
    }
}

async fn supervise_application(
    app: impl Future<Output = Result<(), NacelleError>>,
    mut watcher: tokio::task::JoinHandle<()>,
    shutdown: NacelleShutdown,
) -> Result<(), ProxyError> {
    tokio::pin!(app);
    tokio::select! {
        app_result = &mut app => {
            shutdown.shutdown();
            let watcher_result = watcher.await;
            match (app_result, watcher_result) {
                (Err(error), _) => Err(ProxyError::runtime(error)),
                (Ok(()), Err(error)) => Err(ProxyError::task(error)),
                (Ok(()), Ok(())) => Ok(()),
            }
        }
        watcher_result = &mut watcher => {
            shutdown.shutdown();
            match watcher_result {
                Ok(()) => app.await.map_err(ProxyError::runtime),
                Err(error) => {
                    drop(app.await);
                    Err(ProxyError::task(error))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProxyErrorKind;

    #[tokio::test]
    async fn watcher_failure_shuts_down_the_application() {
        let shutdown = NacelleShutdown::new();
        let mut shutdown_token = shutdown.token();
        let app = async move {
            shutdown_token.changed().await;
            Ok(())
        };
        let watcher = tokio::spawn(async { panic!("watcher failed") });

        let error = supervise_application(app, watcher, shutdown)
            .await
            .expect_err("watcher panic should fail the application");

        assert_eq!(error.kind(), ProxyErrorKind::Task);
    }
}
