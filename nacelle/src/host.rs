#[cfg(any(feature = "tcp", feature = "http"))]
use std::net::SocketAddr;
#[cfg(all(feature = "tcp", unix))]
use std::path::Path;

use tokio::task::JoinSet;

use nacelle_core::error::NacelleError;
use nacelle_core::lifecycle::{NacelleDrainDeadline, NacelleShutdown, NacelleShutdownToken};
use nacelle_core::limits::{NacelleLimits, NacelleRuntimeState};
#[cfg(any(feature = "tcp", feature = "http"))]
use nacelle_core::telemetry::NacelleTransport;
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver, NoopObserver};
#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(all(any(feature = "tcp", feature = "http"), feature = "rustls"))]
use nacelle_core::tls::NacelleTlsConfig;
#[cfg(all(feature = "tcp", feature = "openssl"))]
use nacelle_tcp::NacelleTlsDetectionOptions;
#[cfg(all(feature = "tcp", unix))]
use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
use nacelle_tcp::{NacelleTcpBindOptions, NacelleTcpOptions};

pub struct NacelleHost<Observer = NoopObserver> {
    telemetry: NacelleTelemetry<Observer>,
    runtime_state: NacelleRuntimeState,
    shutdown: NacelleShutdown,
    drain_deadline: NacelleDrainDeadline,
    tasks: JoinSet<Result<(), NacelleError>>,
}

impl Default for NacelleHost<NoopObserver> {
    fn default() -> Self {
        Self::new()
    }
}

impl NacelleHost<NoopObserver> {
    pub fn new() -> Self {
        Self {
            telemetry: NacelleTelemetry::default(),
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }
}

impl<Observer> NacelleHost<Observer>
where
    Observer: NacelleTelemetryObserver,
{
    /// Create a host with concrete process-wide telemetry.
    pub fn with_telemetry(telemetry: NacelleTelemetry<Observer>) -> Self {
        Self {
            telemetry,
            runtime_state: NacelleRuntimeState::default(),
            shutdown: NacelleShutdown::new(),
            drain_deadline: NacelleDrainDeadline::default(),
            tasks: JoinSet::new(),
        }
    }

    pub fn with_limits(mut self, limits: NacelleLimits) -> Self {
        self.runtime_state = NacelleRuntimeState::new(limits);
        self
    }

    pub fn with_runtime_state(mut self, runtime_state: NacelleRuntimeState) -> Self {
        self.runtime_state = runtime_state;
        self
    }

    pub fn shutdown_token(&self) -> NacelleShutdownToken {
        self.shutdown.token()
    }

    pub fn with_shutdown(mut self, shutdown: NacelleShutdown) -> Self {
        self.shutdown = shutdown;
        self
    }

    pub fn shutdown(&self) {
        self.telemetry.shutdown_requested();
        self.shutdown.shutdown();
    }

    pub fn with_shutdown_drain_timeout(self, drain_timeout: std::time::Duration) -> Self {
        self.drain_deadline.set(drain_timeout);
        self
    }

    #[cfg(feature = "tcp")]
    pub fn enable_tcp<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    pub fn enable_serial_tcp<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_serial_tcp_with_bind_options(
            name,
            addr,
            NacelleTcpBindOptions::default(),
            server,
        )
    }

    #[cfg(feature = "tcp")]
    pub fn enable_serial_tcp_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::SerialTcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::Protocol,
        P::ConnectionState: Send,
        H: nacelle_tcp::SerialTcpHandler<P>,
        OH: nacelle_tcp::SerialTcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_serial_tcp_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    bind_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    pub fn enable_tcp_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        tcp_options: NacelleTcpOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tcp_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(feature = "tcp")]
    pub fn enable_tcp_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_with_bind_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                bind_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    pub fn enable_unix_socket<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        let path_label = path.display().to_string();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("unix_socket"), &name, &path_label);
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_unix_with_shutdown_deadline(
                std::sync::Arc::new(server),
                path,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("unix_socket"),
                    &name,
                    &path_label,
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", unix))]
    pub fn enable_unix_socket_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let path = path.as_ref().to_path_buf();
        let path_label = path.display().to_string();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("unix_socket"), &name, &path_label);
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_unix_with_options_and_shutdown_deadline(
                std::sync::Arc::new(server),
                path,
                unix_options,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("unix_socket"),
                    &name,
                    &path_label,
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "rustls"))]
    pub fn enable_tcp_tls<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = nacelle_tcp::runtime::serve_tcp_tls_with_shutdown_deadline(
                std::sync::Arc::new(server),
                addr,
                tls_config,
                shutdown,
                drain_deadline,
            )
            .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_openssl_with_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_openssl_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    tls_config,
                    bind_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("tcp"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_optional_openssl<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_optional_openssl_with_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpOptions::default(),
            NacelleTlsDetectionOptions::default(),
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_optional_openssl_with_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        self.enable_tcp_optional_openssl_with_bind_options(
            name,
            addr,
            server,
            tls_config,
            NacelleTcpBindOptions::from(tcp_options),
            detection_options,
        )
    }

    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub fn enable_tcp_optional_openssl_with_bind_options<P, H, OH, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_tcp::TcpServer<P, H, OH, ServerObserver>,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> &mut Self
    where
        P: nacelle_tcp::SharedProtocol,
        H: nacelle_tcp::TcpHandler<P>,
        OH: nacelle_tcp::TcpOneWayHandler<P>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("tcp"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result =
                nacelle_tcp::runtime::serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
                    std::sync::Arc::new(server),
                    addr,
                    tls_config,
                    bind_options,
                    detection_options,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(NacelleTransport::new("tcp"), &name, &addr.to_string(), error);
            }
            result
        });
        self
    }

    #[cfg(feature = "http")]
    pub fn enable_http<H, F, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
    ) -> &mut Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("http"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let result = server
                .serve_with_shutdown_deadline(addr, shutdown, drain_deadline)
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("http"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    #[cfg(all(feature = "http", feature = "rustls"))]
    pub fn enable_http_tls<H, F, ServerObserver>(
        &mut self,
        name: impl Into<String>,
        addr: SocketAddr,
        server: nacelle_http::HyperServer<H, F, ServerObserver>,
        tls_config: NacelleTlsConfig,
    ) -> &mut Self
    where
        F: nacelle_http::HttpConnectionStateFactory,
        H: nacelle_http::HttpHandler<F::State>,
        ServerObserver: NacelleTelemetryObserver,
    {
        let name = name.into();
        let telemetry = self.telemetry.clone();
        let shutdown = self.shutdown.token();
        let drain_deadline = self.drain_deadline.clone();
        let server = server
            .with_runtime_context(self.telemetry.clone(), self.runtime_state.clone())
            .with_listener_label(name.clone());
        telemetry.listener_configured(NacelleTransport::new("http"), &name, &addr.to_string());
        self.tasks.spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let result = server
                .serve_tls_listener_with_shutdown_deadline(
                    listener,
                    tls_config,
                    shutdown,
                    drain_deadline,
                )
                .await;
            if let Err(error) = &result {
                telemetry.listener_failed(
                    NacelleTransport::new("http"),
                    &name,
                    &addr.to_string(),
                    error,
                );
            }
            result
        });
        self
    }

    pub async fn wait(mut self) -> Result<(), NacelleError> {
        let mut first_error = None;
        while let Some(result) = self.tasks.join_next().await {
            let error = match result {
                Ok(Ok(())) => continue,
                Ok(Err(error)) => error,
                Err(error) => NacelleError::from(error),
            };
            if first_error.is_none() {
                self.telemetry.shutdown_requested();
                self.shutdown.shutdown();
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn shutdown_and_wait(self) -> Result<(), NacelleError> {
        self.shutdown_and_wait_timeout(std::time::Duration::from_secs(30))
            .await
    }

    pub async fn shutdown_and_wait_timeout(
        mut self,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        self.drain_deadline.set(drain_timeout);
        self.telemetry.shutdown_requested();
        self.shutdown.shutdown();
        while let Some(result) = self.tasks.join_next().await {
            result??;
        }

        let drain = async {
            while self.runtime_state.active_connections() != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        };
        tokio::time::timeout(drain_timeout, drain)
            .await
            .map_err(|_| NacelleError::Timeout("shutdown_drain"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    #[tokio::test]
    async fn listener_failure_requests_shutdown_and_drains_remaining_tasks() {
        let mut host = NacelleHost::new();
        let mut shutdown = host.shutdown_token();
        let (drained_tx, drained_rx) = oneshot::channel();

        host.tasks
            .spawn(async { Err(NacelleError::ResourceLimit("test_listener_failure")) });
        host.tasks.spawn(async move {
            assert!(shutdown.changed().await);
            drained_tx.send(()).expect("drain observer should be open");
            Ok(())
        });

        let error = host.wait().await.expect_err("listener failure should win");

        assert!(matches!(
            error,
            NacelleError::ResourceLimit("test_listener_failure")
        ));
        drained_rx
            .await
            .expect("remaining listener should observe shutdown and drain");
    }
}
