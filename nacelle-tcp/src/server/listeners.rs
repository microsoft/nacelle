use std::net::SocketAddr;
#[cfg(unix)]
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "openssl")]
use crate::options::NacelleTlsDetectionOptions;
#[cfg(unix)]
use crate::options::NacelleUnixSocketOptions;
use crate::options::{NacelleTcpBindOptions, NacelleTcpOptions};
use crate::protocol::{SharedProtocol, TcpHandler, TcpOneWayHandler};
use nacelle_core::error::NacelleError;
use nacelle_core::telemetry::NacelleTelemetryObserver;
#[cfg(feature = "openssl")]
use nacelle_core::tls::NacelleOpenSslConfig;
#[cfg(feature = "rustls")]
use nacelle_core::tls::NacelleTlsConfig;

use super::TcpServer;

impl<P, H, OH, Observer> TcpServer<P, H, OH, Observer>
where
    P: SharedProtocol,
    H: TcpHandler<P>,
    OH: TcpOneWayHandler<P>,
    Observer: NacelleTelemetryObserver,
{
    pub async fn serve_tcp(&self, addr: SocketAddr) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
        )
        .await
    }

    pub async fn serve_tcp_with_shutdown(
        &self,
        addr: SocketAddr,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            shutdown,
        )
        .await
    }

    pub async fn serve_tcp_with_shutdown_timeout(
        &self,
        addr: SocketAddr,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            shutdown,
            drain_timeout,
        )
        .await
    }

    pub async fn serve_tcp_with_options(
        &self,
        addr: SocketAddr,
        tcp_options: NacelleTcpOptions,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_options(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tcp_options,
        )
        .await
    }

    pub async fn serve_tcp_with_options_and_shutdown(
        &self,
        addr: SocketAddr,
        tcp_options: NacelleTcpOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_options_and_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tcp_options,
            shutdown,
        )
        .await
    }

    pub async fn serve_tcp_with_options_and_shutdown_timeout(
        &self,
        addr: SocketAddr,
        tcp_options: NacelleTcpOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_options_and_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tcp_options,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[doc(hidden)]
    pub async fn serve_tcp_with_bind_options_and_shutdown_deadline(
        &self,
        addr: SocketAddr,
        bind_options: NacelleTcpBindOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_deadline: nacelle_core::lifecycle::NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_with_bind_options_and_shutdown_deadline(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            bind_options,
            shutdown,
            drain_deadline,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix(&self, path: impl AsRef<Path>) -> Result<(), NacelleError> {
        crate::runtime::serve_unix(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix_with_shutdown(
        &self,
        path: impl AsRef<Path>,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_unix_with_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
            shutdown,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix_with_shutdown_timeout(
        &self,
        path: impl AsRef<Path>,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_unix_with_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix_with_options(
        &self,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_unix_with_options(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
            unix_options,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix_with_options_and_shutdown(
        &self,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_unix_with_options_and_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
            unix_options,
            shutdown,
        )
        .await
    }

    #[cfg(unix)]
    pub async fn serve_unix_with_options_and_shutdown_timeout(
        &self,
        path: impl AsRef<Path>,
        unix_options: NacelleUnixSocketOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_unix_with_options_and_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            path,
            unix_options,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tcp_tls(
        &self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_tls(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
        )
        .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tcp_tls_with_shutdown(
        &self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_tls_with_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            shutdown,
        )
        .await
    }

    #[cfg(feature = "rustls")]
    pub async fn serve_tcp_tls_with_shutdown_timeout(
        &self,
        addr: SocketAddr,
        tls_config: NacelleTlsConfig,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_tls_with_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl_with_shutdown(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            shutdown,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl_with_shutdown_timeout(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl_with_options(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_options(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            tcp_options,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl_with_options_and_shutdown(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_options_and_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            tcp_options,
            shutdown,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_openssl_with_options_and_shutdown_timeout(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_options_and_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            tcp_options,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    #[doc(hidden)]
    pub async fn serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_deadline: nacelle_core::lifecycle::NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_openssl_with_bind_options_and_shutdown_deadline(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            bind_options,
            shutdown,
            drain_deadline,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_optional_openssl(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_optional_openssl(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_optional_openssl_with_shutdown(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_optional_openssl_with_shutdown(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            shutdown,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_optional_openssl_with_options(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_optional_openssl_with_options(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            tcp_options,
            detection_options,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    pub async fn serve_tcp_optional_openssl_with_options_and_shutdown_timeout(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        tcp_options: NacelleTcpOptions,
        detection_options: NacelleTlsDetectionOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_timeout: std::time::Duration,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_optional_openssl_with_options_and_shutdown_timeout(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            tcp_options,
            detection_options,
            shutdown,
            drain_timeout,
        )
        .await
    }

    #[cfg(feature = "openssl")]
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub async fn serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
        &self,
        addr: SocketAddr,
        tls_config: NacelleOpenSslConfig,
        bind_options: NacelleTcpBindOptions,
        detection_options: NacelleTlsDetectionOptions,
        shutdown: nacelle_core::lifecycle::NacelleShutdownToken,
        drain_deadline: nacelle_core::lifecycle::NacelleDrainDeadline,
    ) -> Result<(), NacelleError> {
        crate::runtime::serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline(
            Arc::<TcpServer<P, H, OH, Observer>>::new(self.clone()),
            addr,
            tls_config,
            bind_options,
            detection_options,
            shutdown,
            drain_deadline,
        )
        .await
    }
}
