//! Streaming application handlers across TCP and HTTP transports.
//!
//! Nacelle centers application code around one async handler shape:
//!
//! ```rust,no_run
//! # use nacelle::{NacelleError, NacelleRequest, NacelleResponse};
//! async fn handle(request: NacelleRequest) -> Result<NacelleResponse, NacelleError> {
//!     Ok(NacelleResponse::tcp(request.body))
//! }
//! ```
//!
//! Use [`handler_fn`] for simple services, [`TcpServer`] for custom TCP
//! protocols, [`HyperServer`] for HTTP/1, and [`NacelleHost`] when one process
//! owns several listeners with shared limits.
//!
//! Production deployments should configure [`NacelleLimits`] explicitly and
//! attach [`NacelleTelemetry`] to expose low-cardinality lifecycle, request,
//! rejection, timeout, and byte-accounting events.
//!
//! Additional operational notes live in the repository `docs/` directory.

/// Framing, encoding, decoding, and asynchronous message I/O APIs.
pub use nacelle_codec as codec;
/// Transport-neutral request, response, handler, lifecycle, and limit APIs.
pub use nacelle_core as core;
/// HTTP transport APIs.
#[cfg(feature = "http")]
pub use nacelle_http as http;
/// TCP and Unix socket transport APIs.
#[cfg(feature = "tcp")]
pub use nacelle_tcp as tcp;

pub use nacelle_core::{config, error, handler, lifecycle, limits, request, response, telemetry};
#[cfg(feature = "tcp")]
pub use nacelle_tcp::{connection, protocol, server};

pub mod app;
pub mod host;
#[cfg(feature = "http")]
pub use nacelle_http::server as http_server;
pub mod runtime {
    pub use crate::app::{NacelleApp, NacelleProtocols, serve};
    pub use crate::host::NacelleHost;
    pub use nacelle_core::runtime::*;
    #[cfg(feature = "tcp")]
    pub use nacelle_tcp::runtime::{
        serve_tcp, serve_tcp_listener_with_options_and_shutdown_deadline,
        serve_tcp_listener_with_shutdown_deadline,
        serve_tcp_with_bind_options_and_shutdown_deadline, serve_tcp_with_options,
        serve_tcp_with_options_and_shutdown, serve_tcp_with_options_and_shutdown_deadline,
        serve_tcp_with_options_and_shutdown_timeout, serve_tcp_with_shutdown,
        serve_tcp_with_shutdown_deadline, serve_tcp_with_shutdown_timeout,
    };
    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub use nacelle_tcp::runtime::{
        serve_tcp_openssl, serve_tcp_openssl_listener_with_options_and_shutdown_deadline,
        serve_tcp_openssl_listener_with_shutdown_deadline,
        serve_tcp_openssl_with_bind_options_and_shutdown_deadline, serve_tcp_openssl_with_options,
        serve_tcp_openssl_with_options_and_shutdown,
        serve_tcp_openssl_with_options_and_shutdown_deadline,
        serve_tcp_openssl_with_options_and_shutdown_timeout, serve_tcp_openssl_with_shutdown,
        serve_tcp_openssl_with_shutdown_deadline, serve_tcp_openssl_with_shutdown_timeout,
        serve_tcp_optional_openssl,
        serve_tcp_optional_openssl_listener_with_options_and_shutdown_deadline,
        serve_tcp_optional_openssl_with_bind_options_and_shutdown_deadline,
        serve_tcp_optional_openssl_with_options,
        serve_tcp_optional_openssl_with_options_and_shutdown,
        serve_tcp_optional_openssl_with_options_and_shutdown_deadline,
        serve_tcp_optional_openssl_with_options_and_shutdown_timeout,
        serve_tcp_optional_openssl_with_shutdown, serve_tcp_optional_openssl_with_shutdown_timeout,
    };
    #[cfg(all(feature = "tcp", feature = "rustls"))]
    pub use nacelle_tcp::runtime::{
        serve_tcp_tls, serve_tcp_tls_listener_with_shutdown_deadline, serve_tcp_tls_with_shutdown,
        serve_tcp_tls_with_shutdown_deadline, serve_tcp_tls_with_shutdown_timeout,
    };
    #[cfg(all(feature = "tcp", unix))]
    pub use nacelle_tcp::runtime::{
        serve_unix, serve_unix_listener_with_shutdown_deadline, serve_unix_with_options,
        serve_unix_with_options_and_shutdown, serve_unix_with_options_and_shutdown_deadline,
        serve_unix_with_options_and_shutdown_timeout, serve_unix_with_shutdown,
        serve_unix_with_shutdown_deadline, serve_unix_with_shutdown_timeout,
    };
}
pub use app::{NacelleApp, NacelleProtocols, serve};
pub use host::NacelleHost;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub use nacelle_core::tls;
#[cfg(feature = "tower")]
pub use nacelle_core::tower;

pub mod prelude {
    pub use crate::{
        BoxError, Handler, HandlerFn, NacelleApp, NacelleBody, NacelleConfig,
        NacelleConnectionMeta, NacelleError, NacelleLimits, NacelleMemoryBudget, NacelleProtocols,
        NacelleRequest, NacelleRequestMeta, NacelleRequestMetricsConfig, NacelleResponse,
        NacelleResponseMeta, NacelleRuntimeState, NacelleShutdown, NacelleShutdownToken,
        NacelleTelemetry, NacelleTelemetryConfig, NacelleTransport, RequestBodyMode,
        RequestMetadata, handler_fn, serve,
    };
    #[cfg(feature = "tcp")]
    pub use crate::{
        DecodedRequest, MessageDecoder, NacelleTcpBindOptions, NacelleTcpLimits, NacelleTcpOptions,
        NacelleTlsDetectionOptions, Protocol, TcpRequestMeta, TcpResponseMeta, TcpServer,
    };
    #[cfg(feature = "http")]
    pub use crate::{
        HttpRequestMeta, HttpResponseMeta, HyperServer, NacelleHttpLimits, NacelleHttpPolicy,
    };
}

#[cfg(feature = "tls-self-signed")]
pub use nacelle_core::NacelleGeneratedTlsConfig;
#[cfg(feature = "openssl")]
pub use nacelle_core::NacelleOpenSslConfig;
#[cfg(feature = "rustls")]
pub use nacelle_core::NacelleTlsConfig;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub use nacelle_core::NacelleTlsProvider;
#[cfg(feature = "tower")]
pub use nacelle_core::handler_from_tower_service;
pub use nacelle_core::{
    BoxError, Handler, HandlerFn, NacelleBody, NacelleConfig, NacelleConnectionExtension,
    NacelleConnectionExtensionFactory, NacelleConnectionMeta, NacelleConnectionTlsMeta,
    NacelleError, NacelleInMemoryTelemetrySink, NacelleLimits, NacelleMemoryAllocation,
    NacelleMemoryBudget, NacelleMetricsContext, NacelleRequest, NacelleRequestMeta,
    NacelleRequestMetricsConfig, NacelleResponse, NacelleResponseMeta, NacelleRuntimeState,
    NacelleShutdown, NacelleShutdownToken, NacelleTelemetry, NacelleTelemetryConfig,
    NacelleTelemetryEvent, NacelleTelemetryEventKind, NacelleTelemetrySink, NacelleTransport,
    RequestBodyMode, RequestMetadata, TcpRequestMeta, TcpResponseMeta, TrackedPermit, handler_fn,
};
#[cfg(feature = "http")]
pub use nacelle_core::{HttpRequestMeta, HttpResponseMeta};
#[cfg(feature = "http")]
pub use nacelle_http::{HyperServer, NacelleHttpLimits, NacelleHttpPolicy};
#[cfg(all(feature = "tcp", unix))]
pub use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
pub use nacelle_tcp::{
    DecodedRequest, MessageDecoder, NacelleServer, NacelleServerBuilder, NacelleTcpBindOptions,
    NacelleTcpKeepalive, NacelleTcpLimits, NacelleTcpOptions, NacelleTlsDetectionOptions, Protocol,
    TcpServer, serve_connection, serve_stream,
};
