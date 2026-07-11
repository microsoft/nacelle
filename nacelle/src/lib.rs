//! Typed streaming application pipelines across TCP and HTTP transports.
//!
//! Use [`core::pipeline`] for static handler composition, [`tcp`] for typed TCP
//! protocols, [`http`] for HTTP/1, and [`NacelleApp`] to compose listeners with
//! shared limits, telemetry, and shutdown.
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

pub use nacelle_core::{error, lifecycle, limits, pipeline, request, telemetry};
#[cfg(feature = "tcp")]
pub use nacelle_tcp::{connection, protocol, server};

pub mod app;
pub mod host;
pub mod thread_per_core;
#[cfg(feature = "http")]
pub use nacelle_http::server as http_server;
pub mod runtime {
    pub use crate::app::NacelleApp;
    pub use crate::host::NacelleHost;
    #[cfg(feature = "http")]
    pub use crate::thread_per_core::{LocalHttpRuntimeConfig, run_local_http_thread_per_core};
    #[cfg(feature = "tcp")]
    pub use crate::thread_per_core::{LocalTcpRuntimeConfig, run_local_tcp_thread_per_core};
    pub use crate::thread_per_core::{
        RuntimeMode, ThreadPerCoreConfig, ThreadPerCoreLimits, Worker, WorkerContext, WorkerSet,
        bind_reuse_port_listener, run_thread_per_core, run_thread_per_core_with_shutdown,
    };
    pub use nacelle_core::{NacelleShutdown, NacelleShutdownToken};
}

/// Low-level executor and transport runtime integration.
pub mod advanced {
    pub mod runtime {
        pub use nacelle_core::runtime::*;
        #[cfg(feature = "tcp")]
        pub use nacelle_tcp::runtime::*;
    }
}
pub use app::NacelleApp;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub use nacelle_core::tls;
pub mod prelude {
    pub use crate::{NacelleApp, NacelleBody, NacelleError};
}

#[cfg(feature = "tls-self-signed")]
pub use nacelle_core::NacelleGeneratedTlsConfig;
#[cfg(feature = "openssl")]
pub use nacelle_core::NacelleOpenSslConfig;
#[cfg(feature = "rustls")]
pub use nacelle_core::NacelleTlsConfig;
#[cfg(any(feature = "tls", feature = "openssl"))]
pub use nacelle_core::NacelleTlsProvider;
pub use nacelle_core::{
    BoxError, NacelleBody, NacelleConnectionMeta, NacelleConnectionTlsMeta, NacelleError,
    NacelleInMemoryTelemetrySink, NacelleLimits, NacelleMemoryAllocation, NacelleMemoryBudget,
    NacelleMetricsContext, NacelleRequestMetricsConfig, NacelleRuntimeState, NacelleShutdown,
    NacelleShutdownToken, NacelleTelemetry, NacelleTelemetryConfig, NacelleTelemetryEvent,
    NacelleTelemetryEventKind, NacelleTelemetrySink, NacelleTransport, TrackedPermit,
};
#[cfg(feature = "http")]
pub use nacelle_http::{HyperServer, NacelleHttpLimits, NacelleHttpPolicy};
#[cfg(all(feature = "tcp", unix))]
pub use nacelle_tcp::NacelleUnixSocketOptions;
#[cfg(feature = "tcp")]
pub use nacelle_tcp::{
    DecodedRequest, MessageDecoder, NacelleServer, NacelleServerBuilder, NacelleTcpBindOptions,
    NacelleTcpConfig, NacelleTcpKeepalive, NacelleTcpLimits, NacelleTcpOptions,
    NacelleTlsDetectionOptions, Protocol, TcpRequestBodyMode, TcpServer, serve_connection,
    serve_stream,
};
