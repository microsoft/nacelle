//! Typed streaming application pipelines across TCP and HTTP transports.
//!
//! Use [`core::pipeline`] for static handler composition, `tcp` for typed TCP
//! protocols, `http` for HTTP/1, and [`NacelleApp`] to compose listeners with
//! shared limits, telemetry, and shutdown.
//!
//! Production deployments should configure [`core::NacelleLimits`] explicitly and
//! attach [`core::NacelleTelemetry`] to expose low-cardinality lifecycle, request,
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

mod app;
mod host;
mod thread_per_core;
pub mod runtime {
    pub use crate::app::NacelleApp;
    pub use crate::host::NacelleHost;
    #[cfg(all(feature = "http", feature = "rustls"))]
    pub use crate::thread_per_core::run_local_http_tls_thread_per_core;
    #[cfg(all(feature = "tcp", feature = "openssl"))]
    pub use crate::thread_per_core::run_local_tcp_openssl_thread_per_core;
    #[cfg(all(feature = "tcp", feature = "rustls"))]
    pub use crate::thread_per_core::run_local_tcp_tls_thread_per_core;
    #[cfg(feature = "http")]
    pub use crate::thread_per_core::{LocalHttpRuntimeConfig, run_local_http_thread_per_core};
    #[cfg(feature = "tcp")]
    pub use crate::thread_per_core::{
        LocalTcpRuntimeConfig, run_local_serial_tcp_thread_per_core, run_local_tcp_thread_per_core,
    };
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
pub mod prelude {
    pub use crate::NacelleApp;
    pub use nacelle_core::{NacelleBody, NacelleError};
}
