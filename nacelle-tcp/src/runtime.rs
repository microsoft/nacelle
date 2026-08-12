//! Tokio TCP listener helpers.
//!
//! Visible functions in this module own their listener or bind address and an
//! `Arc`-backed server for the lifetime of the returned future. Each accepted
//! connection runs in a supervised task. Variants without a shutdown token run
//! until listener failure or future cancellation. Token-aware variants stop
//! accepting when shutdown is requested, drain active tasks for the configured
//! timeout, and then abort remaining tasks. Dropping a listener future does not
//! guarantee that drain sequence.
//!
//! Listener overloads preserve the process-wide limits in the server's
//! [`nacelle_core::NacelleRuntimeState`] and the transport limits in
//! [`crate::NacelleTcpLimits`]. Plain TCP requires the facade's `tcp` feature;
//! Unix-domain listeners are Unix-only; Rustls requires `rustls`; and OpenSSL
//! requires `openssl`. Worker-local functions participate in the experimental
//! thread-per-core execution model. Plaintext/OpenSSL detection additionally
//! requires `experimental-openssl-detection` and is outside the supported `0.3`
//! contract.
//!
//! # Errors
//!
//! Listener futures return [`nacelle_core::NacelleError`] for bind, accept,
//! socket-option, TLS configuration/handshake, timeout, resource-limit, and
//! shutdown failures. Connection-local failures are reported through telemetry
//! and do not normally stop the listener.
//!
//! # Panics
//!
//! Shared listener futures must be polled inside a Tokio runtime. Worker-local
//! listeners additionally require a [`tokio::task::LocalSet`]. Nacelle does not
//! intentionally panic for peer or configuration input.
//!
//! # Example
//!
//! Run the visible plain-TCP listener helper directly with:
//!
//! ```text
//! cargo run -p nacelle-examples --bin listener_tcp
//! ```
//!
//! Unix and OpenSSL variants have provider-specific runnable targets:
//!
//! ```text
//! cargo run -p nacelle-examples --bin unix_echo
//! cargo run -p nacelle-examples --bin openssl_echo --no-default-features --features openssl -- cert.pem key.pem
//! ```

mod common;
mod local;
#[cfg(feature = "openssl")]
mod openssl;
#[cfg(feature = "experimental-openssl-detection")]
mod openssl_optional;
#[cfg(feature = "rustls")]
mod rustls;
mod tcp;
#[cfg(unix)]
mod unix;

#[cfg(all(test, feature = "openssl"))]
mod openssl_tests;
#[cfg(all(test, feature = "tls-self-signed"))]
mod rustls_tests;

pub use local::*;
#[cfg(feature = "openssl")]
pub use openssl::*;
#[cfg(feature = "experimental-openssl-detection")]
pub use openssl_optional::*;
#[cfg(feature = "rustls")]
pub use rustls::*;
pub use tcp::*;
#[cfg(unix)]
pub use unix::*;
