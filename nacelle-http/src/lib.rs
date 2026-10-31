//! HTTP transport for Nacelle.
//!
//! [`HyperServer::with_admission`] installs one concrete application policy
//! before body-channel allocation or `Incoming` polling. A pending policy does
//! not trigger `100 Continue`; rejected unread bodies close HTTP/1.
//! [`HttpParserLimits`] configures Hyper's parser strategy, not an exact socket
//! read allowance. Response production owns the policy permit independently
//! of the existing runtime request gauge and response-byte metrics.

pub mod admission;
mod encoder;
pub mod limits;
pub mod pipeline;
mod policy;
mod rate_limit;
pub mod server;

pub use admission::{HttpAdmissionContext, HttpRequestHead, LocalHttpAdmissionContext};
#[doc(hidden)]
pub use admission::{HttpService, LocalHttpService};
pub use http::StatusCode;
pub use limits::{HttpParserLimits, HttpParserLimitsError, NacelleHttpLimits};
pub use pipeline::{
    HttpCompletion, HttpConnectionStateFactory, HttpHandler, HttpHandlerCompletion, HttpRequest,
    HttpRequestContext, HttpResponder, HttpResponse, LocalHttpConnectionStateFactory,
    LocalHttpHandler, LocalHttpRequestContext, NoHttpConnectionState,
};
pub use server::{
    HyperServer, LocalHttpSharedState, LocalHyperServer, NacelleForwardedHeader, NacelleHttpPolicy,
};
