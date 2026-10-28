//! Statically dispatched, server-owned HTTP admission checkpoints.
//!
//! These controls supplement [`crate::NacelleHttpPolicy`] and runtime limits.
//! Hyper may read ahead before the decoded-head checkpoint. A head lease lasts
//! until response-body EOF, error, or drop, not socket flush or peer delivery.
//!
//! A checkpoint rejects policies for a different input type at registration:
//!
//! ```compile_fail
//! use nacelle_core::policy::check_fn;
//! use nacelle_http::policies::{HttpPolicies, HttpSocket};
//! let policies = HttpPolicies::new()
//!     .on_request_head(check_fn(|_: &HttpSocket| Ok(())));
//! nacelle_http::HyperServer::new(()).with_policies(policies);
//! ```
//!
//! State-dependent checks borrow the connection factory's state:
//!
//! ```
//! use nacelle_core::pipeline::ConnectionInfo;
//! use nacelle_core::policy::{check_fn, Rejection, RejectionKind};
//! use nacelle_http::{HttpConnectionStateFactory, HttpPolicies, HttpPolicySet, HttpRequestHead, HyperServer};
//! struct Factory;
//! impl HttpConnectionStateFactory for Factory {
//!     type State = bool;
//!     fn create(&self, _: &ConnectionInfo) -> bool { true }
//! }
//! fn configured_policies() -> impl HttpPolicySet<bool> {
//!     HttpPolicies::new().on_request_head(check_fn(
//!         |head: &HttpRequestHead<'_, bool>| {
//!             if *head.state() { Ok(()) }
//!             else { Err(Rejection::new(RejectionKind::Denied, "connection_state")) }
//!         },
//!     ))
//! }
//! let server = HyperServer::new(())
//!     .with_connection_state_factory(Factory)
//!     .with_policies(configured_policies());
//! ```
//!
//! A set expecting another factory state cannot be attached:
//!
//! ```compile_fail
//! use nacelle_core::policy::check_fn;
//! use nacelle_http::{HttpPolicies, HttpRequestHead, HyperServer};
//! let policies = HttpPolicies::new().on_request_head(check_fn(
//!     |_: &HttpRequestHead<'_, String>| Ok(()),
//! ));
//! HyperServer::new(()).with_policies(policies); // Default factory state is ().
//! ```

use std::future::Future;
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;

use http::{HeaderMap, Method, Uri};
use nacelle_core::pipeline::{ConnectionContext, ConnectionInfo};
use nacelle_core::policy::{
    Admission, Cancellation, Chain, Control, LocalAdmission, LocalSocketAdmission, NoControl,
    NoPolicy, PolicyLease, Rejection, SocketAdmission,
};
use nacelle_core::request::NacelleConnectionMeta;

use crate::HttpRequest;

/// Socket facts available synchronously before spawning a connection task.
pub struct HttpSocket {
    pub(crate) peer_addr: SocketAddr,
    pub(crate) local_addr: Option<SocketAddr>,
}

impl HttpSocket {
    /// Immediate socket peer, before trusted-proxy processing.
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Listener address, if available.
    pub const fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}

/// Connection facts at accepted-socket or transport-ready admission.
pub struct HttpConnection<'a> {
    pub(crate) meta: &'a NacelleConnectionMeta,
}

impl HttpConnection<'_> {
    /// Immutable connection metadata. TLS metadata is present only at ready.
    pub const fn connection(&self) -> &NacelleConnectionMeta {
        self.meta
    }
}

/// Validated HTTP head before Nacelle allocates or pumps the request body.
///
/// `State` is the factory's state type. The view exclusively borrows its actual
/// connection context; getters expose only read access. Shared serving uses
/// `Arc<State>` storage; [`LocalHttpRequestHead`] selects `Rc<State>`.
pub struct HttpRequestHead<'a, State = (), Storage = Arc<State>> {
    pub(crate) parts: &'a mut http::request::Parts,
    pub(crate) connection: &'a mut ConnectionContext<Storage>,
    pub(crate) peer_ip: Option<IpAddr>,
    pub(crate) body_len: Option<usize>,
    pub(crate) state_type: PhantomData<fn() -> State>,
}

/// Worker-local HTTP head borrowing the actual `Rc` connection state.
pub type LocalHttpRequestHead<'a, State = ()> = HttpRequestHead<'a, State, Rc<State>>;

impl<State, Storage: Deref<Target = State>> HttpRequestHead<'_, State, Storage> {
    /// Request method.
    pub const fn method(&self) -> &Method {
        &self.parts.method
    }

    /// Request URI.
    pub const fn uri(&self) -> &Uri {
        &self.parts.uri
    }

    /// Request headers.
    pub const fn headers(&self) -> &HeaderMap {
        &self.parts.headers
    }

    /// Effective identity after configured trusted-proxy processing.
    pub const fn peer_ip(&self) -> Option<IpAddr> {
        self.peer_ip
    }

    /// Exact or upper-bound body length reported by Hyper, when available.
    pub const fn body_len(&self) -> Option<usize> {
        self.body_len
    }

    /// Connection metadata.
    pub const fn connection(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    /// Read-only state created by this server's connection-state factory.
    pub fn state(&self) -> &State {
        self.connection.state.deref()
    }
}

/// Request facts immediately before invoking the application handler.
///
/// `State` is the factory's state type. Shared serving exclusively borrows the
/// `Arc<State>` context; [`LocalHttpHandlerInput`] selects the local `Rc` context.
pub struct HttpHandlerInput<'a, State = (), Storage = Arc<State>> {
    pub(crate) request: &'a mut HttpRequest,
    pub(crate) connection: &'a mut ConnectionContext<Storage>,
    pub(crate) state_type: PhantomData<fn() -> State>,
}

/// Worker-local handler facts borrowing the actual `Rc` connection state.
pub type LocalHttpHandlerInput<'a, State = ()> = HttpHandlerInput<'a, State, Rc<State>>;

impl<State, Storage: Deref<Target = State>> HttpHandlerInput<'_, State, Storage> {
    /// Read-only request facts; body consumption remains with the handler.
    pub const fn request(&self) -> &HttpRequest {
        self.request
    }

    /// Connection metadata.
    pub const fn connection(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    /// Read-only state created by this server's connection-state factory.
    pub fn state(&self) -> &State {
        self.connection.state.deref()
    }
}

/// HTTP checkpoint set. Registration appends in call order.
///
/// Policies are initialized by callers. Attaching a set does not change handler
/// or request-context types. The empty set is zero-sized and allocates nothing.
#[derive(Clone)]
pub struct HttpPolicies<
    S = NoPolicy,
    A = NoPolicy,
    R = NoPolicy,
    H = NoPolicy,
    E = NoPolicy,
    C = NoControl,
> {
    socket: S,
    accepted: A,
    ready: R,
    head: H,
    handler: E,
    control: C,
}

/// Default allocation-free HTTP policy set.
#[derive(Clone, Copy, Default)]
pub struct NoHttpPolicies;

/// Attached checkpoint set, including sets returned as opaque trait-bound types.
///
/// This inline marker preserves attach-once registration even when the supplied
/// set is [`NoHttpPolicies`]. It allocates no storage beyond the wrapped set.
#[derive(Clone)]
pub struct AttachedHttpPolicies<Policies>(pub(crate) Policies);

impl Default for HttpPolicies {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpPolicies {
    /// Construct an empty checkpoint set.
    pub const fn new() -> Self {
        Self {
            socket: NoPolicy,
            accepted: NoPolicy,
            ready: NoPolicy,
            head: NoPolicy,
            handler: NoPolicy,
            control: NoControl,
        }
    }
}

impl<S, A, R, H, E, C> HttpPolicies<S, A, R, H, E, C> {
    /// Append a synchronous pre-spawn socket gate.
    pub fn on_socket<P>(self, policy: P) -> HttpPolicies<Chain<S, P>, A, R, H, E, C>
    where
        P: SocketAdmission<HttpSocket>,
    {
        HttpPolicies {
            socket: Chain::new(self.socket, policy),
            accepted: self.accepted,
            ready: self.ready,
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append a worker-local synchronous pre-spawn socket gate.
    pub fn on_local_socket<P>(self, policy: P) -> HttpPolicies<Chain<S, P>, A, R, H, E, C>
    where
        P: LocalSocketAdmission<HttpSocket>,
    {
        HttpPolicies {
            socket: Chain::new(self.socket, policy),
            accepted: self.accepted,
            ready: self.ready,
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append connection admission before TLS negotiation.
    pub fn on_accepted<P>(self, policy: P) -> HttpPolicies<S, Chain<A, P>, R, H, E, C>
    where
        P: for<'a> Admission<HttpConnection<'a>>,
    {
        HttpPolicies {
            socket: self.socket,
            accepted: Chain::new(self.accepted, policy),
            ready: self.ready,
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append connection admission after TLS and before connection state.
    pub fn on_ready<P>(self, policy: P) -> HttpPolicies<S, A, Chain<R, P>, H, E, C>
    where
        P: for<'a> Admission<HttpConnection<'a>>,
    {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: Chain::new(self.ready, policy),
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append decoded-head admission before Nacelle body allocation.
    ///
    /// The input and state types are validated when attaching the set.
    pub fn on_request_head<P>(self, policy: P) -> HttpPolicies<S, A, R, Chain<H, P>, E, C> {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            head: Chain::new(self.head, policy),
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append admission whose lease lasts only for the handler future.
    ///
    /// The input and state types are validated when attaching the set.
    pub fn around_handler<P>(self, policy: P) -> HttpPolicies<S, A, R, H, Chain<E, P>, C> {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            head: self.head,
            handler: Chain::new(self.handler, policy),
            control: self.control,
        }
    }

    /// Append a worker-local accepted-socket admission.
    pub fn on_local_accepted<P>(self, policy: P) -> HttpPolicies<S, Chain<A, P>, R, H, E, C>
    where
        P: for<'a> LocalAdmission<HttpConnection<'a>>,
    {
        HttpPolicies {
            socket: self.socket,
            accepted: Chain::new(self.accepted, policy),
            ready: self.ready,
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append a worker-local transport-ready admission.
    pub fn on_local_ready<P>(self, policy: P) -> HttpPolicies<S, A, Chain<R, P>, H, E, C>
    where
        P: for<'a> LocalAdmission<HttpConnection<'a>>,
    {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: Chain::new(self.ready, policy),
            head: self.head,
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append a worker-local decoded-head admission.
    ///
    /// The input and state types are validated when attaching the set.
    pub fn on_local_request_head<P>(self, policy: P) -> HttpPolicies<S, A, R, Chain<H, P>, E, C> {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            head: Chain::new(self.head, policy),
            handler: self.handler,
            control: self.control,
        }
    }

    /// Append a worker-local handler-scoped admission.
    ///
    /// The input and state types are validated when attaching the set.
    pub fn around_local_handler<P>(self, policy: P) -> HttpPolicies<S, A, R, H, Chain<E, P>, C> {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            head: self.head,
            handler: Chain::new(self.handler, policy),
            control: self.control,
        }
    }
}

impl<S, A, R, H, E> HttpPolicies<S, A, R, H, E, NoControl> {
    /// Cancel the complete serving connection, including waits and body writes.
    ///
    /// Cancellation is opt-in and does not undo detached application work.
    pub fn with_cancellation(
        self,
        cancellation: Cancellation,
    ) -> HttpPolicies<S, A, R, H, E, Cancellation> {
        HttpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            head: self.head,
            handler: self.handler,
            control: cancellation,
        }
    }
}

/// Shared-runtime checkpoint dispatch contract used by HTTP servers.
pub trait HttpPolicySet<State = ()>: Send + Sync + 'static {
    /// Socket reservation.
    type SocketLease: PolicyLease + Send + 'static;
    /// Accepted connection reservation.
    type AcceptedLease: PolicyLease + Send + 'static;
    /// Ready connection reservation.
    type ReadyLease: PolicyLease + Send + 'static;
    /// Request/response-body reservation.
    type HeadLease: PolicyLease + Send + 'static;
    /// Handler-scoped reservation.
    type HandlerLease: PolicyLease + Send + 'static;
    /// Explicit connection cancellation capability.
    type Control: Control;

    /// Check the socket synchronously before spawning.
    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection>;
    /// Admit before TLS; retain the lease until the connection ends.
    fn accepted(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> + Send;
    /// Admit after TLS and before constructing connection state.
    fn ready(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + Send;
    /// Admit a validated head; retain the lease in the transport response body.
    fn head(
        &self,
        input: &mut HttpRequestHead<'_, State>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>> + Send;
    /// Admit immediately before dispatch; release when the handler ends.
    fn handler(
        &self,
        input: &mut HttpHandlerInput<'_, State>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> + Send;
    /// Borrow the explicit connection control capability.
    fn control(&self) -> &Self::Control;
}

impl<State, S, A, R, H, E, C, AL, RL, HL, EL> HttpPolicySet<State>
    for HttpPolicies<S, A, R, H, E, C>
where
    State: Send + Sync + 'static,
    S: SocketAdmission<HttpSocket> + 'static,
    A: for<'a> Admission<HttpConnection<'a>, Lease = AL> + 'static,
    R: for<'a> Admission<HttpConnection<'a>, Lease = RL> + 'static,
    H: for<'a> Admission<HttpRequestHead<'a, State>, Lease = HL> + 'static,
    E: for<'a> Admission<HttpHandlerInput<'a, State>, Lease = EL> + 'static,
    AL: PolicyLease + Send + 'static,
    RL: PolicyLease + Send + 'static,
    HL: PolicyLease + Send + 'static,
    EL: PolicyLease + Send + 'static,
    C: Control + 'static,
{
    type SocketLease = S::Lease;
    type AcceptedLease = AL;
    type ReadyLease = RL;
    type HeadLease = HL;
    type HandlerLease = EL;
    type Control = C;

    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection> {
        self.socket.admit(input)
    }
    async fn accepted(&self, input: &mut HttpConnection<'_>) -> Result<AL, Rejection> {
        self.accepted.enter(input).await
    }
    async fn ready(&self, input: &mut HttpConnection<'_>) -> Result<RL, Rejection> {
        self.ready.enter(input).await
    }
    async fn head(&self, input: &mut HttpRequestHead<'_, State>) -> Result<HL, Rejection> {
        self.head.enter(input).await
    }
    async fn handler(&self, input: &mut HttpHandlerInput<'_, State>) -> Result<EL, Rejection> {
        self.handler.enter(input).await
    }
    fn control(&self) -> &C {
        &self.control
    }
}

/// Worker-local checkpoint dispatch without `Send`/`Sync` requirements.
pub trait LocalHttpPolicySet<State = ()>: 'static {
    /// Worker-local socket reservation.
    type SocketLease: PolicyLease + 'static;
    /// Worker-local accepted connection reservation.
    type AcceptedLease: PolicyLease + 'static;
    /// Worker-local ready connection reservation.
    type ReadyLease: PolicyLease + 'static;
    /// Worker-local request/response-body reservation.
    type HeadLease: PolicyLease + 'static;
    /// Worker-local handler reservation.
    type HandlerLease: PolicyLease + 'static;
    /// Explicit connection cancellation capability.
    type Control: Control;

    /// Check the socket synchronously before spawning locally.
    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection>;
    /// Admit before TLS; retain the lease until the connection ends.
    fn accepted(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>>;
    /// Admit after TLS and before constructing connection state.
    fn ready(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>>;
    /// Admit a validated head; retain the lease in the transport response body.
    fn head(
        &self,
        input: &mut LocalHttpRequestHead<'_, State>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>>;
    /// Admit immediately before local dispatch; release when the handler ends.
    fn handler(
        &self,
        input: &mut LocalHttpHandlerInput<'_, State>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>>;
    /// Borrow the explicit connection control capability.
    fn control(&self) -> &Self::Control;
}

impl<State, Policies: HttpPolicySet<State>> HttpPolicySet<State>
    for AttachedHttpPolicies<Policies>
{
    type SocketLease = Policies::SocketLease;
    type AcceptedLease = Policies::AcceptedLease;
    type ReadyLease = Policies::ReadyLease;
    type HeadLease = Policies::HeadLease;
    type HandlerLease = Policies::HandlerLease;
    type Control = Policies::Control;

    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection> {
        self.0.socket(input)
    }

    fn accepted(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> + Send {
        self.0.accepted(input)
    }

    fn ready(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + Send {
        self.0.ready(input)
    }

    fn head(
        &self,
        input: &mut HttpRequestHead<'_, State>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>> + Send {
        self.0.head(input)
    }

    fn handler(
        &self,
        input: &mut HttpHandlerInput<'_, State>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> + Send {
        self.0.handler(input)
    }

    fn control(&self) -> &Self::Control {
        self.0.control()
    }
}

impl<State, Policies: LocalHttpPolicySet<State>> LocalHttpPolicySet<State>
    for AttachedHttpPolicies<Policies>
{
    type SocketLease = Policies::SocketLease;
    type AcceptedLease = Policies::AcceptedLease;
    type ReadyLease = Policies::ReadyLease;
    type HeadLease = Policies::HeadLease;
    type HandlerLease = Policies::HandlerLease;
    type Control = Policies::Control;

    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection> {
        self.0.socket(input)
    }

    fn accepted(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> {
        self.0.accepted(input)
    }

    fn ready(
        &self,
        input: &mut HttpConnection<'_>,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> {
        self.0.ready(input)
    }

    fn head(
        &self,
        input: &mut LocalHttpRequestHead<'_, State>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>> {
        self.0.head(input)
    }

    fn handler(
        &self,
        input: &mut LocalHttpHandlerInput<'_, State>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> {
        self.0.handler(input)
    }

    fn control(&self) -> &Self::Control {
        self.0.control()
    }
}

impl<State, S, A, R, H, E, C, AL, RL, HL, EL> LocalHttpPolicySet<State>
    for HttpPolicies<S, A, R, H, E, C>
where
    State: 'static,
    S: LocalSocketAdmission<HttpSocket> + 'static,
    A: for<'a> LocalAdmission<HttpConnection<'a>, Lease = AL> + 'static,
    R: for<'a> LocalAdmission<HttpConnection<'a>, Lease = RL> + 'static,
    H: for<'a> LocalAdmission<LocalHttpRequestHead<'a, State>, Lease = HL> + 'static,
    E: for<'a> LocalAdmission<LocalHttpHandlerInput<'a, State>, Lease = EL> + 'static,
    AL: PolicyLease + 'static,
    RL: PolicyLease + 'static,
    HL: PolicyLease + 'static,
    EL: PolicyLease + 'static,
    C: Control + 'static,
{
    type SocketLease = S::Lease;
    type AcceptedLease = AL;
    type ReadyLease = RL;
    type HeadLease = HL;
    type HandlerLease = EL;
    type Control = C;

    fn socket(&self, input: &HttpSocket) -> Result<Self::SocketLease, Rejection> {
        self.socket.admit(input)
    }
    async fn accepted(&self, input: &mut HttpConnection<'_>) -> Result<AL, Rejection> {
        self.accepted.enter(input).await
    }
    async fn ready(&self, input: &mut HttpConnection<'_>) -> Result<RL, Rejection> {
        self.ready.enter(input).await
    }
    async fn head(&self, input: &mut LocalHttpRequestHead<'_, State>) -> Result<HL, Rejection> {
        self.head.enter(input).await
    }
    async fn handler(&self, input: &mut LocalHttpHandlerInput<'_, State>) -> Result<EL, Rejection> {
        self.handler.enter(input).await
    }
    fn control(&self) -> &C {
        &self.control
    }
}

impl<State> HttpPolicySet<State> for NoHttpPolicies {
    type SocketLease = ();
    type AcceptedLease = ();
    type ReadyLease = ();
    type HeadLease = ();
    type HandlerLease = ();
    type Control = NoControl;

    fn socket(&self, _: &HttpSocket) -> Result<(), Rejection> {
        Ok(())
    }
    async fn accepted(&self, _: &mut HttpConnection<'_>) -> Result<(), Rejection> {
        Ok(())
    }
    async fn ready(&self, _: &mut HttpConnection<'_>) -> Result<(), Rejection> {
        Ok(())
    }
    fn head(
        &self,
        _: &mut HttpRequestHead<'_, State>,
    ) -> impl Future<Output = Result<(), Rejection>> + Send {
        std::future::ready(Ok(()))
    }
    fn handler(
        &self,
        _: &mut HttpHandlerInput<'_, State>,
    ) -> impl Future<Output = Result<(), Rejection>> + Send {
        std::future::ready(Ok(()))
    }
    fn control(&self) -> &NoControl {
        &NoControl
    }
}

impl<State> LocalHttpPolicySet<State> for NoHttpPolicies {
    type SocketLease = ();
    type AcceptedLease = ();
    type ReadyLease = ();
    type HeadLease = ();
    type HandlerLease = ();
    type Control = NoControl;

    fn socket(&self, _: &HttpSocket) -> Result<(), Rejection> {
        Ok(())
    }
    async fn accepted(&self, _: &mut HttpConnection<'_>) -> Result<(), Rejection> {
        Ok(())
    }
    async fn ready(&self, _: &mut HttpConnection<'_>) -> Result<(), Rejection> {
        Ok(())
    }
    async fn head(&self, _: &mut LocalHttpRequestHead<'_, State>) -> Result<(), Rejection> {
        Ok(())
    }
    async fn handler(&self, _: &mut LocalHttpHandlerInput<'_, State>) -> Result<(), Rejection> {
        Ok(())
    }
    fn control(&self) -> &NoControl {
        &NoControl
    }
}

#[cfg(test)]
mod tests;
