//! Server-owned, statically dispatched TCP admission checkpoints.

use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use nacelle_core::pipeline::{ConnectionContext, ConnectionInfo};
use nacelle_core::policy::{
    Admission, Cancellation, Chain, Control, LocalAdmission, LocalSocketAdmission, NoControl,
    NoPolicy, PolicyLease, Rejection, SocketAdmission,
};
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::NacelleTelemetryObserver;
use tokio::io::AsyncWrite;

use crate::connection::checkpoint::{Combine, Empty, FlushMode, Io, Mode, Slot};

/// Read-only accepted or negotiated transport metadata.
pub struct TcpTransport<'a> {
    connection: &'a mut NacelleConnectionMeta,
}

impl TcpTransport<'_> {
    /// Accepted socket addresses and, after negotiation, TLS metadata.
    pub fn connection(&self) -> &NacelleConnectionMeta {
        self.connection
    }
}

/// Read-only connection facts borrowed exclusively during admission.
///
/// `Storage` is `Arc<P::ConnectionState>` for shared-state servers and
/// `P::ConnectionState` for serial servers. Exclusive borrowing avoids imposing
/// `Sync` on serial state merely because admission can await.
pub struct TcpConnection<'a, Storage> {
    connection: &'a mut ConnectionContext<Storage>,
}

impl<Storage> TcpConnection<'_, Storage> {
    /// Connection metadata presented to the handler.
    pub fn connection(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    /// The connection's concrete state storage.
    pub fn state(&self) -> &Storage {
        &self.connection.state
    }
}

/// Read-only decoded request facts borrowed exclusively during admission.
pub struct TcpHead<'a, Request, Storage> {
    request: &'a mut Request,
    connection: &'a mut ConnectionContext<Storage>,
    body_len: usize,
}

impl<Request, Storage> TcpHead<'_, Request, Storage> {
    /// The protocol's decoded request head.
    pub fn request(&self) -> &Request {
        self.request
    }

    /// Declared request body length, before further body allocation or reads.
    pub const fn body_len(&self) -> usize {
        self.body_len
    }

    /// Connection metadata presented to the handler.
    pub fn connection(&self) -> &ConnectionInfo {
        &self.connection.info
    }

    /// The connection's concrete state storage.
    pub fn state(&self) -> &Storage {
        &self.connection.state
    }
}

/// Empty server policy storage. This type does not allocate.
#[derive(Clone, Copy, Default)]
pub struct NoTcpPolicies;

/// Typed TCP checkpoints, evaluated in registration order.
///
/// Attach once using a server's `with_policies` method or the TCP builder's
/// `policies` method. Attaching policies never changes the protocol or handler
/// context type. Legacy protocol hooks run before the corresponding new hook.
///
/// Policies for a different checkpoint are rejected when attached:
///
/// ```compile_fail
/// use nacelle_core::{policy::check_fn, request::NacelleConnectionMeta};
/// use nacelle_tcp::{SharedProtocol, TcpPolicies, TcpServer};
///
/// fn attach<P: SharedProtocol, H, O>(server: TcpServer<P, H, O>) {
///     server.with_policies(TcpPolicies::new().on_request_head(check_fn(
///         |_: &NacelleConnectionMeta| Ok(()),
///     )));
/// }
/// ```
#[derive(Clone)]
pub struct TcpPolicies<
    Socket = NoPolicy,
    Accepted = NoPolicy,
    Ready = NoPolicy,
    Before = NoPolicy,
    Head = NoPolicy,
    Handler = NoPolicy,
    CancellationControl = NoControl,
> {
    socket: Socket,
    accepted: Accepted,
    ready: Ready,
    before: Before,
    head: Head,
    handler: Handler,
    control: CancellationControl,
}

impl Default for TcpPolicies {
    fn default() -> Self {
        Self::new()
    }
}

impl TcpPolicies {
    /// Construct an empty checkpoint set.
    pub const fn new() -> Self {
        Self {
            socket: NoPolicy,
            accepted: NoPolicy,
            ready: NoPolicy,
            before: NoPolicy,
            head: NoPolicy,
            handler: NoPolicy,
            control: NoControl,
        }
    }
}

macro_rules! checkpoint {
    ($name:ident, $field:ident, $result:ty, $description:literal; $($other:ident),+) => {
        #[doc = $description]
        pub fn $name<Next>(self, policy: Next) -> $result {
            TcpPolicies {
                $field: Chain::new(self.$field, policy),
                $($other: self.$other,)+
            }
        }
    };
}

impl<S, A, T, B, H, R, C> TcpPolicies<S, A, T, B, H, R, C> {
    checkpoint!(on_socket, socket, TcpPolicies<Chain<S, Next>, A, T, B, H, R, C>,
        "Append a synchronous check after accept and before task spawning."; accepted, ready, before, head, handler, control);
    checkpoint!(on_accepted, accepted, TcpPolicies<S, Chain<A, Next>, T, B, H, R, C>,
        "Append admission inside the connection task, before TLS negotiation."; socket, ready, before, head, handler, control);
    checkpoint!(on_transport_ready, ready, TcpPolicies<S, A, Chain<T, Next>, B, H, R, C>,
        "Append admission after negotiation, before buffers and connection state."; socket, accepted, before, head, handler, control);
    checkpoint!(before_request, before, TcpPolicies<S, A, T, Chain<B, Next>, H, R, C>,
        "Append next-head admission, retained through request and response delivery."; socket, accepted, ready, head, handler, control);
    checkpoint!(on_request_head, head, TcpPolicies<S, A, T, B, Chain<H, Next>, R, C>,
        "Append admission after mandatory head validation, before further body reads."; socket, accepted, ready, before, handler, control);
    checkpoint!(around_handler, handler, TcpPolicies<S, A, T, B, H, Chain<R, Next>, C>,
        "Append admission immediately before the handler, held only while it runs."; socket, accepted, ready, before, head, control);
}

impl<S, A, T, B, H, R> TcpPolicies<S, A, T, B, H, R> {
    /// Cancel policy waits, negotiation, reads, handlers, and writes together.
    pub fn with_cancellation(
        self,
        control: Cancellation,
    ) -> TcpPolicies<S, A, T, B, H, R, Cancellation> {
        TcpPolicies {
            socket: self.socket,
            accepted: self.accepted,
            ready: self.ready,
            before: self.before,
            head: self.head,
            handler: self.handler,
            control,
        }
    }
}

/// Shared-runtime policy contract used by TCP servers and listener factories.
#[doc(hidden)]
pub trait SharedTcpPolicySet<Request, Storage>: Send + Sync + 'static {
    type SocketLease: PolicyLease + Send + 'static;
    type AcceptedLease: PolicyLease + Send + 'static;
    type ReadyLease: PolicyLease + Send + 'static;
    type BeforeLease: PolicyLease + Send + 'static;
    type HeadLease: PolicyLease + Send + 'static;
    type HandlerLease: PolicyLease + Send + 'static;
    type BeforeMode: Mode<Self::BeforeLease>;
    type HeadMode: Mode<Self::HeadLease>;
    type HandlerMode: Mode<Self::HandlerLease>;
    type DeliveryMode: FlushMode;

    fn socket(&self, input: &NacelleConnectionMeta) -> Result<Self::SocketLease, Rejection>;
    fn accepted<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> + Send + 'a;
    fn ready<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + Send + 'a;
    fn before<'a>(
        &'a self,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::BeforeLease, Rejection>> + Send + 'a;
    fn head<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>> + Send + 'a;
    fn handler<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> + Send + 'a;
    fn run<F: Future + Send>(
        &self,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Rejection>> + Send;
}

/// Worker-local policy contract, retaining support for `!Send` state and leases.
#[doc(hidden)]
pub trait LocalTcpPolicySet<Request, Storage>: 'static {
    type SocketLease: PolicyLease + 'static;
    type AcceptedLease: PolicyLease + 'static;
    type ReadyLease: PolicyLease + 'static;
    type BeforeLease: PolicyLease + 'static;
    type HeadLease: PolicyLease + 'static;
    type HandlerLease: PolicyLease + 'static;
    type BeforeMode: Mode<Self::BeforeLease>;
    type HeadMode: Mode<Self::HeadLease>;
    type HandlerMode: Mode<Self::HandlerLease>;
    type DeliveryMode: FlushMode;

    fn socket(&self, input: &NacelleConnectionMeta) -> Result<Self::SocketLease, Rejection>;
    fn accepted<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> + 'a;
    fn ready<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + 'a;
    fn before<'a>(
        &'a self,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::BeforeLease, Rejection>> + 'a;
    fn head<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::HeadLease, Rejection>> + 'a;
    fn handler<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> + 'a;
    fn run<F: Future>(&self, future: F) -> impl Future<Output = Result<F::Output, Rejection>>;
}

macro_rules! empty_set {
    ($trait:ident $(, $send:ident)?) => {
        impl<Request, Storage> $trait<Request, Storage> for NoTcpPolicies {
            type SocketLease = ();
            type AcceptedLease = ();
            type ReadyLease = ();
            type BeforeLease = ();
            type HeadLease = ();
            type HandlerLease = ();
            type BeforeMode = Empty;
            type HeadMode = Empty;
            type HandlerMode = Empty;
            type DeliveryMode = Empty;

            fn socket(&self, _: &NacelleConnectionMeta) -> Result<(), Rejection> { Ok(()) }
            fn accepted<'a>(&'a self, _: &'a mut NacelleConnectionMeta) -> impl Future<Output = Result<(), Rejection>> + $($send +)? 'a { async { Ok(()) } }
            fn ready<'a>(&'a self, _: &'a mut NacelleConnectionMeta) -> impl Future<Output = Result<(), Rejection>> + $($send +)? 'a { async { Ok(()) } }
            fn before<'a>(&'a self, _: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<(), Rejection>> + $($send +)? 'a { async { Ok(()) } }
            fn head<'a>(&'a self, _: &'a mut Request, _: usize, _: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<(), Rejection>> + $($send +)? 'a { async { Ok(()) } }
            fn handler<'a>(&'a self, _: &'a mut Request, _: usize, _: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<(), Rejection>> + $($send +)? 'a { async { Ok(()) } }
            fn run<F: Future $(+ $send)?>(&self, future: F) -> impl Future<Output = Result<F::Output, Rejection>> $(+ $send)? { async { Ok(future.await) } }
        }
    };
}

empty_set!(SharedTcpPolicySet, Send);
empty_set!(LocalTcpPolicySet);

macro_rules! owned_set {
    ($owner:ident, $set:ident $(, $send:ident)?) => {
        impl<Request, Storage, P: $set<Request, Storage>> $set<Request, Storage> for $owner<P> {
            type SocketLease = P::SocketLease;
            type AcceptedLease = P::AcceptedLease;
            type ReadyLease = P::ReadyLease;
            type BeforeLease = P::BeforeLease;
            type HeadLease = P::HeadLease;
            type HandlerLease = P::HandlerLease;
            type BeforeMode = P::BeforeMode;
            type HeadMode = P::HeadMode;
            type HandlerMode = P::HandlerMode;
            type DeliveryMode = P::DeliveryMode;

            fn socket(&self, input: &NacelleConnectionMeta) -> Result<Self::SocketLease, Rejection> {
                self.as_ref().socket(input)
            }
            fn accepted<'a>(&'a self, input: &'a mut NacelleConnectionMeta) -> impl Future<Output = Result<Self::AcceptedLease, Rejection>> + $($send +)? 'a {
                self.as_ref().accepted(input)
            }
            fn ready<'a>(&'a self, input: &'a mut NacelleConnectionMeta) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + $($send +)? 'a {
                self.as_ref().ready(input)
            }
            fn before<'a>(&'a self, input: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<Self::BeforeLease, Rejection>> + $($send +)? 'a {
                self.as_ref().before(input)
            }
            fn head<'a>(&'a self, request: &'a mut Request, body_len: usize, input: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<Self::HeadLease, Rejection>> + $($send +)? 'a {
                self.as_ref().head(request, body_len, input)
            }
            fn handler<'a>(&'a self, request: &'a mut Request, body_len: usize, input: &'a mut ConnectionContext<Storage>) -> impl Future<Output = Result<Self::HandlerLease, Rejection>> + $($send +)? 'a {
                self.as_ref().handler(request, body_len, input)
            }
            fn run<F: Future $(+ $send)?>(&self, future: F) -> impl Future<Output = Result<F::Output, Rejection>> $(+ $send)? {
                self.as_ref().run(future)
            }
        }
    };
}

owned_set!(Arc, SharedTcpPolicySet, Send);
owned_set!(Rc, LocalTcpPolicySet);

impl<Request, Storage, S, A, T, B, H, R, C, AL, TL, BL, HL, RL> SharedTcpPolicySet<Request, Storage>
    for TcpPolicies<S, A, T, B, H, R, C>
where
    Request: Send + 'static,
    Storage: Send + 'static,
    S: SocketAdmission<NacelleConnectionMeta> + 'static,
    A: for<'a> Admission<TcpTransport<'a>, Lease = AL> + 'static,
    T: for<'a> Admission<TcpTransport<'a>, Lease = TL> + 'static,
    B: for<'a> Admission<TcpConnection<'a, Storage>, Lease = BL> + Slot + 'static,
    H: for<'a> Admission<TcpHead<'a, Request, Storage>, Lease = HL> + Slot + 'static,
    R: for<'a> Admission<TcpHead<'a, Request, Storage>, Lease = RL> + Slot + 'static,
    B::Mode: Mode<BL>,
    H::Mode: Mode<HL>,
    R::Mode: Mode<RL>,
    B::Mode: Combine<H::Mode>,
    BL: PolicyLease + Send + 'static,
    HL: PolicyLease + Send + 'static,
    RL: PolicyLease + Send + 'static,
    C: Control + 'static,
    AL: PolicyLease + Send + 'static,
    TL: PolicyLease + Send + 'static,
{
    type SocketLease = S::Lease;
    type AcceptedLease = AL;
    type ReadyLease = TL;
    type BeforeLease = BL;
    type HeadLease = HL;
    type HandlerLease = RL;
    type BeforeMode = B::Mode;
    type HeadMode = H::Mode;
    type HandlerMode = R::Mode;
    type DeliveryMode = <B::Mode as Combine<H::Mode>>::Mode;

    fn socket(&self, input: &NacelleConnectionMeta) -> Result<Self::SocketLease, Rejection> {
        SocketAdmission::admit(&self.socket, input)
    }
    async fn accepted<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> Result<Self::AcceptedLease, Rejection> {
        self.accepted
            .enter(&mut TcpTransport { connection: input })
            .await
    }
    async fn ready<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> Result<Self::ReadyLease, Rejection> {
        self.ready
            .enter(&mut TcpTransport { connection: input })
            .await
    }
    async fn before<'a>(
        &'a self,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::BeforeLease, Rejection> {
        self.before.enter(&mut TcpConnection { connection }).await
    }
    async fn head<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::HeadLease, Rejection> {
        self.head
            .enter(&mut TcpHead {
                request,
                connection,
                body_len,
            })
            .await
    }
    async fn handler<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::HandlerLease, Rejection> {
        self.handler
            .enter(&mut TcpHead {
                request,
                connection,
                body_len,
            })
            .await
    }
    fn run<F: Future + Send>(
        &self,
        future: F,
    ) -> impl Future<Output = Result<F::Output, Rejection>> + Send {
        self.control.run_send(future)
    }
}

impl<Request: 'static, Storage: 'static, S, A, T, B, H, R, C, AL, TL, BL, HL, RL>
    LocalTcpPolicySet<Request, Storage> for TcpPolicies<S, A, T, B, H, R, C>
where
    S: LocalSocketAdmission<NacelleConnectionMeta> + 'static,
    A: for<'a> LocalAdmission<TcpTransport<'a>, Lease = AL> + 'static,
    T: for<'a> LocalAdmission<TcpTransport<'a>, Lease = TL> + 'static,
    B: for<'a> LocalAdmission<TcpConnection<'a, Storage>, Lease = BL> + Slot + 'static,
    H: for<'a> LocalAdmission<TcpHead<'a, Request, Storage>, Lease = HL> + Slot + 'static,
    R: for<'a> LocalAdmission<TcpHead<'a, Request, Storage>, Lease = RL> + Slot + 'static,
    B::Mode: Mode<BL>,
    H::Mode: Mode<HL>,
    R::Mode: Mode<RL>,
    B::Mode: Combine<H::Mode>,
    BL: PolicyLease + 'static,
    HL: PolicyLease + 'static,
    RL: PolicyLease + 'static,
    C: Control + 'static,
    AL: PolicyLease + 'static,
    TL: PolicyLease + 'static,
{
    type SocketLease = S::Lease;
    type AcceptedLease = AL;
    type ReadyLease = TL;
    type BeforeLease = BL;
    type HeadLease = HL;
    type HandlerLease = RL;
    type BeforeMode = B::Mode;
    type HeadMode = H::Mode;
    type HandlerMode = R::Mode;
    type DeliveryMode = <B::Mode as Combine<H::Mode>>::Mode;

    fn socket(&self, input: &NacelleConnectionMeta) -> Result<Self::SocketLease, Rejection> {
        LocalSocketAdmission::admit(&self.socket, input)
    }
    async fn accepted<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> Result<Self::AcceptedLease, Rejection> {
        self.accepted
            .enter(&mut TcpTransport { connection: input })
            .await
    }
    async fn ready<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> Result<Self::ReadyLease, Rejection> {
        self.ready
            .enter(&mut TcpTransport { connection: input })
            .await
    }
    async fn before<'a>(
        &'a self,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::BeforeLease, Rejection> {
        self.before.enter(&mut TcpConnection { connection }).await
    }
    async fn head<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::HeadLease, Rejection> {
        self.head
            .enter(&mut TcpHead {
                request,
                connection,
                body_len,
            })
            .await
    }
    async fn handler<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
    ) -> Result<Self::HandlerLease, Rejection> {
        self.handler
            .enter(&mut TcpHead {
                request,
                connection,
                body_len,
            })
            .await
    }
    fn run<F: Future>(&self, future: F) -> impl Future<Output = Result<F::Output, Rejection>> {
        self.control.run(future)
    }
}

pub(crate) struct SharedDispatch<'a, P>(pub(crate) &'a P);
pub(crate) struct LocalDispatch<'a, P>(pub(crate) &'a P);

pub(crate) trait PolicyDispatch<Request, Storage> {
    type ReadyLease: PolicyLease + 'static;
    type BeforeLease: PolicyLease + 'static;
    type HeadLease: PolicyLease + 'static;
    type HandlerLease: PolicyLease + 'static;

    fn ready<'a>(
        &'a self,
        input: &'a mut NacelleConnectionMeta,
    ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> + 'a;
}

pub(crate) trait DeliveryDispatch<Request, Storage, W, Observer>:
    PolicyDispatch<Request, Storage>
{
    fn flush<'a>(
        &'a self,
        required: bool,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), nacelle_core::NacelleError>> + 'a;
    fn before<'a>(
        &'a self,
        connection: &'a mut ConnectionContext<Storage>,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Self::BeforeLease, nacelle_core::NacelleError>> + 'a;
    fn head<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Self::HeadLease, nacelle_core::NacelleError>> + 'a;
    fn handler<'a>(
        &'a self,
        request: &'a mut Request,
        body_len: usize,
        connection: &'a mut ConnectionContext<Storage>,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Self::HandlerLease, nacelle_core::NacelleError>> + 'a;
}

macro_rules! dispatch {
    ($wrapper:ident, $set:ident, $enter:ident, $flush:ident $(, $send:ident)?) => {
        #[allow(refining_impl_trait)]
        impl<Request, Storage, P> PolicyDispatch<Request, Storage>
            for $wrapper<'_, P>
        where
            P: $set<Request, Storage>,
        {
            type ReadyLease = P::ReadyLease;
            type BeforeLease = P::BeforeLease;
            type HeadLease = P::HeadLease;
            type HandlerLease = P::HandlerLease;
            fn ready<'a>(
                &'a self,
                input: &'a mut NacelleConnectionMeta,
            ) -> impl Future<Output = Result<Self::ReadyLease, Rejection>> $(+ $send)? + 'a {
                self.0.ready(input)
            }
        }
        #[allow(refining_impl_trait)]
        impl<Request, Storage, P, W, Observer> DeliveryDispatch<Request, Storage, W, Observer>
            for $wrapper<'_, P>
        where
            P: $set<Request, Storage>,
            W: AsyncWrite + Unpin $(+ $send)?,
            Observer: NacelleTelemetryObserver,
        {
            fn flush<'a>(
                &'a self,
                required: bool,
                io: Io<'a, W, Observer>,
            ) -> impl Future<Output = Result<(), nacelle_core::NacelleError>> $(+ $send)? + 'a {
                P::DeliveryMode::$flush(required, io)
            }
            fn before<'a>(
                &'a self,
                connection: &'a mut ConnectionContext<Storage>,
                io: Io<'a, W, Observer>,
            ) -> impl Future<Output = Result<Self::BeforeLease, nacelle_core::NacelleError>> $(+ $send)? + 'a {
                P::BeforeMode::$enter(move || self.0.before(connection), io)
            }
            fn head<'a>(
                &'a self,
                request: &'a mut Request,
                body_len: usize,
                connection: &'a mut ConnectionContext<Storage>,
                io: Io<'a, W, Observer>,
            ) -> impl Future<Output = Result<Self::HeadLease, nacelle_core::NacelleError>> $(+ $send)? + 'a {
                P::HeadMode::$enter(move || self.0.head(request, body_len, connection), io)
            }
            fn handler<'a>(
                &'a self,
                request: &'a mut Request,
                body_len: usize,
                connection: &'a mut ConnectionContext<Storage>,
                io: Io<'a, W, Observer>,
            ) -> impl Future<Output = Result<Self::HandlerLease, nacelle_core::NacelleError>> $(+ $send)? + 'a {
                P::HandlerMode::$enter(move || self.0.handler(request, body_len, connection), io)
            }
        }
    };
}

dispatch!(SharedDispatch, SharedTcpPolicySet, enter_send, flush_send, Send);
dispatch!(LocalDispatch, LocalTcpPolicySet, enter, flush);
