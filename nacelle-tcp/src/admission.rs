//! Borrowed TCP admission contexts and transport dispatch adapters.

use std::future::{Future, ready};
use std::ops::Deref;
use std::sync::Arc;

use nacelle_core::NacelleError;
use nacelle_core::admission::{
    Admission, AdmissionConfig, AdmissionContext, AdmissionRejection, AdmissionState,
    LocalAdmission,
};
use nacelle_core::pipeline::{Handler, LocalHandler};

use crate::protocol::{
    LocalAppStateHandler, LocalSerialTcpHandler, LocalTcpHandler, Protocol, SerialTcpHandler,
    SerialTcpRequestContext, SharedAppStateHandler, TcpHandler, TcpHandlerCompletion,
    TcpRequestContext,
};

/// TCP head and shared connection state borrowed before body dispatch.
pub type TcpAdmissionContext<'a, P, AppState = ()> =
    AdmissionContext<'a, <P as Protocol>::Request, Arc<<P as Protocol>::ConnectionState>, AppState>;

/// TCP head and exclusive serial connection state borrowed before body dispatch.
pub type SerialTcpAdmissionContext<'a, P, AppState = ()> =
    AdmissionContext<'a, <P as Protocol>::Request, <P as Protocol>::ConnectionState, AppState>;

/// Concrete policy storage installed by the server's admission builder.
#[doc(hidden)]
pub struct TcpAdmitted<H, A> {
    handler: H,
    policy: A,
    state: AdmissionState,
}

#[doc(hidden)]
pub type SharedTcpAdmitted<H, A> = TcpAdmitted<Arc<H>, A>;

#[doc(hidden)]
pub type LocalTcpAdmitted<H, A> = TcpAdmitted<std::rc::Rc<H>, A>;

impl<H, A> TcpAdmitted<H, A> {
    pub(crate) const fn new(handler: H, policy: A, config: AdmissionConfig) -> Self {
        Self {
            handler,
            policy,
            state: AdmissionState::new(config),
        }
    }
}

/// Default serial dispatch, installed by serial server constructors.
#[doc(hidden)]
pub struct SerialHandlerAdapter<H>(pub H);

impl<P, H, AppState> SerialTcpHandler<P, AppState> for SerialHandlerAdapter<H>
where
    P: Protocol,
    P::ConnectionState: Send,
    H: SerialTcpHandler<P, AppState>,
{
    fn call<'a>(
        &'a self,
        context: SerialTcpRequestContext<'a, P, AppState>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + Send + 'a {
        SerialTcpHandler::call(&self.0, context)
    }
}

#[allow(clippy::future_not_send)]
impl<P, H, AppState> LocalSerialTcpHandler<P, AppState> for SerialHandlerAdapter<H>
where
    P: Protocol,
    H: LocalSerialTcpHandler<P, AppState>,
{
    fn call<'a>(
        &'a self,
        context: SerialTcpRequestContext<'a, P, AppState>,
    ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'a {
        LocalSerialTcpHandler::call(&self.0, context)
    }
}

macro_rules! base_handler {
    ($this:ident, plain) => {
        $this
    };
    ($this:ident, serial) => {
        &$this.0
    };
}

// Each dispatch mode retains its existing handler contract and thread bounds.
// Only this transport adapter combines the optional policy with that handler.
macro_rules! service {
    (
        $service:ident, $handler:ident, $call:path, $policy:ident, $adapter:ident,
        $context:ty, $unit_context:ty, $admission:ty, $unit_admission:ty,
        [$($thread:ident),*], [$($future:tt)*], [$($state:tt)*],
        $base:ty, $base_kind:ident
    ) => {
        #[doc(hidden)]
        #[allow(clippy::future_not_send)]
        pub trait $service<P, AppState = ()>: 'static $(+ $thread)*
        where
            P: Protocol,
            P::ConnectionState: $($state)*,
            AppState: 'static $(+ $thread)*,
        {
            type Permit: 'static $($future)*;

            fn admission_state(&self) -> Option<&AdmissionState>;

            fn admit<'a>(
                &'a self,
                context: $admission,
            ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + 'a $($future)*;

            fn call<'a>(
                &'a self,
                context: $context,
            ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'a $($future)*;
        }

        #[allow(clippy::future_not_send)]
        impl<P, H, AppState> $service<P, AppState> for $base
        where
            P: Protocol,
            P::ConnectionState: $($state)*,
            AppState: 'static $(+ $thread)*,
            H: $handler<P, AppState> + 'static $(+ $thread)*,
        {
            type Permit = ();

            fn admission_state(&self) -> Option<&AdmissionState> { None }

            fn admit<'a>(
                &'a self,
                _context: $admission,
            ) -> impl Future<Output = Result<(), AdmissionRejection>> + 'a $($future)* {
                ready(Ok(()))
            }

            fn call<'a>(
                &'a self,
                context: $context,
            ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'a $($future)* {
                $call(base_handler!(self, $base_kind), context)
            }
        }

        #[allow(clippy::future_not_send)]
        impl<P, H, A, AppState, Permit> $service<P, AppState> for TcpAdmitted<H, A>
        where
            P: Protocol,
            P::ConnectionState: $($state)*,
            AppState: 'static $(+ $thread)*,
            H: Deref + 'static $(+ $thread)*,
            H::Target: $handler<P, AppState>,
            A: for<'a> $policy<$admission, Permit = Permit> + 'static $(+ $thread)*,
            Permit: 'static $($future)*,
        {
            type Permit = Permit;

            fn admission_state(&self) -> Option<&AdmissionState> { Some(&self.state) }

            fn admit<'a>(
                &'a self,
                context: $admission,
            ) -> impl Future<Output = Result<Permit, AdmissionRejection>> + 'a $($future)* {
                $policy::admit(&self.policy, context)
            }

            fn call<'a>(
                &'a self,
                context: $context,
            ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'a $($future)* {
                $call(self.handler.deref(), context)
            }
        }

        #[allow(clippy::future_not_send)]
        impl<P, H, AppState> $service<P> for $adapter<P, H, AppState>
        where
            P: Protocol,
            P::ConnectionState: $($state)*,
            AppState: 'static $(+ $thread)*,
            H: $service<P, AppState>,
        {
            type Permit = H::Permit;

            fn admission_state(&self) -> Option<&AdmissionState> {
                self.handler.admission_state()
            }

            fn admit<'a>(
                &'a self,
                context: $unit_admission,
            ) -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + 'a $($future)* {
                self.handler.admit(context.map_app_state(self.app_state.as_ref()))
            }

            fn call<'a>(
                &'a self,
                context: $unit_context,
            ) -> impl Future<Output = Result<TcpHandlerCompletion<P>, NacelleError>> + 'a $($future)* {
                $service::call(self.handler.as_ref(), context.map_app_state(self.app_state.clone()))
            }
        }
    };
}

service!(
    TcpService, TcpHandler, Handler::call, Admission, SharedAppStateHandler,
    TcpRequestContext<P, AppState>, TcpRequestContext<P>,
    TcpAdmissionContext<'a, P, AppState>, TcpAdmissionContext<'a, P>,
    [Send, Sync], [+ Send], [Send + Sync], H, plain
);
service!(
    LocalTcpService, LocalTcpHandler, LocalHandler::call, LocalAdmission, LocalAppStateHandler,
    TcpRequestContext<P, AppState>, TcpRequestContext<P>,
    TcpAdmissionContext<'a, P, AppState>, TcpAdmissionContext<'a, P>,
    [], [], ['static], H, plain
);
service!(
    SerialTcpService, SerialTcpHandler, SerialTcpHandler::call, Admission, SharedAppStateHandler,
    SerialTcpRequestContext<'a, P, AppState>, SerialTcpRequestContext<'a, P>,
    SerialTcpAdmissionContext<'a, P, AppState>, SerialTcpAdmissionContext<'a, P>,
    [Send, Sync], [+ Send], [Send], SerialHandlerAdapter<H>, serial
);
service!(
    LocalSerialTcpService, LocalSerialTcpHandler, LocalSerialTcpHandler::call, LocalAdmission,
    LocalAppStateHandler,
    SerialTcpRequestContext<'a, P, AppState>, SerialTcpRequestContext<'a, P>,
    SerialTcpAdmissionContext<'a, P, AppState>, SerialTcpAdmissionContext<'a, P>,
    [], [], ['static], SerialHandlerAdapter<H>, serial
);
