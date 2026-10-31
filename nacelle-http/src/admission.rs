//! HTTP admission runs after Hyper parsing and before the body bridge.

use std::future::{Future, ready};
use std::net::IpAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use http::{HeaderMap, Method, Uri, Version};
use nacelle_core::NacelleError;
use nacelle_core::admission::{
    Admission, AdmissionConfig, AdmissionContext, AdmissionRejection, AdmissionState,
    LocalAdmission,
};
use nacelle_core::pipeline::{Handler, LocalHandler};

use crate::pipeline::{
    HttpHandler, HttpHandlerCompletion, HttpRequestContext, LocalHttpAppStateHandler,
    LocalHttpHandler, LocalHttpRequestContext, SharedHttpAppStateHandler,
};

/// Parsed HTTP head, without a body channel or a copy of the header map.
#[non_exhaustive]
pub struct HttpRequestHead {
    /// Request method.
    pub method: Method,
    /// Request target.
    pub uri: Uri,
    /// Parsed headers.
    pub headers: HeaderMap,
    /// Parsed HTTP version.
    pub version: Version,
    /// Effective peer after trusted-proxy resolution.
    pub peer_ip: Option<IpAddr>,
}

/// Shared HTTP admission context with typed connection and application state.
pub type HttpAdmissionContext<'a, State = (), AppState = ()> =
    AdmissionContext<'a, HttpRequestHead, Arc<State>, AppState>;

/// Worker-local HTTP admission context with `Rc`-owned connection state.
pub type LocalHttpAdmissionContext<'a, State = (), AppState = ()> =
    AdmissionContext<'a, HttpRequestHead, Rc<State>, AppState>;

#[doc(hidden)]
pub struct HttpAdmitted<H, A> {
    handler: H,
    policy: A,
    state: AdmissionState,
}

impl<H, A> HttpAdmitted<H, A> {
    pub(crate) const fn new(handler: H, policy: A, config: AdmissionConfig) -> Self {
        Self {
            handler,
            policy,
            state: AdmissionState::new(config),
        }
    }
}

macro_rules! service {
    ($service:ident, $handler:ident, $call:path, $policy:ident, $adapter:ident,
     $request:ident, $context:ident, [$($thread:ident),*], [$($future:tt)*]) => {
        #[doc(hidden)]
        #[allow(clippy::future_not_send)]
        pub trait $service<State, AppState = ()>: 'static $(+ $thread)*
        where
            State: 'static $(+ $thread)*,
            AppState: 'static $(+ $thread)*,
        {
            type Permit: 'static $($future)*;
            fn admission_state(&self) -> Option<&AdmissionState>;
            fn admit<'a>(&'a self, context: $context<'a, State, AppState>)
                -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + 'a $($future)*;
            fn call(&self, context: $request<State, AppState>)
                -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> $($future)*;
        }

        #[allow(clippy::future_not_send)]
        impl<State, AppState, H> $service<State, AppState> for H
        where
            State: 'static $(+ $thread)*,
            AppState: 'static $(+ $thread)*,
            H: $handler<State, AppState> + 'static $(+ $thread)*,
        {
            type Permit = ();
            fn admission_state(&self) -> Option<&AdmissionState> { None }
            fn admit<'a>(&'a self, _context: $context<'a, State, AppState>)
                -> impl Future<Output = Result<(), AdmissionRejection>> + 'a $($future)* {
                ready(Ok(()))
            }
            fn call(&self, context: $request<State, AppState>)
                -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> $($future)* {
                $call(self, context)
            }
        }

        #[allow(clippy::future_not_send)]
        impl<State, AppState, H, A, Permit> $service<State, AppState> for HttpAdmitted<H, A>
        where
            State: 'static $(+ $thread)*,
            AppState: 'static $(+ $thread)*,
            H: Deref + 'static $(+ $thread)*,
            H::Target: $handler<State, AppState>,
            A: for<'a> $policy<$context<'a, State, AppState>, Permit = Permit> + 'static $(+ $thread)*,
            Permit: 'static $($future)*,
        {
            type Permit = Permit;
            fn admission_state(&self) -> Option<&AdmissionState> { Some(&self.state) }
            fn admit<'a>(&'a self, context: $context<'a, State, AppState>)
                -> impl Future<Output = Result<Permit, AdmissionRejection>> + 'a $($future)* {
                $policy::admit(&self.policy, context)
            }
            fn call(&self, context: $request<State, AppState>)
                -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> $($future)* {
                $call(self.handler.deref(), context)
            }
        }

        #[allow(clippy::future_not_send)]
        impl<State, AppState, H> $service<State> for $adapter<H, AppState>
        where
            State: 'static $(+ $thread)*,
            AppState: 'static $(+ $thread)*,
            H: $service<State, AppState>,
        {
            type Permit = H::Permit;
            fn admission_state(&self) -> Option<&AdmissionState> { self.handler.admission_state() }
            fn admit<'a>(&'a self, context: $context<'a, State>)
                -> impl Future<Output = Result<Self::Permit, AdmissionRejection>> + 'a $($future)* {
                self.handler.admit(context.map_app_state(self.app_state.as_ref()))
            }
            fn call(&self, context: $request<State>)
                -> impl Future<Output = Result<HttpHandlerCompletion, NacelleError>> $($future)* {
                $service::call(self.handler.as_ref(), context.map_app_state(self.app_state.clone()))
            }
        }
    };
}

service!(HttpService, HttpHandler, Handler::call, Admission, SharedHttpAppStateHandler,
    HttpRequestContext, HttpAdmissionContext, [Send, Sync], [+ Send]);
service!(
    LocalHttpService,
    LocalHttpHandler,
    LocalHandler::call,
    LocalAdmission,
    LocalHttpAppStateHandler,
    LocalHttpRequestContext,
    LocalHttpAdmissionContext,
    [],
    []
);

pub(crate) async fn wait<Permit>(
    future: impl Future<Output = Result<Permit, AdmissionRejection>>,
    timeout: Duration,
) -> Result<Permit, NacelleError> {
    tokio::pin!(future);
    match std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await {
        Poll::Ready(result) => result.map_err(NacelleError::AdmissionRejected),
        Poll::Pending => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| NacelleError::Timeout(nacelle_core::NacelleTimeoutReason::Admission))?
            .map_err(NacelleError::AdmissionRejected),
    }
}
