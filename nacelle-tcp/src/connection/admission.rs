use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use nacelle_core::request::{NacelleConnectionMeta, NacelleConnectionTlsMeta};
use nacelle_core::telemetry::{NacelleTelemetry, NacelleTelemetryObserver};
use nacelle_core::tracking::{NacelleConnectionActivity, NacelleConnectionRegistration};
use nacelle_core::{NacelleConfigurationError, NacelleError, NacelleRuntimeState};

/// Owned TCP/Unix admission held across application-owned setup and serving.
///
/// This non-cloneable token charges connection admission once, retains capacity
/// and optional tracking, and is bound to the exact runtime state that created
/// it. Clones of that state may consume it; other states and worker partitions
/// may not. Dropping it releases capacity but does not refund rate budget.
/// Serving keeps the controlled future pinned inline without boxing it.
///
/// Use [`Self::setup`] for cancellable setup, then pass the token to
/// [`super::serve_stream_with_admission`], [`super::serve_serial_stream_with_admission`],
/// or a server's `serve_io_with_admission` method. Automatic admission remains
/// the default on ordinary serving methods.
#[must_use = "retain admission through setup and transfer it into serving"]
pub struct TcpConnectionAdmission {
    registration: NacelleConnectionRegistration,
    runtime_state: NacelleRuntimeState,
    connection: NacelleConnectionMeta,
}

impl TcpConnectionAdmission {
    /// Reserve admission before performing transport setup, including TLS.
    ///
    /// Metadata fixes the peer, connection ID, listener, and accounting mode.
    /// Global and peer rates, concurrent capacity, and tracking use the same
    /// admission path as automatic serving. Rejection returns before setup.
    pub fn new<Observer: NacelleTelemetryObserver>(
        runtime_state: &NacelleRuntimeState,
        telemetry: &NacelleTelemetry<Observer>,
        connection: NacelleConnectionMeta,
    ) -> Result<Self, NacelleError> {
        telemetry.register_runtime_state(runtime_state.clone());
        let registration = runtime_state.register_connection(&connection)?;
        Ok(Self {
            registration,
            runtime_state: runtime_state.clone(),
            connection,
        })
    }

    /// Accepted metadata; identity and accounting cannot change during handoff.
    pub fn connection(&self) -> &NacelleConnectionMeta {
        &self.connection
    }

    /// Attach negotiated TLS metadata without changing admission identity.
    ///
    /// Tracking publishes this metadata when the serving driver establishes
    /// the connection. Until then the registry continues to report setup.
    pub fn with_tls(mut self, tls: NacelleConnectionTlsMeta) -> Self {
        self.connection.tls = Some(tls);
        self
    }

    /// Perform application-owned setup while retaining admission.
    ///
    /// Administrative close cancels tracked setup. Failure, cancellation, or
    /// dropping this future releases capacity after dropping owned setup work.
    /// Success returns the setup output and the same admission token.
    pub fn setup<F, T>(self, setup: F) -> impl Future<Output = Result<(T, Self), NacelleError>>
    where
        F: Future<Output = Result<T, NacelleError>>,
    {
        let Self {
            registration,
            runtime_state,
            connection,
        } = self;
        let controlled = registration.run_setup(setup);
        async move {
            let (value, registration) = controlled.await?;
            Ok((
                value,
                Self {
                    registration,
                    runtime_state,
                    connection,
                },
            ))
        }
    }

    pub(super) fn run<F>(
        self,
        runtime_state: NacelleRuntimeState,
        work: impl FnOnce(
            NacelleConnectionMeta,
            Option<NacelleConnectionActivity>,
            NacelleRuntimeState,
        ) -> F,
    ) -> impl Future<Output = Result<(), NacelleError>>
    where
        F: Future<Output = Result<(), NacelleError>>,
    {
        let same_runtime = self.runtime_state.shares_state_with(&runtime_state);
        let future = work(self.connection, self.registration.activity(), runtime_state);
        let controlled = self.registration.run(future);
        AdmittedConnection {
            controlled: Some(controlled),
            same_runtime,
        }
    }
}

pin_project_lite::pin_project! {
    // Keep one in-place copy instead of captured and awaiting copies.
    struct AdmittedConnection<F> {
        #[pin]
        controlled: Option<F>,
        same_runtime: bool,
    }
}

impl<F: Future<Output = Result<(), NacelleError>>> Future for AdmittedConnection<F> {
    type Output = Result<(), NacelleError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if !*this.same_runtime {
            this.controlled.set(None);
            return Poll::Ready(Err(
                NacelleConfigurationError::ConnectionAdmissionRuntime.into()
            ));
        }
        this.controlled
            .as_mut()
            .as_pin_mut()
            .expect("admitted connection future")
            .poll(cx)
    }
}
