//! Transport-owned scheduling for empty and registered checkpoints.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use nacelle_core::NacelleError;
use nacelle_core::policy::{Chain, NoPolicy, PolicyLease, Rejection};
use nacelle_core::telemetry::{NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver};
use tokio::io::AsyncWrite;

use crate::limits::NacelleTcpLimits;

use super::response::ResponseDelivery;

mod sealed {
    pub trait Slot {}
    pub trait Mode {}
}

/// Library-selected checkpoint presence; consumers cannot override it.
#[doc(hidden)]
pub trait Slot: sealed::Slot {
    type Mode;
}

impl sealed::Slot for NoPolicy {}
impl Slot for NoPolicy {
    type Mode = Empty;
}

impl<A, B> sealed::Slot for Chain<A, B> {}
impl<A, B> Slot for Chain<A, B> {
    type Mode = Registered;
}

/// No registered checkpoint.
#[doc(hidden)]
pub struct Empty;

/// At least one registered checkpoint.
#[doc(hidden)]
pub struct Registered;

impl sealed::Mode for Empty {}
impl sealed::Mode for Registered {}

#[doc(hidden)]
pub trait Combine<Other>: sealed::Mode {
    type Mode: FlushMode;
}

macro_rules! combine {
    ($first:ty, $second:ty, $result:ty) => {
        impl Combine<$second> for $first {
            type Mode = $result;
        }
    };
}

combine!(Empty, Empty, Empty);
combine!(Empty, Registered, Registered);
combine!(Registered, Empty, Registered);
combine!(Registered, Registered, Registered);

/// Delivery state borrowed only by a registered admission future.
#[doc(hidden)]
pub struct Io<'a, W, Observer> {
    pub(super) writer: &'a mut W,
    pub(super) delivery: &'a mut ResponseDelivery,
    pub(super) limits: &'a NacelleTcpLimits,
    pub(super) telemetry: &'a NacelleTelemetry<Observer>,
    pub(super) metrics: Option<&'a NacelleMetricsContext>,
    pub(super) phase_duration: bool,
    pub(super) failure: &'a mut Option<usize>,
}

#[doc(hidden)]
pub trait FlushMode: sealed::Mode {
    fn flush<'a, W, Observer>(
        required: bool,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + 'a
    where
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver;

    fn flush_send<'a, W, Observer>(
        required: bool,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + Send + 'a
    where
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver;
}

impl FlushMode for Empty {
    fn flush<'a, W, Observer>(
        _: bool,
        _: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + 'a
    where
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver,
    {
        EmptyAdmission
    }

    fn flush_send<'a, W, Observer>(
        _: bool,
        _: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + Send + 'a
    where
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver,
    {
        EmptyAdmission
    }
}

impl FlushMode for Registered {
    async fn flush<'a, W, Observer>(
        required: bool,
        io: Io<'a, W, Observer>,
    ) -> Result<(), NacelleError>
    where
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver,
    {
        if required {
            flush_delivery(io).await?;
        }
        Ok(())
    }

    async fn flush_send<'a, W, Observer>(
        required: bool,
        io: Io<'a, W, Observer>,
    ) -> Result<(), NacelleError>
    where
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver,
    {
        if required {
            flush_delivery(io).await?;
        }
        Ok(())
    }
}

/// Statically omit waiting and flushing state for an empty checkpoint.
#[doc(hidden)]
pub trait Mode<Lease: PolicyLease + 'static>: sealed::Mode {
    fn enter<'a, F, Make, W, Observer>(
        make: Make,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Lease, NacelleError>> + 'a
    where
        F: Future<Output = Result<Lease, Rejection>> + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver;

    fn enter_send<'a, F, Make, W, Observer>(
        make: Make,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Lease, NacelleError>> + Send + 'a
    where
        F: Future<Output = Result<Lease, Rejection>> + Send + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver,
        Lease: Send;
}

struct EmptyAdmission;

impl Future for EmptyAdmission {
    type Output = Result<(), NacelleError>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

impl Mode<()> for Empty {
    fn enter<'a, F, Make, W, Observer>(
        _: Make,
        _: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + 'a
    where
        F: Future<Output = Result<(), Rejection>> + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver,
    {
        EmptyAdmission
    }

    fn enter_send<'a, F, Make, W, Observer>(
        _: Make,
        _: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<(), NacelleError>> + Send + 'a
    where
        F: Future<Output = Result<(), Rejection>> + Send + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver,
    {
        EmptyAdmission
    }
}

impl<Lease: PolicyLease + 'static> Mode<Lease> for Registered {
    fn enter<'a, F, Make, W, Observer>(
        make: Make,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Lease, NacelleError>> + 'a
    where
        F: Future<Output = Result<Lease, Rejection>> + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + 'a,
        Observer: NacelleTelemetryObserver,
    {
        admit(make(), io)
    }

    fn enter_send<'a, F, Make, W, Observer>(
        make: Make,
        io: Io<'a, W, Observer>,
    ) -> impl Future<Output = Result<Lease, NacelleError>> + Send + 'a
    where
        F: Future<Output = Result<Lease, Rejection>> + Send + 'a,
        Make: FnOnce() -> F + 'a,
        W: AsyncWrite + Unpin + Send + 'a,
        Observer: NacelleTelemetryObserver,
        Lease: Send,
    {
        admit(make(), io)
    }
}

async fn admit<F, Lease, W, Observer>(
    admission: F,
    io: Io<'_, W, Observer>,
) -> Result<Lease, NacelleError>
where
    F: Future<Output = Result<Lease, Rejection>>,
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    tokio::pin!(admission);
    let first = std::future::poll_fn(|cx| Poll::Ready(admission.as_mut().poll(cx))).await;
    let result = match first {
        Poll::Ready(result) => result,
        Poll::Pending => {
            flush_delivery(io).await?;
            admission.await
        }
    };
    result.map_err(Rejection::into_error)
}

async fn flush_delivery<W, Observer>(io: Io<'_, W, Observer>) -> Result<(), NacelleError>
where
    W: AsyncWrite + Unpin,
    Observer: NacelleTelemetryObserver,
{
    io.delivery
        .flush(
            io.writer,
            io.limits,
            io.telemetry,
            io.metrics,
            io.phase_duration,
        )
        .await
        .inspect_err(|error| {
            *io.failure = Some(error.delivered_bytes);
        })
        .map_err(|error| error.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NacelleTcpConfig;
    use crate::policies::{NoTcpPolicies, SharedTcpPolicySet};

    #[test]
    fn no_policy_control_stores_underlying_future_once() {
        let future = std::future::ready([0_u8; 4096]);
        let future_bytes = std::mem::size_of_val(&future);
        let controlled = <NoTcpPolicies as SharedTcpPolicySet<(), ()>>::run(&NoTcpPolicies, future);
        assert_eq!(std::mem::size_of_val(&controlled), future_bytes);
    }

    #[test]
    fn empty_checkpoint_does_not_construct_admission_or_store_delivery_state() {
        let mut writer = tokio::io::sink();
        let mut delivery = ResponseDelivery::new(&NacelleTcpConfig::default());
        let mut failure = None;
        let limits = NacelleTcpLimits::default();
        let telemetry = NacelleTelemetry::default();
        let future = Empty::enter_send(
            || -> std::future::Ready<Result<(), Rejection>> {
                panic!("an empty checkpoint must not construct admission state")
            },
            Io {
                writer: &mut writer,
                delivery: &mut delivery,
                limits: &limits,
                telemetry: &telemetry,
                metrics: None,
                phase_duration: false,
                failure: &mut failure,
            },
        );
        assert_eq!(std::mem::size_of_val(&future), 0);
        drop(future);
        let flush = Empty::flush_send(
            true,
            Io {
                writer: &mut writer,
                delivery: &mut delivery,
                limits: &limits,
                telemetry: &telemetry,
                metrics: None,
                phase_duration: false,
                failure: &mut failure,
            },
        );
        assert_eq!(std::mem::size_of_val(&flush), 0);
    }
}
