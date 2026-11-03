use nacelle_core::NacelleError;
use nacelle_core::request::NacelleConnectionMeta;
use nacelle_core::telemetry::{
    NacelleMetricsContext, NacelleTelemetry, NacelleTelemetryObserver, NacelleTransport,
};
use nacelle_core::tracking::NacelleConnectionActivity;

pub(super) trait Activity: Sync {
    const TRACKED: bool;
    fn established(&self, connection: &NacelleConnectionMeta);
    fn idle(&self);
    fn activate(&self) -> Result<(), NacelleError>;
    fn finishing(&self);
    fn tracked(&self) -> Option<&NacelleConnectionActivity>;
}

pub(super) struct Untracked;

impl Activity for Untracked {
    const TRACKED: bool = false;
    fn established(&self, _: &NacelleConnectionMeta) {}
    fn idle(&self) {}
    fn activate(&self) -> Result<(), NacelleError> {
        Ok(())
    }
    fn finishing(&self) {}
    fn tracked(&self) -> Option<&NacelleConnectionActivity> {
        None
    }
}

impl Activity for NacelleConnectionActivity {
    const TRACKED: bool = true;
    fn established(&self, connection: &NacelleConnectionMeta) {
        self.established(connection);
    }
    fn idle(&self) {
        self.idle();
    }
    fn activate(&self) -> Result<(), NacelleError> {
        if self.activate() {
            Ok(())
        } else {
            Err(NacelleError::ConnectionTerminated(
                self.close_reason()
                    .expect("administrative close won activation"),
            ))
        }
    }
    fn finishing(&self) {
        self.finishing();
    }
    fn tracked(&self) -> Option<&NacelleConnectionActivity> {
        Some(self)
    }
}

pub(super) struct CloseGuard<'a, Observer: NacelleTelemetryObserver> {
    activity: &'a NacelleConnectionActivity,
    telemetry: &'a NacelleTelemetry<Observer>,
    metrics: Option<&'a NacelleMetricsContext>,
    transport: NacelleTransport,
    completed: bool,
}

impl<'a, Observer: NacelleTelemetryObserver> CloseGuard<'a, Observer> {
    pub(super) fn new(
        activity: &'a NacelleConnectionActivity,
        telemetry: &'a NacelleTelemetry<Observer>,
        metrics: Option<&'a NacelleMetricsContext>,
        transport: NacelleTransport,
    ) -> Self {
        Self {
            activity,
            telemetry,
            metrics,
            transport,
            completed: false,
        }
    }

    pub(super) fn complete(&mut self, result: &Result<(), NacelleError>) {
        self.completed = true;
        self.telemetry.connection_closed(
            self.transport,
            self.metrics,
            self.activity.close_reason().map_or_else(
                || super::metrics::tcp_close_reason(result),
                nacelle_core::NacelleConnectionCloseReason::as_str,
            ),
        );
    }
}

impl<Observer: NacelleTelemetryObserver> Drop for CloseGuard<'_, Observer> {
    fn drop(&mut self) {
        if !self.completed {
            self.telemetry.connection_closed(
                self.transport,
                self.metrics,
                self.activity.close_reason().map_or(
                    "cancelled",
                    nacelle_core::NacelleConnectionCloseReason::as_str,
                ),
            );
        }
    }
}
