//! Flight query observer for enterprise SLL (Service Level Logging).
//!
//! Provides trait definitions and stream wrappers that allow enterprise code
//! to observe Flight query lifecycle events (start, success, error, cancel).

use std::fmt::Debug;
use std::pin::Pin;
use std::task::Poll;

use arrow_flight::FlightData;
use futures::{Stream, ready};
use generated_types::tonic::Status;

use super::TonicStream;

/// Re-export so downstream crates use the same `tonic::Code` as the trait.
pub use generated_types::Code as GrpcCode;

/// Information about a Flight query, passed to the observer on start.
#[derive(Debug)]
pub struct FlightQueryInfo {
    /// Database name from the request ticket.
    pub database: String,
    /// Query variant: `"sql"`, `"influxql"`, or `"flightsql"`.
    pub query_variant: &'static str,
}

/// Handle for a single in-flight query observation.
///
/// The implementation tracks its own start time (from creation in
/// `on_query_start`). When the concrete implementation is dropped without
/// `success()` or `error()` being called, the `Drop` impl should treat it
/// as a cancellation.
pub trait FlightQueryObservation: Send + 'static {
    /// The query stream completed successfully.
    fn success(self: Box<Self>);
    /// The query failed with an error.
    fn error(self: Box<Self>, code: GrpcCode);
}

/// Observer for Flight query lifecycle events.
///
/// Enterprise code provides an SLL-emitting implementation.
pub trait FlightQueryObserver: Send + Sync + Debug + 'static {
    /// Called when a query starts (after auth, before execution).
    /// Returns an observation handle for tracking completion.
    fn on_query_start(&self, info: FlightQueryInfo) -> Box<dyn FlightQueryObservation>;
}

/// Wraps a `TonicStream<FlightData>` to notify an observer on completion.
///
/// When the stream ends normally, calls `observation.success()`.
/// When the stream yields an error, calls `observation.error()`.
/// When dropped without completing (client disconnect), the observation's
/// `Drop` impl fires — the enterprise impl logs a cancel entry.
pub(crate) struct ObservedStream {
    inner: TonicStream<FlightData>,
    observation: Option<Box<dyn FlightQueryObservation>>,
}

impl ObservedStream {
    pub(crate) fn new(
        inner: TonicStream<FlightData>,
        observation: Box<dyn FlightQueryObservation>,
    ) -> Self {
        Self {
            inner,
            observation: Some(observation),
        }
    }
}

impl Stream for ObservedStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = ready!(self.inner.as_mut().poll_next(cx));
        match result {
            Some(Ok(data)) => Poll::Ready(Some(Ok(data))),
            Some(Err(status)) => {
                if let Some(obs) = self.observation.take() {
                    obs.error(status.code());
                }
                Poll::Ready(Some(Err(status)))
            }
            None => {
                if let Some(obs) = self.observation.take() {
                    obs.success();
                }
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
