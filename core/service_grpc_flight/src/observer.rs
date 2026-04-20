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
/// `Drop` impl fires - the enterprise impl logs a cancel entry.
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::{StreamExt, stream};
    use generated_types::tonic::Code;

    use super::*;

    /// Captures which callback was observed
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct ObservationState {
        success: bool,
        error: Option<GrpcCode>,
    }

    /// Shared observer that records which callback was called.
    ///
    /// Clones of this observer share the same inner state (and thus
    /// observations)
    #[derive(Debug, Clone)]
    struct SharedObserver {
        state: Arc<Mutex<ObservationState>>,
    }

    impl SharedObserver {
        /// Construct a new observer with empty state.
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(ObservationState::default())),
            }
        }

        /// Return a snapshot of the currently recorded state.
        fn state(&self) -> ObservationState {
            self.state.lock().expect("lock poisoned").clone()
        }
    }

    impl FlightQueryObservation for SharedObserver {
        fn success(self: Box<Self>) {
            self.state.lock().expect("lock poisoned").success = true;
        }

        fn error(self: Box<Self>, code: GrpcCode) {
            self.state.lock().expect("lock poisoned").error = Some(code);
        }
    }

    /// Build an observed stream plus shared state
    fn test_stream<I>(items: I) -> (ObservedStream, SharedObserver)
    where
        I: IntoIterator<Item = Result<FlightData, Status>>,
        I::IntoIter: Send + 'static,
    {
        let observer = SharedObserver::new();
        let inner: TonicStream<FlightData> = Box::pin(stream::iter(items));
        let observed = ObservedStream::new(inner, Box::new(observer.clone()));

        (observed, observer)
    }

    #[tokio::test]
    async fn test_observed_stream_success() {
        let (mut observed, observer) = test_stream([Ok(FlightData::default())]);

        assert!(observed.next().await.unwrap().is_ok());
        assert!(observed.next().await.is_none());

        assert_eq!(
            observer.state(),
            ObservationState {
                success: true,
                error: None,
            }
        );
    }

    #[tokio::test]
    async fn test_observed_stream_error() {
        let (mut observed, observer) = test_stream([Err(Status::new(Code::Internal, "boom"))]);

        let err = observed
            .next()
            .await
            .expect("stream should yield an item")
            .expect_err("stream should yield an error");
        assert_eq!(err.code(), Code::Internal);

        assert_eq!(
            observer.state(),
            ObservationState {
                success: false,
                error: Some(Code::Internal),
            }
        );
    }
}
