//! Metrics layer.

use std::{borrow::Cow, sync::Arc, task::Poll};

use async_trait::async_trait;
use futures::StreamExt;
use iox_time::{Time, TimeProvider};
use metric::{DurationHistogram, Metric, Registry};

use crate::{
    error::DynError,
    layer::{Layer, QueryResponse},
};

/// Metrics layer.
#[derive(Debug)]
pub struct MetricsLayer<L>
where
    L: Layer,
{
    /// Inner layer.
    inner: L,

    /// Metrics.
    metrics: Arc<Metrics>,

    /// Time provider.
    time_provider: Arc<dyn TimeProvider>,
}

impl<L> MetricsLayer<L>
where
    L: Layer,
{
    /// Create new metrics wrapper.
    pub fn new(
        inner: L,
        registry: &Registry,
        addr: &str,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let ingester_duration: Metric<DurationHistogram> = registry.register_metric(
            "ingester_duration",
            "ingester request query execution duration",
        );
        let ingester_duration_success = ingester_duration.recorder([
            ("result", Cow::from("success")),
            ("addr", Cow::from(addr.to_owned())),
        ]);
        let ingester_duration_error = ingester_duration.recorder([
            ("result", Cow::from("error")),
            ("addr", Cow::from(addr.to_owned())),
        ]);
        let ingester_duration_cancelled = ingester_duration.recorder([
            ("result", Cow::from("cancelled")),
            ("addr", Cow::from(addr.to_owned())),
        ]);

        Self {
            inner,
            metrics: Arc::new(Metrics {
                ingester_duration_success,
                ingester_duration_error,
                ingester_duration_cancelled,
            }),
            time_provider,
        }
    }
}

#[async_trait]
impl<L> Layer for MetricsLayer<L>
where
    L: Layer,
{
    type Request = L::Request;
    type ResponseMetadata = L::ResponseMetadata;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        let mut tracker = Tracker {
            t_start: self.time_provider.now(),
            metrics: Arc::clone(&self.metrics),
            time_provider: Arc::clone(&self.time_provider),
            res: None,
        };

        match self.inner.query(request).await {
            Ok(QueryResponse {
                metadata,
                mut payload,
            }) => Ok(QueryResponse {
                metadata,
                payload: futures::stream::poll_fn(move |cx| {
                    let res = payload.poll_next_unpin(cx);

                    match &res {
                        Poll::Ready(Some(Ok(_))) => {}
                        Poll::Ready(Some(Err(_))) => {
                            tracker.res = Some(Err(tracker.time_provider.now()));
                        }
                        Poll::Ready(None) => {
                            tracker.res = Some(Ok(tracker.time_provider.now()));
                        }
                        Poll::Pending => {}
                    }

                    res
                })
                .boxed(),
            }),
            Err(e) => {
                tracker.res = Some(Err(self.time_provider.now()));
                Err(e)
            }
        }
    }
}

/// All the metrics.
#[derive(Debug)]
struct Metrics {
    /// Time spent waiting for successful ingester queries
    ingester_duration_success: DurationHistogram,

    /// Time spent waiting for unsuccessful ingester queries
    ingester_duration_error: DurationHistogram,

    /// Time spent waiting for a request that was cancelled.
    ingester_duration_cancelled: DurationHistogram,
}

struct Tracker {
    t_start: Time,
    metrics: Arc<Metrics>,
    time_provider: Arc<dyn TimeProvider>,
    res: Option<Result<Time, Time>>,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        let (t_end, metric) = match &self.res {
            Some(Ok(t_end)) => (*t_end, &self.metrics.ingester_duration_success),
            Some(Err(t_end)) => (*t_end, &self.metrics.ingester_duration_error),
            None => (
                self.time_provider.now(),
                &self.metrics.ingester_duration_cancelled,
            ),
        };

        if let Some(duration) = t_end.checked_duration_since(self.t_start) {
            metric.record(duration);
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use iox_time::SystemProvider;
    use metric::Attributes;
    use tokio::{sync::Barrier, task::JoinSet};

    use crate::layers::testing::{TestLayer, TestResponse};

    use super::*;

    #[tokio::test]
    async fn test() {
        const N_CANCELLED: u64 = 20;
        const N_SUCCESSFUL: u64 = 3;
        const N_ERROR_EARLY: u64 = 5;
        const N_ERROR_LATE: u64 = 7;

        let barrier_1 = Arc::new(Barrier::new(N_CANCELLED as usize + 1));
        let barrier_2 = Arc::new(Barrier::new(N_CANCELLED as usize + 1));

        let registry = Registry::new();

        let l = TestLayer::<(), (), ()>::default();
        for _ in 0..N_ERROR_EARLY {
            l.mock_response(TestResponse::err(DynError::from("error 1")));
        }
        for _ in 0..N_ERROR_LATE {
            l.mock_response(
                TestResponse::ok(())
                    .with_ok_payload(())
                    .with_err_payload(DynError::from("error 2")),
            );
        }
        for _ in 0..N_SUCCESSFUL {
            l.mock_response(TestResponse::ok(()).with_ok_payload(()).with_ok_payload(()));
        }
        for _ in 0..N_CANCELLED {
            l.mock_response(
                TestResponse::ok(())
                    .with_initial_barrier(Arc::clone(&barrier_1))
                    .with_initial_barrier(Arc::clone(&barrier_2)),
            );
        }
        let l = Arc::new(MetricsLayer::new(
            l,
            &registry,
            "foo.bar",
            Arc::new(SystemProvider::new()),
        ));

        for _ in 0..N_ERROR_EARLY {
            l.query(()).await.unwrap_err();
        }
        for _ in 0..N_ERROR_LATE {
            l.query(())
                .await
                .unwrap()
                .payload
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
        }
        for _ in 0..N_SUCCESSFUL {
            l.query(())
                .await
                .unwrap()
                .payload
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
        }

        let mut join_set = JoinSet::new();
        for _ in 0..N_CANCELLED {
            let l = Arc::clone(&l);
            join_set.spawn(async move {
                l.query(()).await.unwrap();
                unreachable!("request should have been cancelled");
            });
        }

        barrier_1.wait().await;
        join_set.shutdown().await;

        assert_eq!(sample_count(&registry, "success"), N_SUCCESSFUL,);
        assert_eq!(
            sample_count(&registry, "error"),
            N_ERROR_EARLY + N_ERROR_LATE,
        );
        assert_eq!(sample_count(&registry, "cancelled"), N_CANCELLED,);
    }

    fn sample_count(registry: &Registry, result: &'static str) -> u64 {
        registry
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("result", result),
                ("addr", "foo.bar"),
            ]))
            .expect("failed to get observer")
            .fetch()
            .sample_count()
    }
}
