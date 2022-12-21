use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};
use trace::span::Span;

use super::QueryExec;
use crate::query::QueryError;

/// An instrumentation decorator over a [`QueryExec`] implementation.
///
/// This wrapper captures the latency distribution of the decorated
/// [`QueryExec::query_exec()`] call, faceted by success/error result.
#[derive(Debug)]
pub(crate) struct QueryExecInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// Query execution duration distribution for successes.
    query_duration_success: DurationHistogram,

    /// Query execution duration distribution for "not found" errors
    query_duration_error_not_found: DurationHistogram,
}

impl<T> QueryExecInstrumentation<T> {
    pub(crate) fn new(name: &'static str, inner: T, metrics: &metric::Registry) -> Self {
        // Record query duration metrics, broken down by query execution result
        let query_duration: Metric<DurationHistogram> = metrics.register_metric(
            "ingester_flight_query_duration",
            "flight request query execution duration",
        );
        let query_duration_success =
            query_duration.recorder(&[("handler", name), ("result", "success")]);
        let query_duration_error_not_found = query_duration.recorder(&[
            ("handler", name),
            ("result", "error"),
            ("reason", "not_found"),
        ]);

        Self {
            inner,
            time_provider: Default::default(),
            query_duration_success,
            query_duration_error_not_found,
        }
    }
}

#[async_trait]
impl<T, P> QueryExec for QueryExecInstrumentation<T, P>
where
    T: QueryExec,
    P: TimeProvider,
{
    type Response = T::Response;

    #[inline(always)]
    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        let t = self.time_provider.now();

        let res = self
            .inner
            .query_exec(namespace_id, table_id, columns, span)
            .await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.query_duration_success.record(delta),
                Err(QueryError::TableNotFound { .. } | QueryError::NamespaceNotFound { .. }) => {
                    self.query_duration_error_not_found.record(delta)
                }
            };
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use metric::Attributes;

    use super::*;
    use crate::query::{
        mock_query_exec::MockQueryExec,
        response::{PartitionStream, QueryResponse},
    };

    const LAYER_NAME: &str = "test-bananas";

    macro_rules! test_metric {
        (
            $name:ident,
            inner = $inner:expr,
            want_metric_attr = $want_metric_attr:expr,
            want_ret = $($want_ret:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_metric_ $name>]() {
                    let metrics = metric::Registry::default();
                    let decorator = QueryExecInstrumentation::new(LAYER_NAME, $inner, &metrics);

                    // Call the decorator and assert the return value
                    let got = decorator
                        .query_exec(NamespaceId::new(42), TableId::new(24), vec![], None)
                        .await;
                    assert_matches!(got, $($want_ret)+);

                    // Validate the histogram with the specified attributes saw
                    // an observation
                    let histogram = metrics
                        .get_instrument::<Metric<DurationHistogram>>("ingester_flight_query_duration")
                        .expect("failed to find metric")
                        .get_observer(&Attributes::from(&$want_metric_attr))
                        .expect("failed to find attributes")
                        .fetch();
                    assert_eq!(histogram.sample_count(), 1);
                }
            }
        };
    }

    test_metric!(
        ok,
        inner = {
            let stream: PartitionStream = PartitionStream::new(futures::stream::iter([]));
            MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)))
        },
        want_metric_attr = [("handler", LAYER_NAME), ("result", "success")],
        want_ret = Ok(_)
    );

    test_metric!(
        namespace_not_found,
        inner = MockQueryExec::default()
            .with_result(Err(QueryError::NamespaceNotFound(NamespaceId::new(42)))),
        want_metric_attr = [("handler", LAYER_NAME), ("result", "error"), ("reason", "not_found")],
        want_ret = Err(QueryError::NamespaceNotFound(ns)) => {
            assert_eq!(ns, NamespaceId::new(42));
        }
    );

    test_metric!(
        table_not_found,
        inner = MockQueryExec::default()
            .with_result(Err(QueryError::TableNotFound(NamespaceId::new(42), TableId::new(24)))),
        want_metric_attr = [("handler", LAYER_NAME), ("result", "error"), ("reason", "not_found")],
        want_ret = Err(QueryError::TableNotFound(ns, t)) => {
            assert_eq!(ns, NamespaceId::new(42));
            assert_eq!(t, TableId::new(24));
        }
    );
}
