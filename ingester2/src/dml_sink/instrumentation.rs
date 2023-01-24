use async_trait::async_trait;
use dml::DmlOperation;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};

use super::DmlSink;

/// An instrumentation decorator over a [`DmlSink`] implementation.
///
/// This wrapper captures the latency distribution of the decorated
/// [`DmlSink::apply()`] call, faceted by success/error result.
#[derive(Debug)]
pub(crate) struct DmlSinkInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// Query execution duration distribution for successes.
    apply_duration_success: DurationHistogram,

    /// Query execution duration distribution for "not found" errors
    apply_duration_error: DurationHistogram,
}

impl<T> DmlSinkInstrumentation<T> {
    pub(crate) fn new(name: &'static str, inner: T, metrics: &metric::Registry) -> Self {
        // Record query duration metrics, broken down by query execution result
        let apply_duration: Metric<DurationHistogram> = metrics.register_metric(
            "ingester_dml_sink_apply_duration",
            "duration distribution of dml apply calls",
        );
        let apply_duration_success =
            apply_duration.recorder(&[("handler", name), ("result", "success")]);
        let apply_duration_error =
            apply_duration.recorder(&[("handler", name), ("result", "error")]);

        Self {
            inner,
            time_provider: Default::default(),
            apply_duration_success,
            apply_duration_error,
        }
    }
}

#[async_trait]
impl<T, P> DmlSink for DmlSinkInstrumentation<T, P>
where
    T: DmlSink,
    P: TimeProvider,
{
    type Error = T::Error;

    /// Apply `op` to the implementer's state.
    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        let t = self.time_provider.now();

        let res = self.inner.apply(op).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.apply_duration_success.record(delta),
                Err(_) => self.apply_duration_error.record(delta),
            };
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionId, PartitionKey, TableId};
    use iox_query::exec::Executor;
    use lazy_static::lazy_static;
    use metric::Attributes;

    use super::*;
    use crate::{
        buffer_tree::{namespace::NamespaceName, table::TableName},
        deferred_load::DeferredLoad,
        dml_sink::{mock_sink::MockDmlSink, DmlError},
        test_util::make_write_op,
    };

    const PARTITION_ID: PartitionId = PartitionId::new(42);
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(24);
    const TABLE_ID: TableId = TableId::new(2442);
    const TABLE_NAME: &str = "banana-report";
    const NAMESPACE_NAME: &str = "platanos";

    lazy_static! {
        static ref EXEC: Arc<Executor> = Arc::new(Executor::new_testing());
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("bananas");
        static ref NAMESPACE_NAME_LOADER: Arc<DeferredLoad<NamespaceName>> =
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NamespaceName::from(NAMESPACE_NAME)
            }));
        static ref TABLE_NAME_LOADER: Arc<DeferredLoad<TableName>> =
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            }));
    }

    const LAYER_NAME: &str = "test-bananas";

    macro_rules! test_metric {
        (
            $name:ident,
            ret = $ret:expr,
            want_metric_attr = $want_metric_attr:expr,
            want_ret = $($want_ret:tt)+
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_metric_ $name>]() {
                    let mock = MockDmlSink::default().with_apply_return([$ret]);
                    let metrics = metric::Registry::default();
                    let decorator = DmlSinkInstrumentation::new(LAYER_NAME, mock, &metrics);

                    let op = DmlOperation::Write(make_write_op(
                        &PARTITION_KEY,
                        NAMESPACE_ID,
                        TABLE_NAME,
                        TABLE_ID,
                        42,
                        "banana-report,tag=1 v=2 42424242",
                    ));

                    // Call the decorator and assert the return value
                    let got = decorator
                        .apply(op)
                        .await;
                    assert_matches!(got, $($want_ret)+);

                    // Validate the histogram with the specified attributes saw
                    // an observation
                    let histogram = metrics
                        .get_instrument::<Metric<DurationHistogram>>("ingester_dml_sink_apply_duration")
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
        ret = Ok(()),
        want_metric_attr = [("handler", LAYER_NAME), ("result", "success")],
        want_ret = Ok(_)
    );

    test_metric!(
        error,
        ret = Err(DmlError::Wal("broken".to_string())),
        want_metric_attr = [("handler", LAYER_NAME), ("result", "error")],
        want_ret = Err(DmlError::Wal(e)) => {
            assert_eq!(e, "broken");
        }
    );
}
