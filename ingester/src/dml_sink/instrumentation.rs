use async_trait::async_trait;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};

use crate::dml_payload::IngestOp;

use super::DmlSink;

/// An instrumentation decorator over a [`DmlSink`] implementation.
///
/// This wrapper captures the latency distribution of the decorated
/// [`DmlSink::apply()`] call, faceted by success/error result.
#[derive(Debug, Clone)]
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
    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
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
    use super::*;
    use crate::{
        dml_sink::{mock_sink::MockDmlSink, DmlError},
        test_util::{
            make_write_op, ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME,
        },
    };
    use assert_matches::assert_matches;
    use metric::Attributes;

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

                    let op = IngestOp::Write(make_write_op(
                        &ARBITRARY_PARTITION_KEY,
                        ARBITRARY_NAMESPACE_ID,
                        &ARBITRARY_TABLE_NAME,
                        ARBITRARY_TABLE_ID,
                        42,
                        &format!("{},tag=1 v=2 42424242", &*ARBITRARY_TABLE_NAME),
                        None,
                    ));

                    // Call the decorator and assert the return value
                    let got = decorator
                        .apply(op)
                        .await;
                    assert_matches!(got, $($want_ret)+);

                    // Validate the histogram with the specified attributes saw
                    // an observation
                    let histogram = metrics
                        .get_instrument::<Metric<DurationHistogram>>(
                            "ingester_dml_sink_apply_duration"
                        )
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
