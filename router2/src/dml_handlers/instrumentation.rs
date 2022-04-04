use super::DmlHandler;
use async_trait::async_trait;
use data_types2::{DatabaseName, DeletePredicate};
use metric::{Metric, U64Histogram, U64HistogramOptions};
use time::{SystemProvider, TimeProvider};
use trace::{ctx::SpanContext, span::SpanRecorder};

/// An instrumentation decorator recording call latencies for [`DmlHandler`]
/// implementations.
///
/// Metrics are broken down by operation (write/delete) and result
/// (success/error) with call latency reported in milliseconds.
#[derive(Debug)]
pub struct InstrumentationDecorator<T, P = SystemProvider> {
    name: &'static str,
    inner: T,
    time_provider: P,

    write_success: U64Histogram,
    write_error: U64Histogram,

    delete_success: U64Histogram,
    delete_error: U64Histogram,
}

impl<T> InstrumentationDecorator<T> {
    /// Wrap a new [`InstrumentationDecorator`] over `T` exposing metrics
    /// labelled with `handler=name`.
    pub fn new(name: &'static str, registry: &metric::Registry, inner: T) -> Self {
        let buckets = || {
            U64HistogramOptions::new([
                5,
                10,
                20,
                40,
                80,
                160,
                320,
                640,
                1280,
                2560,
                5120,
                10240,
                20480,
                u64::MAX,
            ])
        };

        let write: Metric<U64Histogram> = registry.register_metric_with_options(
            "dml_handler_write_duration_ms",
            "write handler call duration in milliseconds",
            buckets,
        );
        let delete: Metric<U64Histogram> = registry.register_metric_with_options(
            "dml_handler_delete_duration_ms",
            "delete handler call duration in milliseconds",
            buckets,
        );

        let write_success = write.recorder(&[("handler", name), ("result", "success")]);
        let write_error = write.recorder(&[("handler", name), ("result", "error")]);

        let delete_success = delete.recorder(&[("handler", name), ("result", "success")]);
        let delete_error = delete.recorder(&[("handler", name), ("result", "error")]);

        Self {
            name,
            inner,
            time_provider: Default::default(),
            write_success,
            write_error,
            delete_success,
            delete_error,
        }
    }
}

#[async_trait]
impl<T> DmlHandler for InstrumentationDecorator<T>
where
    T: DmlHandler,
{
    type WriteInput = T::WriteInput;
    type WriteError = T::WriteError;
    type DeleteError = T::DeleteError;
    type WriteOutput = T::WriteOutput;

    /// Call the inner `write` method and record the call latency.
    async fn write(
        &self,
        namespace: &DatabaseName<'static>,
        input: Self::WriteInput,
        span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let t = self.time_provider.now();

        // Create a tracing span for this handler.
        let mut span_recorder =
            SpanRecorder::new(span_ctx.clone().map(|parent| parent.child(self.name)));

        let res = self.inner.write(namespace, input, span_ctx).await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => {
                    span_recorder.ok("success");
                    self.write_success.record(delta.as_millis() as _)
                }
                Err(e) => {
                    span_recorder.error(e.to_string());
                    self.write_error.record(delta.as_millis() as _)
                }
            };
        }

        res
    }

    /// Call the inner `delete` method and record the call latency.
    async fn delete(
        &self,
        namespace: &DatabaseName<'static>,
        table_name: &str,
        predicate: &DeletePredicate,
        span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        let t = self.time_provider.now();

        // Create a tracing span for this handler.
        let mut span_recorder =
            SpanRecorder::new(span_ctx.clone().map(|parent| parent.child(self.name)));

        let res = self
            .inner
            .delete(namespace, table_name, predicate, span_ctx)
            .await;

        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => {
                    span_recorder.ok("success");
                    self.delete_success.record(delta.as_millis() as _)
                }
                Err(e) => {
                    span_recorder.error(e.to_string());
                    self.delete_error.record(delta.as_millis() as _)
                }
            };
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dml_handlers::{mock::MockDmlHandler, DmlError};
    use assert_matches::assert_matches;
    use data_types2::TimestampRange;
    use metric::Attributes;
    use std::sync::Arc;
    use trace::{span::SpanStatus, RingBufferTraceCollector, TraceCollector};
    use write_summary::WriteSummary;

    const HANDLER_NAME: &str = "bananas";

    fn assert_metric_hit(
        metrics: &metric::Registry,
        metric_name: &'static str,
        result: &'static str,
    ) {
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>(metric_name)
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("handler", HANDLER_NAME),
                ("result", result),
            ]))
            .expect("failed to get observer")
            .fetch();

        let hit_count = histogram.buckets.iter().fold(0, |acc, v| acc + v.count);
        assert!(hit_count > 0, "metric did not record any calls");
    }

    fn assert_trace(traces: Arc<dyn TraceCollector>, status: SpanStatus) {
        let traces = traces
            .as_any()
            .downcast_ref::<RingBufferTraceCollector>()
            .expect("unexpected collector impl");

        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == HANDLER_NAME)
            .expect("tracing span not found");

        assert_eq!(
            span.status, status,
            "span status does not match expected value"
        );
    }

    fn summary() -> WriteSummary {
        WriteSummary::default()
    }

    #[tokio::test]
    async fn test_write_ok() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(summary())]));

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &*metrics, handler);

        decorator
            .write(&ns, (), Some(span))
            .await
            .expect("inner handler configured to succeed");

        assert_metric_hit(&*metrics, "dml_handler_write_duration_ms", "success");
        assert_trace(traces, SpanStatus::Ok);
    }

    #[tokio::test]
    async fn test_write_err() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(
            MockDmlHandler::default()
                .with_write_return([Err(DmlError::DatabaseNotFound("nope".to_owned()))]),
        );

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &*metrics, handler);

        let err = decorator
            .write(&ns, (), Some(span))
            .await
            .expect_err("inner handler configured to fail");

        assert_matches!(err, DmlError::DatabaseNotFound(_));

        assert_metric_hit(&*metrics, "dml_handler_write_duration_ms", "error");
        assert_trace(traces, SpanStatus::Err);
    }

    #[tokio::test]
    async fn test_delete_ok() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(MockDmlHandler::<()>::default().with_delete_return([Ok(())]));

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &*metrics, handler);

        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        decorator
            .delete(&ns, "a table", &pred, Some(span))
            .await
            .expect("inner handler configured to succeed");

        assert_metric_hit(&*metrics, "dml_handler_delete_duration_ms", "success");
        assert_trace(traces, SpanStatus::Ok);
    }

    #[tokio::test]
    async fn test_delete_err() {
        let ns = "platanos".try_into().unwrap();
        let handler = Arc::new(
            MockDmlHandler::<()>::default()
                .with_delete_return([Err(DmlError::DatabaseNotFound("nope".to_owned()))]),
        );

        let metrics = Arc::new(metric::Registry::default());
        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let decorator = InstrumentationDecorator::new(HANDLER_NAME, &*metrics, handler);

        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        decorator
            .delete(&ns, "a table", &pred, Some(span))
            .await
            .expect_err("inner handler configured to fail");

        assert_metric_hit(&*metrics, "dml_handler_delete_duration_ms", "error");
        assert_trace(traces, SpanStatus::Err);
    }
}
