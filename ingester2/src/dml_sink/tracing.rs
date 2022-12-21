use std::borrow::Cow;

use async_trait::async_trait;
use dml::DmlOperation;
use trace::span::SpanRecorder;

use super::DmlSink;

/// An tracing decorator over a [`DmlSink`] implementation.
///
/// This wrapper emits child tracing spans covering the execution of the inner
/// [`DmlSink::apply()`] call.
///
/// Constructing this decorator is cheap.
#[derive(Debug)]
pub(crate) struct DmlSinkTracing<T> {
    inner: T,
    name: Cow<'static, str>,
}

impl<T> DmlSinkTracing<T> {
    pub(crate) fn new(inner: T, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }
}

#[async_trait]
impl<T> DmlSink for DmlSinkTracing<T>
where
    T: DmlSink,
{
    type Error = T::Error;

    /// Apply `op` to the implementer's state, emitting a trace for the duration.
    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        let span = op.meta().span_context().map(|x| x.child(self.name.clone()));
        let mut recorder = SpanRecorder::new(span);

        match self.inner.apply(op).await {
            Ok(v) => {
                recorder.ok("apply complete");
                Ok(v)
            }
            Err(e) => {
                recorder.error(e.to_string());
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionId, PartitionKey, TableId};
    use dml::DmlMeta;
    use iox_query::exec::Executor;
    use lazy_static::lazy_static;
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector, TraceCollector};

    use crate::{
        buffer_tree::{namespace::NamespaceName, table::TableName},
        deferred_load::DeferredLoad,
        dml_sink::{mock_sink::MockDmlSink, DmlError},
        test_util::make_write_op,
    };

    use super::*;

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

    #[track_caller]
    fn assert_trace(name: impl Into<String>, status: SpanStatus, traces: &dyn TraceCollector) {
        let traces = traces
            .as_any()
            .downcast_ref::<RingBufferTraceCollector>()
            .expect("unexpected collector impl");

        let name = name.into();
        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("tracing span {name} not found"));

        assert_eq!(
            span.status, status,
            "span status does not match expected value"
        );
    }

    #[tokio::test]
    async fn test_ok() {
        let mock = MockDmlSink::default().with_apply_return([Ok(())]);

        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let mut op = DmlOperation::Write(make_write_op(
            &PARTITION_KEY,
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            42,
            "banana-report,tag=1 v=2 42424242",
        ));

        // Populate the metadata with a span context.
        let meta = op.meta();
        op.set_meta(DmlMeta::sequenced(
            *meta.sequence().unwrap(),
            meta.producer_ts().unwrap(),
            Some(span),
            42,
        ));

        // Drive the trace wrapper
        DmlSinkTracing::new(mock, "bananas")
            .apply(op)
            .await
            .expect("wrapper should not modify result");

        // Assert the trace showed up.
        assert_trace("bananas", SpanStatus::Ok, &*traces);
    }

    #[tokio::test]
    async fn test_err() {
        let mock =
            MockDmlSink::default().with_apply_return([Err(DmlError::Wal("broken".to_string()))]);

        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        let mut op = DmlOperation::Write(make_write_op(
            &PARTITION_KEY,
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            42,
            "banana-report,tag=1 v=2 42424242",
        ));

        // Populate the metadata with a span context.
        let meta = op.meta();
        op.set_meta(DmlMeta::sequenced(
            *meta.sequence().unwrap(),
            meta.producer_ts().unwrap(),
            Some(span),
            42,
        ));

        // Drive the trace wrapper
        let got = DmlSinkTracing::new(mock, "bananas")
            .apply(op)
            .await
            .expect_err("wrapper should not modify result");
        assert_matches!(got, DmlError::Wal(s) => {
            assert_eq!(s, "broken");
        });

        // Assert the trace showed up.
        assert_trace("bananas", SpanStatus::Err, &*traces);
    }
}
