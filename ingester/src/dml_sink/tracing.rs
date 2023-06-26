use std::borrow::Cow;

use async_trait::async_trait;
use trace::span::SpanRecorder;

use crate::dml_payload::IngestOp;

use super::DmlSink;

/// An tracing decorator over a [`DmlSink`] implementation.
///
/// This wrapper emits child tracing spans covering the execution of the inner
/// [`DmlSink::apply()`] call.
///
/// Constructing this decorator is cheap.
#[derive(Debug, Clone)]
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
    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
        let span = op.span_context().map(|x| x.child(self.name.clone()));
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
    use super::*;
    use crate::{
        dml_sink::{mock_sink::MockDmlSink, DmlError},
        test_util::{
            make_write_op, ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME,
        },
    };
    use assert_matches::assert_matches;
    use std::sync::Arc;
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector, TraceCollector};

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

        let op = IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            42,
            &format!("{},tag=1 v=2 42424242", &*ARBITRARY_TABLE_NAME),
            Some(span),
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

        let op = IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            42,
            &format!("{},tag=1 v=2 42424242", &*ARBITRARY_TABLE_NAME),
            Some(span),
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
