use std::borrow::Cow;

use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use trace::span::{Span, SpanRecorder};

use super::QueryExec;
use crate::query::QueryError;

/// An tracing decorator over a [`QueryExec`] implementation.
///
/// This wrapper emits child tracing spans covering the execution of the inner
/// [`QueryExec::query_exec()`] call.
///
/// Constructing this decorator is cheap.
#[derive(Debug)]
pub(crate) struct QueryExecTracing<T> {
    inner: T,
    name: Cow<'static, str>,
}

impl<T> QueryExecTracing<T> {
    pub(crate) fn new(inner: T, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }
}

#[async_trait]
impl<T> QueryExec for QueryExecTracing<T>
where
    T: QueryExec,
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
        let span = span.map(|s| s.child(self.name.clone()));
        let mut recorder = SpanRecorder::new(span.clone());

        match self
            .inner
            .query_exec(namespace_id, table_id, columns, span)
            .await
        {
            Ok(v) => {
                recorder.ok("query_exec complete");
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
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector, TraceCollector};

    use crate::query::{
        mock_query_exec::MockQueryExec,
        response::{PartitionStream, QueryResponse},
    };

    use super::*;

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
        let stream: PartitionStream = PartitionStream::new(futures::stream::iter([]));
        let mock = MockQueryExec::default().with_result(Ok(QueryResponse::new(stream)));

        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        // Drive the trace wrapper
        let _ = QueryExecTracing::new(mock, "bananas")
            .query_exec(
                NamespaceId::new(42),
                TableId::new(24),
                vec![],
                Some(span.child("root span")),
            )
            .await
            .expect("wrapper should not modify result");

        // Assert the trace showed up.
        assert_trace("bananas", SpanStatus::Ok, &*traces);
    }

    #[tokio::test]
    async fn test_err() {
        let mock = MockQueryExec::default()
            .with_result(Err(QueryError::NamespaceNotFound(NamespaceId::new(42))));

        let traces: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));
        let span = SpanContext::new(Arc::clone(&traces));

        // Drive the trace wrapper
        let got = QueryExecTracing::new(mock, "bananas")
            .query_exec(
                NamespaceId::new(42),
                TableId::new(24),
                vec![],
                Some(span.child("root span")),
            )
            .await
            .expect_err("wrapper should not modify result");
        assert_matches!(got, QueryError::NamespaceNotFound(ns) => {
            assert_eq!(ns, NamespaceId::new(42));
        });

        // Assert the trace showed up.
        assert_trace("bananas", SpanStatus::Err, &*traces);
    }
}
