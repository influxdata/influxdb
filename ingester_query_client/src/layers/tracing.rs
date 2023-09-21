//! Tracing layer.

use async_trait::async_trait;
use futures::StreamExt;
use std::{fmt::Debug, sync::Arc, task::Poll};
use trace::span::{Span, SpanRecorder};

use crate::{
    error::DynError,
    layer::{Layer, QueryResponse},
};

/// Tracing layer.
#[derive(Debug)]
pub struct TracingLayer<L, R>
where
    L: Layer<Request = (R, Option<Span>)>,
    R: Clone + Debug + Send + Sync + 'static,
{
    addr: Arc<str>,
    inner: L,
}

impl<L, R> TracingLayer<L, R>
where
    L: Layer<Request = (R, Option<Span>)>,
    R: Clone + Debug + Send + Sync + 'static,
{
    /// Create new tracing wrapper.
    pub fn new(inner: L, addr: Arc<str>) -> Self {
        Self { addr, inner }
    }
}

#[async_trait]
impl<L, R> Layer for TracingLayer<L, R>
where
    L: Layer<Request = (R, Option<Span>)>,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Request = (R, Option<Span>);
    type ResponseMetadata = L::ResponseMetadata;
    type ResponsePayload = L::ResponsePayload;

    async fn query(
        &self,
        request: Self::Request,
    ) -> Result<QueryResponse<Self::ResponseMetadata, Self::ResponsePayload>, DynError> {
        let (r, span) = request;

        let mut tracker = CancelationTracker {
            span_recorder: SpanRecorder::new(span),
            done: false,
        };

        tracker
            .span_recorder
            .set_metadata("addr", self.addr.as_ref().to_owned());

        match self
            .inner
            .query((r, tracker.span_recorder.child_span("ingester request")))
            .await
        {
            Ok(QueryResponse {
                metadata,
                mut payload,
            }) => {
                tracker
                    .span_recorder
                    .event("ingester response stream starts");

                Ok(QueryResponse {
                    metadata,
                    payload: futures::stream::poll_fn(move |cx| {
                        let res = payload.poll_next_unpin(cx);

                        match &res {
                            Poll::Ready(Some(Ok(_))) => {
                                tracker
                                    .span_recorder
                                    .event("ingester response stream response");
                            }
                            Poll::Ready(Some(Err(e))) => {
                                tracker.span_recorder.error(e.to_string());
                                tracker.done = true;
                            }
                            Poll::Ready(None) => {
                                tracker.span_recorder.ok("ingester response stream end");
                                tracker.done = true;
                            }
                            Poll::Pending => {}
                        }

                        res
                    })
                    .boxed(),
                })
            }
            Err(e) => {
                tracker.span_recorder.error(e.to_string());
                tracker.done = true;
                Err(e)
            }
        }
    }
}

struct CancelationTracker {
    span_recorder: SpanRecorder,
    done: bool,
}

impl Drop for CancelationTracker {
    fn drop(&mut self) {
        if !self.done {
            self.span_recorder.event("cancelled");
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use std::fmt::Write;
    use tokio::{sync::Barrier, task::JoinSet};
    use trace::RingBufferTraceCollector;

    use crate::{
        layers::testing::{TestLayer, TestResponse},
        testing::span,
    };

    use super::*;

    #[tokio::test]
    async fn test() {
        let barrier_1 = Arc::new(Barrier::new(2));
        let barrier_2 = Arc::new(Barrier::new(2));

        let l = TestLayer::<((), Option<Span>), (), ()>::default();
        l.mock_response(TestResponse::err(DynError::from("error 1")));
        l.mock_response(
            TestResponse::ok(())
                .with_ok_payload(())
                .with_err_payload(DynError::from("error 2")),
        );
        l.mock_response(TestResponse::ok(()).with_ok_payload(()).with_ok_payload(()));
        l.mock_response(
            TestResponse::ok(())
                .with_initial_barrier(Arc::clone(&barrier_1))
                .with_initial_barrier(barrier_2),
        );
        let l = TracingLayer::new(l, "foo.bar".into());

        let collector = Arc::new(RingBufferTraceCollector::new(100));

        let mut span = span();
        span.ctx.collector = Some(Arc::clone(&collector) as _);

        l.query(((), Some(span.clone()))).await.unwrap_err();
        l.query(((), Some(span.clone())))
            .await
            .unwrap()
            .payload
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        l.query(((), Some(span.clone())))
            .await
            .unwrap()
            .payload
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            l.query(((), Some(span))).await.unwrap();
            unreachable!("request should have been cancelled");
        });

        barrier_1.wait().await;
        join_set.shutdown().await;

        assert_eq!(
            format_spans(&collector.spans()).trim(),
            [
                "span:",
                "  name: span",
                "  status: Err",
                "  metadata:",
                "    addr: foo.bar",
                "  events:",
                "    error 1",
                "span:",
                "  name: span",
                "  status: Err",
                "  metadata:",
                "    addr: foo.bar",
                "  events:",
                "    ingester response stream starts",
                "    ingester response stream response",
                "    error 2",
                "span:",
                "  name: span",
                "  status: Ok",
                "  metadata:",
                "    addr: foo.bar",
                "  events:",
                "    ingester response stream starts",
                "    ingester response stream response",
                "    ingester response stream response",
                "    ingester response stream end",
                "span:",
                "  name: span",
                "  status: Unknown",
                "  metadata:",
                "    addr: foo.bar",
                "  events:",
                "    cancelled",
            ]
            .join("\n")
        );
    }

    fn format_spans(spans: &[Span]) -> String {
        let mut out = String::new();

        for span in spans {
            writeln!(&mut out, "span:",).unwrap();

            writeln!(&mut out, "  name: {}", span.name).unwrap();
            writeln!(&mut out, "  status: {:?}", span.status).unwrap();

            writeln!(&mut out, "  metadata:").unwrap();
            for (k, v) in &span.metadata {
                writeln!(&mut out, "    {}: {}", k, v.string().unwrap_or_default(),).unwrap();
            }

            writeln!(&mut out, "  events:").unwrap();
            for evt in &span.events {
                writeln!(&mut out, "    {}", evt.msg,).unwrap();
            }
        }

        out
    }
}
