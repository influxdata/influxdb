use std::sync::Arc;

use async_trait::async_trait;
use ingester_query_grpc::IngesterQueryRequest;
use trace::{ctx::SpanContext, span::SpanRecorder};

use crate::ingester::flight_client::{Error as FlightClientError, IngesterFlightClient, QueryData};

#[derive(Debug)]
pub struct InvalidateOnErrorFlightClient {
    /// The underlying client.
    inner: Arc<dyn IngesterFlightClient>,
}

impl InvalidateOnErrorFlightClient {
    pub fn new(inner: Arc<dyn IngesterFlightClient>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl IngesterFlightClient for InvalidateOnErrorFlightClient {
    async fn invalidate_connection(&self, ingester_address: Arc<str>) {
        self.inner.invalidate_connection(ingester_address).await;
    }

    async fn query(
        &self,
        ingester_addr: Arc<str>,
        request: IngesterQueryRequest,
        span_context: Option<SpanContext>,
    ) -> Result<Box<dyn QueryData>, FlightClientError> {
        let span = span_context.map(|s| s.child("invalidator"));
        let mut span_recorder = SpanRecorder::new(span.clone());

        let res = self
            .inner
            .query(
                Arc::clone(&ingester_addr),
                request,
                span_recorder.span().map(|span| span.ctx.clone()),
            )
            .await;

        // IOx vs borrow checker
        let is_err = if let Err(e) = &res {
            e.is_upstream_error()
        } else {
            false
        };

        if is_err {
            self.inner.invalidate_connection(ingester_addr).await;
            span_recorder.event("invalidate connection");
        }

        res
    }
}
