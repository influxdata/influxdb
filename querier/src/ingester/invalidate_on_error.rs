use std::sync::Arc;

use async_trait::async_trait;
use generated_types::ingester::IngesterQueryRequest;
use trace::ctx::SpanContext;

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
        let res = self
            .inner
            .query(Arc::clone(&ingester_addr), request, span_context)
            .await;

        // IOx vs borrow checker
        let is_err = if let Err(e) = &res {
            e.is_upstream_error()
        } else {
            false
        };

        if is_err {
            self.inner.invalidate_connection(ingester_addr).await;
        }

        res
    }
}
