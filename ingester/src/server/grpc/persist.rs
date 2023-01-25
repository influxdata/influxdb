use crate::handler::IngestHandler;
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, persist_service_server::PersistService,
};
use std::sync::Arc;
use tonic::{Request, Response};

#[derive(Debug)]
pub(crate) struct PersistHandler<I: IngestHandler> {
    ingest_handler: Arc<I>,
}

impl<I: IngestHandler> PersistHandler<I> {
    pub fn new(ingest_handler: Arc<I>) -> Self {
        Self { ingest_handler }
    }
}

#[tonic::async_trait]
impl<I: IngestHandler + 'static> PersistService for PersistHandler<I> {
    /// Handle the RPC request to persist immediately. This is useful in tests asserting on
    /// persisted data. May behave in unexpected ways if used concurrently with writes or lifecycle
    /// persists.
    async fn persist(
        &self,
        _request: Request<proto::PersistRequest>,
    ) -> Result<Response<proto::PersistResponse>, tonic::Status> {
        // Even though the request specifies the namespace, persist everything. This means tests
        // that use this API need to be using non-shared MiniClusters in order to avoid messing
        // with each others' states.
        self.ingest_handler.persist_all().await;

        Ok(Response::new(proto::PersistResponse {}))
    }
}
