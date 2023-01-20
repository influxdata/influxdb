use crate::{
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, persist_service_server::PersistService,
};
use iox_catalog::interface::Catalog;
use std::sync::Arc;
use tonic::{Request, Response};

#[derive(Debug)]
pub(crate) struct PersistHandler<T, P> {
    buffer: T,
    persist_handle: P,
    catalog: Arc<dyn Catalog>,
}

impl<T, P> PersistHandler<T, P>
where
    T: PartitionIter + Sync + 'static,
    P: PersistQueue + Clone + Sync + 'static,
{
    pub(crate) fn new(buffer: T, persist_handle: P, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            buffer,
            persist_handle,
            catalog,
        }
    }
}

#[tonic::async_trait]
impl<T, P> PersistService for PersistHandler<T, P>
where
    T: PartitionIter + Sync + 'static,
    P: PersistQueue + Clone + Sync + 'static,
{
    /// Handle the RPC request to persist immediately. Will block until the data has persisted,
    /// which is useful in tests asserting on persisted data. May behave in unexpected ways if used
    /// concurrently with writes and ingester WAL rotations.
    async fn persist(
        &self,
        request: Request<proto::PersistRequest>,
    ) -> Result<Response<proto::PersistResponse>, tonic::Status> {
        let request = request.into_inner();

        let namespace = self
            .catalog
            .repositories()
            .await
            .namespaces()
            .get_by_name(&request.namespace)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .ok_or_else(|| tonic::Status::not_found(&request.namespace))?;

        persist_partitions(
            self.buffer
                .partition_iter()
                .filter(|p| p.lock().namespace_id() == namespace.id),
            &self.persist_handle,
        )
        .await;

        Ok(Response::new(proto::PersistResponse {}))
    }
}
