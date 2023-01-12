use crate::{
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};
use generated_types::influxdata::iox::ingester::v1::{
    self as proto, persist_service_server::PersistService,
};
use tonic::{Request, Response};

#[derive(Debug)]
pub(crate) struct PersistNow<T, P> {
    buffer: T,
    persist_handle: P,
}

impl<T, P> PersistNow<T, P>
where
T: PartitionIter + Sync + 'static,
P: PersistQueue + Clone + Sync + 'static,
 {
    pub(crate) fn new(buffer: T, persist_handle: P) -> Self {
        Self {
            buffer,
            persist_handle,
        }
    }
}

#[tonic::async_trait]
impl<T, P> PersistService for PersistNow<T, P>
where
T: PartitionIter + Sync + 'static,
P: PersistQueue + Clone + Sync + 'static,
 {
    /// Handle the RPC request to persist immediately.
    async fn persist(
        &self,
        _request: Request<proto::PersistRequest>,
    ) -> Result<Response<proto::PersistResponse>, tonic::Status> {
        persist_partitions(self.buffer.partition_iter(), &self.persist_handle).await;

        Ok(Response::new(proto::PersistResponse {}))
    }
}
