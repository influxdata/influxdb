use std::sync::Arc;

use generated_types::influxdata::iox::ingester::v1::{
    self as proto, write_info_service_server::WriteInfoService,
};
use observability_deps::tracing::*;
use tonic::{Request, Response};
use write_summary::WriteSummary;

use crate::handler::IngestHandler;

/// Implementation of write info
pub(super) struct WriteInfoServiceImpl {
    handler: Arc<dyn IngestHandler + Send + Sync + 'static>,
}

impl WriteInfoServiceImpl {
    pub fn new(handler: Arc<dyn IngestHandler + Send + Sync + 'static>) -> Self {
        Self { handler }
    }
}

#[tonic::async_trait]
impl WriteInfoService for WriteInfoServiceImpl {
    async fn get_write_info(
        &self,
        request: Request<proto::GetWriteInfoRequest>,
    ) -> Result<Response<proto::GetWriteInfoResponse>, tonic::Status> {
        let proto::GetWriteInfoRequest { write_token } = request.into_inner();

        let write_summary =
            WriteSummary::try_from_token(&write_token).map_err(tonic::Status::invalid_argument)?;

        let progresses = self.handler.progresses(write_summary.shard_indexes()).await;

        let shard_infos = progresses
            .into_iter()
            .map(|(shard_index, progress)| {
                let status = write_summary
                    .write_status(shard_index, &progress)
                    .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

                let shard_index = shard_index.get();
                let status = proto::ShardStatus::from(status);
                debug!(shard_index, ?status, "write info status",);
                Ok(proto::ShardInfo {
                    shard_index,
                    status: status.into(),
                })
            })
            .collect::<Result<Vec<_>, tonic::Status>>()?;

        Ok(tonic::Response::new(proto::GetWriteInfoResponse {
            shard_infos,
        }))
    }
}
