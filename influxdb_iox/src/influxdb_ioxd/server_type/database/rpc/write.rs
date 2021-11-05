use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use chrono::Utc;
use tonic::Response;

use data_types::DatabaseName;
use generated_types::{
    google::{FieldViolation, FieldViolationExt},
    influxdata::iox::write::v1::*,
};
use mutable_batch::{DbWrite, WriteMeta};
use observability_deps::tracing::debug;
use server::{connection::ConnectionManager, Server};

use super::error::default_server_error_handler;

/// Implementation of the write service
struct WriteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

#[tonic::async_trait]
impl<M> write_service_server::WriteService for WriteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let span_ctx = request.extensions().get().cloned();
        let request = request.into_inner();

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = Utc::now().timestamp_nanos();
        let lp_data = request.lp_data;
        let db_name = DatabaseName::new(&request.db_name).field("db_name")?;

        let (tables, stats) = mutable_batch_lp::lines_to_batches_stats(&lp_data, default_time)
            .map_err(|e| FieldViolation {
                field: "lp_data".into(),
                description: format!("Invalid Line Protocol: {}", e),
            })?;

        debug!(%db_name, lp_line_count=stats.num_lines, body_size=lp_data.len(), num_fields=stats.num_fields, "Writing lines into database");

        let write = DbWrite::new(tables, WriteMeta::unsequenced(span_ctx));

        self.server
            .write(&db_name, write)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(WriteResponse {
            lines_written: stats.num_lines as u64,
        }))
    }

    async fn write_entry(
        &self,
        request: tonic::Request<WriteEntryRequest>,
    ) -> Result<tonic::Response<WriteEntryResponse>, tonic::Status> {
        let span_ctx = request.extensions().get().cloned();
        let request = request.into_inner();
        let db_name = DatabaseName::new(&request.db_name).field("db_name")?;

        if request.entry.is_empty() {
            return Err(FieldViolation::required("entry").into());
        }

        let entry = entry::Entry::try_from(request.entry).field("entry")?;
        let tables = mutable_batch_entry::entry_to_batches(&entry).map_err(|e| FieldViolation {
            field: "entry".into(),
            description: format!("Invalid Entry: {}", e),
        })?;
        let write = DbWrite::new(tables, WriteMeta::unsequenced(span_ctx));

        self.server
            .write(&db_name, write)
            .await
            .map_err(default_server_error_handler)?;

        Ok(Response::new(WriteEntryResponse {}))
    }
}

/// Instantiate the write service
pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    write_service_server::WriteServiceServer::new(WriteService { server })
}
