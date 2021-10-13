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
use influxdb_line_protocol::parse_lines;
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
        let request = request.into_inner();

        // The time, in nanoseconds since the epoch, to assign to any points that don't
        // contain a timestamp
        let default_time = Utc::now().timestamp_nanos();

        let db_name = DatabaseName::new(&request.db_name).field("db_name")?;
        let lp_data = request.lp_data;
        let lp_chars = lp_data.len();
        let mut num_fields = 0;

        let lines = parse_lines(&lp_data)
            .inspect(|line| {
                if let Ok(line) = line {
                    num_fields += line.field_set.len();
                }
            })
            .collect::<Result<Vec<_>, influxdb_line_protocol::Error>>()
            .map_err(|e| FieldViolation {
                field: "lp_data".into(),
                description: format!("Invalid Line Protocol: {}", e),
            })?;

        let lp_line_count = lines.len();
        debug!(%db_name, %lp_chars, lp_line_count, body_size=lp_data.len(), num_fields, "Writing lines into database");

        self.server
            .write_lines(&db_name, &lines, default_time)
            .await
            .map_err(default_server_error_handler)?;

        let lines_written = lp_line_count as u64;
        Ok(Response::new(WriteResponse { lines_written }))
    }

    async fn write_entry(
        &self,
        request: tonic::Request<WriteEntryRequest>,
    ) -> Result<tonic::Response<WriteEntryResponse>, tonic::Status> {
        let request = request.into_inner();
        let db_name = DatabaseName::new(&request.db_name).field("db_name")?;

        if request.entry.is_empty() {
            return Err(FieldViolation::required("entry").into());
        }

        let entry = entry::Entry::try_from(request.entry).field("entry")?;

        self.server
            .write_entry_local(&db_name, entry)
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
