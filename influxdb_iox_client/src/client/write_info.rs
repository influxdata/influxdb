use self::generated_types::{write_info_service_client::WriteInfoServiceClient, *};

use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::ingester::v1::{
        write_info_service_client, write_info_service_server, GetWriteInfoRequest,
        GetWriteInfoResponse, KafkaPartitionInfo, KafkaPartitionStatus,
    };
    pub use generated_types::write_info::merge_responses;
}

/// A basic client for fetching information about write tokens from a
/// single ingester.
///
/// NOTE: This is an ALPHA / Internal API that is used as part of the
/// end to end tests.
///
/// A public API is tracked here:
/// <https://github.com/influxdata/influxdb_iox/issues/4354>
#[derive(Debug, Clone)]
pub struct Client {
    inner: WriteInfoServiceClient<Connection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(channel: Connection) -> Self {
        Self {
            inner: WriteInfoServiceClient::new(channel),
        }
    }

    /// Get the write information for a write token
    pub async fn get_write_info(
        &mut self,
        write_token: &str,
    ) -> Result<GetWriteInfoResponse, Error> {
        let response = self
            .inner
            .get_write_info(GetWriteInfoRequest {
                write_token: write_token.to_string(),
            })
            .await?;

        Ok(response.into_inner())
    }
}
