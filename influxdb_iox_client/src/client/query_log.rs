use client_util::connection::GrpcConnection;

use self::generated_types::{query_log_service_client::QueryLogServiceClient, *};
use crate::connection::Connection;
use crate::error::Error;

/// Re-export generated_types
pub mod generated_types {
    pub use generated_types::influxdata::iox::querier::v1::*;
}

/// A basic client for working with the query log.
#[derive(Debug, Clone)]
pub struct Client {
    inner: QueryLogServiceClient<GrpcConnection>,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self {
            inner: QueryLogServiceClient::new(connection.into_grpc_connection()),
        }
    }

    /// Get log.
    pub async fn get_log(&mut self) -> Result<GetLogResponse, Error> {
        Ok(self.inner.get_log(GetLogRequest {}).await?.into_inner())
    }
}
