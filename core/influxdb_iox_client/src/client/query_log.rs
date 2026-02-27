use ::generated_types::google::{FieldViolation, OptionalField};
use client_util::connection::GrpcConnection;
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};

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
    pub async fn get_log(&mut self) -> Result<Log, Error> {
        let mut stream = self.inner.get_log(GetLogRequest {}).await?.into_inner();

        // first message should be metadata
        let Some(first_msg) = stream.try_next().await? else {
            return Err(Error::InvalidResponse(FieldViolation::required(
                "first message",
            )));
        };
        let get_log_response::Data::Metadata(metadata) = first_msg.data.unwrap_field("data")?
        else {
            return Err(Error::InvalidResponse(FieldViolation::required(
                "metadata in first message",
            )));
        };

        Ok(Log {
            metadata,
            entries: stream
                .map_err(Error::from)
                .and_then(async move |msg| {
                    let get_log_response::Data::Entry(entry) = msg.data.unwrap_field("data")?
                    else {
                        return Err(Error::InvalidResponse(FieldViolation::required(
                            "entry in tail messages",
                        )));
                    };
                    Ok(entry)
                })
                .boxed(),
        })
    }
}

/// [`get_log`](Client::get_log) response.
pub struct Log {
    /// Metadata.
    pub metadata: Metadata,

    /// Stream of entries.
    pub entries: BoxStream<'static, Result<LogEntry, Error>>,
}

impl std::fmt::Debug for Log {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Log")
            .field("metadata", &self.metadata)
            .field("entries", &"<STREAM>")
            .finish()
    }
}
