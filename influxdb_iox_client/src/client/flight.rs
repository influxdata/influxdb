use arrow_deps::{
    arrow::{
        array::Array,
        datatypes::Schema,
        ipc::{self, reader},
        record_batch::RecordBatch,
    },
    arrow_flight::{
        flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch, FlightData,
        Ticket,
    },
};
use futures_util::stream::StreamExt;
use serde::Serialize;
use std::{convert::TryFrom, sync::Arc};
use tonic::Streaming;

use crate::errors::{GrpcError, GrpcQueryError};

/// An IOx Arrow Flight gRPC API client.
///
/// ```rust,no_run
/// #[tokio::main]
/// # async fn main() {
/// use data_types::database_rules::DatabaseRules;
/// use influxdb_iox_client::FlightClientBuilder;
///
/// let mut client = FlightClientBuilder::default()
///     .build("http://127.0.0.1:8082")
///     .await
///     .expect("client should be valid");
///
/// let mut query_results = client
///     .perform_query("my_database", "select * from cpu_load")
///     .await
///     .expect("query request should work");
///
/// let mut batches = vec![];
///
/// while let Some(data) = query_results.next().await.expect("valid batches") {
///     batches.push(data);
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct FlightClient {
    inner: FlightServiceClient<tonic::transport::Channel>,
}

impl FlightClient {
    pub(crate) async fn connect<D>(dst: D) -> Result<Self, GrpcError>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        Ok(Self {
            inner: FlightServiceClient::connect(dst).await?,
        })
    }

    /// Query the given database with the given SQL query, and return a
    /// [`PerformQuery`] instance that streams Arrow `RecordBatch` results.
    pub async fn perform_query(
        &mut self,
        database_name: impl Into<String>,
        sql_query: impl Into<String>,
    ) -> Result<PerformQuery, GrpcQueryError> {
        PerformQuery::new(self, database_name.into(), sql_query.into()).await
    }
}

// TODO: this should be shared
#[derive(Serialize, Debug)]
struct ReadInfo {
    database_name: String,
    sql_query: String,
}

/// A struct that manages the stream of Arrow `RecordBatch` results from an
/// Arrow Flight query. Created by calling the `perform_query` method on a
/// [`FlightClient`].
#[derive(Debug)]
pub struct PerformQuery {
    schema: Arc<Schema>,
    dictionaries_by_field: Vec<Option<Arc<dyn Array>>>,
    response: Streaming<FlightData>,
}

impl PerformQuery {
    pub(crate) async fn new(
        flight: &mut FlightClient,
        database_name: String,
        sql_query: String,
    ) -> Result<Self, GrpcQueryError> {
        let query = ReadInfo {
            database_name,
            sql_query,
        };

        let t = Ticket {
            ticket: serde_json::to_string(&query)?.into(),
        };
        let mut response = flight.inner.do_get(t).await?.into_inner();

        let flight_data_schema = response.next().await.ok_or(GrpcQueryError::NoSchema)??;
        let schema = Arc::new(Schema::try_from(&flight_data_schema)?);

        let dictionaries_by_field = vec![None; schema.fields().len()];

        Ok(Self {
            schema,
            dictionaries_by_field,
            response,
        })
    }

    /// Returns the next `RecordBatch` available for this query, or `None` if
    /// there are no further results available.
    pub async fn next(&mut self) -> Result<Option<RecordBatch>, GrpcQueryError> {
        let Self {
            schema,
            dictionaries_by_field,
            response,
        } = self;

        let mut data = match response.next().await {
            Some(d) => d?,
            None => return Ok(None),
        };

        let mut message = ipc::root_as_message(&data.data_header[..])
            .map_err(|e| GrpcQueryError::InvalidFlatbuffer(e.to_string()))?;

        while message.header_type() == ipc::MessageHeader::DictionaryBatch {
            reader::read_dictionary(
                &data.data_body,
                message
                    .header_as_dictionary_batch()
                    .ok_or(GrpcQueryError::CouldNotGetDictionaryBatch)?,
                &schema,
                dictionaries_by_field,
            )?;

            data = match response.next().await {
                Some(d) => d?,
                None => return Ok(None),
            };

            message = ipc::root_as_message(&data.data_header[..])
                .map_err(|e| GrpcQueryError::InvalidFlatbuffer(e.to_string()))?;
        }

        Ok(Some(flight_data_to_arrow_batch(
            &data,
            Arc::clone(&schema),
            &dictionaries_by_field,
        )?))
    }
}
