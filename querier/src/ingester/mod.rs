use std::sync::Arc;

use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use data_types2::{
    ChunkAddr, ChunkId, ChunkOrder, IngesterQueryRequest, SequenceNumber, TableSummary,
};
use datafusion_util::MemoryStream;
use observability_deps::tracing::{debug, trace};
use predicate::{Predicate, PredicateMatch};
use query::{
    exec::{stringset::StringSet, IOxSessionContext},
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use schema::{selection::Selection, sort::SortKey, Schema};
use snafu::{ResultExt, Snafu};

use self::{
    flight_client::{Error as FlightClientError, FlightClient, FlightClientImpl, FlightError},
    test_util::MockIngesterConnection,
};

mod flight_client;
mod test_util;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(display("Internal error: failed to resolve ingester record batch types for column '{}' type '{}': {}",
                    column_name, data_type, source))]
    ConvertingRecordBatch {
        column_name: String,
        data_type: DataType,
        source: ArrowError,
    },

    #[snafu(display("Internal error creating record batch: {}", source))]
    CreatingRecordBatch { source: ArrowError },

    #[snafu(display("Internal error creating IOx schema: {}", source))]
    CreatingSchema { source: schema::Error },

    #[snafu(display("Failed ingester query '{}': {}", ingester_address, source))]
    RemoteQuery {
        ingester_address: String,
        source: FlightClientError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a new connection given Vec of `ingester_address` such as
/// "http://127.0.0.1:8083"
pub fn create_ingester_connection(ingester_addresses: Vec<String>) -> Arc<dyn IngesterConnection> {
    Arc::new(IngesterConnectionImpl::new(ingester_addresses))
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Handles communicating with the ingester(s) to retrieve
/// data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    async fn partitions(
        &self,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
    ) -> Result<Vec<Arc<IngesterPartition>>>;
}

// IngesterConnection that communicates with an ingester.
#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub(crate) struct IngesterConnectionImpl {
    ingester_addresses: Vec<String>,
    flight_client: Arc<dyn FlightClient>,
}

impl IngesterConnectionImpl {
    /// Create a new connection given a Vec of `ingester_address` such as
    /// "http://127.0.0.1:8083"
    pub fn new(ingester_addresses: Vec<String>) -> Self {
        Self::new_with_flight_client(ingester_addresses, Arc::new(FlightClientImpl::new()))
    }

    fn new_with_flight_client(
        ingester_addresses: Vec<String>,
        flight_client: Arc<dyn FlightClient>,
    ) -> Self {
        Self {
            ingester_addresses,
            flight_client,
        }
    }
}

#[async_trait]
impl IngesterConnection for IngesterConnectionImpl {
    /// Retrieve chunks from the ingester for the particular table and
    /// predicate
    async fn partitions(
        &self,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
    ) -> Result<Vec<Arc<IngesterPartition>>> {
        let mut partitions = vec![];

        // TODO make these requests in parallel
        for ingester_address in &self.ingester_addresses {
            let ingester_query_request = IngesterQueryRequest {
                namespace: namespace_name.to_string(),
                table: table_name.to_string(),
                columns: columns.clone(),
                predicate: Some(predicate.clone()),
            };

            let query_res = self
                .flight_client
                .query(ingester_address, ingester_query_request)
                .await;
            if let Err(FlightClientError::Flight {
                source: FlightError::GrpcError(status),
            }) = &query_res
            {
                if status.code() == tonic::Code::NotFound {
                    debug!(
                        %ingester_address,
                        %namespace_name,
                        %table_name,
                        "Ingester does not know namespace or table, skipping",
                    );
                    continue;
                }
            }
            let mut perform_query = query_res.context(RemoteQuerySnafu { ingester_address })?;

            // Gather up all the results (todo pass along partition information in metadata)
            let batches = perform_query.collect().await.expect("collecting");
            debug!(
                num_batches = batches.len(),
                "Received batches from ingester"
            );
            trace!(?batches, schema=?perform_query.schema(), "Detailed from ingester");

            // TODO split chunks up based on table partition, gather
            // sequence numbers per partition
            let parquet_max_sequence_number = None;

            let ingester_partition = IngesterPartition::try_new(
                ChunkId::new(),
                Arc::clone(&namespace_name),
                Arc::clone(&table_name),
                Arc::clone(&expected_schema),
                parquet_max_sequence_number,
                batches,
            )?;

            partitions.push(Arc::new(ingester_partition));
        }
        Ok(partitions)
    }
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog heirarchy:
///
/// ```text
/// (Catalog) Sequencer -> (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog
/// partition from a sequencer. Thus, there can be more than one
/// IngesterPartition for each table the ingester knows about.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    chunk_id: ChunkId,
    namespace_name: Arc<str>,
    table_name: Arc<str>,

    schema: Arc<Schema>,
    /// Maximum sequence number of persisted data for this partition in the ingester
    /// TODO make this not an option
    parquet_max_sequence_number: Option<SequenceNumber>,

    batches: Vec<RecordBatch>,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed
    /// `RecordBatches` into the correct types
    pub fn try_new(
        chunk_id: ChunkId,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        expected_schema: Arc<Schema>,
        parquet_max_sequence_number: Option<SequenceNumber>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        // ensure that the schema of the batches matches the required
        // output schema by CAST'ing to the needed type.
        //
        // This is needed because the flight client doesn't send
        // dictionaries (see comments on ensure_schema for more
        // details)
        let batches = batches
            .into_iter()
            .map(|batch| ensure_schema(batch, expected_schema.as_ref()))
            .collect::<Result<Vec<RecordBatch>>>()?;

        Ok(Self {
            chunk_id,
            namespace_name,
            table_name,
            schema: expected_schema,
            parquet_max_sequence_number,
            batches,
        })
    }
}

impl QueryChunkMeta for IngesterPartition {
    fn summary(&self) -> Option<&TableSummary> {
        None
    }

    fn schema(&self) -> Arc<Schema> {
        trace!(schema=?self.schema, "IngesterPartition schema");
        Arc::clone(&self.schema)
    }

    fn sort_key(&self) -> Option<&SortKey> {
        //Some(&self.sort_key)
        // Data is not sorted
        None
    }

    fn delete_predicates(&self) -> &[Arc<data_types2::DeletePredicate>] {
        &[]
    }
}

impl QueryChunk for IngesterPartition {
    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn addr(&self) -> data_types2::ChunkAddr {
        ChunkAddr {
            db_name: Arc::clone(&self.namespace_name),
            table_name: Arc::clone(&self.table_name),
            partition_key: Arc::from("TODO PARTITION KEY"),
            chunk_id: self.chunk_id,
        }
    }

    fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester runs dedup before creating the record batches so
        // when the querier gets them they have no duplicates
        false
    }

    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<PredicateMatch, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(PredicateMatch::Unknown)
    }

    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Selection<'_>,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, QueryChunkError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn read_filter(
        &self,
        _ctx: IOxSessionContext,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, QueryChunkError> {
        trace!(?predicate, ?selection, input_batches=?self.batches, "Reading data");

        // Apply selection to in-memory batch
        let batches = match selection {
            Selection::All => self.batches.clone(),
            Selection::Some(columns) => {
                let projection = self
                    .schema
                    .compute_select_indicies(columns)
                    // TODO real error
                    .expect("error with selection");

                self.batches
                    .iter()
                    .map(|batch| batch.project(&projection))
                    .collect::<std::result::Result<Vec<_>, ArrowError>>()
                    .expect("error with projection to batches")
            }
        };
        trace!(?predicate, ?selection, output_batches=?batches, input_batches=?self.batches, "Reading data");

        Ok(Box::pin(MemoryStream::new(batches)))
    }

    fn chunk_type(&self) -> &str {
        "IngesterPartition"
    }

    fn order(&self) -> ChunkOrder {
        // since this is always the 'most recent' chunk for this
        // partition, put it at the end
        ChunkOrder::new(u32::MAX).unwrap()
    }
}

/// Cast arrays in record batch to be the type of schema this is a
/// workaround for
/// https://github.com/influxdata/influxdb_iox/pull/4273 where the
/// flight API doesn't necessairly return the same schema as was
/// provided by the ingester.
///
/// Namely, dictionary encoded columns (e.g. tags) are returned as
/// `DataType::Utf8` even when they were sent as
/// `DataType::Dictionary(Int32, Utf8)`.
fn ensure_schema(batch: RecordBatch, expected_schema: &Schema) -> Result<RecordBatch> {
    let old_columns = batch.columns().iter();
    let desired_fields = expected_schema.iter().map(|(_, f)| f);

    let new_columns = old_columns
        .zip(desired_fields)
        .map(|(col, f)| {
            let data_type = f.data_type();
            arrow::compute::cast(col, data_type).context(ConvertingRecordBatchSnafu {
                column_name: f.name(),
                data_type: data_type.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(expected_schema.as_arrow(), new_columns).context(CreatingRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::{
        array::{ArrayRef, DictionaryArray, StringArray, TimestampNanosecondArray},
        datatypes::Int32Type,
    };
    use assert_matches::assert_matches;
    use influxdb_iox_client::flight::{
        generated_types::IngesterQueryResponseMetadata, PerformQuery,
    };
    use schema::builder::SchemaBuilder;
    use tokio::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn test_flight_handshake_error() {
        let mock_flight_client = Arc::new(MockFlightClient::from([(
            "addr1",
            Err(FlightClientError::Handshake {
                ingester_address: String::from("addr1"),
                source: FlightError::GrpcError(tonic::Status::internal("don't know")),
            }),
        )]));
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_internal_error() {
        let mock_flight_client = Arc::new(MockFlightClient::from([(
            "addr1",
            Err(FlightClientError::Flight {
                source: FlightError::GrpcError(tonic::Status::internal("cow exploded")),
            }),
        )]));
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_not_found() {
        let mock_flight_client = Arc::new(MockFlightClient::from([(
            "addr1",
            Err(FlightClientError::Flight {
                source: FlightError::GrpcError(tonic::Status::not_found("something")),
            }),
        )]));
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let partitions = get_partitions(&ingester_conn).await.unwrap();
        assert!(partitions.is_empty());
    }

    async fn get_partitions(
        ingester_conn: &IngesterConnectionImpl,
    ) -> Result<Vec<Arc<IngesterPartition>>, Error> {
        let namespace = Arc::from("namespace");
        let table = Arc::from("table");
        let columns = vec![String::from("col")];
        let schema = Arc::new(SchemaBuilder::new().build().unwrap());
        ingester_conn
            .partitions(namespace, table, columns, &Predicate::default(), schema)
            .await
    }

    #[derive(Debug)]
    struct MockFlightClient {
        responses: Mutex<
            HashMap<String, Result<PerformQuery<IngesterQueryResponseMetadata>, FlightClientError>>,
        >,
    }

    impl MockFlightClient {
        async fn ingester_conn(self: &Arc<Self>) -> IngesterConnectionImpl {
            let ingester_addresses = self.responses.lock().await.keys().cloned().collect();
            IngesterConnectionImpl::new_with_flight_client(
                ingester_addresses,
                Arc::clone(self) as _,
            )
        }
    }

    impl<const N: usize>
        From<
            [(
                &'static str,
                Result<PerformQuery<IngesterQueryResponseMetadata>, FlightClientError>,
            ); N],
        > for MockFlightClient
    {
        fn from(
            responses: [(
                &'static str,
                Result<PerformQuery<IngesterQueryResponseMetadata>, FlightClientError>,
            ); N],
        ) -> Self {
            Self {
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|(k, v)| (String::from(k), v))
                        .collect(),
                ),
            }
        }
    }

    #[async_trait]
    impl FlightClient for MockFlightClient {
        async fn query(
            &self,
            ingester_address: &str,
            _request: IngesterQueryRequest,
        ) -> Result<PerformQuery<IngesterQueryResponseMetadata>, FlightClientError> {
            self.responses
                .lock()
                .await
                .remove(ingester_address)
                .expect("Response not mocked")
        }
    }

    #[test]
    fn test_ingester_partition_type_cast() {
        let expected_schema = Arc::new(SchemaBuilder::new().tag("t").timestamp().build().unwrap());

        let cases = vec![
            // send a batch that matches the schema exactly
            RecordBatch::try_from_iter(vec![("t", dict_array()), ("time", ts_array())]).unwrap(),
            // Model what the ingester sends (dictionary decoded to string)
            RecordBatch::try_from_iter(vec![("t", string_array()), ("time", ts_array())]).unwrap(),
        ];

        for case in cases {
            let parquet_max_sequence_number = None;
            // Construct a partition and ensure it doesn't error
            let ingester_partition = IngesterPartition::try_new(
                ChunkId::new(),
                "ns".into(),
                "table".into(),
                Arc::clone(&expected_schema),
                parquet_max_sequence_number,
                vec![case],
            )
            .unwrap();

            for batch in &ingester_partition.batches {
                assert_eq!(batch.schema(), expected_schema.as_arrow());
            }
        }
    }

    fn ts_array() -> ArrayRef {
        Arc::new(
            [Some(1), Some(2), Some(3)]
                .iter()
                .collect::<TimestampNanosecondArray>(),
        )
    }

    fn string_array() -> ArrayRef {
        Arc::new(str_vec().iter().collect::<StringArray>())
    }

    fn dict_array() -> ArrayRef {
        Arc::new(
            str_vec()
                .iter()
                .copied()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    }

    fn str_vec() -> &'static [Option<&'static str>] {
        &[Some("foo"), Some("bar"), Some("baz")]
    }
}
