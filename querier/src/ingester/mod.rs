use self::{
    flight_client::{Error as FlightClientError, FlightClient, FlightClientImpl, FlightError},
    test_util::MockIngesterConnection,
};
use crate::{cache::CatalogCache, chunk::util::create_basic_summary};
use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use async_trait::async_trait;
use client_util::connection;
use data_types::{
    ChunkId, ChunkOrder, IngesterMapping, KafkaPartition, PartitionId, SequenceNumber, SequencerId,
    TableSummary, TimestampMinMax,
};
use datafusion_util::MemoryStream;
use futures::{stream::FuturesUnordered, TryStreamExt};
use generated_types::{
    influxdata::iox::ingester::v1::GetWriteInfoResponse,
    ingester::{encode_proto_predicate_as_base64, IngesterQueryRequest},
    write_info::merge_responses,
};
use influxdb_iox_client::flight::{
    generated_types::IngesterQueryResponseMetadata, low_level::LowLevelMessage,
};
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::compute_timenanosecond_min_max,
    QueryChunk, QueryChunkError, QueryChunkMeta,
};
use iox_time::{Time, TimeProvider};
use metric::{DurationHistogram, Metric};
use observability_deps::tracing::{debug, info, trace, warn};
use predicate::Predicate;
use schema::{selection::Selection, sort::SortKey, Schema};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use trace::span::{Span, SpanRecorder};

pub(crate) mod flight_client;
pub(crate) mod test_util;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "Internal error: \
        ingester record batch for column '{}' has type '{}' but should have type '{}'",
        column_name,
        actual_data_type,
        desired_data_type
    ))]
    RecordBatchType {
        column_name: String,
        actual_data_type: DataType,
        desired_data_type: DataType,
    },

    #[snafu(display(
        "Internal error: \
        failed to resolve ingester record batch types for column '{}' type '{}': {}",
        column_name,
        data_type,
        source
    ))]
    ConvertingRecordBatch {
        column_name: String,
        data_type: DataType,
        source: ArrowError,
    },

    #[snafu(display("Cannot convert schema: {}", source))]
    ConvertingSchema { source: schema::Error },

    #[snafu(display("Internal error creating record batch: {}", source))]
    CreatingRecordBatch { source: ArrowError },

    #[snafu(display("Failed ingester query '{}': {}", ingester_address, source))]
    RemoteQuery {
        ingester_address: String,
        source: FlightClientError,
    },

    #[snafu(display("Failed to connect to ingester '{}': {}", ingester_address, source))]
    Connecting {
        ingester_address: String,
        source: connection::Error,
    },

    #[snafu(display(
        "Error retrieving write info from '{}' for write token '{}': {}",
        ingester_address,
        write_token,
        source,
    ))]
    WriteInfo {
        ingester_address: String,
        write_token: String,
        source: influxdb_iox_client::error::Error,
    },

    #[snafu(display(
        "Partition status missing for partition {partition_id}, ingestger: {ingester_address}"
    ))]
    PartitionStatusMissing {
        partition_id: PartitionId,
        ingester_address: String,
    },

    #[snafu(display("Got batch without chunk information from ingester: {ingester_address}"))]
    BatchWithoutChunk { ingester_address: String },

    #[snafu(display("Got chunk without partition information from ingester: {ingester_address}"))]
    ChunkWithoutPartition { ingester_address: String },

    #[snafu(display(
        "Duplicate partition info for partition {partition_id}, ingestger: {ingester_address}"
    ))]
    DuplicatePartitionInfo {
        partition_id: PartitionId,
        ingester_address: String,
    },

    #[snafu(display(
        "No ingester found in sequencer to ingester mapping for sequencer {sequencer_id}"
    ))]
    NoIngesterFoundForSequencer { sequencer_id: KafkaPartition },

    #[snafu(display(
        "Sequencer {sequencer_id} was neither mapped to an ingester nor marked ignore"
    ))]
    SequencerNotMapped { sequencer_id: KafkaPartition },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a new set of connections given a map of sequencer IDs to Ingester configurations
pub fn create_ingester_connections_by_sequencer(
    sequencer_to_ingesters: HashMap<i32, IngesterMapping>,
    catalog_cache: Arc<CatalogCache>,
) -> Arc<dyn IngesterConnection> {
    Arc::new(IngesterConnectionImpl::by_sequencer(
        sequencer_to_ingesters,
        catalog_cache,
    ))
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
    ///
    /// # Panics
    ///
    /// Panics if the list of sequencer_ids is empty.
    #[allow(clippy::too_many_arguments)]
    async fn partitions(
        &self,
        sequencer_ids: &[KafkaPartition],
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>>;

    /// Returns the most recent partition sstatus info across all ingester(s) for the specified
    /// write token.
    async fn get_write_info(&self, write_token: &str) -> Result<GetWriteInfoResponse>;

    /// Return backend as [`Any`] which can be used to downcast to a specifc implementation.
    fn as_any(&self) -> &dyn Any;
}

/// Structure that holds metrics for ingester connections.
#[derive(Debug)]
struct IngesterConnectionMetrics {
    /// Time spent waiting for successful ingester queries
    ingester_duration_success: DurationHistogram,

    /// Time spent waiting for unsuccessful ingester queries
    ingester_duration_error: DurationHistogram,

    /// Time spent waiting for a request that was cancelled.
    ingester_duration_cancelled: DurationHistogram,
}

impl IngesterConnectionMetrics {
    fn new(metric_registry: &metric::Registry) -> Self {
        let ingester_duration: Metric<DurationHistogram> = metric_registry.register_metric(
            "ingester_duration",
            "ingester request query execution duration",
        );
        let ingester_duration_success = ingester_duration.recorder(&[("result", "success")]);
        let ingester_duration_error = ingester_duration.recorder(&[("result", "error")]);
        let ingester_duration_cancelled = ingester_duration.recorder(&[("result", "cancelled")]);

        Self {
            ingester_duration_success,
            ingester_duration_error,
            ingester_duration_cancelled,
        }
    }
}

/// Helper to observe a single ingester request.
///
/// Use [`set_ok`](Self::set_ok) or [`set_err`](Self::set_err) if an ingester result was observered. Otherwise the
/// request will count as "cancelled".
struct ObserveIngesterRequest<'a> {
    res: Option<Result<(), ()>>,
    t_start: Time,
    time_provider: Arc<dyn TimeProvider>,
    metrics: Arc<IngesterConnectionMetrics>,
    request: GetPartitionForIngester<'a>,
    span_recorder: SpanRecorder,
}

impl<'a> ObserveIngesterRequest<'a> {
    fn new(
        request: GetPartitionForIngester<'a>,
        metrics: Arc<IngesterConnectionMetrics>,
        span_recorder: &SpanRecorder,
    ) -> Self {
        let time_provider = request.catalog_cache.time_provider();
        let t_start = time_provider.now();
        let span_recorder = span_recorder.child("flight request");

        Self {
            res: None,
            t_start,
            time_provider,
            metrics,
            request,
            span_recorder,
        }
    }

    fn span_recorder(&self) -> &SpanRecorder {
        &self.span_recorder
    }

    fn set_ok(mut self) {
        self.res = Some(Ok(()));
        self.span_recorder.ok("done");
    }

    fn set_err(mut self) {
        self.res = Some(Err(()));
        self.span_recorder.error("failed");
    }
}

impl<'a> Drop for ObserveIngesterRequest<'a> {
    fn drop(&mut self) {
        let t_end = self.time_provider.now();

        if let Some(ingester_duration) = t_end.checked_duration_since(self.t_start) {
            let (metric, status) = match self.res {
                None => (&self.metrics.ingester_duration_cancelled, "cancelled"),
                Some(Ok(())) => (&self.metrics.ingester_duration_success, "success"),
                Some(Err(())) => (&self.metrics.ingester_duration_error, "error"),
            };

            metric.record(ingester_duration);

            info!(
                predicate=?self.request.predicate,
                namespace=%self.request.namespace_name,
                table_name=%self.request.table_name,
                ?ingester_duration,
                status,
                "Time spent in ingester"
            );
        }
    }
}

/// IngesterConnection that communicates with an ingester.
#[derive(Debug)]
pub struct IngesterConnectionImpl {
    sequencer_to_ingesters: HashMap<KafkaPartition, IngesterMapping>,
    unique_ingester_addresses: HashSet<Arc<str>>,
    flight_client: Arc<dyn FlightClient>,
    catalog_cache: Arc<CatalogCache>,
    metrics: Arc<IngesterConnectionMetrics>,
}

impl IngesterConnectionImpl {
    /// Create a new set of connections given a map of sequencer IDs to Ingester addresses, such as:
    ///
    /// ```json
    /// {
    ///   "sequencers": {
    ///     "0": {
    ///       "ingesters": [
    ///         {"addr": "http://ingester-0:8082"},
    ///         {"addr": "http://ingester-3:8082"}
    ///       ]
    ///     },
    ///     "1": { "ingesters": [{"addr": "http://ingester-1:8082"}]},
    ///   }
    /// }
    /// ```
    pub fn by_sequencer(
        sequencer_to_ingesters: HashMap<i32, IngesterMapping>,
        catalog_cache: Arc<CatalogCache>,
    ) -> Self {
        Self::by_sequencer_with_flight_client(
            sequencer_to_ingesters,
            Arc::new(FlightClientImpl::new()),
            catalog_cache,
        )
    }

    /// Create new set of connections with specific flight client implementation.
    ///
    /// This is helpful for testing, i.e. when the flight client should not be backed by normal
    /// network communication.
    pub fn by_sequencer_with_flight_client(
        sequencer_to_ingesters: HashMap<i32, IngesterMapping>,
        flight_client: Arc<dyn FlightClient>,
        catalog_cache: Arc<CatalogCache>,
    ) -> Self {
        let unique_ingester_addresses: HashSet<_> = sequencer_to_ingesters
            .values()
            .flat_map(|v| match v {
                IngesterMapping::Addr(addr) => Some(addr),
                _ => None,
            })
            .cloned()
            .collect();
        let sequencer_to_ingesters = sequencer_to_ingesters
            .into_iter()
            .map(|(sequencer_id, ingester_address)| {
                (KafkaPartition::new(sequencer_id), ingester_address)
            })
            .collect();

        let metric_registry = catalog_cache.metric_registry();
        let metrics = Arc::new(IngesterConnectionMetrics::new(&metric_registry));

        Self {
            sequencer_to_ingesters,
            unique_ingester_addresses,
            flight_client,
            catalog_cache,
            metrics,
        }
    }
}

/// Struct that names all parameters to `execute`
#[derive(Debug, Clone)]
struct GetPartitionForIngester<'a> {
    flight_client: Arc<dyn FlightClient>,
    catalog_cache: Arc<CatalogCache>,
    ingester_address: Arc<str>,
    namespace_name: Arc<str>,
    table_name: Arc<str>,
    columns: Vec<String>,
    predicate: &'a Predicate,
    expected_schema: Arc<Schema>,
}

/// Fetches the partitions for a single ingester
async fn execute(
    request: GetPartitionForIngester<'_>,
    span_recorder: &SpanRecorder,
) -> Result<Vec<IngesterPartition>> {
    let GetPartitionForIngester {
        flight_client,
        catalog_cache,
        ingester_address,
        namespace_name,
        table_name,
        columns,
        predicate,
        expected_schema,
    } = request;

    let ingester_query_request = IngesterQueryRequest {
        namespace: namespace_name.to_string(),
        table: table_name.to_string(),
        columns: columns.clone(),
        predicate: Some(predicate.clone()),
    };

    let query_res = flight_client
        .query(Arc::clone(&ingester_address), ingester_query_request)
        .await;

    if let Err(FlightClientError::Flight {
        source: FlightError::GrpcError(status),
    }) = &query_res
    {
        if status.code() == tonic::Code::NotFound {
            debug!(
                ingester_address=ingester_address.as_ref(),
                %namespace_name,
                %table_name,
                "Ingester does not know namespace or table, skipping",
            );
            return Ok(vec![]);
        }
    }
    let mut perform_query = query_res
        .context(RemoteQuerySnafu {
            ingester_address: ingester_address.as_ref(),
        })
        .map_err(|e| {
            // generate a warning that is sufficient to replicate the request using CLI tooling
            warn!(
                e=%e,
                ingester_address=ingester_address.as_ref(),
                namespace=namespace_name.as_ref(),
                table=table_name.as_ref(),
                columns=columns.join(",").as_str(),
                predicate_str=%predicate,
                predicate_binary=encode_predicate_as_base64(predicate).as_str(),
                "Failed to perform ingester query",
            );

            //  need to return error until https://github.com/rust-lang/rust/issues/91345 is stable
            e
        })?;

    // collect data from IO stream
    // Drain data from ingester so we don't block the ingester while performing catalog IO or CPU computations
    let mut messages = vec![];
    while let Some(data) = perform_query
        .next()
        .await
        .map_err(|source| FlightClientError::Flight { source })
        .context(RemoteQuerySnafu {
            ingester_address: ingester_address.as_ref(),
        })?
    {
        messages.push(data);
    }

    // reconstruct partitions
    let mut decoder = IngesterStreamDecoder::new(
        ingester_address,
        table_name,
        catalog_cache,
        expected_schema,
        span_recorder.child_span("IngesterStreamDecoder"),
    );
    for (msg, md) in messages {
        decoder.register(msg, md).await?;
    }

    decoder.finalize().await
}

/// Helper to disassemble the data from the ingester Apache Flight arrow stream.
///
/// This should be used AFTER the stream was drained because we will perform some catalog IO and this should likely not
/// block the ingester.
struct IngesterStreamDecoder {
    finished_partitions: HashMap<PartitionId, IngesterPartition>,
    current_partition: Option<IngesterPartition>,
    current_chunk: Option<(Schema, Vec<RecordBatch>)>,
    ingester_address: Arc<str>,
    table_name: Arc<str>,
    catalog_cache: Arc<CatalogCache>,
    expected_schema: Arc<Schema>,
    span_recorder: SpanRecorder,
}

impl IngesterStreamDecoder {
    /// Create empty decoder.
    fn new(
        ingester_address: Arc<str>,
        table_name: Arc<str>,
        catalog_cache: Arc<CatalogCache>,
        expected_schema: Arc<Schema>,
        span: Option<Span>,
    ) -> Self {
        Self {
            finished_partitions: HashMap::new(),
            current_partition: None,
            current_chunk: None,
            ingester_address,
            table_name,
            catalog_cache,
            expected_schema,
            span_recorder: SpanRecorder::new(span),
        }
    }

    /// FLush current chunk, if any.
    fn flush_chunk(&mut self) -> Result<()> {
        if let Some((schema, batches)) = self.current_chunk.take() {
            let current_partition = self
                .current_partition
                .take()
                .expect("Partition should have been checked before chunk creation");
            self.current_partition =
                Some(current_partition.try_add_chunk(ChunkId::new(), Arc::new(schema), batches)?);
        }

        Ok(())
    }

    /// FLush current partition, if any.
    ///
    /// This will also flush the current chunk.
    async fn flush_partition(&mut self) -> Result<()> {
        self.flush_chunk()?;

        if let Some(current_partition) = self.current_partition.take() {
            let schemas: Vec<_> = current_partition
                .chunks()
                .iter()
                .map(|c| c.schema())
                .collect();
            let primary_keys: Vec<_> = schemas.iter().map(|s| s.primary_key()).collect();
            let primary_key: HashSet<_> = primary_keys
                .iter()
                .flat_map(|pk| pk.iter().copied())
                .collect();
            let partition_sort_key = self
                .catalog_cache
                .partition()
                .sort_key(
                    current_partition.partition_id(),
                    &primary_key,
                    self.span_recorder
                        .child_span("cache GET partition sort key"),
                )
                .await;
            let current_partition = current_partition.with_partition_sort_key(partition_sort_key);
            self.finished_partitions
                .insert(current_partition.partition_id, current_partition);
        }

        Ok(())
    }

    /// Register a new message and its metadata from the Flight stream.
    async fn register(
        &mut self,
        msg: LowLevelMessage,
        md: IngesterQueryResponseMetadata,
    ) -> Result<(), Error> {
        match msg {
            LowLevelMessage::None => {
                // new partition announced
                self.flush_partition().await?;

                let partition_id = PartitionId::new(md.partition_id);
                let status = md.status.context(PartitionStatusMissingSnafu {
                    partition_id,
                    ingester_address: self.ingester_address.as_ref(),
                })?;
                ensure!(
                    !self.finished_partitions.contains_key(&partition_id),
                    DuplicatePartitionInfoSnafu {
                        partition_id,
                        ingester_address: self.ingester_address.as_ref()
                    },
                );
                let sequencer_id = self
                    .catalog_cache
                    .partition()
                    .sequencer_id(
                        partition_id,
                        self.span_recorder
                            .child_span("cache GET partition sequencer ID"),
                    )
                    .await;

                // Use a temporary empty partition sort key. We are going to fetch this AFTER we know all chunks because
                // then we are able to detect all relevant primary key columns that the sort key must cover.
                let partition_sort_key = Arc::new(None);

                let partition = IngesterPartition::new(
                    Arc::clone(&self.ingester_address),
                    Arc::clone(&self.table_name),
                    partition_id,
                    sequencer_id,
                    status.parquet_max_sequence_number.map(SequenceNumber::new),
                    status
                        .tombstone_max_sequence_number
                        .map(SequenceNumber::new),
                    partition_sort_key,
                );
                self.current_partition = Some(partition);
            }
            LowLevelMessage::Schema(schema) => {
                self.flush_chunk()?;
                ensure!(
                    self.current_partition.is_some(),
                    ChunkWithoutPartitionSnafu {
                        ingester_address: self.ingester_address.as_ref()
                    }
                );

                // don't use the transmitted arrow schema to construct the IOx schema because some metadata might be
                // missing. Instead select the right columns from the expected schema.
                let column_names: Vec<_> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                let schema = self
                    .expected_schema
                    .select_by_names(&column_names)
                    .context(ConvertingSchemaSnafu)?;
                self.current_chunk = Some((schema, vec![]));
            }
            LowLevelMessage::RecordBatch(batch) => {
                let current_chunk =
                    self.current_chunk
                        .as_mut()
                        .context(BatchWithoutChunkSnafu {
                            ingester_address: self.ingester_address.as_ref(),
                        })?;
                current_chunk.1.push(batch);
            }
        }

        Ok(())
    }

    /// Flush internal state and return sorted set of partitions.
    async fn finalize(mut self) -> Result<Vec<IngesterPartition>> {
        self.flush_partition().await?;

        let mut ids: Vec<_> = self.finished_partitions.keys().copied().collect();
        ids.sort();

        let partitions = ids
            .into_iter()
            .map(|id| {
                self.finished_partitions
                    .remove(&id)
                    .expect("just got key from this map")
            })
            .collect();
        self.span_recorder.ok("finished");
        Ok(partitions)
    }
}

fn encode_predicate_as_base64(predicate: &Predicate) -> String {
    use generated_types::influxdata::iox::ingester::v1::Predicate as ProtoPredicate;

    let predicate = match ProtoPredicate::try_from(predicate.clone()) {
        Ok(predicate) => predicate,
        Err(_) => {
            return String::from("<invalid>");
        }
    };

    match encode_proto_predicate_as_base64(&predicate) {
        Ok(s) => s,
        Err(_) => String::from("<encoding-error>"),
    }
}

#[async_trait]
impl IngesterConnection for IngesterConnectionImpl {
    /// Retrieve chunks from the ingester for the particular table, sequencer, and predicate
    async fn partitions(
        &self,
        sequencer_ids: &[KafkaPartition],
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: Arc<Schema>,
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>> {
        // If no sequencer IDs are specified, no ingester addresses can be found. This is a
        // configuration problem somewhere.
        assert!(
            !sequencer_ids.is_empty(),
            "Called `IngesterConnection.partitions` with an empty `sequencer_ids` list",
        );
        let mut span_recorder = SpanRecorder::new(span);

        let metrics = Arc::clone(&self.metrics);

        let measured_ingester_request = |ingester_address| {
            let request = GetPartitionForIngester {
                flight_client: Arc::clone(&self.flight_client),
                catalog_cache: Arc::clone(&self.catalog_cache),
                ingester_address,
                namespace_name: Arc::clone(&namespace_name),
                table_name: Arc::clone(&table_name),
                columns: columns.clone(),
                predicate,
                expected_schema: Arc::clone(&expected_schema),
            };
            let metrics = Arc::clone(&metrics);

            // wrap `execute` into an additional future so that we can measure the request time
            // INFO: create the measurement structure outside of the async block so cancellation is
            // always measured
            let measure_me = ObserveIngesterRequest::new(request.clone(), metrics, &span_recorder);
            async move {
                let res = execute(request.clone(), measure_me.span_recorder()).await;

                match &res {
                    Ok(_) => measure_me.set_ok(),
                    Err(_) => measure_me.set_err(),
                }

                res
            }
        };

        // Look up the ingesters needed for the sequencer. Collect into a HashSet to avoid making
        // multiple requests to the same ingester if that ingester is responsible for multiple
        // sequencer_ids relevant to this query.
        let mut relevant_ingester_addresses = HashSet::new();

        for sequencer_id in sequencer_ids {
            match self.sequencer_to_ingesters.get(sequencer_id) {
                None => {
                    return NoIngesterFoundForSequencerSnafu {
                        sequencer_id: *sequencer_id,
                    }
                    .fail()
                }
                Some(mapping) => match mapping {
                    IngesterMapping::Addr(addr) => {
                        relevant_ingester_addresses.insert(Arc::clone(addr));
                    }
                    IngesterMapping::Ignore => (),
                    IngesterMapping::NotMapped => {
                        return SequencerNotMappedSnafu {
                            sequencer_id: *sequencer_id,
                        }
                        .fail()
                    }
                },
            }
        }

        let mut ingester_partitions: Vec<IngesterPartition> = relevant_ingester_addresses
            .into_iter()
            .map(move |ingester_address| measured_ingester_request(ingester_address))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| {
                span_recorder.error("failed");
                e
            })?
            // We have a Vec<Vec<..>> flatten to Vec<_>
            .into_iter()
            .flatten()
            .collect();

        ingester_partitions.sort_by_key(|p| p.partition_id);
        span_recorder.ok("done");
        Ok(ingester_partitions)
    }

    async fn get_write_info(&self, write_token: &str) -> Result<GetWriteInfoResponse> {
        let responses = self
            .unique_ingester_addresses
            .iter()
            .map(|ingester_address| execute_get_write_infos(ingester_address, write_token))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await?;

        Ok(merge_responses(responses))
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

async fn execute_get_write_infos(
    ingester_address: &str,
    write_token: &str,
) -> Result<GetWriteInfoResponse, Error> {
    let connection = connection::Builder::new()
        .build(ingester_address)
        .await
        .context(ConnectingSnafu { ingester_address })?;

    influxdb_iox_client::write_info::Client::new(connection)
        .get_write_info(write_token)
        .await
        .context(WriteInfoSnafu {
            ingester_address,
            write_token,
        })
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog hierarchy:
///
/// ```text
/// (Catalog) Sequencer -> (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog
/// partition from a sequencer. Thus, there can be more than one
/// IngesterPartition for each table the ingester knows about.
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    ingester: Arc<str>,
    table_name: Arc<str>,
    partition_id: PartitionId,
    sequencer_id: SequencerId,

    /// Maximum sequence number of parquet files the ingester has
    /// persisted for this partition
    parquet_max_sequence_number: Option<SequenceNumber>,

    /// Maximum sequence number of tombstone that the ingester has
    /// persisted for this partition
    tombstone_max_sequence_number: Option<SequenceNumber>,

    /// Partition-wide sort key.
    partition_sort_key: Arc<Option<SortKey>>,

    chunks: Vec<IngesterChunk>,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed
    /// `RecordBatches` into the correct types
    pub fn new(
        ingester: Arc<str>,
        table_name: Arc<str>,
        partition_id: PartitionId,
        sequencer_id: SequencerId,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
        partition_sort_key: Arc<Option<SortKey>>,
    ) -> Self {
        Self {
            ingester,
            table_name,
            partition_id,
            sequencer_id,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            partition_sort_key,
            chunks: vec![],
        }
    }

    /// Try to add a new chunk to this partition.
    pub(crate) fn try_add_chunk(
        mut self,
        chunk_id: ChunkId,
        expected_schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        // ignore chunk if there are no batches
        if batches.is_empty() {
            return Ok(self);
        }

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

        // TODO: may want to ask the Ingester to send this value instead of computing it here.
        let ts_min_max = compute_timenanosecond_min_max(&batches).expect("Should have time range");

        let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64;
        let summary = Arc::new(create_basic_summary(
            row_count,
            &expected_schema,
            ts_min_max,
        ));

        let chunk = IngesterChunk {
            chunk_id,
            table_name: Arc::clone(&self.table_name),
            partition_id: self.partition_id,
            schema: expected_schema,
            partition_sort_key: Arc::clone(&self.partition_sort_key),
            batches,
            ts_min_max,
            summary,
        };

        self.chunks.push(chunk);

        Ok(self)
    }

    /// Update partition sort key
    pub(crate) fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key: Arc::clone(&partition_sort_key),
            chunks: self
                .chunks
                .into_iter()
                .map(|c| c.with_partition_sort_key(Arc::clone(&partition_sort_key)))
                .collect(),
            ..self
        }
    }

    pub(crate) fn ingester(&self) -> &Arc<str> {
        &self.ingester
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub(crate) fn sequencer_id(&self) -> SequencerId {
        self.sequencer_id
    }

    pub(crate) fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.parquet_max_sequence_number
    }

    pub(crate) fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
        self.tombstone_max_sequence_number
    }

    pub(crate) fn chunks(&self) -> &[IngesterChunk] {
        &self.chunks
    }

    pub(crate) fn into_chunks(self) -> Vec<IngesterChunk> {
        self.chunks
    }
}

#[derive(Debug, Clone)]
pub struct IngesterChunk {
    chunk_id: ChunkId,
    table_name: Arc<str>,
    partition_id: PartitionId,
    schema: Arc<Schema>,

    /// Partition-wide sort key.
    partition_sort_key: Arc<Option<SortKey>>,

    /// The raw table data
    batches: Vec<RecordBatch>,

    /// Timestamp-specific stats
    ts_min_max: TimestampMinMax,

    /// Summary Statistics
    summary: Arc<TableSummary>,
}

impl IngesterChunk {
    pub(crate) fn with_partition_sort_key(self, partition_sort_key: Arc<Option<SortKey>>) -> Self {
        Self {
            partition_sort_key,
            ..self
        }
    }

    pub(crate) fn estimate_size(&self) -> usize {
        self.batches
            .iter()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| array.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum::<usize>()
    }

    pub(crate) fn rows(&self) -> usize {
        self.batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
    }
}

impl QueryChunkMeta for IngesterChunk {
    fn summary(&self) -> Option<Arc<TableSummary>> {
        Some(Arc::clone(&self.summary))
    }

    fn schema(&self) -> Arc<Schema> {
        trace!(schema=?self.schema, "IngesterChunk schema");
        Arc::clone(&self.schema)
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().as_ref()
    }

    fn partition_id(&self) -> Option<PartitionId> {
        Some(self.partition_id)
    }

    fn sort_key(&self) -> Option<&SortKey> {
        //Some(&self.sort_key)
        // Data is not sorted
        None
    }

    fn delete_predicates(&self) -> &[Arc<data_types::DeletePredicate>] {
        &[]
    }

    fn timestamp_min_max(&self) -> Option<TimestampMinMax> {
        Some(self.ts_min_max)
    }
}

impl QueryChunk for IngesterChunk {
    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester runs dedup before creating the record batches so
        // when the querier gets them they have no duplicates
        false
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
        let batches = match self.schema.df_projection(selection)? {
            None => self.batches.clone(),
            Some(projection) => self
                .batches
                .iter()
                .map(|batch| batch.project(&projection))
                .collect::<std::result::Result<Vec<_>, ArrowError>>()?,
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
        ChunkOrder::new(i64::MAX)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Ensure that the record batch has the given schema.
///
/// # Dictionary Type Recovery
/// Cast arrays in record batch to be the type of schema. This is a workaround for
/// <https://github.com/influxdata/influxdb_iox/pull/4273> where the Flight API doesn't necessarily
/// return the same schema as was provided by the ingester.
///
/// Namely, dictionary encoded columns (e.g. tags) are returned as `DataType::Utf8` even when they
/// were sent as `DataType::Dictionary(Int32, Utf8)`.
///
/// # Panic
/// Panics when a column was not found in the given batch.
fn ensure_schema(batch: RecordBatch, expected_schema: &Schema) -> Result<RecordBatch> {
    let actual_schema = batch.schema();
    let desired_fields = expected_schema.iter().map(|(_, f)| f);

    let new_columns = desired_fields
        .map(|desired_field| {
            let desired_type = desired_field.data_type();

            // find column by name
            match actual_schema.column_with_name(desired_field.name()) {
                Some((idx, _field)) => {
                    let col = batch.column(idx);
                    let actual_type = col.data_type();

                    // check type
                    if desired_type != actual_type {
                        if let DataType::Dictionary(_key_type, value_type) = desired_type.clone() {
                            if value_type.as_ref() == actual_type {
                                // convert
                                return arrow::compute::cast(col, desired_type).context(
                                    ConvertingRecordBatchSnafu {
                                        column_name: desired_field.name(),
                                        data_type: desired_type.clone(),
                                    },
                                );
                            }
                        }

                        RecordBatchTypeSnafu {
                            column_name: desired_field.name(),
                            actual_data_type: actual_type.clone(),
                            desired_data_type: desired_type.clone(),
                        }
                        .fail()
                    } else {
                        Ok(Arc::clone(col))
                    }
                }
                None => panic!("Column not found: {}", desired_field.name()),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(expected_schema.as_arrow(), new_columns).context(CreatingRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use super::{flight_client::QueryData, *};
    use arrow::{
        array::{ArrayRef, DictionaryArray, Int64Array, StringArray, TimestampNanosecondArray},
        datatypes::Int32Type,
    };
    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::ingester::v1::PartitionStatus;
    use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
    use iox_tests::util::TestCatalog;
    use metric::Attributes;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use std::collections::{BTreeSet, HashMap};
    use test_helpers::assert_error;
    use tokio::sync::Mutex;
    use trace::{span::SpanStatus, RingBufferTraceCollector};

    #[tokio::test]
    async fn test_flight_handshake_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Handshake {
                    ingester_address: String::from("addr1"),
                    source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_internal_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Flight {
                    source: FlightError::GrpcError(tonic::Status::internal("cow exploded")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_not_found() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Flight {
                    source: FlightError::GrpcError(tonic::Status::not_found("something")),
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_flight_stream_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Err(FlightError::GrpcError(tonic::Status::internal(
                        "don't know",
                    )))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::RemoteQuery { .. });
    }

    #[tokio::test]
    async fn test_flight_no_partitions() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([("addr1", Ok(MockQueryData { results: vec![] }))]).await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn no_ingester_addresses_found_is_a_configuration_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([("addr1", Ok(MockQueryData { results: vec![] }))]).await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        // Sequencer ID 0 doesn't have an associated ingester address in the test setup
        assert_error!(
            get_partitions(&ingester_conn, &[0]).await,
            Error::NoIngesterFoundForSequencer { .. },
        );
    }

    #[tokio::test]
    async fn test_flight_no_batches() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::None,
                        IngesterQueryResponseMetadata {
                            partition_id: 1,
                            status: Some(PartitionStatus {
                                parquet_max_sequence_number: None,
                                tombstone_max_sequence_number: None,
                            }),
                        },
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let p = &partitions[0];
        assert_eq!(p.partition_id.get(), 1);
        assert_eq!(p.sequencer_id.get(), 1);
        assert_eq!(p.parquet_max_sequence_number, None);
        assert_eq!(p.tombstone_max_sequence_number, None);
        assert_eq!(p.chunks.len(), 0);
    }

    #[tokio::test]
    async fn test_flight_err_partition_status_missing() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::None,
                        IngesterQueryResponseMetadata {
                            partition_id: 1,
                            status: None,
                        },
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::PartitionStatusMissing { .. });
    }

    #[tokio::test]
    async fn test_flight_err_duplicate_partition_info() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 1,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 2,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                        Ok((
                            LowLevelMessage::None,
                            IngesterQueryResponseMetadata {
                                partition_id: 1,
                                status: Some(PartitionStatus {
                                    parquet_max_sequence_number: None,
                                    tombstone_max_sequence_number: None,
                                }),
                            },
                        )),
                    ],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::DuplicatePartitionInfo { .. });
    }

    #[tokio::test]
    async fn test_flight_err_chunk_without_partition() {
        let record_batch = lp_to_record_batch("table foo=1 1");
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::Schema(record_batch.schema()),
                        IngesterQueryResponseMetadata::default(),
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::ChunkWithoutPartition { .. });
    }

    #[tokio::test]
    async fn test_flight_err_batch_without_chunk() {
        let record_batch = lp_to_record_batch("table foo=1 1");
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Ok((
                        LowLevelMessage::RecordBatch(record_batch),
                        IngesterQueryResponseMetadata::default(),
                    ))],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let err = get_partitions(&ingester_conn, &[1]).await.unwrap_err();
        assert_matches!(err, Error::BatchWithoutChunk { .. });
    }

    #[tokio::test]
    async fn test_flight_many_batches_no_sequencer() {
        let record_batch_1_1_1 = lp_to_record_batch("table foo=1 1");
        let record_batch_1_1_2 = lp_to_record_batch("table foo=2 2");
        let record_batch_1_2 = lp_to_record_batch("table bar=20,foo=2 2");
        let record_batch_2_1 = lp_to_record_batch("table foo=3 3");
        let record_batch_3_1 = lp_to_record_batch("table baz=40,foo=4 4");

        let schema_1_1 = record_batch_1_1_1.schema();
        assert_eq!(schema_1_1, record_batch_1_1_2.schema());
        let schema_1_2 = record_batch_1_2.schema();
        let schema_2_1 = record_batch_2_1.schema();
        let schema_3_1 = record_batch_3_1.schema();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                (
                    "addr1",
                    Ok(MockQueryData {
                        results: vec![
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 1,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(11),
                                        tombstone_max_sequence_number: Some(12),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_1_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_1_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_1_2),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_1_2)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_2),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 2,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(21),
                                        tombstone_max_sequence_number: Some(22),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_2_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_2_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Ok(MockQueryData {
                        results: vec![
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 3,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(31),
                                        tombstone_max_sequence_number: Some(32),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_3_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_3_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        let partitions = get_partitions(&ingester_conn, &[1, 2]).await.unwrap();
        assert_eq!(partitions.len(), 3);

        let p1 = &partitions[0];
        assert_eq!(p1.partition_id.get(), 1);
        assert_eq!(p1.sequencer_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(
            p1.tombstone_max_sequence_number,
            Some(SequenceNumber::new(12))
        );
        assert_eq!(p1.chunks.len(), 2);
        assert_eq!(p1.chunks[0].schema().as_arrow(), schema_1_1);
        assert_eq!(p1.chunks[0].batches.len(), 2);
        assert_eq!(p1.chunks[0].batches[0].schema(), schema_1_1);
        assert_eq!(p1.chunks[0].batches[1].schema(), schema_1_1);
        assert_eq!(p1.chunks[1].schema().as_arrow(), schema_1_2);
        assert_eq!(p1.chunks[1].batches.len(), 1);
        assert_eq!(p1.chunks[1].batches[0].schema(), schema_1_2);

        let p2 = &partitions[1];
        assert_eq!(p2.partition_id.get(), 2);
        assert_eq!(p2.sequencer_id.get(), 1);
        assert_eq!(
            p2.parquet_max_sequence_number,
            Some(SequenceNumber::new(21))
        );
        assert_eq!(
            p2.tombstone_max_sequence_number,
            Some(SequenceNumber::new(22))
        );
        assert_eq!(p2.chunks.len(), 1);
        assert_eq!(p2.chunks[0].schema().as_arrow(), schema_2_1);
        assert_eq!(p2.chunks[0].batches.len(), 1);
        assert_eq!(p2.chunks[0].batches[0].schema(), schema_2_1);

        let p3 = &partitions[2];
        assert_eq!(p3.partition_id.get(), 3);
        assert_eq!(p3.sequencer_id.get(), 2);
        assert_eq!(
            p3.parquet_max_sequence_number,
            Some(SequenceNumber::new(31))
        );
        assert_eq!(
            p3.tombstone_max_sequence_number,
            Some(SequenceNumber::new(32))
        );
        assert_eq!(p3.chunks.len(), 1);
        assert_eq!(p3.chunks[0].schema().as_arrow(), schema_3_1);
        assert_eq!(p3.chunks[0].batches.len(), 1);
        assert_eq!(p3.chunks[0].batches[0].schema(), schema_3_1);
    }

    #[tokio::test]
    async fn test_ingester_metrics_and_tracing() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                ("addr1", Ok(MockQueryData { results: vec![] })),
                ("addr2", Ok(MockQueryData { results: vec![] })),
                (
                    "addr3",
                    Err(FlightClientError::Handshake {
                        ingester_address: String::from("addr3"),
                        source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                    }),
                ),
                (
                    "addr4",
                    Err(FlightClientError::Handshake {
                        ingester_address: String::from("addr4"),
                        source: FlightError::GrpcError(tonic::Status::internal("don't know")),
                    }),
                ),
                ("addr5", Ok(MockQueryData { results: vec![] })),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let traces = Arc::new(RingBufferTraceCollector::new(100));
        get_partitions_with_span(
            &ingester_conn,
            &[1, 2, 3, 4, 5],
            Some(Span::root("root", Arc::clone(&traces) as _)),
        )
        .await
        .unwrap_err();

        let histogram_error = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "error")]))
            .expect("failed to get observer")
            .fetch();

        // only one error got counted because the other got cancelled
        let hit_count_error = histogram_error.sample_count();
        assert_eq!(hit_count_error, 1);

        let histogram_success = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "success")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count_success = histogram_success.sample_count();

        let histogram_cancelled = mock_flight_client
            .catalog
            .metric_registry()
            .get_instrument::<Metric<DurationHistogram>>("ingester_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[("result", "cancelled")]))
            .expect("failed to get observer")
            .fetch();

        let hit_count_cancelled = histogram_cancelled.sample_count();

        // we don't know how errors are propagated because the futures are polled unordered
        assert_eq!(hit_count_success + hit_count_cancelled, 4);

        // check spans
        let root_span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "root")
            .expect("root span not found");
        assert_eq!(root_span.status, SpanStatus::Err);
        let n_spans_err = traces
            .spans()
            .into_iter()
            .filter(|s| (s.name == "flight request") && (s.status == SpanStatus::Err))
            .count();
        assert_eq!(n_spans_err, 1);
        let n_spans_ok_or_cancelled = traces
            .spans()
            .into_iter()
            .filter(|s| {
                (s.name == "flight request")
                    && ((s.status == SpanStatus::Ok) || (s.status == SpanStatus::Unknown))
            })
            .count();
        assert_eq!(n_spans_ok_or_cancelled, 4);
    }

    #[tokio::test]
    async fn test_flight_per_sequencer_querying() {
        let record_batch_1_1 = lp_to_record_batch("table foo=1 1");
        let schema_1_1 = record_batch_1_1.schema();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                (
                    "addr1",
                    Ok(MockQueryData {
                        results: vec![
                            Ok((
                                LowLevelMessage::None,
                                IngesterQueryResponseMetadata {
                                    partition_id: 1,
                                    status: Some(PartitionStatus {
                                        parquet_max_sequence_number: Some(11),
                                        tombstone_max_sequence_number: Some(12),
                                    }),
                                },
                            )),
                            Ok((
                                LowLevelMessage::Schema(Arc::clone(&schema_1_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                LowLevelMessage::RecordBatch(record_batch_1_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Err(FlightClientError::Flight {
                        source: FlightError::GrpcError(tonic::Status::internal(
                            "if this is queried, the test should fail",
                        )),
                    }),
                ),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        // Only use sequencer ID 1, which will correspond to only querying the ingester at
        // "addr1"
        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let p1 = &partitions[0];
        assert_eq!(p1.partition_id.get(), 1);
        assert_eq!(p1.sequencer_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(
            p1.tombstone_max_sequence_number,
            Some(SequenceNumber::new(12))
        );
        assert_eq!(p1.chunks.len(), 1);
    }

    async fn get_partitions(
        ingester_conn: &IngesterConnectionImpl,
        sequencer_ids: &[i32],
    ) -> Result<Vec<IngesterPartition>, Error> {
        get_partitions_with_span(ingester_conn, sequencer_ids, None).await
    }

    async fn get_partitions_with_span(
        ingester_conn: &IngesterConnectionImpl,
        sequencer_ids: &[i32],
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>, Error> {
        let sequencer_ids: Vec<_> = sequencer_ids
            .iter()
            .copied()
            .map(KafkaPartition::new)
            .collect();
        let namespace = Arc::from("namespace");
        let table = Arc::from("table");
        let columns = vec![String::from("col")];
        let schema = schema();
        ingester_conn
            .partitions(
                &sequencer_ids,
                namespace,
                table,
                columns,
                &Predicate::default(),
                schema,
                span,
            )
            .await
    }

    fn schema() -> Arc<Schema> {
        Arc::new(
            SchemaBuilder::new()
                .influx_field("bar", InfluxFieldType::Float)
                .influx_field("baz", InfluxFieldType::Float)
                .influx_field("foo", InfluxFieldType::Float)
                .timestamp()
                .build()
                .unwrap(),
        )
    }

    fn lp_to_record_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Selection::All).unwrap()
    }

    #[derive(Debug)]
    struct MockQueryData {
        results: Vec<Result<(LowLevelMessage, IngesterQueryResponseMetadata), FlightError>>,
    }

    #[async_trait]
    impl QueryData for MockQueryData {
        async fn next(
            &mut self,
        ) -> Result<Option<(LowLevelMessage, IngesterQueryResponseMetadata)>, FlightError> {
            if self.results.is_empty() {
                Ok(None)
            } else {
                self.results.remove(0).map(Some)
            }
        }
    }

    #[derive(Debug)]
    struct MockFlightClient {
        catalog: Arc<TestCatalog>,
        responses: Mutex<HashMap<String, Result<MockQueryData, FlightClientError>>>,
    }

    impl MockFlightClient {
        async fn new<const N: usize>(
            responses: [(&'static str, Result<MockQueryData, FlightClientError>); N],
        ) -> Self {
            let catalog = TestCatalog::new();
            let ns = catalog.create_namespace("namespace").await;
            let table = ns.create_table("table").await;

            let s0 = ns.create_sequencer(0).await;
            let s1 = ns.create_sequencer(1).await;

            table.with_sequencer(&s0).create_partition("k1").await;
            table.with_sequencer(&s0).create_partition("k2").await;
            table.with_sequencer(&s1).create_partition("k3").await;

            Self {
                catalog,
                responses: Mutex::new(
                    responses
                        .into_iter()
                        .map(|(k, v)| (String::from(k), v))
                        .collect(),
                ),
            }
        }

        // Assign one sequencer per address, sorted consistently.
        // Don't assign any addresses to sequencer ID 0 to test error case
        async fn ingester_conn(self: &Arc<Self>) -> IngesterConnectionImpl {
            let ingester_addresses: BTreeSet<_> =
                self.responses.lock().await.keys().cloned().collect();

            let sequencer_to_ingesters = ingester_addresses
                .into_iter()
                .enumerate()
                .map(|(sequencer_id, ingester_address)| {
                    (
                        sequencer_id as i32 + 1,
                        IngesterMapping::Addr(Arc::from(ingester_address.as_str())),
                    )
                })
                .collect();

            IngesterConnectionImpl::by_sequencer_with_flight_client(
                sequencer_to_ingesters,
                Arc::clone(self) as _,
                Arc::new(CatalogCache::new_testing(
                    self.catalog.catalog(),
                    self.catalog.time_provider(),
                    self.catalog.metric_registry(),
                )),
            )
        }
    }

    #[async_trait]
    impl FlightClient for MockFlightClient {
        async fn query(
            &self,
            ingester_address: Arc<str>,
            _request: IngesterQueryRequest,
        ) -> Result<Box<dyn QueryData>, FlightClientError> {
            self.responses
                .lock()
                .await
                .remove(ingester_address.as_ref())
                .expect("Response not mocked")
                .map(|query_data| Box::new(query_data) as _)
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
            let tombstone_max_sequence_number = None;
            // Construct a partition and ensure it doesn't error
            let ingester_partition = IngesterPartition::new(
                "ingester".into(),
                "table".into(),
                PartitionId::new(1),
                SequencerId::new(1),
                parquet_max_sequence_number,
                tombstone_max_sequence_number,
                Arc::new(None),
            )
            .try_add_chunk(ChunkId::new(), Arc::clone(&expected_schema), vec![case])
            .unwrap();

            for batch in &ingester_partition.chunks[0].batches {
                assert_eq!(batch.schema(), expected_schema.as_arrow());
            }
        }
    }

    #[test]
    fn test_ingester_partition_fail_type_cast() {
        let expected_schema = Arc::new(
            SchemaBuilder::new()
                .field("b", DataType::Boolean)
                .timestamp()
                .build()
                .unwrap(),
        );

        let batch =
            RecordBatch::try_from_iter(vec![("b", int64_array()), ("time", ts_array())]).unwrap();

        let parquet_max_sequence_number = None;
        let tombstone_max_sequence_number = None;
        let err = IngesterPartition::new(
            "ingester".into(),
            "table".into(),
            PartitionId::new(1),
            SequencerId::new(1),
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            Arc::new(None),
        )
        .try_add_chunk(ChunkId::new(), Arc::clone(&expected_schema), vec![batch])
        .unwrap_err();

        assert_matches!(err, Error::RecordBatchType { .. });
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

    fn int64_array() -> ArrayRef {
        Arc::new(i64_vec().iter().collect::<Int64Array>())
    }

    fn str_vec() -> &'static [Option<&'static str>] {
        &[Some("foo"), Some("bar"), Some("baz")]
    }

    fn i64_vec() -> &'static [Option<i64>] {
        &[Some(1), Some(2), Some(3)]
    }
}
