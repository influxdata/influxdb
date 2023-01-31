use self::{
    circuit_breaker::CircuitBreakerFlightClient,
    flight_client::{
        Error as FlightClientError, FlightClientImpl, FlightError, IngesterFlightClient,
    },
    invalidate_on_error::InvalidateOnErrorFlightClient,
    test_util::MockIngesterConnection,
};
use crate::cache::{namespace::CachedTable, CatalogCache};
use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};
use arrow_flight::decode::DecodedPayload;
use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig, BackoffError};
use client_util::connection;
use data_types::{
    ChunkId, ChunkOrder, IngesterMapping, NamespaceId, PartitionId, SequenceNumber, ShardId,
    ShardIndex, TableSummary, TimestampMinMax,
};
use datafusion::error::DataFusionError;
use futures::{stream::FuturesUnordered, TryStreamExt};
use generated_types::{
    influxdata::iox::ingester::v1::GetWriteInfoResponse,
    ingester::{encode_proto_predicate_as_base64, IngesterQueryRequest},
    write_info::merge_responses,
};
use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
use iox_query::{
    exec::{stringset::StringSet, IOxSessionContext},
    util::{compute_timenanosecond_min_max, create_basic_summary},
    QueryChunk, QueryChunkData, QueryChunkMeta,
};
use iox_time::{Time, TimeProvider};
use metric::{DurationHistogram, Metric};
use observability_deps::tracing::{debug, trace, warn};
use predicate::Predicate;
use schema::{sort::SortKey, Projection, Schema};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use trace::span::{Span, SpanRecorder};
use uuid::Uuid;

mod circuit_breaker;
pub(crate) mod flight_client;
mod invalidate_on_error;
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
        "No ingester found in shard to ingester mapping for shard index {shard_index}"
    ))]
    NoIngesterFoundForShard { shard_index: ShardIndex },

    #[snafu(display(
        "Shard index {shard_index} was neither mapped to an ingester nor marked ignore"
    ))]
    ShardNotMapped { shard_index: ShardIndex },

    #[snafu(display("Could not parse `{ingester_uuid}` as a UUID: {source}"))]
    IngesterUuid {
        ingester_uuid: String,
        source: uuid::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a new set of connections given ingester configurations
pub fn create_ingester_connections(
    shard_to_ingesters: Option<HashMap<ShardIndex, IngesterMapping>>,
    ingester_addresses: Option<Vec<Arc<str>>>,
    catalog_cache: Arc<CatalogCache>,
    open_circuit_after_n_errors: u64,
) -> Arc<dyn IngesterConnection> {
    // This backoff config is used to retry requests for a specific table-scoped query.
    let retry_backoff_config = BackoffConfig {
        init_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(1),
        base: 3.0,
        deadline: Some(Duration::from_secs(10)),
    };

    // This backoff config is used to half-open the circuit after it was opened. Circuits are
    // ingester-scoped.
    let circuit_breaker_backoff_config = BackoffConfig {
        init_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(60),
        base: 3.0,
        deadline: None,
    };

    // Exactly one of `shard_to_ingesters` or `ingester_addreses` must be specified.
    // `shard_to_ingesters` uses the Kafka write buffer path.
    // `ingester_addresses` uses the RPC write path.
    match (shard_to_ingesters, ingester_addresses) {
        (None, None) => panic!("Neither shard_to_ingesters nor ingester_addresses was specified!"),
        (Some(_), Some(_)) => {
            panic!("Both shard_to_ingesters and ingester_addresses were specified!")
        }
        (Some(shard_to_ingesters), None) => Arc::new(IngesterConnectionImpl::by_shard(
            shard_to_ingesters,
            catalog_cache,
            retry_backoff_config,
            circuit_breaker_backoff_config,
            open_circuit_after_n_errors,
        )),
        (None, Some(ingester_addresses)) => Arc::new(IngesterConnectionImpl::by_addrs(
            ingester_addresses,
            catalog_cache,
            retry_backoff_config,
            circuit_breaker_backoff_config,
            open_circuit_after_n_errors,
        )),
    }
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Handles communicating with the ingester(s) to retrieve data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    ///
    /// # Panics
    ///
    /// Panics if the list of shard_indexes is empty.
    #[allow(clippy::too_many_arguments)]
    async fn partitions(
        &self,
        shard_indexes: Option<Vec<ShardIndex>>,
        namespace_id: NamespaceId,
        cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        predicate: &Predicate,
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>>;

    /// Returns the most recent partition status info across all ingester(s) for the specified
    /// write token.
    async fn get_write_info(&self, write_token: &str) -> Result<GetWriteInfoResponse>;

    /// Return backend as [`Any`] which can be used to downcast to a specific implementation.
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

/// Information about an OK/success ingester request.
#[derive(Debug, Default)]
struct IngesterResponseOk {
    n_partitions: usize,
    n_chunks: usize,
    n_rows: usize,
}

/// Helper to observe a single ingester request.
///
/// Use [`set_ok`](Self::set_ok) or [`set_err`](Self::set_err) if an ingester result was
/// observered. Otherwise the request will count as "cancelled".
struct ObserveIngesterRequest<'a> {
    res: Option<Result<IngesterResponseOk, ()>>,
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

    fn set_ok(mut self, ok_status: IngesterResponseOk) {
        self.res = Some(Ok(ok_status));
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
            let (metric, status, ok_status) = match self.res {
                None => (&self.metrics.ingester_duration_cancelled, "cancelled", None),
                Some(Ok(ref ok_status)) => (
                    &self.metrics.ingester_duration_success,
                    "success",
                    Some(ok_status),
                ),
                Some(Err(())) => (&self.metrics.ingester_duration_error, "error", None),
            };

            metric.record(ingester_duration);

            debug!(
                predicate=?self.request.predicate,
                namespace_id=self.request.namespace_id.get(),
                table_id=self.request.cached_table.id.get(),
                n_partitions=?ok_status.map(|s| s.n_partitions),
                n_chunks=?ok_status.map(|s| s.n_chunks),
                n_rows=?ok_status.map(|s| s.n_rows),
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
    shard_to_ingesters: HashMap<ShardIndex, IngesterMapping>,
    unique_ingester_addresses: HashSet<Arc<str>>,
    flight_client: Arc<dyn IngesterFlightClient>,
    catalog_cache: Arc<CatalogCache>,
    metrics: Arc<IngesterConnectionMetrics>,
    backoff_config: BackoffConfig,
}

impl IngesterConnectionImpl {
    /// Create a new set of connections given a map of shard indexes to Ingester addresses, such as:
    ///
    /// ```json
    /// {
    ///   "shards": {
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
    pub fn by_shard(
        shard_to_ingesters: HashMap<ShardIndex, IngesterMapping>,
        catalog_cache: Arc<CatalogCache>,
        retry_backoff_config: BackoffConfig,
        circuit_breaker_backoff_config: BackoffConfig,
        open_circuit_after_n_errors: u64,
    ) -> Self {
        let flight_client = Arc::new(FlightClientImpl::new());
        let flight_client = Arc::new(InvalidateOnErrorFlightClient::new(flight_client));
        let flight_client = Arc::new(CircuitBreakerFlightClient::new(
            flight_client,
            catalog_cache.time_provider(),
            catalog_cache.metric_registry(),
            open_circuit_after_n_errors,
            circuit_breaker_backoff_config,
        ));
        Self::by_shard_with_flight_client(
            shard_to_ingesters,
            flight_client,
            catalog_cache,
            retry_backoff_config,
        )
    }

    /// Create new set of connections with specific flight client implementation.
    ///
    /// This is helpful for testing, i.e. when the flight client should not be backed by normal
    /// network communication.
    pub fn by_shard_with_flight_client(
        shard_to_ingesters: HashMap<ShardIndex, IngesterMapping>,
        flight_client: Arc<dyn IngesterFlightClient>,
        catalog_cache: Arc<CatalogCache>,
        backoff_config: BackoffConfig,
    ) -> Self {
        let unique_ingester_addresses: HashSet<_> = shard_to_ingesters
            .values()
            .flat_map(|v| match v {
                IngesterMapping::Addr(addr) => Some(addr),
                _ => None,
            })
            .cloned()
            .collect();

        let metric_registry = catalog_cache.metric_registry();
        let metrics = Arc::new(IngesterConnectionMetrics::new(&metric_registry));

        Self {
            shard_to_ingesters,
            unique_ingester_addresses,
            flight_client,
            catalog_cache,
            metrics,
            backoff_config,
        }
    }

    /// Create a new set of connections given a list of ingester2 addresses.
    pub fn by_addrs(
        ingester_addresses: Vec<Arc<str>>,
        catalog_cache: Arc<CatalogCache>,
        backoff_config: BackoffConfig,
        circuit_breaker_backoff_config: BackoffConfig,
        open_circuit_after_n_errors: u64,
    ) -> Self {
        let flight_client = Arc::new(FlightClientImpl::new());
        let flight_client = Arc::new(InvalidateOnErrorFlightClient::new(flight_client));
        let flight_client = Arc::new(CircuitBreakerFlightClient::new(
            flight_client,
            catalog_cache.time_provider(),
            catalog_cache.metric_registry(),
            open_circuit_after_n_errors,
            circuit_breaker_backoff_config,
        ));

        let metric_registry = catalog_cache.metric_registry();
        let metrics = Arc::new(IngesterConnectionMetrics::new(&metric_registry));

        Self {
            shard_to_ingesters: HashMap::new(),
            unique_ingester_addresses: ingester_addresses.into_iter().collect(),
            flight_client,
            catalog_cache,
            metrics,
            backoff_config,
        }
    }
}

/// Struct that names all parameters to `execute`
#[derive(Debug, Clone)]
struct GetPartitionForIngester<'a> {
    flight_client: Arc<dyn IngesterFlightClient>,
    catalog_cache: Arc<CatalogCache>,
    ingester_address: Arc<str>,
    namespace_id: NamespaceId,
    columns: Vec<String>,
    predicate: &'a Predicate,
    cached_table: Arc<CachedTable>,
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
        namespace_id,
        columns,
        predicate,
        cached_table,
    } = request;

    let ingester_query_request = IngesterQueryRequest {
        namespace_id,
        table_id: cached_table.id,
        columns: columns.clone(),
        predicate: Some(predicate.clone()),
    };

    let query_res = flight_client
        .query(
            Arc::clone(&ingester_address),
            ingester_query_request,
            span_recorder.span().map(|span| span.ctx.clone()),
        )
        .await;

    match &query_res {
        Err(FlightClientError::CircuitBroken { .. }) => {
            warn!(
                ingester_address = ingester_address.as_ref(),
                namespace_id = namespace_id.get(),
                table_id = cached_table.id.get(),
                "Could not connect to ingester, circuit broken",
            );
            return Ok(vec![]);
        }
        Err(FlightClientError::Flight {
            source: FlightError::ArrowFlightError(arrow_flight::error::FlightError::Tonic(status)),
        }) if status.code() == tonic::Code::NotFound => {
            debug!(
                ingester_address = ingester_address.as_ref(),
                namespace_id = namespace_id.get(),
                table_id = cached_table.id.get(),
                "Ingester does not know namespace or table, skipping",
            );
            return Ok(vec![]);
        }
        _ => {}
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
                namespace_id=namespace_id.get(),
                table_id=cached_table.id.get(),
                columns=columns.join(",").as_str(),
                predicate_str=%predicate,
                predicate_binary=encode_predicate_as_base64(predicate).as_str(),
                "Failed to perform ingester query",
            );

            //  need to return error until https://github.com/rust-lang/rust/issues/91345 is stable
            e
        })?;

    // collect data from IO stream
    // Drain data from ingester so we don't block the ingester while performing catalog IO or CPU
    // computations
    let mut messages = vec![];
    while let Some(data) = perform_query
        .next_message()
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
        catalog_cache,
        cached_table,
        span_recorder.child_span("IngesterStreamDecoder"),
    );
    for (msg, md) in messages {
        decoder.register(msg, md).await?;
    }

    decoder.finalize().await
}

/// Current partition used while decoding the ingester response stream.
#[derive(Debug)]
enum CurrentPartition {
    /// There exists a partition.
    Some(IngesterPartition),

    /// There is no existing partition.
    None,

    /// Skip the current partition (e.g. because it is gone from the catalog).
    Skip,
}

impl CurrentPartition {
    fn take(&mut self) -> Option<IngesterPartition> {
        let mut tmp = Self::None;
        std::mem::swap(&mut tmp, self);

        match tmp {
            Self::None | Self::Skip => None,
            Self::Some(p) => Some(p),
        }
    }

    fn is_skip(&self) -> bool {
        matches!(self, Self::Skip)
    }

    fn is_some(&self) -> bool {
        matches!(self, Self::Some(_))
    }
}

/// Helper to disassemble the data from the ingester Apache Flight arrow stream.
///
/// This should be used AFTER the stream was drained because we will perform some catalog IO and
/// this should likely not block the ingester.
struct IngesterStreamDecoder {
    finished_partitions: HashMap<PartitionId, IngesterPartition>,
    current_partition: CurrentPartition,
    current_chunk: Option<(Schema, Vec<RecordBatch>)>,
    ingester_address: Arc<str>,
    catalog_cache: Arc<CatalogCache>,
    cached_table: Arc<CachedTable>,
    span_recorder: SpanRecorder,
}

impl IngesterStreamDecoder {
    /// Create empty decoder.
    fn new(
        ingester_address: Arc<str>,
        catalog_cache: Arc<CatalogCache>,
        cached_table: Arc<CachedTable>,
        span: Option<Span>,
    ) -> Self {
        Self {
            finished_partitions: HashMap::new(),
            current_partition: CurrentPartition::None,
            current_chunk: None,
            ingester_address,
            catalog_cache,
            cached_table,
            span_recorder: SpanRecorder::new(span),
        }
    }

    /// Flush current chunk, if any.
    fn flush_chunk(&mut self) -> Result<()> {
        if let Some((schema, batches)) = self.current_chunk.take() {
            let current_partition = self
                .current_partition
                .take()
                .expect("Partition should have been checked before chunk creation");
            self.current_partition = CurrentPartition::Some(current_partition.try_add_chunk(
                ChunkId::new(),
                schema,
                batches,
            )?);
        }

        Ok(())
    }

    /// Flush current partition, if any.
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
            let primary_key: Vec<_> = primary_keys
                .iter()
                .flat_map(|pk| pk.iter())
                // cache may be older then the ingester response status, so some entries might be missing
                .filter_map(|name| {
                    self.cached_table
                        .column_id_map_rev
                        .get(&Arc::from(name.to_owned()))
                })
                .copied()
                .collect();
            let partition_sort_key = self
                .catalog_cache
                .partition()
                .sort_key(
                    Arc::clone(&self.cached_table),
                    current_partition.partition_id(),
                    &primary_key,
                    self.span_recorder
                        .child_span("cache GET partition sort key"),
                )
                .await
                .map(|sort_key| Arc::clone(&sort_key.sort_key));
            let current_partition = current_partition.with_partition_sort_key(partition_sort_key);
            self.finished_partitions
                .insert(current_partition.partition_id, current_partition);
        }

        Ok(())
    }

    /// Register a new message and its metadata from the Flight stream.
    async fn register(
        &mut self,
        msg: DecodedPayload,
        md: IngesterQueryResponseMetadata,
    ) -> Result<(), Error> {
        match msg {
            DecodedPayload::None => {
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
                let shard_id = self
                    .catalog_cache
                    .partition()
                    .shard_id(
                        Arc::clone(&self.cached_table),
                        partition_id,
                        self.span_recorder
                            .child_span("cache GET partition shard ID"),
                    )
                    .await;

                let Some(shard_id) = shard_id else {
                    self.current_partition = CurrentPartition::Skip;
                    return Ok(())
                };

                // Use a temporary empty partition sort key. We are going to fetch this AFTER we
                // know all chunks because then we are able to detect all relevant primary key
                // columns that the sort key must cover.
                let partition_sort_key = None;

                let ingester_uuid = if md.ingester_uuid.is_empty() {
                    // Using the write buffer path, no UUID specified
                    None
                } else {
                    Some(
                        Uuid::parse_str(&md.ingester_uuid).context(IngesterUuidSnafu {
                            ingester_uuid: md.ingester_uuid,
                        })?,
                    )
                };

                let partition = IngesterPartition::new(
                    Arc::clone(&self.ingester_address),
                    ingester_uuid,
                    partition_id,
                    shard_id,
                    md.completed_persistence_count,
                    status.parquet_max_sequence_number.map(SequenceNumber::new),
                    None,
                    partition_sort_key,
                );
                self.current_partition = CurrentPartition::Some(partition);
            }
            DecodedPayload::Schema(schema) => {
                if self.current_partition.is_skip() {
                    return Ok(());
                }

                self.flush_chunk()?;
                ensure!(
                    self.current_partition.is_some(),
                    ChunkWithoutPartitionSnafu {
                        ingester_address: self.ingester_address.as_ref()
                    }
                );

                // don't use the transmitted arrow schema to construct the IOx schema because some
                // metadata might be missing. Instead select the right columns from the expected
                // schema.
                let column_names: Vec<_> =
                    schema.fields().iter().map(|f| f.name().as_str()).collect();
                let schema = self
                    .cached_table
                    .schema
                    .select_by_names(&column_names)
                    .context(ConvertingSchemaSnafu)?;
                self.current_chunk = Some((schema, vec![]));
            }
            DecodedPayload::RecordBatch(batch) => {
                if self.current_partition.is_skip() {
                    return Ok(());
                }

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
    /// Retrieve chunks from the ingester for the particular table, shard, and predicate
    async fn partitions(
        &self,
        shard_indexes: Option<Vec<ShardIndex>>,
        namespace_id: NamespaceId,
        cached_table: Arc<CachedTable>,
        columns: Vec<String>,
        predicate: &Predicate,
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>> {
        let relevant_ingester_addresses = match shard_indexes {
            // If shard indexes is None, we're using the RPC write path, and all ingesters should
            // be queried.
            None => self.unique_ingester_addresses.clone(),
            // If shard indexes is Some([]), no ingester addresses can be found. This is a
            // configuration problem somewhwere.
            Some(shard_indexes) if shard_indexes.is_empty() => {
                panic!("Called `IngesterConnection.partitions` with an empty `shard_indexes` list");
            }
            // Otherwise, we're using the write buffer and need to look up the ingesters to contact
            // by their shard index.
            Some(shard_indexes) => {
                // Look up the ingesters needed for the shard. Collect into a HashSet to avoid
                // making multiple requests to the same ingester if that ingester is responsible
                // for multiple shard_indexes relevant to this query.
                let mut relevant_ingester_addresses = HashSet::new();

                for shard_index in &shard_indexes {
                    match self.shard_to_ingesters.get(shard_index) {
                        None => {
                            return NoIngesterFoundForShardSnafu {
                                shard_index: *shard_index,
                            }
                            .fail()
                        }
                        Some(mapping) => match mapping {
                            IngesterMapping::Addr(addr) => {
                                relevant_ingester_addresses.insert(Arc::clone(addr));
                            }
                            IngesterMapping::Ignore => (),
                            IngesterMapping::NotMapped => {
                                return ShardNotMappedSnafu {
                                    shard_index: *shard_index,
                                }
                                .fail()
                            }
                        },
                    }
                }
                relevant_ingester_addresses
            }
        };

        let mut span_recorder = SpanRecorder::new(span);

        let metrics = Arc::clone(&self.metrics);

        let measured_ingester_request = |ingester_address: Arc<str>| {
            let metrics = Arc::clone(&metrics);
            let request = GetPartitionForIngester {
                flight_client: Arc::clone(&self.flight_client),
                catalog_cache: Arc::clone(&self.catalog_cache),
                ingester_address: Arc::clone(&ingester_address),
                namespace_id,
                cached_table: Arc::clone(&cached_table),
                columns: columns.clone(),
                predicate,
            };

            let backoff_config = self.backoff_config.clone();

            // wrap `execute` into an additional future so that we can measure the request time
            // INFO: create the measurement structure outside of the async block so cancellation is
            // always measured
            let measure_me = ObserveIngesterRequest::new(request.clone(), metrics, &span_recorder);
            async move {
                let span_recorder = measure_me
                    .span_recorder()
                    .child("ingester request (retry block)");

                let res = Backoff::new(&backoff_config)
                    .retry_all_errors("ingester request", move || {
                        let request = request.clone();
                        let span_recorder = span_recorder.child("ingester request (single try)");

                        async move { execute(request, &span_recorder).await }
                    })
                    .await;

                match &res {
                    Ok(partitions) => {
                        let mut status = IngesterResponseOk::default();
                        for p in partitions {
                            status.n_partitions += 1;
                            for c in p.chunks() {
                                status.n_chunks += 1;
                                status.n_rows += c.rows();
                            }
                        }

                        measure_me.set_ok(status);
                    }
                    Err(_) => measure_me.set_err(),
                }

                res
            }
        };

        let mut ingester_partitions: Vec<IngesterPartition> = relevant_ingester_addresses
            .into_iter()
            .map(move |ingester_address| measured_ingester_request(ingester_address))
            .collect::<FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| {
                span_recorder.error("failed");
                match e {
                    BackoffError::DeadlineExceeded { source, .. } => source,
                }
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
/// (Catalog) Shard -> (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog
/// partition from a shard. Thus, there can be more than one
/// IngesterPartition for each table the ingester knows about.
#[derive(Debug, Clone)]
pub struct IngesterPartition {
    ingester: Arc<str>,

    /// If using ingester2/rpc write path, the ingester UUID will be present and will identify
    /// whether this ingester has restarted since the last time it was queried or not.
    ///
    /// When we fully switch over to always using the RPC write path, the `Option` in this type can
    /// be removed.
    ingester_uuid: Option<Uuid>,

    partition_id: PartitionId,
    shard_id: ShardId,

    /// If using ingester2/rpc write path, this will be the number of Parquet files this ingester
    /// UUID has persisted for this partition.
    completed_persistence_count: u64,

    /// Maximum sequence number of parquet files the ingester has
    /// persisted for this partition
    parquet_max_sequence_number: Option<SequenceNumber>,

    /// Maximum sequence number of tombstone that the ingester has
    /// persisted for this partition
    tombstone_max_sequence_number: Option<SequenceNumber>,

    /// Partition-wide sort key.
    partition_sort_key: Option<Arc<SortKey>>,

    chunks: Vec<IngesterChunk>,
}

impl IngesterPartition {
    /// Creates a new IngesterPartition, translating the passed
    /// `RecordBatches` into the correct types
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ingester: Arc<str>,
        ingester_uuid: Option<Uuid>,
        partition_id: PartitionId,
        shard_id: ShardId,
        completed_persistence_count: u64,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
        partition_sort_key: Option<Arc<SortKey>>,
    ) -> Self {
        Self {
            ingester,
            ingester_uuid,
            partition_id,
            shard_id,
            completed_persistence_count,
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
        expected_schema: Schema,
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
            .map(|batch| ensure_schema(batch, &expected_schema))
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
            partition_id: self.partition_id,
            schema: expected_schema,
            partition_sort_key: self.partition_sort_key.clone(),
            batches,
            ts_min_max,
            summary,
        };

        self.chunks.push(chunk);

        Ok(self)
    }

    /// Update partition sort key
    pub(crate) fn with_partition_sort_key(self, partition_sort_key: Option<Arc<SortKey>>) -> Self {
        Self {
            partition_sort_key: partition_sort_key.clone(),
            chunks: self
                .chunks
                .into_iter()
                .map(|c| c.with_partition_sort_key(partition_sort_key.clone()))
                .collect(),
            ..self
        }
    }

    pub(crate) fn ingester(&self) -> &Arc<str> {
        &self.ingester
    }

    pub(crate) fn ingester_uuid(&self) -> Option<Uuid> {
        self.ingester_uuid
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
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
    partition_id: PartitionId,
    schema: Schema,

    /// Partition-wide sort key.
    partition_sort_key: Option<Arc<SortKey>>,

    /// The raw table data
    batches: Vec<RecordBatch>,

    /// Timestamp-specific stats
    ts_min_max: TimestampMinMax,

    /// Summary Statistics
    summary: Arc<TableSummary>,
}

impl IngesterChunk {
    /// [`Arc`]ed version of the partition sort key.
    ///
    /// Note that this might NOT be the up-to-date sort key of the partition but the one that existed when the chunk was
    /// created. You must sync the keys to use the chunks.
    pub(crate) fn partition_sort_key_arc(&self) -> Option<Arc<SortKey>> {
        self.partition_sort_key.clone()
    }

    pub(crate) fn with_partition_sort_key(self, partition_sort_key: Option<Arc<SortKey>>) -> Self {
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
    fn summary(&self) -> Arc<TableSummary> {
        Arc::clone(&self.summary)
    }

    fn schema(&self) -> &Schema {
        trace!(schema=?self.schema, "IngesterChunk schema");
        &self.schema
    }

    fn partition_sort_key(&self) -> Option<&SortKey> {
        self.partition_sort_key.as_ref().map(|sk| sk.as_ref())
    }

    fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    fn sort_key(&self) -> Option<&SortKey> {
        // Data is not sorted
        None
    }

    fn delete_predicates(&self) -> &[Arc<data_types::DeletePredicate>] {
        &[]
    }
}

impl QueryChunk for IngesterChunk {
    fn id(&self) -> ChunkId {
        self.chunk_id
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        // ingester just dumps data, may contain duplicates!
        true
    }

    fn column_names(
        &self,
        _ctx: IOxSessionContext,
        _predicate: &Predicate,
        _columns: Projection<'_>,
    ) -> Result<Option<StringSet>, DataFusionError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn column_values(
        &self,
        _ctx: IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<StringSet>, DataFusionError> {
        // TODO maybe some special handling?
        Ok(None)
    }

    fn data(&self) -> QueryChunkData {
        QueryChunkData::RecordBatches(self.batches.clone())
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
    use data_types::TableId;
    use generated_types::influxdata::iox::ingester::v1::PartitionStatus;
    use influxdb_iox_client::flight::generated_types::IngesterQueryResponseMetadata;
    use iox_tests::util::TestCatalog;
    use metric::Attributes;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use std::{
        collections::{BTreeSet, HashMap},
        time::Duration,
    };
    use test_helpers::assert_error;
    use tokio::{runtime::Handle, sync::Mutex};
    use trace::{ctx::SpanContext, span::SpanStatus, RingBufferTraceCollector};

    #[tokio::test]
    async fn test_flight_handshake_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Err(FlightClientError::Handshake {
                    ingester_address: String::from("addr1"),
                    source: tonic::Status::internal("don't know").into(),
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
                    source: tonic::Status::internal("cow exploded").into(),
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
                    source: tonic::Status::not_found("something").into(),
                }),
            )])
            .await,
        );
        let mut ingester_conn = mock_flight_client.ingester_conn().await;
        ingester_conn.backoff_config = BackoffConfig::default();
        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_flight_stream_error() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![Err(tonic::Status::internal("don't know").into())],
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
    async fn test_flight_unknown_partitions() {
        let record_batch = lp_to_record_batch("table foo=1 1");

        let schema = record_batch.schema();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![
                        metadata(
                            1000,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: Some(11),
                            }),
                        ),
                        metadata(
                            1001,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: Some(11),
                            }),
                        ),
                        Ok((
                            DecodedPayload::Schema(Arc::clone(&schema)),
                            IngesterQueryResponseMetadata::default(),
                        )),
                        metadata(
                            1002,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: Some(11),
                            }),
                        ),
                        Ok((
                            DecodedPayload::Schema(Arc::clone(&schema)),
                            IngesterQueryResponseMetadata::default(),
                        )),
                        Ok((
                            DecodedPayload::RecordBatch(record_batch),
                            IngesterQueryResponseMetadata::default(),
                        )),
                    ],
                }),
            )])
            .await,
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

        // Shard index 0 doesn't have an associated ingester address in the test setup
        assert_error!(
            get_partitions(&ingester_conn, &[0]).await,
            Error::NoIngesterFoundForShard { .. },
        );
    }

    #[tokio::test]
    async fn test_flight_no_batches() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![metadata(
                        1,
                        Some(PartitionStatus {
                            parquet_max_sequence_number: None,
                        }),
                    )],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let p = &partitions[0];
        assert_eq!(p.partition_id.get(), 1);
        assert_eq!(p.shard_id.get(), 1);
        assert_eq!(p.parquet_max_sequence_number, None);
        assert_eq!(p.tombstone_max_sequence_number, None);
        assert_eq!(p.chunks.len(), 0);

        // When using the write buffer path, there should never be a UUID present and the
        // completed_persistence_count should always be 0.
        assert!(p.ingester_uuid.is_none());
        assert_eq!(p.completed_persistence_count, 0);
    }

    #[tokio::test]
    async fn test_flight_err_partition_status_missing() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![metadata(1, None)],
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
                        metadata(
                            1,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: None,
                            }),
                        ),
                        metadata(
                            2,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: None,
                            }),
                        ),
                        metadata(
                            1,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: None,
                            }),
                        ),
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
                        DecodedPayload::Schema(record_batch.schema()),
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
                        DecodedPayload::RecordBatch(record_batch),
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
    async fn test_flight_many_batches_no_shard() {
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
                            metadata(
                                1,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(11),
                                }),
                            ),
                            Ok((
                                DecodedPayload::Schema(Arc::clone(&schema_1_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_1_1_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_1_1_2),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::Schema(Arc::clone(&schema_1_2)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_1_2),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            metadata(
                                2,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(21),
                                }),
                            ),
                            Ok((
                                DecodedPayload::Schema(Arc::clone(&schema_2_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_2_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Ok(MockQueryData {
                        results: vec![
                            metadata(
                                3,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(31),
                                }),
                            ),
                            Ok((
                                DecodedPayload::Schema(Arc::clone(&schema_3_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_3_1),
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
        assert_eq!(p1.shard_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(p1.tombstone_max_sequence_number, None);
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
        assert_eq!(p2.shard_id.get(), 1);
        assert_eq!(
            p2.parquet_max_sequence_number,
            Some(SequenceNumber::new(21))
        );
        assert_eq!(p2.tombstone_max_sequence_number, None);
        assert_eq!(p2.chunks.len(), 1);
        assert_eq!(p2.chunks[0].schema().as_arrow(), schema_2_1);
        assert_eq!(p2.chunks[0].batches.len(), 1);
        assert_eq!(p2.chunks[0].batches[0].schema(), schema_2_1);

        let p3 = &partitions[2];
        assert_eq!(p3.partition_id.get(), 3);
        assert_eq!(p3.shard_id.get(), 2);
        assert_eq!(
            p3.parquet_max_sequence_number,
            Some(SequenceNumber::new(31))
        );
        assert_eq!(p3.tombstone_max_sequence_number, None);
        assert_eq!(p3.chunks.len(), 1);
        assert_eq!(p3.chunks[0].schema().as_arrow(), schema_3_1);
        assert_eq!(p3.chunks[0].batches.len(), 1);
        assert_eq!(p3.chunks[0].batches[0].schema(), schema_3_1);
    }

    #[tokio::test]
    async fn ingester2_rpc_write_path_expects_valid_uuid() {
        let mock_flight_client = Arc::new(
            MockFlightClient::new([(
                "addr1",
                Ok(MockQueryData {
                    results: vec![ingester2_metadata(
                        1,
                        Some(PartitionStatus {
                            parquet_max_sequence_number: Some(11),
                        }),
                        "not-a-valid-uuid",
                        42,
                    )],
                }),
            )])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let columns = vec![String::from("col")];
        let err = ingester_conn
            .partitions(
                None,
                NamespaceId::new(1),
                cached_table(),
                columns,
                &Predicate::default(),
                None,
            )
            .await
            .unwrap_err();

        assert_matches!(err, Error::IngesterUuid { .. });
    }

    #[tokio::test]
    async fn ingester2_uuid_completed_persistence_count() {
        let ingester_uuid1 = Uuid::new_v4();
        let ingester_uuid2 = Uuid::new_v4();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                (
                    "addr1",
                    Ok(MockQueryData {
                        results: vec![
                            ingester2_metadata(
                                1,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(11),
                                }),
                                ingester_uuid1.to_string(),
                                0,
                            ),
                            ingester2_metadata(
                                2,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(21),
                                }),
                                ingester_uuid1.to_string(),
                                42,
                            ),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Ok(MockQueryData {
                        results: vec![ingester2_metadata(
                            3,
                            Some(PartitionStatus {
                                parquet_max_sequence_number: Some(31),
                            }),
                            ingester_uuid2.to_string(),
                            9000,
                        )],
                    }),
                ),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;
        let columns = vec![String::from("col")];
        let partitions = ingester_conn
            .partitions(
                None,
                NamespaceId::new(1),
                cached_table(),
                columns,
                &Predicate::default(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(partitions.len(), 3);

        let p1 = &partitions[0];
        assert_eq!(p1.ingester_uuid.unwrap(), ingester_uuid1);
        assert_eq!(p1.completed_persistence_count, 0);
        assert_eq!(p1.partition_id.get(), 1);
        assert_eq!(p1.shard_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(p1.tombstone_max_sequence_number, None);

        let p2 = &partitions[1];
        assert_eq!(p2.ingester_uuid.unwrap(), ingester_uuid1);
        assert_eq!(p2.completed_persistence_count, 42);
        assert_eq!(p2.partition_id.get(), 2);
        assert_eq!(p2.shard_id.get(), 1);
        assert_eq!(
            p2.parquet_max_sequence_number,
            Some(SequenceNumber::new(21))
        );
        assert_eq!(p2.tombstone_max_sequence_number, None);

        let p3 = &partitions[2];
        assert_eq!(p3.ingester_uuid.unwrap(), ingester_uuid2);
        assert_eq!(p3.completed_persistence_count, 9000);
        assert_eq!(p3.partition_id.get(), 3);
        assert_eq!(p3.shard_id.get(), 2);
        assert_eq!(
            p3.parquet_max_sequence_number,
            Some(SequenceNumber::new(31))
        );
        assert_eq!(p3.tombstone_max_sequence_number, None);
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
                        source: tonic::Status::internal("don't know").into(),
                    }),
                ),
                (
                    "addr4",
                    Err(FlightClientError::Handshake {
                        ingester_address: String::from("addr4"),
                        source: tonic::Status::internal("don't know").into(),
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
    async fn test_flight_per_shard_querying() {
        let record_batch_1_1 = lp_to_record_batch("table foo=1 1");
        let schema_1_1 = record_batch_1_1.schema();

        let mock_flight_client = Arc::new(
            MockFlightClient::new([
                (
                    "addr1",
                    Ok(MockQueryData {
                        results: vec![
                            metadata(
                                1,
                                Some(PartitionStatus {
                                    parquet_max_sequence_number: Some(11),
                                }),
                            ),
                            Ok((
                                DecodedPayload::Schema(Arc::clone(&schema_1_1)),
                                IngesterQueryResponseMetadata::default(),
                            )),
                            Ok((
                                DecodedPayload::RecordBatch(record_batch_1_1),
                                IngesterQueryResponseMetadata::default(),
                            )),
                        ],
                    }),
                ),
                (
                    "addr2",
                    Err(FlightClientError::Flight {
                        source: tonic::Status::internal("if this is queried, the test should fail")
                            .into(),
                    }),
                ),
            ])
            .await,
        );
        let ingester_conn = mock_flight_client.ingester_conn().await;

        // Only use shard index 1, which will correspond to only querying the ingester at
        // "addr1"
        let partitions = get_partitions(&ingester_conn, &[1]).await.unwrap();
        assert_eq!(partitions.len(), 1);

        let p1 = &partitions[0];
        assert_eq!(p1.partition_id.get(), 1);
        assert_eq!(p1.shard_id.get(), 1);
        assert_eq!(
            p1.parquet_max_sequence_number,
            Some(SequenceNumber::new(11))
        );
        assert_eq!(p1.tombstone_max_sequence_number, None);
        assert_eq!(p1.chunks.len(), 1);
    }

    async fn get_partitions(
        ingester_conn: &IngesterConnectionImpl,
        shard_indexes: &[i32],
    ) -> Result<Vec<IngesterPartition>, Error> {
        get_partitions_with_span(ingester_conn, shard_indexes, None).await
    }

    async fn get_partitions_with_span(
        ingester_conn: &IngesterConnectionImpl,
        shard_indexes: &[i32],
        span: Option<Span>,
    ) -> Result<Vec<IngesterPartition>, Error> {
        let columns = vec![String::from("col")];
        let shard_indexes: Vec<_> = shard_indexes.iter().copied().map(ShardIndex::new).collect();
        ingester_conn
            .partitions(
                Some(shard_indexes),
                NamespaceId::new(1),
                cached_table(),
                columns,
                &Predicate::default(),
                span,
            )
            .await
    }

    fn schema() -> Schema {
        SchemaBuilder::new()
            .influx_field("bar", InfluxFieldType::Float)
            .influx_field("baz", InfluxFieldType::Float)
            .influx_field("foo", InfluxFieldType::Float)
            .timestamp()
            .build()
            .unwrap()
    }

    fn lp_to_record_batch(lp: &str) -> RecordBatch {
        lp_to_mutable_batch(lp).1.to_arrow(Projection::All).unwrap()
    }

    type MockFlightResult = Result<(DecodedPayload, IngesterQueryResponseMetadata), FlightError>;

    fn metadata(partition_id: i64, status: Option<PartitionStatus>) -> MockFlightResult {
        Ok((
            DecodedPayload::None,
            IngesterQueryResponseMetadata {
                partition_id,
                status,
                // These fields are only used in ingester2.
                ingester_uuid: String::new(),
                completed_persistence_count: 0,
            },
        ))
    }

    fn ingester2_metadata(
        partition_id: i64,
        status: Option<PartitionStatus>,
        ingester_uuid: impl Into<String>,
        completed_persistence_count: u64,
    ) -> MockFlightResult {
        Ok((
            DecodedPayload::None,
            IngesterQueryResponseMetadata {
                partition_id,
                status,
                ingester_uuid: ingester_uuid.into(),
                completed_persistence_count,
            },
        ))
    }

    #[derive(Debug)]
    struct MockQueryData {
        results: Vec<MockFlightResult>,
    }

    #[async_trait]
    impl QueryData for MockQueryData {
        async fn next_message(
            &mut self,
        ) -> Result<Option<(DecodedPayload, IngesterQueryResponseMetadata)>, FlightError> {
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
            let ns = catalog.create_namespace_1hr_retention("namespace").await;
            let table = ns.create_table("table").await;

            let s0 = ns.create_shard(0).await;
            let s1 = ns.create_shard(1).await;

            table.with_shard(&s0).create_partition("k1").await;
            table.with_shard(&s0).create_partition("k2").await;
            table.with_shard(&s1).create_partition("k3").await;

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

        // Assign one shard per address, sorted consistently.
        // Don't assign any addresses to shard index 0 to test error case
        async fn ingester_conn(self: &Arc<Self>) -> IngesterConnectionImpl {
            let ingester_addresses: BTreeSet<_> =
                self.responses.lock().await.keys().cloned().collect();

            let shard_to_ingesters = ingester_addresses
                .into_iter()
                .enumerate()
                .map(|(shard_index, ingester_address)| {
                    (
                        ShardIndex::new(shard_index as i32 + 1),
                        IngesterMapping::Addr(Arc::from(ingester_address.as_str())),
                    )
                })
                .collect();

            IngesterConnectionImpl::by_shard_with_flight_client(
                shard_to_ingesters,
                Arc::clone(self) as _,
                Arc::new(CatalogCache::new_testing(
                    self.catalog.catalog(),
                    self.catalog.time_provider(),
                    self.catalog.metric_registry(),
                    self.catalog.object_store(),
                    &Handle::current(),
                )),
                BackoffConfig {
                    init_backoff: Duration::from_secs(1),
                    max_backoff: Duration::from_secs(2),
                    base: 1.1,
                    deadline: Some(Duration::from_millis(500)),
                },
            )
        }
    }

    #[async_trait]
    impl IngesterFlightClient for MockFlightClient {
        async fn invalidate_connection(&self, _ingester_address: Arc<str>) {
            // no cache
        }

        async fn query(
            &self,
            ingester_address: Arc<str>,
            _request: IngesterQueryRequest,
            _span_context: Option<SpanContext>,
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
        let expected_schema = SchemaBuilder::new().tag("t").timestamp().build().unwrap();

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
                None,
                PartitionId::new(1),
                ShardId::new(1),
                0,
                parquet_max_sequence_number,
                tombstone_max_sequence_number,
                None,
            )
            .try_add_chunk(ChunkId::new(), expected_schema.clone(), vec![case])
            .unwrap();

            for batch in &ingester_partition.chunks[0].batches {
                assert_eq!(batch.schema(), expected_schema.as_arrow());
            }
        }
    }

    #[test]
    fn test_ingester_partition_fail_type_cast() {
        let expected_schema = SchemaBuilder::new()
            .field("b", DataType::Boolean)
            .unwrap()
            .timestamp()
            .build()
            .unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("b", int64_array()), ("time", ts_array())]).unwrap();

        let parquet_max_sequence_number = None;
        let tombstone_max_sequence_number = None;
        let err = IngesterPartition::new(
            "ingester".into(),
            None,
            PartitionId::new(1),
            ShardId::new(1),
            0,
            parquet_max_sequence_number,
            tombstone_max_sequence_number,
            None,
        )
        .try_add_chunk(ChunkId::new(), expected_schema, vec![batch])
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

    fn cached_table() -> Arc<CachedTable> {
        Arc::new(CachedTable {
            id: TableId::new(2),
            schema: schema(),
            column_id_map: Default::default(),
            column_id_map_rev: Default::default(),
            primary_key_column_ids: Default::default(),
        })
    }
}
