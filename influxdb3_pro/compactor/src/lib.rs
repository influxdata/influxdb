use crate::planner::{CompactionPlanGroup, SnapshotAdvancePlan};
use arrow::array::as_largestring_array;
use arrow::array::AsArray;
use arrow::array::RecordBatch;
use arrow::compute::cast_with_options;
use arrow::compute::partition;
use arrow::compute::CastOptions;
use arrow::compute::Partitions;
use arrow::datatypes::TimestampNanosecondType;
use arrow::util::display::FormatOptions;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_util::util::ensure_schema;
use bytes::Bytes;
use data_types::ChunkId;
use data_types::ChunkOrder;
use data_types::PartitionHashId;
use data_types::PartitionKey;
use data_types::TableId;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::{future::join_all, TryFutureExt};
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_catalog::catalog::TIME_COLUMN_NAME;
use influxdb3_config::ProConfig;
use influxdb3_id::ColumnId;
use influxdb3_id::ParquetFileId;
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::{CompactedFilePath, Generation, GenerationLevel};
use influxdb3_pro_index::FileIndex;
use influxdb3_write::ParquetFile;
use influxdb3_write::{
    chunk::ParquetChunk,
    parquet_cache::{CacheRequest, ParquetCacheOracle},
};
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::chunk_statistics::NoColumnRanges;
use iox_query::exec::Executor;
use iox_query::frontend::reorg;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::PutResult;
use observability_deps::tracing::{debug, error, info, warn};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::errors::ParquetError;
use parquet_file::storage::ParquetExecInput;
use schema::sort::SortKey;
use schema::Schema;
use std::fmt::Debug;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::RwLock;

pub mod planner;
mod runner;

#[derive(Debug, thiserror::Error)]
pub enum CompactorError {
    #[error("The db to be compacted does not exist")]
    MissingDB,
    #[error("The table to be compacted does not exist and no schema can be found")]
    MissingSchema,
    #[error("No files were compacted when executing the compactor")]
    NoCompactedFiles,
    #[error("Failed to put data into obj store: {0}")]
    FailedPut(object_store::Error),
    #[error("Failed get to obj store: {0}")]
    FailedGet(object_store::Error),
    #[error("Failed to create a new AsyncArrowWriter: {0}")]
    NewArrowWriter(ParquetError),
    #[error("Failed to write into an AsyncArrowWriter: {0}")]
    WriteArrowWriter(ParquetError),
    #[error("Failed to close an AsyncArrowWriter: {0}")]
    CloseArrowWriter(ParquetError),
    #[error("Failed to flush an AsyncArrowWriter: {0}")]
    FlushArrowWriter(ParquetError),
    #[error("Failed to ensure schema: {0}")]
    EnsureSchema(ArrowError),
    #[error("Failed to produce a RecordBatch stream: {0}")]
    RecordStream(DataFusionError),
    #[error("Failed to produce a RecordBatch: {0}")]
    FailedRecordStream(DataFusionError),
    #[error("'{0}' was not a column name in this table")]
    MissingColumnName(Arc<str>),
    #[error("'{0}' was not a column id in this table")]
    MissingColumnId(ColumnId),
    #[error("Failed to partition RecordBatch: {0}")]
    FailedPartition(ArrowError),
    #[error("Failed to create a reorg plan: {0}")]
    ReorgError(reorg::Error),
    #[error("Failed to execute a LogicalPlan: {0}")]
    FailedLogicalPlan(DataFusionError),
    #[error("Failed to cast an array while indexing data: {0}")]
    CastError(ArrowError),
}

#[derive(Debug)]
pub struct CompactorOutput {
    pub output_paths: Vec<ObjPath>,
    pub file_index: FileIndex,
    pub file_metadata: Vec<ParquetFile>,
}

#[derive(Debug)]
pub struct Compactor {
    compacted_data: Arc<CompactedData>,
    catalog: Arc<Catalog>,
    object_store_url: ObjectStoreUrl,
    executor: Arc<Executor>,
    parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
    pro_config: Arc<RwLock<ProConfig>>,
}

impl Compactor {
    pub async fn new(
        compacted_data: Arc<CompactedData>,
        catalog: Arc<Catalog>,
        object_store_url: ObjectStoreUrl,
        executor: Arc<Executor>,
        parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
        pro_config: Arc<RwLock<ProConfig>>,
    ) -> Result<Self, influxdb3_pro_data_layout::compacted_data::Error> {
        Ok(Self {
            compacted_data,
            catalog,
            object_store_url,
            executor,
            parquet_cache_prefetcher,
            pro_config,
        })
    }

    pub async fn compact(self) {
        info!("starting compaction loop");
        let mut new_snapshot = self.compacted_data.snapshot_notification_receiver();
        let generation_levels = self.compacted_data.compaction_config.compaction_levels();

        loop {
            let check_snapshot = tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => false,
                v = new_snapshot.recv() => {
                    match v {
                        Ok(_) => true,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            // Ignore the lagged error and continue
                            warn!("lagged snapshot notification, continuing");
                            true
                        }
                        Err(e) => {
                            error!(error = ?e, "error receiving snapshot, exiting compaction loop");
                            break;
                        }
                    }
                }
            };

            if check_snapshot {
                let host = self
                    .compacted_data
                    .last_snapshot_host()
                    .unwrap_or("unknown".to_string());
                debug!(host = ?host, "received new snapshot");

                if let Some(snapshot_plan) =
                    SnapshotAdvancePlan::should_advance(&self.compacted_data)
                {
                    let compaction_summary = runner::run_snapshot_plan(
                        snapshot_plan,
                        Arc::clone(&self.compacted_data),
                        Arc::clone(&self.catalog),
                        self.object_store_url.clone(),
                        Arc::clone(&self.executor),
                        self.parquet_cache_prefetcher.clone(),
                        Arc::clone(&self.pro_config),
                    )
                    .await;
                    info!(?compaction_summary, "completed snapshot plan");
                }
            }

            for level in &generation_levels {
                // TODO: wire up later generation compactions
                if *level > GenerationLevel::new(4) {
                    break;
                }

                if let Some(plan_group) =
                    CompactionPlanGroup::plans_for_level(&self.compacted_data, *level)
                {
                    let compaction_summary = runner::run_compaction_plan_group(
                        plan_group,
                        Arc::clone(&self.compacted_data),
                        Arc::clone(&self.catalog),
                        self.object_store_url.clone(),
                        Arc::clone(&self.executor),
                        self.parquet_cache_prefetcher.clone(),
                        Arc::clone(&self.pro_config),
                    )
                    .await;
                    info!(?compaction_summary, "completed compaction plan group");

                    // only one run set of compactions, then go back to waiting for a snapshot
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParquetCachePreFetcher {
    /// Cache oracle to prefetch into cache
    parquet_cache: Arc<dyn ParquetCacheOracle>,
    /// Duration allowed for prefetching
    preemptive_cache_age_secs: std::time::Duration,
    /// Time provider
    time_provider: Arc<dyn TimeProvider>,
}

impl ParquetCachePreFetcher {
    pub fn new(
        parquet_cache: Arc<dyn ParquetCacheOracle>,
        preemptive_cache_age: humantime::Duration,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            parquet_cache,
            preemptive_cache_age_secs: std::time::Duration::from_secs(
                preemptive_cache_age.as_secs(),
            ),
            time_provider,
        }
    }

    pub async fn prefetch_all(
        &self,
        parquet_infos: &Vec<ParquetFile>,
    ) -> Vec<Result<(), RecvError>> {
        let all_futures = self.prepare_prefetch_requests(parquet_infos);
        // `join_all` uses `FuturesOrdered` internally, might be nicer to have `FuturesUnordered` for this
        // case
        join_all(all_futures).await
    }

    fn prepare_prefetch_requests(
        &self,
        parquet_infos: &Vec<ParquetFile>,
    ) -> Vec<impl Future<Output = Result<(), RecvError>>> {
        let mut futures = Vec::with_capacity(parquet_infos.len());
        for parquet_meta in parquet_infos {
            let (cache_request, receiver) =
                CacheRequest::create(ObjPath::from(parquet_meta.path.as_str()));
            let logging_receiver = receiver.inspect_err(|err| {
                // NOTE: This warning message never comes out when file is missing in object store,
                //       instead see "failed to fulfill cache request with object store". Looks like
                //       `background_cache_request_handler` always calls `notifier.send(())`. This
                //       might be the expected behaviour in this interface.
                warn!(err = ?err, "Errored when trying to prefetch into cache");
            });
            self.register(cache_request, parquet_meta.max_time);
            futures.push(logging_receiver);
        }
        futures
    }

    fn register(&self, cache_request: CacheRequest, parquet_max_time: i64) {
        let now = self.time_provider.now();
        self.check_and_register(now, cache_request, parquet_max_time);
    }

    fn check_and_register(&self, now: Time, cache_request: CacheRequest, parquet_max_time: i64) {
        if self.should_prefetch(now, parquet_max_time) {
            self.parquet_cache.register(cache_request);
        }
    }

    fn should_prefetch(&self, now: Time, parquet_max_time: i64) -> bool {
        // This check is to make sure we don't prefetch compacted files that are old
        // and don't cover most recent period. If we are interested in caching only last
        // 3 days periods worth of data, there is no point in prefetching a file that holds
        // data for a period that ends before last 3 days.
        let min_time_for_prefetching = now - self.preemptive_cache_age_secs;
        parquet_max_time >= min_time_for_prefetching.timestamp_nanos()
    }
}

#[derive(Debug)]
pub struct CompactFilesArgs {
    pub compactor_id: Arc<str>,
    pub table_def: Arc<TableDefinition>,
    pub paths: Vec<ObjPath>,
    pub limit: usize,
    pub generation: Generation,
    pub index_columns: Vec<ColumnId>,
    pub object_store: Arc<dyn ObjectStore>,
    pub object_store_url: ObjectStoreUrl,
    pub exec: Arc<Executor>,
    pub parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
}

/// Compact `paths` together into one or more parquet files
///
/// The limit is the maximum number of rows that can be in a single file.
/// This number can be exceeded if a single series is larger than the limit
pub async fn compact_files(
    CompactFilesArgs {
        compactor_id,
        table_def,
        paths,
        limit,
        generation,
        index_columns,
        object_store,
        object_store_url,
        exec,
        parquet_cache_prefetcher,
    }: CompactFilesArgs,
) -> Result<CompactorOutput, CompactorError> {
    let dedupe_key: Vec<_> = table_def.schema.primary_key();
    let dedupe_key = SortKey::from_columns(dedupe_key);

    let records = record_stream(
        Arc::clone(&table_def),
        dedupe_key,
        paths,
        Arc::clone(&object_store),
        object_store_url,
        exec,
    )
    .await?;

    let mut series_writer = SeriesWriter::new(
        table_def,
        object_store,
        limit,
        records,
        compactor_id,
        generation,
        index_columns,
        parquet_cache_prefetcher.clone(),
    );

    loop {
        // If there is nothing left exit the loop
        let Some(batch) = series_writer.next_batch().await? else {
            break;
        };
        series_writer.push_batch(batch).await?;
    }

    series_writer.finish().await
}

/// Get a stream of `RecordBatch` formed from a set of input paths that get merged and deduplicated
/// into a single stream.
async fn record_stream(
    table_def: Arc<TableDefinition>,
    sort_key: SortKey,
    paths: Vec<ObjPath>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<SendableRecordBatchStream, CompactorError> {
    // sort, but to dedupe data. We use the same PartitionKey for every file to accomplish this.
    // This is a concept for IOx and the query planner can be clever about deduping data if data
    // is in separate partitions. Mainly that it won't due to assuming that different partitions
    // will have non overlapping data.
    let partition_id = data_types::TransitionPartitionId::Deterministic(PartitionHashId::new(
        TableId::new(0),
        &PartitionKey::from("synthetic-key"),
    ));

    let mut chunks = Vec::new();
    for (id, location) in paths.iter().enumerate() {
        let meta = object_store
            .get(location)
            .await
            .map_err(CompactorError::FailedGet)?
            .meta;
        let parquet_exec = ParquetExecInput {
            object_store_url: object_store_url.clone(),
            object_meta: ObjectMeta {
                location: location.clone(),
                last_modified: meta.last_modified,
                size: meta.size,
                e_tag: meta.e_tag,
                version: meta.version,
            },
            object_store: Arc::clone(&object_store),
        };

        let chunk_stats = create_chunk_statistics(None, &table_def.schema, None, &NoColumnRanges);

        let parquet_chunk: Arc<dyn QueryChunk> = Arc::new(ParquetChunk {
            partition_id: partition_id.clone(),
            schema: table_def.schema.clone(),
            stats: Arc::new(chunk_stats),
            sort_key: Some(sort_key.clone()),
            id: ChunkId::new_id(id as u128),
            chunk_order: ChunkOrder::new(id as i64),
            parquet_exec,
        });

        chunks.push(parquet_chunk);
    }

    // Create the plan and execute it over all of the ParquetChunks
    let reorg = ReorgPlanner::new();
    let plan = reorg
        .compact_plan(
            TableId::new(0),
            Arc::clone(&table_def.table_name),
            &table_def.schema,
            chunks,
            sort_key,
        )
        .map_err(CompactorError::ReorgError)?;
    exec.new_context()
        .inner()
        .execute_logical_plan(plan)
        .await
        .map_err(CompactorError::FailedLogicalPlan)?
        .execute_stream()
        .await
        .map_err(CompactorError::RecordStream)
}

/// Handles writing RecordBatches to a file in the object store
/// ensuring each series is written to the same file
struct SeriesWriter {
    table_def: Arc<TableDefinition>,
    object_store: Arc<dyn ObjectStore>,
    /// the target size of each output file
    limit: usize,
    /// Rows from the previous record batch that were in the last series, if any
    last_batch: Option<RecordBatch>,
    /// The currently open writer
    writer: Option<AsyncArrowWriter<AsyncMultiPart>>,
    /// Used to get the next `RecordBatch` to process
    record_batches: RecordBatchHolder,
    /// Series key (e.g. tags) to separate data out by
    series_key: Vec<Arc<str>>,
    /// Files we have created
    output_paths: Vec<ObjPath>,
    /// Metadata for the files we have created
    file_metadata: Vec<ParquetFile>,
    /// The ID of the compactor
    compactor_id: Arc<str>,
    /// What generation the data is being compacted into
    generation: Generation,
    /// The current file id to use for this file we are compacting into
    current_file_id: ParquetFileId,
    /// What columns we are indexing on for the `FileIndex`
    index_columns: Vec<ColumnId>,
    /// The `FileIndex` for this table that we are compacting
    file_index: FileIndex,
    /// Min time for the current file
    min_time: i64,
    /// Max time for the current file
    max_time: i64,
    /// Parquet cache oracle to prefetch parquet file into cache
    parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
}

impl SeriesWriter {
    /// Create a new `SeriesWriter` which maintains all of the state for writing out series for
    /// compactions
    #[allow(clippy::too_many_arguments)]
    fn new(
        table_def: Arc<TableDefinition>,
        object_store: Arc<dyn ObjectStore>,
        limit: usize,
        stream: SendableRecordBatchStream,
        compactor_id: Arc<str>,
        generation: Generation,
        index_columns: Vec<ColumnId>,
        parquet_cache_prefetcher: Option<ParquetCachePreFetcher>,
    ) -> Self {
        // TODO: this should come directly from a method on the table definition, and it is being
        // somewhat confused with the "series key" from the v3 line protocol.
        let series_key = table_def
            .schema
            .primary_key()
            .iter()
            .filter(|f| **f != TIME_COLUMN_NAME)
            .map(|f| Arc::from(*f))
            .collect::<Vec<_>>();

        Self {
            record_batches: RecordBatchHolder::new(stream, Arc::new(table_def.schema.clone())),
            table_def,
            object_store,
            limit,
            last_batch: None,
            writer: None,
            output_paths: Vec::new(),
            file_metadata: Vec::new(),
            compactor_id,
            generation,
            current_file_id: ParquetFileId::new(),
            index_columns: index_columns.into_iter().map(Into::into).collect(),
            file_index: FileIndex::new(),
            min_time: i64::MAX,
            max_time: i64::MIN,
            series_key,
            parquet_cache_prefetcher,
        }
    }

    /// Close out any leftover writers and return the paths
    async fn finish(mut self) -> Result<CompactorOutput, CompactorError> {
        // close the remaining writer, if any
        if let Some(writer) = self.writer.take() {
            self.close(writer).await?;
        }

        // This should never happen, but if it is we should throw an error
        if self.output_paths.is_empty() {
            Err(CompactorError::NoCompactedFiles)
        } else {
            // prefetch compacted files into cache
            if let Some(pre_fetcher) = self.parquet_cache_prefetcher {
                pre_fetcher.prefetch_all(&self.file_metadata).await;
            }

            Ok(CompactorOutput {
                output_paths: self.output_paths,
                file_index: self.file_index,
                file_metadata: self.file_metadata,
            })
        }
    }

    /// Push a new batch of data into the writer
    async fn push_batch(&mut self, batch: RecordBatch) -> Result<(), CompactorError> {
        let mut writer = self.get_writer().await?;
        let time = batch
            .column_by_name(TIME_COLUMN_NAME)
            .expect("RecordBatch has a time column");
        let min =
            arrow::compute::min(time.as_primitive::<TimestampNanosecondType>()).unwrap_or(i64::MAX);
        let max =
            arrow::compute::max(time.as_primitive::<TimestampNanosecondType>()).unwrap_or(i64::MIN);
        if self.min_time > min {
            self.min_time = min;
        }
        if self.max_time < max {
            self.max_time = max
        }

        // if there is space remaining within the limit for this writer, write the whole batch.
        if writer.in_progress_rows() + batch.num_rows() <= self.limit {
            writer
                .write(&batch)
                .await
                .map_err(CompactorError::WriteArrowWriter)?;
            self.index_record_batch(&batch)?;
            self.last_batch = Some(batch);
            self.writer = Some(writer);
            return Ok(());
        }

        // Check if this current batch starts with the same series the last batch ended with
        let mut same_series = true;
        if let Some(last_batch) = self.last_batch.take() {
            let last_batch_slice = last_batch.slice(last_batch.num_rows() - 1, 1);
            let batch_slice = batch.slice(0, 1);
            for key in self.series_key.iter() {
                same_series = same_series
                    && batch_slice
                        .column_by_name(key)
                        .eq(&last_batch_slice.column_by_name(key));
                if !same_series {
                    break;
                }
            }
        }

        // Find where the series are
        let partitions = self.partition_by_series(&batch)?;

        let ranges = partitions.ranges();
        let partition = &ranges[0];
        let len = partition.end - partition.start;

        match partitions.len() {
            // If there is only one partition and we're the same series write out the whole
            // batch. We might still have more records for this series and we will close it
            // later.
            1 if same_series => {
                writer
                    .write(&batch)
                    .await
                    .map_err(CompactorError::WriteArrowWriter)?;
                self.index_record_batch(&batch)?;
                self.last_batch = Some(batch);
                self.writer = Some(writer);
                return Ok(());
            }
            // If there's only one partition, but it's not the same series, then we check
            // to see if we're over or at the limit. If we are close it, if not then in
            // the next iteration of the loop we'll write out the batch into the writer.
            1 if !same_series => {
                self.set_leftover_batch(batch);
                if writer.in_progress_rows() >= self.limit {
                    self.close(writer).await?;
                    return Ok(());
                }
                self.last_batch = None;
                self.writer = Some(writer);
            }
            // If there are more than one partitions then we need to write out the slice
            // and close the writer if we go over the limit
            _ => {
                let next_partition = &ranges[1];
                let next_len = next_partition.end - next_partition.start;
                let slice = batch.slice(0, len);
                writer
                    .write(&slice)
                    .await
                    .map_err(CompactorError::WriteArrowWriter)?;

                self.index_record_batch(&slice)?;
                self.last_batch = Some(slice);

                let batch_slice = batch.slice(len, batch.num_rows() - len);
                if batch_slice.num_rows() > 0 {
                    self.set_leftover_batch(batch_slice);
                }

                if writer.in_progress_rows() + next_len > self.limit {
                    self.close(writer).await?;
                    return Ok(());
                }

                self.writer = Some(writer);
            }
        }

        Ok(())
    }

    /// Get the next batch to push if it exists
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, CompactorError> {
        self.record_batches.next().await
    }

    /// Get a writer for the next file, if there is no writer available create a new one and
    /// push a new path into our outputs that will contain all of the written data eventually
    async fn get_writer(&mut self) -> Result<AsyncArrowWriter<AsyncMultiPart>, CompactorError> {
        match self.writer.take() {
            Some(writer) => Ok(writer),
            None => {
                self.current_file_id = ParquetFileId::new();
                let path = CompactedFilePath::new(
                    &self.compactor_id,
                    self.generation.id,
                    self.current_file_id,
                );
                let obj_store_multipart = self
                    .object_store
                    .put_multipart(path.as_object_store_path())
                    .await
                    .map_err(CompactorError::FailedPut)?;
                let writer = AsyncArrowWriter::try_new(
                    AsyncMultiPart::new(obj_store_multipart),
                    self.record_batches.schema.as_arrow(),
                    None,
                )
                .map_err(CompactorError::NewArrowWriter)?;
                self.output_paths.push(path.as_object_store_path().clone());
                self.min_time = i64::MAX;
                self.max_time = i64::MIN;
                Ok(writer)
            }
        }
    }

    /// Find where the series are in the record batch
    fn partition_by_series(&self, batch: &RecordBatch) -> Result<Partitions, CompactorError> {
        // We want to partition by the series key columns in the batch
        let columns = self
            .series_key
            .iter()
            .map(|name| {
                Ok(Arc::clone(batch.column_by_name(name).ok_or_else(|| {
                    CompactorError::MissingColumnName(Arc::clone(name))
                })?))
            })
            .collect::<Result<Vec<_>, _>>()?;

        partition(&columns).map_err(CompactorError::FailedPartition)
    }

    /// Sets the leftover record batch in the `RecordBatchHolder`
    fn set_leftover_batch(&mut self, batch: RecordBatch) {
        self.record_batches.leftover_batch = Some(batch)
    }

    /// Index a record batch for the current output file
    fn index_record_batch(&mut self, batch: &RecordBatch) -> Result<(), CompactorError> {
        for column_id in self.index_columns.iter() {
            let column_name = self
                .table_def
                .column_id_to_name(column_id)
                .ok_or_else(|| CompactorError::MissingColumnId(*column_id))?;
            let array = batch.column_by_name(&column_name).unwrap();
            // If the cast fails use null for the value. We might lose out on the indexing,
            // but this way we can handle most things
            let casted = cast_with_options(
                &array,
                &DataType::LargeUtf8,
                &CastOptions {
                    safe: true,
                    format_options: FormatOptions::new().with_null("null"),
                },
            )
            .map_err(CompactorError::CastError)?;
            let downcasted = as_largestring_array(&casted);
            for value in downcasted.iter().map(|s| s.unwrap_or("null")) {
                self.file_index
                    .insert(column_name.as_ref(), value, &self.current_file_id);
            }
        }
        Ok(())
    }

    async fn close(
        &mut self,
        mut writer: AsyncArrowWriter<AsyncMultiPart>,
    ) -> Result<(), CompactorError> {
        writer
            .flush()
            .await
            .map_err(CompactorError::FlushArrowWriter)?;

        let metadata = writer
            .close()
            .await
            .map_err(CompactorError::CloseArrowWriter)?;

        // we need the size of the file written into object storage and that doesn't seem to
        // be available from the writer. The bytes_written method on the writer returns a number
        // smaller than the actual file size. So we'll get the metadata from object storage to
        // see what we wrote.
        let size_bytes = self
            .object_store
            .head(self.output_paths.last().unwrap())
            .await
            .map_err(CompactorError::FailedGet)?
            .size;

        self.file_metadata.push(ParquetFile {
            id: self.current_file_id,
            path: self.output_paths.last().unwrap().to_string(),
            size_bytes: size_bytes as u64,
            // i64 to u64 cast
            row_count: metadata.num_rows as u64,
            chunk_time: self.min_time,
            min_time: self.min_time,
            max_time: self.max_time,
        });

        Ok(())
    }
}

/// Maintains the state for what is the next batch to use
pub struct RecordBatchHolder {
    /// The stream of sorted record batches from all of the parquet files to iterate over
    stream: SendableRecordBatchStream,
    /// If we partition a batch store the leftover batch here
    leftover_batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl RecordBatchHolder {
    /// Create a new `RecordBatchHolder`
    fn new(stream: SendableRecordBatchStream, schema: Arc<Schema>) -> Self {
        RecordBatchHolder {
            stream,
            schema,
            leftover_batch: None,
        }
    }

    /// Get the next batch. We prioritize the leftover batch as this was what's left
    /// from splitting the prior `RecordBatch`. If there is nothing leftover we attempt
    /// to grab the next batch from the stream if there is one. We also make sure that
    /// the record that we read from the stream matches the schema via `ensure_schema`
    async fn next(&mut self) -> Result<Option<RecordBatch>, CompactorError> {
        match self.leftover_batch.take() {
            Some(record) => Ok(Some(record)),
            None => match self.stream.next().await {
                Some(Ok(record)) => ensure_schema(&self.schema.as_arrow(), &record)
                    .map(Some)
                    .map_err(CompactorError::EnsureSchema),
                Some(Err(err)) => Err(CompactorError::FailedRecordStream(err)),
                None => Ok(None),
            },
        }
    }
}

impl Debug for RecordBatchHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchHolder")
            .field(
                "stream",
                &"Peekable<SendableRecordBatchStream>".to_string() as &dyn Debug,
            )
            .field("leftover_batch", &self.leftover_batch)
            .field("schema", &self.schema)
            .finish()
    }
}

struct AsyncMultiPart {
    multi_part: Box<dyn MultipartUpload>,
    bytes_buffer: Vec<u8>,
}

impl AsyncMultiPart {
    fn new(multi_part: Box<dyn MultipartUpload>) -> Self {
        Self {
            multi_part,
            bytes_buffer: Vec::new(),
        }
    }
}

struct AsyncMultiPartWrite {
    payload: Pin<Box<dyn Future<Output = Result<(), object_store::Error>> + Send + 'static>>,
}

impl Future for AsyncMultiPartWrite {
    type Output = parquet::errors::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.payload.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(parquet::errors::ParquetError::General(err.to_string())))
            }
        }
    }
}

struct AsyncMultiPartComplete<'complete> {
    complete:
        Pin<Box<dyn Future<Output = Result<PutResult, object_store::Error>> + Send + 'complete>>,
}

impl Future for AsyncMultiPartComplete<'_> {
    type Output = parquet::errors::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.complete.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(parquet::errors::ParquetError::General(err.to_string())))
            }
        }
    }
}

/// s3 and other Object Stores require an 5mb minimum for all parts and in some cases like with R2 each
/// part must be 5mb. We need to handle that use case
const MULTIPART_UPLOAD_MINIMUM: usize = 5242880; // 5mb as bytes

impl AsyncFileWriter for AsyncMultiPart {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        self.bytes_buffer.extend_from_slice(bs.as_ref());
        if self.bytes_buffer.len() < MULTIPART_UPLOAD_MINIMUM {
            Box::pin(async { Ok(()) })
        } else {
            let buffer = self
                .bytes_buffer
                .drain(0..MULTIPART_UPLOAD_MINIMUM)
                .collect::<Vec<u8>>();
            Box::pin(AsyncMultiPartWrite {
                payload: self
                    .multi_part
                    .put_part(PutPayload::from_bytes(Bytes::from(buffer))),
            })
        }
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async {
            if !self.bytes_buffer.is_empty() {
                Box::pin(AsyncMultiPartWrite {
                    payload: self
                        .multi_part
                        // Since we're completing the upload grab the rest of the bytes and write them out
                        // to object store before completing the upload
                        .put_part(PutPayload::from_bytes(Bytes::from(mem::take(
                            &mut self.bytes_buffer,
                        )))),
                })
                .await?;
            }

            // Now that we have written out any residual data we can complete the upload
            AsyncMultiPartComplete {
                complete: self.multi_part.complete(),
            }
            .await
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use chrono::Utc;
    use humantime::Duration;
    use influxdb3_id::ParquetFileId;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_write::{
        parquet_cache::{create_cached_obj_store_and_oracle, CacheRequest, ParquetCacheOracle},
        ParquetFile,
    };
    use iox_time::{MockProvider, Time};
    use object_store::{memory::InMemory, path::Path as ObjPath, PutPayload};
    use observability_deps::tracing::debug;
    use pretty_assertions::assert_eq;

    use crate::ParquetCachePreFetcher;

    const PATH: &str = "sample/test/file/path.parquet";

    #[derive(Debug)]
    struct MockCacheOracle;

    impl ParquetCacheOracle for MockCacheOracle {
        fn register(&self, cache_request: CacheRequest) {
            debug!(path = ?cache_request.get_path(), "calling cache with request path");
            assert_eq!(PATH, cache_request.get_path().as_ref());
        }

        fn prune_notifier(&self) -> tokio::sync::watch::Receiver<usize> {
            unimplemented!()
        }
    }

    fn setup_prefetcher_3days(oracle: Arc<dyn ParquetCacheOracle>) -> ParquetCachePreFetcher {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let mock_time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(now)));
        ParquetCachePreFetcher::new(
            oracle,
            Duration::from_str("3d").unwrap(),
            mock_time_provider,
        )
    }

    fn setup_mock_prefetcher_3days() -> ParquetCachePreFetcher {
        let mock_cache_oracle = Arc::new(MockCacheOracle);
        setup_prefetcher_3days(mock_cache_oracle)
    }

    fn parquet_max_time_nanos(num_days: i64) -> (Time, i64) {
        let now = Utc::now();
        let parquet_max_time = (now - chrono::Duration::days(num_days))
            .timestamp_nanos_opt()
            .unwrap();
        (
            Time::from_timestamp_nanos(now.timestamp_nanos_opt().unwrap()),
            parquet_max_time,
        )
    }

    #[test]
    fn test_cache_pre_fetcher_time_after_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(2);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(should_prefetch);
    }

    #[test]
    fn test_cache_pre_fetcher_time_equals_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(3);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(should_prefetch);
    }

    #[test]
    fn test_cache_pre_fetcher_time_before_lower_bound() {
        let (now, parquet_max_time) = parquet_max_time_nanos(4);
        let pre_fetcher = setup_mock_prefetcher_3days();

        let should_prefetch = pre_fetcher.should_prefetch(now, parquet_max_time);

        assert!(!should_prefetch);
    }

    #[test]
    fn test_cache_prefetcher_register() {
        let (now, parquet_max_time) = parquet_max_time_nanos(3);
        let pre_fetcher = setup_mock_prefetcher_3days();
        let (cache_request, _) = CacheRequest::create(ObjPath::from(PATH));

        pre_fetcher.check_and_register(now, cache_request, parquet_max_time);
    }

    #[test_log::test(tokio::test)]
    async fn test_cache_prefetcher_with_errors() {
        let inner_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // These values are taken from parquet_cache tests. These are not relevant per se
        // for these tests just reuse.
        let cache_capacity_bytes = 60;
        let cache_prune_percent = 0.4;
        let cache_prune_interval = std::time::Duration::from_secs(100);
        let (cached_store, oracle) = create_cached_obj_store_and_oracle(
            Arc::clone(&inner_store) as _,
            Arc::clone(&time_provider) as _,
            cache_capacity_bytes,
            cache_prune_percent,
            cache_prune_interval,
        );
        // add a file to object store
        let path_100 = ObjPath::from("100.parquet");
        let payload_100 = b"file100";
        cached_store
            .put(&path_100, PutPayload::from_static(payload_100))
            .await
            .unwrap();

        let parquet_cache_prefetcher = setup_prefetcher_3days(oracle);
        let file_1 = ParquetFile {
            id: ParquetFileId::from(0),
            path: "1.parquet".to_owned(),
            size_bytes: 200,
            row_count: 8,
            chunk_time: 123456789000,
            min_time: 123456789000,
            max_time: Utc::now().timestamp_nanos_opt().unwrap(),
        };

        let file_2 = ParquetFile {
            id: ParquetFileId::from(1),
            path: path_100.as_ref().to_owned(),
            size_bytes: 200,
            row_count: 8,
            chunk_time: 123456789000,
            min_time: 123456789000,
            max_time: Utc::now().timestamp_nanos_opt().unwrap(),
        };

        let all_parquet_metas = vec![file_1, file_2];
        let results = parquet_cache_prefetcher
            .prefetch_all(&all_parquet_metas)
            .await;
        // both requests should've been sent
        assert_eq!(2, results.len());
        debug!(results = ?results, "results");
        // both requests should return ok although `file_1` was not
        // cached as it was missing in object store
        assert!(results.first().unwrap().is_ok());
        assert!(results.get(1).unwrap().is_ok());
    }
}
