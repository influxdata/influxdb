use crate::planner::SnapshotTracker;
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
use chrono::DateTime;
use chrono::Datelike;
use chrono::Timelike;
use chrono::Utc;
use data_types::ChunkId;
use data_types::ChunkOrder;
use data_types::PartitionHashId;
use data_types::PartitionKey;
use data_types::TableId;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::{Catalog, TIME_COLUMN_NAME};
use influxdb3_pro_data_layout::{
    CompactedData, CompactionConfig, CompactionSequenceNumber, GenerationLevel,
};
use influxdb3_pro_index::FileIndex;
use influxdb3_write::chunk::ParquetChunk;
use influxdb3_write::persister::DEFAULT_OBJECT_STORE_URL;
use influxdb3_write::ParquetFileId;
use influxdb3_write::{ParquetFile, PersistedSnapshot};
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::chunk_statistics::NoColumnRanges;
use iox_query::exec::Executor;
use iox_query::frontend::reorg;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use object_store::path::Path as ObjPath;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::PutResult;
use observability_deps::tracing::{error, info};
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::errors::ParquetError;
use parquet_file::storage::ParquetExecInput;
use schema::sort::SortKey;
use schema::Schema;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::SystemTime;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;

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
    MissingColumnName(String),
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
    compactor_id: Arc<str>,
    compaction_config: CompactionConfig,
    compacted_data: Arc<Mutex<CompactedData>>,
    catalog: Arc<Catalog>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    executor: Arc<Executor>,
    /// New snapshots for gen1 files will come through this channel
    persisted_snapshot_notify_rx: Receiver<Option<PersistedSnapshot>>,
    snapshot_tracker: SnapshotTracker,
}

#[derive(Debug)]
pub struct CompactorConfig {
    pub compactor_id: Arc<str>,
    pub compaction_hosts: Vec<String>,
    compaction_config: CompactionConfig,
}

impl CompactorConfig {
    pub fn new(compactor_id: Arc<str>, compaction_hosts: Vec<String>) -> Self {
        Self {
            compactor_id,
            compaction_hosts,
            // TODO: make this configurable
            compaction_config: CompactionConfig::default(),
        }
    }

    pub fn test() -> Self {
        Self {
            compactor_id: Arc::from("compactor_1"),
            compaction_hosts: vec![],
            compaction_config: CompactionConfig::default(),
        }
    }
}

impl Compactor {
    pub fn new(
        compactor_config: CompactorConfig,
        catalog: Arc<Catalog>,
        object_store: Arc<dyn ObjectStore>,
        executor: Arc<Executor>,
        persisted_snapshot_notify_rx: Receiver<Option<PersistedSnapshot>>,
    ) -> Self {
        let snapshot_tracker = SnapshotTracker::new(compactor_config.compaction_hosts);

        // TODO: load this from object store
        let compacted_data = Arc::new(Mutex::new(CompactedData::new(Arc::clone(
            &compactor_config.compactor_id,
        ))));

        Self {
            compactor_id: compactor_config.compactor_id,
            compaction_config: compactor_config.compaction_config,
            compacted_data,
            catalog,
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            executor,
            persisted_snapshot_notify_rx,
            snapshot_tracker,
        }
    }

    pub async fn compact(self) {
        let mut persisted_snapshot_notify_rx = self.persisted_snapshot_notify_rx.clone();

        loop {
            if persisted_snapshot_notify_rx.changed().await.is_err() {
                break;
            }

            let snapshot = match persisted_snapshot_notify_rx.borrow_and_update().clone() {
                Some(snapshot) => snapshot,
                None => continue,
            };

            info!(
                "Received new snapshot for compaction {} from {}",
                snapshot.snapshot_sequence_number.as_u64(),
                snapshot.host_id
            );
            if let Err(e) = self.snapshot_tracker.add_snapshot(&snapshot) {
                error!("Failed to add snapshot to tracker: {}", e);
                continue;
            }

            if self.snapshot_tracker.should_compact() {
                let compacted_data = self.compacted_data.lock().await;

                let snapshot_plan = self.snapshot_tracker.to_plan_and_reset(
                    &self.compaction_config,
                    &compacted_data,
                    Arc::clone(&self.object_store),
                );

                let _compaction_summary = runner::run_snapshot_plan(
                    snapshot_plan,
                    Arc::clone(&self.compactor_id),
                    Arc::clone(&self.catalog),
                    Arc::clone(&self.object_store),
                    self.object_store_url.clone(),
                    Arc::clone(&self.executor),
                )
                .await;
            }
        }
    }
}

#[derive(Debug)]
pub struct CompactFilesArgs {
    pub compactor_id: Arc<str>,
    pub compaction_sequence_number: CompactionSequenceNumber,
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    pub table_schema: Schema,
    pub paths: Vec<ObjPath>,
    pub limit: usize,
    pub generation: GenerationLevel,
    pub index_columns: Vec<String>,
    pub object_store: Arc<dyn ObjectStore>,
    pub object_store_url: ObjectStoreUrl,
    pub exec: Arc<Executor>,
}

/// Compact `paths` together into one or more parquet files
///
/// The limit is the maximum number of rows that can be in a single file.
/// This number can be exceeded if a single series is larger than the limit
pub async fn compact_files(
    CompactFilesArgs {
        compactor_id,
        compaction_sequence_number,
        db_name,
        table_name,
        table_schema,
        paths,
        limit,
        generation,
        index_columns,
        object_store,
        object_store_url,
        exec,
    }: CompactFilesArgs,
) -> Result<CompactorOutput, CompactorError> {
    executor::register_current_runtime_for_io();

    let mut dedupe_key: Vec<_> = table_schema
        .tags_iter()
        .map(|t| t.name().as_ref())
        .collect();
    dedupe_key.push(TIME_COLUMN_NAME);
    let dedupe_key = SortKey::from_columns(dedupe_key);
    println!("dedupe_key: {:?}", dedupe_key);

    let records = record_stream(
        table_name.as_ref(),
        dedupe_key,
        paths,
        &table_schema,
        Arc::clone(&object_store),
        object_store_url,
        exec,
    )
    .await?;

    let mut series_writer = SeriesWriter::new(
        Arc::new(table_schema.clone()),
        object_store,
        limit,
        records,
        compactor_id,
        generation,
        compaction_sequence_number,
        db_name,
        table_name,
        index_columns,
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
    table_name: &str,
    sort_key: SortKey,
    paths: Vec<ObjPath>,
    table_schema: &Schema,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    exec: Arc<Executor>,
) -> Result<SendableRecordBatchStream, CompactorError> {
    // We need to use the same partition id for every file if we want the reorg plan to not only
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

        let chunk_stats = create_chunk_statistics(None, table_schema, None, &NoColumnRanges);

        let parquet_chunk: Arc<dyn QueryChunk> = Arc::new(ParquetChunk {
            partition_id: partition_id.clone(),
            schema: table_schema.clone(),
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
            table_name.into(),
            table_schema,
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
    series_key: Vec<String>,
    /// Files we have created
    output_paths: Vec<ObjPath>,
    /// Metadata for the files we have created
    file_metadata: Vec<ParquetFile>,
    /// The ID of the compactor
    compactor_id: Arc<str>,
    /// What generation the data is being compacted into
    generation: GenerationLevel,
    /// Which compaction sequence we are currently on
    compaction_seq: CompactionSequenceNumber,
    /// The name of the database we are doing a compaction cycle for
    db_name: Arc<str>,
    /// The name of the table we are doing a compaction cycle for
    table_name: Arc<str>,
    /// The current file id to use for this file we are compacting into
    current_file_id: ParquetFileId,
    /// What time we started the compaction cycle at
    compaction_time: DateTime<Utc>,
    /// What columns we are indexing on for the `FileIndex`
    index_columns: Vec<Arc<str>>,
    /// The `FileIndex` for this table that we are compacting
    file_index: FileIndex,
    /// Min time for the current file
    min_time: i64,
    /// Max time for the current file
    max_time: i64,
}

impl SeriesWriter {
    /// Create a new `SeriesWriter` which maintains all of the state for writing out series for
    /// compactions
    #[allow(clippy::too_many_arguments)]
    fn new(
        table_schema: Arc<Schema>,
        object_store: Arc<dyn ObjectStore>,
        limit: usize,
        stream: SendableRecordBatchStream,
        compactor_id: Arc<str>,
        generation: GenerationLevel,
        compaction_seq: CompactionSequenceNumber,
        db_name: Arc<str>,
        table_name: Arc<str>,
        index_columns: Vec<String>,
    ) -> Self {
        let series_key = table_schema
            .tags_iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();

        Self {
            record_batches: RecordBatchHolder::new(stream, table_schema),
            object_store,
            limit,
            last_batch: None,
            writer: None,
            output_paths: Vec::new(),
            file_metadata: Vec::new(),
            compactor_id,
            generation,
            compaction_seq,
            db_name,
            table_name,
            current_file_id: ParquetFileId::new(),
            compaction_time: SystemTime::now().into(),
            index_columns: index_columns.into_iter().map(Into::into).collect(),
            file_index: FileIndex::new(),
            min_time: i64::MAX,
            max_time: i64::MIN,
            series_key,
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
            for key in self
                .series_key
                .iter()
                .filter(|name| name != &TIME_COLUMN_NAME)
            {
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
                let path = ObjPath::from(format!(
                    "{}/c/{}/{}/{}/{}-{}-{}/{}-{}/f/{}.{}.parquet",
                    self.compactor_id,
                    self.db_name,
                    self.table_name,
                    self.generation,
                    self.compaction_time.year(),
                    self.compaction_time.month(),
                    self.compaction_time.day(),
                    self.compaction_time.hour(),
                    self.compaction_time.minute(),
                    self.compaction_seq.as_u64(),
                    self.current_file_id.as_u64(),
                ));
                let obj_store_multipart = self
                    .object_store
                    .put_multipart(&path)
                    .await
                    .map_err(CompactorError::FailedPut)?;
                let writer = AsyncArrowWriter::try_new(
                    AsyncMultiPart::new(obj_store_multipart),
                    self.record_batches.schema.as_arrow(),
                    None,
                )
                .map_err(CompactorError::NewArrowWriter)?;
                self.output_paths.push(path.clone());
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
                    CompactorError::MissingColumnName(name.clone())
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
        for column_name in self.index_columns.iter() {
            let array = batch.column_by_name(column_name).unwrap();
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
            for string in downcasted.iter().map(|s| s.unwrap_or("null")) {
                let value: Arc<str> = string.into();
                self.file_index
                    .insert(Arc::clone(column_name), value, &self.current_file_id);
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

        let size_bytes = writer.bytes_written() as u64;
        let metadata = writer
            .close()
            .await
            .map_err(CompactorError::CloseArrowWriter)?;
        self.file_metadata.push(ParquetFile {
            id: self.current_file_id,
            path: self.output_paths.last().unwrap().to_string(),
            size_bytes,
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
}

impl AsyncMultiPart {
    fn new(multi_part: Box<dyn MultipartUpload>) -> Self {
        Self { multi_part }
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

impl AsyncFileWriter for AsyncMultiPart {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(AsyncMultiPartWrite {
            payload: self.multi_part.put_part(PutPayload::from_bytes(bs)),
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(AsyncMultiPartComplete {
            complete: self.multi_part.complete(),
        })
    }
}
