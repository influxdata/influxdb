use arrow::array::RecordBatch;
use arrow::compute::partition;
use arrow::compute::Partitions;
use arrow_schema::ArrowError;
use arrow_util::util::ensure_schema;
use bytes::Bytes;
use data_types::ChunkId;
use data_types::ChunkOrder;
use data_types::PartitionHashId;
use data_types::PartitionKey;
use data_types::TableId;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures_util::future::BoxFuture;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::TIME_COLUMN_NAME;
use influxdb3_write::chunk::ParquetChunk;
use influxdb3_write::persister::Persister;
use influxdb3_write::write_buffer::WriteBufferImpl;
use iox_query::chunk_statistics::create_chunk_statistics;
use iox_query::chunk_statistics::NoColumnRanges;
use iox_query::exec::Executor;
use iox_query::frontend::reorg;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use iox_time::TimeProvider;
use object_store::path::Path as ObjPath;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::PutResult;
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
use tokio::time::sleep;
use tokio::time::Duration;
use uuid::Uuid;

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
}

#[derive(Debug)]
pub struct Compactor<T> {
    write_buffer: Arc<WriteBufferImpl<T>>,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
}

impl<T: TimeProvider> Compactor<T> {
    pub fn new(
        write_buffer: Arc<WriteBufferImpl<T>>,
        persister: Arc<Persister>,
        executor: Arc<Executor>,
    ) -> Self {
        Self {
            write_buffer,
            persister,
            executor,
        }
    }

    pub async fn compact(self) {
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }

    /// Compact `paths` together into one or more parquet files
    ///
    /// The limit is the maximum number of rows that can be in a single file.
    /// This number can be exceed if a single series is larger than the limit
    pub async fn compact_files(
        &self,
        database_name: &str,
        table_name: &str,
        mut sort_keys: Vec<String>,
        paths: Vec<ObjPath>,
        limit: usize,
    ) -> Result<Vec<ObjPath>, CompactorError> {
        executor::register_current_runtime_for_io();

        let db_schema = self
            .write_buffer
            .catalog()
            .db_schema(database_name)
            .ok_or_else(|| CompactorError::MissingDB)?;

        let table_schema = db_schema
            .get_table_schema(table_name)
            .ok_or_else(|| CompactorError::MissingSchema)?;

        let records = self
            .record_stream(table_name, &mut sort_keys, paths, table_schema)
            .await?;

        let mut series_writer = SeriesWriter::new(
            Arc::new(table_schema.clone()),
            self.persister.object_store(),
            limit,
            sort_keys.clone(),
            records,
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

    /// Get a stream of `RecordBatch` to process after being compacted and deduped for that table
    async fn record_stream(
        &self,
        table_name: &str,
        sort_keys: &mut Vec<String>,
        paths: Vec<ObjPath>,
        table_schema: &Schema,
    ) -> Result<SendableRecordBatchStream, CompactorError> {
        let time = TIME_COLUMN_NAME.into();
        if !sort_keys.contains(&time) {
            sort_keys.push(time);
        }

        let sort_key = SortKey::from_columns(sort_keys.iter().map(String::as_str));

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
            let meta = self
                .persister
                .object_store()
                .get(location)
                .await
                .map_err(CompactorError::FailedGet)?
                .meta;
            let parquet_exec = ParquetExecInput {
                object_store_url: self.persister.object_store_url().clone(),
                object_meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: meta.last_modified,
                    size: meta.size,
                    e_tag: meta.e_tag,
                    version: meta.version,
                },
                object_store: self.persister.object_store(),
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
            .compact_plan(table_name.into(), table_schema, chunks, sort_key)
            .map_err(CompactorError::ReorgError)?;
        self.executor
            .new_context()
            .inner()
            .execute_logical_plan(plan)
            .await
            .map_err(CompactorError::FailedLogicalPlan)?
            .execute_stream()
            .await
            .map_err(CompactorError::RecordStream)
    }
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
    /// Tags to sort all of the record batches by
    sort_keys: Vec<String>,
    /// files we have created
    output_paths: Vec<ObjPath>,
}

impl SeriesWriter {
    /// Create a new `SeriesWriter` which maintains all of the state for writing out series for
    /// compactions
    fn new(
        table_schema: Arc<Schema>,
        object_store: Arc<dyn ObjectStore>,
        limit: usize,
        sort_keys: Vec<String>,
        stream: SendableRecordBatchStream,
    ) -> Self {
        Self {
            record_batches: RecordBatchHolder::new(stream, Arc::clone(&table_schema)),
            object_store,
            limit,
            last_batch: None,
            writer: None,
            sort_keys,
            output_paths: Vec::new(),
        }
    }

    /// Close out any leftover writers and return the paths
    async fn finish(mut self) -> Result<Vec<ObjPath>, CompactorError> {
        // close the remaining writer, if any
        if let Some(writer) = self.writer.take() {
            writer
                .close()
                .await
                .map_err(CompactorError::CloseArrowWriter)?;
        }

        // This should never happen, but if it is we should throw an error
        if self.output_paths.is_empty() {
            Err(CompactorError::NoCompactedFiles)
        } else {
            Ok(self.output_paths)
        }
    }

    /// Push a new batch of data into the writer
    async fn push_batch(&mut self, batch: RecordBatch) -> Result<(), CompactorError> {
        let mut writer = self.get_writer().await?;

        // if there is space remaining within the limit for this writer, write the whole batch.
        if writer.in_progress_rows() + batch.num_rows() <= self.limit {
            writer
                .write(&batch)
                .await
                .map_err(CompactorError::WriteArrowWriter)?;
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
                .sort_keys
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
                    writer
                        .close()
                        .await
                        .map_err(CompactorError::CloseArrowWriter)?;
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

                self.last_batch = Some(slice);

                let batch_slice = batch.slice(len, batch.num_rows() - len);
                if batch_slice.num_rows() > 0 {
                    self.set_leftover_batch(batch_slice);
                }

                if writer.in_progress_rows() + next_len > self.limit {
                    writer
                        .close()
                        .await
                        .map_err(CompactorError::CloseArrowWriter)?;
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
                let path = ObjPath::from(Uuid::new_v4().to_string());
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
                Ok(writer)
            }
        }
    }

    /// Find where the series are in the record batch
    fn partition_by_series(&self, batch: &RecordBatch) -> Result<Partitions, CompactorError> {
        assert_eq!(self.sort_keys.last(), Some(&TIME_COLUMN_NAME.into()));

        // We want to partition on everything except time
        let columns = self
            .sort_keys
            .iter()
            .filter(|name| name != &TIME_COLUMN_NAME)
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
