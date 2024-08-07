use crate::chunk::BufferChunk;
use crate::last_cache::LastCacheProvider;
use crate::paths::ParquetFilePath;
use crate::persister::PersisterImpl;
use crate::write_buffer::parquet_chunk_from_file;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::table_buffer::TableBuffer;
use crate::{ParquetFile, PersistedSnapshot, Persister};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, PartitionKey, TableId, TimestampMinMax, TransitionPartitionId,
};
use datafusion::common::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion_util::stream_from_batches;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier, WalOp, WriteBatch};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use observability_deps::tracing::{error, info};
use parking_lot::RwLock;
use parquet::format::FileMetaData;
use schema::sort::SortKey;
use schema::Schema;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

#[derive(Debug)]
pub struct QueryableBuffer {
    executor: Arc<Executor>,
    catalog: Arc<Catalog>,
    last_cache_provider: Arc<LastCacheProvider>,
    persister: Arc<PersisterImpl>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<RwLock<BufferState>>,
}

impl QueryableBuffer {
    pub(crate) fn new(
        executor: Arc<Executor>,
        catalog: Arc<Catalog>,
        persister: Arc<PersisterImpl>,
        last_cache_provider: Arc<LastCacheProvider>,
        persisted_files: Arc<PersistedFiles>,
    ) -> Self {
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(&catalog))));
        Self {
            executor,
            catalog,
            last_cache_provider,
            persister,
            persisted_files,
            buffer,
        }
    }

    pub(crate) fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let table = db_schema
            .tables
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;

        let arrow_schema: SchemaRef = match projection {
            Some(projection) => Arc::new(table.schema.as_arrow().project(projection).unwrap()),
            None => table.schema.as_arrow(),
        };

        let schema = schema::Schema::try_from(Arc::clone(&arrow_schema))
            .map_err(|e| DataFusionError::Execution(format!("schema error {}", e)))?;

        let mut chunks: Vec<Arc<dyn QueryChunk>> = vec![];

        for parquet_file in self.persisted_files.get_files(&db_schema.name, table_name) {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &schema,
                self.persister.object_store_url(),
                self.persister.object_store(),
                chunks
                    .len()
                    .try_into()
                    .expect("should never have this many chunks"),
            );

            chunks.push(Arc::new(parquet_chunk));
        }

        let buffer = self.buffer.read();

        let table_buffer = buffer
            .db_to_table
            .get(db_schema.name.as_ref())
            .ok_or_else(|| {
                DataFusionError::Execution(format!("database {} not found", db_schema.name))
            })?
            .get(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;

        let batches = table_buffer
            .record_batches(Arc::clone(&arrow_schema), filters)
            .map_err(|e| DataFusionError::Execution(format!("error getting batches {}", e)))?;

        let timestamp_min_max = table_buffer.timestamp_min_max();

        let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let chunk_stats = create_chunk_statistics(
            Some(row_count),
            &schema,
            Some(timestamp_min_max),
            &NoColumnRanges,
        );

        chunks.push(Arc::new(BufferChunk {
            batches,
            schema: schema.clone(),
            stats: Arc::new(chunk_stats),
            partition_id: TransitionPartitionId::new(
                TableId::new(0),
                &PartitionKey::from("buffer_partition"),
            ),
            sort_key: None,
            id: ChunkId::new(),
            chunk_order: ChunkOrder::new(
                chunks
                    .len()
                    .try_into()
                    .expect("should never have this many chunks"),
            ),
        }));

        Ok(chunks)
    }

    /// Called when the wal has persisted a new file. Buffer the contents in memory and update the last cache so the data is queryable.
    fn buffer_contents(&self, write: WalContents) {
        let mut buffer = self.buffer.write();
        self.last_cache_provider.evict_expired_cache_entries();
        self.last_cache_provider.write_wal_contents_to_cache(&write);

        for op in write.ops {
            match op {
                WalOp::Write(write_batch) => buffer.add_write_batch(write_batch),
                WalOp::Catalog(catalog_batch) => buffer
                    .catalog
                    .apply_catalog_batch(&catalog_batch)
                    .expect("catalog batch should apply"),
            }
        }
    }

    /// Called when the wal has written a new file and is attempting to snapshot. Kicks off persistence of
    /// data that can be snapshot in the background after putting the data in the buffer.
    async fn buffer_contents_and_persist_snapshotted_data(
        &self,
        write: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        let persist_jobs = {
            let mut buffer = self.buffer.write();

            for op in write.ops {
                match op {
                    WalOp::Write(write_batch) => buffer.add_write_batch(write_batch),
                    WalOp::Catalog(catalog_batch) => buffer
                        .catalog
                        .apply_catalog_batch(&catalog_batch)
                        .expect("catalog batch should apply"),
                }
            }

            let mut persisting_chunks = vec![];
            for (database_name, table_map) in buffer.db_to_table.iter_mut() {
                for (table_name, table_buffer) in table_map.iter_mut() {
                    let snapshot_chunks = table_buffer.snapshot(snapshot_details.end_time_marker);

                    for chunk in snapshot_chunks {
                        let persist_job = PersistJob {
                            database_name: Arc::clone(database_name),
                            table_name: Arc::clone(table_name),
                            chunk_time: chunk.chunk_time,
                            path: ParquetFilePath::new_with_chunk_time(
                                database_name.as_ref(),
                                table_name.as_ref(),
                                chunk.chunk_time,
                                write.wal_file_number,
                            ),
                            batch: chunk.record_batch,
                            schema: chunk.schema,
                            timestamp_min_max: chunk.timestamp_min_max,
                            sort_key: table_buffer.sort_key.clone(),
                        };

                        persisting_chunks.push(persist_job);
                    }
                }
            }

            persisting_chunks
        };

        let (sender, receiver) = oneshot::channel();

        let persister = Arc::clone(&self.persister);
        let executor = Arc::clone(&self.executor);
        let persisted_files = Arc::clone(&self.persisted_files);
        let wal_file_number = write.wal_file_number;
        let buffer = Arc::clone(&self.buffer);
        let catalog = Arc::clone(&self.catalog);

        tokio::spawn(async move {
            // persist the catalog
            loop {
                let catalog = catalog.clone_inner();

                match persister
                    .persist_catalog(wal_file_number, Catalog::from_inner(catalog))
                    .await
                {
                    Ok(_) => break,
                    Err(e) => {
                        error!(%e, "Error persisting catalog, sleeping and retrying...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            // persist the individual files, building the snapshot as we go
            let mut persisted_snapshot = PersistedSnapshot::new(wal_file_number);
            for persist_job in persist_jobs {
                let path = persist_job.path.to_string();
                let database_name = Arc::clone(&persist_job.database_name);
                let table_name = Arc::clone(&persist_job.table_name);
                let chunk_time = persist_job.chunk_time;
                let min_time = persist_job.timestamp_min_max.min;
                let max_time = persist_job.timestamp_min_max.max;

                let (size_bytes, meta) =
                    sort_dedupe_persist(persist_job, Arc::clone(&persister), Arc::clone(&executor))
                        .await;
                persisted_snapshot.add_parquet_file(
                    database_name,
                    table_name,
                    ParquetFile {
                        path,
                        size_bytes,
                        row_count: meta.num_rows as u64,
                        chunk_time,
                        min_time,
                        max_time,
                    },
                )
            }

            // persist the snapshot file
            loop {
                match persister.persist_snapshot(&persisted_snapshot).await {
                    Ok(_) => break,
                    Err(e) => {
                        error!(%e, "Error persisting snapshot, sleeping and retrying...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            // clear out the write buffer and add all the persisted files to the persisted files list
            let mut buffer = buffer.write();
            for (_, table_map) in buffer.db_to_table.iter_mut() {
                for (_, table_buffer) in table_map.iter_mut() {
                    table_buffer.clear_snapshots();
                }
            }

            persisted_files.add_persisted_snapshot_files(persisted_snapshot);

            let _ = sender.send(snapshot_details);
        });

        receiver
    }

    pub fn persisted_parquet_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_name, table_name)
    }
}

#[async_trait]
impl WalFileNotifier for QueryableBuffer {
    fn notify(&self, write: WalContents) {
        self.buffer_contents(write)
    }

    async fn notify_and_snapshot(
        &self,
        write: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        self.buffer_contents_and_persist_snapshotted_data(write, snapshot_details)
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
struct BufferState {
    db_to_table: HashMap<Arc<str>, TableNameToBufferMap>,
    catalog: Arc<Catalog>,
}

type TableNameToBufferMap = HashMap<Arc<str>, TableBuffer>;

impl BufferState {
    fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            db_to_table: HashMap::new(),
            catalog,
        }
    }

    fn add_write_batch(&mut self, write_batch: WriteBatch) {
        let db_schema = self
            .catalog
            .db_schema(&write_batch.database_name)
            .expect("database should exist");
        let database_buffer = self
            .db_to_table
            .entry(write_batch.database_name)
            .or_default();

        for (table_name, table_chunks) in write_batch.table_chunks {
            let table_buffer = database_buffer
                .entry_ref(table_name.as_ref())
                .or_insert_with(|| {
                    let table_schema = db_schema
                        .get_table(table_name.as_ref())
                        .expect("table should exist");
                    let sort_key = table_schema
                        .schema
                        .primary_key()
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>();
                    let index_columns = table_schema.index_columns();

                    TableBuffer::new(&index_columns, SortKey::from(sort_key))
                });
            for (chunk_time, chunk) in table_chunks.chunk_time_to_chunk {
                table_buffer.buffer_chunk(chunk_time, chunk.rows);
            }
        }
    }
}

#[derive(Debug)]
struct PersistJob {
    database_name: Arc<str>,
    table_name: Arc<str>,
    chunk_time: i64,
    path: ParquetFilePath,
    batch: RecordBatch,
    schema: Schema,
    timestamp_min_max: TimestampMinMax,
    sort_key: SortKey,
}

async fn sort_dedupe_persist<P>(
    persist_job: PersistJob,
    persister: Arc<P>,
    executor: Arc<Executor>,
) -> (u64, FileMetaData)
where
    P: Persister,
{
    // Dedupe and sort using the COMPACT query built into
    // iox_query
    let row_count = persist_job.batch.num_rows();

    let chunk_stats = create_chunk_statistics(
        Some(row_count),
        &persist_job.schema,
        Some(persist_job.timestamp_min_max),
        &NoColumnRanges,
    );

    let chunks: Vec<Arc<dyn QueryChunk>> = vec![Arc::new(BufferChunk {
        batches: vec![persist_job.batch],
        schema: persist_job.schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id: TransitionPartitionId::new(
            TableId::new(0),
            &PartitionKey::from(format!("{}", persist_job.chunk_time)),
        ),
        sort_key: Some(persist_job.sort_key.clone()),
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(1),
    })];

    let ctx = executor.new_context();

    let logical_plan = ReorgPlanner::new()
        .compact_plan(
            persist_job.table_name,
            &persist_job.schema,
            chunks,
            persist_job.sort_key,
        )
        .unwrap();

    // Build physical plan
    let physical_plan = ctx.create_physical_plan(&logical_plan).await.unwrap();

    // Execute the plan and return compacted record batches
    let data = ctx.collect(physical_plan).await.unwrap();

    // keep attempting to persist forever. If we can't reach the object store, we'll stop accepting
    // writes elsewhere in the system, so we need to keep trying to persist.
    loop {
        let batch_stream = stream_from_batches(persist_job.schema.as_arrow(), data.clone());

        match persister
            .persist_parquet_file(persist_job.path.clone(), batch_stream)
            .await
        {
            Ok((size_bytes, meta)) => {
                info!("Persisted parquet file: {}", persist_job.path.to_string());
                return (size_bytes, meta);
            }
            Err(e) => {
                error!(
                    "Error persisting parquet file {:?}, sleeping and retrying...",
                    e
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
