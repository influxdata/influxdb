use crate::chunk::BufferChunk;
use crate::paths::ParquetFilePath;
use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::table_buffer::TableBuffer;
use crate::{ChunkFilter, ParquetFile, ParquetFileId, PersistedSnapshot, PersistedSnapshotVersion};
use anyhow::Context;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, PartitionHashId, PartitionId, PartitionKey, TimestampMinMax,
    TransitionPartitionId,
};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion_util::stream_from_batches;
use hashbrown::HashMap;
use influxdb3_cache::parquet_cache::{CacheRequest, ParquetCacheOracle};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{SnapshotDetails, WalContents, WalFileNotifier, WalOp, WriteBatch};
use iox_query::QueryChunk;
use iox_query::chunk_statistics::{NoColumnRanges, create_chunk_statistics};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use object_store::Error;
use object_store::path::Path;
use observability_deps::tracing::{error, info};
use parking_lot::Mutex;
use parking_lot::RwLock;
use parquet::format::FileMetaData;
use schema::Schema;
use schema::sort::SortKey;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::{self, Receiver};
use tokio::task::JoinSet;

#[derive(Debug)]
pub struct QueryableBuffer {
    pub(crate) executor: Arc<Executor>,
    catalog: Arc<Catalog>,
    distinct_cache_provider: Arc<DistinctCacheProvider>,
    last_cache_provider: Arc<LastCacheProvider>,
    persister: Arc<Persister>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<RwLock<BufferState>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    /// Sends a notification to this watch channel whenever a snapshot info is persisted
    persisted_snapshot_notify_rx: tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>>,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshotVersion>>,
}

#[derive(Debug)]
pub struct QueryableBufferArgs {
    pub executor: Arc<Executor>,
    pub catalog: Arc<Catalog>,
    pub persister: Arc<Persister>,
    pub last_cache_provider: Arc<LastCacheProvider>,
    pub distinct_cache_provider: Arc<DistinctCacheProvider>,
    pub persisted_files: Arc<PersistedFiles>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

impl QueryableBuffer {
    pub fn new(
        QueryableBufferArgs {
            executor,
            catalog,
            persister,
            last_cache_provider,
            distinct_cache_provider,
            persisted_files,
            parquet_cache,
        }: QueryableBufferArgs,
    ) -> Self {
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(&catalog))));
        let (persisted_snapshot_notify_tx, persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);
        Self {
            executor,
            catalog,
            last_cache_provider,
            distinct_cache_provider,
            persister,
            persisted_files,
            buffer,
            parquet_cache,
            persisted_snapshot_notify_rx,
            persisted_snapshot_notify_tx,
        }
    }

    pub fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        buffer_filter: &ChunkFilter<'_>,
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let influx_schema = table_def.influx_schema();

        let buffer = self.buffer.read();

        let Some(db_buffer) = buffer.db_to_table.get(&db_schema.id) else {
            return Ok(vec![]);
        };
        let Some(table_buffer) = db_buffer.get(&table_def.table_id) else {
            return Ok(vec![]);
        };

        Ok(table_buffer
            .partitioned_record_batches(Arc::clone(&table_def), buffer_filter)
            .map_err(|e| DataFusionError::Execution(format!("error getting batches {e}")))?
            .into_iter()
            .map(|(gen_time, (ts_min_max, batches))| {
                let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    influx_schema,
                    Some(ts_min_max),
                    &NoColumnRanges,
                );
                Arc::new(BufferChunk {
                    batches,
                    schema: influx_schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::from_parts(
                        PartitionId::new(0),
                        Some(PartitionHashId::new(
                            data_types::TableId::new(0),
                            &PartitionKey::from(gen_time.to_string()),
                        )),
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: ChunkOrder::new(i64::MAX),
                }) as Arc<dyn QueryChunk>
            })
            .collect())
    }

    /// Update the caches managed by the database
    fn write_wal_contents_to_caches(&self, write: &WalContents) {
        self.last_cache_provider.write_wal_contents_to_cache(write);
        self.distinct_cache_provider
            .write_wal_contents_to_cache(write);
    }

    /// Called when the wal has persisted a new file. Buffer the contents in memory and update the
    /// last cache so the data is queryable.
    fn buffer_wal_contents(&self, write: Arc<WalContents>) {
        self.write_wal_contents_to_caches(&write);
        let mut buffer = self.buffer.write();
        buffer.buffer_write_ops(&write.ops);
    }

    /// Called when the wal has written a new file and is attempting to snapshot. Kicks off persistence of
    /// data that can be snapshot in the background after putting the data in the buffer.
    async fn buffer_wal_contents_and_persist_snapshotted_data(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        info!(
            ?snapshot_details,
            "Buffering contents and persisting snapshotted data"
        );
        self.write_wal_contents_to_caches(&write);

        let persist_jobs = {
            let mut buffer = self.buffer.write();
            // need to buffer first before snapshotting
            buffer.buffer_write_ops(&write.ops);

            let mut persisting_chunks = vec![];
            let catalog = Arc::clone(&buffer.catalog);
            for (database_id, table_map) in buffer.db_to_table.iter_mut() {
                // Skip buffering deleted databases.
                if catalog.database_deletion_status(*database_id).is_some() {
                    continue;
                }
                let db_schema = catalog.db_schema_by_id(database_id).expect("db exists");
                for (table_id, table_buffer) in table_map.iter_mut() {
                    // Skip buffering deleted tables.
                    if catalog
                        .table_deletion_status(*database_id, *table_id)
                        .is_some()
                    {
                        continue;
                    }

                    let table_def = db_schema
                        .table_definition_by_id(table_id)
                        .expect("table exists");
                    let sort_key = table_def.sort_key.clone();
                    let snapshot_chunks =
                        table_buffer.snapshot(table_def, snapshot_details.end_time_marker);

                    for chunk in snapshot_chunks {
                        let table_name =
                            db_schema.table_id_to_name(table_id).expect("table exists");
                        let persist_job = PersistJob {
                            database_id: *database_id,
                            table_id: *table_id,
                            table_name: Arc::clone(&table_name),
                            chunk_time: chunk.chunk_time,
                            path: ParquetFilePath::new(
                                self.persister.node_identifier_prefix(),
                                db_schema.name.as_ref(),
                                database_id.get(),
                                table_name.as_ref(),
                                table_id.get(),
                                chunk.chunk_time,
                                snapshot_details.last_wal_sequence_number,
                            ),
                            batch: chunk.record_batch,
                            schema: chunk.schema,
                            timestamp_min_max: chunk.timestamp_min_max,
                            sort_key: sort_key.clone(),
                        };

                        persisting_chunks.push(persist_job);
                    }
                }
            }

            persisting_chunks
        };

        let removed_files = self
            .persisted_files
            .remove_files_for_deletion(Arc::clone(&self.catalog));

        for (_, tables) in &removed_files {
            for (_, files) in &tables.tables {
                for file in files {
                    let path = file.path.clone();
                    let object_store = Arc::clone(&self.persister.object_store());
                    // We've removed the file from the PersistedFiles field.
                    // We'll store them as part of the snapshot so that other parts
                    // that depend on knowing if they exist or not can update their
                    // index accordingly. We try to delete them on a best effort
                    // basis, but if they don't get deleted that's fine they aren't
                    // referenced anymore.
                    tokio::spawn(async move {
                        let mut retry_count = 0;
                        let path = path.into();
                        while retry_count <= 10 {
                            match object_store.delete(&path).await {
                                Ok(()) => break,
                                // This was already deleted so we can just skip it
                                Err(Error::NotFound { .. }) => break,
                                Err(_) => {
                                    retry_count += 1;
                                    // Sleep and increase the time with each retry.
                                    // This adds up to about 9 minutes over time.
                                    tokio::time::sleep(tokio::time::Duration::from_secs(
                                        retry_count * 10,
                                    ))
                                    .await;
                                }
                            }
                        }
                    });
                }
            }
        }

        let (sender, receiver) = oneshot::channel();

        let persister = Arc::clone(&self.persister);
        let executor = Arc::clone(&self.executor);
        let persisted_files = Arc::clone(&self.persisted_files);
        let wal_file_number = write.wal_file_number;
        let buffer = Arc::clone(&self.buffer);
        let catalog = Arc::clone(&self.catalog);
        let notify_snapshot_tx = self.persisted_snapshot_notify_tx.clone();
        let parquet_cache = self.parquet_cache.clone();

        tokio::spawn(async move {
            info!(
                "persisting {} chunks for wal number {}",
                persist_jobs.len(),
                wal_file_number.as_u64(),
            );
            let mut snapshot = PersistedSnapshot::new(
                persister.node_identifier_prefix().to_string(),
                snapshot_details.snapshot_sequence_number,
                snapshot_details.last_wal_sequence_number,
                catalog.sequence_number(),
            );

            snapshot.removed_files = removed_files;
            // persist the individual files, building the snapshot as we go
            let persisted_snapshot = Arc::new(Mutex::new(snapshot));

            let persist_jobs_empty = persist_jobs.is_empty();
            let mut set = JoinSet::new();
            for persist_job in persist_jobs {
                let persister = Arc::clone(&persister);
                let executor = Arc::clone(&executor);
                let persisted_snapshot = Arc::clone(&persisted_snapshot);
                let parquet_cache = parquet_cache.clone();
                let buffer = Arc::clone(&buffer);
                let persisted_files = Arc::clone(&persisted_files);

                set.spawn(async move {
                    let path = persist_job.path.to_string();
                    let database_id = persist_job.database_id;
                    let table_id = persist_job.table_id;
                    let chunk_time = persist_job.chunk_time;
                    let min_time = persist_job.timestamp_min_max.min;
                    let max_time = persist_job.timestamp_min_max.max;

                    let SortDedupePersistSummary {
                        file_size_bytes,
                        file_meta_data,
                    } = sort_dedupe_persist(
                        persist_job,
                        persister,
                        executor,
                        parquet_cache
                    )
                    .await
                    .inspect_err(|error| {
                        error!(
                            %error,
                            debug = ?error,
                            "error during sort, deduplicate, and persist of buffer data as parquet"
                        );
                    })
                    // for now, we are still panicking in this case, see:
                    // https://github.com/influxdata/influxdb/issues/25676
                    // https://github.com/influxdata/influxdb/issues/25677
                    .expect("sort, deduplicate, and persist buffer data as parquet");
                    let parquet_file = ParquetFile {
                        id: ParquetFileId::new(),
                        path,
                        size_bytes: file_size_bytes,
                        row_count: file_meta_data.num_rows as u64,
                        chunk_time,
                        min_time,
                        max_time,
                    };

                    {
                        // we can clear the buffer as we move on
                        let mut buffer = buffer.write();

                        // add file first
                        persisted_files.add_persisted_file(&database_id, &table_id, &parquet_file);
                        // then clear the buffer
                        if let Some(db) = buffer.db_to_table.get_mut(&database_id) {
                            if let Some(table) = db.get_mut(&table_id) {
                                table.clear_snapshots();
                            }
                        }
                    }

                    persisted_snapshot
                        .lock()
                        .add_parquet_file(database_id, table_id, parquet_file)
                });
            }

            set.join_all().await;

            // persist the snapshot file - only if persist jobs are present or
            // files have been removed due to retention policies.
            // If persist_jobs is empty, then the parquet file wouldn't have been
            // written out, so it's desirable to not write empty snapshot files.
            //
            // How can persist jobs be empty even though a snapshot is triggered?
            //
            // When force snapshot is set, wal_periods (tracked by
            // snapshot_tracker) will never be empty as a no-op is added. This
            // means even though there is a wal period the query buffer might
            // still be empty. The reason is, when snapshots are happening very
            // close to each other (when force snapshot is set), they could get
            // queued to run immediately one after the other as illustrated in
            // example series of flushes and force snapshots below,
            //
            //   1 (only wal flush) // triggered by flush interval 1s
            //   2 (snapshot)       // triggered by flush interval 1s
            //   3 (force_snapshot) // triggered by mem check interval 10s
            //   4 (force_snapshot) // triggered by mem check interval 10s
            //
            // Although the flush interval an mem check intervals aren't same
            // there's a good chance under high memory pressure there will be
            // a lot of overlapping.
            //
            // In this setup - after 2 (snapshot), we emptied wal buffer and as
            // soon as snapshot is done, 3 will try to run the snapshot but wal
            // buffer can be empty at this point, which means it adds a no-op.
            // no-op has the current time which will be used as the
            // end_time_marker. That would evict everything from query buffer, so
            // when 4 (force snapshot) runs there's no data in the query
            // buffer though it has a wal_period. When normal (i.e without
            // force_snapshot) snapshot runs, snapshot_tracker will check if
            // wal_periods are empty so it won't trigger a snapshot in the first
            // place.
            let removed_files_empty = persisted_snapshot.lock().removed_files.is_empty();
            let persisted_snapshot = PersistedSnapshotVersion::V1(
                Arc::into_inner(persisted_snapshot)
                    .expect("Should only have one strong reference")
                    .into_inner(),
            );
            if !persist_jobs_empty || !removed_files_empty {
                loop {
                    match persister.persist_snapshot(&persisted_snapshot).await {
                        Ok(_) => {
                            let persisted_snapshot = Some(persisted_snapshot.clone());
                            notify_snapshot_tx
                                .send(persisted_snapshot)
                                .expect("persisted snapshot notify tx should not be closed");
                            break;
                        }
                        Err(e) => {
                            error!(%e, "Error persisting snapshot, sleeping and retrying...");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            let _ = sender.send(snapshot_details);
        });

        receiver
    }

    pub fn persisted_parquet_files(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.persisted_files
            .get_files_filtered(db_id, table_id, filter)
    }

    pub fn persisted_snapshot_notify_rx(
        &self,
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshotVersion>> {
        self.persisted_snapshot_notify_rx.clone()
    }

    pub fn clear_buffer_for_db(&self, db_id: &DbId) {
        let mut buffer = self.buffer.write();
        buffer.db_to_table.remove(db_id);
    }

    pub fn get_total_size_bytes(&self) -> usize {
        let buffer = self.buffer.read();
        buffer.find_overall_buffer_size_bytes()
    }
}

#[async_trait]
impl WalFileNotifier for QueryableBuffer {
    async fn notify(&self, write: Arc<WalContents>) {
        self.buffer_wal_contents(write)
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        self.buffer_wal_contents_and_persist_snapshotted_data(write, snapshot_details)
            .await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct BufferState {
    pub db_to_table: HashMap<DbId, TableIdToBufferMap>,
    catalog: Arc<Catalog>,
}

type TableIdToBufferMap = HashMap<TableId, TableBuffer>;

impl BufferState {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            db_to_table: HashMap::new(),
            catalog,
        }
    }

    pub fn buffer_write_ops(&mut self, ops: &[WalOp]) {
        for op in ops {
            if let WalOp::Write(write_batch) = op {
                self.add_write_batch(write_batch);
            }
        }
    }

    fn add_write_batch(&mut self, write_batch: &WriteBatch) {
        let database_buffer = self.db_to_table.entry(write_batch.database_id).or_default();

        for (table_id, table_chunks) in &write_batch.table_chunks {
            let table_buffer = database_buffer.entry(*table_id).or_default();
            for (chunk_time, chunk) in &table_chunks.chunk_time_to_chunk {
                table_buffer.buffer_chunk(*chunk_time, &chunk.rows);
            }
        }
    }

    pub fn find_overall_buffer_size_bytes(&self) -> usize {
        let mut total = 0;
        for (_, all_tables) in &self.db_to_table {
            for (_, table_buffer) in all_tables {
                total += table_buffer.computed_size();
            }
        }
        total
    }
}

#[derive(Debug)]
struct PersistJob {
    database_id: DbId,
    table_id: TableId,
    table_name: Arc<str>,
    chunk_time: i64,
    path: ParquetFilePath,
    batch: RecordBatch,
    schema: Schema,
    timestamp_min_max: TimestampMinMax,
    sort_key: SortKey,
}

pub(crate) struct SortDedupePersistSummary {
    pub file_size_bytes: u64,
    pub file_meta_data: FileMetaData,
}

impl SortDedupePersistSummary {
    fn new(file_size_bytes: u64, file_meta_data: FileMetaData) -> Self {
        Self {
            file_size_bytes,
            file_meta_data,
        }
    }
}
async fn sort_dedupe_persist(
    persist_job: PersistJob,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
) -> Result<SortDedupePersistSummary, anyhow::Error> {
    // Dedupe and sort using the COMPACT query built into
    // iox_query
    let row_count = persist_job.batch.num_rows();
    info!(
        "Persisting {} rows for db id {} and table id {} and chunk {} to file {}",
        row_count,
        persist_job.database_id,
        persist_job.table_id,
        persist_job.chunk_time,
        persist_job.path.to_string()
    );

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
        partition_id: TransitionPartitionId::from_parts(
            PartitionId::new(0),
            Some(PartitionHashId::new(
                data_types::TableId::new(0),
                &PartitionKey::from(format!("{}", persist_job.chunk_time)),
            )),
        ),
        sort_key: Some(persist_job.sort_key.clone()),
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(1),
    })];

    let ctx = executor.new_context();

    let logical_plan = ReorgPlanner::new()
        .compact_plan(
            data_types::TableId::new(0),
            persist_job.table_name,
            &persist_job.schema,
            chunks,
            persist_job.sort_key,
        )
        .context(
            "failed to produce a logical plan to deduplicate and sort chunked data from the buffer",
        )?;

    // Build physical plan
    let physical_plan = ctx.create_physical_plan(&logical_plan).await.context(
        "failed to produce a physical plan to deduplicate and sort chunked data from the buffer",
    )?;

    // Execute the plan and return compacted record batches
    let data = ctx
        .collect(physical_plan)
        .await
        .context("failed to execute the sort and deduplication of chunked data from the buffer")?;

    // keep attempting to persist forever. If we can't reach the object store, we'll stop accepting
    // writes elsewhere in the system, so we need to keep trying to persist.
    loop {
        let batch_stream = stream_from_batches(persist_job.schema.as_arrow(), data.clone());

        match persister
            .persist_parquet_file(persist_job.path.clone(), batch_stream)
            .await
        {
            Ok((size_bytes, parquet_meta, to_cache)) => {
                info!("Persisted parquet file: {}", persist_job.path.to_string());
                if let Some(parquet_cache_oracle) = parquet_cache {
                    let cache_request = CacheRequest::create_immediate_mode_cache_request(
                        Path::from(persist_job.path.to_string()),
                        to_cache,
                    );
                    parquet_cache_oracle.register(cache_request);
                }
                return Ok(SortDedupePersistSummary::new(size_bytes, parquet_meta));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Precision;
    use crate::test_helpers::TestWriter;
    use crate::write_buffer::validator::WriteValidator;
    use datafusion_util::config::register_iox_object_store;
    use executor::{DedicatedExecutor, register_current_runtime_for_io};
    use influxdb3_catalog::resource::CatalogResource;
    use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::{ExecutorConfig, PerQueryMemoryPoolConfig};
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use parquet::arrow::arrow_reader;
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::num::NonZeroUsize;

    #[tokio::test]
    async fn snapshot_works_with_not_all_columns_in_buffer() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let metrics = Arc::new(metric::Registry::default());

        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
        let exec = Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new(
                "hosta",
                Arc::clone(&object_store),
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap(),
        );
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "hosta",
            time_provider,
        ));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

        let queryable_buffer_args = QueryableBufferArgs {
            executor: Arc::clone(&exec),
            catalog: Arc::clone(&catalog),
            persister: Arc::clone(&persister),
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let db = data_types::NamespaceName::new("testdb").unwrap();

        // create the initial write with two tags
        let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog)).unwrap();
        let lp = format!(
            "foo,t2=a,t1=b f1=1i {}",
            time_provider.now().timestamp_nanos()
        );

        let lines = val
            .v1_parse_lines_and_catalog_updates(
                &lp,
                false,
                time_provider.now(),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_1m());
        let batch: WriteBatch = lines.into();
        let wal_contents = WalContents {
            persist_timestamp_ms: 0,
            min_timestamp_ns: batch.min_time_ns,
            max_timestamp_ns: batch.max_time_ns,
            wal_file_number: WalFileSequenceNumber::new(1),
            ops: vec![WalOp::Write(batch)],
            snapshot: None,
        };
        let end_time =
            wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

        // write the lp into the buffer
        queryable_buffer.notify(Arc::new(wal_contents)).await;

        // now force a snapshot, persisting the data to parquet file. Also, buffer up a new write
        let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number,
            end_time_marker: end_time,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: false,
        };

        // create another write, this time with only one tag, in a different gen1 block
        let lp = "foo,t2=b f1=1i 240000000000";
        let val = WriteValidator::initialize(db, Arc::clone(&catalog)).unwrap();

        let lines = val
            .v1_parse_lines_and_catalog_updates(
                lp,
                false,
                time_provider.now(),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .unwrap()
            .unwrap_success()
            .convert_lines_to_buffer(Gen1Duration::new_1m());
        let batch: WriteBatch = lines.into();
        let wal_contents = WalContents {
            persist_timestamp_ms: 0,
            min_timestamp_ns: batch.min_time_ns,
            max_timestamp_ns: batch.max_time_ns,
            wal_file_number: WalFileSequenceNumber::new(2),
            ops: vec![WalOp::Write(batch)],
            snapshot: None,
        };
        let end_time =
            wal_contents.max_timestamp_ns + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

        let details = queryable_buffer
            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
            .await;
        let _details = details.await.unwrap();

        // validate we have a single persisted file
        let db = catalog.db_schema("testdb").unwrap();
        let table = db.table_definition("foo").unwrap();
        assert_eq!(
            table.sort_key,
            SortKey::from_columns(vec!["t2", "t1", "time"])
        );
        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        assert_eq!(files.len(), 1);

        // now force another snapshot, persisting the data to parquet file
        let snapshot_sequence_number = SnapshotSequenceNumber::new(2);
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number,
            end_time_marker: end_time,
            first_wal_sequence_number: WalFileSequenceNumber::new(3),
            last_wal_sequence_number: WalFileSequenceNumber::new(3),
            forced: false,
        };
        queryable_buffer
            .notify_and_snapshot(
                Arc::new(WalContents {
                    persist_timestamp_ms: 0,
                    min_timestamp_ns: 0,
                    max_timestamp_ns: 0,
                    wal_file_number: WalFileSequenceNumber::new(3),
                    ops: vec![],
                    snapshot: Some(snapshot_details),
                }),
                snapshot_details,
            )
            .await
            .await
            .unwrap();

        // validate we have two persisted files
        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        assert_eq!(files.len(), 2);

        // Verify the `iox::series::key` metadata is present in the parquet file
        {
            let path = Path::parse(&files[0].path).expect("path should be parseable");
            let res = object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let metadata =
                arrow_reader::ArrowReaderMetadata::load(&res, ArrowReaderOptions::new()).unwrap();
            let schema: Schema = Schema::try_from(Arc::clone(metadata.schema())).unwrap();
            let primary_key = schema.primary_key();
            assert_eq!(primary_key, &["t2", "t1", "time"]);
        }
    }

    /// This test validates that buffer replay ignores data from deleted tables.
    #[tokio::test]
    async fn snapshot_skips_deleted_table() {
        // Setup test infrastructure
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let metrics = Arc::new(metric::Registry::default());
        let parquet_store =
            ParquetStorage::new(Arc::clone(&object_store), StorageId::from("influxdb3"));
        let exec = Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                mem_pool_size: 1024 * 1024 * 1024,
                per_query_mem_pool_config: PerQueryMemoryPoolConfig::Disabled,
                heap_memory_limit: None,
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(
            Catalog::new(
                "hosta",
                Arc::clone(&object_store),
                Arc::clone(&time_provider) as _,
                Default::default(),
            )
            .await
            .unwrap(),
        );

        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            "hosta",
            Arc::clone(&time_provider) as _,
        ));

        let queryable_buffer_args = QueryableBufferArgs {
            executor: Arc::clone(&exec),
            catalog: Arc::clone(&catalog),
            persister: Arc::clone(&persister),
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider) as _,
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let writer = TestWriter::new_with_catalog(Arc::clone(&catalog));
        let lines1 = writer
            .write_lp_to_write_batch("table1,tag=a value=1i 1000", 0)
            .await;
        let lines2 = writer
            .write_lp_to_write_batch("table2,tag=b value=2i 2000", 0)
            .await;

        let db_schema = catalog.db_schema(TestWriter::DB_NAME).unwrap();
        let db_id = db_schema.id();
        let table1_id = db_schema.table_name_to_id("table1").unwrap();
        let table2_id = db_schema.table_name_to_id("table2").unwrap();

        // Soft delete the second table
        use influxdb3_catalog::catalog::HardDeletionTime;
        catalog
            .soft_delete_table(TestWriter::DB_NAME, "table2", HardDeletionTime::Now)
            .await
            .unwrap();

        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 1000,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: false,
        };

        let wal_contents = influxdb3_wal::create::wal_contents_with_snapshot(
            (0, 100, 1),
            [
                influxdb3_wal::create::write_batch_op(lines1),
                influxdb3_wal::create::write_batch_op(lines2),
            ],
            snapshot_details,
        );

        queryable_buffer
            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
            .await
            .await
            .unwrap();

        // Verify only table1 has persisted files
        let files1 = queryable_buffer.persisted_files.get_files(db_id, table1_id);
        assert_eq!(files1.len(), 1, "Should have 1 persisted file for table1");

        let files2 = queryable_buffer.persisted_files.get_files(db_id, table2_id);
        assert_eq!(
            files2.len(),
            0,
            "Soft deleted table should not have persisted files"
        );
    }
}
