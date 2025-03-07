use crate::paths::ParquetFilePath;
use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::table_buffer::TableBuffer;
use crate::{ChunkFilter, ParquetFile, ParquetFileId, PersistedSnapshot};
use crate::{chunk::BufferChunk, write_buffer::table_buffer::SnapshotChunkIter};
use anyhow::Context;
use arrow::record_batch::RecordBatch;
use arrow_util::util::ensure_schema;
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
use influxdb3_wal::{
    CatalogOp, Gen1Duration, SnapshotDetails, WalContents, WalFileNotifier, WalFileSequenceNumber,
    WalOp, WriteBatch,
};
use iox_query::QueryChunk;
use iox_query::chunk_statistics::{NoColumnRanges, create_chunk_statistics};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use object_store::path::Path;
use observability_deps::tracing::{debug, error, info, trace};
use parking_lot::Mutex;
use parking_lot::RwLock;
use parquet::format::FileMetaData;
use schema::Schema as IoxSchema;
use schema::sort::SortKey;
use std::any::Any;
use std::time::Duration;
use std::{iter::Peekable, slice::Iter, sync::Arc};
use sysinfo::{MemoryRefreshKind, RefreshKind};
use tokio::sync::{
    Semaphore,
    oneshot::{self, Receiver},
};
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
    persisted_snapshot_notify_rx: tokio::sync::watch::Receiver<Option<PersistedSnapshot>>,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
    gen1_duration: Gen1Duration,
    max_size_per_parquet_file_bytes: u64,
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
    pub gen1_duration: Gen1Duration,
    pub max_size_per_parquet_file_bytes: u64,
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
            gen1_duration,
            max_size_per_parquet_file_bytes,
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
            gen1_duration,
            max_size_per_parquet_file_bytes,
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
            .map_err(|e| DataFusionError::Execution(format!("error getting batches {}", e)))?
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
    fn buffer_contents(&self, write: Arc<WalContents>) {
        self.write_wal_contents_to_caches(&write);
        let mut buffer = self.buffer.write();
        buffer.buffer_ops(
            &write.ops,
            &self.last_cache_provider,
            &self.distinct_cache_provider,
        );
    }

    /// Called when the wal has written a new file and is attempting to snapshot. Kicks off persistence of
    /// data that can be snapshot in the background after putting the data in the buffer.
    async fn buffer_contents_and_persist_snapshotted_data(
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
            buffer.buffer_ops(
                &write.ops,
                &self.last_cache_provider,
                &self.distinct_cache_provider,
            );

            let mut persisting_chunks = vec![];
            let catalog = Arc::clone(&buffer.catalog);
            for (database_id, table_map) in buffer.db_to_table.iter_mut() {
                let db_schema = catalog.db_schema_by_id(database_id).expect("db exists");
                for (table_id, table_buffer) in table_map.iter_mut() {
                    trace!(db_name = ?db_schema.name, ?table_id, ">>> working on db, table");
                    let table_def = db_schema
                        .table_definition_by_id(table_id)
                        .expect("table exists");
                    let sort_key = table_buffer.sort_key.clone();
                    let all_keys_to_remove =
                        table_buffer.get_keys_to_remove(snapshot_details.end_time_marker);
                    trace!(num_keys_to_remove = ?all_keys_to_remove.len(), ">>> num keys to remove");

                    let chunk_time_to_chunk = &mut table_buffer.chunk_time_to_chunks;
                    let snapshot_chunks = &mut table_buffer.snapshotting_chunks;
                    let snapshot_chunks_iter = SnapshotChunkIter {
                        keys_to_remove: all_keys_to_remove.iter(),
                        map: chunk_time_to_chunk,
                        table_def,
                    };

                    for chunk in snapshot_chunks_iter {
                        trace!(">>> starting with new chunk");
                        let table_name =
                            db_schema.table_id_to_name(table_id).expect("table exists");

                        let persist_job = PersistJob {
                            database_id: *database_id,
                            database_name: Arc::clone(&db_schema.name),
                            table_id: *table_id,
                            table_name: Arc::clone(&table_name),
                            chunk_time: chunk.chunk_time,
                            path: ParquetFilePath::new(
                                self.persister.node_identifier_prefix(),
                                db_schema.name.as_ref(),
                                database_id.as_u32(),
                                table_name.as_ref(),
                                table_id.as_u32(),
                                chunk.chunk_time,
                                snapshot_details.last_wal_sequence_number,
                            ),
                            // these clones are cheap and done one at a time
                            batches: vec![chunk.record_batch.clone()],
                            iox_schema: chunk.schema.clone(),
                            timestamp_min_max: chunk.timestamp_min_max,
                            sort_key: sort_key.clone(),
                        };
                        persisting_chunks.push(persist_job);
                        snapshot_chunks.push(chunk);
                        trace!(">>> finished with chunk");
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
        let notify_snapshot_tx = self.persisted_snapshot_notify_tx.clone();
        let parquet_cache = self.parquet_cache.clone();
        let gen1_duration = self.gen1_duration;
        let max_size_per_parquet_file = self.max_size_per_parquet_file_bytes;

        tokio::spawn(async move {
            // persist the catalog if it has been updated
            loop {
                if !catalog.is_updated() {
                    break;
                }
                info!(
                    "persisting catalog for wal file {}",
                    wal_file_number.as_u64()
                );
                let inner_catalog = catalog.clone_inner();
                let sequence_number = inner_catalog.sequence_number();

                match persister
                    .persist_catalog(&Catalog::from_inner(inner_catalog))
                    .await
                {
                    Ok(_) => {
                        catalog.set_updated_false_if_sequence_matches(sequence_number);
                        break;
                    }
                    Err(e) => {
                        error!(%e, "Error persisting catalog, sleeping and retrying...");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            info!(
                "persisting {} chunks for wal number {}",
                persist_jobs.len(),
                wal_file_number.as_u64(),
            );

            let persist_jobs_empty = persist_jobs.is_empty();

            let persisted_snapshot = if snapshot_details.forced {
                let mut persisted_snapshot = PersistedSnapshot::new(
                    persister.node_identifier_prefix().to_string(),
                    snapshot_details.snapshot_sequence_number,
                    snapshot_details.last_wal_sequence_number,
                    catalog.sequence_number(),
                );

                debug!(num = ?persist_jobs.len(), ">>> number of persist jobs before grouping forced");
                let iterator = PersistJobGroupedIterator::new(
                    &persist_jobs,
                    Arc::from(persister.node_identifier_prefix()),
                    wal_file_number,
                    Arc::clone(&catalog),
                    gen1_duration.as_10m() as usize,
                    Some(max_size_per_parquet_file),
                );

                sort_dedupe_serial(
                    iterator,
                    &persister,
                    executor,
                    parquet_cache,
                    buffer,
                    persisted_files,
                    &mut persisted_snapshot,
                )
                .await;
                persisted_snapshot
            } else {
                // persist the individual files, building the snapshot as we go
                debug!(num = ?persist_jobs.len(), ">>> number of persist jobs before grouping not forced");
                let persisted_snapshot = Arc::new(Mutex::new(PersistedSnapshot::new(
                    persister.node_identifier_prefix().to_string(),
                    snapshot_details.snapshot_sequence_number,
                    snapshot_details.last_wal_sequence_number,
                    catalog.sequence_number(),
                )));

                let iterator = PersistJobGroupedIterator::new(
                    &persist_jobs,
                    Arc::from(persister.node_identifier_prefix()),
                    wal_file_number,
                    Arc::clone(&catalog),
                    gen1_duration.as_10m() as usize,
                    None,
                );

                sort_dedupe_parallel(
                    iterator,
                    &persister,
                    executor,
                    parquet_cache,
                    buffer,
                    persisted_files,
                    Arc::clone(&persisted_snapshot),
                )
                .await;

                Arc::into_inner(persisted_snapshot)
                    .expect("Should only have one strong reference")
                    .into_inner()
            };

            // persist the snapshot file - only if persist jobs are present
            // if persist_jobs is empty, then parquet file wouldn't have been
            // written out, so it's desirable to not write empty snapshot file.
            //
            // How can persist jobs be empty even though snapshot is triggered?
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
            if !persist_jobs_empty {
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
    ) -> tokio::sync::watch::Receiver<Option<PersistedSnapshot>> {
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

async fn sort_dedupe_parallel<I: Iterator<Item = PersistJob>>(
    iterator: I,
    persister: &Arc<Persister>,
    executor: Arc<Executor>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    buffer: Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    persisted_snapshot: Arc<Mutex<PersistedSnapshot>>,
) {
    info!("running sort/dedupe in parallel");

    let mut set = JoinSet::new();
    let sempahore = Arc::new(Semaphore::new(5));
    for persist_job in iterator {
        let persister = Arc::clone(persister);
        let executor = Arc::clone(&executor);
        let persisted_snapshot = Arc::clone(&persisted_snapshot);
        let parquet_cache = parquet_cache.clone();
        let buffer = Arc::clone(&buffer);
        let persisted_files = Arc::clone(&persisted_files);
        let semaphore = Arc::clone(&sempahore);

        set.spawn(async move {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("to get permit to run sort/dedupe in parallel");
            let (database_id, table_id, parquet_file) = process_single_persist_job(
                persist_job,
                persister,
                executor,
                parquet_cache,
                buffer,
                persisted_files,
            )
            .await;

            persisted_snapshot
                .lock()
                .add_parquet_file(database_id, table_id, parquet_file);
            drop(permit);
        });
    }

    while let Some(res) = set.join_next().await {
        if let Err(e) = res {
            error!(?e, "error when running sort/dedupe in parallel");
        }
    }
}

async fn sort_dedupe_serial<I: Iterator<Item = PersistJob>>(
    iterator: I,
    persister: &Arc<Persister>,
    executor: Arc<Executor>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    buffer: Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    persisted_snapshot: &mut PersistedSnapshot,
) {
    info!("running sort/dedupe serially");

    for persist_job in iterator {
        let persister = Arc::clone(persister);
        let executor = Arc::clone(&executor);
        let parquet_cache = parquet_cache.clone();
        let buffer = Arc::clone(&buffer);
        let persisted_files = Arc::clone(&persisted_files);

        let (database_id, table_id, parquet_file) = process_single_persist_job(
            persist_job,
            persister,
            executor,
            parquet_cache,
            buffer,
            persisted_files,
        )
        .await;

        persisted_snapshot.add_parquet_file(database_id, table_id, parquet_file)
    }
}

async fn process_single_persist_job(
    persist_job: PersistJob,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    buffer: Arc<parking_lot::lock_api::RwLock<parking_lot::RawRwLock, BufferState>>,
    persisted_files: Arc<PersistedFiles>,
) -> (DbId, TableId, ParquetFile) {
    let path = persist_job.path.to_string();
    let database_id = persist_job.database_id;
    let table_id = persist_job.table_id;
    let chunk_time = persist_job.chunk_time;
    let min_time = persist_job.timestamp_min_max.min;
    let max_time = persist_job.timestamp_min_max.max;

    let SortDedupePersistSummary {
        file_size_bytes,
        file_meta_data,
    } = sort_dedupe_persist(persist_job, persister, executor, parquet_cache)
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
    (database_id, table_id, parquet_file)
}

#[async_trait]
impl WalFileNotifier for QueryableBuffer {
    async fn notify(&self, write: Arc<WalContents>) {
        self.buffer_contents(write)
    }

    async fn notify_and_snapshot(
        &self,
        write: Arc<WalContents>,
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

    pub fn buffer_ops(
        &mut self,
        ops: &[WalOp],
        last_cache_provider: &LastCacheProvider,
        distinct_cache_provider: &DistinctCacheProvider,
    ) {
        for op in ops {
            match op {
                WalOp::Write(write_batch) => self.add_write_batch(write_batch),
                WalOp::Catalog(catalog_batch) => {
                    let Some(catalog_batch) = self
                        .catalog
                        .apply_ordered_catalog_batch(catalog_batch)
                        .expect("should be able to reapply")
                    else {
                        continue;
                    };
                    let db_schema = self
                        .catalog
                        .db_schema_by_id(&catalog_batch.database_id)
                        .expect("database should exist");

                    // catalog changes that has external actions are applied here
                    // eg. creating or deleting last cache itself
                    for op in catalog_batch.ops {
                        match op {
                            CatalogOp::CreateDistinctCache(definition) => {
                                let table_def = db_schema
                                    .table_definition_by_id(&definition.table_id)
                                    .expect("table should exist");
                                distinct_cache_provider.create_from_definition(
                                    db_schema.id,
                                    table_def,
                                    &definition,
                                );
                            }
                            CatalogOp::DeleteDistinctCache(cache) => {
                                // this only fails if the db/table/cache do not exist, so we ignore
                                // the error if it happens.
                                let _ = distinct_cache_provider.delete_cache(
                                    &db_schema.id,
                                    &cache.table_id,
                                    &cache.cache_name,
                                );
                            }
                            CatalogOp::CreateLastCache(definition) => {
                                let table_def = db_schema
                                    .table_definition_by_id(&definition.table_id)
                                    .expect("table should exist");
                                last_cache_provider.create_cache_from_definition(
                                    db_schema.id,
                                    table_def,
                                    &definition,
                                );
                            }
                            CatalogOp::DeleteLastCache(cache) => {
                                // this only fails if the db/table/cache do not exist, so we ignore
                                // the error if it happens.
                                let _ = last_cache_provider.delete_cache(
                                    db_schema.id,
                                    cache.table_id,
                                    &cache.name,
                                );
                            }
                            CatalogOp::AddFields(_) => (),
                            CatalogOp::CreateTable(_) => (),
                            CatalogOp::CreateDatabase(_) => (),
                            CatalogOp::DeleteDatabase(db_definition) => {
                                self.db_to_table.remove(&db_definition.database_id);
                                last_cache_provider
                                    .delete_caches_for_db(&db_definition.database_id);
                                distinct_cache_provider
                                    .delete_caches_for_db(&db_definition.database_id);
                            }
                            CatalogOp::DeleteTable(table_definition) => {
                                last_cache_provider.delete_caches_for_table(
                                    &table_definition.database_id,
                                    &table_definition.table_id,
                                );
                                distinct_cache_provider.delete_caches_for_db_and_table(
                                    &table_definition.database_id,
                                    &table_definition.table_id,
                                );
                                if let Some(table_buffer_map) =
                                    self.db_to_table.get_mut(&table_definition.database_id)
                                {
                                    table_buffer_map.remove(&table_definition.table_id);
                                }
                            }
                            CatalogOp::CreateTrigger(_) => {}
                            CatalogOp::DeleteTrigger(_) => {}
                            CatalogOp::EnableTrigger(_) => {}
                            CatalogOp::DisableTrigger(_) => {}
                        }
                    }
                }
                WalOp::Noop(_) => {}
            }
        }
    }

    fn add_write_batch(&mut self, write_batch: &WriteBatch) {
        let db_schema = self
            .catalog
            .db_schema_by_id(&write_batch.database_id)
            .expect("database should exist");

        let database_buffer = self.db_to_table.entry(write_batch.database_id).or_default();

        for (table_id, table_chunks) in &write_batch.table_chunks {
            let table_buffer = database_buffer.entry(*table_id).or_insert_with(|| {
                let table_def = db_schema
                    .table_definition_by_id(table_id)
                    .expect("table should exist");
                TableBuffer::new(table_def.sort_key())
            });
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
    database_name: Arc<str>,
    table_id: TableId,
    table_name: Arc<str>,
    chunk_time: i64,
    path: ParquetFilePath,
    // when creating job per chunk, this will be just
    // a single RecordBatch, however when grouped this be
    // multiple RecordBatch'es that can be passed on to
    // ReorgPlanner (as vec of batches in BufferChunk)
    batches: Vec<RecordBatch>,
    iox_schema: IoxSchema,
    timestamp_min_max: TimestampMinMax,
    sort_key: SortKey,
}

impl PersistJob {
    fn total_batch_size(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.get_array_memory_size())
            .sum::<usize>() as u64
    }
}

/// This iterator groups persist jobs together to create a single persist job out of it with the
/// combined record batches from all of them. By default it'll try to pick as many as 10 persist
/// jobs (gen1 defaults to 1m so groups 10 of them to get to 10m) whilst maintaining memory
/// bound. There are 2 places where it's called from,
///   - when forcing snapshot due to memory pressure
///   - normal snapshot (i.e based on snapshot tracker)
///
/// In the force snapshot case, explicit memory bound is passed in (defaults to 100M), however a
/// single record batch may well exceed 100M (for 1m duration), if that happens then it will
/// naively try to do a sort/dedupe with a bigger chunk and this could very well OOM. There is no
/// dynamic behaviour to break down a bigger record batch and since this requires allocation to
/// create smaller record batches, there is a period of time where the bigger batch and the smaller
/// batches need to be in memory which has so far proven tricky.
///
/// In the normal snapshot case, unlimited memory bound is set, but it will still only put together
/// 10 persist jobs, so in this case the assumption is there is still plenty of room for the
/// memory as at the point of invocation there is no memory pressure.
struct PersistJobGroupedIterator<'a> {
    iter: Peekable<Iter<'a, PersistJob>>,
    host_prefix: Arc<str>,
    wal_file_number: WalFileSequenceNumber,
    catalog: Arc<Catalog>,
    chunk_size: usize,
    max_size_bytes: u64,
    system: sysinfo::System,
}

impl<'a> PersistJobGroupedIterator<'a> {
    fn new(
        data: &'a [PersistJob],
        host_prefix: Arc<str>,
        wal_file_number: WalFileSequenceNumber,
        catalog: Arc<Catalog>,
        chunk_size: usize,
        max_size_bytes: Option<u64>,
    ) -> Self {
        PersistJobGroupedIterator {
            iter: data.iter().peekable(),
            host_prefix: Arc::clone(&host_prefix),
            wal_file_number,
            catalog,
            chunk_size,
            max_size_bytes: max_size_bytes.unwrap_or(u64::MAX),
            system: sysinfo::System::new_with_specifics(
                RefreshKind::new().with_memory(MemoryRefreshKind::new().with_ram()),
            ),
        }
    }

    fn free_mem_hint(&mut self) -> (u64, u64) {
        self.system.refresh_memory();
        let system_mem_bytes = self.system.free_memory() - 100_000_000;
        let cgroup_free_mem_bytes = self
            .system
            .cgroup_limits()
            .map(|limit| limit.free_memory)
            .unwrap_or(u64::MAX);
        let system_mem_bytes = system_mem_bytes.min(cgroup_free_mem_bytes);
        let max_size_bytes = self.max_size_bytes.min(system_mem_bytes);
        (system_mem_bytes, max_size_bytes)
    }
}

impl Iterator for PersistJobGroupedIterator<'_> {
    type Item = PersistJob;

    fn next(&mut self) -> Option<Self::Item> {
        let current_data = self.iter.next()?;
        let current_table_id = &current_data.table_id;

        let mut ts_min_max = current_data.timestamp_min_max;

        let mut all_batches = Vec::with_capacity(self.chunk_size);
        all_batches.extend_from_slice(&current_data.batches);

        let mut min_chunk_time = current_data.chunk_time;
        let mut current_size_bytes = current_data.total_batch_size();
        debug!(?current_size_bytes, table_name = ?current_data.table_name, ">>> current_size_bytes for table");
        let (system_mem_bytes, max_size_bytes) = self.free_mem_hint();
        debug!(
            max_size_bytes,
            system_mem_bytes, ">>> max size bytes/system mem bytes"
        );

        while all_batches.len() < self.chunk_size && current_size_bytes < max_size_bytes {
            trace!(?current_size_bytes, ">>> current_size_bytes");
            if let Some(next_data) = self.iter.peek() {
                if next_data.table_id == *current_table_id
                    && (current_size_bytes + next_data.total_batch_size()) < max_size_bytes
                {
                    let next = self.iter.next().unwrap();
                    ts_min_max = ts_min_max.union(&next.timestamp_min_max);
                    min_chunk_time = min_chunk_time.min(next.chunk_time);
                    current_size_bytes += next.total_batch_size();
                    all_batches.extend_from_slice(&next.batches);
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        debug!(?current_size_bytes, ">>> final batch size in bytes");

        let table_defn = self
            .catalog
            .db_schema_by_id(&current_data.database_id)?
            .table_definition_by_id(&current_data.table_id)?;

        let arrow = table_defn.schema.as_arrow();
        let all_schema_aligned_batches: Vec<RecordBatch> = all_batches
            .iter()
            .map(|batch| ensure_schema(&arrow, batch).expect("batches should have same schema"))
            .collect();

        Some(PersistJob {
            database_id: current_data.database_id,
            database_name: Arc::clone(&current_data.database_name),
            table_id: current_data.table_id,
            path: ParquetFilePath::new(
                &self.host_prefix,
                &current_data.database_name,
                current_data.database_id.as_u32(),
                &current_data.table_name,
                current_data.table_id.as_u32(),
                min_chunk_time,
                self.wal_file_number,
            ),
            table_name: Arc::clone(&current_data.table_name),
            chunk_time: min_chunk_time,
            batches: all_schema_aligned_batches,
            iox_schema: table_defn.schema.clone(),
            timestamp_min_max: ts_min_max,
            sort_key: table_defn.sort_key(),
        })
    }
}

pub(crate) struct SortDedupePersistSummary {
    pub(crate) file_size_bytes: u64,
    pub(crate) file_meta_data: FileMetaData,
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
    let row_count = persist_job
        .batches
        .iter()
        .map(|batch| batch.num_rows())
        .sum();
    info!(
        "Persisting {} rows for db id {} and table id {} and chunk {} and ts min {} max {} to file {}",
        row_count,
        persist_job.database_id,
        persist_job.table_id,
        persist_job.chunk_time,
        persist_job.timestamp_min_max.min,
        persist_job.timestamp_min_max.max,
        persist_job.path.to_string()
    );

    let chunk_stats = create_chunk_statistics(
        Some(row_count),
        &persist_job.iox_schema,
        Some(persist_job.timestamp_min_max),
        &NoColumnRanges,
    );

    let chunks: Vec<Arc<dyn QueryChunk>> = vec![Arc::new(BufferChunk {
        batches: persist_job.batches,
        schema: persist_job.iox_schema.clone(),
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
            &persist_job.iox_schema,
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
        let batch_stream = stream_from_batches(persist_job.iox_schema.as_arrow(), data.clone());

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
    use crate::write_buffer::validator::WriteValidator;
    use datafusion_util::config::register_iox_object_store;
    use executor::{DedicatedExecutor, register_current_runtime_for_io};
    use influxdb3_wal::{Gen1Duration, NoopDetails, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::ExecutorConfig;
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use pretty_assertions::assert_eq;
    use std::num::NonZeroUsize;

    #[test_log::test(tokio::test)]
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
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let catalog = Arc::new(Catalog::new("hosta".into(), "foo".into()));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
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
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
            gen1_duration: Gen1Duration::new_1m(),
            max_size_per_parquet_file_bytes: 4_000,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let db = data_types::NamespaceName::new("testdb").unwrap();

        // create the initial write with two tags
        let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog), 0).unwrap();
        let lp = format!(
            "foo,t1=a,t2=b f1=1i {}",
            time_provider.now().timestamp_nanos()
        );

        let lines = val
            .parse_lines_and_update_schema(&lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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
        let val = WriteValidator::initialize(db, Arc::clone(&catalog), 0).unwrap();

        let lines = val
            .parse_lines_and_update_schema(lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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
    }

    #[test_log::test(tokio::test)]
    async fn test_when_snapshot_in_parallel_group_multiple_gen_1_durations() {
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
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let catalog = Arc::new(Catalog::new("hosta".into(), "foo".into()));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
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
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
            gen1_duration: Gen1Duration::new_1m(),
            max_size_per_parquet_file_bytes: 50_000,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let db = data_types::NamespaceName::new("testdb").unwrap();

        // create the initial write with one tag
        let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog), 0).unwrap();
        let lp = format!(
            "foo,t1=a,t2=b f1=1i {}",
            time_provider.now().timestamp_nanos()
        );

        let lines = val
            .parse_lines_and_update_schema(&lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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

        // write the lp into the buffer
        queryable_buffer.notify(Arc::new(wal_contents)).await;

        // create another write, this time with two tags, in a different gen1 block
        let lp = "foo,t1=a,t2=b f1=1i,f2=2 61000000000";
        let val = WriteValidator::initialize(db, Arc::clone(&catalog), 0).unwrap();

        let lines = val
            .parse_lines_and_update_schema(lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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

        let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number,
            end_time_marker: end_time,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: false,
        };

        let details = queryable_buffer
            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
            .await;
        let _details = details.await.unwrap();

        // validate we have a single persisted file
        let db = catalog.db_schema("testdb").unwrap();
        let table = db.table_definition("foo").unwrap();
        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        debug!(?files, ">>> test: queryable buffer persisted files");
        // although there were 2 writes for different gen 1 durations, they'd be written
        // together
        assert_eq!(files.len(), 1);

        let first_file = get_file(&files, ParquetFileId::from(0)).unwrap();
        assert_eq!(first_file.chunk_time, 0);
        assert_eq!(first_file.timestamp_min_max().min, 0);
        assert_eq!(first_file.timestamp_min_max().max, 61_000_000_000);
        assert_eq!(first_file.row_count, 2);
    }

    #[test_log::test(tokio::test)]
    async fn test_when_snapshot_serially_separate_gen_1_durations_are_written() {
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
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let catalog = Arc::new(Catalog::new("hosta".into(), "foo".into()));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
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
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
            gen1_duration: Gen1Duration::new_1m(),
            max_size_per_parquet_file_bytes: 2_000,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let db = data_types::NamespaceName::new("testdb").unwrap();

        // create the initial write with one tag
        let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog), 0).unwrap();
        let lp = format!(
            "foo,t1=a,t2=b f1=1i {}",
            time_provider.now().timestamp_nanos()
        );

        let lines = val
            .parse_lines_and_update_schema(&lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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

        // write the lp into the buffer
        queryable_buffer.notify(Arc::new(wal_contents)).await;

        // create another write, this time with two tags, in a different gen1 block
        let lp = "foo,t1=a,t2=b f1=1i,f2=2 61000000000";
        let val = WriteValidator::initialize(db, Arc::clone(&catalog), 0).unwrap();

        let lines = val
            .parse_lines_and_update_schema(lp, false, time_provider.now(), Precision::Nanosecond)
            .unwrap()
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

        let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number,
            end_time_marker: end_time,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: true,
        };

        let details = queryable_buffer
            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
            .await;
        let _details = details.await.unwrap();

        // validate we have a single persisted file
        let db = catalog.db_schema("testdb").unwrap();
        let table = db.table_definition("foo").unwrap();

        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        debug!(?files, ">>> test: queryable buffer persisted files");
        assert_eq!(files.len(), 2);

        let first_file = get_file(&files, ParquetFileId::from(0)).unwrap();
        let second_file = get_file(&files, ParquetFileId::from(1)).unwrap();
        assert_eq!(first_file.chunk_time, 0);
        assert_eq!(first_file.timestamp_min_max().min, 0);
        assert_eq!(first_file.timestamp_min_max().max, 0);
        assert_eq!(first_file.row_count, 1);

        assert_eq!(second_file.chunk_time, 60_000_000_000);
        assert_eq!(second_file.timestamp_min_max().min, 61_000_000_000);
        assert_eq!(second_file.timestamp_min_max().max, 61_000_000_000);
        assert_eq!(first_file.row_count, 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_snapshot_serially_two_tables_with_varying_throughput() {
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
            },
            DedicatedExecutor::new_testing(),
        ));
        let runtime_env = exec.new_context().inner().runtime_env();
        register_iox_object_store(runtime_env, parquet_store.id(), Arc::clone(&object_store));
        register_current_runtime_for_io();

        let catalog = Arc::new(Catalog::new("hosta".into(), "foo".into()));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
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
            last_cache_provider: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
            distinct_cache_provider: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap(),
            persisted_files: Arc::new(PersistedFiles::new()),
            parquet_cache: None,
            gen1_duration: Gen1Duration::new_1m(),
            max_size_per_parquet_file_bytes: 150_000,
        };
        let queryable_buffer = QueryableBuffer::new(queryable_buffer_args);

        let db = data_types::NamespaceName::new("testdb").unwrap();

        for i in 0..2 {
            // create another write, this time with two tags, in a different gen1 block
            let ts = Gen1Duration::new_1m().as_duration().as_nanos() as i64 + (i * 240_000_000_000);
            // keep these tags different to bar so that it's easier to spot the byte differences
            // in the logs, otherwise foo and bar report exact same usage in bytes
            let lp = format!("foo,t1=foo_a f1={}i,f2={} {}", i, i, ts);
            debug!(?lp, ">>> writing line");
            let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog), 0).unwrap();

            let lines = val
                .parse_lines_and_update_schema(
                    lp.as_str(),
                    false,
                    time_provider.now(),
                    Precision::Nanosecond,
                )
                .unwrap()
                .convert_lines_to_buffer(Gen1Duration::new_1m());
            let batch: WriteBatch = lines.into();
            let wal_contents = WalContents {
                persist_timestamp_ms: 0,
                min_timestamp_ns: batch.min_time_ns,
                max_timestamp_ns: batch.max_time_ns,
                wal_file_number: WalFileSequenceNumber::new((i + 10) as u64),
                ops: vec![WalOp::Write(batch)],
                snapshot: None,
            };
            queryable_buffer.notify(Arc::new(wal_contents)).await;
        }

        let mut max_timestamp_ns = None;
        for i in 0..10 {
            // create another write, this time with two tags, in a different gen1 block
            let ts = Gen1Duration::new_1m().as_duration().as_nanos() as i64 + (i * 240_000_000_000);
            let lp = format!(
                "bar,t1=br_a,t2=br_b f1=3i,f2=3 {}\nbar,t1=br_a,t2=br_c f1=4i,f2=3 {}\nbar,t1=ab,t2=bb f1=5i,f2=3 {}",
                ts, ts, ts
            );
            debug!(?lp, ">>> writing line");
            let val = WriteValidator::initialize(db.clone(), Arc::clone(&catalog), 0).unwrap();

            let lines = val
                .parse_lines_and_update_schema(
                    lp.as_str(),
                    false,
                    time_provider.now(),
                    Precision::Nanosecond,
                )
                .unwrap()
                .convert_lines_to_buffer(Gen1Duration::new_1m());
            let batch: WriteBatch = lines.into();
            max_timestamp_ns = Some(batch.max_time_ns);
            let wal_contents = WalContents {
                persist_timestamp_ms: 0,
                min_timestamp_ns: batch.min_time_ns,
                max_timestamp_ns: batch.max_time_ns,
                wal_file_number: WalFileSequenceNumber::new((i + 10) as u64),
                ops: vec![WalOp::Write(batch)],
                snapshot: None,
            };
            queryable_buffer.notify(Arc::new(wal_contents)).await;
        }

        let end_time =
            max_timestamp_ns.unwrap() + Gen1Duration::new_1m().as_duration().as_nanos() as i64;

        let snapshot_sequence_number = SnapshotSequenceNumber::new(1);
        let snapshot_details = SnapshotDetails {
            snapshot_sequence_number,
            end_time_marker: end_time,
            first_wal_sequence_number: WalFileSequenceNumber::new(0),
            last_wal_sequence_number: WalFileSequenceNumber::new(9),
            forced: true,
        };

        let wal_contents = WalContents {
            persist_timestamp_ms: 0,
            min_timestamp_ns: 0,
            max_timestamp_ns: max_timestamp_ns.unwrap(),
            wal_file_number: WalFileSequenceNumber::new(11),
            snapshot: None,
            ops: vec![WalOp::Noop(NoopDetails {
                timestamp_ns: end_time,
            })],
        };
        let details = queryable_buffer
            .notify_and_snapshot(Arc::new(wal_contents), snapshot_details)
            .await;
        let _details = details.await.unwrap();

        let db = catalog.db_schema("testdb").unwrap();
        let table = db.table_definition("foo").unwrap();

        // foo had 2 writes - should write single file when force snapshotted, with both rows in
        // them even though they are in two separate chunks
        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        debug!(?files, ">>> test: queryable buffer persisted files");
        assert_eq!(1, files.len());
        for foo_file in files {
            assert_eq!(2, foo_file.row_count);
        }

        // bar had 10 writes (each in separate chunk) with 3 lines in each write,
        // so these are grouped but because of the larger writes and max memory
        // is set to 150_000 bytes at the top, we end up with 4 files.
        let table = db.table_definition("bar").unwrap();
        let files = queryable_buffer
            .persisted_files
            .get_files(db.id, table.table_id);
        debug!(?files, ">>> test: queryable buffer persisted files");

        // Below is the growth in memory (bytes) as reported by arrow record batches
        //
        // >>> current_size_bytes for table current_size_bytes=43952 table_name="bar"
        // >>> final batch size in bytes current_size_bytes=131856
        // >>> current_size_bytes for table current_size_bytes=43952 table_name="bar"
        // >>> final batch size in bytes current_size_bytes=131856
        // >>> current_size_bytes for table current_size_bytes=43952 table_name="bar"
        // >>> final batch size in bytes current_size_bytes=131856
        // >>> current_size_bytes for table current_size_bytes=43952 table_name="bar"
        // >>> final batch size in bytes current_size_bytes=43952
        // >>> current_size_bytes for table current_size_bytes=34408 table_name="foo"
        // >>> final batch size in bytes current_size_bytes=68816
        assert_eq!(4, files.len());
        for bar_file in files {
            debug!(?bar_file, ">>> test: bar_file");
            assert!(bar_file.row_count == 3 || bar_file.row_count == 9);
        }
    }

    fn get_file(files: &[ParquetFile], parquet_file_id: ParquetFileId) -> Option<&ParquetFile> {
        files.iter().find(|file| file.id == parquet_file_id)
    }
}
