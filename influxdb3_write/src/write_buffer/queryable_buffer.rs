use crate::{chunk::BufferChunk, write_buffer::table_buffer::SnaphotChunkIter};
use crate::paths::ParquetFilePath;
use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::table_buffer::TableBuffer;
use crate::{ChunkFilter, ParquetFile, ParquetFileId, PersistedSnapshot};
use anyhow::Context;
use arrow::{
    array::{AsArray, UInt64Array},
    compute::take,
    datatypes::TimestampNanosecondType,
    record_batch::RecordBatch,
};
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
use influxdb3_wal::{CatalogOp, SnapshotDetails, WalContents, WalFileNotifier, WalOp, WriteBatch};
use iox_query::QueryChunk;
use iox_query::chunk_statistics::{NoColumnRanges, create_chunk_statistics};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use object_store::path::Path;
use observability_deps::tracing::{debug, error, info};
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
    persisted_snapshot_notify_rx: tokio::sync::watch::Receiver<Option<PersistedSnapshot>>,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
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
                    info!(db_name = ?db_schema.name, ?table_id, ">>> working on db, table");
                    let table_def = db_schema
                        .table_definition_by_id(table_id)
                        .expect("table exists");
                    let sort_key = table_buffer.sort_key.clone();
                    let all_keys_to_remove = table_buffer.get_keys_to_remove(snapshot_details.end_time_marker);
                    info!(num_keys_to_remove = ?all_keys_to_remove.len(), ">>> num keys to remove");

                    let chunk_time_to_chunk = &mut table_buffer.chunk_time_to_chunks;
                    let snapshot_chunks = &mut table_buffer.snapshotting_chunks;
                    let snapshot_chunks_iter = SnaphotChunkIter {
                        keys_to_remove: all_keys_to_remove.iter(),
                        map: chunk_time_to_chunk,
                        table_def,
                    };

                    for chunk in snapshot_chunks_iter {
                        debug!(">>> starting with new chunk");
                        let table_name =
                            db_schema.table_id_to_name(table_id).expect("table exists");

                        // TODO: just for experimentation we want to force all snapshots to go
                        // through without breaking down the record batches
                        if !snapshot_details.forced {
                            // when forced, we're already under memory pressure so create smaller
                            // chunks (by time) and they need to be non-overlapping.
                            // 1. Create smaller groups (using smaller duration), 10 secs here
                            let mut smaller_chunks: BTreeMap<i64, (MinMax, Vec<u64>)> = BTreeMap::new();
                            let smaller_duration = Duration::from_secs(10).as_nanos() as i64;
                            let all_times = chunk
                                .record_batch
                                .column_by_name("time")
                                .expect("time col to be present")
                                .as_primitive::<TimestampNanosecondType>()
                                .values();

                            for (idx, time) in all_times.iter().enumerate() {
                                let smaller_chunk_time = time - (time % smaller_duration);
                                let (min_max, vec_indices) = smaller_chunks
                                    .entry(smaller_chunk_time)
                                    .or_insert_with(|| (MinMax::new(i64::MAX, i64::MIN), Vec::new()));

                                min_max.update(*time);
                                vec_indices.push(idx as u64);
                            }

                            let total_row_count = chunk.record_batch.column(0).len();

                            for (smaller_chunk_time, (min_max, all_indexes)) in smaller_chunks.iter() {
                                debug!(
                                    ?smaller_chunk_time,
                                    ?min_max,
                                    num_indexes = ?all_indexes.len(),
                                    ?total_row_count,
                                    ">>> number of small chunks");
                            }

                            // 2. At this point we have a bucket for each 10 sec block with related
                            //    indexes from main chunk. Use those indexes to "cheaply" create
                            //    smaller record batches.
                            let batch_schema = chunk.record_batch.schema();
                            let parent_cols = chunk.record_batch.columns();

                            for (loop_idx, (smaller_chunk_time, (min_max, all_indexes))) in
                                smaller_chunks.into_iter().enumerate()
                            {
                                let mut smaller_chunk_cols = vec![];
                                let indices = UInt64Array::from_iter(all_indexes);
                                for arr in parent_cols {
                                    // `take` here minimises allocations but is not completely free,
                                    // it still needs to allocate for smaller batches. The
                                    // allocations are in `ScalarBuffer::from_iter` under the hood
                                    let filtered =
                                        take(&arr, &indices, None)
                                            .expect("index should be accessible in parent cols");

                                    smaller_chunk_cols.push(filtered);
                                }
                                let smaller_rec_batch =
                                    RecordBatch::try_new(Arc::clone(&batch_schema), smaller_chunk_cols)
                                        .expect("create smaller record batch");
                                let persist_job = PersistJob {
                                    database_id: *database_id,
                                    table_id: *table_id,
                                    table_name: Arc::clone(&table_name),
                                    chunk_time: smaller_chunk_time,
                                    path: ParquetFilePath::new(
                                        self.persister.node_identifier_prefix(),
                                        db_schema.name.as_ref(),
                                        database_id.as_u32(),
                                        table_name.as_ref(),
                                        table_id.as_u32(),
                                        smaller_chunk_time,
                                        snapshot_details.last_wal_sequence_number,
                                        Some(loop_idx as u64),
                                    ),
                                    batch: smaller_rec_batch,
                                    schema: chunk.schema.clone(),
                                    timestamp_min_max: min_max.to_ts_min_max(),
                                    sort_key: sort_key.clone(),
                                };
                                persisting_chunks.push(persist_job);
                            }

                        } else {
                            let persist_job = PersistJob {
                                database_id: *database_id,
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
                                    None,
                                ),
                                // these clones are cheap and done one at a time
                                batch: chunk.record_batch.clone(),
                                schema: chunk.schema.clone(),
                                timestamp_min_max: chunk.timestamp_min_max,
                                sort_key: sort_key.clone(),
                            };
                            persisting_chunks.push(persist_job);
                        }
                        snapshot_chunks.push_back(chunk);
                        // snapshot_chunks.add_one(chunk);
                        debug!(">>> finished with chunk");
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
            // persist the individual files, building the snapshot as we go
            // let persisted_snapshot = Arc::new(Mutex::new(PersistedSnapshot::new(
            //     persister.node_identifier_prefix().to_string(),
            //     snapshot_details.snapshot_sequence_number,
            //     snapshot_details.last_wal_sequence_number,
            //     catalog.sequence_number(),
            // )));
            //
            let mut persisted_snapshot = PersistedSnapshot::new(
                persister.node_identifier_prefix().to_string(),
                snapshot_details.snapshot_sequence_number,
                snapshot_details.last_wal_sequence_number,
                catalog.sequence_number(),
            );

            let persist_jobs_empty = persist_jobs.is_empty();
            // let mut set = JoinSet::new();
            for persist_job in persist_jobs {
                let persister = Arc::clone(&persister);
                let executor = Arc::clone(&executor);
                // let persisted_snapshot = Arc::clone(&persisted_snapshot);
                let parquet_cache = parquet_cache.clone();
                let buffer = Arc::clone(&buffer);
                let persisted_files = Arc::clone(&persisted_files);

                // set.spawn(async move {
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
                        // .lock()
                        .add_parquet_file(database_id, table_id, parquet_file)
                // });
            }

            // set.join_all().await;

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

            // let persisted_snapshot = Arc::into_inner(persisted_snapshot)
            //     .expect("Should only have one strong reference")
            //     .into_inner();

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

#[derive(Debug)]
struct MinMax {
    min: i64,
    max: i64,
}

impl MinMax {
    fn new(min: i64, max: i64) -> Self {
        // this doesn't check if min < max, a lot of the times
        // it's good to start with i64::MAX for min and i64::MIN
        // for max in loops so this type unlike TimestampMinMax
        // doesn't check this pre-condition
        Self { min, max }
    }

    fn update(&mut self, other: i64) {
        self.min = other.min(self.min);
        self.max = other.max(self.max);
    }

    fn to_ts_min_max(&self) -> TimestampMinMax {
        // at this point min < max
        TimestampMinMax::new(self.min, self.max)
    }
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
                let sort_key = table_def
                    .series_key
                    .iter()
                    .map(|c| Arc::clone(&table_def.column_id_to_name_unchecked(c)));

                TableBuffer::new(SortKey::from_columns(sort_key))
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

    // TODO: this is a good place to use multiple batches
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
    use crate::write_buffer::validator::WriteValidator;
    use datafusion_util::config::register_iox_object_store;
    use executor::{DedicatedExecutor, register_current_runtime_for_io};
    use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::ExecutorConfig;
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
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
}
