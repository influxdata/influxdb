use crate::chunk::BufferChunk;
use crate::paths::ParquetFilePath;
use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::table_buffer::TableBuffer;
use crate::write_buffer::PluginEvent;
use crate::{ParquetFile, ParquetFileId, PersistedSnapshot};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, PartitionHashId, PartitionId, PartitionKey, TimestampMinMax,
    TransitionPartitionId,
};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion_util::stream_from_batches;
use hashbrown::HashMap;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::meta_cache::MetaCacheProvider;
use influxdb3_cache::parquet_cache::{CacheRequest, ParquetCacheOracle};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{CatalogOp, SnapshotDetails, WalContents, WalFileNotifier, WalOp, WriteBatch};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::exec::Executor;
use iox_query::frontend::reorg::ReorgPlanner;
use iox_query::QueryChunk;
use object_store::path::Path;
use observability_deps::tracing::{error, info};
use parking_lot::{Mutex, RwLock};
use parquet::format::FileMetaData;
use schema::sort::SortKey;
use schema::Schema;
use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{broadcast, oneshot};

#[derive(Debug)]
pub struct QueryableBuffer {
    pub(crate) executor: Arc<Executor>,
    catalog: Arc<Catalog>,
    meta_cache_provider: Arc<MetaCacheProvider>,
    last_cache_provider: Arc<LastCacheProvider>,
    persister: Arc<Persister>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<RwLock<BufferState>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    /// Sends a notification to this watch channel whenever a snapshot info is persisted
    persisted_snapshot_notify_rx: tokio::sync::watch::Receiver<Option<PersistedSnapshot>>,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
    plugin_event_tx: Mutex<Option<broadcast::Sender<PluginEvent>>>,
}

pub struct QueryableBufferArgs {
    pub executor: Arc<Executor>,
    pub catalog: Arc<Catalog>,
    pub persister: Arc<Persister>,
    pub last_cache_provider: Arc<LastCacheProvider>,
    pub meta_cache_provider: Arc<MetaCacheProvider>,
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
            meta_cache_provider,
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
            meta_cache_provider,
            persister,
            persisted_files,
            buffer,
            parquet_cache,
            persisted_snapshot_notify_rx,
            persisted_snapshot_notify_tx,
            plugin_event_tx: Mutex::new(None),
        }
    }

    pub fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_name: &str,
        filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let (table_id, table_def) = db_schema
            .table_id_and_definition(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("table {} not found", table_name)))?;

        let influx_schema = table_def.influx_schema();

        let buffer = self.buffer.read();

        let Some(db_buffer) = buffer.db_to_table.get(&db_schema.id) else {
            return Ok(vec![]);
        };
        let Some(table_buffer) = db_buffer.get(&table_id) else {
            return Ok(vec![]);
        };

        Ok(table_buffer
            .partitioned_record_batches(Arc::clone(&table_def), filters)
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
        self.meta_cache_provider.write_wal_contents_to_cache(write);
    }

    /// Called when the wal has persisted a new file. Buffer the contents in memory and update the
    /// last cache so the data is queryable.
    fn buffer_contents(&self, write: WalContents) {
        self.write_wal_contents_to_caches(&write);
        let mut buffer = self.buffer.write();
        buffer.buffer_ops(
            write.ops,
            &self.last_cache_provider,
            &self.meta_cache_provider,
        );
    }

    /// Called when the wal has written a new file and is attempting to snapshot. Kicks off persistence of
    /// data that can be snapshot in the background after putting the data in the buffer.
    async fn buffer_contents_and_persist_snapshotted_data(
        &self,
        write: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        info!(
            ?snapshot_details,
            "Buffering contents and persisting snapshotted data"
        );
        self.write_wal_contents_to_caches(&write);
        let persist_jobs = {
            let mut buffer = self.buffer.write();

            let mut persisting_chunks = vec![];
            let catalog = Arc::clone(&buffer.catalog);
            for (database_id, table_map) in buffer.db_to_table.iter_mut() {
                let db_schema = catalog.db_schema_by_id(database_id).expect("db exists");
                for (table_id, table_buffer) in table_map.iter_mut() {
                    let table_def = db_schema
                        .table_definition_by_id(table_id)
                        .expect("table exists");
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
                                self.persister.host_identifier_prefix(),
                                db_schema.name.as_ref(),
                                database_id.as_u32(),
                                table_name.as_ref(),
                                table_id.as_u32(),
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

            // we must buffer the ops after the snapshotting as this data should not be persisted
            // with this set of wal files
            buffer.buffer_ops(
                write.ops,
                &self.last_cache_provider,
                &self.meta_cache_provider,
            );

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
            let mut persisted_snapshot = PersistedSnapshot::new(
                persister.host_identifier_prefix().to_string(),
                snapshot_details.snapshot_sequence_number,
                wal_file_number,
                catalog.sequence_number(),
            );
            let mut cache_notifiers = vec![];
            for persist_job in persist_jobs {
                let path = persist_job.path.to_string();
                let database_id = persist_job.database_id;
                let table_id = persist_job.table_id;
                let chunk_time = persist_job.chunk_time;
                let min_time = persist_job.timestamp_min_max.min;
                let max_time = persist_job.timestamp_min_max.max;

                let (size_bytes, meta, cache_notifier) = sort_dedupe_persist(
                    persist_job,
                    Arc::clone(&persister),
                    Arc::clone(&executor),
                    parquet_cache.clone(),
                )
                .await;
                cache_notifiers.push(cache_notifier);
                persisted_snapshot.add_parquet_file(
                    database_id,
                    table_id,
                    ParquetFile {
                        id: ParquetFileId::new(),
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

            // clear out the write buffer and add all the persisted files to the persisted files
            // on a background task to ensure that the cache has been populated before we clear
            // the buffer
            tokio::spawn(async move {
                // wait on the cache updates to complete if there is a cache:
                for notifier in cache_notifiers.into_iter().flatten() {
                    let _ = notifier.await;
                }
                let mut buffer = buffer.write();
                for (_, table_map) in buffer.db_to_table.iter_mut() {
                    for (_, table_buffer) in table_map.iter_mut() {
                        table_buffer.clear_snapshots();
                    }
                }

                persisted_files.add_persisted_snapshot_files(persisted_snapshot);
            });

            let _ = sender.send(snapshot_details);
        });

        receiver
    }

    pub fn persisted_parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_id, table_id)
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

    #[cfg(feature = "system-py")]
    pub(crate) fn subscribe_to_plugin_events(&self) -> broadcast::Receiver<PluginEvent> {
        let mut sender = self.plugin_event_tx.lock();

        if sender.is_none() {
            let (tx, rx) = broadcast::channel(1024);
            *sender = Some(tx);
            return rx;
        }
        sender.as_ref().unwrap().subscribe()
    }
}

#[async_trait]
impl WalFileNotifier for QueryableBuffer {
    fn notify(&self, write: WalContents) {
        if let Some(sender) = self.plugin_event_tx.lock().as_ref() {
            if let Err(err) = sender.send(PluginEvent::WriteWalContents(Arc::new(write.clone()))) {
                error!(%err, "Error sending WAL content to plugins");
            }
        }
        self.buffer_contents(write)
    }

    async fn notify_and_snapshot(
        &self,
        write: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> Receiver<SnapshotDetails> {
        if let Some(sender) = self.plugin_event_tx.lock().as_ref() {
            if let Err(err) = sender.send(PluginEvent::WriteWalContents(Arc::new(write.clone()))) {
                error!(%err, "Error sending WAL content to plugins");
            }
        }
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
        ops: Vec<WalOp>,
        last_cache_provider: &LastCacheProvider,
        meta_cache_provider: &MetaCacheProvider,
    ) {
        for op in ops {
            match op {
                WalOp::Write(write_batch) => self.add_write_batch(write_batch),
                WalOp::Catalog(catalog_batch) => {
                    self.catalog
                        // just catalog level changes
                        .apply_catalog_batch(&catalog_batch)
                        .expect("catalog batch should apply");

                    let db_schema = self
                        .catalog
                        .db_schema_by_id(&catalog_batch.database_id)
                        .expect("database should exist");

                    // catalog changes that has external actions are applied here
                    // eg. creating or deleting last cache itself
                    for op in catalog_batch.ops {
                        match op {
                            CatalogOp::CreateMetaCache(definition) => {
                                let table_def = db_schema
                                    .table_definition_by_id(&definition.table_id)
                                    .expect("table should exist");
                                meta_cache_provider.create_from_definition(
                                    db_schema.id,
                                    table_def,
                                    &definition,
                                );
                            }
                            CatalogOp::DeleteMetaCache(cache) => {
                                // this only fails if the db/table/cache do not exist, so we ignore
                                // the error if it happens.
                                let _ = meta_cache_provider.delete_cache(
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
                                meta_cache_provider
                                    .delete_caches_for_db(&db_definition.database_id);
                            }
                            CatalogOp::DeleteTable(table_definition) => {
                                last_cache_provider.delete_caches_for_table(
                                    &table_definition.database_id,
                                    &table_definition.table_id,
                                );
                                meta_cache_provider.delete_caches_for_db_and_table(
                                    &table_definition.database_id,
                                    &table_definition.table_id,
                                );
                                if let Some(table_buffer_map) =
                                    self.db_to_table.get_mut(&table_definition.database_id)
                                {
                                    table_buffer_map.remove(&table_definition.table_id);
                                }
                            }
                            CatalogOp::CreatePlugin(_) => {}
                            CatalogOp::CreateTrigger(_) => {}
                        }
                    }
                }
            }
        }
    }

    fn add_write_batch(&mut self, write_batch: WriteBatch) {
        let db_schema = self
            .catalog
            .db_schema_by_id(&write_batch.database_id)
            .expect("database should exist");

        let database_buffer = self.db_to_table.entry(write_batch.database_id).or_default();

        for (table_id, table_chunks) in write_batch.table_chunks {
            let table_buffer = database_buffer.entry(table_id).or_insert_with(|| {
                let table_def = db_schema
                    .table_definition_by_id(&table_id)
                    .expect("table should exist");
                let sort_key = table_def
                    .series_key
                    .iter()
                    .map(|c| Arc::clone(&table_def.column_id_to_name_unchecked(c)));
                let index_columns = table_def.index_column_ids();

                TableBuffer::new(index_columns, SortKey::from_columns(sort_key))
            });
            for (chunk_time, chunk) in table_chunks.chunk_time_to_chunk {
                table_buffer.buffer_chunk(chunk_time, chunk.rows);
            }
        }
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

async fn sort_dedupe_persist(
    persist_job: PersistJob,
    persister: Arc<Persister>,
    executor: Arc<Executor>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
) -> (u64, FileMetaData, Option<oneshot::Receiver<()>>) {
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
                if let Some(pq) = parquet_cache {
                    let (cache_request, cache_notify_rx) =
                        CacheRequest::create(Path::from(persist_job.path.to_string()));
                    pq.register(cache_request);
                    return (size_bytes, meta, Some(cache_notify_rx));
                } else {
                    return (size_bytes, meta, None);
                }
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
