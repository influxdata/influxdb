use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::Context;
use data_types::{
    ChunkId, ChunkOrder, PartitionHashId, PartitionId, PartitionKey, TableId as IoxTableId,
    TransitionPartitionId,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use futures::future::try_join_all;
use futures_util::StreamExt;
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider,
    last_cache::LastCacheProvider,
    parquet_cache::{CacheRequest, ParquetCacheOracle},
};
use influxdb3_catalog::{
    catalog::{Catalog, CatalogSequenceNumber, DatabaseSchema, NodeDefinition, TableDefinition},
    log::NodeModes,
    resource::CatalogResource,
};
use influxdb3_enterprise_data_layout::NodeSnapshotMarker;
use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_wal::{
    SnapshotDetails, WalContents, WalFileSequenceNumber, WalOp, object_store::wal_path,
    serialize::verify_file_type_and_deserialize,
};
use influxdb3_write::{
    ChunkFilter, ParquetFile, PersistedSnapshot, PersistedSnapshotVersion,
    chunk::BufferChunk,
    paths::SnapshotInfoFilePath,
    persister::{DEFAULT_OBJECT_STORE_URL, Persister},
    write_buffer::{
        N_SNAPSHOTS_TO_LOAD_ON_START, cache_parquet_files, parquet_chunk_from_file,
        persisted_files::PersistedFiles, queryable_buffer::BufferState,
    },
};
use iox_query::{
    QueryChunk,
    chunk_statistics::{NoColumnRanges, create_chunk_statistics},
};
use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, Registry};
use object_store::{ObjectStore, path::Path};
use observability_deps::tracing::{debug, error, info, warn};
use parking_lot::RwLock;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("unexpected replication error: {0:#}")]
    Unexpected(#[from] anyhow::Error),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug)]
pub struct ReplicationConfig {
    pub interval: Duration,
}

impl ReplicationConfig {
    pub fn new(interval: Duration) -> Self {
        Self { interval }
    }
}

#[derive(Debug)]
pub(crate) struct Replicas {
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    replicated_buffers: Vec<Arc<ReplicatedBuffer>>,
}

pub(crate) struct CreateReplicasArgs {
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub metric_registry: Arc<Registry>,
    pub replication_interval: Duration,
    // NOTE(trevor/catalog-refactor): this needs to go, nodes can just be pulled from the catalog
    // once there is a mechanism for handling the broadcast of a RegisterNodeLog through the
    // catalog.
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub catalog: Arc<Catalog>,
    pub time_provider: Arc<dyn TimeProvider>,
}

impl Replicas {
    pub(crate) async fn new(
        CreateReplicasArgs {
            last_cache,
            distinct_cache,
            object_store,
            metric_registry,
            replication_interval,
            parquet_cache,
            catalog,
            time_provider,
        }: CreateReplicasArgs,
    ) -> Result<Self> {
        let mut replicated_buffers = vec![];
        let nodes: Vec<Arc<NodeDefinition>> = catalog
            .list_nodes()
            .iter()
            .filter(|nd| {
                // do not replicate the current node, i.e., itself
                let current_node = catalog.current_node().expect("current node should be set");
                // only replicate nodes that have `ingest`
                let node_modes = NodeModes::from(nd.as_ref().modes().clone());
                current_node.id() != nd.id() && node_modes.is_ingester()
            })
            .cloned()
            .collect();
        for (i, node_def) in nodes.into_iter().enumerate() {
            let object_store = Arc::clone(&object_store);
            let last_cache = Arc::clone(&last_cache);
            let distinct_cache = Arc::clone(&distinct_cache);
            let metric_registry = Arc::clone(&metric_registry);
            let parquet_cache = parquet_cache.clone();
            let catalog = Arc::clone(&catalog);
            let time_provider = Arc::clone(&time_provider);
            info!(
                node_id = node_def.name().as_ref(),
                "creating replicated buffer for node"
            );
            replicated_buffers.push(
                ReplicatedBuffer::new(CreateReplicatedBufferArgs {
                    replica_order: i as i64,
                    object_store,
                    node_def,
                    last_cache,
                    distinct_cache,
                    replication_interval,
                    metric_registry,
                    parquet_cache,
                    catalog,
                    time_provider,
                })
                .await?,
            )
        }
        Ok(Self {
            last_cache,
            distinct_cache,
            replicated_buffers,
        })
    }

    pub(crate) fn last_cache(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }

    pub(crate) fn distinct_cache(&self) -> Arc<DistinctCacheProvider> {
        Arc::clone(&self.distinct_cache)
    }

    #[cfg(test)]
    pub(crate) fn parquet_files(&self, db_id: DbId, tbl_id: TableId) -> Vec<ParquetFile> {
        self.parquet_files_filtered(db_id, tbl_id, &ChunkFilter::default())
    }

    pub(crate) fn parquet_files_filtered(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let mut files = vec![];
        for replica in &self.replicated_buffers {
            files.append(&mut replica.parquet_files_filtered(db_id, tbl_id, filter));
        }
        files
    }

    #[cfg(test)]
    fn get_all_chunks(&self, database_name: &str, table_name: &str) -> Vec<Arc<dyn QueryChunk>> {
        let db_schema = self.catalog().db_schema(database_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        let filter = ChunkFilter::default();
        let mut chunks = self
            .get_buffer_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), &filter)
            .unwrap();
        chunks.extend(self.get_persisted_chunks(db_schema, table_def, &filter, &[], 0));
        chunks
    }

    pub(crate) fn get_buffer_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let mut chunks = vec![];
        for replica in &self.replicated_buffers {
            chunks.append(&mut replica.get_buffer_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
            )?);
        }
        Ok(chunks)
    }

    pub(crate) fn get_persisted_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        writer_markers: &[Arc<NodeSnapshotMarker>],
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut chunks = vec![];
        for replica in &self.replicated_buffers {
            let last_parquet_file_id = writer_markers.iter().find_map(|marker| {
                if marker.node_id == replica.node_def.name().as_ref() {
                    Some(marker.next_file_id)
                } else {
                    None
                }
            });

            chunks.append(&mut replica.get_persisted_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
                last_parquet_file_id,
                chunk_order_offset,
            ));
            chunk_order_offset += chunks.len() as i64;
        }
        chunks
    }

    #[cfg(test)]
    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        self.replicated_buffers[0].catalog()
    }
}

#[derive(Debug)]
pub(crate) struct ReplicatedBuffer {
    replica_order: i64,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    node_def: Arc<NodeDefinition>,
    buffer: Arc<RwLock<BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    catalog: Arc<Catalog>,
    metrics: ReplicatedBufferMetrics,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    time_provider: Arc<dyn TimeProvider>,
}

pub const REPLICA_TTBR_METRIC: &str = "influxdb3_replica_ttbr_duration";

#[derive(Debug)]
struct ReplicatedBufferMetrics {
    replica_ttbr: DurationHistogram,
}

pub(crate) struct CreateReplicatedBufferArgs {
    replica_order: i64,
    object_store: Arc<dyn ObjectStore>,
    node_def: Arc<NodeDefinition>,
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    replication_interval: Duration,
    metric_registry: Arc<Registry>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
}

impl ReplicatedBuffer {
    pub(crate) async fn new(
        CreateReplicatedBufferArgs {
            replica_order,
            object_store,
            node_def,
            last_cache,
            distinct_cache,
            replication_interval,
            metric_registry,
            parquet_cache,
            catalog,
            time_provider,
        }: CreateReplicatedBufferArgs,
    ) -> Result<Arc<Self>> {
        let persisted_snapshots = get_persisted_snapshots_for_writer(
            Arc::clone(&object_store),
            &node_def.name(),
            Arc::clone(&time_provider),
        )
        .await?;
        let node_id: Cow<'static, str> = Cow::from(node_def.name().as_ref().to_owned());
        let attributes = Attributes::from([("from_node_id", node_id)]);
        let replica_ttbr = metric_registry
            .register_metric::<DurationHistogram>(
                REPLICA_TTBR_METRIC,
                "time to be readable for the data in each replicated write buffer",
            )
            .recorder(attributes);
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(&catalog))));
        let last_snapshotted_wal_file_sequence_number = persisted_snapshots
            .first()
            .map(|snapshot| snapshot.wal_file_sequence_number);
        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
            persisted_snapshots,
        ));
        let replicated_buffer = Self {
            replica_order,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            node_def,
            buffer,
            persisted_files,
            last_cache,
            distinct_cache,
            metrics: ReplicatedBufferMetrics { replica_ttbr },
            parquet_cache,
            catalog,
            time_provider,
        };
        let last_wal_file_sequence_number = replicated_buffer
            .replay(last_snapshotted_wal_file_sequence_number)
            .await?;
        let replicated_buffer = Arc::new(replicated_buffer);
        background_replication_interval(
            Arc::clone(&replicated_buffer),
            last_wal_file_sequence_number,
            replication_interval,
        );

        Ok(replicated_buffer)
    }

    fn node_id(&self) -> Arc<str> {
        self.node_def.name()
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    #[cfg(test)]
    pub(crate) fn parquet_files(&self, db_id: DbId, tbl_id: TableId) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_id, tbl_id)
    }

    pub(crate) fn parquet_files_filtered(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.persisted_files
            .get_files_filtered(db_id, tbl_id, filter)
    }

    pub(crate) fn get_buffer_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        self.get_buffer_table_chunks(db_schema, table_def, filter)
    }

    pub(crate) fn get_persisted_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        last_compacted_parquet_file_id: Option<ParquetFileId>, // only return chunks with a file id > than this
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut files =
            self.persisted_files
                .get_files_filtered(db_schema.id, table_def.table_id, filter);

        // filter out any files that have been compacted
        if let Some(last_parquet_file_id) = last_compacted_parquet_file_id {
            files.retain(|f| f.id > last_parquet_file_id);
        }

        // cache any parquet files that match cache policy
        cache_parquet_files(self.parquet_cache.clone(), &files);

        files
            .into_iter()
            .map(|parquet_file| {
                chunk_order_offset += 1;

                let parquet_chunk = parquet_chunk_from_file(
                    &parquet_file,
                    &table_def.schema,
                    self.object_store_url.clone(),
                    Arc::clone(&self.object_store),
                    chunk_order_offset,
                );

                Arc::new(parquet_chunk) as Arc<dyn QueryChunk>
            })
            .collect()
    }

    /// Get chunks from the in-memory buffer for a given database and table, along with the
    /// given filters.
    ///
    /// This does not error on database or table not-found, as that could indicate the replica
    /// just has not buffered data for those entities.
    fn get_buffer_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let buffer = self.buffer.read();
        let Some(db_buffer) = buffer.db_to_table.get(&db_schema.id) else {
            return Ok(vec![]);
        };
        let Some(table_buffer) = db_buffer.get(&table_def.table_id) else {
            return Ok(vec![]);
        };
        Ok(table_buffer
            .partitioned_record_batches(Arc::clone(&table_def), filter)
            .context("error getting partitioned batches from table buffer")?
            .into_iter()
            .map(|(gen_time, (ts_min_max, batches))| {
                let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &table_def.schema,
                    Some(ts_min_max),
                    &NoColumnRanges,
                );
                Arc::new(BufferChunk {
                    batches,
                    schema: table_def.schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::from_parts(
                        PartitionId::new(0),
                        Some(PartitionHashId::new(
                            IoxTableId::new(0),
                            &PartitionKey::from(gen_time.to_string()),
                        )),
                    ),
                    sort_key: None,
                    id: ChunkId::new(),
                    chunk_order: self.chunk_order(),
                }) as Arc<dyn QueryChunk>
            })
            .collect())
    }

    /// Get the `ChunkOrder` for this replica
    ///
    /// Uses the replica's `replica_order` to determine which replica wins in the event of a dedup.
    /// Replicas with lower order win, therefore, those listed first will take precedence.
    fn chunk_order(&self) -> ChunkOrder {
        // subtract an additional 1, as primary buffer chunks will use i64::MAX, so this should
        // be at most i64::MAX - 1
        ChunkOrder::new(i64::MAX - self.replica_order - 1)
    }

    /// Reload the snapshots and catalog for this writer
    async fn reload_snapshots(&self) -> Result<()> {
        let persisted_snapshots = get_persisted_snapshots_for_writer(
            Arc::clone(&self.object_store),
            &self.node_id(),
            Arc::clone(&self.time_provider),
        )
        .await?;

        for persisted_snapshot in persisted_snapshots {
            self.persisted_files
                .add_persisted_snapshot_files(persisted_snapshot);
        }

        Ok(())
    }

    /// Replay the WAL of the replicated write buffer to catch up
    ///
    /// Returns `true` if any WAL files were replayed
    async fn replay(
        &self,
        last_snapshotted_wal_file_sequence_number: Option<WalFileSequenceNumber>,
    ) -> Result<Option<WalFileSequenceNumber>> {
        let paths = self
            .load_existing_wal_paths(last_snapshotted_wal_file_sequence_number)
            .await?;
        let last_path = paths.last().cloned();
        if !paths.is_empty() {
            // track some information about the paths loaded for this replicated write buffer
            info!(
                from_node_id = self.node_def.name().as_ref(),
                number_of_wal_files = paths.len(),
                first_wal_file_path = ?paths.first(),
                last_wal_file_path = ?paths.last(),
                ?last_snapshotted_wal_file_sequence_number,
                "loaded existing wal paths from object store"
            );
        } else {
            debug!(
                from_node_id = self.node_def.name().as_ref(),
                "no existing wal paths loaded from object store"
            );
        }

        // fetch WAL files from object store in parallel:
        let mut fetch_handles = Vec::new();
        for path in paths {
            let object_store = Arc::clone(&self.object_store);
            fetch_handles.push(tokio::spawn(async move {
                get_wal_contents_from_object_store(object_store, path.clone())
                    .await
                    .map(|wal_contents| (path, wal_contents))
            }));
        }

        // process WAL files in series:
        for handle in fetch_handles {
            let (path, wal_contents) = handle
                .await
                .context("failed to complete task to fetch wal contents")??;
            self.replay_wal_file(wal_contents).await?;
            debug!(
                from_node_id = self.node_id().as_ref(),
                wal_file_path = %path,
                "replayed wal file"
            );
        }

        last_path
            .as_ref()
            .map(WalFileSequenceNumber::try_from)
            // if we cannot find any last_path (because it's from snapshot)
            // use the seq num from last snapshot if that's been passed in
            .or(Ok(last_snapshotted_wal_file_sequence_number).transpose())
            .transpose()
            .context("invalid wal file path")
            .map_err(Into::into)
    }

    async fn load_existing_wal_paths(
        &self,
        last_snapshotted_wal_file_sequence_number: Option<WalFileSequenceNumber>,
    ) -> Result<Vec<Path>> {
        let mut paths = vec![];
        let mut offset: Option<Path> = None;
        let path = Path::from(format!("{base}/wal", base = self.node_id()));
        loop {
            let mut listing = match offset {
                Some(ref offset) => self.object_store.list_with_offset(Some(&path), offset),
                None => self.object_store.list(Some(&path)),
            };
            let path_count = paths.len();
            while let Some(item) = listing.next().await {
                paths.push(
                    item.context("error in list item from object store")?
                        .location,
                );
            }
            if path_count == paths.len() {
                break;
            }
            paths.sort();
            offset = Some(paths.last().unwrap().clone());
        }

        if let Some(last_wal_number) = last_snapshotted_wal_file_sequence_number {
            let last_wal_path = wal_path(&self.node_id(), last_wal_number);
            paths.retain(|path| path > &last_wal_path);
        }

        paths.sort();

        Ok(paths)
    }

    /// Replay the given [`WalContents`].
    ///
    /// The maximum catalog sequence is extracted from the replayed WAL files and used to update
    /// the local catalog if they are in advance of that.
    async fn replay_wal_file(&self, wal_contents: WalContents) -> Result<()> {
        let persist_time_ms = wal_contents.persist_timestamp_ms;

        // get the latest catalog sequence from the wal contents so catalog can be updated
        if let Some(latest_catalog_sequence) = wal_contents
            .ops
            .iter()
            .filter_map(|op| match op {
                WalOp::Write(write_batch) => Some(write_batch.catalog_sequence),
                WalOp::Noop(_) => None,
            })
            .max()
            .map(CatalogSequenceNumber::new)
        {
            let current_catalog_sequence = self.catalog.sequence_number().get();
            if latest_catalog_sequence.get() > current_catalog_sequence {
                debug!(
                    catlog_sequence_from_wal = latest_catalog_sequence.get(),
                    current_catalog_sequence = self.catalog.sequence_number().get(),
                    from_node = self.node_id().as_ref(),
                    "updating catalog for replica to latest from WAL"
                );
                self.catalog
                .update_to_sequence_number(latest_catalog_sequence)
                .await
                .context("failed to update the catalog from latest sequence number found in replicated WAL file")?;
            }
        }

        match wal_contents.snapshot {
            None => self.buffer_wal_contents(wal_contents),
            Some(snapshot_details) => {
                self.buffer_wal_contents_and_handle_snapshots(wal_contents, snapshot_details)
            }
        }

        self.record_ttbr(persist_time_ms);

        Ok(())
    }

    /// Record the time to be readable (TTBR) for a WAL file's content
    fn record_ttbr(&self, persist_time_ms: i64) {
        let now_time = self.time_provider.now();

        let Some(persist_time) = Time::from_timestamp_millis(persist_time_ms) else {
            warn!(
                from_node_id = self.node_id().as_ref(),
                %persist_time_ms,
                "the millisecond persist timestamp in the replayed wal file was out-of-range or invalid"
            );
            return;
        };

        // track TTBR:
        match now_time.checked_duration_since(persist_time) {
            Some(ttbr) => self.metrics.replica_ttbr.record(ttbr),
            None => {
                info!(
                    from_node_id = self.node_id().as_ref(),
                    %now_time,
                    %persist_time,
                    "unable to get duration since WAL file was created"
                );
            }
        }
    }

    fn buffer_wal_contents(&self, wal_contents: WalContents) {
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        self.distinct_cache
            .write_wal_contents_to_cache(&wal_contents);
        let mut buffer = self.buffer.write();
        // TODO: what about the catalog stuff :shrug:
        buffer.buffer_write_ops(&wal_contents.ops);
    }

    fn buffer_wal_contents_and_handle_snapshots(
        &self,
        wal_contents: WalContents,
        snapshot_details: SnapshotDetails,
    ) {
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        self.distinct_cache
            .write_wal_contents_to_cache(&wal_contents);
        let catalog = self.catalog();
        // Update the Buffer by invoking the snapshot, to separate data in the buffer that will
        // get cleared by the snapshot, before fetching the snapshot from object store:
        {
            // get the lock inside this block so that it is dropped
            // when it is no longer needed, and is not held accross
            // await points below, which the compiler does not allow
            let mut buffer = self.buffer.write();
            // TODO: what about the catalog stuff :shrug:
            buffer.buffer_write_ops(&wal_contents.ops);
            for (db_id, tbl_map) in buffer.db_to_table.iter_mut() {
                let db_schema = catalog.db_schema_by_id(db_id).expect("db exists");
                for (tbl_id, tbl_buf) in tbl_map.iter_mut() {
                    let table_def = db_schema
                        .table_definition_by_id(tbl_id)
                        .expect("table exists");
                    tbl_buf.snapshot(table_def, snapshot_details.end_time_marker);
                }
            }
        }

        let snapshot_path =
            SnapshotInfoFilePath::new(&self.node_id(), snapshot_details.snapshot_sequence_number);
        let object_store = Arc::clone(&self.object_store);
        let buffer = Arc::clone(&self.buffer);
        let persisted_files = Arc::clone(&self.persisted_files);
        let parquet_cache = self.parquet_cache.clone();

        tokio::spawn(async move {
            // Update the persisted files:
            // This will continue to request the file from the object store if the request errors.
            // However, if the snapshot is retrieved, and fails to map, an error will be logged and
            // the loop will break. This will not halt replication.
            let mut not_found_attempts = 0;
            const RETRY_INTERVAL: Duration = Duration::from_secs(1);
            loop {
                match object_store.get(&snapshot_path).await {
                    Ok(get_result) => {
                        let snapshot_bytes = get_result
                            .bytes()
                            .await
                            .expect("unable to collect get result from object storage into bytes");
                        let snapshot = serde_json::from_slice::<PersistedSnapshot>(&snapshot_bytes)
                            .expect("unable to deserialize snapshot bytes");

                        // Now that the snapshot has been loaded, clear the buffer of the data that
                        // was separated out previously and update the persisted files. If there is
                        // a parquet cache, then load parquet files one table at a time and
                        // clearing the buffer for each table
                        for (db_id, db_tables) in &snapshot.databases {
                            for (table_id, parquet_files) in &db_tables.tables {
                                if let Some(ref parquet_cache) = parquet_cache {
                                    cache_parquet_from_snapshot_for_table(
                                        parquet_cache,
                                        parquet_files,
                                    )
                                    .await;
                                }

                                {
                                    let mut buffer = buffer.write();
                                    // By the time we get here, there is a chance that db_id and
                                    // table_id are deleted from outside. In those circumstances
                                    // we can ignore those ids, but the cache already has been
                                    // loaded - this will be evicted gradually (internal pruning)
                                    if let Some(db) = buffer.db_to_table.get_mut(db_id) {
                                        if let Some(table) = db.get_mut(table_id) {
                                            table.clear_snapshots();
                                        }
                                    }
                                }
                            }
                        }
                        persisted_files.add_persisted_snapshot_files(snapshot);
                        break;
                    }
                    Err(object_store::Error::NotFound { path, source }) => {
                        not_found_attempts += 1;
                        // only log NOT_FOUND errors every ten seconds as they can clutter up the
                        // logs. NOT_FOUND is common as the snapshot file may not be immediately
                        // available. This is because the WAL file may refer to a snapshot that is
                        // still in the process of being persisted, i.e., all of its parquet is
                        // being persisted, and the snapshot file itself is not created until all
                        // of the parquet has been persisted.
                        if not_found_attempts % 10 == 0 {
                            info!(
                                error = %source,
                                %path,
                                "persisted snapshot not found in replica's object storage after \
                                {not_found_attempts} attempts"
                            );
                        }
                    }
                    Err(error) => {
                        error!(
                            %error,
                            path = ?snapshot_path,
                            "error getting persisted snapshot from replica's object storage"
                        );
                    }
                }
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        });
    }
}

async fn get_persisted_snapshots_for_writer(
    object_store: Arc<dyn ObjectStore>,
    node_identifier_prefix: &str,
    time_provider: Arc<dyn TimeProvider>,
) -> Result<Vec<PersistedSnapshot>> {
    // Create a temporary persister to load snapshot files
    let persister = Persister::new(
        Arc::clone(&object_store),
        node_identifier_prefix,
        time_provider,
    );
    persister
        .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
        .await
        .context("failed to load snapshots for replicated write buffer")
        .map(|v| {
            v.into_iter()
                .map(|ps| match ps {
                    PersistedSnapshotVersion::V1(persisted_snapshot) => persisted_snapshot,
                })
                .collect()
        })
        .map_err(Into::into)
}

async fn get_wal_contents_from_object_store(
    object_store: Arc<dyn ObjectStore>,
    path: Path,
) -> Result<WalContents> {
    let obj = object_store.get(&path).await?;
    let file_bytes = obj
        .bytes()
        .await
        .context("failed to collect data for known file into bytes")?;
    verify_file_type_and_deserialize(file_bytes)
        .context("failed to verify and deserialize wal file contents")
        .map_err(Into::into)
}

async fn cache_parquet_from_snapshot_for_table(
    parquet_cache: &Arc<dyn ParquetCacheOracle>,
    parquet_files_for_table: &[ParquetFile],
) {
    let mut cache_notifiers = vec![];
    for parquet_file in parquet_files_for_table {
        debug!(path = ?parquet_file.path.as_str(), "Trying to cache parquet file when replaying wal");
        let (req, not) = CacheRequest::create_eventual_mode_cache_request(
            parquet_file.path.as_str().into(),
            None,
        );
        parquet_cache.register(req);
        cache_notifiers.push(not);
    }
    try_join_all(cache_notifiers)
        .await
        .expect("receive all parquet cache notifications");
}

fn background_replication_interval(
    replicated_buffer: Arc<ReplicatedBuffer>,
    mut last_wal_file_sequence_number: Option<WalFileSequenceNumber>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            // try to fetch new WAL files and buffer them...
            if let Some(mut wal_number) = last_wal_file_sequence_number {
                // Fetch WAL files until a NOT FOUND is encountered or other error:
                'inner: loop {
                    wal_number = wal_number.next();
                    let wal_path = wal_path(&replicated_buffer.node_id(), wal_number);
                    let wal_contents = match get_wal_contents_from_object_store(
                        Arc::clone(&replicated_buffer.object_store),
                        wal_path.clone(),
                    )
                    .await
                    {
                        Ok(w) => w,
                        Err(error) => {
                            match error {
                                // When the file is not found, we assume that it hasn't been created
                                // yet, so do nothing. Logging NOT_FOUND could get noisy.
                                Error::ObjectStore(object_store::Error::NotFound { .. }) => {}
                                // Otherwise, we log the error:
                                error => {
                                    error!(
                                        %error,
                                        from_node_id = replicated_buffer.node_id().as_ref(),
                                        wal_file_path = %wal_path,
                                        "failed to fetch next WAL file"
                                    );
                                }
                            }
                            break 'inner;
                        }
                    };

                    match replicated_buffer.replay_wal_file(wal_contents).await {
                        Ok(_) => {
                            info!(
                                from_node_id = replicated_buffer.node_id().as_ref(),
                                wal_file_path = %wal_path,
                                "replayed wal file"
                            );
                            last_wal_file_sequence_number.replace(wal_number);
                            // Don't break the inner loop here, since we want to try for more
                            // WAL files if they exist...
                        }
                        Err(error) => {
                            error!(
                                %error,
                                from_node_id = replicated_buffer.node_id().as_ref(),
                                wal_file_path = %wal_path,
                                "failed to replay next WAL file"
                            );
                            break 'inner;
                        }
                    }
                }
            } else {
                // If we don't have a last WAL file, we don't know what WAL number to fetch yet, so
                // need to rely on replay to get that.
                match replicated_buffer
                    .replay(last_wal_file_sequence_number)
                    .await
                {
                    Ok(Some(new_last_wal_number)) => {
                        last_wal_file_sequence_number.replace(new_last_wal_number);
                    }
                    Ok(None) => {
                        // if nothing was replayed, for whatever reason, we try to do the initial
                        // snapshot and catalog load again...
                        if let Err(error) = replicated_buffer.reload_snapshots().await {
                            error!(
                                %error,
                                from_node_id = replicated_buffer.node_id().as_ref(),
                                "failed to reload snapshots and catalog for replicated write buffer"
                            );
                        }
                    }

                    Err(error) => {
                        error!(
                            %error,
                            from_node_id = replicated_buffer.node_id().as_ref(),
                            "failed to replay replicated buffer on replication interval"
                        );
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use datafusion::assert_batches_sorted_eq;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::{
        distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
        parquet_cache::test_cached_obj_store_and_oracle,
    };
    use influxdb3_catalog::{catalog::Catalog, log::NodeMode};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::{
        Bufferer, ChunkFilter, DistinctCacheManager, LastCacheManager, ParquetFile,
        persister::Persister,
        test_helpers::WriteBufferTester,
        write_buffer::{WriteBufferImpl, WriteBufferImplArgs},
    };
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::{Attributes, DurationHistogram, Metric, Registry};
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use observability_deps::tracing::debug;

    use crate::{
        replica::{
            CreateReplicasArgs, CreateReplicatedBufferArgs, REPLICA_TTBR_METRIC, Replicas,
            ReplicatedBuffer,
        },
        test_helpers::{
            TestWrite, chunks_to_record_batches, do_writes, make_exec, verify_snapshot_count,
        },
    };

    // TODO - this needs to be addressed, see:
    // https://github.com/influxdata/influxdb_pro/issues/375
    #[test_log::test(tokio::test)]
    async fn replay_and_replicate_other_wal() {
        let cluster_id = "test_cluster";
        // Spin up a primary write buffer to do some writes and generate files in an object store:
        let primary_id = "espresso";
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Create a session context:
        // Since we are using the same object store accross primary and replica in this test, we
        // only need one context
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&obj_store));
        let primary = setup_primary(
            primary_id,
            cluster_id,
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            Arc::clone(&time_provider),
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu_items";

        // Do some writes to the primary to trigger snapshot:
        do_writes(
            db_name,
            primary.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso,type=drink price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano,type=drink price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=croissant,type=snack price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Check that snapshots (and parquet) have been persisted:
        verify_snapshot_count(
            1,
            Arc::clone(&obj_store),
            primary_id,
            Arc::clone(&time_provider),
        )
        .await;

        // Spin up a replicated buffer:
        let catalog = Arc::new(
            Catalog::new(
                cluster_id,
                Arc::clone(&obj_store),
                Arc::clone(&time_provider),
            )
            .await
            .unwrap(),
        );
        let replica = ReplicatedBuffer::new(CreateReplicatedBufferArgs {
            replica_order: 0,
            object_store: Arc::clone(&obj_store),
            node_def: catalog.node(primary_id).unwrap(),
            last_cache: primary.last_cache_provider(),
            distinct_cache: primary.distinct_cache_provider(),
            replication_interval: Duration::from_millis(10),
            metric_registry: Arc::new(Registry::new()),
            parquet_cache: None,
            catalog,
            time_provider: Arc::clone(&time_provider),
        })
        .await
        .unwrap();

        wait_for_replicated_buffer_persistence(&replica, db_name, tbl_name, 1).await;

        // Check that the replica replayed the primary and contains its data:
        {
            let db_schema = replica.catalog().db_schema(db_name).unwrap();
            let table_def = db_schema.table_definition(tbl_name).unwrap();
            let mut chunks = replica
                .get_buffer_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    &ChunkFilter::default(),
                )
                .unwrap();
            chunks.extend(replica.get_persisted_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                &ChunkFilter::default(),
                None,
                0,
            ));
            let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
            assert_batches_sorted_eq!(
                [
                    "+-----------+-------+---------------------+-------+",
                    "| name      | price | time                | type  |",
                    "+-----------+-------+---------------------+-------+",
                    "| americano | 3.0   | 1970-01-01T00:00:02 | drink |",
                    "| croissant | 4.5   | 1970-01-01T00:00:03 | snack |",
                    "| espresso  | 2.5   | 1970-01-01T00:00:01 | drink |",
                    "+-----------+-------+---------------------+-------+",
                ],
                &batches
            );
        }

        // Do more writes to the primary:
        do_writes(
            db_name,
            primary.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=muffin,type=snack price=4.00"),
                    time_seconds: 4,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte,type=drink price=6.00"),
                    time_seconds: 5,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=cortado,type=drink price=4.50"),
                    time_seconds: 6,
                },
            ],
        )
        .await;

        // Allow for another snapshot on primary:
        verify_snapshot_count(
            2,
            Arc::clone(&obj_store),
            primary_id,
            Arc::clone(&time_provider),
        )
        .await;

        // Check the primary chunks:
        {
            let batches = primary
                .get_record_batches_unchecked(db_name, tbl_name, &ctx)
                .await;
            assert_batches_sorted_eq!(
                [
                    "+-----------+-------+---------------------+-------+",
                    "| name      | price | time                | type  |",
                    "+-----------+-------+---------------------+-------+",
                    "| americano | 3.0   | 1970-01-01T00:00:02 | drink |",
                    "| cortado   | 4.5   | 1970-01-01T00:00:06 | drink |",
                    "| croissant | 4.5   | 1970-01-01T00:00:03 | snack |",
                    "| espresso  | 2.5   | 1970-01-01T00:00:01 | drink |",
                    "| latte     | 6.0   | 1970-01-01T00:00:05 | drink |",
                    "| muffin    | 4.0   | 1970-01-01T00:00:04 | snack |",
                    "+-----------+-------+---------------------+-------+",
                ],
                &batches
            );
        }

        wait_for_replicated_buffer_persistence(&replica, db_name, tbl_name, 2).await;

        // Check the replica again for the new writes:
        {
            let db_schema = replica.catalog().db_schema(db_name).unwrap();
            let table_def = db_schema.table_definition(tbl_name).unwrap();
            let mut chunks = replica
                .get_buffer_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    &ChunkFilter::default(),
                )
                .unwrap();
            chunks.extend(replica.get_persisted_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                &ChunkFilter::default(),
                None,
                0,
            ));
            let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
            assert_batches_sorted_eq!(
                [
                    "+-----------+-------+---------------------+-------+",
                    "| name      | price | time                | type  |",
                    "+-----------+-------+---------------------+-------+",
                    "| americano | 3.0   | 1970-01-01T00:00:02 | drink |",
                    "| cortado   | 4.5   | 1970-01-01T00:00:06 | drink |",
                    "| croissant | 4.5   | 1970-01-01T00:00:03 | snack |",
                    "| espresso  | 2.5   | 1970-01-01T00:00:01 | drink |",
                    "| latte     | 6.0   | 1970-01-01T00:00:05 | drink |",
                    "| muffin    | 4.0   | 1970-01-01T00:00:04 | snack |",
                    "+-----------+-------+---------------------+-------+",
                ],
                &batches
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn multi_replicated_buffers_with_overlap() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Create a session context:
        // Since we are using the same object store accross primary and replica in this test, we
        // only need one context
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&obj_store));
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cluster_id = "vulcans";
        // Spin up two primary write buffers to do some writes and generate files in an object store:
        let primary_ids = ["spock", "tuvok"];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
                cluster_id,
                Arc::clone(&obj_store),
                WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    snapshot_size: 1_000,
                },
                Arc::clone(&time_provider),
            )
            .await;
            primaries.insert(p, primary);
        }

        // Spin up a set of replicated buffers:

        // give the replicas a separate instance of the catalog so they manage reading updates to it
        let replica_id = "data";
        let replica_catalog = Arc::new(
            Catalog::new_enterprise(
                replica_id,
                cluster_id,
                Arc::clone(&obj_store) as _,
                Arc::clone(&time_provider) as _,
            )
            .await
            .unwrap(),
        );
        replica_catalog
            .register_node(replica_id, 1, vec![NodeMode::Query])
            .await
            .unwrap();

        let replicas = Replicas::new(CreateReplicasArgs {
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&replica_catalog))
                .await
                .unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&replica_catalog),
            )
            .await
            .unwrap(),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            parquet_cache: None,
            catalog: Arc::clone(&replica_catalog),
            time_provider,
        })
        .await
        .unwrap();
        // write to spock:
        do_writes(
            "foo",
            primaries["spock"].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=a val=false",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=a val=false",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=a val=false",
                },
            ],
        )
        .await;
        // write to tuvok, with values flipped to true:
        do_writes(
            "foo",
            primaries["tuvok"].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=a val=true",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=a val=true",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=a val=true",
                },
            ],
        )
        .await;

        // sleep for replicas to replicate:
        tokio::time::sleep(Duration::from_millis(50)).await;

        let db_schema = replicas.catalog().db_schema("foo").unwrap();
        let table_def = db_schema.table_definition("bar").unwrap();
        let chunks = replicas
            .get_buffer_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                &ChunkFilter::default(),
            )
            .unwrap();
        // there are only two chunks because all data falls in a single gen time block for each
        // respective buffer:
        assert_eq!(2, chunks.len());
        // the first chunk will be from spock, so should have a higher chunk order than tuvok:
        assert!(chunks[0].order() > chunks[1].order());
        // convert the chunks from both replicas as batches; the duplicates appear here, i.e.,
        // the deduplication happens elsewhere:
        let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
        assert_batches_sorted_eq!(
            [
                "+-----+---------------------+-------+",
                "| tag | time                | val   |",
                "+-----+---------------------+-------+",
                "| a   | 1970-01-01T00:00:01 | false |",
                "| a   | 1970-01-01T00:00:01 | true  |",
                "| a   | 1970-01-01T00:00:02 | false |",
                "| a   | 1970-01-01T00:00:02 | true  |",
                "| a   | 1970-01-01T00:00:03 | false |",
                "| a   | 1970-01-01T00:00:03 | true  |",
                "+-----+---------------------+-------+",
            ],
            &batches
        );
    }

    #[test_log::test(tokio::test)]
    async fn replica_buffer_ttbr_metrics() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Create a session context:
        // Since we are using the same object store accross primary and replica in this test, we
        // only need one context
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&obj_store));
        // start the primary time provider at 1000ms; this will be the `persist_time` on WAL files
        // that are persisted, and which is compared with the replica's time provider `now()` time
        // to determine TTBR
        let primary_time_provider = Arc::new(MockProvider::new(
            Time::from_timestamp_millis(1_000).unwrap(),
        ));

        let cluster_id = "test_cluster";

        // Spin up a primary write buffer to do some writes and generate files in an object store:
        let primary = setup_primary(
            "newton",
            cluster_id,
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1_000,
            },
            Arc::<MockProvider>::clone(&primary_time_provider),
        )
        .await;

        // Spin up a replicated buffer:
        let metric_registry = Arc::new(Registry::new());
        let replication_interval_ms = 50;
        let replica_id = "replica_node";
        // give the replica a separate time provider so it fixes its time at 100 ms ahead of the
        // primary, i.e., so that the TTBR is always 100ms.
        let replica_time_provider = Arc::new(MockProvider::new(
            Time::from_timestamp_millis(1_100).unwrap(),
        ));
        let catalog = Arc::new(
            Catalog::new_enterprise(
                replica_id,
                cluster_id,
                Arc::clone(&obj_store) as _,
                Arc::clone(&replica_time_provider) as _,
            )
            .await
            .unwrap(),
        );
        catalog
            .register_node(replica_id, 1, vec![NodeMode::Query])
            .await
            .unwrap();

        Replicas::new(CreateReplicasArgs {
            // just using the catalog from primary for caches since they aren't used:
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::<MockProvider>::clone(&replica_time_provider),
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::clone(&metric_registry),
            replication_interval: Duration::from_millis(replication_interval_ms),
            parquet_cache: None,
            catalog,
            time_provider: Arc::<MockProvider>::clone(&replica_time_provider),
        })
        .await
        .unwrap();

        // write to newton:
        do_writes(
            "foo",
            primary.as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=a val=false",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=a val=false",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=a val=false",
                },
            ],
        )
        .await;

        // sleep for replicas to replicate:
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check the metric registry:
        let metric = metric_registry
            .get_instrument::<Metric<DurationHistogram>>(REPLICA_TTBR_METRIC)
            .expect("get the metric");
        let ttbr_ms = metric
            .get_observer(&Attributes::from(&[("from_node_id", "newton")]))
            .expect("failed to get observer")
            .fetch();
        debug!(?ttbr_ms, "ttbr metric for writer");
        assert_eq!(ttbr_ms.sample_count(), 3);
        assert_eq!(ttbr_ms.total, Duration::from_millis(300));
        assert!(
            ttbr_ms
                .buckets
                .iter()
                .any(|bucket| bucket.le == Duration::from_millis(100) && bucket.count == 3)
        );
    }

    // TODO: this no longer holds, because the snapshot process has deleted the WAL files
    // on the primary writers, and therefore, this is no longer replaying via the WAL, but
    // via loading snapshots directly. The parquet cache is not being populated via the
    // snapshots, and I don't want to do this carelessly, because all of a sudden trying to
    // cache every parquet file encountered in snapshots on load could very easily lead to OOM
    // The issue to handle parquet cache pre-population is here:
    // https://github.com/influxdata/influxdb_pro/issues/191
    #[test_log::test(tokio::test)]
    #[ignore = "see comment"]
    async fn parquet_cache_with_read_replicas() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cluster_id = "simpsons_characters";
        // spin up two primary write buffers:
        let primary_ids = ["skinner", "chalmers"];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
                cluster_id,
                Arc::clone(&obj_store),
                WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    // small snapshot size will have parquet written out after 3 WAL periods:
                    snapshot_size: 1,
                },
                Arc::clone(&time_provider),
            )
            .await;
            primaries.insert(p, primary);
        }

        // write to skinner:
        do_writes(
            "foo",
            primaries["skinner"].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,source=skinner f1=0.1",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,source=skinner f1=0.2",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,source=skinner f1=0.3",
                },
            ],
        )
        .await;
        // write to chalmers:
        do_writes(
            "foo",
            primaries["chalmers"].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,source=chalmers f1=0.4",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,source=chalmers f1=0.5",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,source=chalmers f1=0.6",
                },
            ],
        )
        .await;

        // ensure snapshots have been taken so there are parquet files for each writer:
        verify_snapshot_count(
            1,
            Arc::clone(&obj_store),
            "skinner",
            Arc::clone(&time_provider),
        )
        .await;
        verify_snapshot_count(
            1,
            Arc::clone(&obj_store),
            "chalmers",
            Arc::clone(&time_provider),
        )
        .await;

        // Spin up a set of replicated buffers with a cached object store. This is scoped so that
        // everything set up in the block is dropped before the block below that tests without a
        // cache:
        {
            let time_provider: Arc<dyn TimeProvider> =
                Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let test_store = Arc::new(RequestCountedObjectStore::new(Arc::clone(&obj_store)));
            let (cached_obj_store, parquet_cache) = test_cached_obj_store_and_oracle(
                Arc::clone(&test_store) as _,
                Arc::clone(&time_provider),
                Default::default(),
            );
            let ctx = IOxSessionContext::with_testing();
            let runtime_env = ctx.inner().runtime_env();
            register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&cached_obj_store));

            let catalog = Arc::new(
                Catalog::new(
                    cluster_id,
                    Arc::clone(&test_store) as _,
                    Arc::clone(&time_provider),
                )
                .await
                .unwrap(),
            );
            let replicas = Replicas::new(CreateReplicasArgs {
                // could load a new catalog from the object store, but it is easier to just re-use
                // skinner's:
                last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                    .await
                    .unwrap(),
                distinct_cache: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&time_provider),
                    Arc::clone(&catalog),
                )
                .await
                .unwrap(),
                object_store: Arc::clone(&cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                parquet_cache: Some(parquet_cache),
                catalog,
                time_provider,
            })
            .await
            .unwrap();

            // once the `Replicas` has some persisted files, then it will also have those files in
            // the cache, since the buffer isn't cleared until the cache requests are registered/
            // fulfilled:
            let persisted_files = wait_for_replica_persistence(&replicas, "foo", "bar", 2).await;
            // should be 2 parquet files, 1 from each primary:
            assert_eq!(2, persisted_files.len());
            // use the RequestCountedObjectStore to check for read requests to the inner object
            // store for each persisted parquet file:
            for ParquetFile { path, .. } in &persisted_files {
                // there should be 1 request made to the store for each file, by the cache oracle:
                assert_eq!(
                    1,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }

            // fetch chunks/record batches from the `Replicas`, as if performing a query, i.e., so
            // that datafusion will request to the object store for the persisted files:
            let chunks = replicas.get_all_chunks("foo", "bar");
            let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
            assert_batches_sorted_eq!(
                [
                    "+-----+----------+---------------------+",
                    "| f1  | source   | time                |",
                    "+-----+----------+---------------------+",
                    "| 0.1 | skinner  | 1970-01-01T00:00:01 |",
                    "| 0.2 | skinner  | 1970-01-01T00:00:02 |",
                    "| 0.3 | skinner  | 1970-01-01T00:00:03 |",
                    "| 0.4 | chalmers | 1970-01-01T00:00:01 |",
                    "| 0.5 | chalmers | 1970-01-01T00:00:02 |",
                    "| 0.6 | chalmers | 1970-01-01T00:00:03 |",
                    "+-----+----------+---------------------+",
                ],
                &batches
            );

            // check the RequestCountedObjectStore again for each persisted file:
            for ParquetFile { path, .. } in &persisted_files {
                // requests to this path should not have changed, due to the cache:
                assert_eq!(
                    1,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }
        }

        // Spin up another set of replicated buffers but this time do not use a cached store:
        {
            let test_store = Arc::new(RequestCountedObjectStore::new(Arc::clone(&obj_store)));
            let non_cached_obj_store: Arc<dyn ObjectStore> = Arc::clone(&test_store) as _;
            let ctx = IOxSessionContext::with_testing();
            let runtime_env = ctx.inner().runtime_env();
            register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&non_cached_obj_store));

            let catalog = Arc::new(
                Catalog::new(
                    "replica",
                    Arc::clone(&test_store) as _,
                    Arc::clone(&time_provider),
                )
                .await
                .unwrap(),
            );
            let replicas = Replicas::new(CreateReplicasArgs {
                // like above, just re-use skinner's catalog for ease:
                last_cache: LastCacheProvider::new_from_catalog(primaries["skinner"].catalog())
                    .await
                    .unwrap(),
                distinct_cache: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&time_provider),
                    primaries["skinner"].catalog(),
                )
                .await
                .unwrap(),
                object_store: Arc::clone(&non_cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                parquet_cache: None,
                catalog,
                time_provider,
            })
            .await
            .unwrap();

            // still need to wait for files to be persisted, despite there being no cache, otherwise
            // the queries below could just be served by the buffer:
            let persisted_files = wait_for_replica_persistence(&replicas, "foo", "bar", 2).await;
            // should be 2 parquet files, 1 from each primary:
            assert_eq!(2, persisted_files.len());
            // check for requests made to the inner object store for each persisted file, before
            // making a query:
            for ParquetFile { path, .. } in &persisted_files {
                // there should be 0 requests made to the store for each file, since no querying or
                // caching has taken place yet:
                assert_eq!(
                    0,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }

            // do the "query":
            let chunks = replicas.get_all_chunks("foo", "bar");
            let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
            assert_batches_sorted_eq!(
                [
                    "+-----+----------+---------------------+",
                    "| f1  | source   | time                |",
                    "+-----+----------+---------------------+",
                    "| 0.1 | skinner  | 1970-01-01T00:00:01 |",
                    "| 0.2 | skinner  | 1970-01-01T00:00:02 |",
                    "| 0.3 | skinner  | 1970-01-01T00:00:03 |",
                    "| 0.4 | chalmers | 1970-01-01T00:00:01 |",
                    "| 0.5 | chalmers | 1970-01-01T00:00:02 |",
                    "| 0.6 | chalmers | 1970-01-01T00:00:03 |",
                    "+-----+----------+---------------------+",
                ],
                &batches
            );

            // check for requests made to inner object store for each persisted file, after the
            // query was made:
            for ParquetFile { path, .. } in &persisted_files {
                // requests should have gone up, in this case, several get_range/get_ranges requests
                // are made, hence it going up by more than 1:
                assert_eq!(
                    3,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_parquet_cache_in_write_path_single_read_replica() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cluster_id = "test_cluster";
        let primary_node_id = "primary_node";
        // spin up primary write buffers:
        let primary_ids = [primary_node_id];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
                cluster_id,
                Arc::clone(&obj_store),
                WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    // small snapshot size will have parquet written out after 3 WAL periods:
                    snapshot_size: 1,
                },
                Arc::clone(&time_provider),
            )
            .await;
            primaries.insert(p, primary);
        }

        // setup replica
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::clone(&obj_store)));
        let (cached_obj_store, parquet_cache) = test_cached_obj_store_and_oracle(
            Arc::clone(&test_store) as _,
            Arc::clone(&time_provider),
            Default::default(),
        );
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&cached_obj_store));
        let replica_id = "replica_node";
        let catalog = Arc::new(
            Catalog::new_enterprise(
                replica_id,
                cluster_id,
                Arc::clone(&test_store) as _,
                Arc::clone(&time_provider) as _,
            )
            .await
            .unwrap(),
        );
        catalog
            .register_node(replica_id, 1, vec![NodeMode::Query])
            .await
            .unwrap();

        // This is a READ replica
        let replicas = Replicas::new(CreateReplicasArgs {
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            object_store: Arc::clone(&cached_obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            parquet_cache: Some(parquet_cache),
            catalog,
            time_provider: Arc::clone(&time_provider),
        })
        .await
        .unwrap();

        do_writes(
            "foo",
            primaries[primary_node_id].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,source=primary_src f1=0.1",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,source=primary_src f1=0.2",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,source=primary_src f1=0.3",
                },
            ],
        )
        .await;
        // ensure snapshots have been taken so there are parquet files for each writer:
        verify_snapshot_count(
            1,
            Arc::clone(&obj_store),
            primary_node_id,
            Arc::clone(&time_provider),
        )
        .await;

        // spin up a single read replica and see if the file is getting cached on replica
        {
            // once the `Replicas` has some persisted files, then it will also have those files in
            // the cache, since the buffer isn't cleared until the cache requests are registered/
            // fulfilled:
            let persisted_files = wait_for_replica_persistence(&replicas, "foo", "bar", 1).await;
            // should be 1 parquet file
            assert_eq!(1, persisted_files.len());
            // use the RequestCountedObjectStore to check for read requests to the inner object
            // store for each persisted parquet file:
            for ParquetFile { path, .. } in &persisted_files {
                // there should be 1 request made to the store for each file, by the cache oracle:
                assert_eq!(
                    1,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }

            // fetch chunks/record batches from the `Replicas`, as if performing a query, i.e., so
            // that datafusion will request to the object store for the persisted files:
            let chunks = replicas.get_all_chunks("foo", "bar");
            let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
            assert_batches_sorted_eq!(
                [
                    "+-----+-------------+---------------------+",
                    "| f1  | source      | time                |",
                    "+-----+-------------+---------------------+",
                    "| 0.1 | primary_src | 1970-01-01T00:00:01 |",
                    "| 0.2 | primary_src | 1970-01-01T00:00:02 |",
                    "| 0.3 | primary_src | 1970-01-01T00:00:03 |",
                    "+-----+-------------+---------------------+",
                ],
                &batches
            );

            // check the RequestCountedObjectStore again for each persisted file:
            for ParquetFile { path, .. } in &persisted_files {
                // requests to this path should not have changed, due to the cache:
                assert_eq!(
                    1,
                    test_store.total_read_request_count(&Path::from(path.as_str()))
                );
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_parquet_cache_in_query_path_single_read_replica() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cluster_id = "test_cluster";
        let primary_node_id = "primary_node";
        // spin up primary write buffers:
        let primary_ids = [primary_node_id];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
                cluster_id,
                Arc::clone(&obj_store),
                WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    // small snapshot size will have parquet written out after 3 WAL periods:
                    snapshot_size: 1,
                },
                Arc::clone(&time_provider) as _,
            )
            .await;
            primaries.insert(p, primary);
        }

        do_writes(
            "foo",
            primaries[primary_node_id].as_ref(),
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,source=primary_src f1=0.1",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,source=primary_src f1=0.2",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,source=primary_src f1=0.3",
                },
            ],
        )
        .await;
        // ensure snapshots have been taken so there are parquet files for each writer:
        verify_snapshot_count(
            1,
            Arc::clone(&obj_store),
            primary_node_id,
            Arc::clone(&time_provider) as _,
        )
        .await;

        // setup read replica/move the time
        time_provider.set(Time::from_timestamp_millis(2000).unwrap());
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::clone(&obj_store)));
        let (cached_obj_store, parquet_cache) = test_cached_obj_store_and_oracle(
            Arc::clone(&test_store) as _,
            Arc::clone(&time_provider) as _,
            Default::default(),
        );
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&cached_obj_store));
        // create a new catalog for the replica instead of re-using the one from above:
        let replica_id = "replica_node";
        let catalog = Arc::new(
            Catalog::new_enterprise(
                replica_id,
                cluster_id,
                Arc::clone(&test_store) as _,
                Arc::clone(&time_provider) as _,
            )
            .await
            .unwrap(),
        );
        catalog
            .register_node(replica_id, 1, vec![NodeMode::Query])
            .await
            .unwrap();

        let replicas = Replicas::new(CreateReplicasArgs {
            last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
                .await
                .unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider) as _,
                Arc::clone(&catalog),
            )
            .await
            .unwrap(),
            object_store: Arc::clone(&cached_obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            parquet_cache: Some(parquet_cache),
            catalog,
            time_provider: Arc::clone(&time_provider) as _,
        })
        .await
        .unwrap();

        let persisted_files = wait_for_replica_persistence(&replicas, "foo", "bar", 1).await;
        assert_eq!(1, persisted_files.len());
        for ParquetFile { path, .. } in &persisted_files {
            assert_eq!(
                0,
                test_store.total_read_request_count(&Path::from(path.as_str()))
            );
        }

        // try fetching to cache in query path
        let chunks = replicas.get_all_chunks("foo", "bar");
        let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
        assert_batches_sorted_eq!(
            [
                "+-----+-------------+---------------------+",
                "| f1  | source      | time                |",
                "+-----+-------------+---------------------+",
                "| 0.1 | primary_src | 1970-01-01T00:00:01 |",
                "| 0.2 | primary_src | 1970-01-01T00:00:02 |",
                "| 0.3 | primary_src | 1970-01-01T00:00:03 |",
                "+-----+-------------+---------------------+",
            ],
            &batches
        );

        debug!("===============================================");
        // This time allows the cache request to be fulfilled, not sure
        // if there's any way around this.
        tokio::time::sleep(Duration::from_millis(200)).await;
        debug!("===============================================");

        // check the RequestCountedObjectStore again for each persisted file
        for ParquetFile { path, .. } in &persisted_files {
            assert_eq!(
                4,
                test_store.total_read_request_count(&Path::from(path.as_str()))
            );
        }

        // try fetching again, this time everything should be cached
        let chunks = replicas.get_all_chunks("foo", "bar");
        let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
        assert_batches_sorted_eq!(
            [
                "+-----+-------------+---------------------+",
                "| f1  | source      | time                |",
                "+-----+-------------+---------------------+",
                "| 0.1 | primary_src | 1970-01-01T00:00:01 |",
                "| 0.2 | primary_src | 1970-01-01T00:00:02 |",
                "| 0.3 | primary_src | 1970-01-01T00:00:03 |",
                "+-----+-------------+---------------------+",
            ],
            &batches
        );

        for ParquetFile { path, .. } in &persisted_files {
            // requests to this path should not have changed, due to the cache:
            assert_eq!(
                4,
                test_store.total_read_request_count(&Path::from(path.as_str()))
            );
        }
    }

    async fn setup_primary(
        node_id: &str,
        cluster_id: &str,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Arc<WriteBufferImpl> {
        debug!(node_id, cluster_id, "setting up primary for test");
        let catalog = Arc::new(
            Catalog::new_enterprise(
                node_id,
                cluster_id,
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
            )
            .await
            .unwrap(),
        );
        catalog
            .register_node(node_id, 1, vec![NodeMode::Ingest])
            .await
            .unwrap();
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            node_id,
            Arc::clone(&time_provider),
        ));
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let metric_registry = Arc::new(Registry::new());
        WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            executor: make_exec(object_store, Arc::clone(&metric_registry)),
            wal_config,
            parquet_cache: None,
            metric_registry,
            snapshotted_wal_files_to_keep: 10,
            query_file_limit: None,
        })
        .await
        .unwrap()
    }

    /// Wait for a [`Replicas`] to go from having no persisted files to having some persisted files
    async fn wait_for_replica_persistence(
        replicas: &Replicas,
        db: &str,
        tbl: &str,
        expected_file_count: usize,
    ) -> Vec<ParquetFile> {
        let db_schema = replicas.catalog().db_schema(db).unwrap();
        let table_id = db_schema.table_name_to_id(tbl).unwrap();
        for _ in 0..10 {
            let persisted_files = replicas.parquet_files(db_schema.id, table_id);
            if persisted_files.len() >= expected_file_count {
                return persisted_files;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("no files were persisted after several tries");
    }

    /// Wait for a [`Replicas`] to go from having no persisted files to having some persisted files
    async fn wait_for_replicated_buffer_persistence(
        replica: &ReplicatedBuffer,
        db: &str,
        tbl: &str,
        expected_file_count: usize,
    ) -> Vec<ParquetFile> {
        let db_schema = replica.catalog().db_schema(db).unwrap();
        let table_id = db_schema.table_name_to_id(tbl).unwrap();
        let mut most_files_found = 0;
        for _ in 0..10 {
            let persisted_files = replica.parquet_files(db_schema.id, table_id);
            most_files_found = most_files_found.max(persisted_files.len());
            if persisted_files.len() >= expected_file_count {
                return persisted_files;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("no files were persisted after several tries, most found: {most_files_found}");
    }
}
