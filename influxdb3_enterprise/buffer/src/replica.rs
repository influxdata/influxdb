use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::{anyhow, Context};
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
use influxdb3_catalog::catalog::{
    enterprise::CatalogIdMap, Catalog, DatabaseSchema, TableDefinition,
};
use influxdb3_enterprise_data_layout::WriterSnapshotMarker;
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::{
    object_store::wal_path, serialize::verify_file_type_and_deserialize, SnapshotDetails, Wal,
    WalContents, WalFileSequenceNumber, WalOp,
};
use influxdb3_write::{
    chunk::BufferChunk,
    paths::SnapshotInfoFilePath,
    persister::{Persister, DEFAULT_OBJECT_STORE_URL},
    write_buffer::{
        parquet_chunk_from_file, persisted_files::PersistedFiles, queryable_buffer::BufferState,
        N_SNAPSHOTS_TO_LOAD_ON_START,
    },
    ChunkFilter, DatabaseTables, ParquetFile, PersistedSnapshot,
};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, NoColumnRanges},
    QueryChunk,
};
use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, Registry};
use object_store::{path::Path, ObjectStore};
use observability_deps::tracing::{error, info, warn};
use parking_lot::RwLock;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("unexpected replication error: {0:#}")]
    Unexpected(#[from] anyhow::Error),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ReplicationConfig {
    pub interval: Duration,
    pub writer_ids: Vec<String>,
}

impl ReplicationConfig {
    pub fn new(interval: Duration, writer_ids: Vec<String>) -> Self {
        Self {
            interval,
            writer_ids,
        }
    }

    pub fn writer_ids(&self) -> &[String] {
        &self.writer_ids
    }
}

#[derive(Debug)]
pub(crate) struct Replicas {
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
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
    pub writer_ids: Vec<String>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub catalog: Arc<Catalog>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub wal: Option<Arc<dyn Wal>>,
}

impl Replicas {
    pub(crate) async fn new(
        CreateReplicasArgs {
            last_cache,
            distinct_cache,
            object_store,
            metric_registry,
            replication_interval,
            writer_ids,
            parquet_cache,
            catalog,
            time_provider,
            wal,
        }: CreateReplicasArgs,
    ) -> Result<Self> {
        let mut replicated_buffers = vec![];
        for (i, writer_identifier_prefix) in writer_ids.into_iter().enumerate() {
            let object_store = Arc::clone(&object_store);
            let last_cache = Arc::clone(&last_cache);
            let distinct_cache = Arc::clone(&distinct_cache);
            let metric_registry = Arc::clone(&metric_registry);
            let parquet_cache = parquet_cache.clone();
            let catalog = Arc::clone(&catalog);
            let time_provider = Arc::clone(&time_provider);
            let wal = wal.clone();
            info!(%writer_identifier_prefix, "creating replicated buffer for writer");
            replicated_buffers.push(
                ReplicatedBuffer::new(CreateReplicatedBufferArgs {
                    replica_order: i as i64,
                    object_store,
                    writer_identifier_prefix,
                    last_cache,
                    distinct_cache,
                    replication_interval,
                    metric_registry,
                    parquet_cache,
                    catalog,
                    time_provider,
                    wal,
                })
                .await?,
            )
        }
        Ok(Self {
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            last_cache,
            distinct_cache,
            replicated_buffers,
        })
    }

    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        self.object_store_url.clone()
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        self.replicated_buffers[0].catalog()
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
        writer_markers: &[Arc<WriterSnapshotMarker>],
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut chunks = vec![];
        for replica in &self.replicated_buffers {
            let last_parquet_file_id = writer_markers.iter().find_map(|marker| {
                if marker.writer_id == replica.writer_identifier_prefix {
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
}

#[derive(Debug)]
pub(crate) struct ReplicatedBuffer {
    replica_order: i64,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    writer_identifier_prefix: String,
    buffer: Arc<RwLock<BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    catalog: Arc<ReplicatedCatalog>,
    metrics: ReplicatedBufferMetrics,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    time_provider: Arc<dyn TimeProvider>,
    wal: Option<Arc<dyn Wal>>,
}

#[derive(Debug)]
struct ReplicatedCatalog {
    catalog: Arc<Catalog>,
    id_map: Arc<parking_lot::Mutex<CatalogIdMap>>,
}

impl ReplicatedCatalog {
    /// Create a replicated catalog from a primary, i.e., local catalog, and the catalog of another
    /// write buffer that is being replicated.
    fn new(primary: Arc<Catalog>, replica: Arc<Catalog>) -> Result<Self> {
        let id_map = primary
            .merge(replica)
            .context("unable to merge replica catalog with primary")?;
        Ok(Self {
            catalog: primary,
            id_map: Arc::new(parking_lot::Mutex::new(id_map)),
        })
    }

    /// Merge the `other` catalog into this one
    ///
    /// This may be needed if the replicated catalog is re-initialized
    fn merge_catlog(&self, other: Arc<Catalog>) -> Result<()> {
        let ids = self
            .catalog
            .merge(other)
            .context("failed to merge other catalog into this replicated catalog")?;
        self.id_map.lock().extend(ids);
        Ok(())
    }

    fn map_wal_contents(&self, wal_contents: WalContents) -> Result<WalContents> {
        self.id_map
            .lock()
            .map_wal_contents(&self.catalog, wal_contents)
            .context("failed to map WAL contents")
            .map_err(Into::into)
    }

    fn map_snapshot_contents(&self, snapshot: PersistedSnapshot) -> Result<PersistedSnapshot> {
        let id_map = self.id_map.lock();
        Ok(PersistedSnapshot {
            databases: snapshot
                .databases
                .into_iter()
                .map(|(replica_db_id, db_tables)| {
                    let local_db_id = id_map.map_db_id(&replica_db_id).ok_or_else(|| {
                        Error::Unexpected(anyhow!(
                            "invalid database id in persisted snapshot: {replica_db_id}"
                        ))
                    })?;
                    Ok((
                        local_db_id,
                        DatabaseTables {
                            tables: db_tables
                                .tables
                                .into_iter()
                                .map(|(replica_table_id, files)| {
                                    Ok((
                                        id_map
                                            .map_table_id(&replica_table_id)
                                            .ok_or_else(|| Error::Unexpected(anyhow!("invalid table id in persisted snapshot: {replica_table_id}")))?,
                                        files,
                                    ))
                                })
                                .collect::<Result<SerdeVecMap<_, _>>>()?,
                        },
                    ))
                })
                .collect::<Result<SerdeVecMap<DbId, DatabaseTables>>>()?,
            ..snapshot
        })
    }
}

pub const REPLICA_TTBR_METRIC: &str = "influxdb3_replica_ttbr_duration";

#[derive(Debug)]
struct ReplicatedBufferMetrics {
    replica_ttbr: DurationHistogram,
}

const REPLICA_CATALOG_RETRY_INTERVAL_SECONDS: u64 = 1;

pub(crate) struct CreateReplicatedBufferArgs {
    replica_order: i64,
    object_store: Arc<dyn ObjectStore>,
    writer_identifier_prefix: String,
    last_cache: Arc<LastCacheProvider>,
    distinct_cache: Arc<DistinctCacheProvider>,
    replication_interval: Duration,
    metric_registry: Arc<Registry>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    wal: Option<Arc<dyn Wal>>,
}

impl ReplicatedBuffer {
    pub(crate) async fn new(
        CreateReplicatedBufferArgs {
            replica_order,
            object_store,
            writer_identifier_prefix,
            last_cache,
            distinct_cache,
            replication_interval,
            metric_registry,
            parquet_cache,
            catalog,
            time_provider,
            wal,
        }: CreateReplicatedBufferArgs,
    ) -> Result<Arc<Self>> {
        let (persisted_catalog, persisted_snapshots) =
            get_persisted_catalog_and_snapshots_for_writer(
                Arc::clone(&object_store),
                &writer_identifier_prefix,
            )
            .await?;
        let writer_id: Cow<'static, str> = Cow::from(writer_identifier_prefix.clone());
        let attributes = Attributes::from([("from_writer_id", writer_id)]);
        let replica_ttbr = metric_registry
            .register_metric::<DurationHistogram>(
                REPLICA_TTBR_METRIC,
                "time to be readable for the data in each replicated write buffer",
            )
            .recorder(attributes);
        let replica_catalog = ReplicatedCatalog::new(Arc::clone(&catalog), persisted_catalog)?;
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(
            &replica_catalog.catalog,
        ))));
        let persisted_snapshots = persisted_snapshots
            .into_iter()
            .map(|snapshot| replica_catalog.map_snapshot_contents(snapshot))
            .collect::<Result<Vec<_>>>()?;
        let last_snapshotted_wal_file_sequence_number = persisted_snapshots
            .first()
            .map(|snapshot| snapshot.wal_file_sequence_number);
        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
            Arc::clone(&time_provider),
            persisted_snapshots,
        ));
        let replicated_buffer = Self {
            replica_order,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            writer_identifier_prefix,
            buffer,
            persisted_files,
            last_cache,
            distinct_cache,
            metrics: ReplicatedBufferMetrics { replica_ttbr },
            parquet_cache,
            catalog: Arc::new(replica_catalog),
            time_provider,
            wal,
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

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog.catalog)
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
    async fn reload_snapshots_and_catalog(&self) -> Result<()> {
        let (persisted_catalog, persisted_snapshots) =
            get_persisted_catalog_and_snapshots_for_writer(
                Arc::clone(&self.object_store),
                &self.writer_identifier_prefix,
            )
            .await?;

        self.catalog.merge_catlog(persisted_catalog)?;
        for persisted_snapshot in persisted_snapshots {
            self.persisted_files.add_persisted_snapshot_files(
                self.catalog.map_snapshot_contents(persisted_snapshot)?,
            );
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
        // track some information about the paths loaded for this replicated write buffer
        info!(
            from_writer_id = self.writer_identifier_prefix,
            number_of_wal_files = paths.len(),
            first_wal_file_path = ?paths.first(),
            last_wal_file_path = ?paths.last(),
            "loaded existing wal paths from object store"
        );

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
            info!(
                from_writer_id = self.writer_identifier_prefix,
                wal_file_path = %path,
                "replayed wal file"
            );
        }

        last_path
            .as_ref()
            .map(WalFileSequenceNumber::try_from)
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
        let path = Path::from(format!("{base}/wal", base = self.writer_identifier_prefix));
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
            let last_wal_path = wal_path(&self.writer_identifier_prefix, last_wal_number);
            paths.retain(|path| path > &last_wal_path);
        }

        paths.sort();

        Ok(paths)
    }

    /// Replay the given [`WalContents`].
    ///
    /// This will map identifiers within the `WalContents`, i.e., those on the replicated write buffer, to
    /// those of the local catalog, then apply the `WalContents` to the local buffer, handling
    /// snapshots if present.
    ///
    /// If this replicated buffer is running on a write-enabled server, any catalog batches will be
    /// extracted from the `WalContents` and written to the local pirimary buffer's WAL.
    async fn replay_wal_file(&self, wal_contents: WalContents) -> Result<()> {
        let persist_time_ms = wal_contents.persist_timestamp_ms;

        let wal_contents = self.catalog.map_wal_contents(wal_contents)?;
        self.apply_catalog_batches_to_local(&wal_contents).await?;

        match wal_contents.snapshot {
            None => self.buffer_wal_contents(wal_contents),
            Some(snapshot_details) => {
                self.buffer_wal_contents_and_handle_snapshots(wal_contents, snapshot_details)
            }
        }

        self.record_ttbr(persist_time_ms);

        Ok(())
    }

    /// Apply catalog batches from a WAL file to the local catalog if this replica is tied to a
    /// local primary buffer that has a WAL
    ///
    /// This is to ensure that catalog updates received by other replicated write buffers are also made
    /// durable on the local buffer
    ///
    /// Replicated catalog batches are extracted from the mapped [`WalContents`] and filtered to
    /// remove any that would not actually change the local catalog to prevent insertion of ops
    /// to the local WAL that make changes that were already applied locally or by other replicas.
    async fn apply_catalog_batches_to_local(
        &self,
        mapped_wal_contents: &WalContents,
    ) -> Result<()> {
        let Some(wal) = self.wal.as_ref() else {
            return Ok(());
        };
        let catalog_ops: Vec<WalOp> = mapped_wal_contents
            .ops
            .iter()
            .filter_map(|op| match op {
                WalOp::Write(_) => None,
                WalOp::Catalog(ordered_catalog_batch) => Some(
                    self.catalog
                        .catalog
                        .apply_catalog_batch(ordered_catalog_batch.clone().batch())
                        .transpose()?
                        .map(WalOp::Catalog)
                        .context("failed to apply and order the replicated catalog batch")
                        .map_err(Into::into),
                ),
                WalOp::Noop(_) => None,
            })
            .collect::<Result<Vec<_>>>()?;
        if !catalog_ops.is_empty() {
            info!(
                from_writer_id = self.writer_identifier_prefix,
                "writing catalog ops from replicated write buffer to local WAL"
            );
            wal.write_ops(catalog_ops)
                .await
                .context("failed to write replicated catalog ops to local WAL")?;
        }
        Ok(())
    }

    /// Record the time to be readable (TTBR) for a WAL file's content
    fn record_ttbr(&self, persist_time_ms: i64) {
        let now_time = self.time_provider.now();

        let Some(persist_time) = Time::from_timestamp_millis(persist_time_ms) else {
            warn!(
                from_writer_id = self.writer_identifier_prefix,
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
                    from_writer_id = self.writer_identifier_prefix,
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
        buffer.buffer_ops(&wal_contents.ops, &self.last_cache, &self.distinct_cache);
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
            buffer.buffer_ops(&wal_contents.ops, &self.last_cache, &self.distinct_cache);
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

        let writer_id = self.writer_identifier_prefix.clone();
        let snapshot_path = SnapshotInfoFilePath::new(
            &self.writer_identifier_prefix,
            snapshot_details.snapshot_sequence_number,
        );
        let object_store = Arc::clone(&self.object_store);
        let buffer = Arc::clone(&self.buffer);
        let persisted_files = Arc::clone(&self.persisted_files);
        let parquet_cache = self.parquet_cache.clone();
        let replica_catalog = Arc::clone(&self.catalog);

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
                        // Map the IDs in the snapshot:
                        let snapshot = replica_catalog
                            .map_snapshot_contents(snapshot)
                            .inspect_err(|error| {
                                error!(
                                    from_writer = writer_id,
                                    ?snapshot_path,
                                    %error,
                                    "the replicated write buffer produced an invalid snapshot file"
                                );
                            })
                            .expect("failed to map the persisted snapshot file");

                        // Now that the snapshot has been loaded, clear the buffer of the data that
                        // was separated out previously and update the persisted files. If there is
                        // a parquet cache, then load parquet files from the snapshot into the cache
                        // before clearing the buffer, to minimize time holding the buffer lock:
                        if let Some(parquet_cache) = parquet_cache {
                            cache_parquet_from_snapshot(&parquet_cache, &snapshot).await;
                        }
                        let mut buffer = buffer.write();
                        for (_, tbl_map) in buffer.db_to_table.iter_mut() {
                            for (_, tbl_buf) in tbl_map.iter_mut() {
                                tbl_buf.clear_snapshots();
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

async fn get_persisted_catalog_and_snapshots_for_writer(
    object_store: Arc<dyn ObjectStore>,
    writer_identifier_prefix: &str,
) -> Result<(Arc<Catalog>, Vec<PersistedSnapshot>)> {
    // Create a temporary persister to load snapshot files and catalog
    let persister = Persister::new(Arc::clone(&object_store), writer_identifier_prefix);
    let persisted_snapshots = persister
        .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
        .await
        .context("failed to load snapshots for replicated write buffer")?;
    // Attempt to load the catalog for the replica in a retry loop. This is for the scenario
    // where two writers are started at the same time, and the catalog may not be persisted
    // yet in the other replicated writer when this code runs. So, the retry only happens when
    // there are no catalogs found for the replica.
    loop {
        let catalog = match persister.load_catalog().await {
            Ok(Some(persisted)) => Ok(Arc::new(Catalog::from_inner(persisted))),
            Ok(None) => {
                warn!(
                    from_writer_id = writer_identifier_prefix,
                    "there was no catalog for replicated write buffer, this may mean that it has \
                            not been persisted to object store yet, or that the writer-id of the \
                            replica was not specified correctly, will retry in {} second(s)",
                    REPLICA_CATALOG_RETRY_INTERVAL_SECONDS
                );
                tokio::time::sleep(Duration::from_secs(REPLICA_CATALOG_RETRY_INTERVAL_SECONDS))
                    .await;
                continue;
            }
            Err(error) => {
                error!(
                    from_writer_id = writer_identifier_prefix,
                    %error,
                    "error when attempting to load catalog from replicated write buffer from \
                    object store, will retry"
                );
                Err(error)
            }
        }
        .context(
            "received error from object store when accessing catalog for replica, \
                    please see the logs",
        )?;
        break Ok((catalog, persisted_snapshots));
    }
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

async fn cache_parquet_from_snapshot(
    parquet_cache: &Arc<dyn ParquetCacheOracle>,
    snapshot: &PersistedSnapshot,
) {
    let mut cache_notifiers = vec![];
    for ParquetFile { path, .. } in snapshot
        .databases
        .iter()
        .flat_map(|(_, db)| db.tables.iter().flat_map(|(_, tbl)| tbl.iter()))
    {
        let (req, not) = CacheRequest::create(path.as_str().into());
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
                    let wal_path =
                        wal_path(&replicated_buffer.writer_identifier_prefix, wal_number);
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
                                        from_writer_id = replicated_buffer.writer_identifier_prefix,
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
                                from_writer_id = replicated_buffer.writer_identifier_prefix,
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
                                from_writer_id = replicated_buffer.writer_identifier_prefix,
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
                        if let Err(error) = replicated_buffer.reload_snapshots_and_catalog().await {
                            error!(
                                %error,
                                from_writer_id = replicated_buffer.writer_identifier_prefix,
                                "failed to reload snapshots and catalog for replicated write buffer"
                            );
                        }
                    }

                    Err(error) => {
                        error!(
                            %error,
                            from_writer_id = replicated_buffer.writer_identifier_prefix,
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

    use datafusion::{assert_batches_sorted_eq, common::assert_contains};
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::{
        distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
        parquet_cache::test_cached_obj_store_and_oracle,
    };
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
    use influxdb3_id::{ColumnId, DbId, ParquetFileId, TableId};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{
        CatalogBatch, FieldDataType, Gen1Duration, OrderedCatalogBatch, WalConfig, WalContents,
        WalFileSequenceNumber, WalOp,
    };
    use influxdb3_write::{
        persister::Persister,
        test_helpers::WriteBufferTester,
        write_buffer::{WriteBufferImpl, WriteBufferImplArgs},
        ChunkFilter, DistinctCacheManager, LastCacheManager, ParquetFile, PersistedSnapshot,
    };
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::{Attributes, DurationHistogram, Metric, Registry};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use observability_deps::tracing::debug;
    use schema::InfluxColumnType;

    use crate::{
        replica::{
            CreateReplicasArgs, CreateReplicatedBufferArgs, Replicas, ReplicatedBuffer,
            REPLICA_TTBR_METRIC,
        },
        test_helpers::{
            chunks_to_record_batches, do_writes, make_exec, verify_snapshot_count, TestWrite,
        },
    };

    use super::ReplicatedCatalog;

    // TODO - this needs to be addressed, see:
    // https://github.com/influxdata/influxdb_pro/issues/375
    #[test_log::test(tokio::test)]
    async fn replay_and_replicate_other_wal() {
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
        verify_snapshot_count(1, Arc::clone(&obj_store), primary_id).await;

        // Spin up a replicated buffer:
        let replica = ReplicatedBuffer::new(CreateReplicatedBufferArgs {
            replica_order: 0,
            object_store: Arc::clone(&obj_store),
            writer_identifier_prefix: primary_id.to_string(),
            last_cache: primary.last_cache_provider(),
            distinct_cache: primary.distinct_cache_provider(),
            replication_interval: Duration::from_millis(10),
            metric_registry: Arc::new(Registry::new()),
            parquet_cache: None,
            catalog: Arc::new(Catalog::new(
                "replica-host".into(),
                "replica-instance".into(),
            )),
            time_provider,
            wal: None,
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
        verify_snapshot_count(2, Arc::clone(&obj_store), primary_id).await;

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

    #[tokio::test]
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
        // Spin up two primary write buffers to do some writes and generate files in an object store:
        let primary_ids = ["spock", "tuvok"];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
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
        let replicas = Replicas::new(CreateReplicasArgs {
            last_cache: LastCacheProvider::new_from_catalog(primaries["spock"].catalog()).unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                primaries["spock"].catalog(),
            )
            .unwrap(),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            writer_ids: primary_ids.iter().map(|s| s.to_string()).collect(),
            parquet_cache: None,
            catalog: primaries["spock"].catalog(),
            time_provider,
            wal: None,
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
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // Spin up a primary write buffer to do some writes and generate files in an object store:
        let primary = setup_primary(
            "newton",
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1_000,
            },
            Arc::<MockProvider>::clone(&time_provider),
        )
        .await;

        // Spin up a replicated buffer:
        let metric_registry = Arc::new(Registry::new());
        let replication_interval_ms = 50;
        Replicas::new(CreateReplicasArgs {
            // just using the catalog from primary for caches since they aren't used:
            last_cache: LastCacheProvider::new_from_catalog(primary.catalog()).unwrap(),
            distinct_cache: DistinctCacheProvider::new_from_catalog(
                Arc::<MockProvider>::clone(&time_provider),
                primary.catalog(),
            )
            .unwrap(),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::clone(&metric_registry),
            replication_interval: Duration::from_millis(replication_interval_ms),
            writer_ids: vec!["newton".to_string()],
            parquet_cache: None,
            catalog: Arc::new(Catalog::new("replica".into(), "replica".into())),
            time_provider: Arc::<MockProvider>::clone(&time_provider),
            wal: None,
        })
        .await
        .unwrap();

        // set the time provider before writing to primary... this will be the persist_time_ms in
        // the WAL files created by the primary:
        time_provider.set(Time::from_timestamp_millis(1_000).unwrap());

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

        // set the time provider before doing replication so that the replica's "now" time is in
        // advance of the persist time of the WAL files it is replaying:
        time_provider.set(Time::from_timestamp_millis(1_100).unwrap());

        // sleep for replicas to replicate:
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check the metric registry:
        let metric = metric_registry
            .get_instrument::<Metric<DurationHistogram>>(REPLICA_TTBR_METRIC)
            .expect("get the metric");
        let ttbr_ms = metric
            .get_observer(&Attributes::from(&[("from_writer_id", "newton")]))
            .expect("failed to get observer")
            .fetch();
        debug!(?ttbr_ms, "ttbr metric for writer");
        assert_eq!(ttbr_ms.sample_count(), 3);
        assert_eq!(ttbr_ms.total, Duration::from_millis(200));
        assert!(ttbr_ms
            .buckets
            .iter()
            .any(|bucket| bucket.le == Duration::from_millis(100) && bucket.count == 2));
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
        // spin up two primary write buffers:
        let primary_ids = ["skinner", "chalmers"];
        let mut primaries = HashMap::new();
        for p in primary_ids {
            let primary = setup_primary(
                p,
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
        verify_snapshot_count(1, Arc::clone(&obj_store), "skinner").await;
        verify_snapshot_count(1, Arc::clone(&obj_store), "chalmers").await;

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
            let catalog = Arc::new(Catalog::new("replica".into(), "replica".into()));
            let replicas = Replicas::new(CreateReplicasArgs {
                // could load a new catalog from the object store, but it is easier to just re-use
                // skinner's:
                last_cache: LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap(),
                distinct_cache: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&time_provider),
                    Arc::clone(&catalog),
                )
                .unwrap(),
                object_store: Arc::clone(&cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                writer_ids: primary_ids.iter().map(|s| s.to_string()).collect(),
                parquet_cache: Some(parquet_cache),
                catalog,
                time_provider,
                wal: None,
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
            let replicas = Replicas::new(CreateReplicasArgs {
                // like above, just re-use skinner's catalog for ease:
                last_cache: LastCacheProvider::new_from_catalog(primaries["skinner"].catalog())
                    .unwrap(),
                distinct_cache: DistinctCacheProvider::new_from_catalog(
                    Arc::clone(&time_provider),
                    primaries["skinner"].catalog(),
                )
                .unwrap(),
                object_store: Arc::clone(&non_cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                writer_ids: primary_ids.iter().map(|s| s.to_string()).collect(),
                parquet_cache: None,
                catalog: Arc::new(Catalog::new("replica".into(), "replica".into())),
                time_provider,
                wal: None,
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

    #[test]
    fn map_wal_content_for_replica_existing_db_new_table() {
        // setup two catalogs, one as the "primary" i.e. local catalog that is shared between a
        // local write buffer as well as replicas and a compactor, and one as a "replica", that is
        // mapped onto the local primary
        let primary = create::catalog("a");
        let replica = create::catalog("b");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // get the DbId of the "foo" database as it is represented _on the replica_:
        let db_id = replica.db_name_to_id("foo").unwrap();
        // fabricate some WalContents that would originate from the "b" replica buffer that contain
        // a single CreateTable operation for the table "pow" that does not exist locally:
        let t1_col_id = ColumnId::new();
        let wal_content = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    "foo",
                    TableId::new(),
                    "pow",
                    [
                        influxdb3_wal::create::field_def(t1_col_id, "t1", FieldDataType::Tag),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f1",
                            FieldDataType::Boolean,
                        ),
                    ],
                    [t1_col_id],
                )],
                0,
            )],
        );
        // check the replicated catalog's id map before we map the above wal content
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // do the mapping, which will allocate a new ID for the "pow" table locally:
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content).unwrap();
        // check the replicated catalog's id map again, which will contain an entry for the new table:
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // check the mapped wal contents, which should now use the local IDs for DB/tables
        insta::with_settings!({ description => "mapped WAL content for local catalog"}, {
            insta::assert_yaml_snapshot!(mapped_wal_content);
        });
        // apply the mapped catalog batch to the primary catalog, as it would be done during replay:
        primary
            .apply_catalog_batch(&mapped_wal_content.ops[0].as_catalog().cloned().unwrap())
            .unwrap();
        // check for the new table definition in the local primary catalog after the mapped batch
        // was applied:
        let db = primary.db_schema("foo").unwrap();
        let tbl = db.table_definition("pow").unwrap();
        insta::with_settings!({
            description => "table definition in primary catalog after mapping and applying replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(tbl);
        });
    }

    /// note that there isn't really a way to just do new db right now.
    #[test]
    fn map_wal_content_for_replica_new_db_new_table() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // create a new db and table id as if they were on the replica:
        let db_id = DbId::new();
        let table_id = TableId::new();
        let t1_col_id = ColumnId::new();
        let wal_content = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::catalog_batch_op(
                db_id,
                "sup",
                0,
                [influxdb3_wal::create::create_table_op(
                    db_id,
                    "sup",
                    table_id,
                    "dog",
                    [
                        influxdb3_wal::create::field_def(t1_col_id, "t1", FieldDataType::Tag),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f1",
                            FieldDataType::Float,
                        ),
                        influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "time",
                            FieldDataType::Timestamp,
                        ),
                    ],
                    [t1_col_id],
                )],
                0,
            )],
        );
        replica
            .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
            .expect("catalog batch should apply successfully on replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content).unwrap();
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(&mapped_wal_content.ops[0].as_catalog().cloned().unwrap())
            .unwrap();
        let db = primary.db_schema("sup").unwrap();
        insta::with_settings!({
            description => "database schema in primary catalog after mapping and applying replica \
            WAL content, should include table 'dog'"
        }, {
            insta::assert_yaml_snapshot!(db);
        });
    }

    #[test]
    fn map_wal_content_for_replica_new_db_new_table_already_on_local() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // create a new db and table as if they were on the local primary and apply it to the primary:
        let (primary_db_id, primary_table_id) = {
            let db_id = DbId::new();
            let table_id = TableId::new();
            let t1_col_id = ColumnId::new();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "fizz",
                    0,
                    [influxdb3_wal::create::create_table_op(
                        db_id,
                        "fizz",
                        table_id,
                        "buzz",
                        [
                            influxdb3_wal::create::field_def(t1_col_id, "t1", FieldDataType::Tag),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "f1",
                                FieldDataType::Float,
                            ),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "time",
                                FieldDataType::Timestamp,
                            ),
                        ],
                        [t1_col_id],
                    )],
                    0,
                )],
            );
            primary
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("catalog batch should apply successfully on primary catalog");
            (db_id, table_id)
        };
        // now do the same thing as if the db/table were created separately on the replica:
        let (replica_db_id, replica_table_id, replica_wal_content) = {
            let db_id = DbId::new();
            let table_id = TableId::new();
            let t1_col_id = ColumnId::new();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "fizz",
                    0,
                    [influxdb3_wal::create::create_table_op(
                        db_id,
                        "fizz",
                        table_id,
                        "buzz",
                        [
                            influxdb3_wal::create::field_def(t1_col_id, "t1", FieldDataType::Tag),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "f1",
                                FieldDataType::Float,
                            ),
                            influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "time",
                                FieldDataType::Timestamp,
                            ),
                        ],
                        [t1_col_id],
                    )],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("catalog batch should apply successfully on replica catalog");
            (db_id, table_id, wal_content)
        };
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .unwrap();
        // the replicated catalog ops would not result in changes, so they are ignored:
        assert!(mapped_wal_content.ops.is_empty());
        let id_map = replicated_catalog.id_map.lock().clone();
        assert_eq!(primary_db_id, id_map.map_db_id(&replica_db_id).unwrap());
        assert_eq!(
            primary_table_id,
            id_map.map_table_id(&replica_table_id).unwrap()
        );
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
    }

    #[test]
    fn map_wal_content_for_replica_field_additions() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // there is a db called "foo" and a table called "bar" already, but get their IDs on the
        // replica's catalog, so we can use the ID map to map them onto the primary
        let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let wal_content = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [influxdb3_wal::create::add_fields_op(
                    db_id,
                    "foo",
                    table_id,
                    "bar",
                    [influxdb3_wal::create::field_def(
                        ColumnId::new(),
                        "f4",
                        FieldDataType::Float,
                    )],
                )],
                0,
            )],
        );
        replica
            .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
            .expect("catalog batch should apply successfully to the replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content).unwrap();
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // apply the mapped wal content to the primary:
        primary
            .apply_catalog_batch(&mapped_wal_content.ops[0].as_catalog().cloned().unwrap())
            .unwrap();
        let db = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema in primary catalog after mapping and applying replica \
            WAL content, table 'bar' should include field 'f4'"
        }, {
            insta::assert_yaml_snapshot!(db);
        });
    }

    #[test]
    fn map_wal_content_for_replica_field_additions_already_on_primary() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // perform a set of field additions on the primary, and then separately on the replica.
        let (primary_db_id, primary_table_id) = {
            let (db_id, db_schema) = primary.db_id_and_schema("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f4",
                            FieldDataType::Float,
                        )],
                    )],
                    0,
                )],
            );
            primary
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("catalog batch should apply on primary");
            (db_id, table_id)
        };
        let (replica_db_id, replica_table_id, replica_wal_content) = {
            let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f4",
                            FieldDataType::Float,
                        )],
                    )],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("catalog batch should apply on primary");
            (db_id, table_id, wal_content)
        };
        // the same field additions have been made on both primary and replica independently, now
        // check the id map in the replicated catalog before mapping the wal content from the
        // replica onto the primary
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .unwrap();
        // the catalog op would not result in changes, so it is ignored:
        assert!(mapped_wal_content.ops.is_empty());
        let id_map = replicated_catalog.id_map.lock().clone();
        assert_eq!(primary_db_id, id_map.map_db_id(&replica_db_id).unwrap());
        assert_eq!(
            primary_table_id,
            id_map.map_table_id(&replica_table_id).unwrap()
        );
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // check the structure of the db schema in primary to ensure only one "f4" column is there:
        let db = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            sort_maps => true,
            description => "db schema in primary after applying mapped replica batch, there should \
            be a single field 'f4' in the 'bar' table."
        }, {
            insta::assert_yaml_snapshot!(db);
        });
    }

    #[test]
    fn map_wal_content_for_replica_last_cache_create_and_delete() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // create a last cache on the replica:
        let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
        let (table_id, table_def) = db_schema.table_id_and_definition("bar").unwrap();
        let t1_col_id = table_def.column_name_to_id("t1").unwrap();
        let wal_content = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [influxdb3_wal::create::create_last_cache_op_builder(
                    table_id,
                    "bar",
                    "test_cache",
                    [t1_col_id],
                )
                .build()],
                0,
            )],
        );
        replica
            .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
            .expect("catalog batch should apply successfully on replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map before mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content).unwrap();
        let id_map = replicated_catalog.id_map.lock().clone();
        // NOTE: this wont have changed, unless we give last caches IDs
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(&mapped_wal_content.ops[0].as_catalog().cloned().unwrap())
            .unwrap();
        let db = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema with table 'bar' that now has a last cache definition \
            from the replica"
        }, {
            insta::assert_yaml_snapshot!(db);
        });
        // now delete the last cache on the replica:
        let wal_content = influxdb3_wal::create::wal_contents(
            (0, 1, 0),
            [influxdb3_wal::create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [influxdb3_wal::create::delete_last_cache_op(
                    table_id,
                    "bar",
                    "test_cache",
                )],
                0,
            )],
        );
        replica
            .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
            .expect("catalog batch to delete last cache should apply on replica catalog");
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content).unwrap();
        primary
            .apply_catalog_batch(&mapped_wal_content.ops[0].as_catalog().cloned().unwrap())
            .expect("mapped catalog batch should apply on primary to delete last cache");
        let db = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema with table 'bar' that no longer has a last cache"
        }, {
            insta::assert_yaml_snapshot!(db);
        });
    }

    #[test]
    fn map_wal_content_for_replica_last_cache_create_already_on_primary() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // create the same last cache on both primary and replica:
        {
            let (db_id, db_schema) = primary.db_id_and_schema("foo").unwrap();
            let (table_id, table_def) = db_schema.table_id_and_definition("bar").unwrap();
            let t1_col_id = table_def.column_name_to_id("t1").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::create_last_cache_op_builder(
                        table_id,
                        "bar",
                        "test_cache",
                        [t1_col_id],
                    )
                    .build()],
                    0,
                )],
            );
            primary
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to primary to create last cache");
        }
        let replica_wal_content = {
            let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
            let (table_id, table_def) = db_schema.table_id_and_definition("bar").unwrap();
            let t1_col_id = table_def.column_name_to_id("t1").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::create_last_cache_op_builder(
                        table_id,
                        "bar",
                        "test_cache",
                        [t1_col_id],
                    )
                    .build()],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to replica to create last cache");
            wal_content
        };
        let mapped_wal_content = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .unwrap();
        // the replicated catalog op would not result in a change, so it is ignored:
        assert!(mapped_wal_content.ops.is_empty());
        // check the structure of the primary db schema to ensure it has only a single last cache:
        let db_before_applying = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema for 'foo' db before applying the mapped catalog \
            batch from the replica; it should have a 'bar' table containing a single last cache \
            definition"
        }, {
            insta::assert_yaml_snapshot!(db_before_applying);
        });
        // check structure of the db schema on primary to ensure only a single last cache:
        let db_after_applying = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema for 'foo' db after applying the mapped catalog batch \
            from the replica; it should still have a 'bar' table containing just a single last \
            cache definition"
        }, {
            insta::assert_yaml_snapshot!(db_after_applying);
        });
    }

    #[test]
    fn map_wal_content_for_replica_with_incompatible_last_cache_def() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // Simulate a WalOp on each writer that creates a last cache using the same cache
        // name, but with different cache configurations.
        //
        // First on the primary:
        {
            let (db_id, db_schema) = primary.db_id_and_schema("foo").unwrap();
            let (table_id, table_def) = db_schema.table_id_and_definition("bar").unwrap();
            let t1_col_id = table_def.column_name_to_id("t1").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::create_last_cache_op_builder(
                        table_id,
                        "bar",
                        "test_cache",
                        [t1_col_id],
                    )
                    .build()],
                    0,
                )],
            );
            primary
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to primary to create last cache");
        }
        // now on the replica:
        let replica_wal_content = {
            let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::create_last_cache_op_builder(
                        table_id,
                        "bar",
                        "test_cache",
                        // this cache has a different set of key cols:
                        [],
                    )
                    .build()],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to replica to create last cache");
            wal_content
        };
        let err = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .expect_err(
                "mapping the wal content should fail due to incompatible last cache definitions",
            );
        assert_contains!(
            err.to_string(),
            "WAL contained a CreateLastCache operation with a last cache name that already \
            exists in the local catalog, but is not compatible"
        );
    }

    #[test]
    fn map_wal_content_for_replica_last_cache_create_unseen_table() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // create a wal op that creates a table and then a last cache for it on the replica:
        let mut replica_wal_content = {
            let db_id = replica.db_name_to_id("foo").unwrap();
            let table_id = TableId::new();
            let fruits_col_id = ColumnId::new();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [
                        influxdb3_wal::create::create_table_op(
                            db_id,
                            "foo",
                            table_id,
                            "baz",
                            [influxdb3_wal::create::field_def(
                                fruits_col_id,
                                "fruits",
                                FieldDataType::Tag,
                            )],
                            [fruits_col_id],
                        ),
                        influxdb3_wal::create::create_last_cache_op_builder(
                            table_id,
                            "bar",
                            "test_cache",
                            // this cache has a different set of key cols:
                            [],
                        )
                        .build(),
                    ],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch with table create and last cache create on replica");
            wal_content
        };
        // now remove the create table op, simulating a corrupted wal:
        replica_wal_content = WalContents {
            ops: replica_wal_content
                .ops
                .into_iter()
                .map(|op| match op {
                    WalOp::Write(_) => op,
                    WalOp::Catalog(catalog_batch) => {
                        let sequence = catalog_batch.sequence_number();
                        let batch = catalog_batch.into_batch();
                        WalOp::Catalog(OrderedCatalogBatch::new(
                            CatalogBatch {
                                ops: batch.ops.into_iter().skip(1).collect(),
                                ..batch
                            },
                            sequence,
                        ))
                    }
                    WalOp::Noop(_) => op,
                })
                .collect(),
            ..replica_wal_content
        };
        let err = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .expect_err("mapping wal contents should fail");
        assert_contains!(
            err.to_string(),
            "failed to map WAL contents: last cache definition contained invalid table id"
        );
    }

    #[test]
    fn map_wal_with_incompatible_field_additions() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // add a field "f4" as a string column on the primary:
        {
            let (db_id, db_schema) = primary.db_id_and_schema("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f4",
                            FieldDataType::String,
                        )],
                    )],
                    0,
                )],
            );
            primary
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to primary");
        }
        // now add the same "f4", but as a float, to the replica:
        let replica_wal_content = {
            let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [influxdb3_wal::create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [influxdb3_wal::create::field_def(
                            ColumnId::new(),
                            "f4",
                            FieldDataType::Float,
                        )],
                    )],
                    0,
                )],
            );
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to replica to make sure it is valid");
            wal_content
        };
        let err = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .unwrap_err()
            .to_string();
        assert_contains!(err, "Field type mismatch on table bar column f4");
    }

    #[test]
    fn map_wal_with_field_additions_for_invalid_table() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // Perform two WAL operations on the replica to create a table, then add fields to it:
        let mut replica_wal_content = {
            let db_id = replica.db_name_to_id("foo").unwrap();
            let table_id = TableId::new();
            let wal_content = influxdb3_wal::create::wal_contents(
                (0, 1, 0),
                [influxdb3_wal::create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [
                        influxdb3_wal::create::create_table_op(
                            db_id,
                            "foo",
                            table_id,
                            "phi",
                            [influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "zeta",
                                FieldDataType::Integer,
                            )],
                            [],
                        ),
                        influxdb3_wal::create::add_fields_op(
                            db_id,
                            "foo",
                            table_id,
                            "phi",
                            [influxdb3_wal::create::field_def(
                                ColumnId::new(),
                                "theta",
                                FieldDataType::UInteger,
                            )],
                        ),
                    ],
                    0,
                )],
            );
            // apply the wal content to the replica to make sure it is valid:
            replica
                .apply_catalog_batch(&wal_content.ops[0].as_catalog().cloned().unwrap())
                .expect("apply catalog batch to replica to check its validity");
            wal_content
        };
        // now remove the create table op, simulating a corrupted wal:
        replica_wal_content = WalContents {
            ops: replica_wal_content
                .ops
                .into_iter()
                .map(|op| match op {
                    WalOp::Write(_) => op,
                    WalOp::Catalog(cat) => {
                        let sequence = cat.sequence_number();
                        let batch = cat.into_batch();
                        WalOp::Catalog(OrderedCatalogBatch::new(
                            CatalogBatch {
                                ops: batch.ops.into_iter().skip(1).collect(),
                                ..batch
                            },
                            sequence,
                        ))
                    }
                    WalOp::Noop(_) => op,
                })
                .collect(),
            ..replica_wal_content
        };
        let err = replicated_catalog
            .map_wal_contents(replica_wal_content)
            .expect_err("applying corrupt wal content to primary should fail");

        assert_contains!(
            err.to_string(),
            "failed to map WAL contents: Table phi not in DB schema for foo"
        );
    }

    #[test]
    fn map_persisted_snapshot_for_replica() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        let (db_id, db_schema) = replica.db_id_and_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let snapshot = create::persisted_snapshot("host-primary")
            .table(
                db_id,
                table_id,
                create::parquet_file(ParquetFileId::new()).build(),
            )
            .build();
        insta::with_settings!({
            sort_maps => true,
            description => "persisted snapshot as it was created on the replica, before mapping"
        }, {
            // use JSON snapshots since persisted snapshots are actually persisted as JSON:
            insta::assert_json_snapshot!(snapshot);
        });
        let mapped_snapshot = replicated_catalog.map_snapshot_contents(snapshot).unwrap();
        insta::with_settings!({
            sort_maps => true,
            description => "persisted snapshot after mapping, as intended for the primary"
        }, {
            insta::assert_json_snapshot!(mapped_snapshot);
        });
    }

    #[test]
    fn map_persisted_snapshot_with_invalid_ids_fails() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        let snapshot = create::persisted_snapshot("host-replica")
            .table(
                DbId::new(),
                TableId::new(),
                create::parquet_file(ParquetFileId::new()).build(),
            )
            .build();
        let err = replicated_catalog
            .map_snapshot_contents(snapshot)
            .expect_err("snapshot with unseen database id should fail to map");
        assert_contains!(
            err.to_string(),
            "invalid database id in persisted snapshot: 2"
        );
        let db_id = replica.db_name_to_id("foo").unwrap();
        let snapshot = create::persisted_snapshot("host-replica")
            .table(
                db_id,
                TableId::new(),
                create::parquet_file(ParquetFileId::new()).build(),
            )
            .build();
        let err = replicated_catalog
            .map_snapshot_contents(snapshot)
            .expect_err("snapshot with unseen table id should fail to map");
        assert_contains!(err.to_string(), "invalid table id in persisted snapshot: 3");
    }

    async fn setup_primary(
        writer_id: &str,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Arc<WriteBufferImpl> {
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), writer_id));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
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
        let (db_id, db_schema) = replicas.catalog().db_id_and_schema(db).unwrap();
        let table_id = db_schema.table_name_to_id(tbl).unwrap();
        for _ in 0..10 {
            let persisted_files = replicas.parquet_files(db_id, table_id);
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
        let (db_id, db_schema) = replica.catalog().db_id_and_schema(db).unwrap();
        let table_id = db_schema.table_name_to_id(tbl).unwrap();
        let mut most_files_found = 0;
        for _ in 0..10 {
            let persisted_files = replica.parquet_files(db_id, table_id);
            most_files_found = most_files_found.max(persisted_files.len());
            if persisted_files.len() >= expected_file_count {
                return persisted_files;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("no files were persisted after several tries, most found: {most_files_found}");
    }

    mod create {
        use influxdb3_catalog::catalog::CatalogSequenceNumber;
        use influxdb3_id::{ColumnId, ParquetFileId, SerdeVecMap};
        use influxdb3_wal::SnapshotSequenceNumber;
        use influxdb3_write::DatabaseTables;

        use super::*;

        pub(super) fn table<C, N, SK>(name: &str, cols: C, series_key: SK) -> TableDefinition
        where
            C: IntoIterator<Item = (ColumnId, N, InfluxColumnType)>,
            N: Into<Arc<str>>,
            SK: IntoIterator<Item = ColumnId>,
        {
            TableDefinition::new(
                TableId::new(),
                name.into(),
                cols.into_iter()
                    .map(|(id, name, ty)| (id, name.into(), ty))
                    .collect(),
                series_key.into_iter().collect(),
            )
            .expect("create table definition")
        }

        pub(super) fn catalog(name: &str) -> Arc<Catalog> {
            let writer_id = format!("host-{name}").as_str().into();
            let instance_name = format!("instance-{name}").as_str().into();
            let cat = Catalog::new(writer_id, instance_name);
            let t1_col_id = ColumnId::new();
            let t2_col_id = ColumnId::new();
            let tbl = table(
                "bar",
                [
                    (t1_col_id, "t1", InfluxColumnType::Tag),
                    (t2_col_id, "t2", InfluxColumnType::Tag),
                    (
                        ColumnId::new(),
                        "f1",
                        InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                    ),
                ],
                [t1_col_id, t2_col_id],
            );
            let mut db = DatabaseSchema::new(DbId::new(), "foo".into());
            db.table_map
                .insert(tbl.table_id, Arc::clone(&tbl.table_name));
            db.tables.insert(tbl.table_id, Arc::new(tbl));
            cat.insert_database(db);
            cat.into()
        }

        pub(super) struct ParquetFileBuilder {
            id: ParquetFileId,
            path: Option<String>,
            size_bytes: Option<u64>,
            row_count: Option<u64>,
            chunk_time: Option<i64>,
            min_time: Option<i64>,
            max_time: Option<i64>,
        }

        impl ParquetFileBuilder {
            pub(super) fn build(self) -> ParquetFile {
                ParquetFile {
                    id: self.id,
                    path: self.path.unwrap_or_default(),
                    size_bytes: self.size_bytes.unwrap_or_default(),
                    row_count: self.row_count.unwrap_or_default(),
                    chunk_time: self.chunk_time.unwrap_or_default(),
                    min_time: self.min_time.unwrap_or_default(),
                    max_time: self.max_time.unwrap_or_default(),
                }
            }
        }

        pub(super) fn parquet_file(id: ParquetFileId) -> ParquetFileBuilder {
            ParquetFileBuilder {
                id,
                path: None,
                size_bytes: None,
                row_count: None,
                chunk_time: None,
                min_time: None,
                max_time: None,
            }
        }

        pub(super) struct PersistedSnapshotBuilder {
            writer_id: String,
            next_file_id: ParquetFileId,
            next_db_id: DbId,
            next_table_id: TableId,
            next_column_id: ColumnId,
            snapshot_sequence_number: Option<SnapshotSequenceNumber>,
            wal_file_sequence_number: Option<WalFileSequenceNumber>,
            catalog_sequence_number: Option<CatalogSequenceNumber>,
            parquet_size_bytes: Option<u64>,
            row_count: Option<u64>,
            min_time: Option<i64>,
            max_time: Option<i64>,
            databases: SerdeVecMap<DbId, DatabaseTables>,
        }

        impl PersistedSnapshotBuilder {
            pub(super) fn build(self) -> PersistedSnapshot {
                PersistedSnapshot {
                    writer_id: self.writer_id,
                    next_file_id: self.next_file_id,
                    next_db_id: self.next_db_id,
                    next_table_id: self.next_table_id,
                    next_column_id: self.next_column_id,
                    snapshot_sequence_number: self.snapshot_sequence_number.unwrap_or_default(),
                    wal_file_sequence_number: self.wal_file_sequence_number.unwrap_or_default(),
                    catalog_sequence_number: self.catalog_sequence_number.unwrap_or_default(),
                    parquet_size_bytes: self.parquet_size_bytes.unwrap_or_default(),
                    row_count: self.row_count.unwrap_or_default(),
                    min_time: self.min_time.unwrap_or_default(),
                    max_time: self.max_time.unwrap_or_default(),
                    databases: self.databases,
                }
            }

            pub(super) fn table(
                mut self,
                db_id: DbId,
                table_id: TableId,
                parquet_file: ParquetFile,
            ) -> Self {
                self.databases
                    .entry(db_id)
                    .or_default()
                    .tables
                    .entry(table_id)
                    .or_default()
                    .push(parquet_file);
                self
            }
        }

        pub(super) fn persisted_snapshot(writer_id: &str) -> PersistedSnapshotBuilder {
            PersistedSnapshotBuilder {
                writer_id: writer_id.into(),
                next_file_id: ParquetFileId::next_id(),
                next_db_id: DbId::next_id(),
                next_table_id: TableId::next_id(),
                next_column_id: ColumnId::next_id(),
                snapshot_sequence_number: None,
                wal_file_sequence_number: None,
                catalog_sequence_number: None,
                parquet_size_bytes: None,
                row_count: None,
                min_time: None,
                max_time: None,
                databases: Default::default(),
            }
        }
    }
}
