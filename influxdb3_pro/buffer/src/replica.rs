use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::Utc;
use data_types::{ChunkId, ChunkOrder, PartitionKey, TableId as IoxTableId, TransitionPartitionId};
use datafusion::{execution::object_store::ObjectStoreUrl, logical_expr::Expr};
use futures::future::try_join_all;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::{pro::CatalogIdMap, Catalog};
use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::HostSnapshotMarker;
use influxdb3_wal::{
    object_store::wal_path, serialize::verify_file_type_and_deserialize, SnapshotDetails,
    WalContents, WalFileSequenceNumber,
};
use influxdb3_write::{
    chunk::BufferChunk,
    last_cache::LastCacheProvider,
    parquet_cache::{CacheRequest, ParquetCacheOracle},
    paths::SnapshotInfoFilePath,
    persister::{Persister, DEFAULT_OBJECT_STORE_URL},
    write_buffer::{
        parquet_chunk_from_file, persisted_files::PersistedFiles, queryable_buffer::BufferState,
        N_SNAPSHOTS_TO_LOAD_ON_START,
    },
    DatabaseTables, ParquetFile, PersistedSnapshot,
};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, NoColumnRanges},
    QueryChunk,
};
use metric::{Attributes, Registry, U64Gauge};
use object_store::{path::Path, ObjectStore};
use observability_deps::tracing::{debug, error, info};
use parking_lot::RwLock;
use schema::Schema;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("unexpected replication error: {0}")]
    Unexpected(#[from] anyhow::Error),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ReplicationConfig {
    pub(crate) interval: Duration,
    pub(crate) hosts: Vec<String>,
}

impl ReplicationConfig {
    pub fn new(interval: Duration, hosts: Vec<String>) -> Self {
        Self { interval, hosts }
    }

    pub fn hosts(&self) -> &[String] {
        &self.hosts
    }
}

#[derive(Debug)]
pub(crate) struct Replicas {
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    last_cache: Arc<LastCacheProvider>,
    replicas: Vec<Arc<ReplicatedBuffer>>,
}

pub(crate) struct CreateReplicasArgs {
    pub last_cache: Arc<LastCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub metric_registry: Arc<Registry>,
    pub replication_interval: Duration,
    pub hosts: Vec<String>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub catalog: Arc<Catalog>,
    pub compacted_data: Option<Arc<CompactedData>>,
}

impl Replicas {
    pub(crate) async fn new(
        CreateReplicasArgs {
            last_cache,
            object_store,
            metric_registry,
            replication_interval,
            hosts,
            parquet_cache,
            catalog,
            compacted_data,
        }: CreateReplicasArgs,
    ) -> Result<Self> {
        let mut replicas = vec![];
        for (i, host_identifier_prefix) in hosts.into_iter().enumerate() {
            let object_store = Arc::clone(&object_store);
            let last_cache = Arc::clone(&last_cache);
            let metric_registry = Arc::clone(&metric_registry);
            let parquet_cache = parquet_cache.clone();
            let catalog = Arc::clone(&catalog);
            let compacted_data = compacted_data.clone();
            info!(%host_identifier_prefix, "creating replicated buffer for host");
            replicas.push(
                ReplicatedBuffer::new(CreateReplicatedBufferArgs {
                    replica_order: i as i64,
                    object_store,
                    host_identifier_prefix,
                    last_cache,
                    replication_interval,
                    metric_registry,
                    compacted_data,
                    parquet_cache,
                    catalog,
                })
                .await?,
            )
        }
        Ok(Self {
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            last_cache,
            replicas,
        })
    }

    pub(crate) fn object_store_url(&self) -> ObjectStoreUrl {
        self.object_store_url.clone()
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }

    pub(crate) fn catalog(&self) -> Arc<Catalog> {
        self.replicas[0].catalog()
    }

    pub(crate) fn last_cache(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }

    pub(crate) fn parquet_files(&self, db_id: DbId, tbl_id: TableId) -> Vec<ParquetFile> {
        let mut files = vec![];
        for replica in &self.replicas {
            files.append(&mut replica.parquet_files(db_id, tbl_id));
        }
        files
    }

    #[cfg(test)]
    fn get_all_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        table_schema: Schema,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut chunks = self
            .get_buffer_chunks(database_name, table_name, &[])
            .unwrap();
        chunks.extend(self.get_persisted_chunks(
            database_name,
            table_name,
            table_schema,
            &[],
            &[],
            0,
        ));
        chunks
    }

    pub(crate) fn get_buffer_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let mut chunks = vec![];
        for replica in &self.replicas {
            chunks.append(&mut replica.get_buffer_chunks(database_name, table_name, filters)?);
        }
        Ok(chunks)
    }

    pub(crate) fn get_persisted_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        table_schema: Schema,
        _filters: &[Expr],
        host_markers: &[Arc<HostSnapshotMarker>],
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        let mut chunks = vec![];
        for replica in &self.replicas {
            let last_parquet_file_id = host_markers.iter().find_map(|marker| {
                if marker.host_id == replica.host_identifier_prefix {
                    Some(marker.next_file_id)
                } else {
                    None
                }
            });

            chunks.append(&mut replica.get_persisted_chunks(
                database_name,
                table_name,
                table_schema.clone(),
                _filters,
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
    host_identifier_prefix: String,
    last_wal_file_sequence_number: Mutex<Option<WalFileSequenceNumber>>,
    buffer: Arc<RwLock<BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    last_cache: Arc<LastCacheProvider>,
    catalog: Arc<ReplicatedCatalog>,
    metrics: ReplicatedBufferMetrics,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    compacted_data: Option<Arc<CompactedData>>,
}

#[derive(Debug)]
struct ReplicatedCatalog {
    catalog: Arc<Catalog>,
    id_map: Arc<parking_lot::Mutex<CatalogIdMap>>,
}

impl ReplicatedCatalog {
    /// Create a replicated catalog from a primary, i.e., local catalog, and the catalog of another
    /// host that is being replicated.
    fn new(primary: Arc<Catalog>, replica: Arc<Catalog>) -> Result<Self> {
        let id_map = primary
            .merge(replica)
            .context("unable to merge replica catalog with primary")?;
        Ok(Self {
            catalog: primary,
            id_map: Arc::new(parking_lot::Mutex::new(id_map)),
        })
    }

    fn map_wal_contents(&self, wal_contents: WalContents) -> WalContents {
        self.id_map
            .lock()
            .map_wal_contents(&self.catalog, wal_contents)
    }

    fn map_snapshot_contents(&self, snapshot: PersistedSnapshot) -> PersistedSnapshot {
        let id_map = self.id_map.lock();
        PersistedSnapshot {
            databases: snapshot
                .databases
                .into_iter()
                .map(|(replica_db_id, db_tables)| {
                    let local_db_id = id_map
                        .map_db_id(replica_db_id)
                        .expect("unseen db id from replica");
                    (
                        local_db_id,
                        DatabaseTables {
                            tables: db_tables
                                .tables
                                .into_iter()
                                .map(|(replica_table_id, files)| {
                                    (
                                        id_map
                                            .map_table_id(replica_table_id)
                                            .expect("unseen table id from replica"),
                                        files,
                                    )
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            ..snapshot
        }
    }
}

pub const REPLICA_TTBR_METRIC: &str = "influxdb3_replica_ttbr";

#[derive(Debug)]
struct ReplicatedBufferMetrics {
    replica_ttbr: U64Gauge,
}

pub(crate) struct CreateReplicatedBufferArgs {
    replica_order: i64,
    object_store: Arc<dyn ObjectStore>,
    host_identifier_prefix: String,
    last_cache: Arc<LastCacheProvider>,
    replication_interval: Duration,
    metric_registry: Arc<Registry>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    catalog: Arc<Catalog>,
    compacted_data: Option<Arc<CompactedData>>,
}

impl ReplicatedBuffer {
    pub(crate) async fn new(
        CreateReplicatedBufferArgs {
            replica_order,
            object_store,
            host_identifier_prefix,
            last_cache,
            replication_interval,
            metric_registry,
            parquet_cache,
            catalog,
            compacted_data,
        }: CreateReplicatedBufferArgs,
    ) -> Result<Arc<Self>> {
        let (persisted_catalog, persisted_files) = {
            // Create a temporary persister to load snapshot files and catalog
            let persister = Persister::new(Arc::clone(&object_store), &host_identifier_prefix);
            let persisted_snapshots = persister
                .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
                .await
                .context("failed to load snapshots for replicated host")?;
            let catalog = persister.load_catalog()
                .await
                .with_context(|| format!("unable to load a catalog for host '{host_identifier_prefix}' from object store"))?
                .map(|persisted| Arc::new(Catalog::from_inner(persisted.catalog)))
                .with_context(|| format!("there was no catalog for host '{host_identifier_prefix}'"))?;
            let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
                persisted_snapshots,
            ));
            (catalog, persisted_files)
        };
        let host: Cow<'static, str> = Cow::from(host_identifier_prefix.clone());
        let attributes = Attributes::from([("host", host)]);
        let replica_ttbr = metric_registry
            .register_metric::<U64Gauge>(
                REPLICA_TTBR_METRIC,
                "time to be readable for the data in each replicated host buffer",
            )
            .recorder(attributes);
        let replica_catalog = ReplicatedCatalog::new(Arc::clone(&catalog), persisted_catalog)?;
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(&catalog))));
        let replicated_buffer = Self {
            replica_order,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            host_identifier_prefix,
            last_wal_file_sequence_number: Mutex::new(None),
            buffer,
            persisted_files,
            last_cache,
            metrics: ReplicatedBufferMetrics { replica_ttbr },
            parquet_cache,
            catalog: Arc::new(replica_catalog),
            compacted_data,
        };
        replicated_buffer.replay().await?;
        let replicated_buffer = Arc::new(replicated_buffer);
        background_replication_interval(Arc::clone(&replicated_buffer), replication_interval);

        Ok(replicated_buffer)
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog.catalog)
    }

    pub(crate) fn parquet_files(&self, db_id: DbId, tbl_id: TableId) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_id, tbl_id)
    }

    pub(crate) fn get_buffer_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        self.get_buffer_table_chunks(database_name, table_name, filters)
    }

    pub(crate) fn get_persisted_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        table_schema: Schema,
        _filters: &[Expr],
        last_compacted_parquet_file_id: Option<ParquetFileId>, // only return chunks with a file id > than this
        mut chunk_order_offset: i64, // offset the chunk order by this amount
    ) -> Vec<Arc<dyn QueryChunk>> {
        debug!(%database_name, %table_name, "getting persisted chunks for replicated buffer");
        let Some((db_id, db_schema)) = self.catalog().db_schema_and_id(database_name) else {
            return vec![];
        };
        let Some(table_id) = db_schema.table_name_to_id(table_name) else {
            return vec![];
        };
        let mut files = self.persisted_files.get_files(db_id, table_id);
        debug!(%db_id, %table_id, n_files = files.len(), "got persisted files for database/table");

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
                    &table_schema,
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
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let Some((db_id, db_schema)) = self.catalog().db_schema_and_id(database_name) else {
            return Ok(vec![]);
        };
        let Some((table_id, table_schema)) = db_schema.table_schema_and_id(table_name) else {
            return Ok(vec![]);
        };
        let buffer = self.buffer.read();
        let Some(db_buffer) = buffer.db_to_table.get(&db_id) else {
            return Ok(vec![]);
        };
        let Some(table_buffer) = db_buffer.get(&table_id) else {
            return Ok(vec![]);
        };
        Ok(table_buffer
            .partitioned_record_batches(table_schema.as_arrow(), filters)
            .context("error getting partitioned batches from table buffer")?
            .into_iter()
            .map(|(gen_time, (ts_min_max, batches))| {
                let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &table_schema,
                    Some(ts_min_max),
                    &NoColumnRanges,
                );
                Arc::new(BufferChunk {
                    batches,
                    schema: table_schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::new(
                        IoxTableId::new(0),
                        &PartitionKey::from(gen_time.to_string()),
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

    async fn replay(&self) -> Result<()> {
        let paths = self.load_existing_wal_paths().await?;
        info!(host = %self.host_identifier_prefix, num_wal_files = paths.len(), "replaying WAL files for replica");

        for path in &paths {
            self.replay_wal_file(path).await?;
        }

        if let Some(path) = paths.last() {
            let wal_number =
                WalFileSequenceNumber::try_from(path).context("invalid wal file path")?;
            self.last_wal_file_sequence_number
                .lock()
                .await
                .replace(wal_number);
        }

        Ok(())
    }

    async fn load_existing_wal_paths(&self) -> Result<Vec<Path>> {
        let mut paths = vec![];
        let mut offset: Option<Path> = None;
        let path = Path::from(format!("{host}/wal", host = self.host_identifier_prefix));
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
        paths.sort();

        Ok(paths)
    }

    async fn replay_wal_file(&self, path: &Path) -> Result<()> {
        let obj = self.object_store.get(path).await?;
        let file_written_time = obj.meta.last_modified;
        let file_bytes = obj
            .bytes()
            .await
            .context("failed to collect data for known file into bytes")?;
        let wal_contents = verify_file_type_and_deserialize(file_bytes)
            .context("failed to verify and deserialize wal file contents")?;

        debug!(host = %self.host_identifier_prefix, ?wal_contents, catalog = ?self.catalog, "replay wal file (pre-map)");
        let wal_contents = self.catalog.map_wal_contents(wal_contents);
        debug!(host = %self.host_identifier_prefix, ?wal_contents, "replay wal file (post-map)");

        match wal_contents.snapshot {
            None => self.buffer_wal_contents(wal_contents),
            Some(snapshot_details) => {
                self.buffer_wal_contents_and_handle_snapshots(wal_contents, snapshot_details)
            }
        }

        match Utc::now().signed_duration_since(file_written_time).to_std() {
            Ok(ttbr) => self.metrics.replica_ttbr.set(ttbr.as_millis() as u64),
            Err(message) => error!(%message, "unable to get duration since WAL file was created"),
        }

        Ok(())
    }

    fn buffer_wal_contents(&self, wal_contents: WalContents) {
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        let mut buffer = self.buffer.write();
        buffer.buffer_ops(wal_contents.ops, &self.last_cache);
    }

    fn buffer_wal_contents_and_handle_snapshots(
        &self,
        wal_contents: WalContents,
        snapshot_details: SnapshotDetails,
    ) {
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        // Update the Buffer by invoking the snapshot, to separate data in the buffer that will
        // get cleared by the snapshot, before fetching the snapshot from object store:
        {
            // get the lock inside this block so that it is dropped
            // when it is no longer needed, and is not held accross
            // await points below, which the compiler does not allow
            let mut buffer = self.buffer.write();
            for (_, tbl_map) in buffer.db_to_table.iter_mut() {
                for (_, tbl_buf) in tbl_map.iter_mut() {
                    tbl_buf.snapshot(snapshot_details.end_time_marker);
                }
            }
            buffer.buffer_ops(wal_contents.ops, &self.last_cache);
        }

        let snapshot_path = SnapshotInfoFilePath::new(
            &self.host_identifier_prefix,
            snapshot_details.snapshot_sequence_number,
        );
        let object_store = Arc::clone(&self.object_store);
        let buffer = Arc::clone(&self.buffer);
        let persisted_files = Arc::clone(&self.persisted_files);
        let parquet_cache = self.parquet_cache.clone();
        let replica_catalog = Arc::clone(&self.catalog);
        let compacted_data = self.compacted_data.clone();

        tokio::spawn(async move {
            // Update the persisted files:
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
                        debug!(?snapshot, "map snapshot contents for replicated buffer");
                        let snapshot = replica_catalog.map_snapshot_contents(snapshot);
                        debug!(
                            mapped_snapshot = ?snapshot,
                            "map snapshot contents for replicated buffer (done)"
                        );

                        // Now that the snapshot has been loaded, clear the buffer of the data that
                        // was separated out previously and update the persisted files. If there is
                        // a parquet cache, then load parquet files from the snapshot into the cache
                        // before clearing the buffer, to minimize time holding the buffer lock:
                        if let Some(parquet_cache) = parquet_cache {
                            let mut cache_notifiers = vec![];
                            for ParquetFile { path, .. } in
                                snapshot.databases.iter().flat_map(|(_, db)| {
                                    db.tables.iter().flat_map(|(_, tbl)| tbl.iter())
                                })
                            {
                                let (req, not) = CacheRequest::create(path.as_str().into());
                                parquet_cache.register(req);
                                cache_notifiers.push(not);
                            }
                            try_join_all(cache_notifiers)
                                .await
                                .expect("receive all parquet cache notifications");
                        }
                        let mut buffer = buffer.write();
                        for (_, tbl_map) in buffer.db_to_table.iter_mut() {
                            for (_, tbl_buf) in tbl_map.iter_mut() {
                                tbl_buf.clear_snapshots();
                            }
                        }
                        persisted_files.add_persisted_snapshot_files(snapshot.clone());
                        if let Some(compacted_data) = compacted_data {
                            compacted_data.add_snapshot(snapshot);
                        }
                        break;
                    }
                    Err(error) => {
                        error!(
                            %error,
                            path = ?snapshot_path,
                            "error getting persisted snapshot from replica's object storage"
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        });
    }
}

fn background_replication_interval(
    replicated_buffer: Arc<ReplicatedBuffer>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            // try to fetch new WAL files and buffer them...
            let mut last_wal_number = replicated_buffer.last_wal_file_sequence_number.lock().await;
            if let Some(mut wal_number) = *last_wal_number {
                // Fetch WAL files until a NOT FOUND is encountered or other error:
                'inner: loop {
                    wal_number = wal_number.next();
                    let wal_path = wal_path(&replicated_buffer.host_identifier_prefix, wal_number);
                    match replicated_buffer.replay_wal_file(&wal_path).await {
                        Ok(_) => {
                            info!(
                                host = %replicated_buffer.host_identifier_prefix,
                                path = %wal_path,
                                "replayed WAL file"
                            );
                            last_wal_number.replace(wal_number);
                            // Don't break the inner loop here, since we want to try for more
                            // WAL files if they exist...
                        }
                        Err(error) => {
                            match error {
                                // When the file is not found, we assume that it hasn't been created
                                // yet, so do nothing. Logging NOT_FOUND could get noisy.
                                Error::ObjectStore(object_store::Error::NotFound { .. }) => {}
                                // Otherwise, we log the error:
                                error => {
                                    error!(%error, "failed to fetch and replay next WAL file");
                                }
                            }
                            break 'inner;
                        }
                    }
                }
            } else {
                // If we don't have a last WAL file, we don't know what WAL number to fetch yet, so
                // need to rely on replay to get that. In this case, we need to drop the lock to
                // prevent a deadlock:
                drop(last_wal_number);
                if let Err(error) = replicated_buffer.replay().await {
                    error!(%error, "failed to replay replicated buffer on replication interval");
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
    use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
    use influxdb3_id::{DbId, ParquetFileId, TableId};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{
        CatalogBatch, CatalogOp, FieldDataType, FieldDefinition, Gen1Duration, WalConfig,
        WalContents, WalFileSequenceNumber, WalOp,
    };
    use influxdb3_write::{
        last_cache::LastCacheProvider, parquet_cache::test_cached_obj_store_and_oracle,
        persister::Persister, write_buffer::WriteBufferImpl, ChunkContainer, LastCacheManager,
        ParquetFile, PersistedSnapshot,
    };
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::{Attributes, Metric, Registry, U64Gauge};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
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
            Time::from_timestamp_nanos(0),
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu_items";

        // Do some writes to the primary to trigger snapshot:
        do_writes(
            db_name,
            &primary,
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
            host_identifier_prefix: primary_id.to_string(),
            last_cache: primary.last_cache_provider(),
            replication_interval: Duration::from_millis(10),
            metric_registry: Arc::new(Registry::new()),
            parquet_cache: None,
            catalog: Arc::new(Catalog::new(
                "replica-host".into(),
                "replica-instance".into(),
            )),
            compacted_data: None,
        })
        .await
        .unwrap();

        wait_for_replicated_buffer_persistence(&replica, db_name, tbl_name, 1).await;

        // Check that the replica replayed the primary and contains its data:
        {
            let mut chunks = replica.get_buffer_chunks(db_name, tbl_name, &[]).unwrap();
            chunks.extend(replica.get_persisted_chunks(
                db_name,
                tbl_name,
                chunks[0].schema().clone(),
                &[],
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
            &primary,
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
            let chunks = primary
                .get_table_chunks(db_name, tbl_name, &[], None, &ctx.inner().state())
                .unwrap();
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

        // Check the replica again for the new writes:
        {
            let mut chunks = replica.get_buffer_chunks(db_name, tbl_name, &[]).unwrap();
            chunks.extend(replica.get_persisted_chunks(
                db_name,
                tbl_name,
                chunks[0].schema().clone(),
                &[],
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
                Time::from_timestamp_nanos(0),
            )
            .await;
            primaries.insert(p, primary);
        }

        // Spin up a set of replicated buffers:
        let replicas = Replicas::new(CreateReplicasArgs {
            last_cache: Arc::new(
                LastCacheProvider::new_from_catalog(primaries["spock"].catalog()).unwrap(),
            ),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
            compacted_data: None,
            parquet_cache: None,
            catalog: primaries["spock"].catalog(),
        })
        .await
        .unwrap();
        // write to spock:
        do_writes(
            "foo",
            &primaries["spock"],
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
            &primaries["tuvok"],
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

        let chunks = replicas.get_buffer_chunks("foo", "bar", &[]).unwrap();
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

    #[tokio::test]
    async fn replica_buffer_ttbr_metrics() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Create a session context:
        // Since we are using the same object store accross primary and replica in this test, we
        // only need one context
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&obj_store));
        // Spin up two primary write buffers to do some writes and generate files in an object store:
        let primary_ids = ["newton", "faraday"];
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
                Time::from_timestamp_nanos(0),
            )
            .await;
            primaries.insert(p, primary);
        }
        // Spin up a set of replicated buffers:
        let metric_registry = Arc::new(Registry::new());
        let replication_interval_ms = 50;
        Replicas::new(CreateReplicasArgs {
            last_cache: Arc::new(
                LastCacheProvider::new_from_catalog(primaries["newton"].catalog()).unwrap(),
            ),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::clone(&metric_registry),
            replication_interval: Duration::from_millis(replication_interval_ms),
            hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
            parquet_cache: None,
            catalog: Arc::new(Catalog::new("replica".into(), "replica".into())),
            compacted_data: None,
        })
        .await
        .unwrap();
        // write to newton:
        do_writes(
            "foo",
            &primaries["newton"],
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
        // write to faraday:
        do_writes(
            "foo",
            &primaries["faraday"],
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=b val=true",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=b val=true",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=b val=true",
                },
            ],
        )
        .await;
        // sleep for replicas to replicate:
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check the metric registry:
        let metric = metric_registry
            .get_instrument::<Metric<U64Gauge>>(REPLICA_TTBR_METRIC)
            .expect("get the metric");
        for host in primary_ids {
            let _ttbr_ms = metric
                .get_observer(&Attributes::from(&[("host", host)]))
                .expect("failed to get observer")
                .fetch();
        }
    }

    #[tokio::test]
    async fn parquet_cache_with_read_replicas() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
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
                Time::from_timestamp_nanos(0),
            )
            .await;
            primaries.insert(p, primary);
        }

        // write to skinner:
        do_writes(
            "foo",
            &primaries["skinner"],
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
            &primaries["chalmers"],
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

        // ensure snapshots have been taken so there are parquet files for each host:
        verify_snapshot_count(1, Arc::clone(&obj_store), "skinner").await;
        verify_snapshot_count(1, Arc::clone(&obj_store), "chalmers").await;

        let table_schema = primaries["skinner"]
            .catalog()
            .db_schema("foo")
            .unwrap()
            .table_schema("bar")
            .unwrap()
            .clone();

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
            );
            let ctx = IOxSessionContext::with_testing();
            let runtime_env = ctx.inner().runtime_env();
            register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&cached_obj_store));
            let replicas = Replicas::new(CreateReplicasArgs {
                // could load a new catalog from the object store, but it is easier to just re-use
                // skinner's:
                last_cache: Arc::new(
                    LastCacheProvider::new_from_catalog(primaries["skinner"].catalog()).unwrap(),
                ),
                object_store: Arc::clone(&cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
                parquet_cache: Some(parquet_cache),
                catalog: Arc::new(Catalog::new("replica".into(), "replica".into())),
                compacted_data: None,
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
            let chunks = replicas.get_all_chunks("foo", "bar", table_schema.clone());
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
                last_cache: Arc::new(
                    LastCacheProvider::new_from_catalog(primaries["skinner"].catalog()).unwrap(),
                ),
                object_store: Arc::clone(&non_cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
                parquet_cache: None,
                catalog: Arc::new(Catalog::new("replica".into(), "replica".into())),
                compacted_data: None,
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
            let chunks = replicas.get_all_chunks("foo", "bar", table_schema.clone());
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
        // fabricate some WalContents that would originate from the "b" replica host that contain
        // a single CreateTable operation for the table "pow" that does not exist locally:
        let wal_content = create::wal_content(
            (0, 1, 0),
            [create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [create::create_table_op(
                    db_id,
                    "foo",
                    TableId::new(),
                    "pow",
                    [
                        create::field_def("t1", FieldDataType::Tag),
                        create::field_def("f1", FieldDataType::Boolean),
                    ],
                )],
            )],
        );
        // check the replicated catalog's id map before we map the above wal content
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({ description => "id map before mapping replica WAL content" }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // do the mapping, which will allocate a new ID for the "pow" table locally:
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content);
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
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
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
        let wal_content = create::wal_content(
            (0, 1, 0),
            [create::catalog_batch_op(
                db_id,
                "sup",
                0,
                [create::create_table_op(
                    db_id,
                    "sup",
                    table_id,
                    "dog",
                    [
                        create::field_def("t1", FieldDataType::Tag),
                        create::field_def("f1", FieldDataType::Float),
                        create::field_def("time", FieldDataType::Timestamp),
                    ],
                )],
            )],
        );
        replica
            .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
            .expect("catalog batch should apply successfully on replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({ description => "id map before mapping replica WAL content" }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content);
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
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
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "fizz",
                    0,
                    [create::create_table_op(
                        db_id,
                        "fizz",
                        table_id,
                        "buzz",
                        [
                            create::field_def("t1", FieldDataType::Tag),
                            create::field_def("f1", FieldDataType::Float),
                            create::field_def("time", FieldDataType::Timestamp),
                        ],
                    )],
                )],
            );
            primary
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
                .expect("catalog batch should apply successfully on primary catalog");
            (db_id, table_id)
        };
        // now do the same thing as if the db/table were created separately on the replica:
        let (replica_db_id, replica_table_id, replica_wal_content) = {
            let db_id = DbId::new();
            let table_id = TableId::new();
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "fizz",
                    0,
                    [create::create_table_op(
                        db_id,
                        "fizz",
                        table_id,
                        "buzz",
                        [
                            create::field_def("t1", FieldDataType::Tag),
                            create::field_def("f1", FieldDataType::Float),
                            create::field_def("time", FieldDataType::Timestamp),
                        ],
                    )],
                )],
            );
            replica
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
                .expect("catalog batch should apply successfully on replica catalog");
            (db_id, table_id, wal_content)
        };
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({ description => "id map before mapping replica WAL content" }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(replica_wal_content);
        let id_map = replicated_catalog.id_map.lock().clone();
        assert_eq!(primary_db_id, id_map.map_db_id(replica_db_id).unwrap());
        assert_eq!(
            primary_table_id,
            id_map.map_table_id(replica_table_id).unwrap()
        );
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
            .expect("mapped batch should still apply successfully to primary");
    }

    #[test]
    fn map_wal_content_for_replica_field_additions() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        // there is a db called "foo" and a table called "bar" already, but get their IDs on the
        // replica's catalog, so we can use the ID map to map them onto the primary
        let (db_id, db_schema) = replica.db_schema_and_id("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let wal_content = create::wal_content(
            (0, 1, 0),
            [create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [create::add_fields_op(
                    db_id,
                    "foo",
                    table_id,
                    "bar",
                    [create::field_def("f4", FieldDataType::Float)],
                )],
            )],
        );
        replica
            .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
            .expect("catalog batch should apply successfully to the replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({ description => "id map before mapping replica WAL content" }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content);
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        // apply the mapped wal content to the primary:
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
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
            let (db_id, db_schema) = primary.db_schema_and_id("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [create::field_def("f4", FieldDataType::Float)],
                    )],
                )],
            );
            primary
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
                .expect("catalog batch should apply on primary");
            (db_id, table_id)
        };
        let (replica_db_id, replica_table_id, replica_wal_content) = {
            let (db_id, db_schema) = replica.db_schema_and_id("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [create::add_fields_op(
                        db_id,
                        "foo",
                        table_id,
                        "bar",
                        [create::field_def("f4", FieldDataType::Float)],
                    )],
                )],
            );
            replica
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
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
        let mapped_wal_content = replicated_catalog.map_wal_contents(replica_wal_content);
        let id_map = replicated_catalog.id_map.lock().clone();
        assert_eq!(primary_db_id, id_map.map_db_id(replica_db_id).unwrap());
        assert_eq!(
            primary_table_id,
            id_map.map_table_id(replica_table_id).unwrap()
        );
        // TODO: should assert on column IDs when those are present
        // NOTE: the following snapshot wont change since no new tables/dbs were added, but including
        // column IDs into the mix should cause this snapshot to fail, and we can fix it then!
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
            .expect("mapped batch should still apply successfully to primary");
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
        let (db_id, db_schema) = replica.db_schema_and_id("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let wal_content = create::wal_content(
            (0, 1, 0),
            [create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [
                    create::create_last_cache_op_builder(table_id, "bar", "test_cache", ["t1"])
                        .build(),
                ],
            )],
        );
        replica
            .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
            .expect("catalog batch should apply successfully on replica catalog");
        let id_map = replicated_catalog.id_map.lock().clone();
        insta::with_settings!({ description => "id map before mapping replica WAL content" }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content);
        let id_map = replicated_catalog.id_map.lock().clone();
        // NOTE: this wont have changed, unless we give last caches IDs
        insta::with_settings!({
            sort_maps => true,
            description => "id map after mapping replica WAL content"
        }, {
            insta::assert_yaml_snapshot!(id_map);
        });
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
            .unwrap();
        let db = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema with table 'bar' that now has a last cache definition \
            from the replica"
        }, {
            insta::assert_yaml_snapshot!(db);
        });
        // now delete the last cache on the replica:
        let wal_content = create::wal_content(
            (0, 1, 0),
            [create::catalog_batch_op(
                db_id,
                "foo",
                0,
                [create::delete_last_cache_op(table_id, "bar", "test_cache")],
            )],
        );
        replica
            .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
            .expect("catalog batch to delete last cache should apply on replica catalog");
        let mapped_wal_content = replicated_catalog.map_wal_contents(wal_content);
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
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
            let (db_id, db_schema) = primary.db_schema_and_id("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [
                        create::create_last_cache_op_builder(table_id, "bar", "test_cache", ["t1"])
                            .build(),
                    ],
                )],
            );
            primary
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
                .expect("apply catalog batch to primary to create last cache");
        }
        let replica_wal_content = {
            let (db_id, db_schema) = replica.db_schema_and_id("foo").unwrap();
            let table_id = db_schema.table_name_to_id("bar").unwrap();
            let wal_content = create::wal_content(
                (0, 1, 0),
                [create::catalog_batch_op(
                    db_id,
                    "foo",
                    0,
                    [
                        create::create_last_cache_op_builder(table_id, "bar", "test_cache", ["t1"])
                            .build(),
                    ],
                )],
            );
            replica
                .apply_catalog_batch(wal_content.ops[0].as_catalog().unwrap())
                .expect("apply catalog batch to primary to create last cache");
            wal_content
        };
        let mapped_wal_content = replicated_catalog.map_wal_contents(replica_wal_content);
        // check the structure of the primary db schema to ensure it has only a single last cache:
        let db_before_applying = primary.db_schema("foo").unwrap();
        insta::with_settings!({
            description => "database schema for 'foo' db before applying the mapped catalog \
            batch from the replica; it should have a 'bar' table containing a single last cache \
            definition"
        }, {
            insta::assert_yaml_snapshot!(db_before_applying);
        });
        primary
            .apply_catalog_batch(mapped_wal_content.ops[0].as_catalog().unwrap())
            .expect("apply mapped catalog batch with last cache create from replica");
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
    fn map_persisted_snapshot_for_replica() {
        let primary = create::catalog("primary");
        let replica = create::catalog("replica");
        let replicated_catalog =
            ReplicatedCatalog::new(Arc::clone(&primary), Arc::clone(&replica)).unwrap();
        let (db_id, db_schema) = replica.db_schema_and_id("foo").unwrap();
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
        let mapped_snapshot = replicated_catalog.map_snapshot_contents(snapshot);
        insta::with_settings!({
            sort_maps => true,
            description => "persisted snapshot after mapping, as intended for the primary"
        }, {
            insta::assert_json_snapshot!(mapped_snapshot);
        });
    }

    async fn setup_primary(
        host_id: &str,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        start_time: Time,
    ) -> WriteBufferImpl {
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), host_id));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        let metric_registry = Arc::new(Registry::new());
        WriteBufferImpl::new(
            Arc::clone(&persister),
            catalog,
            Arc::new(last_cache),
            time_provider,
            make_exec(object_store, metric_registry),
            wal_config,
            None,
        )
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
        let (db_id, db_schema) = replicas.catalog().db_schema_and_id(db).unwrap();
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
        let (db_id, db_schema) = replica.catalog().db_schema_and_id(db).unwrap();
        let table_id = db_schema.table_name_to_id(tbl).unwrap();
        for _ in 0..10 {
            let persisted_files = replica.parquet_files(db_id, table_id);
            if persisted_files.len() >= expected_file_count {
                return persisted_files;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("no files were persisted after several tries");
    }

    mod create {
        use influxdb3_catalog::catalog::SequenceNumber;
        use influxdb3_id::ParquetFileId;
        use influxdb3_wal::{
            LastCacheDefinition, LastCacheDelete, LastCacheSize, LastCacheValueColumnsDef,
            SnapshotSequenceNumber,
        };
        use influxdb3_write::DatabaseTables;

        use super::*;
        type SeriesKey<'a> = Option<&'a [&'a str]>;

        pub(super) fn table<C, N, SK>(
            name: &str,
            cols: C,
            series_key: Option<SK>,
        ) -> TableDefinition
        where
            C: AsRef<[(N, InfluxColumnType)]>,
            N: AsRef<str>,
            SK: IntoIterator<Item: AsRef<str>>,
        {
            TableDefinition::new(TableId::new(), name.into(), cols, series_key)
                .expect("create table definition")
        }

        pub(super) fn catalog(name: &str) -> Arc<Catalog> {
            let host_name = format!("host-{name}").as_str().into();
            let instance_name = format!("instance-{name}").as_str().into();
            let cat = Catalog::new(host_name, instance_name);
            let tbl = table(
                "bar",
                [
                    ("t1", InfluxColumnType::Tag),
                    ("t2", InfluxColumnType::Tag),
                    (
                        "f1",
                        InfluxColumnType::Field(schema::InfluxFieldType::Boolean),
                    ),
                ],
                SeriesKey::None,
            );
            let mut db = DatabaseSchema::new(DbId::new(), "foo".into());
            db.table_map
                .insert(tbl.table_id, Arc::clone(&tbl.table_name));
            db.tables.insert(tbl.table_id, tbl);
            cat.insert_database(db);
            cat.into()
        }

        pub(super) fn wal_content(
            (min_timestamp_ns, max_timestamp_ns, wal_file_number): (i64, i64, u64),
            ops: impl IntoIterator<Item = WalOp>,
        ) -> WalContents {
            WalContents {
                min_timestamp_ns,
                max_timestamp_ns,
                wal_file_number: WalFileSequenceNumber::new(wal_file_number),
                ops: ops.into_iter().collect(),
                snapshot: None,
            }
        }

        pub(super) fn catalog_batch_op(
            db_id: DbId,
            db_name: impl Into<Arc<str>>,
            time_ns: i64,
            ops: impl IntoIterator<Item = CatalogOp>,
        ) -> WalOp {
            WalOp::Catalog(CatalogBatch {
                database_id: db_id,
                database_name: db_name.into(),
                time_ns,
                ops: ops.into_iter().collect(),
            })
        }

        pub(super) fn add_fields_op(
            database_id: DbId,
            db_name: impl Into<Arc<str>>,
            table_id: TableId,
            table_name: impl Into<Arc<str>>,
            fields: impl IntoIterator<Item = FieldDefinition>,
        ) -> CatalogOp {
            CatalogOp::AddFields(influxdb3_wal::FieldAdditions {
                database_name: db_name.into(),
                database_id,
                table_name: table_name.into(),
                table_id,
                field_definitions: fields.into_iter().collect(),
            })
        }

        pub(super) fn create_table_op(
            db_id: DbId,
            db_name: impl Into<Arc<str>>,
            table_id: TableId,
            table_name: impl Into<Arc<str>>,
            fields: impl IntoIterator<Item = FieldDefinition>,
        ) -> CatalogOp {
            CatalogOp::CreateTable(influxdb3_wal::TableDefinition {
                database_id: db_id,
                database_name: db_name.into(),
                table_name: table_name.into(),
                table_id,
                field_definitions: fields.into_iter().collect(),
                key: None,
            })
        }

        pub(super) fn field_def(
            name: impl Into<Arc<str>>,
            data_type: FieldDataType,
        ) -> FieldDefinition {
            FieldDefinition {
                name: name.into(),
                data_type,
            }
        }

        pub(super) struct CreateLastCacheOpBuilder {
            table_id: TableId,
            table_name: String,
            name: String,
            key_columns: Vec<String>,
            value_columns: Option<LastCacheValueColumnsDef>,
            count: Option<LastCacheSize>,
            ttl: Option<u64>,
        }

        impl CreateLastCacheOpBuilder {
            pub(super) fn build(self) -> CatalogOp {
                CatalogOp::CreateLastCache(LastCacheDefinition {
                    table_id: self.table_id,
                    table: self.table_name,
                    name: self.name,
                    key_columns: self.key_columns,
                    value_columns: self
                        .value_columns
                        .unwrap_or(LastCacheValueColumnsDef::AllNonKeyColumns),
                    count: self.count.unwrap_or_else(|| LastCacheSize::new(1).unwrap()),
                    ttl: self.ttl.unwrap_or(3600),
                })
            }
        }

        pub(super) fn create_last_cache_op_builder(
            table_id: TableId,
            table_name: impl Into<String>,
            cache_name: impl Into<String>,
            key_columns: impl IntoIterator<Item: Into<String>>,
        ) -> CreateLastCacheOpBuilder {
            CreateLastCacheOpBuilder {
                table_id,
                table_name: table_name.into(),
                name: cache_name.into(),
                key_columns: key_columns.into_iter().map(Into::into).collect(),
                value_columns: None,
                count: None,
                ttl: None,
            }
        }

        pub(super) fn delete_last_cache_op(
            table_id: TableId,
            table_name: impl Into<String>,
            cache_name: impl Into<String>,
        ) -> CatalogOp {
            CatalogOp::DeleteLastCache(LastCacheDelete {
                table_name: table_name.into(),
                table_id,
                name: cache_name.into(),
            })
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
            host_id: String,
            next_file_id: ParquetFileId,
            next_db_id: DbId,
            next_table_id: TableId,
            snapshot_sequence_number: Option<SnapshotSequenceNumber>,
            wal_file_sequence_number: Option<WalFileSequenceNumber>,
            catalog_sequence_number: Option<SequenceNumber>,
            parquet_size_bytes: Option<u64>,
            row_count: Option<u64>,
            min_time: Option<i64>,
            max_time: Option<i64>,
            databases: HashMap<DbId, DatabaseTables>,
        }

        impl PersistedSnapshotBuilder {
            pub(super) fn build(self) -> PersistedSnapshot {
                PersistedSnapshot {
                    host_id: self.host_id,
                    next_file_id: self.next_file_id,
                    next_db_id: self.next_db_id,
                    next_table_id: self.next_table_id,
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

        pub(super) fn persisted_snapshot(host_id: &str) -> PersistedSnapshotBuilder {
            PersistedSnapshotBuilder {
                host_id: host_id.into(),
                next_file_id: ParquetFileId::next_id(),
                next_db_id: DbId::next_id(),
                next_table_id: TableId::next_id(),
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
