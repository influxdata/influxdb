use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::Utc;
use data_types::{ChunkId, ChunkOrder, PartitionKey, TableId, TransitionPartitionId};
use datafusion::{execution::object_store::ObjectStoreUrl, logical_expr::Expr};
use futures::future::try_join_all;
use futures_util::StreamExt;
use influxdb3_catalog::catalog::Catalog;
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
    ParquetFile, ParquetFileId, PersistedSnapshot,
};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, NoColumnRanges},
    QueryChunk,
};
use metric::{Attributes, Registry, U64Gauge};
use object_store::{path::Path, ObjectStore};
use observability_deps::tracing::{error, info};
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
    catalog: Arc<Catalog>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    last_cache: Arc<LastCacheProvider>,
    replicas: Vec<Arc<ReplicatedBuffer>>,
}

pub(crate) struct CreateReplicasArgs {
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub metric_registry: Arc<Registry>,
    pub replication_interval: Duration,
    pub hosts: Vec<String>,
    pub persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

impl Replicas {
    pub(crate) async fn new(
        CreateReplicasArgs {
            catalog,
            last_cache,
            object_store,
            metric_registry,
            replication_interval,
            hosts,
            persisted_snapshot_notify_tx,
            parquet_cache,
        }: CreateReplicasArgs,
    ) -> Result<Self> {
        let mut handles = vec![];
        for (i, host_identifier_prefix) in hosts.into_iter().enumerate() {
            let object_store = Arc::clone(&object_store);
            let catalog = Arc::clone(&catalog);
            let last_cache = Arc::clone(&last_cache);
            let metric_registry = Arc::clone(&metric_registry);
            let persisted_snapshot_notify_tx = persisted_snapshot_notify_tx.clone();
            let parquet_cache = parquet_cache.clone();
            let handle = tokio::spawn(async move {
                info!(%host_identifier_prefix, "creating replicated buffer for host");
                ReplicatedBuffer::new(CreateReplicatedBufferArgs {
                    replica_order: i as i64,
                    object_store,
                    host_identifier_prefix,
                    catalog,
                    last_cache,
                    replication_interval,
                    metric_registry,
                    persisted_snapshot_notify_tx,
                    parquet_cache,
                })
                .await
            });
            handles.push(handle);
        }
        let replicas = futures::future::try_join_all(handles)
            .await
            .context("failed to initialize replicated buffers in parallel")?
            .into_iter()
            .collect::<Result<Vec<Arc<ReplicatedBuffer>>>>()?;
        Ok(Self {
            catalog,
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
        Arc::clone(&self.catalog)
    }

    pub(crate) fn last_cache(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }

    pub(crate) fn parquet_files(&self, db_name: &str, tbl_name: &str) -> Vec<ParquetFile> {
        let mut files = vec![];
        for replica in &self.replicas {
            files.append(&mut replica.parquet_files(db_name, tbl_name));
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
        host_markers: &[HostSnapshotMarker],
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
    catalog: Arc<Catalog>,
    metrics: ReplicatedBufferMetrics,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
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
    catalog: Arc<Catalog>,
    last_cache: Arc<LastCacheProvider>,
    replication_interval: Duration,
    metric_registry: Arc<Registry>,
    persisted_snapshot_notify_tx: tokio::sync::watch::Sender<Option<PersistedSnapshot>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

impl ReplicatedBuffer {
    pub(crate) async fn new(
        CreateReplicatedBufferArgs {
            replica_order,
            object_store,
            host_identifier_prefix,
            catalog,
            last_cache,
            replication_interval,
            metric_registry,
            persisted_snapshot_notify_tx,
            parquet_cache,
        }: CreateReplicatedBufferArgs,
    ) -> Result<Arc<Self>> {
        let buffer = Arc::new(RwLock::new(BufferState::new(Arc::clone(&catalog))));
        let persisted_files = {
            // Create a temporary persister to load snapshot files
            let persister = Persister::new(Arc::clone(&object_store), &host_identifier_prefix);
            let persisted_snapshots = persister
                .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
                .await
                .context("failed to load snapshots for replicated host")?;
            Arc::new(PersistedFiles::new_from_persisted_snapshots(
                persisted_snapshots,
            ))
        };
        let host: Cow<'static, str> = Cow::from(host_identifier_prefix.clone());
        let attributes = Attributes::from([("host", host)]);
        let replica_ttbr = metric_registry
            .register_metric::<U64Gauge>(
                REPLICA_TTBR_METRIC,
                "time to be readable for the data in each replicated host buffer",
            )
            .recorder(attributes);
        let replicated_buffer = Self {
            replica_order,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            host_identifier_prefix,
            last_wal_file_sequence_number: Mutex::new(None),
            buffer,
            persisted_files,
            last_cache,
            catalog,
            metrics: ReplicatedBufferMetrics { replica_ttbr },
            persisted_snapshot_notify_tx,
            parquet_cache,
        };
        replicated_buffer.replay().await?;
        let replicated_buffer = Arc::new(replicated_buffer);
        background_replication_interval(Arc::clone(&replicated_buffer), replication_interval);

        Ok(replicated_buffer)
    }

    pub(crate) fn parquet_files(&self, db_name: &str, tbl_name: &str) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_name, tbl_name)
    }

    pub(crate) fn get_buffer_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        // Get DB/table schema from the catalog:
        let db_schema = self
            .catalog
            .db_schema(database_name)
            .with_context(|| format!("db {} not found in catalog", database_name))?;
        let table_schema = db_schema
            .tables
            .get(table_name)
            .with_context(|| format!("table {} not found in catalog", table_name))?;
        let schema = table_schema.schema().clone();

        // Get chunks from the in-memory buffer:
        self.get_buffer_table_chunks(database_name, table_name, filters, schema.clone())
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
        let mut files = self.persisted_files.get_files(database_name, table_name);

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
        schema: Schema,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let buffer = self.buffer.read();
        let Some(db_buffer) = buffer.db_to_table.get(database_name) else {
            return Ok(vec![]);
        };
        let Some(table_buffer) = db_buffer.get(table_name) else {
            return Ok(vec![]);
        };
        Ok(table_buffer
            .partitioned_record_batches(schema.as_arrow(), filters)
            .context("error getting partitioned batches from table buffer")?
            .into_iter()
            .map(|(gen_time, (ts_min_max, batches))| {
                let row_count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                let chunk_stats = create_chunk_statistics(
                    Some(row_count),
                    &schema,
                    Some(ts_min_max),
                    &NoColumnRanges,
                );
                Arc::new(BufferChunk {
                    batches,
                    schema: schema.clone(),
                    stats: Arc::new(chunk_stats),
                    partition_id: TransitionPartitionId::new(
                        TableId::new(0),
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
        let mut buffer = self.buffer.write();
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        buffer.buffer_ops(wal_contents.ops, &self.last_cache);
    }

    fn buffer_wal_contents_and_handle_snapshots(
        &self,
        wal_contents: WalContents,
        snapshot_details: SnapshotDetails,
    ) {
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
        let persisted_snapshot_notify_tx = self.persisted_snapshot_notify_tx.clone();
        let parquet_cache = self.parquet_cache.clone();

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
                        persisted_snapshot_notify_tx
                            .send(Some(snapshot))
                            .expect("watch failed");
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
    use std::{collections::HashMap, num::NonZeroUsize, sync::Arc, time::Duration};

    use data_types::NamespaceName;
    use datafusion::{
        arrow::array::RecordBatch, assert_batches_sorted_eq, execution::context::SessionContext,
    };
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::{
        last_cache::LastCacheProvider, parquet_cache::test_cached_obj_store_and_oracle,
        persister::Persister, write_buffer::WriteBufferImpl, ChunkContainer, LastCacheManager,
        ParquetFile, Precision, WriteBuffer,
    };
    use iox_query::{
        exec::{DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext},
        QueryChunk,
    };
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::{Attributes, Metric, Registry, U64Gauge};
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use parquet_file::storage::{ParquetStorage, StorageId};

    use crate::replica::{
        CreateReplicasArgs, CreateReplicatedBufferArgs, Replicas, ReplicatedBuffer,
        REPLICA_TTBR_METRIC,
    };

    #[tokio::test]
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

        // Create a unified snapshot channel for all replicas:
        let (persisted_snapshot_notify_tx, mut persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);
        persisted_snapshot_notify_rx.mark_unchanged();

        // Spin up a replicated buffer:
        let replica = ReplicatedBuffer::new(CreateReplicatedBufferArgs {
            replica_order: 0,
            object_store: Arc::clone(&obj_store),
            host_identifier_prefix: primary_id.to_string(),
            catalog: primary.catalog(),
            last_cache: primary.last_cache_provider(),
            replication_interval: Duration::from_millis(10),
            metric_registry: Arc::new(Registry::new()),
            persisted_snapshot_notify_tx,
            parquet_cache: None,
        })
        .await
        .unwrap();

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

        // verify that it came through on the channel
        assert!(persisted_snapshot_notify_rx.changed().await.is_ok());
        assert!(persisted_snapshot_notify_rx.borrow_and_update().is_some());

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

        // Create a unified snapshot channel for all replicas:
        let (persisted_snapshot_notify_tx, _persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);

        // Spin up a set of replicated buffers:
        let replicas = Replicas::new(CreateReplicasArgs {
            catalog: Arc::new(Catalog::new("replica-1".into(), "test-id-1".into())),
            last_cache: Arc::new(LastCacheProvider::new()),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::new(Registry::new()),
            replication_interval: Duration::from_millis(10),
            hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
            persisted_snapshot_notify_tx,
            parquet_cache: None,
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
        // Create a unified snapshot channel for all replicas:
        let (persisted_snapshot_notify_tx, _persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);

        let metric_registry = Arc::new(Registry::new());
        let replication_interval_ms = 50;
        Replicas::new(CreateReplicasArgs {
            catalog: Arc::new(Catalog::new("replica-1".into(), "test-id-1".into())),
            last_cache: Arc::new(LastCacheProvider::new()),
            object_store: Arc::clone(&obj_store),
            metric_registry: Arc::clone(&metric_registry),
            replication_interval: Duration::from_millis(replication_interval_ms),
            hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
            persisted_snapshot_notify_tx,
            parquet_cache: None,
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
            .get_table_schema("bar")
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
            let (persisted_snapshot_notify_tx, _persisted_snapshot_notify_rx) =
                tokio::sync::watch::channel(None);
            let replicas = Replicas::new(CreateReplicasArgs {
                // could load a new catalog from the object store, but it is easier to just re-use
                // skinner's:
                catalog: primaries["skinner"].catalog(),
                last_cache: Arc::new(LastCacheProvider::new()),
                object_store: Arc::clone(&cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
                persisted_snapshot_notify_tx,
                parquet_cache: Some(parquet_cache),
            })
            .await
            .unwrap();

            // once the `Replicas` has some persisted files, then it will also have those files in
            // the cache, since the buffer isn't cleared until the cache requests are registered/
            // fulfilled:
            let persisted_files = wait_for_replica_persistence(&replicas).await;
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
            let (persisted_snapshot_notify_tx, _persisted_snapshot_notify_rx) =
                tokio::sync::watch::channel(None);
            let replicas = Replicas::new(CreateReplicasArgs {
                // like above, just re-use skinner's catalog for ease:
                catalog: primaries["skinner"].catalog(),
                last_cache: Arc::new(LastCacheProvider::new()),
                object_store: Arc::clone(&non_cached_obj_store),
                metric_registry: Arc::new(Registry::new()),
                replication_interval: Duration::from_millis(10),
                hosts: primary_ids.iter().map(|s| s.to_string()).collect(),
                persisted_snapshot_notify_tx,
                parquet_cache: None,
            })
            .await
            .unwrap();

            // still need to wait for files to be persisted, despite there being no cache, otherwise
            // the queries below could just be served by the buffer:
            let persisted_files = wait_for_replica_persistence(&replicas).await;
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

    async fn chunks_to_record_batches(
        chunks: Vec<Arc<dyn QueryChunk>>,
        ctx: &SessionContext,
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        for chunk in chunks {
            batches.append(&mut chunk.data().read_to_batches(chunk.schema(), ctx).await);
        }
        batches
    }

    struct TestWrite<LP> {
        lp: LP,
        time_seconds: i64,
    }

    async fn do_writes<LP: AsRef<str> + Send + Sync>(
        db: &'static str,
        buffer: &impl WriteBuffer,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                )
                .await
                .unwrap();
        }
    }

    async fn setup_primary(
        host_id: &str,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        start_time: Time,
    ) -> WriteBufferImpl {
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), host_id));
        let catalog = persister.load_or_create_catalog().await.unwrap();
        let last_cache = LastCacheProvider::new_from_catalog(&catalog.clone_inner()).unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            time_provider,
            make_exec(),
            wal_config,
            None,
        )
        .await
        .unwrap()
    }

    fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
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
        ))
    }

    async fn verify_snapshot_count(n: usize, object_store: Arc<dyn ObjectStore>, host_id: &str) {
        let mut checks = 0;
        let persister = Persister::new(object_store, host_id);
        loop {
            let persisted_snapshots = persister.load_snapshots(1000).await.unwrap();
            if persisted_snapshots.len() > n {
                panic!(
                    "checking for {} snapshots but found {}",
                    n,
                    persisted_snapshots.len()
                );
            } else if persisted_snapshots.len() == n && checks > 5 {
                // let enough checks happen to ensure extra snapshots aren't running ion the background
                break;
            } else {
                checks += 1;
                if checks > 10 {
                    panic!("not persisting snapshots");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    /// Wait for a [`Replicas`] to go from having no persisted files to having some persisted files
    async fn wait_for_replica_persistence(replicas: &Replicas) -> Vec<ParquetFile> {
        for _ in 0..10 {
            let persisted_files = replicas.parquet_files("foo", "bar");
            if !persisted_files.is_empty() {
                return persisted_files;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        panic!("no files were persisted after several tries");
    }
}
