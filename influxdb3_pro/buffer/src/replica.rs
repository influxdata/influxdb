use std::{sync::Arc, time::Duration};

use anyhow::Context;
use data_types::{ChunkId, ChunkOrder, PartitionKey, TableId, TransitionPartitionId};
use datafusion::{
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::Expr,
};
use futures_util::StreamExt;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::{
    object_store::wal_path, serialize::verify_file_type_and_deserialize, SnapshotDetails,
    WalContents, WalFileSequenceNumber,
};
use influxdb3_write::{
    chunk::BufferChunk,
    last_cache::LastCacheProvider,
    paths::SnapshotInfoFilePath,
    persister::{Persister, DEFAULT_OBJECT_STORE_URL},
    write_buffer::{
        parquet_chunk_from_file, persisted_files::PersistedFiles, queryable_buffer::BufferState,
        N_SNAPSHOTS_TO_LOAD_ON_START,
    },
    ParquetFile, PersistedSnapshot,
};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, NoColumnRanges},
    QueryChunk,
};
use object_store::{path::Path, ObjectStore};
use observability_deps::tracing::error;
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

#[derive(Debug)]
pub(crate) struct Replicas {
    catalog: Arc<Catalog>,
    last_cache: Arc<LastCacheProvider>,
    replicas: Vec<Arc<ReplicatedBuffer>>,
}

impl Replicas {
    pub(crate) async fn new(
        catalog: Arc<Catalog>,
        last_cache: Arc<LastCacheProvider>,
        object_store: Arc<dyn ObjectStore>,
        replication_interval: Duration,
        hosts: Vec<String>,
    ) -> Result<Self> {
        let mut handles = vec![];
        for host in hosts {
            let object_store = Arc::clone(&object_store);
            let catalog = Arc::clone(&catalog);
            let last_cache = Arc::clone(&last_cache);
            let handle = tokio::spawn(async move {
                ReplicatedBuffer::new(
                    object_store,
                    host,
                    catalog,
                    last_cache,
                    replication_interval,
                )
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
            last_cache,
            replicas,
        })
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

    pub(crate) fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let mut chunks = vec![];
        for replica in &self.replicas {
            // TODO: can we set the priority here based on the host order, i.e, by setting the
            // ChunkOrder on produced chunks from each replica...
            chunks.append(&mut replica.get_table_chunks(
                database_name,
                table_name,
                filters,
                projection,
                ctx,
            )?);
        }
        Ok(chunks)
    }
}

#[derive(Debug)]
pub(crate) struct ReplicatedBuffer {
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    host_identifier_prefix: String,
    last_wal_file_sequence_number: Mutex<Option<WalFileSequenceNumber>>,
    buffer: Arc<RwLock<BufferState>>,
    persisted_files: Arc<PersistedFiles>,
    last_cache: Arc<LastCacheProvider>,
    catalog: Arc<Catalog>,
}

impl ReplicatedBuffer {
    pub(crate) async fn new(
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: String,
        catalog: Arc<Catalog>,
        last_cache: Arc<LastCacheProvider>,
        replication_interval: Duration,
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
        let replicated_buffer = Self {
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store,
            host_identifier_prefix,
            last_wal_file_sequence_number: Mutex::new(None),
            buffer,
            persisted_files,
            last_cache,
            catalog,
        };
        replicated_buffer.replay().await?;
        let replicated_buffer = Arc::new(replicated_buffer);
        background_replication_interval(Arc::clone(&replicated_buffer), replication_interval);

        Ok(replicated_buffer)
    }

    pub(crate) fn parquet_files(&self, db_name: &str, tbl_name: &str) -> Vec<ParquetFile> {
        self.persisted_files.get_files(db_name, tbl_name)
    }

    pub(crate) fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>> {
        let mut chunks: Vec<Arc<dyn QueryChunk>> = vec![];

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
        if let Some(chunk) =
            self.get_buffer_table_chunks(database_name, table_name, filters, schema.clone())?
        {
            chunks.push(chunk);
        }

        // Get parquet chunks:
        let parquet_files = self.persisted_files.get_files(database_name, table_name);
        let mut chunk_order = 1;
        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &schema,
                self.object_store_url.clone(),
                Arc::clone(&self.object_store),
                chunk_order,
            );
            chunk_order += 1;
            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
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
    ) -> Result<Option<Arc<dyn QueryChunk>>> {
        let buffer = self.buffer.read();
        let Some(db_buffer) = buffer.db_to_table.get(database_name) else {
            return Ok(None);
        };
        let Some(table_buffer) = db_buffer.get(table_name) else {
            return Ok(None);
        };
        let batches = table_buffer
            .record_batches(schema.as_arrow(), filters)
            .context("error getting record batches from replicated buffer")?;
        let timestamp_min_max = table_buffer.timestamp_min_max();
        let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let chunk_stats = create_chunk_statistics(
            Some(row_count),
            &schema,
            Some(timestamp_min_max),
            &NoColumnRanges,
        );
        Ok(Some(Arc::new(BufferChunk {
            batches,
            schema,
            stats: Arc::new(chunk_stats),
            // TODO: I have no idea if this is right at the moment:
            partition_id: TransitionPartitionId::new(
                TableId::new(0),
                &PartitionKey::from("buffer_partition"),
            ),
            sort_key: None,
            id: ChunkId::new(),
            // TODO: this should probably come from the replica host order:
            chunk_order: ChunkOrder::new(i64::MAX),
        })))
    }

    async fn replay(&self) -> Result<()> {
        let paths = self.load_existing_wal_paths().await?;

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
        let file_bytes = self
            .object_store
            .get(path)
            .await?
            .bytes()
            .await
            .context("failed to collect data for known file into bytes")?;
        let wal_contents = verify_file_type_and_deserialize(file_bytes)
            .context("failed to verify and deserialize wal file contents")?;

        match wal_contents.snapshot {
            None => self.buffer_wal_contents(wal_contents),
            Some(snapshot_details) => {
                self.buffer_wal_contents_and_handle_snapshots(wal_contents, snapshot_details)
                    .await?
            }
        }

        Ok(())
    }

    fn buffer_wal_contents(&self, wal_contents: WalContents) {
        let mut buffer = self.buffer.write();
        self.last_cache.write_wal_contents_to_cache(&wal_contents);
        buffer.buffer_ops(wal_contents.ops, &self.last_cache);
    }

    async fn buffer_wal_contents_and_handle_snapshots(
        &self,
        wal_contents: WalContents,
        snapshot_details: SnapshotDetails,
    ) -> Result<()> {
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
        // Update the persisted files:
        let snapshot_path = SnapshotInfoFilePath::new(
            &self.host_identifier_prefix,
            snapshot_details.snapshot_sequence_number,
        );
        let snapshot_bytes = self
            .object_store
            .get(&snapshot_path)
            .await
            .context("failed to retrieve snapshot file")?
            .bytes()
            .await
            .context("failed to get bytes for object store get request")?;
        let snapshot = serde_json::from_slice::<PersistedSnapshot>(&snapshot_bytes)
            .context("failed to parse snapshot info file as JSON")?;
        // Now that the snapshot has been loaded, clear the buffer of the data that was separated
        // out previously and update the persisted files:
        let mut buffer = self.buffer.write();
        for (_, tbl_map) in buffer.db_to_table.iter_mut() {
            for (_, tbl_buf) in tbl_map.iter_mut() {
                tbl_buf.clear_snapshots();
            }
        }
        self.persisted_files.add_persisted_snapshot_files(snapshot);
        Ok(())
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
            } else if let Err(error) = replicated_buffer.replay().await {
                error!(%error, "failed to replay replicated buffer on replication interval");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use data_types::NamespaceName;
    use datafusion::{
        arrow::array::RecordBatch, assert_batches_sorted_eq, execution::context::SessionContext,
    };
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_wal::{Level0Duration, WalConfig};
    use influxdb3_write::{
        persister::Persister, write_buffer::WriteBufferImpl, ChunkContainer, LastCacheManager,
        Precision, WriteBuffer,
    };
    use iox_query::{
        exec::{DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext},
        QueryChunk,
    };
    use iox_time::{MockProvider, Time, TimeProvider};
    use object_store::{memory::InMemory, ObjectStore};
    use parquet_file::storage::{ParquetStorage, StorageId};
    use schema::Schema;

    use crate::replica::ReplicatedBuffer;

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
                level_0_duration: Level0Duration::new_1m(),
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
        let replica = ReplicatedBuffer::new(
            Arc::clone(&obj_store),
            primary_id.to_string(),
            primary.catalog(),
            primary.last_cache_provider(),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        // Check that the replica replayed the primary and contains its data:
        {
            let chunks = replica
                .get_table_chunks(db_name, tbl_name, &[], None, &ctx.inner().state())
                .unwrap();
            let schema = {
                let db_schema = replica.catalog.db_schema(db_name).unwrap();
                db_schema.get_table_schema(tbl_name).unwrap().clone()
            };
            let batches = chunks_to_record_batches(chunks, &schema, ctx.inner()).await;
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
            let schema = {
                let db_schema = replica.catalog.db_schema(db_name).unwrap();
                db_schema.get_table_schema(tbl_name).unwrap().clone()
            };
            let batches = chunks_to_record_batches(chunks, &schema, ctx.inner()).await;
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
            let chunks = replica
                .get_table_chunks(db_name, tbl_name, &[], None, &ctx.inner().state())
                .unwrap();
            let schema = {
                let db_schema = replica.catalog.db_schema(db_name).unwrap();
                db_schema.get_table_schema(tbl_name).unwrap().clone()
            };
            let batches = chunks_to_record_batches(chunks, &schema, ctx.inner()).await;
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

    async fn chunks_to_record_batches(
        chunks: Vec<Arc<dyn QueryChunk>>,
        schema: &Schema,
        ctx: &SessionContext,
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        for chunk in chunks {
            batches.append(&mut chunk.data().read_to_batches(schema, ctx).await);
        }
        batches
    }

    struct TestWrite {
        lp: String,
        time_seconds: i64,
    }

    async fn do_writes(db: &'static str, buffer: &impl WriteBuffer, writes: &[TestWrite]) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_str(),
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
        let (last_cache, catalog) = persister.load_last_cache_and_catalog().await.unwrap();
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start_time));
        WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            time_provider,
            make_exec(),
            wal_config,
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
}
