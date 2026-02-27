//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub mod checkpoint;
mod metrics;
pub mod persisted_files;
pub mod queryable_buffer;
mod table_buffer;
use influxdb3_shutdown::ShutdownToken;
use tokio::sync::{oneshot, watch::Receiver};
use trace::span::{MetaValue, SpanRecorder};
pub mod validator;

use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshot, PersistedSnapshotCheckpointVersion,
    PersistedSnapshotVersion, Precision, WriteBuffer, WriteLineError,
    chunk::ParquetChunk,
    persister::{Persister, PersisterError},
    write_buffer::{
        checkpoint::year_month_from_timestamp_ms, persisted_files::PersistedFiles,
        queryable_buffer::QueryableBuffer, validator::WriteValidator,
    },
};
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError, PartitionHashId,
};
use datafusion::{
    catalog::Session, common::DataFusionError, datasource::object_store::ObjectStoreUrl,
};
use influxdb3_cache::{
    distinct_cache::{self, DistinctCacheProvider},
    parquet_cache::CacheRequest,
};
use influxdb3_cache::{
    last_cache::{self, LastCacheProvider},
    parquet_cache::ParquetCacheOracle,
};
use influxdb3_catalog::{
    CatalogError,
    catalog::{Catalog, DatabaseSchema, Prompt, TableDefinition},
};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{
    SnapshotSequenceNumber, Wal, WalConfig, WalFileNotifier, WalOp,
    object_store::{CreateWalObjectStoreArgs, WalObjectStore},
};
use iox_query::{
    QueryChunk,
    chunk_statistics::{NoColumnRanges, create_chunk_statistics},
    exec::SessionContextIOxExt,
};
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::WriteMetrics;
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjPath};
use observability_deps::tracing::{debug, info, trace, warn};
use parquet_file::storage::DataSourceExecInput;
use queryable_buffer::QueryableBufferArgs;
use schema::Schema;
use std::{borrow::Borrow, num::NonZeroU64, sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("line protocol parse failed: {}", .0.error_message)]
    ParseError(WriteLineError),

    #[error("incoming write was empty")]
    EmptyWrite,

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[error("catalog update error: {0}")]
    CatalogUpdateError(#[from] CatalogError),

    #[error("error from persister: {0}")]
    PersisterError(#[from] PersisterError),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),

    #[error("error in last cache: {0}")]
    LastCacheError(#[from] last_cache::Error),

    #[error("database not found {db_name:?}")]
    DatabaseNotFound { db_name: String },

    #[error("table not found {table_name:?} in db {db_name:?}")]
    TableNotFound { db_name: String, table_name: String },

    #[error("tried accessing database that does not exist")]
    DbDoesNotExist,

    #[error("tried creating database named '{0}' that already exists")]
    DatabaseExists(String),

    #[error("cannot write to soft-deleted database '{0}' - it is marked for deletion")]
    DatabaseDeleted(String),

    #[error("tried accessing table that do not exist")]
    TableDoesNotExist,

    #[error("table '{db_name}.{table_name}' already exists")]
    TableAlreadyExists {
        db_name: Arc<str>,
        table_name: Arc<str>,
    },

    #[error("tried accessing column with name ({0}) that does not exist")]
    ColumnDoesNotExist(String),

    #[error(
        "updating catalog on delete of last cache failed, you will need to delete the cache \
        again on server restart"
    )]
    DeleteLastCache(#[source] CatalogError),

    #[error("error from wal: {0}")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("error in distinct value cache: {0}")]
    DistinctCacheError(#[from] distinct_cache::ProviderError),

    #[error("cannot write to a compactor-only server")]
    NoWriteInCompactorOnly,

    #[error("error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl {
    catalog: Arc<Catalog>,
    persister: Arc<Persister>,
    // NOTE(trevor): the parquet cache interface may be used to register other cache
    // requests from the write buffer, e.g., during query...
    #[allow(dead_code)]
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    wal_config: WalConfig,
    wal: Arc<dyn Wal>,
    metrics: WriteMetrics,
    distinct_cache: Arc<DistinctCacheProvider>,
    last_cache: Arc<LastCacheProvider>,
    /// The number of files we will accept for a query
    query_file_limit: usize,
}

/// The maximum number of snapshots to load on start
pub const N_SNAPSHOTS_TO_LOAD_ON_START: NonZeroU64 = NonZeroU64::new(1_000).unwrap();

#[derive(Debug)]
pub struct WriteBufferImplArgs {
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub metric_registry: Arc<Registry>,
    pub snapshotted_wal_files_to_keep: u64,
    pub query_file_limit: Option<usize>,
    pub n_snapshots_to_load_on_start: NonZeroU64,
    pub shutdown: ShutdownToken,
    pub wal_replay_concurrency_limit: usize,
}

impl WriteBufferImpl {
    pub async fn new(
        WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            executor,
            wal_config,
            parquet_cache,
            metric_registry,
            snapshotted_wal_files_to_keep,
            query_file_limit,
            n_snapshots_to_load_on_start,
            shutdown,
            wal_replay_concurrency_limit,
        }: WriteBufferImplArgs,
    ) -> Result<Arc<Self>> {
        // Calculate sequence cutoff based on n_snapshots_to_load_on_start
        let sequence_cutoff = match persister.get_latest_snapshot_sequence().await {
            Ok(Some(latest)) => {
                let cutoff = latest
                    .as_u64()
                    .saturating_sub(n_snapshots_to_load_on_start.get());
                Some(SnapshotSequenceNumber::new(cutoff))
            }
            Ok(None) => None,
            Err(e) => {
                warn!(%e, "Failed to get latest snapshot sequence, loading all checkpoints");
                None
            }
        };

        // Try to load from checkpoints first for faster startup
        let checkpoint_paths = match persister
            .list_latest_checkpoints_per_month(sequence_cutoff)
            .await
        {
            Ok(paths) => paths,
            Err(e) => {
                warn!(
                    %e,
                    "Failed to list checkpoints, falling back to snapshot loading. \
                     This may result in slower startup."
                );
                Vec::new()
            }
        };

        let (persisted_files, last_wal_sequence_number, last_snapshot_sequence_number) =
            if !checkpoint_paths.is_empty() {
                // Load checkpoints and any snapshots newer than the checkpoints
                info!(
                    checkpoint_count = checkpoint_paths.len(),
                    "Loading from checkpoints for faster startup"
                );

                let checkpoints = persister
                    .load_checkpoints(checkpoint_paths)
                    .await?
                    .into_iter()
                    .map(|cpv| match cpv {
                        PersistedSnapshotCheckpointVersion::V1(cp) => cp,
                    })
                    .collect::<Vec<_>>();

                // Warm the persister's checkpoint cache with the current month's checkpoint
                // This enables incremental updates during this server session
                {
                    let current_month =
                        year_month_from_timestamp_ms(time_provider.now().timestamp_millis());
                    if let Some(current_month_checkpoint) =
                        checkpoints.iter().find(|c| c.year_month == current_month)
                    {
                        persister.warm_checkpoint_cache(current_month_checkpoint.clone());
                    }
                }

                // Find the max snapshot sequence number across all checkpoints
                let max_checkpoint_snapshot_seq = checkpoints
                    .iter()
                    .map(|c| c.last_snapshot_sequence_number)
                    .max();

                // Load snapshots newer than the checkpoint
                let additional_snapshots = if let Some(max_seq) = max_checkpoint_snapshot_seq {
                    persister
                        .load_snapshots_after(max_seq, n_snapshots_to_load_on_start.get() as usize)
                        .await?
                        .into_iter()
                        .map(|psv| match psv {
                            PersistedSnapshotVersion::V1(ps) => ps,
                        })
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                };

                info!(
                    checkpoints_loaded = checkpoints.len(),
                    additional_snapshots = additional_snapshots.len(),
                    "Checkpoint loading complete"
                );

                // Determine last WAL/snapshot sequence from either additional snapshots or checkpoints
                let (last_wal_seq, last_snap_seq, next_file_id) =
                    if let Some(first_snap) = additional_snapshots.first() {
                        (
                            Some(first_snap.wal_file_sequence_number),
                            Some(first_snap.snapshot_sequence_number),
                            Some(first_snap.next_file_id),
                        )
                    } else if let Some(newest_checkpoint) = checkpoints
                        .iter()
                        .max_by_key(|c| c.last_snapshot_sequence_number)
                    {
                        (
                            Some(newest_checkpoint.wal_file_sequence_number),
                            Some(newest_checkpoint.last_snapshot_sequence_number),
                            newest_checkpoint.next_file_id,
                        )
                    } else {
                        (None, None, None)
                    };

                // Set the next file ID if available
                if let Some(file_id) = next_file_id {
                    file_id.set_next_id();
                }

                let persisted_files = Arc::new(PersistedFiles::new_from_checkpoints_and_snapshots(
                    None,
                    checkpoints,
                    additional_snapshots,
                ));

                (persisted_files, last_wal_seq, last_snap_seq)
            } else {
                // Fall back to loading snapshots directly
                debug!("No checkpoints found, loading snapshots directly");

                let persisted_snapshots = persister
                    .load_snapshots(n_snapshots_to_load_on_start.get() as usize)
                    .await?
                    .into_iter()
                    .map(|psv| match psv {
                        PersistedSnapshotVersion::V1(ps) => ps,
                    })
                    .collect::<Vec<PersistedSnapshot>>();

                // Wrap snapshots in Arc to share between background task and PersistedFiles
                // without cloning the entire Vec
                let persisted_snapshots = Arc::new(persisted_snapshots);

                // Build and persist checkpoints from loaded snapshots for faster future startup
                // This runs in a background task to avoid blocking server startup
                if !persisted_snapshots.is_empty() {
                    let current_month =
                        year_month_from_timestamp_ms(time_provider.now().timestamp_millis());
                    let persister_clone = Arc::clone(&persister);
                    let snapshots_for_background = Arc::clone(&persisted_snapshots);

                    tokio::spawn(async move {
                        if let Some(current_checkpoint) = persister_clone
                            .build_and_persist_checkpoints_from_snapshots(
                                &snapshots_for_background,
                                current_month,
                            )
                            .await
                        {
                            // Warm cache with current month's checkpoint for incremental updates
                            persister_clone.warm_checkpoint_cache(current_checkpoint);
                        }
                    });
                }

                let last_wal_seq = persisted_snapshots
                    .first()
                    .map(|s| s.wal_file_sequence_number);
                let last_snap_seq = persisted_snapshots
                    .first()
                    .map(|s| s.snapshot_sequence_number);

                // If we have any snapshots, set sequential IDs from the newest one.
                if let Some(first_snapshot) = persisted_snapshots.first() {
                    first_snapshot.next_file_id.set_next_id();
                }

                let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
                    None,
                    persisted_snapshots,
                ));

                (persisted_files, last_wal_seq, last_snap_seq)
            };
        let queryable_buffer = Arc::new(QueryableBuffer::new(QueryableBufferArgs {
            executor,
            catalog: Arc::clone(&catalog),
            persister: Arc::clone(&persister),
            last_cache_provider: Arc::clone(&last_cache),
            distinct_cache_provider: Arc::clone(&distinct_cache),
            persisted_files: Arc::clone(&persisted_files),
            parquet_cache: parquet_cache.clone(),
        }));

        // create the wal instance, which will replay into the queryable buffer and start
        // the background flush task.
        trace!(
            ?last_wal_sequence_number,
            ?last_snapshot_sequence_number,
            "WriteBufferImpl::new: creating WalObjectStore"
        );
        let wal = WalObjectStore::new(CreateWalObjectStoreArgs {
            time_provider: Arc::clone(&time_provider),
            object_store: persister.object_store(),
            node_identifier_prefix: persister.node_identifier_prefix(),
            file_notifier: Arc::clone(&queryable_buffer) as Arc<dyn WalFileNotifier>,
            config: wal_config,
            last_wal_sequence_number,
            last_snapshot_sequence_number,
            snapshotted_wal_files_to_keep,
            shutdown,
            wal_replay_concurrency_limit,
        })
        .await?;

        let result = Arc::new(Self {
            catalog,
            parquet_cache,
            persister,
            wal_config,
            wal,
            distinct_cache,
            last_cache,
            persisted_files,
            buffer: queryable_buffer,
            metrics: WriteMetrics::new(&metric_registry),
            query_file_limit: query_file_limit.unwrap_or(432),
        });
        Ok(result)
    }

    pub fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal)
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        Arc::clone(&self.persisted_files)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        // NOTE(trevor/catalog-refactor): should there be some retry limit or timeout?
        loop {
            // validated lines will update the in-memory catalog, ensuring that all write operations
            // past this point will be infallible
            let result = match WriteValidator::initialize(db_name.clone(), self.catalog())?
                .v1_parse_lines_and_catalog_updates(lp, accept_partial, ingest_time, precision)?
                .commit_catalog_changes()
                .await?
            {
                Prompt::Success(r) => r.convert_lines_to_buffer(self.wal_config.gen1_duration),
                Prompt::Retry(_) => {
                    debug!("retrying write_lp after attempted commit");
                    continue;
                }
            };

            // Only buffer to the WAL if there are actually writes in the batch; it
            // is possible to get empty writes with `accept_partial`:
            if result.line_count > 0 {
                let ops = vec![WalOp::Write(result.valid_data)];

                if no_sync {
                    self.wal.write_ops_unconfirmed(ops).await?;
                } else {
                    // write to the wal. Behind the scenes the ops get buffered in memory and once a second (or
                    // whatever the configured wal flush interval is set to) the buffer is flushed and all the
                    // data is persisted into a single wal file in the configured object store. Then the
                    // contents are sent to the configured notifier, which in this case is the queryable buffer.
                    // Thus, after this returns, the data is both durable and queryable.
                    self.wal.write_ops(ops).await?;
                }
            }

            if result.line_count == 0 && result.errors.is_empty() {
                return Err(Error::EmptyWrite);
            }

            // record metrics for lines written, rejected, and bytes written
            self.metrics
                .record_lines(&db_name, result.line_count as u64);
            self.metrics
                .record_lines_rejected(&db_name, result.errors.len() as u64);
            self.metrics
                .record_bytes(&db_name, result.valid_bytes_count);

            break Ok(BufferedWriteRequest {
                db_name,
                invalid_lines: result.errors,
                line_count: result.line_count,
                field_count: result.field_count,
                index_count: result.index_count,
            });
        }
    }

    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let span_ctx = ctx.span_ctx().map(|span| span.child("table_chunks"));
        let mut recorder = SpanRecorder::new(span_ctx);

        let mut chunks = self.buffer.get_table_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            projection,
            ctx,
        )?;
        let num_chunks_from_buffer = chunks.len();
        recorder.set_metadata(
            "buffer_chunks",
            MetaValue::Int(num_chunks_from_buffer as i64),
        );

        let parquet_files =
            self.persisted_files
                .get_files_filtered(db_schema.id, table_def.table_id, filter);
        let num_parquet_files_needed = parquet_files.len();
        recorder.set_metadata(
            "parquet_files",
            MetaValue::Int(num_parquet_files_needed as i64),
        );

        if parquet_files.len() > self.query_file_limit {
            return Err(DataFusionError::External(
                format!(
                    "Query would scan {} Parquet files, exceeding the file limit. \
                     InfluxDB 3 Core caps file access to prevent performance degradation \
                     and memory issues. Use a narrower time range, or increase the limit \
                     with --query-file-limit (this may cause slower queries or instability).\n\n\
                     To remove this limitation, upgrade to InfluxDB 3 Enterprise, which \
                     automatically compacts files for efficient querying across any time range. \
                     Free for non-commercial and home use, and free trials for commercial \
                     evaluation: https://www.influxdata.com/downloads",
                    self.query_file_limit
                )
                .into(),
            ));
        }

        if let Some(parquet_cache) = &self.parquet_cache {
            let num_files_already_in_cache = parquet_files
                .iter()
                .filter(|f| {
                    parquet_cache
                        .in_cache(&ObjPath::parse(&f.path).expect("obj path should be parseable"))
                })
                .count();

            recorder.set_metadata(
                "parquet_files_already_in_cache",
                MetaValue::Int(num_files_already_in_cache as i64),
            );
            debug!(
                num_chunks_from_buffer,
                num_parquet_files_needed, num_files_already_in_cache, "query chunks breakdown"
            );
        } else {
            debug!(
                num_chunks_from_buffer,
                num_parquet_files_needed, "query chunks breakdown (cache disabled)"
            );
        }

        let mut chunk_order = chunks.len() as i64;
        // Although this sends a cache request, it does not mean all these
        // files will be cached. This depends on parquet cache's capacity
        // and whether these files are recent enough
        cache_parquet_files(self.parquet_cache.clone(), &parquet_files);

        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_def.schema,
                self.persister.object_store_url().clone(),
                self.persister.object_store(),
                chunk_order,
            );

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }

    #[cfg(test)]
    fn get_table_chunks_from_buffer_only(
        &self,
        database_name: &str,
        table_name: &str,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Vec<Arc<dyn QueryChunk>> {
        let db_schema = self.catalog.db_schema(database_name).unwrap();
        let table_def = db_schema.table_definition(table_name).unwrap();
        self.buffer
            .get_table_chunks(db_schema, table_def, filter, projection, ctx)
            .unwrap()
    }
}

pub fn cache_parquet_files<T: AsRef<ParquetFile>>(
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    parquet_files: &[T],
) {
    if let Some(parquet_cache) = parquet_cache {
        let all_cache_notifiers: Vec<oneshot::Receiver<()>> = parquet_files
            .iter()
            .map(|file| {
                // When datafusion tries to fetch this file we'll have cache in "Fetching" state.
                // There is a slim chance that this request hasn't been processed yet, then we
                // could incur extra GET req to populate the cache. Having a transparent
                // cache might be handy for this case.
                let f: &ParquetFile = file.borrow().as_ref();
                let (cache_req, receiver) = CacheRequest::create_eventual_mode_cache_request(
                    ObjPath::from(f.path.as_str()),
                    Some(f.timestamp_min_max()),
                );
                parquet_cache.register(cache_req);
                receiver
            })
            .collect();
        // there's no explicit await on these receivers - we're only letting parquet cache know
        // this file can be cached if it meets cache's policy.
        debug!(len = ?all_cache_notifiers.len(), "num parquet file cache requests created");
    }
}

pub fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    let partition_key = data_types::PartitionKey::from(parquet_file.chunk_time.to_string());
    let partition_id = PartitionHashId::new(data_types::TableId::new(0), &partition_key);

    let chunk_stats = create_chunk_statistics(
        Some(parquet_file.row_count as usize),
        table_schema,
        Some(parquet_file.timestamp_min_max()),
        &NoColumnRanges,
    );

    let location = ObjPath::parse(&parquet_file.path).expect("path should be parseable");

    let parquet_exec = DataSourceExecInput {
        object_store_url,
        object_meta: ObjectMeta {
            location,
            last_modified: Default::default(),
            size: parquet_file.size_bytes,
            e_tag: None,
            version: None,
        },
        object_store,
    };

    ParquetChunk {
        schema: table_schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id,
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(chunk_order),
        parquet_exec,
    }
}

#[async_trait]
impl Bufferer for WriteBufferImpl {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(
            database,
            lp,
            ingest_time,
            accept_partial,
            precision,
            no_sync,
        )
        .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::clone(&self.wal)
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.buffer.persisted_parquet_files(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        self.buffer.persisted_snapshot_notify_rx()
    }
}

impl ChunkContainer for WriteBufferImpl {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(db_schema, table_def, filter, projection, ctx)
    }
}

#[async_trait::async_trait]
impl DistinctCacheManager for WriteBufferImpl {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        Arc::clone(&self.distinct_cache)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for WriteBufferImpl {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }
}

impl WriteBuffer for WriteBufferImpl {}

pub async fn check_mem_and_force_snapshot_loop(
    write_buffer: Arc<WriteBufferImpl>,
    memory_threshold_bytes: usize,
    check_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(check_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            check_mem_and_force_snapshot(&write_buffer, memory_threshold_bytes).await;
        }
    })
}

async fn check_mem_and_force_snapshot(
    write_buffer: &Arc<WriteBufferImpl>,
    memory_threshold_bytes: usize,
) {
    let current_buffer_size_bytes = write_buffer.buffer.get_total_size_bytes();
    debug!(
        current_buffer_size_bytes,
        memory_threshold_bytes, "checking buffer size and snapshotting"
    );

    if current_buffer_size_bytes >= memory_threshold_bytes {
        warn!(
            current_buffer_size_bytes,
            memory_threshold_bytes, "forcing snapshot as buffer size > mem threshold"
        );

        let wal = Arc::clone(&write_buffer.wal);

        let cleanup_after_snapshot = wal.force_flush_buffer().await;

        // handle snapshot cleanup outside of the flush loop
        if let Some((snapshot_complete, snapshot_info, snapshot_permit)) = cleanup_after_snapshot {
            let snapshot_wal = Arc::clone(&wal);
            tokio::spawn(async move {
                let snapshot_details = snapshot_complete.await.expect("snapshot failed");
                assert_eq!(snapshot_info, snapshot_details);

                snapshot_wal
                    .cleanup_snapshot(snapshot_info, snapshot_permit)
                    .await;
            });
        }
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests;
