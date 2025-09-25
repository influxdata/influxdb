//! LRU Cache for TableIndex objects.
//!
//! This module provides a caching layer for TableIndex objects to reduce
//! object store access and improve query performance.

use std::{
    collections::BTreeSet,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use futures::TryStreamExt;
use hashbrown::{HashMap, HashSet};
use object_store::{ObjectMeta, ObjectStore, path::Path as ObjPath};
use object_store_utils::RetryableObjectStore;
use observability_deps::tracing::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinSet,
};

use influxdb3_id::{DbId, TableId, TableIndexId};
use influxdb3_wal::SnapshotSequenceNumber;

use crate::{
    ParquetFile,
    paths::{
        SnapshotInfoFilePath, TableIndexConversionCompletedPath, TableIndexPath,
        TableIndexSnapshotPath,
    },
    table_index::{CoreTableIndex, IndexMetadata, TableIndex, TableIndexSnapshot},
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableIndexCacheError {
    #[error("Object store operation failed")]
    ObjectStore(#[source] object_store::Error),

    #[error("JSON serialization/deserialization failed")]
    Json(#[source] serde_json::Error),

    #[error("TableIndex operation failed")]
    TableIndex(#[source] crate::table_index::TableIndexError),

    #[error("Object meta is missing filename")]
    MissingFilename,

    #[error("Failed to parse snapshot sequence number from filename")]
    InvalidSnapshotSequenceNumber,

    #[error("Unexpected error")]
    Unexpected(#[source] anyhow::Error),

    #[error("Table snapshot persistence task failed")]
    TableSnapshotPersistenceTaskFailed(#[source] tokio::task::JoinError),

    #[error("Failed to list table indices from object store")]
    ListIndices(#[source] object_store::Error),

    #[error("Failed to parse table index path from object store path")]
    TableIndexPath(#[source] crate::paths::PathError),

    #[error("Failed to list table index snapshots from object store")]
    ListSnapshots(#[source] object_store::Error),

    #[error("Failed to parse table index snapshot path from object store path")]
    TableIndexSnapshotPath(#[source] crate::paths::PathError),

    #[error("Failed to update table index: join task failed")]
    UpdateTaskFailed(#[source] tokio::task::JoinError),

    #[error("Failed to list object store metadata")]
    ListMetasError(#[source] object_store::Error),

    #[error("Failed to load table index conversion marker bytes from object store")]
    LoadConversionMarkerBytesError(#[source] object_store::Error),

    #[error("Failed to serialize initial table index conversion marker")]
    SerializeConversionMarkerError(#[source] serde_json::Error),

    #[error("Failed to put initial table index conversion marker to object store")]
    PutConversionMarkerError(#[source] object_store::Error),

    #[error("Failed to parse snapshot info file path")]
    ParseSnapshotPathError(#[source] anyhow::Error),

    #[error("Failed to split persisted snapshot into table snapshots")]
    SplitPersistedSnapshotError(#[source] crate::table_index::TableIndexError),

    #[error("Failed to load table index from object store")]
    LoadTableIndexFromObjectStoreError(#[source] crate::table_index::TableIndexError),

    #[error("Failed to update table index from object store")]
    UpdateTableIndexFromObjectStoreError(#[source] crate::table_index::TableIndexError),

    #[error("Failed to create table index from object store")]
    CreateTableIndexFromObjectStoreError(#[source] crate::table_index::TableIndexError),

    #[error("Failed to delete parquet file {path} from object store")]
    DeleteParquetFile {
        path: String,
        #[source]
        source: object_store::Error,
    },

    #[error("Failed to delete table index {table_id} from object store")]
    DeleteTableIndex {
        table_id: TableIndexId,
        #[source]
        source: object_store::Error,
    },
}

pub type Result<T> = std::result::Result<T, TableIndexCacheError>;

/// Configuration for the TableIndexCache
#[derive(Debug, Clone, Copy)]
pub struct TableIndexCacheConfig {
    /// Maximum number of entries to cache (None = unlimited)
    pub max_entries: Option<usize>,
    /// Limit the concurrency of cache <-> object store operations.
    pub concurrency_limit: usize,
}

impl Default for TableIndexCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: None,
            concurrency_limit: 20,
        }
    }
}

/// A wrapper around a TableIndex that tracks access time for LRU eviction.
#[derive(Debug, Clone)]
struct CachedTableIndex {
    inner: Arc<CachedTableIndexInner>,
}

impl CachedTableIndex {
    fn new(index: CoreTableIndex) -> Self {
        let now = Instant::now();
        let last_accessed = now.duration_since(*TABLE_INDEX_CACHE_START_TIME);
        let last_accessed = last_accessed.as_nanos() as u64;
        Self {
            inner: Arc::new(CachedTableIndexInner {
                index: RwLock::new(index),
                last_access_time_since_start_ns: AtomicU64::new(last_accessed),
            }),
        }
    }

    async fn touch(&self) {
        let now = Instant::now();
        let last_accessed = now.duration_since(*TABLE_INDEX_CACHE_START_TIME);
        let last_accessed = last_accessed.as_millis() as u64;
        self.inner
            .last_access_time_since_start_ns
            .store(last_accessed, Ordering::Relaxed);
    }

    // Returns the length of time (ms) that this entry has been in the cache without being accessed.
    //
    // We return this value to make code using this method less confusing when sorting.
    //
    // With the stale duration, sorted lists of `CoreTableIndex` (eg calculated during cache
    // eviction) will be sorted in order of newest to oldest when sorted in ascending order.
    async fn stale_duration_ns(&self) -> u64 {
        let duration_since_start = TABLE_INDEX_CACHE_START_TIME.elapsed();
        let load_access_time_since_start = self
            .inner
            .last_access_time_since_start_ns
            .load(Ordering::Relaxed);

        let load_access_duration = Duration::from_nanos(load_access_time_since_start);
        (duration_since_start - load_access_duration).as_nanos() as u64
    }
}

#[async_trait::async_trait]
impl TableIndex for CachedTableIndex {
    async fn id(&self) -> TableIndexId {
        let index = self.inner.index.read().await;
        index.id().await
    }

    async fn latest_snapshot_sequence_number(&self) -> SnapshotSequenceNumber {
        let index = self.inner.index.read().await;
        index.latest_snapshot_sequence_number().await
    }

    async fn metadata(&self) -> IndexMetadata {
        self.touch().await;
        let index = self.inner.index.read().await;
        let table_id = index.id().await;
        trace!(table_id = %table_id, method = "metadata", "Updating last accessed time");
        index.metadata().await
    }

    async fn parquet_files(
        &self,
    ) -> Box<dyn Iterator<Item = Arc<ParquetFile>> + Send + Sync + 'static> {
        self.touch().await;
        let index = self.inner.index.read().await;
        index.parquet_files().await
    }
}

#[derive(Debug)]
struct CachedTableIndexInner {
    index: RwLock<CoreTableIndex>,
    // NOTE: we use an atomic here rather than storing this value in the RwLock because it's
    // possible that potentially frequently-used operations on the `CachedTableIndex` are used
    // without needing to access the inner index itself (eg, using this value to sort table indices
    // during LRU eviction).
    last_access_time_since_start_ns: AtomicU64,
}

// This is used to keep track of how long each cache entry has been in the cache. It should be safe
// as a static value shared among all [`TableIndexCache`] instances because this value itself
// doesn't matter as much as the relative difference between it and the access times of
// [`CachedTableIndex`] instances.
//
// The relative difference between those access times and this time is all we need because it's
// those differences we use to make LRU cache eviction decisions.
static TABLE_INDEX_CACHE_START_TIME: LazyLock<Instant> = LazyLock::new(Instant::now);

type NodeId = String;
type TableIndicesInner = HashMap<TableId, CachedTableIndex>;
type DbIndicesInner = HashMap<(NodeId, DbId), TableIndicesInner>;

/// A cache for [`TableIndex`] objects with LRU-based eviction.
///
/// In order to be useful as a cache, this needs to be the main interface through which other parts
/// of the system (eg retention period handling, hard delete handling, anything else that nees to
/// know what gen1 parquet files are available for a particular table) access `TableIndex`-related
/// operations.
///
/// This cache reduces object store access by keeping frequently accessed table indices in memory.
///
/// # Memory Consumption
///
/// The main way we limit memory consumption is by enforcing a maximum number of index entries
/// specific for a given instance of this cache. This should be considered a coarse-grained
/// enforcement mechanism since individual tables may become quite large; however, table and
/// database level memory consumption may be automatically limited using retention policies.
///
/// (Of course, table indexes and this cache are kind of necessary to fully implement retention
/// policies so they kind of mutually rely on each other).
#[derive(Clone, Debug)]
pub struct TableIndexCache {
    inner: Arc<TableIndexCacheInner>,
}

#[derive(Debug)]
struct TableIndexCacheInner {
    node_identifier_prefix: String,
    indices: RwLock<DbIndicesInner>,
    config: TableIndexCacheConfig,
    object_store: Arc<dyn ObjectStore>,
}

/// This data structure contains information about the state of the database index at the time that
/// full snapshot -> table index conversion has happened.
#[derive(Debug, Eq, PartialEq, Deserialize, Serialize)]
struct TableIndexConversionCompleted {
    last_sequence_number: SnapshotSequenceNumber,
}

async fn list_metas(
    object_store: Arc<dyn ObjectStore>,
    path: Arc<ObjPath>,
    offset: Option<ObjPath>,
) -> Result<Vec<ObjectMeta>> {
    trace!(prefix = %path.as_ref(), has_offset = offset.is_some(), "Listing object store paths");

    let list_start = std::time::Instant::now();

    let stream = object_store.list_with_default_retries(
        Some(path.as_ref()),
        offset.as_ref(),
        format!("Listing object store paths at prefix {}", path.as_ref()),
    );

    let results: Vec<ObjectMeta> = stream
        .try_collect::<Vec<ObjectMeta>>()
        .await
        .map_err(TableIndexCacheError::ListMetasError)?;

    trace!(
        count = results.len(),
        duration_ms = list_start.elapsed().as_millis(),
        "Object store listing completed"
    );

    Ok(results)
}

impl TableIndexCache {
    /// Create a new TableIndexCache with the given configuration
    pub fn new(
        node_identifier_prefix: String,
        config: TableIndexCacheConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        // initialize table index cache start time
        let _ = *TABLE_INDEX_CACHE_START_TIME;

        Self {
            inner: Arc::new(TableIndexCacheInner {
                indices: RwLock::new(HashMap::new()),
                config,
                object_store,
                node_identifier_prefix,
            }),
        }
    }

    /// Converts PersistedSnapshot files into table-specific TableIndexSnapshot files.
    ///
    /// This method is part of the migration strategy from the old snapshot format (where all
    /// tables were stored in a single PersistedSnapshot) to the new format (where each table
    /// has its own TableIndexSnapshot). It runs during system startup to ensure any unprocessed
    /// snapshots are converted.
    ///
    /// # Behavior
    ///
    /// 1. **Check conversion status**: Reads the TableIndexConversionCompleted marker to determine
    ///    which snapshots have already been processed. If no marker exists, all snapshots will
    ///    be processed.
    ///
    /// 2. **Load unprocessed snapshots**: Lists all PersistedSnapshot files that have a sequence
    ///    number greater than the last processed one (or all snapshots if no marker exists).
    ///
    /// 3. **Split snapshots**: For each unprocessed snapshot:
    ///    - Loads the PersistedSnapshot from object store
    ///    - Splits it into individual TableIndexSnapshot files (one per table)
    ///    - Persists each TableIndexSnapshot to object store
    ///    - Uses concurrent processing with retry logic for reliability
    ///
    /// 4. **Update marker**: After successfully processing all snapshots, updates the
    ///    TableIndexConversionCompleted marker with the sequence number of the last
    ///    processed snapshot.
    ///
    /// # Error Handling
    ///
    /// - Returns early if no snapshots need processing
    /// - Uses exponential backoff retry (up to 10 attempts) for each snapshot
    /// - Propagates errors if any snapshot fails to process after retries
    ///
    /// # Concurrency
    ///
    /// Uses a semaphore to limit concurrent snapshot processing based on the
    /// `config.concurrency_limit` setting.
    async fn split_persisted_snapshots_to_table_index_snapshots(&self) -> Result<()> {
        let start_time = std::time::Instant::now();
        info!("creating table indices from split snapshots");
        let conversion_complet_path =
            TableIndexConversionCompletedPath::new(self.inner.node_identifier_prefix.as_str());
        let snapshot_info_prefix = SnapshotInfoFilePath::dir(&self.inner.node_identifier_prefix);

        let last_conversion_completed: Option<TableIndexConversionCompleted> = match self
            .inner
            .object_store
            .get_with_default_retries(
                conversion_complet_path.as_ref(),
                "Loading table index conversion marker".to_string(),
            )
            .await
        {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(TableIndexCacheError::LoadConversionMarkerBytesError)?;
                let marker: TableIndexConversionCompleted = serde_json::from_slice(&bytes).unwrap();
                info!(
                    "loading snapshot object metas starting from snapshot sequence {:?}",
                    marker.last_sequence_number
                );

                Some(marker)
            }
            Err(object_store::Error::NotFound { .. }) => {
                info!("loading all snapshot object metas");
                None
            }
            Err(e) => return Err(TableIndexCacheError::LoadConversionMarkerBytesError(e)),
        };

        let mut snapshot_metas = list_metas(
            Arc::clone(&self.inner.object_store),
            Arc::new(snapshot_info_prefix.obj_path()),
            None,
        )
        .await?;

        if snapshot_metas.is_empty() {
            // if there was no last conversion completed and there are currently no snapshot metas
            // then we need to write one here for the next attempt.
            if last_conversion_completed.is_none() {
                let contents = TableIndexConversionCompleted {
                    last_sequence_number: SnapshotSequenceNumber::new(0),
                };
                let json = serde_json::to_vec_pretty(&contents)
                    .map_err(TableIndexCacheError::SerializeConversionMarkerError)?;

                self.inner
                    .object_store
                    .put_with_default_retries(
                        conversion_complet_path.as_ref(),
                        json.into(),
                        "Persisting initial table index conversion marker".to_string(),
                    )
                    .await
                    .map_err(TableIndexCacheError::PutConversionMarkerError)?;
            }

            return Ok(());
        }

        snapshot_metas.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        let last = snapshot_metas[snapshot_metas.len() - 1].clone();
        let last_seq_number = SnapshotInfoFilePath::parse_sequence_number(
            last.location
                .filename()
                .ok_or(TableIndexCacheError::MissingFilename)?,
        )
        .ok_or(TableIndexCacheError::InvalidSnapshotSequenceNumber)?;

        // set concurrency limit on persistence load & persist operations
        let sem = Arc::new(Semaphore::new(self.inner.config.concurrency_limit));
        let mut js = tokio::task::JoinSet::new();

        let mut skipped_count = 0;
        let mut processed_count = 0;

        info!("splitting snapshots into table snapshots");
        for meta in snapshot_metas {
            let object_store = Arc::clone(&self.inner.object_store);
            let sem = Arc::clone(&sem);
            let path = SnapshotInfoFilePath::from_path(meta.location.clone())
                .map_err(|e| TableIndexCacheError::ParseSnapshotPathError(anyhow::anyhow!(e)))?;

            let path_seq = SnapshotInfoFilePath::parse_sequence_number(meta.location.as_ref())
                .expect("must be able to parse sequence number from verified SnapshotFileInfoPath");

            if last_conversion_completed.as_ref().is_some_and(
                |conversion_completed: &TableIndexConversionCompleted| {
                    path_seq.as_u64() <= conversion_completed.last_sequence_number.as_u64()
                },
            ) {
                debug!(
                    snapshot_seq = %path_seq,
                    reason = "already_processed",
                    last_completed = %last_conversion_completed.as_ref().unwrap().last_sequence_number,
                    "Skipping persisted snapshot"
                );
                trace!(
                    "last persisted snapshot sequence number: {}, skipping conversion to table index snapshots {}",
                    last_seq_number.as_u64(),
                    path_seq.as_u64()
                );
                skipped_count += 1;
                // NOTE(wayne): we continue here because the completed conversion marker lets us
                // know that we have already processed the given persisted snapshot file --
                // including it in the current conversion process could corrupt our table index.
                //
                // also worth noting, I did try to implement this logic using object store offsets
                // but that wasn't working for some reason.
                continue;
            }

            debug!(
                snapshot_seq = %path_seq,
                snapshot_path = %meta.location,
                "Processing persisted snapshot"
            );

            trace!(
                snapshot_path = %meta.location,
                "Spawning snapshot processing task"
            );

            js.spawn(async move {
                let _permit = sem.acquire_owned().await;
                TableIndexSnapshot::load_split_persist(Arc::clone(&object_store), path.clone())
                    .await
                    .map_err(TableIndexCacheError::SplitPersistedSnapshotError)?;
                Ok(meta)
            });
        }

        while let Some(res) = js.join_next().await {
            match res.map_err(TableIndexCacheError::TableSnapshotPersistenceTaskFailed)? {
                Ok(object_meta) => {
                    info!(
                        "split snapshot at {:?} into table snaphots",
                        object_meta.location
                    );
                    processed_count += 1;
                }
                Err(e) => return Err(e),
            }
        }

        let contents = TableIndexConversionCompleted {
            last_sequence_number: last_seq_number,
        };
        let json = serde_json::to_vec_pretty(&contents)
            .map_err(TableIndexCacheError::SerializeConversionMarkerError)?;

        self.inner
            .object_store
            .put_with_default_retries(
                conversion_complet_path.as_ref(),
                json.into(),
                format!(
                    "Persisting final conversion marker at sequence {}",
                    last_seq_number
                ),
            )
            .await
            .map_err(TableIndexCacheError::PutConversionMarkerError)?;

        let elapsed = start_time.elapsed();
        info!(
            duration_ms = elapsed.as_millis(),
            snapshots_processed = processed_count,
            snapshots_skipped = skipped_count,
            last_sequence = %last_seq_number,
            "Completed splitting persisted snapshots"
        );

        Ok(())
    }

    /// This method is intended to run at startup to make sure that `TableIndex`s exist for each
    /// table and that each one is update to date with the latest `PersistedSnapshot`.
    pub async fn initialize(&self) -> Result<()> {
        // Load any unhandled persisted snapshots and convert them to TableIndexSnapshot
        self.split_persisted_snapshots_to_table_index_snapshots()
            .await?;

        // Load all table indices from the split snapshots
        self.update_all_from_object_store(&self.inner.node_identifier_prefix)
            .await?;

        Ok(())
    }

    /// Calculate total number of entries across all databases
    fn total_entries_count(indices: &DbIndicesInner) -> usize {
        indices.values().map(|db_map| db_map.len()).sum()
    }

    /// get the current number of cached entries
    pub(crate) async fn len(&self) -> usize {
        let indices = self.inner.indices.read().await;
        Self::total_entries_count(&indices)
    }

    /// Clear all entries from the cache
    #[cfg(test)]
    pub(crate) async fn clear(&self) {
        let mut indices = self.inner.indices.write().await;
        let count = Self::total_entries_count(&indices);
        indices.clear();
        debug!("Cleared all {} entries from cache", count);
    }

    /// Get a table index from the cache or load it from object store
    pub(crate) async fn get_or_load(&self, table_id: &TableIndexId) -> Result<Arc<dyn TableIndex>> {
        // First check if we have a cached entry (read lock)
        {
            let indices = self.inner.indices.read().await;
            if let Some(db_map) = indices.get(&(table_id.node_id().to_string(), table_id.db_id()))
                && let Some(cached) = db_map.get(&table_id.table_id())
            {
                debug!(table_id = %table_id, "Cache hit for table index");
                // Return Arc<CachedTableIndex> as Arc<dyn TableIndex>
                return Ok(Arc::new(cached.clone()) as Arc<dyn TableIndex>);
            }
        }

        // Cache miss - need to load from object store
        let current_size = self.len().await;
        debug!(
            table_id = %table_id,
            cache_size_before = current_size,
            "Cache miss for table index, loading from object store"
        );

        let load_start = std::time::Instant::now();

        // Load from object store
        let table_index_path = TableIndexPath::new(
            table_id.node_id(),
            table_id.db_id().get(),
            table_id.table_id().get(),
        );
        let index = CoreTableIndex::from_object_store(
            Arc::clone(&self.inner.object_store),
            &table_index_path,
        )
        .await
        .map_err(TableIndexCacheError::LoadTableIndexFromObjectStoreError)?;

        // Create the cached entry
        let cached_entry = CachedTableIndex::new(index);
        let cached_arc = Arc::new(cached_entry.clone());

        let mut indices = self.inner.indices.write().await;
        indices
            .entry((table_id.node_id().to_string(), table_id.db_id()))
            .or_insert_with(HashMap::new)
            .insert(table_id.table_id(), cached_entry);
        let new_size = Self::total_entries_count(&indices);
        drop(indices);

        // Now evict if necessary
        self.evict_if_full().await;

        let elapsed = load_start.elapsed();
        debug!(
            table_id = %table_id,
            load_duration_ms = elapsed.as_millis(),
            cache_size_after = new_size,
            "Loaded table index from object store"
        );

        // Return the Arc<CachedTableIndex> as Arc<dyn TableIndex>
        Ok(cached_arc as Arc<dyn TableIndex>)
    }

    /// Remove a specific entry from the cache
    async fn invalidate(&self, table_id: &TableIndexId) -> Option<CachedTableIndex> {
        let mut indices = self.inner.indices.write().await;
        let removed = if let Some(db_map) =
            indices.get_mut(&(table_id.node_id().to_string(), table_id.db_id()))
        {
            let removed = db_map.remove(&table_id.table_id());
            // Clean up empty database entries
            if db_map.is_empty() {
                indices.remove(&(table_id.node_id().to_string(), table_id.db_id()));
            }
            removed
        } else {
            None
        };
        let cache_size_after = Self::total_entries_count(&indices);

        debug!(
            table_id = %table_id,
            was_cached = removed.is_some(),
            cache_size_after,
            "Invalidated table index from cache"
        );

        // Check Arc reference count before returning
        if let Some(ref cached) = removed {
            let strong_count = Arc::strong_count(&cached.inner);
            if strong_count > 1 {
                warn!(
                    table_id = %table_id,
                    strong_count = strong_count,
                    "CachedTableIndex has multiple Arc references when being invalidated"
                );
            }
        }

        removed
    }

    /// Remove all traces of the specified database from the cache and object store for the current
    /// node.
    pub(crate) async fn purge_table(&self, db_id: &DbId, table_id: &TableId) -> Result<()> {
        // First, split new persisted snapshots to ensure we have the latest data
        self.split_persisted_snapshots_to_table_index_snapshots()
            .await?;

        // Create TableIndexId for the current node
        let table_index_id =
            TableIndexId::new(&self.inner.node_identifier_prefix, *db_id, *table_id);

        // Update the table index from object store to ensure we have the latest state
        // It's okay if this fails with NotFound - the table might not exist
        match self.update_from_object_store(&table_index_id).await {
            Ok(_) => {
                debug!(
                    ?db_id,
                    ?table_id,
                    node_prefix = %self.inner.node_identifier_prefix,
                    "Updated table index from object store before purging"
                );
            }
            Err(TableIndexCacheError::CreateTableIndexFromObjectStoreError(_)) => {
                // Table doesn't exist in object store, but we still need to check cache
                debug!(
                    ?db_id,
                    ?table_id,
                    node_prefix = %self.inner.node_identifier_prefix,
                    "Table index not found in object store, continuing with purge"
                );
            }
            Err(e) => return Err(e),
        }

        // Invalidate the cache entry and get the TableIndex if it exists
        let cached_index = self.invalidate(&table_index_id).await;

        // If we have a TableIndex, delete all its parquet files
        if let Some(cached) = cached_index {
            let index: Arc<dyn TableIndex> = Arc::new(cached) as _;

            // Delete each parquet file
            for parquet_file in index.parquet_files().await {
                let path = ObjPath::from(parquet_file.path.as_str());
                debug!(
                    path = %path,
                    "Deleting parquet file"
                );

                self.inner
                    .object_store
                    .delete_with_default_retries(&path, format!("Deleting parquet file {}", path))
                    .await
                    .map_err(|e| TableIndexCacheError::DeleteParquetFile {
                        path: path.to_string(),
                        source: e,
                    })?;
                trace!(path = %path, "Successfully deleted parquet file");
            }
        }

        // Finally, delete the table index itself from object store
        let index_path = TableIndexPath::new(
            &self.inner.node_identifier_prefix,
            db_id.get(),
            table_id.get(),
        );

        debug!(
            path = %index_path.as_ref(),
            "Deleting table index from object store"
        );

        self.inner
            .object_store
            .delete_with_default_retries(
                index_path.as_ref(),
                format!("Deleting table index for {}", table_index_id),
            )
            .await
            .map_err(|e| TableIndexCacheError::DeleteTableIndex {
                table_id: table_index_id,
                source: e,
            })?;

        info!(
            ?db_id,
            ?table_id,
            node_prefix = %self.inner.node_identifier_prefix,
            "Successfully purged table"
        );

        Ok(())
    }

    /// Remove all traces of the specified database from the cache and object store for the current
    /// node.
    pub(crate) async fn purge_db(&self, db_id: &DbId) -> Result<()> {
        // First, split new persisted snapshots to ensure we have the latest data
        self.split_persisted_snapshots_to_table_index_snapshots()
            .await?;

        // List all table indices for this database
        let db_indices_prefix =
            TableIndexPath::db_prefix(&self.inner.node_identifier_prefix, *db_id);

        debug!(
            ?db_id,
            node_prefix = %self.inner.node_identifier_prefix,
            prefix = %db_indices_prefix,
            "Listing table indices for database"
        );

        let table_paths: Vec<TableIndexPath> = self
            .inner
            .object_store
            .list(Some(&db_indices_prefix))
            .map_err(TableIndexCacheError::ListIndices)
            .try_filter_map(|meta| async move {
                match TableIndexPath::try_from(meta.location) {
                    Ok(p) => Ok(Some(p)),
                    Err(e) => {
                        // Skip invalid paths
                        debug!(
                            error = %e,
                            "Skipping invalid table index path during database purge"
                        );
                        Ok(None)
                    }
                }
            })
            .try_collect()
            .await?;

        info!(
            ?db_id,
            node_prefix = %self.inner.node_identifier_prefix,
            table_count = table_paths.len(),
            "Found tables to purge in database"
        );

        // Purge each table found
        for table_path in table_paths {
            let table_id = table_path.full_table_id();
            debug!(
                ?db_id,
                table_id = ?table_id.table_id(),
                "Purging table in database"
            );

            // Call purge_table for each table
            // Errors are propagated up - if one table fails to purge, the whole operation fails
            self.purge_table(db_id, &table_id.table_id()).await?;
        }

        // Also check the in-memory cache for any tables that might be cached but not yet persisted
        // to object store (e.g., newly created tables that haven't been snapshotted yet)
        let mut tables_to_remove = Vec::new();
        {
            let indices = self.inner.indices.read().await;
            if let Some(db_map) = indices.get(&(self.inner.node_identifier_prefix.clone(), *db_id))
            {
                for table_id in db_map.keys() {
                    tables_to_remove.push(*table_id);
                }
            }
        }

        // Remove any cached tables that weren't found in object store
        for table_id in tables_to_remove {
            debug!(
                ?db_id,
                ?table_id,
                node_prefix = %self.inner.node_identifier_prefix,
                "Removing cached table during database purge"
            );
            // Invalidate will remove from cache
            self.invalidate(&TableIndexId::new(
                &self.inner.node_identifier_prefix,
                *db_id,
                table_id,
            ))
            .await;
        }

        Ok(())
    }

    /// Purge expired parquet files from a table based on retention cutoff time.
    /// This method selectively deletes only files where max_time < cutoff_time_ns.
    pub async fn purge_expired(
        &self,
        node_id: &str,
        db_id: DbId,
        table_id: TableId,
        cutoff_time_ns: i64,
    ) -> Result<()> {
        let table_index_id = TableIndexId::new(node_id, db_id, table_id);

        // First, split new persisted snapshots to ensure we have the latest data
        self.split_persisted_snapshots_to_table_index_snapshots()
            .await?;

        // NOTE(wayne): it's not clear to me if it would be better here to hold the write lock
        // while performing the object store operations -- holding the write lock would prevent an
        // opportunity for another user (ie snapshotting) to write to the table index in object
        // store before this operation has updated all the deleted files.
        let cached_index = self.update_from_object_store(&table_index_id).await?;

        // Get the CoreTableIndex
        let core_index = cached_index.inner.index.read().await;

        // Collect expired files to delete
        let mut expired_files = Vec::new();
        let mut remaining_files = BTreeSet::new();

        // Process files to separate expired from remaining
        for file in &core_index.files {
            if file.max_time < cutoff_time_ns {
                expired_files.push(Arc::clone(file));
            } else {
                remaining_files.insert(Arc::clone(file));
            }
        }

        if expired_files.is_empty() {
            debug!(
                ?table_index_id,
                cutoff_time_ns, "No expired files found for table"
            );
            return Ok(());
        }

        info!(
            ?table_index_id,
            cutoff_time_ns,
            expired_file_count = expired_files.len(),
            remaining_file_count = remaining_files.len(),
            "Found expired files to delete"
        );

        // Delete expired parquet files from object store
        for file in &expired_files {
            let path = ObjPath::from(file.path.as_str());
            debug!(
                path = %path,
                max_time = file.max_time,
                cutoff_time_ns,
                "Deleting expired parquet file"
            );

            self.inner
                .object_store
                .delete_with_default_retries(
                    &path,
                    format!(
                        "Deleting expired parquet file {} (max_time: {}, cutoff: {})",
                        path, file.max_time, cutoff_time_ns
                    ),
                )
                .await
                .map_err(|e| TableIndexCacheError::DeleteParquetFile {
                    path: path.to_string(),
                    source: e,
                })?;
            trace!(path = %path, "Successfully deleted expired parquet file");
        }

        // Create updated table index with only remaining files
        let updated_index = CoreTableIndex {
            id: core_index.id.clone(),
            files: remaining_files,
            latest_snapshot_sequence_number: core_index.latest_snapshot_sequence_number,
            metadata: core_index.metadata,
        };

        // Drop the lock before we do more operations
        drop(core_index);

        // Persist the updated index to object store
        updated_index
            .persist(Arc::clone(&self.inner.object_store))
            .await
            .map_err(|e| {
                TableIndexCacheError::Unexpected(anyhow::anyhow!(
                    "Failed to persist updated table index: {:?}",
                    e
                ))
            })?;

        // Update the cache with the new index
        let new_cached = CachedTableIndex::new(updated_index);
        {
            let mut indices = self.inner.indices.write().await;
            let db_map = indices.entry((node_id.to_string(), db_id)).or_default();
            db_map.insert(table_id, new_cached);
        }

        info!(
            ?table_index_id,
            cutoff_time_ns,
            deleted_file_count = expired_files.len(),
            "Successfully purged expired files from table"
        );

        Ok(())
    }

    /// Update a cached index from the object store
    async fn update_from_object_store(&self, table_id: &TableIndexId) -> Result<CachedTableIndex> {
        let update_start = std::time::Instant::now();

        // Check if the entry is in cache
        let cached_index: Option<CachedTableIndex> = {
            let indices = self.inner.indices.read().await;
            if let Some(db_map) = indices.get(&(table_id.node_id().to_string(), table_id.db_id())) {
                if let Some(cached) = db_map.get(&table_id.table_id()) {
                    // Touch and get a clone of the index
                    cached.touch().await;
                    Some(cached.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        debug!(
            table_id = %table_id,
            cached = cached_index.is_some(),
            "Updating table index from object store"
        );

        let index = if let Some(cached_index) = cached_index {
            // Found in cache, update the existing index
            let mut index = cached_index.inner.index.write().await;
            index
                .update_from_object_store(Arc::clone(&self.inner.object_store), 10)
                .await
                .map_err(TableIndexCacheError::UpdateTableIndexFromObjectStoreError)?;
            drop(index);
            cached_index
        } else {
            // Not in cache, load from object store
            let table_index_path = TableIndexPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
            );
            let index = CoreTableIndex::from_object_store(
                Arc::clone(&self.inner.object_store),
                &table_index_path,
            )
            .await
            .map_err(TableIndexCacheError::CreateTableIndexFromObjectStoreError)?;

            // Create cached entry and insert
            let cached_index = CachedTableIndex::new(index);

            self.evict_if_full().await;

            let mut indices = self.inner.indices.write().await;
            indices
                .entry((table_id.node_id().to_string(), table_id.db_id()))
                .or_insert_with(HashMap::new)
                .insert(table_id.table_id(), cached_index.clone());

            cached_index
        };

        let elapsed = update_start.elapsed();
        debug!(
            table_id = %table_id,
            duration_ms = elapsed.as_millis(),
            "Completed table index update"
        );

        Ok(index)
    }

    /// Evict the oldest entries down to the configured maximum number of entries.
    async fn evict_if_full(&self) {
        let Some(max_entries) = self.inner.config.max_entries else {
            return;
        };

        let current_size = self.len().await;
        if current_size < max_entries {
            return;
        }

        // NOTE: I considered taking the approach of merge sort into a Vec<(TableIndexId, Instant)>
        // to achieve a single pass through the indices, but decided the complexity of that
        // implementation made it harder to reason about than a single pass with a final sort
        // before choosing
        let difference = current_size - max_entries;

        debug!(
            cache_size = current_size,
            max_entries = max_entries,
            evicting_count = difference,
            "Cache full, evicting least recently used entries"
        );

        let indices = self.inner.indices.read().await;
        let mut entries: Vec<(TableIndexId, u64)> = Vec::with_capacity(current_size);
        for ((node_id, db_id), db_map) in indices.iter() {
            for (table_id, cached) in db_map.iter() {
                // sorts in ascending order of youngest entries to oldest
                let stale_duration_ns = cached.stale_duration_ns().await;
                let full_id = TableIndexId::new(node_id.clone(), *db_id, *table_id);
                entries.push((full_id, stale_duration_ns));
            }
        }

        entries.sort_unstable_by_key(|(_, time)| *time);

        // Get the oldest entry's stale duration for logging
        let oldest_stale_duration_ms = entries.last().map(|(_, duration)| *duration).unwrap_or(0);

        let to_remove: HashSet<_> = entries
            .into_iter()
            .enumerate()
            .map(|(position, (key, stale_duration_ns))| {
                // stale_duration_ns is already how long since last access
                trace!(
                    table_id = %key,
                    stale_duration_ns = stale_duration_ns,
                    position_in_lru = position,
                    "Evaluating entry for eviction"
                );
                key
            })
            // start from the end (highest stale duration = least recently used)
            .rev()
            // take the first `difference` values
            .take(difference)
            .inspect(|key| {
                trace!(table_id = %key, "Evicting entry from cache");
            })
            .collect();

        drop(indices);

        let mut indices = self.inner.indices.write().await;
        for table_id in &to_remove {
            if let Some(db_map) =
                indices.get_mut(&(table_id.node_id().to_string(), table_id.db_id()))
            {
                db_map.remove(&table_id.table_id());
                // Clean up empty database entries
                if db_map.is_empty() {
                    indices.remove(&(table_id.node_id().to_string(), table_id.db_id()));
                }
            }
        }
        let remaining = Self::total_entries_count(&indices);
        drop(indices);

        debug!(
            evicted = to_remove.len(),
            remaining,
            oldest_evicted_stale_duration_ms = oldest_stale_duration_ms,
            "Completed cache eviction"
        );
    }

    /// Updates all table indices in the cache from object store.
    ///
    /// This method performs a comprehensive synchronization between the object store and the local
    /// cache, handling both updates to existing indices and creation of new ones.
    ///
    /// # Behavior
    ///
    /// 1. **List existing indices**: Scans the object store for all existing table indices under
    ///    the given node prefix.
    ///
    /// 2. **Update existing indices**: For each existing index:
    ///    - Calls `update_from_object_store` which loads any new table index snapshots
    ///    - Merges table index snapshots into the index
    ///    - Deletes merged table index snapshots from object store
    ///    - Updates the cache with the refreshed index
    ///
    /// 3. **Identify tables without indices**: Lists all table snapshots to find tables that
    ///    have snapshots but no index yet. This handles edge cases like:
    ///    - Server crashes during initial index creation
    ///    - New table index snapshots created during normal snapshotting flow
    ///
    /// 4. **Create missing indices**: For tables with only snapshots:
    ///    - Creates a new index by loading and merging all snapshots
    ///    - Stores the new index in object store
    ///    - Adds it to the cache
    ///
    /// 5. **Cache management**: After all updates, evicts old entries if the cache exceeds
    ///    its size limit.
    ///
    /// # Concurrency
    ///
    /// All operations are performed concurrently with a configurable concurrency limit
    /// (`config.concurrency_limit`) to prevent overwhelming the object store.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Object store operations fail (listing, reading, writing)
    /// - Path parsing fails for indices or snapshots
    /// - Index creation or update operations fail
    ///
    /// # Performance Considerations
    ///
    /// - In the common case where all indices exist and are up-to-date, this method only performs
    ///   listing operations
    /// - Snapshot merging happens in-memory before persisting the updated index
    /// - Concurrent processing ensures efficient use of I/O resources
    pub(crate) async fn update_all_from_object_store(
        &self,
        node_identifier_prefix: &str,
    ) -> Result<()> {
        let start_time = std::time::Instant::now();
        info!(
            node_prefix = %node_identifier_prefix,
            "Starting table index cache synchronization from object store"
        );

        // List all existing table indices
        let indices_prefix = TableIndexPath::all_db_indices_prefix(node_identifier_prefix);

        trace!(prefix = %indices_prefix, "Listing object store paths for existing indices");

        let table_index_paths: Vec<TableIndexPath> = self
            .inner
            .object_store
            .list(Some(&indices_prefix))
            .map_err(TableIndexCacheError::ListIndices)
            .try_filter_map(|meta| async move {
                match TableIndexPath::from_path(meta.location.clone()) {
                    Ok(p) => Ok(Some(p)),
                    Err(e) => Err(TableIndexCacheError::TableIndexPath(e)),
                }
            })
            .try_collect()
            .await?;

        // Process updates concurrently
        let sem = Arc::new(Semaphore::new(self.inner.config.concurrency_limit));
        let mut js = JoinSet::new();

        for table_index_path in &table_index_paths {
            let table_id = table_index_path.full_table_id();
            let sem = Arc::clone(&sem);
            let cache = self.clone();

            trace!(table_id = %table_id, task_type = "update_index", "Spawning concurrent task");

            js.spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();

                // Use the cache's update_from_object_store method which handles
                // updating existing cached entries or loading new ones
                cache.update_from_object_store(&table_id).await?;

                Ok::<_, TableIndexCacheError>(table_id)
            });
        }

        let mut updated_count = 0;
        while let Some(res) = js.join_next().await {
            match res.map_err(TableIndexCacheError::UpdateTaskFailed)? {
                Ok(table_id) => {
                    debug!("updated table index for {:?}", table_id);
                    updated_count += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // List all table snapshots to find tables that might not have an index yet.
        //
        // _Most_ tables should have an index by this point since this method is almost always only
        // called for cases when initial index creation has already happened, but it's possible to
        // imagine a scenario where that may not have happened (server crashed during index
        // creation) so this is really just here as a relatively cheap additional safety/backstop.
        //
        // This can be considered relatively cheap because in the most common case where indices
        // already exist, there should be no additional snapshots listed here. There should be
        // no unmerged snapshots listed here because the TableIndex.update_from_object_store calls
        // above delete snapshots after merging them into their respective indices.
        let snapshots_prefix = TableIndexSnapshotPath::all_snapshots_prefix(node_identifier_prefix);

        let tables_with_indices: Arc<HashSet<TableIndexId>> = Arc::new(
            table_index_paths
                .iter()
                .map(|p| p.full_table_id())
                .collect(),
        );

        // Group snapshots by table for tables without indices
        let tables_without_indices: Vec<TableIndexId> = self
            .inner
            .object_store
            .list(Some(&snapshots_prefix))
            .map_err(TableIndexCacheError::ListSnapshots)
            .try_filter_map(|meta| {
                let tables_with_indices = Arc::clone(&tables_with_indices);
                async move {
                    match TableIndexSnapshotPath::from_path(meta.location.clone()) {
                        Ok(p) => {
                            let id = p.full_table_id();
                            if tables_with_indices.contains(&id) {
                                Ok(None)
                            } else {
                                Ok(Some(id))
                            }
                        }
                        Err(e) => Err(TableIndexCacheError::TableIndexSnapshotPath(e)),
                    }
                }
            })
            .try_collect()
            .await?;

        if tables_without_indices.is_empty() {
            // Evict entries if we've exceeded the cache size limit
            self.evict_if_full().await;

            let elapsed = start_time.elapsed();
            let cache_size = self.len().await;

            info!(
                duration_ms = elapsed.as_millis(),
                total_cached = cache_size,
                indices_updated = updated_count,
                cache_capacity = ?self.inner.config.max_entries,
                "No table new table indices to be created."
            );

            return Ok(());
        }

        debug!(
            "Found {} tables without indices to create",
            tables_without_indices.len()
        );

        // Process creates concurrently
        let sem = Arc::new(Semaphore::new(self.inner.config.concurrency_limit));
        let mut js = JoinSet::new();

        for table_id in tables_without_indices {
            let sem = Arc::clone(&sem);
            let cache = self.clone();

            trace!(table_id = %table_id, task_type = "create_index", "Spawning concurrent task");

            js.spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();

                // Now load it into the cache
                cache.get_or_load(&table_id).await?;

                Ok::<_, TableIndexCacheError>((table_id, false))
            });
        }

        // wait for index updates/creations to complete
        let mut created_count = 0;

        while let Some(res) = js.join_next().await {
            match res.map_err(TableIndexCacheError::UpdateTaskFailed)? {
                Ok(table_id) => {
                    debug!("created new table index for {:?}", table_id);
                    created_count += 1;
                }
                Err(e) => return Err(e),
            }
        }

        // Evict entries if we've exceeded the cache size limit
        self.evict_if_full().await;

        let cache_size = self.len().await;
        let elapsed = start_time.elapsed();

        info!(
            duration_ms = elapsed.as_millis(),
            indices_updated = updated_count,
            indices_created = created_count,
            total_cached = cache_size,
            cache_capacity = ?self.inner.config.max_entries,
            "Completed table index cache synchronization"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use influxdb3_id::{DbId, ParquetFileId, TableId};
    use influxdb3_wal::SnapshotSequenceNumber;
    use object_store::memory::InMemory;
    use rstest::rstest;

    use crate::ParquetFile;
    use crate::table_index::{CoreTableIndex, IndexMetadata, TableIndexSnapshot};

    /// Helper function to create test ParquetFile
    fn create_test_parquet_file(file_id: ParquetFileId, size: u64, row_count: u64) -> ParquetFile {
        ParquetFile {
            id: file_id,
            path: format!("fake/test/file_{}.parquet", file_id.as_u64()),
            size_bytes: size,
            row_count,
            chunk_time: 1000,
            min_time: 1000,
            max_time: 2000,
        }
    }

    /// Helper function to create test CoreTableIndex
    fn create_test_index(table_id: TableIndexId, file_count: usize) -> CoreTableIndex {
        let mut files = BTreeSet::new();
        for i in 0..file_count {
            files.insert(Arc::new(create_test_parquet_file(
                ParquetFileId::from(i as u64),
                100 * (i as u64 + 1),
                1000 * (i as u64 + 1),
            )));
        }
        let parquet_size_bytes = files.iter().map(|f| f.size_bytes as i64).sum();
        let row_count = files.iter().map(|f| f.row_count as i64).sum();

        CoreTableIndex {
            id: table_id,
            latest_snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            files,
            metadata: IndexMetadata {
                parquet_size_bytes,
                row_count,
                min_time: 1000,
                max_time: 2000,
            },
        }
    }

    #[cfg(test)]
    mod cache_operations {
        use super::*;
        use tokio::time::{Duration, sleep};

        struct CacheTestCase {
            name: &'static str,
            config: TableIndexCacheConfig,
            initial_indices: Vec<CoreTableIndex>,
            test_steps: Vec<TestStep>,
        }

        enum TestStep {
            // Cache operations
            CacheOpGet {
                table_id: TableIndexId,
            },
            CacheOpInvalidate {
                table_id: TableIndexId,
            },
            CacheOpClear,
            CacheOpUpdate {
                table_id: TableIndexId,
            },
            // Call specific TableIndex methods
            CacheOpCallTableIndexVisitFiles {
                table_id: TableIndexId,
            },
            CacheOpCallTableIndexMetadata {
                table_id: TableIndexId,
            },
            CacheOpCallTableIndexId {
                table_id: TableIndexId,
            },
            CacheOpCallTableIndexLatestSnapshot {
                table_id: TableIndexId,
            },

            // Time tracking operations
            CacheOpSaveLastAccessedTime {
                table_id: TableIndexId,
                time_key: String,
            },
            CacheOpSleep {
                millis: u64,
            },

            // Expectations/assertions
            ExpectCacheHit {
                table_id: TableIndexId,
            },
            ExpectCacheMiss {
                table_id: TableIndexId,
            },
            ExpectCacheSize {
                count: usize,
            },
            ExpectCached {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectLastAccessedTimeUnchanged {
                table_id: TableIndexId,
                time_key: String,
            },
            ExpectLastAccessedTimeChanged {
                table_id: TableIndexId,
                time_key: String,
            },
        }

        #[rstest]
        #[case::basic_cache_hit_miss(CacheTestCase {
            name: "basic_cache_hit_miss",
            config: TableIndexCacheConfig {
                max_entries: None,
                concurrency_limit: 10,
            },
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
            ],
            test_steps: vec![
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheHit { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 },
            ],
        })]
        #[case::cache_invalidation(CacheTestCase {
            name: "cache_invalidation",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(2)), 2),
            ],
            test_steps: vec![
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 }, // Both tables are cached
                TestStep::CacheOpInvalidate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheSize { count: 1 }, // Only table 2 remains
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // Re-loaded
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheSize { count: 2 }, // Both tables cached again
            ],
        })]
        #[case::max_entry_eviction(CacheTestCase {
            name: "max_entry_eviction",
            config: TableIndexCacheConfig {
                max_entries: Some(2),
                concurrency_limit: 10,
            },
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 1),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(2)), 1),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(3)), 1),
            ],
            test_steps: vec![
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSleep { millis: 2 }, // Ensure different timestamp
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 },
                TestStep::CacheOpSleep { millis: 2 }, // Ensure different timestamp
                TestStep::CacheOpCallTableIndexMetadata { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // Access table 1 to make it more recent
                TestStep::CacheOpSleep { millis: 2 }, // Ensure different timestamp
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)) }, // Should evict table 2
                TestStep::ExpectCacheSize { count: 2 }, // Still 2, but table 2 was evicted
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)), expected: false }, // Table 1 should still be cached
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)), expected: true }, // Table 3 should be cached
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)), expected: true }, // Table 2 should be evicted
            ],
        })]
        #[case::clear_operation(CacheTestCase {
            name: "clear_operation",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 2),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(2)), 2),
            ],
            test_steps: vec![
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 },
                TestStep::CacheOpClear,
                TestStep::ExpectCacheSize { count: 0 },
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // After clear
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) }, // After clear
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 },
            ],
        })]
        #[case::invalidate_nonexistent(CacheTestCase {
            name: "invalidate_nonexistent",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 1),
            ],
            test_steps: vec![
                TestStep::ExpectCacheMiss { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheSize { count: 1 },
                TestStep::CacheOpInvalidate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(999)) }, // Non-existent
                TestStep::ExpectCacheSize { count: 1 }, // Size unchanged
                TestStep::CacheOpInvalidate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // Existing
                TestStep::ExpectCacheSize { count: 0 }, // Now empty
            ],
        })]
        #[case::update_operations(CacheTestCase {
            name: "update_operations",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 2),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(2)), 2),
            ],
            test_steps: vec![
                // Update non-cached entry (should load and cache it)
                TestStep::CacheOpUpdate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheSize { count: 1 },
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)), expected: true },
                // Get the cached entry
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheHit { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // Get after update
                // Update already cached entry (should update in place)
                TestStep::CacheOpUpdate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectCacheSize { count: 1 }, // Still just 1 entry
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)), expected: true },
                // Update another non-cached entry
                TestStep::CacheOpUpdate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCacheSize { count: 2 },
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)), expected: true },
            ],
        })]
        #[case::test_table_index_visit_files_updates_time(CacheTestCase {
            name: "test_table_index_visit_files_updates_time",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
            ],
            test_steps: vec![
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpCallTableIndexVisitFiles {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1))
                },
                TestStep::ExpectLastAccessedTimeChanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
            ],
        })]
        #[case::test_table_index_metadata_updates_time(CacheTestCase {
            name: "test_table_index_metadata_updates_time",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
            ],
            test_steps: vec![
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpCallTableIndexMetadata {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1))
                },
                TestStep::ExpectLastAccessedTimeChanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
            ],
        })]
        #[case::test_table_index_id_does_not_update_time(CacheTestCase {
            name: "test_table_index_id_does_not_update_time",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
            ],
            test_steps: vec![
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpCallTableIndexId {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1))
                },
                TestStep::ExpectLastAccessedTimeUnchanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
            ],
        })]
        #[case::test_table_index_latest_snapshot_does_not_update_time(CacheTestCase {
            name: "test_table_index_latest_snapshot_does_not_update_time",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 3),
            ],
            test_steps: vec![
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpCallTableIndexLatestSnapshot {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1))
                },
                TestStep::ExpectLastAccessedTimeUnchanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
            ],
        })]
        #[case::test_cache_operations_time_behavior(CacheTestCase {
            name: "test_cache_operations_time_behavior",
            config: TableIndexCacheConfig::default(),
            initial_indices: vec![
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(1)), 2),
                create_test_index(TableIndexId::new("node1", DbId::new(1), TableId::new(2)), 2),
            ],
            test_steps: vec![
                // Test get_or_load doesn't update time for already cached entries
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpGet { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) }, // get_or_load again
                TestStep::ExpectLastAccessedTimeUnchanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "initial".to_string()
                },

                // Test update_from_object_store updates time for cached entries
                TestStep::CacheOpSaveLastAccessedTime {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "before_update".to_string()
                },
                TestStep::CacheOpSleep { millis: 10 },
                TestStep::CacheOpUpdate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)) },
                TestStep::ExpectLastAccessedTimeChanged {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    time_key: "before_update".to_string()
                },

                // Test update_from_object_store creates new entry with current time for uncached entries
                TestStep::CacheOpUpdate { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)) },
                TestStep::ExpectCached { table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)), expected: true },
            ],
        })]
        #[tokio::test]
        #[test_log::test]
        async fn test_cache_operations(#[case] test: CacheTestCase) {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache =
                TableIndexCache::new("node1".to_string(), test.config, Arc::clone(&object_store));

            // Persist initial indices to object store
            for index in test.initial_indices {
                index.persist(Arc::clone(&object_store)).await.unwrap();
            }

            // HashMap to track saved times for comparisons
            let mut saved_times: HashMap<String, u64> = HashMap::new();

            // Execute test steps
            for step in test.test_steps {
                match step {
                    TestStep::CacheOpGet { table_id } => {
                        let _ = cache
                            .get_or_load(&table_id)
                            .await
                            .expect("failed to load table index into cache");
                    }
                    TestStep::CacheOpInvalidate { table_id } => {
                        cache.invalidate(&table_id).await;
                    }
                    TestStep::CacheOpClear => {
                        cache.clear().await;
                    }
                    TestStep::CacheOpUpdate { table_id } => {
                        cache.update_from_object_store(&table_id).await.unwrap();
                    }
                    TestStep::CacheOpCallTableIndexVisitFiles { table_id } => {
                        let index = cache.get_or_load(&table_id).await.unwrap();
                        // Consume the iterator to simulate visiting all files
                        let _: Vec<_> = index.parquet_files().await.collect();
                    }
                    TestStep::CacheOpCallTableIndexMetadata { table_id } => {
                        let index = cache.get_or_load(&table_id).await.unwrap();
                        let _ = index.metadata().await;
                    }
                    TestStep::CacheOpCallTableIndexId { table_id } => {
                        let index = cache.get_or_load(&table_id).await.unwrap();
                        let _ = index.id().await;
                    }
                    TestStep::CacheOpCallTableIndexLatestSnapshot { table_id } => {
                        let index = cache.get_or_load(&table_id).await.unwrap();
                        let _ = index.latest_snapshot_sequence_number().await;
                    }
                    TestStep::CacheOpSaveLastAccessedTime { table_id, time_key } => {
                        let indices = cache.inner.indices.read().await;
                        if let Some(db_map) =
                            indices.get(&(table_id.node_id().to_string(), table_id.db_id()))
                        {
                            if let Some(cached) = db_map.get(&table_id.table_id()) {
                                let time = cached
                                    .inner
                                    .last_access_time_since_start_ns
                                    .load(Ordering::Relaxed);
                                saved_times.insert(time_key, time);
                            } else {
                                panic!(
                                    "Cannot save last accessed time for uncached table {table_id:?}"
                                );
                            }
                        } else {
                            panic!(
                                "Cannot save last accessed time for uncached node/database {:?}/{:?}",
                                table_id.node_id(),
                                table_id.db_id()
                            );
                        }
                    }
                    TestStep::CacheOpSleep { millis } => {
                        sleep(Duration::from_millis(millis)).await;
                    }
                    TestStep::ExpectCacheHit { table_id } => {
                        let indices = cache.inner.indices.read().await;
                        let was_cached = indices
                            .get(&(table_id.node_id().to_string(), table_id.db_id()))
                            .map(|db_map| db_map.contains_key(&table_id.table_id()))
                            .unwrap_or(false);
                        assert!(
                            was_cached,
                            "Expected cache hit for {:?} in test '{}', but got cache miss",
                            table_id, test.name
                        );
                    }
                    TestStep::ExpectCacheMiss { table_id } => {
                        let indices = cache.inner.indices.read().await;
                        let was_cached = indices
                            .get(&(table_id.node_id().to_string(), table_id.db_id()))
                            .map(|db_map| db_map.contains_key(&table_id.table_id()))
                            .unwrap_or(false);
                        assert!(
                            !was_cached,
                            "Expected cache miss for {:?} in test '{}', but got cache hit",
                            table_id, test.name
                        );
                    }
                    TestStep::ExpectCacheSize { count } => {
                        let actual = cache.len().await;
                        assert_eq!(
                            actual, count,
                            "Cache size mismatch in test '{}': expected {}, got {}",
                            test.name, count, actual
                        );
                    }
                    TestStep::ExpectCached { table_id, expected } => {
                        let indices = cache.inner.indices.read().await;
                        let is_cached = indices
                            .get(&(table_id.node_id().to_string(), table_id.db_id()))
                            .map(|db_map| db_map.contains_key(&table_id.table_id()))
                            .unwrap_or(false);
                        assert_eq!(
                            is_cached, expected,
                            "Cache state mismatch for table {:?} in test '{}': expected {}, got {}",
                            table_id, test.name, expected, is_cached
                        );
                    }
                    TestStep::ExpectLastAccessedTimeUnchanged { table_id, time_key } => {
                        let saved_time = saved_times.get(&time_key).unwrap_or_else(|| {
                            panic!("No saved time for key '{time_key}' in test '{}'", test.name)
                        });

                        let indices = cache.inner.indices.read().await;
                        if let Some(db_map) =
                            indices.get(&(table_id.node_id().to_string(), table_id.db_id()))
                        {
                            if let Some(cached) = db_map.get(&table_id.table_id()) {
                                assert_eq!(
                                    cached
                                        .inner
                                        .last_access_time_since_start_ns
                                        .load(Ordering::Relaxed),
                                    *saved_time,
                                    "Expected stale duration to stay the same for table {:?} in test '{}'",
                                    table_id,
                                    test.name,
                                );
                            } else {
                                panic!(
                                    "Table {:?} not in cache for time check in test '{}'",
                                    table_id, test.name
                                );
                            }
                        } else {
                            panic!(
                                "Node/Database {:?}/{:?} not in cache for time check in test '{}'",
                                table_id.node_id(),
                                table_id.db_id(),
                                test.name
                            );
                        }
                    }
                    TestStep::ExpectLastAccessedTimeChanged { table_id, time_key } => {
                        let saved_time = saved_times.get(&time_key).unwrap_or_else(|| {
                            panic!("No saved time for key '{time_key}' in test '{}'", test.name)
                        });

                        let indices = cache.inner.indices.read().await;
                        if let Some(db_map) =
                            indices.get(&(table_id.node_id().to_string(), table_id.db_id()))
                        {
                            if let Some(cached) = db_map.get(&table_id.table_id()) {
                                assert!(
                                    cached
                                        .inner
                                        .last_access_time_since_start_ns
                                        .load(Ordering::Relaxed)
                                        != *saved_time,
                                    "expect last_access_time_since_start_ns for table {:?} to change in test '{}', but it didn't",
                                    table_id,
                                    test.name,
                                );
                            } else {
                                panic!(
                                    "Table {:?} not in cache for time check in test '{}'",
                                    table_id, test.name
                                );
                            }
                        } else {
                            panic!(
                                "Node/Database {:?}/{:?} not in cache for time check in test '{}'",
                                table_id.node_id(),
                                table_id.db_id(),
                                test.name
                            );
                        }
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod concurrent_access {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::task::JoinSet;

        struct ConcurrencyTestCase {
            name: &'static str,
            config: TableIndexCacheConfig,
            table_ids: Vec<TableIndexId>,
            concurrent_readers: usize,
            concurrent_writers: usize,
            operations_per_task: usize,
        }

        #[rstest]
        #[case::multiple_readers_same_table(ConcurrencyTestCase {
            name: "multiple_readers_same_table",
            config: TableIndexCacheConfig::default(),
            table_ids: vec![TableIndexId::new("node1", DbId::new(1), TableId::new(1))],
            concurrent_readers: 10,
            concurrent_writers: 0,
            operations_per_task: 100,
        })]
        #[case::readers_writers_different_tables(ConcurrencyTestCase {
            name: "readers_writers_different_tables",
            config: TableIndexCacheConfig::default(),
            table_ids: (0..5).map(|i| TableIndexId::new("node1", DbId::new(1), TableId::new(i))).collect(),
            concurrent_readers: 5,
            concurrent_writers: 3,
            operations_per_task: 50,
        })]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[test_log::test]
        async fn test_concurrent_access(#[case] test: ConcurrencyTestCase) {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache = Arc::new(TableIndexCache::new(
                "node1".to_string(),
                test.config,
                Arc::clone(&object_store),
            ));
            let successful_reads = Arc::new(AtomicUsize::new(0));
            let successful_writes = Arc::new(AtomicUsize::new(0));

            // Pre-populate object store with indices
            for table_id in &test.table_ids {
                let index = create_test_index(table_id.clone(), 3);
                index.persist(Arc::clone(&object_store)).await.unwrap();
            }

            let mut tasks = JoinSet::new();

            // Spawn reader tasks
            for _ in 0..test.concurrent_readers {
                let cache = Arc::clone(&cache);
                let table_ids = test.table_ids.clone();
                let reads = Arc::clone(&successful_reads);
                let ops = test.operations_per_task;

                tasks.spawn(async move {
                    for i in 0..ops {
                        let table_id = &table_ids[i % table_ids.len()];
                        let result = cache.get_or_load(table_id).await;
                        if result.is_ok() {
                            reads.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }

            // Spawn writer tasks (simulate updates)
            for _ in 0..test.concurrent_writers {
                let cache = Arc::clone(&cache);
                let table_ids = test.table_ids.clone();
                let writes = Arc::clone(&successful_writes);
                let ops = test.operations_per_task;

                tasks.spawn(async move {
                    for i in 0..ops {
                        let table_id = &table_ids[i % table_ids.len()];
                        // Simulate an update by invalidating and reloading
                        cache.invalidate(table_id).await;
                        let result = cache.get_or_load(table_id).await;
                        if result.is_ok() {
                            writes.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
            }

            // Wait for all tasks to complete
            while let Some(result) = tasks.join_next().await {
                result.expect("Task should complete successfully");
            }

            // Verify all operations completed successfully
            let total_expected_reads = test.concurrent_readers * test.operations_per_task;
            let total_expected_writes = test.concurrent_writers * test.operations_per_task;

            assert_eq!(
                successful_reads.load(Ordering::SeqCst),
                total_expected_reads,
                "Not all read operations completed successfully in test '{}'",
                test.name
            );

            assert_eq!(
                successful_writes.load(Ordering::SeqCst),
                total_expected_writes,
                "Not all write operations completed successfully in test '{}'",
                test.name
            );
        }
    }

    #[cfg(test)]
    mod update_all_from_object_store {
        use super::*;
        use crate::paths::{TableIndexPath, TableIndexSnapshotPath};

        struct UpdateAllTestCase {
            name: &'static str,
            config: TableIndexCacheConfig,
            node_prefix: &'static str,
            test_steps: Vec<UpdateTestStep>,
        }

        enum UpdateTestStep {
            // Setup operations
            SetupPersistIndex {
                table_id: TableIndexId,
                file_count: usize,
            },
            SetupPersistSnapshot {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
                file_count: usize,
            },
            SetupLoadIntoCache {
                table_id: TableIndexId,
            },

            // Main operation
            UpdateAll,

            // Verification steps
            ExpectCacheSize {
                count: usize,
            },
            ExpectTableInCache {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectIndexInObjectStore {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectSnapshotNotInObjectStore {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
            },
        }

        // Helper to create a table index snapshot
        fn create_test_snapshot(
            table_id: TableIndexId,
            seq: SnapshotSequenceNumber,
            file_count: usize,
        ) -> TableIndexSnapshot {
            let mut files = BTreeSet::new();
            for i in 0..file_count {
                files.insert(Arc::new(create_test_parquet_file(
                    ParquetFileId::from(i as u64),
                    100 * (i as u64 + 1),
                    1000 * (i as u64 + 1),
                )));
            }

            TableIndexSnapshot {
                id: table_id,
                snapshot_sequence_number: seq,
                files: files.into_iter().map(|arc| (*arc).clone()).collect(),
                removed_files: HashSet::new(),
                metadata: IndexMetadata {
                    parquet_size_bytes: 1000,
                    row_count: 100,
                    min_time: 0,
                    max_time: 1000,
                },
            }
        }

        // Helper to check if an index exists in object store
        async fn index_exists_in_store(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
        ) -> bool {
            let path = TableIndexPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
            );
            object_store.head(&path).await.is_ok()
        }

        // Helper to check if a snapshot exists in object store
        async fn snapshot_exists_in_store(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
            seq: SnapshotSequenceNumber,
        ) -> bool {
            let path = TableIndexSnapshotPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
                seq,
            );
            object_store.head(&path).await.is_ok()
        }

        #[rstest]
        #[case::update_all_with_existing_indices(UpdateAllTestCase {
            name: "update_all_with_existing_indices",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1",
            test_steps: vec![
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    file_count: 2
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    file_count: 3
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(1)),
                    file_count: 1
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 3 },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(1)),
                    expected: true
                },
            ],
        })]
        #[case::update_all_with_snapshots_only(UpdateAllTestCase {
            name: "update_all_with_snapshots_only",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1",
            test_steps: vec![
                UpdateTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(1),
                    file_count: 2
                },
                UpdateTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(1),
                    file_count: 1
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 2 },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                // Verify indices were created from snapshots
                UpdateTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                // Verify snapshots were deleted after being merged into indices
                UpdateTestStep::ExpectSnapshotNotInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(1),
                },
                UpdateTestStep::ExpectSnapshotNotInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(1),
                },
            ],
        })]
        #[case::update_all_mixed_scenario(UpdateAllTestCase {
            name: "update_all_mixed_scenario",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1",
            test_steps: vec![
                // Some existing indices
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    file_count: 2
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    file_count: 1
                },
                // Some snapshots without indices
                UpdateTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(1),
                    file_count: 3
                },
                UpdateTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(1),
                    file_count: 2
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 4 },
                // All tables should be in cache
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(2)),
                    expected: true
                },
                UpdateTestStep::ExpectSnapshotNotInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(1),
                },
                UpdateTestStep::ExpectSnapshotNotInObjectStore {
                    table_id: TableIndexId::new("node1", DbId::new(2), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(1),
                },
            ],
        })]
        #[case::update_all_with_cache_eviction(UpdateAllTestCase {
            name: "update_all_with_cache_eviction",
            config: TableIndexCacheConfig {
                max_entries: Some(2),
                concurrency_limit: 10,
            },
            node_prefix: "node1",
            test_steps: vec![
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(4)),
                    file_count: 1
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 2 }, // Only 2 due to max_entries
            ],
        })]
        #[case::update_all_with_pre_cached_entries(UpdateAllTestCase {
            name: "update_all_with_pre_cached_entries",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1",
            test_steps: vec![
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    file_count: 2
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    file_count: 1
                },
                // Pre-load one into cache
                UpdateTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1))
                },
                UpdateTestStep::ExpectCacheSize { count: 1 },
                // Add new index to object store
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)),
                    file_count: 3
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 3 }, // All tables should be cached
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(3)),
                    expected: true
                },
            ],
        })]
        #[case::update_all_empty_store(UpdateAllTestCase {
            name: "update_all_empty_store",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1",
            test_steps: vec![
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 0 },
            ],
        })]
        #[case::update_all_with_different_node_prefixes(UpdateAllTestCase {
            name: "update_all_with_different_node_prefixes",
            config: TableIndexCacheConfig::default(),
            node_prefix: "node1", // Only process node1 tables
            test_steps: vec![
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node2", DbId::new(1), TableId::new(1)),
                    file_count: 1
                },
                UpdateTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("node2", DbId::new(1), TableId::new(2)),
                    file_count: 1
                },
                UpdateTestStep::UpdateAll,
                UpdateTestStep::ExpectCacheSize { count: 2 }, // Only node1 tables
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(1)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node1", DbId::new(1), TableId::new(2)),
                    expected: true
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node2", DbId::new(1), TableId::new(1)),
                    expected: false
                },
                UpdateTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("node2", DbId::new(1), TableId::new(2)),
                    expected: false
                },
            ],
        })]
        #[tokio::test]
        #[test_log::test]
        async fn test_update_all_operations(#[case] test: UpdateAllTestCase) {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache = Arc::new(TableIndexCache::new(
                "node1".to_string(),
                test.config,
                Arc::clone(&object_store),
            ));

            for step in test.test_steps {
                match step {
                    UpdateTestStep::SetupPersistIndex {
                        table_id,
                        file_count,
                    } => {
                        let index = create_test_index(table_id, file_count);
                        index.persist(Arc::clone(&object_store)).await.unwrap();
                    }
                    UpdateTestStep::SetupPersistSnapshot {
                        table_id,
                        seq,
                        file_count,
                    } => {
                        // In the real code, snapshots are persisted by the persister
                        // For testing, we'll just create the snapshot path and put JSON data
                        let snapshot = create_test_snapshot(table_id.clone(), seq, file_count);
                        snapshot.persist(Arc::clone(&object_store)).await.unwrap();
                    }
                    UpdateTestStep::SetupLoadIntoCache { table_id } => {
                        let _ = cache
                            .get_or_load(&table_id)
                            .await
                            .expect("failed to load table index into cache");
                    }
                    UpdateTestStep::UpdateAll => {
                        cache
                            .update_all_from_object_store(test.node_prefix)
                            .await
                            .unwrap();
                    }
                    UpdateTestStep::ExpectCacheSize { count } => {
                        let actual = cache.len().await;
                        assert_eq!(
                            actual, count,
                            "Cache size mismatch in test '{}': expected {}, got {}",
                            test.name, count, actual
                        );
                    }
                    UpdateTestStep::ExpectTableInCache { table_id, expected } => {
                        let indices = cache.inner.indices.read().await;
                        let is_cached = indices
                            .get(&(table_id.node_id().to_string(), table_id.db_id()))
                            .map(|db_map| db_map.contains_key(&table_id.table_id()))
                            .unwrap_or(false);
                        assert_eq!(
                            is_cached, expected,
                            "Cache state mismatch for table {:?} in test '{}': expected {}, got {}",
                            table_id, test.name, expected, is_cached
                        );
                    }
                    UpdateTestStep::ExpectIndexInObjectStore { table_id, expected } => {
                        let exists = index_exists_in_store(&object_store, &table_id).await;
                        assert_eq!(
                            exists, expected,
                            "Index existence mismatch for table {:?} in test '{}': expected {}, got {}",
                            table_id, test.name, expected, exists
                        );
                    }
                    UpdateTestStep::ExpectSnapshotNotInObjectStore { table_id, seq } => {
                        let exists = snapshot_exists_in_store(&object_store, &table_id, seq).await;
                        assert!(
                            !exists,
                            "Snapshot for table {:?} seq {} unexpectedly found in test '{}'",
                            table_id, seq, test.name
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod split_and_update_indices {
        use super::*;
        use crate::{
            DatabaseTables, PersistedSnapshot, PersistedSnapshotVersion,
            paths::{SnapshotInfoFilePath, TableIndexPath, TableIndexSnapshotPath},
        };
        use influxdb3_catalog::catalog::CatalogSequenceNumber;
        use influxdb3_id::SerdeVecMap;

        struct InitOrUpdateTestCase {
            name: &'static str,
            config: TableIndexCacheConfig,
            node_prefix: &'static str,
            test_steps: Vec<InitOrUpdateTestStep>,
        }

        enum InitOrUpdateTestStep {
            // Setup operations
            SetupPersistSnapshot {
                seq: SnapshotSequenceNumber,
                databases: Vec<(DbId, Vec<(TableId, usize)>)>, // DB -> [(table, file_count)]
            },
            SetupPersistTableIndex {
                table_id: TableIndexId,
                file_count: usize,
            },
            SetupPersistTableSnapshot {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
                file_count: usize,
            },
            SetupPersistConversionMarker {
                last_seq_number: SnapshotSequenceNumber,
            },

            // Main operations
            SplitNewPersistedSnapshots,
            UpdateAllFromObjectStore,

            // Verification steps
            ExpectConversionMarkerExists {
                expected_last_seq: SnapshotSequenceNumber,
            },
            ExpectTableIndexExists {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectTableSnapshotExists {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
                expected: bool,
            },
            ExpectCacheSize {
                count: usize,
            },
            ExpectSnapshotFileExists {
                seq: SnapshotSequenceNumber,
                expected: bool,
            },
        }

        // Helper to create a PersistedSnapshot with multiple databases and tables
        fn create_test_persisted_snapshot(
            node_prefix: &str,
            seq: SnapshotSequenceNumber,
            databases: Vec<(DbId, Vec<(TableId, usize)>)>,
        ) -> PersistedSnapshotVersion {
            let mut db_map = SerdeVecMap::new();
            let mut next_file_id = 0u64;
            let mut total_size = 0u64;
            let mut total_rows = 0u64;

            for (db_id, tables) in databases {
                let mut table_map = SerdeVecMap::new();

                for (table_id, file_count) in tables {
                    let mut files = Vec::new();
                    for i in 0..file_count {
                        let file = ParquetFile {
                            id: ParquetFileId::from(next_file_id),
                            path: format!("test_file_{next_file_id}.parquet"),
                            size_bytes: 1000 * (i as u64 + 1),
                            row_count: 100 * (i as u64 + 1),
                            chunk_time: 0,
                            min_time: 0,
                            max_time: 1000,
                        };
                        total_size += file.size_bytes;
                        total_rows += file.row_count;
                        files.push(file);
                        next_file_id += 1;
                    }
                    table_map.insert(table_id, files);
                }

                db_map.insert(db_id, DatabaseTables { tables: table_map });
            }

            PersistedSnapshotVersion::V1(PersistedSnapshot {
                node_id: node_prefix.to_string(),
                next_file_id: ParquetFileId::from(next_file_id),
                snapshot_sequence_number: seq,
                wal_file_sequence_number: influxdb3_wal::WalFileSequenceNumber::new(seq.as_u64()),
                catalog_sequence_number: CatalogSequenceNumber::new(seq.as_u64()),
                databases: db_map,
                removed_files: SerdeVecMap::new(),
                min_time: 0,
                max_time: 1000,
                row_count: total_rows,
                parquet_size_bytes: total_size,
            })
        }

        // Helper to create a TableIndexSnapshot
        fn create_test_table_snapshot(
            table_id: TableIndexId,
            seq: SnapshotSequenceNumber,
            file_count: usize,
        ) -> TableIndexSnapshot {
            let mut files = HashSet::new();
            for i in 0..file_count {
                files.insert(ParquetFile {
                    id: ParquetFileId::from(i as u64),
                    path: format!("test_file_{i}.parquet"),
                    size_bytes: 100 * (i as u64 + 1),
                    row_count: 1000 * (i as u64 + 1),
                    chunk_time: 0,
                    min_time: 0,
                    max_time: 1000,
                });
            }

            TableIndexSnapshot {
                id: table_id,
                snapshot_sequence_number: seq,
                files,
                removed_files: HashSet::new(),
                metadata: IndexMetadata {
                    parquet_size_bytes: 1000,
                    row_count: 100,
                    min_time: 0,
                    max_time: 1000,
                },
            }
        }

        #[rstest]
        #[case::first_time_initialization_empty_store(InitOrUpdateTestCase {
            name: "first_time_initialization_empty_store",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(0),
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 0 },
            ],
        })]
        #[case::initialize_from_single_snapshot(InitOrUpdateTestCase {
            name: "initialize_from_single_snapshot",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(5),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 2), (TableId::new(2), 3)]),
                        (DbId::new(2), vec![(TableId::new(1), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(5),
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(5),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 3 },
            ],
        })]
        #[case::initialize_from_multiple_snapshots(InitOrUpdateTestCase {
            name: "initialize_from_multiple_snapshots",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(1),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(3),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1), (TableId::new(2), 2)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(7),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1)]),
                        (DbId::new(2), vec![(TableId::new(1), 3)]),
                    ],
                },
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(7),
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 3 },
            ],
        })]
        #[case::update_path_marker_exists(InitOrUpdateTestCase {
            name: "update_path_marker_exists",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Setup: conversion already done, some indices exist, new snapshots added
                InitOrUpdateTestStep::SetupPersistConversionMarker {
                    last_seq_number: SnapshotSequenceNumber::new(9),
                },
                InitOrUpdateTestStep::SetupPersistTableIndex {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    file_count: 2,
                },
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(15),
                    file_count: 1,
                },
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(15),
                    file_count: 2,
                },
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                // Verify: marker unchanged, indices updated, snapshots cleaned up
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(9),
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(15),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(15),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 2 },
            ],
        })]
        #[case::cache_eviction_during_initialize(InitOrUpdateTestCase {
            name: "cache_eviction_during_initialize",
            config: TableIndexCacheConfig {
                max_entries: Some(2),
                concurrency_limit: 10,
            },
            node_prefix: "test_node",
            test_steps: vec![
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(1),
                    databases: vec![
                        (DbId::new(1), vec![
                            (TableId::new(1), 1),
                            (TableId::new(2), 1),
                            (TableId::new(3), 1),
                            (TableId::new(4), 1),
                        ]),
                    ],
                },
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(1),
                },
                // All indices should exist in object store
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(3)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(4)),
                    expected: true,
                },
                // But only 2 should fit in cache
                InitOrUpdateTestStep::ExpectCacheSize { count: 2 },
            ],
        })]
        #[case::partial_split_recovery(InitOrUpdateTestCase {
            name: "partial_split_recovery",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Setup: Start with conversion marker indicating seq 1-5 were processed
                InitOrUpdateTestStep::SetupPersistConversionMarker {
                    last_seq_number: SnapshotSequenceNumber::new(5),
                },
                // But only some table snapshots exist (simulating partial completion)
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(5),
                    file_count: 2,
                },
                // Table 2 snapshot is missing (simulating crash during split)

                // Add new snapshots that need processing
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(6),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1), (TableId::new(2), 2)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(7),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(2), 1)]),
                        (DbId::new(2), vec![(TableId::new(1), 1)]),
                    ],
                },

                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(7),
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: true,
                },
                // Old snapshots should be gone
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(6),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(7),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 3 },
            ],
        })]
        #[case::mixed_old_and_new_format(InitOrUpdateTestCase {
            name: "mixed_old_and_new_format",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Setup: Some tables already converted (have indices and snapshots)
                InitOrUpdateTestStep::SetupPersistTableIndex {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    file_count: 3,
                },
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(10),
                    file_count: 1,
                },

                // Conversion marker indicates up to seq 8 was processed
                InitOrUpdateTestStep::SetupPersistConversionMarker {
                    last_seq_number: SnapshotSequenceNumber::new(11),
                },

                // Add new old snapshots that need conversion
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    // NOTE(wayne): this sequence number falls after what's recorded in the conversion
                    // marker we we expect it NOT to show up. it's not exactly clear how this could
                    // happen except through a bug in the PersistedSnapshot snapshotting process,
                    // so we include this as a test case as a record of the expected behavior.
                    //
                    // the most correct behavior, to prevent table indices from becoming corrupted
                    // by old persisted snapshots, is to exclude this table from the index
                    seq: SnapshotSequenceNumber::new(9),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1), (TableId::new(2), 2)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    // NOTE(wayne): similar to the above, comment, even if the snapshot sequence
                    // number is equal to that recorded in the marker, we ignore it.
                    //
                    // this makes sense because whenever we persist the snapshot conversion marker
                    // to object store we know that we have already succeeded in the conversion
                    // process.
                    seq: SnapshotSequenceNumber::new(11),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(2), 1), (TableId::new(3), 1)]),
                        (DbId::new(2), vec![(TableId::new(1), 2)]),
                    ],
                },

                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                // Verify results
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(11), // seq 11 + 1
                },
                // Table 1,1 should still exist (was already converted)
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    // see the NOTEs above in setup about how the conversion marker prevents this
                    // table form erroneously showing up in the table index.
                    expected: false,
                },
                // New tables should have indices
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    // see the NOTEs above in setup about how the conversion marker prevents this
                    // table form erroneously showing up in the table index.
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(3)),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    // see the NOTEs above in setup about how the conversion marker prevents this
                    // table form erroneously showing up in the table index.
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: false,
                },
                // Old snapshots should still exist
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(9),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(11),
                    expected: true,
                },
                // Table snapshots should be cleaned up after merge
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(10),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 1 },
            ],
        })]
        #[case::update_with_stale_cache_entries(InitOrUpdateTestCase {
            name: "update_with_stale_cache_entries",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Setup: Create initial indices
                InitOrUpdateTestStep::SetupPersistTableIndex {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    file_count: 2,
                },
                InitOrUpdateTestStep::SetupPersistTableIndex {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    file_count: 1,
                },

                // Pre-populate cache by doing an initial update
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                // Verify cache is populated
                InitOrUpdateTestStep::ExpectCacheSize { count: 2 },

                // Now add new table snapshots (simulating new data arriving)
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(20),
                    file_count: 2,
                },
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(20),
                    file_count: 3,
                },
                // Also add a completely new table
                InitOrUpdateTestStep::SetupPersistTableSnapshot {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(20),
                    file_count: 1,
                },

                // Update again - this should refresh stale cache entries
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                // Verify all indices exist and cache is updated
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: true,
                },
                // Snapshots should be cleaned up after merge
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(20),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    seq: SnapshotSequenceNumber::new(20),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(20),
                    expected: false,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 3 },
            ],
        })]
        #[case::incremental_snapshot_processing(InitOrUpdateTestCase {
            name: "incremental_snapshot_processing",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Initial setup: Process snapshots 1-5
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(1),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(3),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1), (TableId::new(2), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(5),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 2), (TableId::new(2), 1)]),
                        (DbId::new(2), vec![(TableId::new(1), 1)]),
                    ],
                },

                // First round of processing
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                // Verify initial state
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(5),
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 3 },

                // Add more snapshots (6-10)
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(6),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(3), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(8),
                    databases: vec![
                        (DbId::new(1), vec![(TableId::new(1), 1), (TableId::new(3), 2)]),
                        (DbId::new(2), vec![(TableId::new(2), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(10),
                    databases: vec![
                        (DbId::new(3), vec![(TableId::new(1), 1)]),
                    ],
                },

                // Second round of processing - should only process new snapshots
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::UpdateAllFromObjectStore,

                // Verify final state
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(10),
                },
                // All old tables should still exist
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(1)),
                    expected: true,
                },
                // New tables should exist
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(1), TableId::new(3)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(2), TableId::new(2)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::new(3), TableId::new(1)),
                    expected: true,
                },
                // Old snapshots (1-5) should still exist
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(1),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(5),
                    expected: true,
                },
                // New snapshots (6-10) should still exist
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(6),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(10),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 6 },
            ],
        })]
        #[case::multiple_splits_without_update(InitOrUpdateTestCase {
            name: "multiple_splits_without_update",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test_node",
            test_steps: vec![
                // Initial snapshots (1-3)
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(1),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                        (DbId::from(1), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(2),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                        (DbId::from(1), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(3),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                        (DbId::from(1), vec![(TableId::from(0), 2), (TableId::from(1), 2)]),
                    ],
                },

                // First split - should process snapshots 1-3
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(3),
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(1),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(2),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(3),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 0 }, // No indices created yet

                // Add more snapshots (4-5)
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(4),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                        (DbId::from(1), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                    ],
                },
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(5),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                        (DbId::from(1), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                    ],
                },

                // Second split - should only process snapshots 4-5
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(5),
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(4),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(5),
                    expected: true,
                },
                // Verify earlier snapshots still exist
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(1), TableId::from(1)),
                    seq: SnapshotSequenceNumber::new(1),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 0 }, // Still no indices created

                // Add final snapshot (6)
                InitOrUpdateTestStep::SetupPersistSnapshot {
                    seq: SnapshotSequenceNumber::new(6),
                    databases: vec![
                        (DbId::from(0), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                        (DbId::from(1), vec![(TableId::from(0), 1), (TableId::from(1), 1)]),
                    ],
                },

                // Third split - should only process snapshot 6
                InitOrUpdateTestStep::SplitNewPersistedSnapshots,
                InitOrUpdateTestStep::ExpectConversionMarkerExists {
                    expected_last_seq: SnapshotSequenceNumber::new(6),
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    seq: SnapshotSequenceNumber::new(6),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableSnapshotExists {
                    table_id: TableIndexId::new("test_node", DbId::from(1), TableId::from(1)),
                    seq: SnapshotSequenceNumber::new(6),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectCacheSize { count: 0 }, // Still no indices created

                // Finally update all indices - should create indices from all 6 snapshots
                InitOrUpdateTestStep::UpdateAllFromObjectStore,
                InitOrUpdateTestStep::ExpectCacheSize { count: 4 }, // 2 databases × 2 tables = 4 indices
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(0)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::from(0), TableId::from(1)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::from(1), TableId::from(0)),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectTableIndexExists {
                    table_id: TableIndexId::new("test_node", DbId::from(1), TableId::from(1)),
                    expected: true,
                },

                // Verify snapshot files still exist after splitting (they are not deleted)
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(1),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(2),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(3),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(4),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(5),
                    expected: true,
                },
                InitOrUpdateTestStep::ExpectSnapshotFileExists {
                    seq: SnapshotSequenceNumber::new(6),
                    expected: true,
                },
            ],
        })]
        #[tokio::test]
        #[test_log::test]
        async fn test_initialize_or_update_operations(#[case] test: InitOrUpdateTestCase) {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let cache = Arc::new(TableIndexCache::new(
                test.node_prefix.to_string(),
                test.config,
                Arc::clone(&object_store),
            ));

            for step in test.test_steps {
                match step {
                    InitOrUpdateTestStep::SetupPersistSnapshot { seq, databases } => {
                        let snapshot =
                            create_test_persisted_snapshot(test.node_prefix, seq, databases);
                        let path = SnapshotInfoFilePath::new(test.node_prefix, seq);
                        let json = serde_json::to_vec(&snapshot).unwrap();
                        object_store.put(path.as_ref(), json.into()).await.unwrap();
                    }
                    InitOrUpdateTestStep::SetupPersistTableIndex {
                        table_id,
                        file_count,
                    } => {
                        let index = create_test_index(table_id, file_count);
                        index.persist(Arc::clone(&object_store)).await.unwrap();
                    }
                    InitOrUpdateTestStep::SetupPersistTableSnapshot {
                        table_id,
                        seq,
                        file_count,
                    } => {
                        let snapshot =
                            create_test_table_snapshot(table_id.clone(), seq, file_count);
                        let path = TableIndexSnapshotPath::new(
                            table_id.node_id(),
                            table_id.db_id().get(),
                            table_id.table_id().get(),
                            seq,
                        );
                        let json = serde_json::to_vec(&snapshot).unwrap();
                        object_store.put(path.as_ref(), json.into()).await.unwrap();
                    }
                    InitOrUpdateTestStep::SetupPersistConversionMarker { last_seq_number } => {
                        let marker = TableIndexConversionCompleted {
                            last_sequence_number: last_seq_number,
                        };
                        let path = TableIndexConversionCompletedPath::new(test.node_prefix);
                        let json = serde_json::to_vec(&marker).unwrap();
                        object_store.put(path.as_ref(), json.into()).await.unwrap();
                    }
                    InitOrUpdateTestStep::SplitNewPersistedSnapshots => {
                        cache
                            .split_persisted_snapshots_to_table_index_snapshots()
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "Failed to split_new_persisted_snapshots in test '{}'",
                                    test.name
                                )
                            });
                    }
                    InitOrUpdateTestStep::UpdateAllFromObjectStore => {
                        cache
                            .update_all_from_object_store(test.node_prefix)
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "Failed to update_all_from_object_store in test '{}'",
                                    test.name
                                )
                            });
                    }
                    InitOrUpdateTestStep::ExpectConversionMarkerExists { expected_last_seq } => {
                        let path = TableIndexConversionCompletedPath::new(test.node_prefix);
                        let result = object_store.get(path.as_ref()).await;
                        assert!(
                            result.is_ok(),
                            "Conversion marker not found in test '{}'",
                            test.name
                        );
                        let bytes = result.unwrap().bytes().await.unwrap();
                        let marker: TableIndexConversionCompleted =
                            serde_json::from_slice(&bytes).unwrap();
                        assert_eq!(
                            marker.last_sequence_number, expected_last_seq,
                            "Unexpected sequence number in conversion marker in test '{}'",
                            test.name
                        );
                    }
                    InitOrUpdateTestStep::ExpectTableIndexExists { table_id, expected } => {
                        let path = TableIndexPath::new(
                            table_id.node_id(),
                            table_id.db_id().get(),
                            table_id.table_id().get(),
                        );
                        let exists = object_store.head(path.as_ref()).await.is_ok();
                        assert_eq!(
                            exists, expected,
                            "Table index for {:?} existence mismatch in test '{}': expected {}, got {}",
                            table_id, test.name, expected, exists
                        );
                    }
                    InitOrUpdateTestStep::ExpectTableSnapshotExists {
                        table_id,
                        seq,
                        expected,
                    } => {
                        let path = TableIndexSnapshotPath::new(
                            table_id.node_id(),
                            table_id.db_id().get(),
                            table_id.table_id().get(),
                            seq,
                        );
                        let exists = object_store.head(path.as_ref()).await.is_ok();
                        assert_eq!(
                            exists, expected,
                            "Table snapshot for {:?} seq {} existence mismatch in test '{}': expected {}, got {}",
                            table_id, seq, test.name, expected, exists
                        );
                    }
                    InitOrUpdateTestStep::ExpectCacheSize { count } => {
                        let actual = cache.len().await;
                        assert_eq!(
                            actual, count,
                            "Cache size mismatch in test '{}': expected {}, got {}",
                            test.name, count, actual
                        );
                    }
                    InitOrUpdateTestStep::ExpectSnapshotFileExists { seq, expected } => {
                        let path = SnapshotInfoFilePath::new(test.node_prefix, seq);
                        let exists = object_store.head(path.as_ref()).await.is_ok();
                        assert_eq!(
                            exists, expected,
                            "Snapshot file seq {} existence mismatch in test '{}': expected {}, got {}",
                            seq, test.name, expected, exists
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod purge {
        use super::*;
        use crate::ParquetFile;
        use crate::paths::{TableIndexPath, TableIndexSnapshotPath};
        use crate::table_index::{CoreTableIndex, TableIndexSnapshot};
        use futures::TryStreamExt;
        use hashbrown::HashMap;
        use influxdb3_id::ParquetFileId;
        use influxdb3_wal::SnapshotSequenceNumber;
        use object_store::memory::InMemory;
        use rstest::rstest;
        use std::sync::Arc;

        struct PurgeTestCase {
            name: &'static str,
            config: TableIndexCacheConfig,
            node_prefix: &'static str,
            test_steps: Vec<PurgeTestStep>,
        }

        enum PurgeTestStep {
            // Setup operations
            SetupPersistIndex {
                table_id: TableIndexId,
                file_count: usize,
            },
            SetupPersistSnapshot {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
                file_count: usize,
            },
            SetupLoadIntoCache {
                table_id: TableIndexId,
            },
            // Main operation
            PurgeTable {
                db_id: DbId,
                table_id: TableId,
            },
            // Verification steps
            VerifyCacheReferencedParquetFilesExist {
                // Verifies that all parquet files for purged tables referenced in the index
                // actually exist.
            },
            ExpectCacheSize {
                count: usize,
            },
            ExpectTableInCache {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectIndexInObjectStore {
                table_id: TableIndexId,
                expected: bool,
            },
            ExpectSnapshotInObjectStore {
                table_id: TableIndexId,
                seq: SnapshotSequenceNumber,
                expected: bool,
            },
            ExpectPurgeResult {
                db_id: DbId,
                table_id: TableId,
                should_succeed: bool,
            },
            VerifyParquetFilesDeleted {
                // Verifies that all parquet files for purged tables have been deleted
                // This step uses the loaded_file_paths collected during SetupLoadIntoCache
            },
            // Database-level operations
            SetupDatabase {
                db_id: DbId,
                tables: Vec<(TableId, usize)>, // (table_id, file_count)
            },
            SetupCachedTable {
                db_id: DbId,
                table_id: TableId,
                file_count: usize,
            },
            PurgeDatabase {
                db_id: DbId,
            },
            ExpectDatabaseCacheState {
                db_id: DbId,
                expected_table_count: usize,
            },
            ExpectPurgeDbResult {
                db_id: DbId,
                should_succeed: bool,
            },
            VerifyAllTablesRemoved {
                db_id: DbId,
            },
            // purge_expired operations
            SetupPersistIndexWithTimeRanges {
                table_id: TableIndexId,
                file_specs: Vec<(i64, i64)>, // (min_time, max_time) pairs
            },
            PurgeExpired {
                node_id: String,
                db_id: DbId,
                table_id: TableId,
                cutoff_time_ns: i64,
            },
            ExpectPurgeExpiredResult {
                node_id: String,
                db_id: DbId,
                table_id: TableId,
                cutoff_time_ns: i64,
            },
        }

        // Type alias for file info to reduce complexity
        type FileInfo = Vec<(ParquetFileId, i64, i64, String)>;

        struct PurgeTestRunner {
            object_store: Arc<dyn ObjectStore>,
            cache: TableIndexCache,
            loaded_file_paths: HashMap<TableIndexId, Vec<String>>,
            // Track file info for purge_expired tests
            file_time_info: HashMap<TableIndexId, FileInfo>,
        }

        impl PurgeTestRunner {
            async fn validate_steps(&mut self, test: PurgeTestCase) {
                for step in test.test_steps {
                    match step {
                        PurgeTestStep::SetupPersistIndex {
                            table_id,
                            file_count,
                        } => {
                            let _file_ids =
                                persist_table_index(&self.object_store, &table_id, file_count)
                                    .await;
                        }
                        PurgeTestStep::SetupPersistSnapshot {
                            table_id,
                            seq,
                            file_count,
                        } => {
                            persist_snapshot(&self.object_store, &table_id, seq, file_count).await;
                        }
                        PurgeTestStep::SetupLoadIntoCache { table_id } => {
                            let index = self
                                .cache
                                .get_or_load(&table_id)
                                .await
                                .expect("failed to load table index into cache");

                            // Collect file paths from the loaded index
                            let file_paths: Vec<String> = index
                                .parquet_files()
                                .await
                                .map(|file| file.path.clone())
                                .collect();
                            self.loaded_file_paths.insert(table_id.clone(), file_paths);
                        }
                        PurgeTestStep::PurgeTable { db_id, table_id } => {
                            self.cache
                                .purge_table(&db_id, &table_id)
                                .await
                                .expect("purge_table should succeed");
                        }
                        PurgeTestStep::ExpectCacheSize { count } => {
                            let actual = self.cache.len().await;
                            assert_eq!(
                                actual, count,
                                "Cache size mismatch in test '{}': expected {}, got {}",
                                test.name, count, actual
                            );
                        }
                        PurgeTestStep::ExpectTableInCache { table_id, expected } => {
                            let indices = self.cache.inner.indices.read().await;
                            let is_cached = indices
                                .get(&(table_id.node_id().to_string(), table_id.db_id()))
                                .map(|db_map| db_map.contains_key(&table_id.table_id()))
                                .unwrap_or(false);
                            assert_eq!(
                                is_cached, expected,
                                "Cache state mismatch for table {:?} in test '{}': expected {}, got {}",
                                table_id, test.name, expected, is_cached
                            );
                        }
                        PurgeTestStep::ExpectIndexInObjectStore { table_id, expected } => {
                            let exists = index_exists_in_store(&self.object_store, &table_id).await;
                            assert_eq!(
                                exists, expected,
                                "Index existence mismatch for table {:?} in test '{}': expected {}, got {}",
                                table_id, test.name, expected, exists
                            );
                        }
                        PurgeTestStep::ExpectSnapshotInObjectStore {
                            table_id,
                            seq,
                            expected,
                        } => {
                            let exists =
                                snapshot_exists_in_store(&self.object_store, &table_id, seq).await;
                            assert_eq!(
                                exists, expected,
                                "Snapshot existence mismatch for table {:?} seq {} in test '{}': expected {}, got {}",
                                table_id, seq, test.name, expected, exists
                            );
                        }
                        PurgeTestStep::ExpectPurgeResult {
                            db_id,
                            table_id,
                            should_succeed,
                        } => {
                            let result = self.cache.purge_table(&db_id, &table_id).await;
                            if should_succeed {
                                assert!(
                                    result.is_ok(),
                                    "Purge should have succeeded in test '{}' but failed with: {:?}",
                                    test.name,
                                    result.err()
                                );
                            } else {
                                assert!(
                                    result.is_err(),
                                    "Purge should have failed in test '{}' but succeeded",
                                    test.name
                                );
                            }
                        }
                        PurgeTestStep::VerifyCacheReferencedParquetFilesExist {} => {
                            // Verify that all parquet files referenced in cache actually exist
                            // Step 1: Create async channel
                            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ObjPath>();

                            // Step 2: Collect all parquet file paths from cached indices
                            {
                                let indices = self.cache.inner.indices.read().await;
                                for ((_node_id, _db_id), tables) in indices.iter() {
                                    for (_table_id, index) in tables.iter() {
                                        let tx_clone = tx.clone();
                                        for file in index.parquet_files().await {
                                            let path = ObjPath::from(file.path.clone());
                                            let _ = tx_clone.send(path);
                                        }
                                    }
                                }
                            }
                            drop(tx);

                            // Step 3: Verify each file exists
                            while let Some(path) = rx.recv().await {
                                println!("verifying path {path:?}");
                                let exists = self.object_store.head(&path).await.is_ok();
                                assert!(
                                    exists,
                                    "Parquet file '{}' referenced in cache should exist in object store for test '{}'",
                                    path, test.name
                                );
                            }
                            println!("all files verified");
                        }
                        PurgeTestStep::VerifyParquetFilesDeleted {} => {
                            // Verify that all parquet files for purged tables have been deleted
                            for (table_id, file_paths) in self.loaded_file_paths.iter() {
                                // Check if this table was purged by seeing if the index still exists
                                let index_exists =
                                    index_exists_in_store(&self.object_store, table_id).await;

                                if !index_exists {
                                    // Table was purged, so all its parquet files should be deleted
                                    for path in file_paths {
                                        let exists = self
                                            .object_store
                                            .head(&object_store::path::Path::from(path.clone()))
                                            .await
                                            .is_ok();
                                        assert!(
                                            !exists,
                                            "Parquet file '{}' should have been deleted after purging table {:?} in test '{}'",
                                            path, table_id, test.name
                                        );
                                    }
                                }
                            }
                        }
                        // Database-level operations
                        PurgeTestStep::SetupDatabase { db_id, tables } => {
                            // Create tables in object store
                            for (table_id, file_count) in tables {
                                let table_index_id =
                                    TableIndexId::new(test.node_prefix, db_id, table_id);
                                persist_table_index(
                                    &self.object_store,
                                    &table_index_id,
                                    file_count,
                                )
                                .await;
                            }
                        }
                        PurgeTestStep::SetupCachedTable {
                            db_id,
                            table_id,
                            file_count,
                        } => {
                            // Create table only in cache, not in object store
                            let table_index_id =
                                TableIndexId::new(test.node_prefix, db_id, table_id);
                            let index = create_test_table_index(&table_index_id, file_count);

                            // Manually insert into cache
                            let mut indices = self.cache.inner.indices.write().await;
                            let db_map = indices
                                .entry((test.node_prefix.to_string(), db_id))
                                .or_insert_with(HashMap::new);
                            db_map.insert(table_id, CachedTableIndex::new(index));
                        }
                        PurgeTestStep::PurgeDatabase { db_id } => {
                            self.cache
                                .purge_db(&db_id)
                                .await
                                .expect("purge_db should succeed");
                        }
                        PurgeTestStep::ExpectDatabaseCacheState {
                            db_id,
                            expected_table_count,
                        } => {
                            let indices = self.cache.inner.indices.read().await;
                            let actual_count = indices
                                .get(&(test.node_prefix.to_string(), db_id))
                                .map(|db_map| db_map.len())
                                .unwrap_or(0);
                            assert_eq!(
                                actual_count, expected_table_count,
                                "Database cache state mismatch in test '{}': expected {} tables, got {}",
                                test.name, expected_table_count, actual_count
                            );
                        }
                        PurgeTestStep::ExpectPurgeDbResult {
                            db_id,
                            should_succeed,
                        } => {
                            let result = self.cache.purge_db(&db_id).await;
                            if should_succeed {
                                assert!(
                                    result.is_ok(),
                                    "Purge should have succeeded for db {:?} in test '{}' but failed with: {:?}",
                                    db_id,
                                    test.name,
                                    result.err()
                                );
                            } else {
                                assert!(
                                    result.is_err(),
                                    "Purge should have failed for db {:?} in test '{}' but succeeded",
                                    db_id,
                                    test.name
                                );
                            }
                        }
                        PurgeTestStep::VerifyAllTablesRemoved { db_id } => {
                            // Verify no tables in cache
                            let indices = self.cache.inner.indices.read().await;
                            let has_tables = indices
                                .get(&(test.node_prefix.to_string(), db_id))
                                .map(|db_map| !db_map.is_empty())
                                .unwrap_or(false);
                            assert!(
                                !has_tables,
                                "Database {:?} should have no tables in cache after purge in test '{}'",
                                db_id, test.name
                            );

                            // Verify no tables in object store
                            let prefix = format!("{}/{}/", test.node_prefix, db_id.get());
                            let mut stream = self.object_store.list(Some(&prefix.into()));
                            let found_any = stream.try_next().await.unwrap().is_some();
                            assert!(
                                !found_any,
                                "Database {:?} should have no tables in object store after purge in test '{}'",
                                db_id, test.name
                            );
                        }
                        // Handlers for purge_expired test steps
                        PurgeTestStep::SetupPersistIndexWithTimeRanges {
                            table_id,
                            file_specs,
                        } => {
                            let file_info = persist_table_index_with_time_ranges(
                                &self.object_store,
                                &table_id,
                                file_specs,
                            )
                            .await;

                            // Store file info for later verification
                            // Combine with existing file info if present
                            self.file_time_info
                                .entry(table_id)
                                .or_insert_with(Vec::new)
                                .extend(file_info);
                        }
                        PurgeTestStep::PurgeExpired {
                            node_id,
                            db_id,
                            table_id,
                            cutoff_time_ns,
                        } => {
                            let result = self
                                .cache
                                .purge_expired(&node_id, db_id, table_id, cutoff_time_ns)
                                .await;

                            assert!(
                                result.is_ok(),
                                "purge_expired failed in test '{}': {:?}",
                                test.name,
                                result.err()
                            );
                        }
                        PurgeTestStep::ExpectPurgeExpiredResult {
                            node_id,
                            db_id,
                            table_id,
                            cutoff_time_ns,
                        } => {
                            let table_index_id = TableIndexId::new(&node_id, db_id, table_id);

                            // Verify files that should remain based on time ranges
                            if let Some(file_info) = self.file_time_info.get(&table_index_id) {
                                let (should_keep, should_delete): (FileInfo, FileInfo) = file_info
                                    .clone()
                                    .into_iter()
                                    .partition(|(_, _, max_time, _)| *max_time >= cutoff_time_ns);
                                for (_, _, max_time, path) in should_keep {
                                    let exists = self
                                        .object_store
                                        .head(&object_store::path::Path::from(path.as_str()))
                                        .await
                                        .is_ok();
                                    assert!(
                                        exists,
                                        "File {} should NOT have been deleted (max_time {} >= cutoff {}) but is missing in test '{}'",
                                        path, max_time, cutoff_time_ns, test.name
                                    );
                                }

                                for (_, _, max_time, path) in should_delete {
                                    let exists = self
                                        .object_store
                                        .head(&object_store::path::Path::from(path.as_str()))
                                        .await
                                        .is_ok();
                                    assert!(
                                        !exists,
                                        "File {} should have been deleted (max_time {} < cutoff {}) but is missing in test '{}'",
                                        path, max_time, cutoff_time_ns, test.name
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        async fn persist_table_index(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
            file_count: usize,
        ) -> Vec<ParquetFileId> {
            let index_path = TableIndexPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
            );

            let mut files = BTreeSet::new();
            let mut file_ids = Vec::new();
            let mut file_paths = Vec::new();

            for _ in 0..file_count {
                let file_id = ParquetFileId::new();
                file_ids.push(file_id);

                // Create the path that matches what the purge_table implementation expects
                let file_path = format!(
                    // NOTE(wayne): This does not match the file path format of an actual gen1
                    // parquet file but I think that's okay. For any parquet-file related
                    // operations I think we should always be getting the file path from the table
                    // index, and that's what we're using the bogus file path for in this test
                    // setup.
                    "{}/{}/{}/{:?}.parquet",
                    table_id.node_id(),
                    table_id.db_id().get(),
                    table_id.table_id().get(),
                    file_id
                );
                file_paths.push(file_path.clone());

                let parquet_file = ParquetFile {
                    id: file_id,
                    path: file_path,
                    size_bytes: 1000,
                    row_count: 100,
                    chunk_time: 1234567890,
                    min_time: 1234567890,
                    max_time: 1234567891,
                };
                files.insert(Arc::new(parquet_file));
            }

            let table_index = CoreTableIndex {
                id: table_id.clone(),
                files,
                latest_snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                metadata: crate::table_index::IndexMetadata::empty(),
            };

            let json = serde_json::to_vec(&table_index).expect("failed to serialize table index");
            object_store
                .put(&index_path, json.into())
                .await
                .expect("failed to persist table index to object store");

            // Also create the actual parquet files
            for path in file_paths {
                let content = b"fake parquet data";
                object_store
                    .put(
                        &object_store::path::Path::from(path),
                        content.to_vec().into(),
                    )
                    .await
                    .expect("failed to create parquet file in object store");
            }

            file_ids
        }

        async fn persist_table_index_with_time_ranges(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
            file_specs: Vec<(i64, i64)>, // (min_time, max_time) pairs
        ) -> Vec<(ParquetFileId, i64, i64, String)> {
            // Returns (file_id, min_time, max_time, path)
            let index_path = TableIndexPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
            );

            let mut files = BTreeSet::new();
            let mut file_info = Vec::new();

            for (i, (min_time, max_time)) in file_specs.into_iter().enumerate() {
                let file_id = ParquetFileId::new();

                let file_path = format!(
                    "{}/{}/{}/{:?}.parquet",
                    table_id.node_id(),
                    table_id.db_id().get(),
                    table_id.table_id().get(),
                    file_id
                );

                file_info.push((file_id, min_time, max_time, file_path.clone()));

                let parquet_file = ParquetFile {
                    id: file_id,
                    path: file_path.clone(),
                    size_bytes: 1000 + i as u64 * 100, // Vary size for testing
                    row_count: 100 + i as u64 * 10,
                    chunk_time: min_time,
                    min_time,
                    max_time,
                };
                files.insert(Arc::new(parquet_file));

                // Create the actual parquet file
                let content = b"fake parquet data";
                object_store
                    .put(
                        &object_store::path::Path::from(file_path),
                        content.to_vec().into(),
                    )
                    .await
                    .expect("failed to create parquet file in object store");
            }

            let table_index = CoreTableIndex {
                id: table_id.clone(),
                files,
                latest_snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                metadata: crate::table_index::IndexMetadata::empty(),
            };

            let json = serde_json::to_vec(&table_index).expect("failed to serialize table index");
            object_store
                .put(&index_path, json.into())
                .await
                .expect("failed to persist table index to object store");

            file_info
        }

        async fn persist_snapshot(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
            seq: SnapshotSequenceNumber,
            file_count: usize,
        ) {
            let snapshot_path = TableIndexSnapshotPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
                seq,
            );

            let mut files = HashSet::new();
            for i in 0..file_count {
                let file_id = ParquetFileId::new();
                let parquet_file = ParquetFile {
                    id: file_id,
                    path: format!("test_file_{i}.parquet"),
                    size_bytes: 1000,
                    row_count: 100,
                    chunk_time: 1234567890,
                    min_time: 1234567890,
                    max_time: 1234567891,
                };
                files.insert(parquet_file);
            }

            // Create the actual parquet files before moving files into snapshot
            for file in &files {
                let content = b"fake parquet data";
                object_store
                    .put(
                        &object_store::path::Path::from(file.path.clone()),
                        content.to_vec().into(),
                    )
                    .await
                    .expect("failed to create parquet file in object store");
            }

            let snapshot = TableIndexSnapshot {
                id: table_id.clone(),
                snapshot_sequence_number: seq,
                files,
                removed_files: HashSet::new(),
                metadata: crate::table_index::IndexMetadata::empty(),
            };

            let json =
                serde_json::to_vec(&snapshot).expect("failed to serialize table index snapshot");
            object_store
                .put(&snapshot_path, json.into())
                .await
                .expect("failed to persist snapshot to object store");
        }

        async fn index_exists_in_store(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
        ) -> bool {
            let path = TableIndexPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
            );
            object_store.head(&path).await.is_ok()
        }

        async fn snapshot_exists_in_store(
            object_store: &Arc<dyn ObjectStore>,
            table_id: &TableIndexId,
            seq: SnapshotSequenceNumber,
        ) -> bool {
            let path = TableIndexSnapshotPath::new(
                table_id.node_id(),
                table_id.db_id().get(),
                table_id.table_id().get(),
                seq,
            );
            object_store.head(&path).await.is_ok()
        }

        // Helper function to create a test table index
        fn create_test_table_index(table_id: &TableIndexId, file_count: usize) -> CoreTableIndex {
            let mut files = BTreeSet::new();
            for i in 0..file_count {
                let file_id = ParquetFileId::new();
                let parquet_file = ParquetFile {
                    id: file_id,
                    path: format!("test_file_{i}.parquet"),
                    size_bytes: 1000,
                    row_count: 100,
                    chunk_time: 1234567890,
                    min_time: 1234567890,
                    max_time: 1234567891,
                };
                files.insert(Arc::new(parquet_file));
            }

            CoreTableIndex {
                id: table_id.clone(),
                files,
                latest_snapshot_sequence_number: SnapshotSequenceNumber::new(0),
                metadata: crate::table_index::IndexMetadata::empty(),
            }
        }

        #[rstest]
        #[case::basic_purge_table_exists(PurgeTestCase {
            name: "basic_purge_table_exists",
            config: TableIndexCacheConfig {
                max_entries: None,
                concurrency_limit: 10,
            },
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_count: 3,
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::ExpectCacheSize { count: 1 },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                },
                PurgeTestStep::VerifyParquetFilesDeleted {},
                PurgeTestStep::ExpectCacheSize { count: 0 },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
                PurgeTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
            ],
        })]
        #[case::purge_non_existent_table(PurgeTestCase {
            name: "purge_non_existent_table",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::ExpectCacheSize { count: 0 },
                PurgeTestStep::ExpectPurgeResult {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    should_succeed: true,
                },
                PurgeTestStep::ExpectCacheSize { count: 0 },
            ],
        })]
        #[case::purge_with_snapshots(PurgeTestCase {
            name: "purge_with_snapshots",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_count: 2,
                },
                PurgeTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(100),
                    file_count: 2,
                },
                PurgeTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(200),
                    file_count: 3,
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                },
                PurgeTestStep::VerifyParquetFilesDeleted {},
                PurgeTestStep::ExpectSnapshotInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(100),
                    expected: false,
                },
                PurgeTestStep::ExpectSnapshotInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(200),
                    expected: false,
                },
                PurgeTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
            ],
        })]
        #[case::purge_idempotency(PurgeTestCase {
            name: "purge_idempotency",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_count: 2,
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                },
                PurgeTestStep::VerifyParquetFilesDeleted {},
                PurgeTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
                PurgeTestStep::ExpectPurgeResult {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    should_succeed: true,
                },
            ],
        })]
        #[case::purge_after_split_new_persisted_snapshots(PurgeTestCase {
            name: "purge_after_split_new_persisted_snapshots",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                // Setup initial index and snapshot
                PurgeTestStep::SetupPersistIndex {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_count: 2,
                },
                PurgeTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(100),
                    file_count: 2,
                },
                // Load into cache
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                // Create a new snapshot that would be discovered by split_new_persisted_snapshots
                PurgeTestStep::SetupPersistSnapshot {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(200),
                    file_count: 3,
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                // Purge - this should internally call split_new_persisted_snapshots and still delete everything
                PurgeTestStep::PurgeTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                },
                PurgeTestStep::VerifyParquetFilesDeleted {},
                // Verify all data is deleted
                PurgeTestStep::ExpectIndexInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
                PurgeTestStep::ExpectSnapshotInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(100),
                    expected: false,
                },
                PurgeTestStep::ExpectSnapshotInObjectStore {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    seq: SnapshotSequenceNumber::new(200),
                    expected: false,
                },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
            ],
        })]
        #[tokio::test]
        async fn test_purge_table(#[case] test: PurgeTestCase) {
            let object_store = Arc::new(InMemory::new());
            let cache = TableIndexCache::new(
                test.node_prefix.to_string(),
                test.config,
                Arc::clone(&object_store) as Arc<dyn ObjectStore>,
            );

            // Track loaded file paths for verification
            let loaded_file_paths: HashMap<TableIndexId, Vec<String>> = HashMap::new();
            let mut runner = PurgeTestRunner {
                object_store,
                cache,
                loaded_file_paths,
                file_time_info: HashMap::new(),
            };

            runner.validate_steps(test).await;
        }

        #[rstest]
        #[case::basic_purge_db(PurgeTestCase {
            name: "basic_purge_db",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupDatabase {
                    db_id: DbId::new(1),
                    tables: vec![(TableId::new(1), 2), (TableId::new(2), 3), (TableId::new(3), 1)],
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(2)),
                },
                PurgeTestStep::ExpectDatabaseCacheState {
                    db_id: DbId::new(1),
                    expected_table_count: 2,
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeDatabase {
                    db_id: DbId::new(1),
                },
                PurgeTestStep::VerifyAllTablesRemoved {
                    db_id: DbId::new(1),
                },
            ],
        })]
        #[case::purge_non_existent_db(PurgeTestCase {
            name: "purge_non_existent_db",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::ExpectPurgeDbResult {
                    db_id: DbId::new(999),
                    should_succeed: true,
                },
            ],
        })]
        #[case::purge_empty_db(PurgeTestCase {
            name: "purge_empty_db",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                // Create database directory but no tables
                PurgeTestStep::SetupDatabase {
                    db_id: DbId::new(1),
                    tables: vec![],
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeDatabase {
                    db_id: DbId::new(1),
                },
                PurgeTestStep::ExpectDatabaseCacheState {
                    db_id: DbId::new(1),
                    expected_table_count: 0,
                },
            ],
        })]
        #[case::cached_but_not_persisted(PurgeTestCase {
            name: "cached_but_not_persisted",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                // Setup cached table without persisting to object store
                PurgeTestStep::SetupCachedTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    file_count: 2,
                },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
                PurgeTestStep::PurgeDatabase {
                    db_id: DbId::new(1),
                },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: false,
                },
            ],
        })]
        #[case::mixed_cache_state(PurgeTestCase {
            name: "mixed_cache_state",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                // Some tables persisted, some only cached
                PurgeTestStep::SetupDatabase {
                    db_id: DbId::new(1),
                    tables: vec![(TableId::new(1), 2), (TableId::new(2), 1)],
                },
                PurgeTestStep::SetupCachedTable {
                    db_id: DbId::new(1),
                    table_id: TableId::new(3),
                    file_count: 3,
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::ExpectDatabaseCacheState {
                    db_id: DbId::new(1),
                    expected_table_count: 2, // table 1 and 3 are cached
                },
                PurgeTestStep::PurgeDatabase {
                    db_id: DbId::new(1),
                },
                PurgeTestStep::VerifyAllTablesRemoved {
                    db_id: DbId::new(1),
                },
            ],
        })]
        #[case::idempotency(PurgeTestCase {
            name: "idempotency",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupDatabase {
                    db_id: DbId::new(1),
                    tables: vec![(TableId::new(1), 2)],
                },
                PurgeTestStep::VerifyCacheReferencedParquetFilesExist{},
                PurgeTestStep::PurgeDatabase {
                    db_id: DbId::new(1),
                },
                // Second purge should not error
                PurgeTestStep::ExpectPurgeDbResult {
                    db_id: DbId::new(1),
                    should_succeed: true,
                },
            ],
        })]
        #[tokio::test]
        async fn test_purge_db(#[case] test: PurgeTestCase) {
            let object_store = Arc::new(InMemory::new());
            let cache = TableIndexCache::new(
                test.node_prefix.to_string(),
                test.config,
                Arc::clone(&object_store) as Arc<dyn ObjectStore>,
            );

            let mut runner = PurgeTestRunner {
                object_store,
                cache,
                loaded_file_paths: HashMap::new(),
                file_time_info: HashMap::new(),
            };

            runner.validate_steps(test).await;
        }

        #[rstest]
        #[case::basic_purge_expired(PurgeTestCase {
            name: "basic_purge_expired",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndexWithTimeRanges {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_specs: vec![
                        (1000, 2000), // Old file
                        (2000, 3000), // Old file
                        (5000, 6000), // New file
                        (6000, 7000), // New file
                    ],
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 4000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 4000,
                },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
            ],
        })]
        #[case::purge_expired_all_old(PurgeTestCase {
            name: "purge_expired_all_old",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndexWithTimeRanges {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_specs: vec![
                        (1000, 2000),
                        (2000, 3000),
                        (3000, 4000),
                    ],
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 5000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 5000,
                },
                // Table should still exist but with no files
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
            ],
        })]
        #[case::purge_expired_all_new(PurgeTestCase {
            name: "purge_expired_all_new",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndexWithTimeRanges {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_specs: vec![
                        (5000, 6000),
                        (6000, 7000),
                        (7000, 8000),
                    ],
                },
                PurgeTestStep::SetupLoadIntoCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                },
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 4000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 4000,
                },
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
            ],
        })]
        #[case::purge_expired_not_in_cache(PurgeTestCase {
            name: "purge_expired_not_in_cache",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndexWithTimeRanges {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_specs: vec![
                        (1000, 2000),
                        (5000, 6000),
                    ],
                },
                // Don't load into cache - purge_expired should still work
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
                // Should be in cache after operation
                PurgeTestStep::ExpectTableInCache {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    expected: true,
                },
            ],
        })]
        #[case::purge_expired_idempotent(PurgeTestCase {
            name: "purge_expired_idempotent",
            config: TableIndexCacheConfig::default(),
            node_prefix: "test-node",
            test_steps: vec![
                PurgeTestStep::SetupPersistIndexWithTimeRanges {
                    table_id: TableIndexId::new("test-node", DbId::new(1), TableId::new(1)),
                    file_specs: vec![
                        (1000, 2000),
                        (5000, 6000),
                    ],
                },
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
                // Run again - should succeed with no files deleted
                PurgeTestStep::PurgeExpired {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
                PurgeTestStep::ExpectPurgeExpiredResult {
                    node_id: "test-node".to_string(),
                    db_id: DbId::new(1),
                    table_id: TableId::new(1),
                    cutoff_time_ns: 3000,
                },
            ],
        })]
        #[tokio::test]
        async fn test_purge_expired(#[case] test: PurgeTestCase) {
            let object_store = Arc::new(InMemory::new());
            let cache = TableIndexCache::new(
                test.node_prefix.to_string(),
                test.config,
                Arc::clone(&object_store) as Arc<dyn ObjectStore>,
            );

            let mut runner = PurgeTestRunner {
                object_store,
                cache,
                loaded_file_paths: HashMap::new(),
                file_time_info: HashMap::new(),
            };

            runner.validate_steps(test).await;
        }
    }
}
