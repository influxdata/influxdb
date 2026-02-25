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
use object_store::{ObjectStore, path::Path as ObjPath};
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

        // Use early-exit streaming: stop listing as soon as we hit an already-processed snapshot.
        // Due to inverted sequence numbering (u64::MAX - seq), newer files have lexicographically
        // smaller paths and come FIRST in listings. Once we encounter a file with
        // seq <= completed_seq, all remaining files are also already processed.
        let completed_seq = last_conversion_completed
            .as_ref()
            .map(|c| c.last_sequence_number);
        let prefix = snapshot_info_prefix.obj_path();

        debug!(prefix = %prefix, ?completed_seq, "Listing snapshot files up to latest processed");
        let list_start = std::time::Instant::now();

        let mut stream = self.inner.object_store.list(Some(&prefix));
        let mut snapshot_metas = Vec::new();

        while let Some(result) = stream
            .try_next()
            .await
            .map_err(TableIndexCacheError::ListMetasError)?
        {
            let seq = SnapshotInfoFilePath::parse_sequence_number(result.location.as_ref());
            match (seq, completed_seq) {
                (Some(s), Some(c)) if s.as_u64() <= c.as_u64() => {
                    debug!(
                        seq = s.as_u64(),
                        completed = c.as_u64(),
                        files_collected = snapshot_metas.len(),
                        "Found latest processed snapshot, done listing"
                    );
                    break;
                }
                _ => snapshot_metas.push(result),
            }
        }

        debug!(
            count = snapshot_metas.len(),
            duration_ms = list_start.elapsed().as_millis(),
            "Snapshot listing completed"
        );

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
        let mut processed_count = 0;

        info!(
            snapshots_to_process = snapshot_metas.len(),
            "Splitting snapshots into table index snapshots"
        );
        for meta in snapshot_metas {
            let object_store = Arc::clone(&self.inner.object_store);
            let sem = Arc::clone(&sem);
            let path = SnapshotInfoFilePath::from_path(meta.location.clone())
                .map_err(|e| TableIndexCacheError::ParseSnapshotPathError(anyhow::anyhow!(e)))?;

            debug!(
                snapshot_path = %meta.location,
                "Splitting persisted snapshot into table index snapshots"
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
                        snapshot_path = %object_meta.location,
                        "Finished splitting snapshot into table index snapshots",
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

        // Collect unique table IDs that have snapshots but no index yet
        let tables_without_indices: HashSet<TableIndexId> = self
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
mod tests;
