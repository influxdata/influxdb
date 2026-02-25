//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::PersistedSnapshot;
use crate::PersistedSnapshotCheckpoint;
use crate::PersistedSnapshotCheckpointVersion;
use crate::PersistedSnapshotVersion;
use crate::YearMonth;
use crate::paths::ParquetFilePath;
use crate::paths::SnapshotCheckpointPath;
use crate::paths::SnapshotInfoFilePath;
use crate::write_buffer::checkpoint::{
    FileIndex, add_snapshot_files, build_file_index, process_removed_files,
    year_month_from_timestamp_ms,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::pin_mut;
use futures_util::stream::TryStreamExt;
use futures_util::stream::{FuturesOrdered, StreamExt};
use influxdb3_cache::parquet_cache::ParquetFileDataToCache;
use influxdb3_wal::SnapshotSequenceNumber;
use iox_time::TimeProvider;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use observability_deps::tracing::{debug, error, info, trace, warn};
use parking_lot::RwLock;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use tokio::sync::Semaphore;

#[derive(Debug, thiserror::Error)]
pub enum PersisterError {
    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("tried to serialize a parquet file with no rows")]
    NoRows,

    #[error("parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("unexpected persister error: {0:?}")]
    Unexpected(#[from] anyhow::Error),

    #[error("table snapshot persistence task panicked: {0}")]
    TableSnapshotPersistenceTaskFailed(#[source] tokio::task::JoinError),

    #[error("table index error: {0}")]
    TableIndexPathError(#[source] crate::paths::PathError),

    #[error("object meta is missing filename")]
    MissingFilename,

    #[error("failed to parse snapshot sequence number from filename")]
    InvalidSnapshotSequenceNumber,
}

impl From<PersisterError> for DataFusionError {
    fn from(error: PersisterError) -> Self {
        match error {
            PersisterError::DataFusion(e) => e,
            PersisterError::ObjectStore(e) => DataFusionError::ObjectStore(Box::new(e)),
            PersisterError::ParquetError(e) => DataFusionError::ParquetError(Box::new(e)),
            _ => DataFusionError::External(Box::new(error)),
        }
    }
}

pub type Result<T, E = PersisterError> = std::result::Result<T, E>;

pub const DEFAULT_OBJECT_STORE_URL: &str = "iox://influxdb3/";
const MAX_CONCURRENT_CHECKPOINT_LOADS: usize = 10;
/// Number of checkpoints to retain per month when cleaning up (latest + previous for safety)
const CHECKPOINTS_TO_RETAIN_PER_MONTH: usize = 2;

/// Cached checkpoint with its file index for efficient incremental updates.
///
/// Storing the FileIndex alongside the checkpoint avoids rebuilding it on every
/// snapshot persist. The index is maintained incrementally as files are added/removed.
#[derive(Debug, Clone)]
struct CachedCheckpoint {
    checkpoint: PersistedSnapshotCheckpoint,
    file_index: FileIndex,
}

/// The persister is the primary interface with object storage where InfluxDB stores all Parquet
/// data, catalog information, as well as WAL and snapshot data.
#[derive(Debug)]
pub struct Persister {
    /// This is used by the query engine to know where to read parquet files from. This assumes
    /// that there is a `ParquetStorage` with an id of `influxdb3` and that this url has been
    /// registered with the query execution context.
    object_store_url: ObjectStoreUrl,
    /// The interface to the object store being used
    object_store: Arc<dyn ObjectStore>,
    /// Prefix used for all paths in the object store for this persister
    node_identifier_prefix: String,
    /// time provider
    time_provider: Arc<dyn TimeProvider>,
    pub(crate) mem_pool: Arc<dyn MemoryPool>,
    /// Interval between checkpoint creation attempts. None means checkpointing is disabled.
    checkpoint_interval: Option<Duration>,
    /// Time of the last successful checkpoint creation.
    last_checkpoint_time: RwLock<Option<Instant>>,
    /// Cached checkpoint for the current month to avoid reloading from object store.
    cached_checkpoint: RwLock<Option<CachedCheckpoint>>,
}

impl Persister {
    /// Create a new Persister.
    ///
    /// # Arguments
    /// * `object_store` - The object store to persist data to
    /// * `node_identifier_prefix` - Prefix for all paths in object store
    /// * `time_provider` - Time provider for timestamps
    /// * `checkpoint_interval` - Optional interval between checkpoint creation. None disables checkpointing.
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        node_identifier_prefix: impl Into<String>,
        time_provider: Arc<dyn TimeProvider>,
        checkpoint_interval: Option<Duration>,
    ) -> Self {
        let nip = node_identifier_prefix.into();
        Self {
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            object_store: Arc::clone(&object_store),
            node_identifier_prefix: nip.clone(),
            time_provider,
            mem_pool: Arc::new(UnboundedMemoryPool::default()),
            checkpoint_interval,
            last_checkpoint_time: RwLock::new(None),
            cached_checkpoint: RwLock::new(None),
        }
    }

    /// Get the Object Store URL
    pub fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
    }

    async fn serialize_to_parquet(
        &self,
        batches: SendableRecordBatchStream,
    ) -> Result<ParquetBytes> {
        serialize_to_parquet(Arc::clone(&self.mem_pool), batches).await
    }

    /// Get the host identifier prefix
    pub fn node_identifier_prefix(&self) -> &str {
        &self.node_identifier_prefix
    }

    /// Gets the latest snapshot sequence number from the object store.
    ///
    /// Returns None if no snapshots exist. This is a lightweight operation that
    /// only loads the most recent snapshot to extract its sequence number.
    pub async fn get_latest_snapshot_sequence(&self) -> Result<Option<SnapshotSequenceNumber>> {
        let snapshots = self.load_snapshots(1).await?;
        Ok(snapshots.first().map(|s| match s {
            PersistedSnapshotVersion::V1(snapshot) => snapshot.snapshot_sequence_number,
        }))
    }

    /// Loads the most recently persisted N snapshot parquet file lists from object storage.
    ///
    /// This is intended to be used on server start.
    pub async fn load_snapshots(
        &self,
        mut most_recent_n: usize,
    ) -> Result<Vec<PersistedSnapshotVersion>> {
        trace!(
            most_recent_n,
            node_identifier_prefix = %self.node_identifier_prefix,
            "load_snapshots: starting"
        );
        let mut futures = FuturesOrdered::new();
        let mut offset: Option<ObjPath> = None;

        while most_recent_n > 0 {
            let count = if most_recent_n > 1000 {
                most_recent_n -= 1000;
                1000
            } else {
                let count = most_recent_n;
                most_recent_n = 0;
                count
            };

            let mut snapshot_list = if let Some(offset) = offset {
                self.object_store.list_with_offset(
                    Some(&SnapshotInfoFilePath::dir(&self.node_identifier_prefix)),
                    &offset,
                )
            } else {
                self.object_store.list(Some(&SnapshotInfoFilePath::dir(
                    &self.node_identifier_prefix,
                )))
            };

            // Why not collect into a Result<Vec<ObjectMeta>, object_store::Error>>
            // like we could with Iterators? Well because it's a stream it ends up
            // using different traits and can't really do that. So we need to loop
            // through to return any errors that might have occurred, then do an
            // unstable sort (which is faster and we know won't have any
            // duplicates) since these can arrive out of order, and then issue gets
            // on the n most recent snapshots that we want and is returned in order
            // of the moste recent to least.
            let mut list = Vec::new();
            while let Some(item) = snapshot_list.next().await {
                list.push(item?);
            }

            list.sort_unstable_by(|a, b| a.location.cmp(&b.location));

            let len = list.len();
            let end = if len <= count { len } else { count };

            async fn get_snapshot(
                location: ObjPath,
                last_modified: DateTime<Utc>,
                object_store: Arc<dyn ObjectStore>,
            ) -> Result<(usize, PersistedSnapshotVersion)> {
                let bytes = object_store.get(&location).await?.bytes().await?;
                let size = bytes.len();
                let mut snapshot: PersistedSnapshotVersion = serde_json::from_slice(&bytes)?;
                match &mut snapshot {
                    PersistedSnapshotVersion::V1(ps) => {
                        ps.persisted_at = Some(last_modified.timestamp_millis());
                    }
                }

                Ok((size, snapshot))
            }

            trace!(count = end, "load_snapshots: queueing snapshot fetches");
            for item in &list[0..end] {
                futures.push_back(get_snapshot(
                    item.location.clone(),
                    item.last_modified,
                    Arc::clone(&self.object_store),
                ));
            }

            if end == 0 {
                break;
            }

            // Get the last path in the array to use as an offset. This assumes
            // we sorted the list as we can't guarantee otherwise the order of
            // the list call to the object store.
            offset = Some(list[end - 1].location.clone());
        }

        trace!(
            pending_fetches = futures.len(),
            "load_snapshots: fetching snapshot contents"
        );
        let mut results = Vec::new();
        let mut total_bytes = 0usize;
        while let Some(result) = futures.next().await {
            let (size, snapshot) = result?;
            total_bytes += size;
            results.push(snapshot);
        }
        trace!(
            count = results.len(),
            total_bytes, "load_snapshots: completed fetching and deserializing"
        );
        Ok(results)
    }

    /// Loads a Parquet file from ObjectStore
    #[cfg(test)]
    pub async fn load_parquet_file(&self, path: ParquetFilePath) -> Result<Bytes> {
        Ok(self.object_store.get(&path).await?.bytes().await?)
    }

    /// Persists the snapshot file.
    ///
    /// After successful persistence, this will:
    /// 1. Update the cached checkpoint incrementally (if checkpointing enabled)
    /// 2. Persist the checkpoint if the checkpoint interval has elapsed
    pub async fn persist_snapshot(
        &self,
        persisted_snapshot: &PersistedSnapshotVersion,
    ) -> Result<()> {
        let PersistedSnapshotVersion::V1(snapshot) = persisted_snapshot;
        let seq = snapshot.snapshot_sequence_number;
        let snapshot_file_path =
            SnapshotInfoFilePath::new(self.node_identifier_prefix.as_str(), seq);
        let json = serde_json::to_vec_pretty(persisted_snapshot)?;
        self.object_store
            .put(snapshot_file_path.as_ref(), json.into())
            .await?;

        debug!(
            snapshot_sequence_number = seq.as_u64(),
            path = %snapshot_file_path.as_ref(),
            "persist_snapshot: completed"
        );

        if self.checkpoint_interval.is_none() {
            return Ok(());
        };

        // Incrementally update cached checkpoint
        self.update_cached_checkpoint(snapshot);

        if self.should_persist_checkpoint() {
            let checkpoint_to_persist = self
                .cached_checkpoint
                .read()
                .as_ref()
                .map(|c| c.checkpoint.clone());
            let Some(checkpoint) = checkpoint_to_persist else {
                return Ok(());
            };

            match self
                .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                .await
            {
                Ok(()) => self.mark_checkpoint_created(),
                Err(e) => error!(%e, "Failed to persist checkpoint after snapshot"),
            }
        }

        Ok(())
    }

    /// Check if a checkpoint should be persisted based on elapsed time.
    fn should_persist_checkpoint(&self) -> bool {
        let Some(interval) = self.checkpoint_interval else {
            return false;
        };

        match *self.last_checkpoint_time.read() {
            None => true,
            Some(last_time) => last_time.elapsed() >= interval,
        }
    }

    /// Mark that a checkpoint was just created.
    fn mark_checkpoint_created(&self) {
        *self.last_checkpoint_time.write() = Some(Instant::now());
    }

    /// Persist the previous month's checkpoint asynchronously when month rolls over.
    ///
    /// This is called from `update_cached_checkpoint()` when a month boundary is detected.
    /// The persist is done asynchronously to avoid blocking snapshot persistence.
    /// Errors are logged but don't fail the current operation.
    fn persist_previous_month_checkpoint_async(
        &self,
        previous_checkpoint: PersistedSnapshotCheckpoint,
    ) {
        let object_store = Arc::clone(&self.object_store);
        let node_id = self.node_identifier_prefix.clone();

        tokio::spawn(async move {
            let year_month = previous_checkpoint.year_month;
            let seq = previous_checkpoint.last_snapshot_sequence_number;

            debug!(
                year_month = %year_month,
                snapshot_seq = seq.as_u64(),
                "persist_previous_month_checkpoint_async: persisting on month rollover"
            );

            let checkpoint_path = SnapshotCheckpointPath::new(&node_id, &year_month, seq);

            let json = match serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(
                previous_checkpoint,
            )) {
                Ok(json) => json,
                Err(e) => {
                    error!(%e, year_month = %year_month, "Failed to serialize previous month checkpoint");
                    return;
                }
            };

            if let Err(e) = object_store
                .put(checkpoint_path.as_ref(), json.into())
                .await
            {
                error!(
                    %e,
                    year_month = %year_month,
                    "Failed to persist previous month checkpoint on rollover"
                );
            } else {
                debug!(
                    year_month = %year_month,
                    path = %checkpoint_path.as_ref(),
                    "persist_previous_month_checkpoint_async: completed"
                );
            }
        });
    }

    /// Warm the checkpoint cache with a loaded checkpoint.
    ///
    /// This should be called during server startup after loading checkpoints from object store.
    /// Only populates the cache if it's currently empty, to avoid overwriting newer data.
    /// Builds the FileIndex once here so incremental updates are O(m) instead of O(n).
    pub fn warm_checkpoint_cache(&self, checkpoint: PersistedSnapshotCheckpoint) {
        if self.checkpoint_interval.is_none() {
            return;
        }
        let mut cache = self.cached_checkpoint.write();
        if cache.is_some() {
            return;
        }

        let file_index = build_file_index(&checkpoint.databases);
        debug!(
            month = %checkpoint.year_month,
            last_seq = checkpoint.last_snapshot_sequence_number.as_u64(),
            file_count = file_index.len(),
            "warm_checkpoint_cache: populated cache from loaded checkpoint"
        );
        *cache = Some(CachedCheckpoint {
            checkpoint,
            file_index,
        });
    }

    /// Update the cached checkpoint with a single snapshot.
    ///
    /// Creates a new checkpoint for the month if cache is empty or stale (month rollover).
    /// On month rollover, persists the previous month's checkpoint asynchronously before
    /// creating the new month's checkpoint to prevent data loss.
    /// This is called on every snapshot persist to maintain an up-to-date in-memory checkpoint.
    /// Uses the cached FileIndex for O(m) incremental updates instead of O(n) rebuilds.
    fn update_cached_checkpoint(&self, snapshot: &PersistedSnapshot) {
        let now_ms = self.time_provider.now().timestamp_millis();
        let current_month = year_month_from_timestamp_ms(now_ms);

        let mut cache = self.cached_checkpoint.write();

        // Get or create CachedCheckpoint for current month
        let cached = match cache.as_mut() {
            Some(c) if c.checkpoint.year_month == current_month => c,
            _ => {
                // Month rollover or first snapshot - start fresh with empty index
                debug!(
                    month = %current_month,
                    "update_cached_checkpoint: creating new checkpoint for month"
                );
                let new_checkpoint = PersistedSnapshotCheckpoint::new(
                    self.node_identifier_prefix.clone(),
                    current_month,
                );
                if let Some(old_checkpoint) = cache.replace(CachedCheckpoint {
                    checkpoint: new_checkpoint,
                    file_index: Default::default(),
                }) {
                    self.persist_previous_month_checkpoint_async(old_checkpoint.checkpoint);
                }
                cache.as_mut().unwrap()
            }
        };

        // Apply snapshot to checkpoint using cached file_index
        cached.checkpoint.update_from_snapshot(snapshot);
        add_snapshot_files(
            &mut cached.checkpoint,
            &mut cached.file_index,
            snapshot.databases.clone(),
        );
        process_removed_files(
            &mut cached.checkpoint,
            &mut cached.file_index,
            snapshot.removed_files.clone(),
        );

        trace!(
            snapshot_seq = snapshot.snapshot_sequence_number.as_u64(),
            "update_cached_checkpoint: applied snapshot to cache"
        );
    }

    /// Lists checkpoint paths and returns the latest path for each month.
    ///
    /// This is a lightweight operation that only lists paths without loading content.
    /// Returns paths sorted by year-month (oldest first).
    ///
    /// If `min_sequence` is provided, only checkpoints whose range overlaps with the
    /// lookback window are returned. A checkpoint is included if:
    /// - Its sequence number >= min_sequence, OR
    /// - The next checkpoint's sequence number >= min_sequence (range overlap)
    pub async fn list_latest_checkpoints_per_month(
        &self,
        min_sequence: Option<SnapshotSequenceNumber>,
    ) -> Result<Vec<ObjPath>> {
        debug!(
            node_identifier_prefix = %self.node_identifier_prefix,
            ?min_sequence,
            "list_latest_checkpoints_per_month: starting"
        );

        let checkpoint_dir = SnapshotCheckpointPath::dir(&self.node_identifier_prefix);
        let mut checkpoint_list = self.object_store.list(Some(&checkpoint_dir));

        // Collect all checkpoint paths
        let mut paths_by_month: HashMap<YearMonth, Vec<ObjPath>> = HashMap::new();

        while let Some(item) = checkpoint_list.next().await {
            match item {
                Ok(meta) => {
                    let path_str = meta.location.as_ref();
                    if let Some(year_month) = SnapshotCheckpointPath::parse_year_month(path_str) {
                        paths_by_month
                            .entry(year_month)
                            .or_default()
                            .push(meta.location);
                    }
                }
                Err(e) => {
                    if e.to_string().contains("not found") {
                        debug!("No checkpoints directory found, returning empty list");
                        return Ok(Vec::new());
                    }
                    return Err(e.into());
                }
            }
        }

        // For each month, select only the latest checkpoint
        let mut result: Vec<(YearMonth, ObjPath)> = paths_by_month
            .into_iter()
            .map(|(month, mut paths)| {
                paths.sort();
                // First path after sorting is the latest due to inverted sequence numbers
                (month, paths.into_iter().next().unwrap())
            })
            .collect();

        // Sort by month (oldest first) for consistent ordering
        result.sort_by(|a, b| a.0.cmp(&b.0));

        if let Some(min_seq) = min_sequence {
            let min_seq_u64 = min_seq.as_u64();
            let with_seqs: Vec<_> = result
                .iter()
                .filter_map(|(month, path)| {
                    SnapshotCheckpointPath::parse_sequence_number(path.as_ref())
                        .map(|seq| (*month, path.clone(), seq.as_u64()))
                })
                .collect();

            // Include checkpoint if its range overlaps with the lookback window:
            // - seq >= min_seq, OR
            // - next checkpoint's seq >= min_seq (this checkpoint's range contains min_seq)
            result = with_seqs
                .iter()
                .enumerate()
                .filter(|(i, (_, _, seq))| {
                    if *seq >= min_seq_u64 {
                        return true;
                    }
                    with_seqs
                        .get(i + 1)
                        .map(|(_, _, next_seq)| *next_seq >= min_seq_u64)
                        .unwrap_or(false)
                })
                .map(|(_, (month, path, _))| (*month, path.clone()))
                .collect();

            debug!(
                filtered_count = result.len(),
                min_seq = min_seq_u64,
                "list_latest_checkpoints_per_month: filtered by sequence"
            );
        }

        let paths: Vec<ObjPath> = result.into_iter().map(|(_, path)| path).collect();

        debug!(
            count = paths.len(),
            "list_latest_checkpoints_per_month: completed"
        );
        Ok(paths)
    }

    /// Loads checkpoints from the given paths.
    ///
    /// Use with paths returned from `list_latest_checkpoints_per_month()`.
    /// Limits concurrency to avoid overwhelming the object store.
    pub async fn load_checkpoints(
        &self,
        paths: Vec<ObjPath>,
    ) -> Result<Vec<PersistedSnapshotCheckpointVersion>> {
        debug!(count = paths.len(), "load_checkpoints: starting");

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKPOINT_LOADS));
        let mut futures = FuturesOrdered::new();

        for path in paths {
            let object_store = Arc::clone(&self.object_store);
            let semaphore = Arc::clone(&semaphore);
            let path_clone = path.clone();

            futures.push_back(async move {
                let _permit = semaphore.acquire().await.expect("semaphore not closed");
                trace!(path = %path_clone, "loading checkpoint");
                let bytes = object_store.get(&path_clone).await?.bytes().await?;
                let checkpoint: PersistedSnapshotCheckpointVersion =
                    serde_json::from_slice(&bytes)?;
                Result::<_, PersisterError>::Ok(checkpoint)
            });
        }

        let mut results = Vec::new();
        while let Some(result) = futures.next().await {
            results.push(result?);
        }

        debug!(count = results.len(), "load_checkpoints: completed");
        Ok(results)
    }

    /// Persists a checkpoint to object store.
    pub async fn persist_checkpoint(
        &self,
        checkpoint: &PersistedSnapshotCheckpointVersion,
    ) -> Result<()> {
        let (year_month, seq) = match checkpoint {
            PersistedSnapshotCheckpointVersion::V1(c) => {
                (&c.year_month, c.last_snapshot_sequence_number)
            }
        };

        debug!(
            %year_month,
            snapshot_sequence_number = seq.as_u64(),
            "persist_checkpoint: starting"
        );

        let checkpoint_path =
            SnapshotCheckpointPath::new(&self.node_identifier_prefix, year_month, seq);

        let json = serde_json::to_vec_pretty(checkpoint)?;
        self.object_store
            .put(checkpoint_path.as_ref(), json.into())
            .await?;

        debug!(
            %year_month,
            snapshot_sequence_number = seq.as_u64(),
            path = %checkpoint_path.as_ref(),
            "persist_checkpoint: completed"
        );

        // Clean up old checkpoints for this month, keeping only the most recent ones
        let node_id_prefix = self.node_identifier_prefix.clone();
        let object_store = Arc::clone(&self.object_store);
        let year_month = *year_month;
        tokio::spawn(Self::cleanup_old_checkpoints_for_month(
            node_id_prefix,
            object_store,
            year_month,
        ));

        Ok(())
    }

    /// Build and persist checkpoints from loaded snapshots.
    ///
    /// Processes one month at a time: groups snapshots by month, builds checkpoint for
    /// the oldest month, persists it, then moves to the next month. This minimizes peak
    /// memory usage by not holding any checkpoints in memory longer than necessary.
    ///
    /// Uses each snapshot's `persisted_at` field (from ObjectMeta.last_modified) to determine
    /// which month it belongs to. Falls back to `max_time` from the snapshot itself.
    ///
    /// Returns only the current month's checkpoint (if any) for cache warming.
    /// All other checkpoints are persisted and dropped to free memory.
    pub async fn build_and_persist_checkpoints_from_snapshots(
        &self,
        snapshots: &[PersistedSnapshot],
        current_month: YearMonth,
    ) -> Option<PersistedSnapshotCheckpoint> {
        if snapshots.is_empty() {
            return None;
        }

        // Group snapshots by month using persisted_at
        let mut snapshots_by_month: HashMap<YearMonth, Vec<&PersistedSnapshot>> = HashMap::new();

        for snapshot in snapshots.iter() {
            if snapshot.max_time == i64::MIN {
                continue;
            }
            let timestamp_ms = snapshot.persisted_at.unwrap_or(snapshot.max_time);
            let month = year_month_from_timestamp_ms(timestamp_ms);
            snapshots_by_month.entry(month).or_default().push(snapshot);
        }

        if snapshots_by_month.is_empty() {
            return None;
        }

        let mut months: Vec<_> = snapshots_by_month.keys().copied().collect();
        months.sort();

        info!(
            month_count = months.len(),
            snapshot_count = snapshots.len(),
            "build_and_persist_checkpoints_from_snapshots: building checkpoints in background"
        );

        let mut current_month_checkpoint = None;

        for month in months {
            let month_snapshots = snapshots_by_month.remove(&month).unwrap();

            let mut checkpoint =
                PersistedSnapshotCheckpoint::new(self.node_identifier_prefix.clone(), month);
            let mut file_index: FileIndex = HashMap::new();
            for snapshot in month_snapshots.into_iter().rev() {
                checkpoint.update_from_snapshot(snapshot);
                add_snapshot_files(&mut checkpoint, &mut file_index, snapshot.databases.clone());
                process_removed_files(
                    &mut checkpoint,
                    &mut file_index,
                    snapshot.removed_files.clone(),
                );
            }

            if let Err(e) = self
                .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint.clone()))
                .await
            {
                warn!(
                    %e,
                    %month,
                    "build_and_persist_checkpoints_from_snapshots: failed to persist checkpoint"
                );
            } else {
                debug!(
                    %month,
                    "build_and_persist_checkpoints_from_snapshots: persisted checkpoint"
                );
            }

            // Keep current month's checkpoint for cache warming
            if month == current_month {
                current_month_checkpoint = Some(checkpoint);
            }
        }

        current_month_checkpoint
    }

    /// Cleans up old checkpoints for a given month, keeping only the N most recent.
    ///
    /// This is called after successfully persisting a new checkpoint to prevent
    /// unbounded accumulation of checkpoint files. We keep 2 checkpoints (latest +
    /// previous) as a safety buffer in case the latest is corrupted.
    async fn cleanup_old_checkpoints_for_month(
        node_identifier_prefix: String,
        object_store: Arc<dyn ObjectStore>,
        year_month: YearMonth,
    ) {
        let month_dir = SnapshotCheckpointPath::month_dir(&node_identifier_prefix, &year_month);

        // List all checkpoints for this month
        let mut checkpoints: Vec<_> = match object_store
            .list(Some(&month_dir))
            .try_collect::<Vec<_>>()
            .await
        {
            Ok(list) => list,
            Err(e) => {
                warn!(
                    %year_month,
                    error = %e,
                    "Failed to list checkpoints for cleanup"
                );
                return;
            }
        };

        // Sort by path - due to inverted sequence numbers, lexicographic order puts newest first
        checkpoints.sort_by(|a, b| a.location.cmp(&b.location));

        // Keep the first N (most recent), delete the rest
        if checkpoints.len() <= CHECKPOINTS_TO_RETAIN_PER_MONTH {
            return; // Nothing to delete
        }

        let to_delete = &checkpoints[CHECKPOINTS_TO_RETAIN_PER_MONTH..];
        let mut deleted_count = 0;

        for meta in to_delete {
            match object_store.delete(&meta.location).await {
                Ok(_) => {
                    deleted_count += 1;
                    debug!(path = %meta.location, "Deleted old checkpoint");
                }
                Err(e) => {
                    // Log but don't fail - deletion is best-effort cleanup
                    // "Not found" is okay (already deleted or race condition)
                    let error_str = e.to_string();
                    if !error_str.contains("not found") && !error_str.contains("No such file") {
                        debug!(
                            path = %meta.location,
                            error = %e,
                            "Failed to delete old checkpoint"
                        );
                    }
                }
            }
        }

        if deleted_count > 0 {
            debug!(
                %year_month,
                deleted = deleted_count,
                retained = CHECKPOINTS_TO_RETAIN_PER_MONTH,
                "Cleaned up old checkpoints"
            );
        }
    }

    /// Loads snapshots newer than a given sequence number.
    ///
    /// Used after loading checkpoints to get any snapshots created since the checkpoint.
    pub async fn load_snapshots_after(
        &self,
        after_sequence_number: SnapshotSequenceNumber,
        most_recent_n: usize,
    ) -> Result<Vec<PersistedSnapshotVersion>> {
        debug!(
            after_sequence_number = after_sequence_number.as_u64(),
            most_recent_n, "load_snapshots_after: starting"
        );

        // Load snapshots and filter to those newer than the given sequence
        let all_snapshots = self.load_snapshots(most_recent_n).await?;
        let total_loaded = all_snapshots.len();

        let filtered: Vec<_> = all_snapshots
            .into_iter()
            .filter(|s| match s {
                PersistedSnapshotVersion::V1(ps) => {
                    ps.snapshot_sequence_number > after_sequence_number
                }
            })
            .collect();

        debug!(
            total_loaded,
            after_filter = filtered.len(),
            "load_snapshots_after: completed"
        );
        Ok(filtered)
    }

    /// Writes a [`SendableRecordBatchStream`] to the Parquet format and persists it to Object Store
    /// at the given path. Returns the number of bytes written and the file metadata.
    pub async fn persist_parquet_file(
        &self,
        path: ParquetFilePath,
        record_batch: SendableRecordBatchStream,
    ) -> Result<(u64, FileMetaData, ParquetFileDataToCache)> {
        // so we have serialized parquet file bytes
        let parquet = self.serialize_to_parquet(record_batch).await?;
        let bytes_written = parquet.bytes.len() as u64;
        let put_result = self
            .object_store
            // this bytes.clone() is cheap - uses underlying Bytes::clone
            .put(path.as_ref(), parquet.bytes.clone().into())
            .await?;

        let to_cache = ParquetFileDataToCache::new(
            path.as_ref(),
            self.time_provider.now().date_time(),
            parquet.bytes,
            put_result,
        );

        Ok((bytes_written, parquet.meta_data, to_cache))
    }

    /// Returns the configured `ObjectStore` that data is loaded from and persisted to.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

pub async fn serialize_to_parquet(
    mem_pool: Arc<dyn MemoryPool>,
    batches: SendableRecordBatchStream,
) -> Result<ParquetBytes> {
    // The ArrowWriter::write() call will return an error if any subsequent
    // batch does not match this schema, enforcing schema uniformity.
    let schema = batches.schema();

    let stream = batches;
    let mut bytes = Vec::new();
    pin_mut!(stream);

    // Construct the arrow serializer with the metadata as part of the parquet
    // file properties.
    let mut writer = TrackedMemoryArrowWriter::try_new(&mut bytes, Arc::clone(&schema), mem_pool)?;

    while let Some(batch) = stream.try_next().await? {
        writer.write(batch)?;
    }

    let writer_meta = writer.close()?;
    if writer_meta.num_rows == 0 {
        return Err(PersisterError::NoRows);
    }

    Ok(ParquetBytes {
        meta_data: writer_meta,
        bytes: Bytes::from(bytes),
    })
}

#[derive(Debug)]
pub struct ParquetBytes {
    pub bytes: Bytes,
    pub meta_data: FileMetaData,
}

/// Wraps an [`ArrowWriter`] to track its buffered memory in a
/// DataFusion [`MemoryPool`]
#[derive(Debug)]
pub struct TrackedMemoryArrowWriter<W: Write + Send> {
    /// The inner ArrowWriter
    inner: ArrowWriter<W>,
    /// DataFusion memory reservation with
    reservation: MemoryReservation,
}

/// Parquet row group write size
pub const ROW_GROUP_WRITE_SIZE: usize = 100_000;

impl<W: Write + Send> TrackedMemoryArrowWriter<W> {
    /// create a new `TrackedMemoryArrowWriter<`
    pub fn try_new(sink: W, schema: SchemaRef, mem_pool: Arc<dyn MemoryPool>) -> Result<Self> {
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(ROW_GROUP_WRITE_SIZE)
            .build();
        let inner = ArrowWriter::try_new(sink, schema, Some(props))?;
        let consumer = MemoryConsumer::new("InfluxDB3 ParquetWriter (TrackedMemoryArrowWriter)");
        let reservation = consumer.register(&mem_pool);

        Ok(Self { inner, reservation })
    }

    /// Push a `RecordBatch` into the underlying writer, updating the
    /// tracked allocation
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // writer encodes the batch into its internal buffers
        self.inner.write(&batch)?;

        // In progress memory, in bytes
        let in_progress_size = self.inner.in_progress_size();

        // update the allocation with the pool.
        self.reservation.try_resize(in_progress_size)?;

        Ok(())
    }

    /// closes the writer, flushing any remaining data and returning
    /// the written [`FileMetaData`]
    ///
    /// [`FileMetaData`]: parquet::format::FileMetaData
    pub fn close(self) -> Result<parquet::format::FileMetaData> {
        // reservation is returned on drop
        Ok(self.inner.close()?)
    }
}

#[cfg(test)]
impl Persister {
    /// Test helper: Get the cached checkpoint
    fn get_cached_checkpoint(&self) -> Option<PersistedSnapshotCheckpoint> {
        self.cached_checkpoint
            .read()
            .as_ref()
            .map(|c| c.checkpoint.clone())
    }

    /// Test helper: Get the last checkpoint time
    fn get_last_checkpoint_time(&self) -> Option<Instant> {
        *self.last_checkpoint_time.read()
    }
}

#[cfg(test)]
mod tests;
