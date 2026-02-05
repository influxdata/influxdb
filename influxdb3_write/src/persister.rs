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

    pub fn is_checkpointing_enabled(&self) -> bool {
        self.checkpoint_interval.is_some()
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
mod tests {
    use super::*;
    use crate::{
        DatabaseTables, ParquetFile, ParquetFileId, PersistedSnapshot, PersistedSnapshotCheckpoint,
        PersistedSnapshotCheckpointVersion, PersistedSnapshotVersion,
    };
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, SerdeVecMap, TableId};
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use {
        arrow::array::Int32Array, arrow::datatypes::DataType, arrow::datatypes::Field,
        arrow::datatypes::Schema, chrono::Utc,
        datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder,
        object_store::local::LocalFileSystem,
    };

    // =========================================================================
    // Test Helpers
    // =========================================================================

    /// Create a test persister with InMemory object store and mock time provider
    fn create_test_persister(
        time_nanos: i64,
        checkpoint_interval: Option<Duration>,
    ) -> (Persister, Arc<MockProvider>) {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(time_nanos)));
        let object_store = Arc::new(InMemory::new());
        let persister = Persister::new(
            object_store,
            "test_host",
            Arc::clone(&time_provider) as _,
            checkpoint_interval,
        );
        (persister, time_provider)
    }

    /// Create a test checkpoint with minimal data
    fn create_test_checkpoint(year_month: YearMonth, seq: u64) -> PersistedSnapshotCheckpoint {
        PersistedSnapshotCheckpoint {
            node_id: "test_host".to_string(),
            year_month,
            last_snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
            next_file_id: Some(ParquetFileId::new()),
            wal_file_sequence_number: WalFileSequenceNumber::new(seq),
            catalog_sequence_number: CatalogSequenceNumber::new(seq),
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases: SerdeVecMap::new(),
            pending_removed_files: SerdeVecMap::new(),
        }
    }

    /// Create a test snapshot with minimal data
    fn create_test_snapshot(seq: u64) -> PersistedSnapshot {
        PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
            wal_file_sequence_number: WalFileSequenceNumber::new(seq),
            catalog_sequence_number: CatalogSequenceNumber::new(seq),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        }
    }

    // Nanoseconds for specific dates
    const JAN_15_2025_NANOS: i64 = 1_736_899_200_000_000_000; // 2025-01-15 00:00:00 UTC
    const FEB_01_2025_NANOS: i64 = 1_738_368_000_000_000_000; // 2025-02-01 00:00:00 UTC

    #[tokio::test]
    async fn persist_snapshot_info_file() {
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });

        persister.persist_snapshot(&info_file).await.unwrap();
    }

    #[tokio::test]
    async fn persist_and_load_snapshot_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });
        let info_file_2 = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(1),
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            wal_file_sequence_number: WalFileSequenceNumber::new(1),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            max_time: 1,
            min_time: 0,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });
        let info_file_3 = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(2),
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
            wal_file_sequence_number: WalFileSequenceNumber::new(2),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });

        persister.persist_snapshot(&info_file).await.unwrap();
        persister.persist_snapshot(&info_file_2).await.unwrap();
        persister.persist_snapshot(&info_file_3).await.unwrap();

        let snapshots = persister.load_snapshots(2).await.unwrap();
        assert_eq!(snapshots.len(), 2);
        // The most recent files are first
        assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 2);
        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 2);
        assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 2);
        assert_eq!(snapshots[1].v1_ref().next_file_id.as_u64(), 1);
        assert_eq!(snapshots[1].v1_ref().wal_file_sequence_number.as_u64(), 1);
        assert_eq!(snapshots[1].v1_ref().snapshot_sequence_number.as_u64(), 1);
    }

    #[tokio::test]
    async fn persist_and_load_snapshot_info_files_with_fewer_than_requested() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(0),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::default(),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });
        persister.persist_snapshot(&info_file).await.unwrap();
        let snapshots = persister.load_snapshots(2).await.unwrap();
        // We asked for the most recent 2 but there should only be 1
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
    }

    #[tokio::test]
    /// This test makes sure that the logic for offset lists works
    async fn persist_and_load_over_1000_snapshot_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
        for id in 0..1001 {
            let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
                node_id: "test_host".to_string(),
                next_file_id: ParquetFileId::from(id),
                snapshot_sequence_number: SnapshotSequenceNumber::new(id),
                wal_file_sequence_number: WalFileSequenceNumber::new(id),
                catalog_sequence_number: CatalogSequenceNumber::new(id),
                databases: SerdeVecMap::new(),
                removed_files: SerdeVecMap::new(),
                min_time: 0,
                max_time: 1,
                row_count: 0,
                parquet_size_bytes: 0,
                persisted_at: None,
            });
            persister.persist_snapshot(&info_file).await.unwrap();
        }
        let snapshots = persister.load_snapshots(1500).await.unwrap();
        // We asked for the most recent 1500 so there should be 1001 of them
        assert_eq!(snapshots.len(), 1001);
        assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 1000);
        assert_eq!(
            snapshots[0].v1_ref().wal_file_sequence_number.as_u64(),
            1000
        );
        assert_eq!(
            snapshots[0].v1_ref().snapshot_sequence_number.as_u64(),
            1000
        );
        assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 1000);
    }

    #[tokio::test]
    // This test makes sure that the proper next_file_id is used if a parquet file
    // is added
    async fn persist_add_parquet_file_and_load_snapshot() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
        let mut info_file = PersistedSnapshot::new(
            "test_host".to_string(),
            SnapshotSequenceNumber::new(0),
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );

        for _ in 0..=9875 {
            let _id = ParquetFileId::new();
        }

        info_file.add_parquet_file(
            DbId::from(0),
            TableId::from(0),
            crate::ParquetFile {
                // Use a number that will be bigger than what's created in the
                // PersistedSnapshot automatically
                id: ParquetFileId::new(),
                path: "test".into(),
                size_bytes: 5,
                row_count: 5,
                chunk_time: 5,
                min_time: 0,
                max_time: 1,
            },
        );
        persister
            .persist_snapshot(&PersistedSnapshotVersion::V1(info_file))
            .await
            .unwrap();
        let snapshots = persister.load_snapshots(10).await.unwrap();
        assert_eq!(snapshots.len(), 1);

        assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
        assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 0);
        assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 0);

        // Should be the next available id after the largest number
        // NOTE(wayne): it's not reasonable to assert on the exact value of a shared process-wide,
        // monotonically-increasing integer.
        assert!(
            snapshots[0].v1_ref().next_file_id.as_u64() >= 9877,
            "parquet file id must be bigger than or equal to"
        );
    }

    #[tokio::test]
    async fn load_snapshot_works_with_no_exising_snapshots() {
        let store = InMemory::new();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(store), "test_host", time_provider, None);

        let snapshots = persister.load_snapshots(100).await.unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn persisted_snapshot_structure() {
        let databases = [
            (
                DbId::new(0),
                DatabaseTables {
                    tables: [
                        (
                            TableId::new(0),
                            vec![
                                ParquetFile::create_for_test("1.parquet"),
                                ParquetFile::create_for_test("2.parquet"),
                            ],
                        ),
                        (
                            TableId::new(1),
                            vec![
                                ParquetFile::create_for_test("3.parquet"),
                                ParquetFile::create_for_test("4.parquet"),
                            ],
                        ),
                    ]
                    .into(),
                },
            ),
            (
                DbId::new(1),
                DatabaseTables {
                    tables: [
                        (
                            TableId::new(0),
                            vec![
                                ParquetFile::create_for_test("5.parquet"),
                                ParquetFile::create_for_test("6.parquet"),
                            ],
                        ),
                        (
                            TableId::new(1),
                            vec![
                                ParquetFile::create_for_test("7.parquet"),
                                ParquetFile::create_for_test("8.parquet"),
                            ],
                        ),
                    ]
                    .into(),
                },
            ),
        ]
        .into();
        let snapshot = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(0),
            wal_file_sequence_number: WalFileSequenceNumber::new(0),
            catalog_sequence_number: CatalogSequenceNumber::new(0),
            parquet_size_bytes: 1_024,
            row_count: 1,
            min_time: 0,
            max_time: 1,
            removed_files: SerdeVecMap::new(),
            databases,
            persisted_at: None,
        });
        // Redact dynamic IDs that depend on global static counter
        insta::assert_json_snapshot!(snapshot, {
            ".next_file_id" => "[next_file_id]",
            ".**.id" => "[id]",
        });
    }

    #[tokio::test]
    async fn get_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let parquet = persister
            .serialize_to_parquet(stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(parquet.meta_data.num_rows, 10);
    }

    #[tokio::test]
    async fn persist_and_load_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let path = ParquetFilePath::new(
            "test_host",
            0,
            0,
            Utc::now().timestamp_nanos_opt().unwrap(),
            WalFileSequenceNumber::new(1),
        );
        let (bytes_written, meta, _) = persister
            .persist_parquet_file(path.clone(), stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(meta.num_rows, 10);

        let bytes = persister.load_parquet_file(path).await.unwrap();

        // Assert that we have a file of bytes > 0
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len() as u64, bytes_written);
    }

    // =========================================================================
    // Checkpoint Tests
    // =========================================================================

    /// Test should_persist_checkpoint timing logic with various scenarios
    #[rstest]
    #[case::disabled_returns_false(None, false)]
    #[case::enabled_no_previous_returns_true(Some(Duration::from_secs(3600)), true)]
    fn test_should_persist_checkpoint_basic(
        #[case] checkpoint_interval: Option<Duration>,
        #[case] expected: bool,
    ) {
        let (persister, _time_provider) =
            create_test_persister(JAN_15_2025_NANOS, checkpoint_interval);
        assert_eq!(persister.should_persist_checkpoint(), expected);
    }

    #[tokio::test]
    async fn test_should_persist_checkpoint_respects_interval() {
        // Create persister with 1-hour checkpoint interval
        let (persister, _) =
            create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

        // First call should return true (no previous checkpoint)
        assert!(persister.should_persist_checkpoint());

        // Mark checkpoint as created
        persister.mark_checkpoint_created();

        // Should now return false (not enough time elapsed)
        assert!(!persister.should_persist_checkpoint());

        // After the mark, some time passed but not enough
        assert!(persister.get_last_checkpoint_time().is_some());
    }

    /// Test checkpoint persist and load roundtrip
    #[tokio::test]
    async fn test_persist_and_load_checkpoint_roundtrip() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        // Create a checkpoint with some data
        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let mut checkpoint = create_test_checkpoint(jan_2025, 5);
        checkpoint.parquet_size_bytes = 1024;
        checkpoint.row_count = 100;
        checkpoint.min_time = 1000;
        checkpoint.max_time = 2000;

        // Persist it
        persister
            .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint.clone()))
            .await
            .unwrap();

        // List checkpoints - should find one
        let paths = persister
            .list_latest_checkpoints_per_month(None)
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].as_ref().contains("2025-01"));

        // Load it back
        let loaded = persister.load_checkpoints(paths).await.unwrap();
        assert_eq!(loaded.len(), 1);

        let PersistedSnapshotCheckpointVersion::V1(loaded_checkpoint) = &loaded[0];

        // Verify fields match
        assert_eq!(loaded_checkpoint.node_id, checkpoint.node_id);
        assert_eq!(loaded_checkpoint.year_month, checkpoint.year_month);
        assert_eq!(
            loaded_checkpoint.last_snapshot_sequence_number,
            checkpoint.last_snapshot_sequence_number
        );
        assert_eq!(
            loaded_checkpoint.parquet_size_bytes,
            checkpoint.parquet_size_bytes
        );
        assert_eq!(loaded_checkpoint.row_count, checkpoint.row_count);
        assert_eq!(loaded_checkpoint.min_time, checkpoint.min_time);
        assert_eq!(loaded_checkpoint.max_time, checkpoint.max_time);
    }

    /// Test list_latest_checkpoints_per_month with various store states
    #[rstest]
    #[case::empty_store(vec![], 0)]
    #[case::single_checkpoint(vec![("2025-01", 1)], 1)]
    #[case::multiple_months(vec![("2025-01", 1), ("2025-02", 1)], 2)]
    #[case::multiple_per_month_picks_latest(vec![("2025-01", 1), ("2025-01", 2), ("2025-01", 3)], 1)]
    #[case::mixed_months_and_sequences(vec![("2025-01", 1), ("2025-01", 5), ("2025-02", 2), ("2025-02", 3)], 2)]
    #[tokio::test]
    async fn test_list_latest_checkpoints_per_month(
        #[case] checkpoints_to_create: Vec<(&str, u64)>,
        #[case] expected_count: usize,
    ) {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        // Persist checkpoints in the given order
        for (year_month_str, seq) in &checkpoints_to_create {
            let year_month: YearMonth = year_month_str.parse().unwrap();
            let checkpoint = create_test_checkpoint(year_month, *seq);
            persister
                .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                .await
                .unwrap();
        }

        // List and verify
        let paths = persister
            .list_latest_checkpoints_per_month(None)
            .await
            .unwrap();
        assert_eq!(paths.len(), expected_count);

        // If we have multiple per month, verify we got the latest sequence numbers
        if !checkpoints_to_create.is_empty() && expected_count > 0 {
            // Load the checkpoints to verify sequence numbers
            let loaded = persister.load_checkpoints(paths).await.unwrap();

            // Group expected by month and find max seq per month
            let mut expected_by_month: HashMap<YearMonth, u64> = HashMap::new();
            for (month_str, seq) in &checkpoints_to_create {
                let month: YearMonth = month_str.parse().unwrap();
                expected_by_month
                    .entry(month)
                    .and_modify(|e| *e = (*e).max(*seq))
                    .or_insert(*seq);
            }

            // Verify each loaded checkpoint has the expected (max) sequence
            for cp in loaded {
                let PersistedSnapshotCheckpointVersion::V1(cp) = cp;
                let expected_seq = expected_by_month.get(&cp.year_month).unwrap();
                assert_eq!(
                    cp.last_snapshot_sequence_number.as_u64(),
                    *expected_seq,
                    "Expected seq {} for month {}, got {}",
                    expected_seq,
                    cp.year_month,
                    cp.last_snapshot_sequence_number.as_u64()
                );
            }
        }
    }

    /// Test warm_checkpoint_cache bootstraps the cache from loaded checkpoint
    #[tokio::test]
    async fn test_warm_checkpoint_cache() {
        let (persister, _) =
            create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

        // Simulate loading a checkpoint during startup
        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let loaded_checkpoint = create_test_checkpoint(jan_2025, 5);
        persister.warm_checkpoint_cache(loaded_checkpoint.clone());

        // Cache should be populated
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.year_month, jan_2025);
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 5);

        // Calling warm again should NOT overwrite (cache already populated)
        let another_checkpoint = create_test_checkpoint(jan_2025, 10);
        persister.warm_checkpoint_cache(another_checkpoint);

        // Should still have original checkpoint
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 5);
    }

    /// Test that persist_snapshot incrementally updates the cached checkpoint
    #[tokio::test]
    async fn test_incremental_checkpoint_update_on_persist() {
        // Enable checkpointing with 1 hour interval
        let (persister, _) =
            create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

        // Initially cache is empty
        assert!(persister.get_cached_checkpoint().is_none());

        // Persist first snapshot - should create checkpoint in cache
        let snapshot1 = PersistedSnapshotVersion::V1(create_test_snapshot(1));
        persister.persist_snapshot(&snapshot1).await.unwrap();

        // Cache should now have checkpoint with seq=1
        let cached = persister.get_cached_checkpoint().unwrap();
        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        assert_eq!(cached.year_month, jan_2025);
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 1);

        // Persist second snapshot - should update cache incrementally
        let snapshot2 = PersistedSnapshotVersion::V1(create_test_snapshot(2));
        persister.persist_snapshot(&snapshot2).await.unwrap();

        // Cache should now have seq=2
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 2);

        // Persist third snapshot
        let snapshot3 = PersistedSnapshotVersion::V1(create_test_snapshot(3));
        persister.persist_snapshot(&snapshot3).await.unwrap();

        // Cache should now have seq=3
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 3);
    }

    /// Test that month rollover creates a new checkpoint (incremental path)
    #[tokio::test]
    async fn test_incremental_checkpoint_month_rollover() {
        let (persister, time_provider) =
            create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

        // Persist January snapshot
        let snapshot1 = PersistedSnapshotVersion::V1(create_test_snapshot(1));
        persister.persist_snapshot(&snapshot1).await.unwrap();

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.year_month, jan_2025);
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 1);

        // Advance to February
        time_provider.set(Time::from_timestamp_nanos(FEB_01_2025_NANOS));

        // Persist February snapshot - should create NEW checkpoint, not update January's
        let snapshot2 = PersistedSnapshotVersion::V1(create_test_snapshot(2));
        persister.persist_snapshot(&snapshot2).await.unwrap();

        let feb_2025 = YearMonth::new_unchecked(2025, 2);
        let cached = persister.get_cached_checkpoint().unwrap();
        assert_eq!(cached.year_month, feb_2025); // New month checkpoint
        assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 2);
    }

    /// Test that checkpointing disabled means no cache updates
    #[tokio::test]
    async fn test_checkpoint_disabled_no_cache_updates() {
        // Checkpointing disabled (None interval)
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        // Persist a snapshot
        let snapshot = PersistedSnapshotVersion::V1(create_test_snapshot(1));
        persister.persist_snapshot(&snapshot).await.unwrap();

        // Cache should still be empty (checkpointing disabled)
        assert!(persister.get_cached_checkpoint().is_none());
    }

    // =========================================================================
    // Tests for build_checkpoints_from_snapshots
    // =========================================================================

    /// Create a test snapshot with a specific max_time for month grouping tests
    fn create_snapshot_with_max_time(seq: u64, max_time_ms: i64) -> PersistedSnapshot {
        PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
            wal_file_sequence_number: WalFileSequenceNumber::new(seq),
            catalog_sequence_number: CatalogSequenceNumber::new(seq),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: max_time_ms,
            row_count: 100,
            parquet_size_bytes: 1024,
            persisted_at: None,
        }
    }

    /// Create a snapshot with file data for testing aggregation
    fn create_snapshot_with_files(
        seq: u64,
        max_time_ms: i64,
        db_id: DbId,
        table_id: TableId,
        files: Vec<ParquetFile>,
    ) -> PersistedSnapshot {
        let size_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
        let row_count: u64 = files.iter().map(|f| f.row_count).sum();
        let min_time = files.iter().map(|f| f.min_time).min().unwrap_or(i64::MAX);
        let max_time = files.iter().map(|f| f.max_time).max().unwrap_or(i64::MIN);

        let mut databases = SerdeVecMap::new();
        let mut db_tables = DatabaseTables::default();
        db_tables.tables.insert(table_id, files);
        databases.insert(db_id, db_tables);

        PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
            wal_file_sequence_number: WalFileSequenceNumber::new(seq),
            catalog_sequence_number: CatalogSequenceNumber::new(seq),
            databases,
            removed_files: SerdeVecMap::new(),
            min_time,
            max_time: max_time_ms.max(max_time), // Use provided max_time_ms for month grouping
            row_count,
            parquet_size_bytes: size_bytes,
            persisted_at: None,
        }
    }

    fn create_test_parquet_file(size_bytes: u64, row_count: u64) -> ParquetFile {
        ParquetFile {
            id: ParquetFileId::new(),
            path: "test/file.parquet".to_string(),
            size_bytes,
            row_count,
            chunk_time: 0,
            min_time: 100,
            max_time: 200,
        }
    }

    // Milliseconds for specific dates (for max_time in snapshots)
    const JAN_15_2025_MS: i64 = 1_736_899_200_000; // 2025-01-15 00:00:00 UTC
    const JAN_20_2025_MS: i64 = 1_737_331_200_000; // 2025-01-20 00:00:00 UTC
    const FEB_01_2025_MS: i64 = 1_738_368_000_000; // 2025-02-01 00:00:00 UTC
    const FEB_15_2025_MS: i64 = 1_739_577_600_000; // 2025-02-15 00:00:00 UTC
    const MAR_01_2025_MS: i64 = 1_740_787_200_000; // 2025-03-01 00:00:00 UTC

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_empty() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&[], jan_2025)
            .await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_single_snapshot() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file = create_test_parquet_file(1024, 100);
        let snapshot = create_snapshot_with_files(1, JAN_15_2025_MS, db_id, table_id, vec![file]);

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&[snapshot], jan_2025)
            .await;

        // Should return the January checkpoint (current month)
        let checkpoint = result.expect("should return current month checkpoint");
        assert_eq!(checkpoint.year_month, jan_2025);
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 1);
        assert_eq!(checkpoint.row_count, 100);
        assert_eq!(checkpoint.parquet_size_bytes, 1024);
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_multiple_same_month() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        let db_id = DbId::new(1);
        let table_id = TableId::new(1);

        // Two snapshots in January (newest-first order as returned by load_snapshots)
        let snapshots = vec![
            create_snapshot_with_files(
                2,
                JAN_20_2025_MS,
                db_id,
                table_id,
                vec![create_test_parquet_file(1024, 100)],
            ),
            create_snapshot_with_files(
                1,
                JAN_15_2025_MS,
                db_id,
                table_id,
                vec![create_test_parquet_file(1024, 100)],
            ),
        ];

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
            .await;

        let checkpoint = result.expect("should return current month checkpoint");
        assert_eq!(checkpoint.year_month, jan_2025);
        // Should have the newest snapshot's sequence number
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 2);
        // Metrics should be aggregated from both snapshots
        assert_eq!(checkpoint.row_count, 200); // 100 + 100
        assert_eq!(checkpoint.parquet_size_bytes, 2048); // 1024 + 1024
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_multiple_months_returns_current() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        let db_id = DbId::new(1);
        let table_id = TableId::new(1);

        // Helper to create snapshot with a file
        let make_snapshot = |seq, max_time_ms| {
            create_snapshot_with_files(
                seq,
                max_time_ms,
                db_id,
                table_id,
                vec![create_test_parquet_file(1024, 100)],
            )
        };

        // Snapshots across 3 months (newest-first order)
        let snapshots = vec![
            make_snapshot(5, MAR_01_2025_MS),
            make_snapshot(4, FEB_15_2025_MS),
            make_snapshot(3, FEB_01_2025_MS),
            make_snapshot(2, JAN_20_2025_MS),
            make_snapshot(1, JAN_15_2025_MS),
        ];

        // Current month is February - should return February's checkpoint
        let feb_2025 = YearMonth::new_unchecked(2025, 2);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&snapshots, feb_2025)
            .await;

        let checkpoint = result.expect("should return current month checkpoint");
        assert_eq!(checkpoint.year_month, feb_2025);
        // February has 2 snapshots (seq 3 and 4)
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 4);
        assert_eq!(checkpoint.row_count, 200);

        // Verify all 3 months' checkpoints were persisted to object store
        let loaded_paths = persister
            .list_latest_checkpoints_per_month(None)
            .await
            .unwrap();
        assert_eq!(
            loaded_paths.len(),
            3,
            "should persist checkpoints for all 3 months"
        );
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_current_month_not_in_snapshots() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        let db_id = DbId::new(1);
        let table_id = TableId::new(1);

        // Only January snapshots
        let snapshots = vec![create_snapshot_with_files(
            1,
            JAN_15_2025_MS,
            db_id,
            table_id,
            vec![create_test_parquet_file(1024, 100)],
        )];

        // Current month is March - not in snapshots
        let mar_2025 = YearMonth::new_unchecked(2025, 3);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&snapshots, mar_2025)
            .await;

        // Should return None since current month isn't in snapshots
        assert!(result.is_none());

        // But January's checkpoint should still be persisted
        let loaded_paths = persister
            .list_latest_checkpoints_per_month(None)
            .await
            .unwrap();
        assert_eq!(loaded_paths.len(), 1);
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_skips_empty_snapshots() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        // Include a snapshot with max_time = i64::MIN (empty, no data)
        let empty_snapshot = PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
            wal_file_sequence_number: WalFileSequenceNumber::new(2),
            catalog_sequence_number: CatalogSequenceNumber::new(2),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: i64::MAX,
            max_time: i64::MIN, // Empty snapshot indicator
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        };

        let snapshots = vec![
            empty_snapshot,
            create_snapshot_with_max_time(1, JAN_15_2025_MS),
        ];

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
            .await;

        // Should return checkpoint from non-empty snapshot only
        let checkpoint = result.expect("should return checkpoint");
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 1);
    }

    #[tokio::test]
    async fn test_build_checkpoints_from_snapshots_with_files() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file1 = create_test_parquet_file(1024, 100);
        let file2 = create_test_parquet_file(2048, 200);

        let snapshot = create_snapshot_with_files(1, JAN_15_2025_MS, db_id, table_id, vec![file1]);
        let snapshot2 = create_snapshot_with_files(2, JAN_20_2025_MS, db_id, table_id, vec![file2]);

        // Newest first
        let snapshots = vec![snapshot2, snapshot];

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let result = persister
            .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
            .await;

        let checkpoint = result.expect("should return checkpoint");

        // Check that files are present in checkpoint
        assert!(checkpoint.databases.contains_key(&db_id));
        let tables = &checkpoint.databases[&db_id].tables;
        assert!(tables.contains_key(&table_id));
        assert_eq!(tables[&table_id].len(), 2); // Both files
    }

    // =========================================================================
    // Checkpoint Cleanup Tests
    // =========================================================================

    /// Helper to count checkpoints for a specific month
    async fn count_checkpoints_for_month(persister: &Persister, year_month: &YearMonth) -> usize {
        let month_dir =
            SnapshotCheckpointPath::month_dir(&persister.node_identifier_prefix, year_month);
        let list: Vec<_> = persister
            .object_store
            .list(Some(&month_dir))
            .try_collect()
            .await
            .unwrap_or_default();
        list.len()
    }

    /// Helper to get checkpoint sequence numbers for a month (sorted newest first)
    async fn get_checkpoint_sequences_for_month(
        persister: &Persister,
        year_month: &YearMonth,
    ) -> Vec<u64> {
        let month_dir =
            SnapshotCheckpointPath::month_dir(&persister.node_identifier_prefix, year_month);
        let mut list: Vec<_> = persister
            .object_store
            .list(Some(&month_dir))
            .try_collect()
            .await
            .unwrap_or_default();
        list.sort_by(|a, b| a.location.cmp(&b.location));

        list.iter()
            .filter_map(|meta| {
                SnapshotCheckpointPath::parse_sequence_number(meta.location.as_ref())
                    .map(|s| s.as_u64())
            })
            .collect()
    }

    /// Test that cleanup keeps exactly 2 checkpoints when more than 2 exist
    #[tokio::test]
    async fn test_checkpoint_cleanup_keeps_two_most_recent() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);

        // Create 4 checkpoints for the same month with increasing sequence numbers
        for seq in [1, 2, 3, 4] {
            let checkpoint = create_test_checkpoint(jan_2025, seq);
            // Use put directly to avoid triggering cleanup (to set up test state)
            let path = SnapshotCheckpointPath::new(
                "test_host",
                &jan_2025,
                SnapshotSequenceNumber::new(seq),
            );
            let json =
                serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                    .unwrap();
            persister
                .object_store
                .put(path.as_ref(), json.into())
                .await
                .unwrap();
        }

        // Verify we have 4 checkpoints before cleanup
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 4);

        // Trigger cleanup
        Persister::cleanup_old_checkpoints_for_month(
            persister.node_identifier_prefix.clone(),
            Arc::clone(&persister.object_store),
            jan_2025,
        )
        .await;

        // Should have exactly 2 checkpoints remaining
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

        // Verify the 2 most recent (seq 4 and seq 3) are retained
        let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;
        assert_eq!(remaining_seqs, vec![4, 3]);
    }

    /// Test that no deletion occurs when 2 or fewer checkpoints exist
    #[tokio::test]
    async fn test_checkpoint_cleanup_no_deletion_when_two_or_fewer() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);

        // Create exactly 2 checkpoints
        for seq in [1, 2] {
            let checkpoint = create_test_checkpoint(jan_2025, seq);
            let path = SnapshotCheckpointPath::new(
                "test_host",
                &jan_2025,
                SnapshotSequenceNumber::new(seq),
            );
            let json =
                serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                    .unwrap();
            persister
                .object_store
                .put(path.as_ref(), json.into())
                .await
                .unwrap();
        }

        // Verify we have 2 checkpoints before cleanup
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

        // Trigger cleanup - should not delete anything
        Persister::cleanup_old_checkpoints_for_month(
            persister.node_identifier_prefix.clone(),
            Arc::clone(&persister.object_store),
            jan_2025,
        )
        .await;

        // Should still have 2 checkpoints
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

        // Both should still be there
        let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;
        assert_eq!(remaining_seqs, vec![2, 1]);
    }

    /// Test that cleanup handles empty month gracefully
    #[tokio::test]
    async fn test_checkpoint_cleanup_handles_empty_month() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);

        // No checkpoints exist - cleanup should not error
        Persister::cleanup_old_checkpoints_for_month(
            persister.node_identifier_prefix.clone(),
            Arc::clone(&persister.object_store),
            jan_2025,
        )
        .await;

        // Should still be 0
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 0);
    }

    /// Test that persist_checkpoint triggers cleanup automatically
    #[tokio::test]
    async fn test_persist_checkpoint_triggers_cleanup() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);

        // First, create 2 checkpoints directly (without cleanup)
        for seq in [1, 2] {
            let checkpoint = create_test_checkpoint(jan_2025, seq);
            let path = SnapshotCheckpointPath::new(
                "test_host",
                &jan_2025,
                SnapshotSequenceNumber::new(seq),
            );
            let json =
                serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                    .unwrap();
            persister
                .object_store
                .put(path.as_ref(), json.into())
                .await
                .unwrap();
        }

        // Verify we have 2 checkpoints
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

        // Now persist a 3rd checkpoint via the normal method (which triggers cleanup)
        let checkpoint = create_test_checkpoint(jan_2025, 3);
        persister
            .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
            .await
            .unwrap();

        // Cleanup runs asynchronously, so we need to wait for it to complete.
        // Retry with timeout: 100 iterations * 50ms = 5 seconds max.
        let expected_seqs = vec![3, 2];
        let mut attempts = 0;
        loop {
            let count = count_checkpoints_for_month(&persister, &jan_2025).await;
            let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;

            if count == 2 && remaining_seqs == expected_seqs {
                break;
            }

            attempts += 1;
            if attempts >= 100 {
                panic!(
                    "Cleanup did not complete within 5 seconds. \
                     Expected 2 checkpoints with sequences {:?}, \
                     but found {} checkpoints with sequences {:?}",
                    expected_seqs, count, remaining_seqs
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Test that cleanup only affects the specified month
    #[tokio::test]
    async fn test_checkpoint_cleanup_only_affects_specified_month() {
        let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let feb_2025 = YearMonth::new_unchecked(2025, 2);

        // Create 4 checkpoints in January
        for seq in [1, 2, 3, 4] {
            let checkpoint = create_test_checkpoint(jan_2025, seq);
            let path = SnapshotCheckpointPath::new(
                "test_host",
                &jan_2025,
                SnapshotSequenceNumber::new(seq),
            );
            let json =
                serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                    .unwrap();
            persister
                .object_store
                .put(path.as_ref(), json.into())
                .await
                .unwrap();
        }

        // Create 3 checkpoints in February
        for seq in [10, 11, 12] {
            let checkpoint = create_test_checkpoint(feb_2025, seq);
            let path = SnapshotCheckpointPath::new(
                "test_host",
                &feb_2025,
                SnapshotSequenceNumber::new(seq),
            );
            let json =
                serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
                    .unwrap();
            persister
                .object_store
                .put(path.as_ref(), json.into())
                .await
                .unwrap();
        }

        // Verify initial state
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 4);
        assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 3);

        // Cleanup January only
        Persister::cleanup_old_checkpoints_for_month(
            persister.node_identifier_prefix.clone(),
            Arc::clone(&persister.object_store),
            jan_2025,
        )
        .await;

        // January should have 2, February should still have 3
        assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);
        assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 3);

        // Cleanup February
        Persister::cleanup_old_checkpoints_for_month(
            persister.node_identifier_prefix.clone(),
            Arc::clone(&persister.object_store),
            feb_2025,
        )
        .await;

        // Now both should have 2
        assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 2);
    }
}
