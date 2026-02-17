use hashbrown::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use iox_time::TimeProvider;
use object_store::Error as ObjectStoreError;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;
use object_store_utils::RetryableObjectStore;
use observability_deps::tracing::{debug, error, info, warn};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::persister::Persister;
use crate::table_index_cache::TableIndexCache;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::{PersistedSnapshot, PersistedSnapshotVersion};

/// Provides compaction markers and leftover file IDs for cleanup.
pub trait CompactedDataProvider: Send + Sync + Debug {
    /// Returns (snapshot_seq, next_file_id) for the given node, if set.
    fn get_snapshot_marker_for_node(
        &self,
        node_id: &str,
    ) -> Option<(SnapshotSequenceNumber, ParquetFileId)>;

    /// Returns Gen1 file IDs the compactor didn't compact (should not be deleted).
    fn leftover_file_ids_for_node(&self, node_id: &str) -> HashSet<ParquetFileId>;

    /// Returns true if compaction data is available (has been loaded).
    fn is_ready(&self) -> bool;
}

/// Result of calling [`Gen1CleanupHandler::try_start_cleanup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gen1CleanupResult {
    /// A new cleanup task was spawned.
    Started,
    /// A cleanup task is already running.
    AlreadyRunning,
    /// Cleanup is not available (e.g., handler not created, no compacted data).
    NotAvailable,
}

/// On-demand cleanup of Gen1 parquet files after compaction.
#[derive(Debug)]
pub struct Gen1CleanupHandler<C: CompactedDataProvider> {
    node_id: String,
    compacted_data: Arc<C>,
    table_index_cache: TableIndexCache,
    persister: Arc<Persister>,
    persisted_files: Arc<PersistedFiles>,
    time_provider: Arc<dyn TimeProvider>,
    is_running: AtomicBool,
}

/// Result of checking whether a snapshot is eligible for cleanup.
#[derive(Debug, PartialEq, Eq)]
enum SnapshotEligibility {
    /// Not relevant — skip with a reason for debug logging.
    Skip { reason: &'static str },
    /// Stop processing — with a reason for debug logging.
    Stop { reason: &'static str },
    /// Ready for cleanup.
    Eligible,
}

/// Result of processing a single eligible snapshot.
enum ProcessOutcome {
    /// Cleaned up successfully.
    Ok,
    /// Snapshot deletion failed — stop processing.
    Failed,
}

/// Accumulated results from removing gen1/snapshots files
#[derive(Debug, Default)]
pub struct CleanupResult {
    pub removed_file_ids: HashMap<(DbId, TableId), HashSet<ParquetFileId>>,
    pub snapshots_deleted: usize,
    pub files_deleted: usize,
    pub files_not_found: usize,
}

impl CleanupResult {
    pub fn new() -> Self {
        Self {
            removed_file_ids: HashMap::new(),
            snapshots_deleted: 0,
            files_deleted: 0,
            files_not_found: 0,
        }
    }
}

/// Result of concurrent file deletion.
struct DeleteFilesResult {
    deleted: usize,
    not_found: usize,
    failed: usize,
}

/// Delete object store files concurrently using
async fn delete_files_concurrent(
    object_store: Arc<dyn ObjectStore>,
    paths: Vec<&str>,
    concurrency: usize,
) -> DeleteFilesResult {
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut js = JoinSet::new();

    for path in paths {
        let obj_path = ObjPath::from(path);
        let object_store = Arc::clone(&object_store);
        let sem = Arc::clone(&sem);
        let path_str = path.to_string();

        js.spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore not closed");
            object_store
                .raw_delete_with_default_retries(
                    &obj_path,
                    format!("Deleting compacted gen1 file {}", path_str),
                )
                .await
        });
    }

    let mut deleted = 0;
    let mut not_found = 0;
    let mut failed = 0;
    while let Some(res) = js.join_next().await {
        match res {
            Ok(Ok(())) => deleted += 1,
            Ok(Err(ObjectStoreError::NotFound { .. })) => not_found += 1,
            _ => failed += 1,
        }
    }

    DeleteFilesResult {
        deleted,
        not_found,
        failed,
    }
}

fn check_snapshot_eligibility(
    snap: &PersistedSnapshot,
    node_id: &str,
    now_ms: i64,
    min_age_ms: i64,
    latest_seq: SnapshotSequenceNumber,
    next_file_id: ParquetFileId,
) -> SnapshotEligibility {
    if snap.node_id != node_id {
        return SnapshotEligibility::Skip {
            reason: "wrong node",
        };
    }

    if let Some(persisted_at) = snap.persisted_at {
        let age_ms = now_ms - persisted_at;
        if age_ms < min_age_ms {
            return SnapshotEligibility::Skip {
                reason: "too recent",
            };
        }
    }

    if snap.snapshot_sequence_number >= latest_seq {
        return SnapshotEligibility::Stop {
            reason: "is latest",
        };
    }

    if has_files_above_compaction_threshold(snap, next_file_id) {
        return SnapshotEligibility::Stop {
            reason: "has uncompacted files",
        };
    }

    SnapshotEligibility::Eligible
}

impl<C: CompactedDataProvider + 'static> Gen1CleanupHandler<C> {
    /// Create a new Gen1 cleanup handler.
    pub fn new(
        node_id: String,
        compacted_data: Arc<C>,
        table_index_cache: TableIndexCache,
        persister: Arc<Persister>,
        persisted_files: Arc<PersistedFiles>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            node_id,
            compacted_data,
            table_index_cache,
            persister,
            persisted_files,
            time_provider,
            is_running: AtomicBool::new(false),
        }
    }

    /// Spawn a cleanup task if none is running. Returns immediately.
    pub fn try_start_cleanup(
        self: &Arc<Self>,
        min_age: Duration,
        batch_size: usize,
        concurrency: usize,
    ) -> Gen1CleanupResult {
        if self
            .is_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Gen1CleanupResult::AlreadyRunning;
        }

        let handler = Arc::clone(self);
        let handler_for_guard = Arc::clone(&handler);
        tokio::spawn(async move {
            let _guard = scopeguard::guard(handler_for_guard, |h| {
                h.is_running.store(false, Ordering::SeqCst);
            });
            handler.cleanup_loop(min_age, batch_size, concurrency).await;
        });

        Gen1CleanupResult::Started
    }

    /// Process eligible snapshots until none remain or an uncompacted one is hit.
    async fn cleanup_loop(&self, min_age: Duration, batch_size: usize, concurrency: usize) {
        info!(node_id = %self.node_id, "Gen1 cleanup started");
        let start = Instant::now();
        let mut result = CleanupResult::new();

        let now_ms = self.time_provider.now().timestamp_millis();
        let min_age_ms = min_age.as_millis() as i64;

        let Some((snapshot_seq, _)) = self
            .compacted_data
            .get_snapshot_marker_for_node(&self.node_id)
        else {
            debug!(
                node_id = %self.node_id,
                "No snapshot marker found for this node, stopping cleanup"
            );
            return;
        };

        let latest_seq = match self.persister.get_latest_snapshot_sequence().await {
            Ok(Some(seq)) => seq,
            Ok(None) => {
                debug!("No snapshots exist, nothing to clean up");
                return;
            }
            Err(e) => {
                error!(error = %e, "Gen1 cleanup: failed to get latest snapshot sequence, aborting");
                return;
            }
        };

        let snapshot_paths = match self
            .persister
            .list_snapshot_paths_older_than(snapshot_seq, batch_size)
            .await
        {
            Ok(paths) => paths,
            Err(e) => {
                error!(error = %e, "Gen1 cleanup: failed to list old snapshot paths, aborting");
                return;
            }
        };

        if snapshot_paths.is_empty() {
            debug!("No old snapshots to clean up");
            info!("Gen1 cleanup: no eligible files found");
            return;
        }

        debug!(
            count = snapshot_paths.len(),
            threshold = snapshot_seq.as_u64(),
            "Listed old snapshot paths"
        );

        // Re-read marker each iteration (it may advance mid-run)
        for meta in &snapshot_paths {
            let Some((_, next_file_id)) = self
                .compacted_data
                .get_snapshot_marker_for_node(&self.node_id)
            else {
                debug!("Compactor marker disappeared, stopping cleanup");
                break;
            };

            let leftover_ids = self
                .compacted_data
                .leftover_file_ids_for_node(&self.node_id);

            let snapshot_version = match self.persister.try_load_snapshot_from_meta(meta).await {
                Some(Ok(snap)) => snap,
                None => {
                    warn!(
                        path = %meta.location,
                        "Gen1 cleanup: snapshot not found, skipping"
                    );
                    continue;
                }
                Some(Err(e)) => {
                    error!(
                        path = %meta.location,
                        error = %e,
                        "Gen1 cleanup: failed to load snapshot, aborting"
                    );
                    break;
                }
            };

            let PersistedSnapshotVersion::V1(snap) = &snapshot_version;

            match check_snapshot_eligibility(
                snap,
                &self.node_id,
                now_ms,
                min_age_ms,
                latest_seq,
                next_file_id,
            ) {
                SnapshotEligibility::Skip { reason } => {
                    debug!(
                        snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                        reason, "Skipping snapshot"
                    );
                    continue;
                }
                SnapshotEligibility::Stop { reason } => {
                    debug!(
                        snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                        reason, "Stopping cleanup"
                    );
                    break;
                }
                SnapshotEligibility::Eligible => {}
            }

            match self
                .process_snapshot(
                    snap,
                    meta,
                    &mut result,
                    concurrency,
                    next_file_id,
                    &leftover_ids,
                )
                .await
            {
                ProcessOutcome::Ok => {}
                ProcessOutcome::Failed => break,
            }
        }

        // Update table indices for affected tables
        if !result.removed_file_ids.is_empty() {
            let index_removed = self
                .table_index_cache
                .purge_compacted_for_tables(&self.node_id, &result, true)
                .await;
            debug!(index_removed, "Updated table indices after cleanup");
        }

        if result.snapshots_deleted > 0 || result.files_deleted > 0 || result.files_not_found > 0 {
            info!(
                total_snapshots_deleted = result.snapshots_deleted,
                total_files_deleted = result.files_deleted,
                total_files_already_deleted = result.files_not_found,
                duration_secs = start.elapsed().as_secs(),
                "Gen1 cleanup finished"
            );
        } else {
            info!("Gen1 cleanup: no eligible files found");
        }
    }

    /// Process a single snapshot: collect files → remove from PersistedFiles →
    /// delete parquet files → delete snapshot.
    ///
    /// The snapshot acts as a retry manifest — kept if any deletes fail.
    /// Crash safety: stale snapshot refs are filtered by the query path via
    /// next_non_compacted_parquet_file_id.
    async fn process_snapshot(
        &self,
        snap: &PersistedSnapshot,
        meta: &ObjectMeta,
        result: &mut CleanupResult,
        concurrency: usize,
        next_file_id: ParquetFileId,
        leftover_ids: &HashSet<ParquetFileId>,
    ) -> ProcessOutcome {
        let mut eligible_ids: HashSet<ParquetFileId> = HashSet::new();
        let mut eligible_paths: Vec<&str> = Vec::new();
        let mut eligible_files_by_table: Vec<(DbId, TableId, Vec<ParquetFileId>)> = Vec::new();
        let mut total_files = 0usize;

        for (db_id, db_tables) in &snap.databases {
            for (table_id, files) in &db_tables.tables {
                let mut ids = Vec::new();
                for file in files {
                    total_files += 1;
                    if is_gen1_file_eligible_for_deletion(file.id, next_file_id, leftover_ids) {
                        eligible_ids.insert(file.id);
                        eligible_paths.push(&file.path);
                        ids.push(file.id);
                    }
                }
                if !ids.is_empty() {
                    eligible_files_by_table.push((*db_id, *table_id, ids));
                }
            }
        }

        if eligible_ids.is_empty() {
            debug!(
                snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                total_files, "No eligible files in snapshot, skipping"
            );
            return ProcessOutcome::Ok;
        }

        let all_eligible = eligible_ids.len() == total_files;

        self.persisted_files.remove_compacted_files(&eligible_ids);

        let delete_result =
            delete_files_concurrent(self.persister.object_store(), eligible_paths, concurrency)
                .await;
        let files_deleted = delete_result.deleted;
        let files_not_found = delete_result.not_found;
        let files_failed = delete_result.failed;

        if files_failed > 0 {
            warn!(
                snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                snapshot_path = %meta.location,
                files_failed,
                files_deleted,
                files_not_found,
                "Gen1 cleanup: skipping snapshot deletion due to failed parquet deletes, aborting"
            );
            return ProcessOutcome::Failed;
        }

        for (db_id, table_id, ids) in eligible_files_by_table {
            result
                .removed_file_ids
                .entry((db_id, table_id))
                .or_default()
                .extend(ids);
        }
        result.files_deleted += files_deleted;
        result.files_not_found += files_not_found;

        if all_eligible {
            match self
                .persister
                .object_store()
                .delete_with_default_retries(
                    &meta.location,
                    format!(
                        "Deleting snapshot file (seq: {})",
                        snap.snapshot_sequence_number.as_u64()
                    ),
                )
                .await
            {
                Ok(_) => {
                    result.snapshots_deleted += 1;
                    ProcessOutcome::Ok
                }
                Err(e) => {
                    warn!(
                        snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                        error = %e,
                        "Gen1 cleanup: failed to delete snapshot, aborting"
                    );
                    ProcessOutcome::Failed
                }
            }
        } else {
            debug!(
                snapshot_seq = snap.snapshot_sequence_number.as_u64(),
                files_deleted,
                files_not_found,
                leftover_kept = total_files - eligible_ids.len(),
                "Partial cleanup: deleted eligible files, keeping snapshot for leftover files"
            );
            ProcessOutcome::Ok
        }
    }
}

fn has_files_above_compaction_threshold(
    snapshot: &PersistedSnapshot,
    next_file_id: ParquetFileId,
) -> bool {
    snapshot.databases.iter().any(|(_, db_tables)| {
        db_tables
            .tables
            .iter()
            .any(|(_, files)| files.iter().any(|f| f.id >= next_file_id))
    })
}

/// True if file ID is below threshold and not in the leftover set.
#[inline]
pub fn is_gen1_file_eligible_for_deletion(
    file_id: ParquetFileId,
    next_file_id: ParquetFileId,
    leftover_ids: &HashSet<ParquetFileId>,
) -> bool {
    file_id < next_file_id && !leftover_ids.contains(&file_id)
}

#[cfg(test)]
mod tests;
