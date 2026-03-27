//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use std::sync::Arc;

use crate::deleter::ObjectDeleter;
use crate::table_index_cache::TableIndexCache;
use crate::{ChunkFilter, DatabaseTables};
use crate::{ParquetFile, PersistedSnapshot, PersistedSnapshotCheckpoint};
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::TableId;
use influxdb3_id::{DbId, SerdeVecMap};
use influxdb3_telemetry::ParquetMetrics;
use observability_deps::tracing::{debug, trace, warn};
use parking_lot::RwLock;

type DatabaseToTables = HashMap<DbId, TableToFiles>;
type TableToFiles = HashMap<TableId, Vec<ParquetFile>>;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    inner: RwLock<Inner>,
    table_index_cache: Option<TableIndexCache>,
}

#[derive(Debug)]
enum DeletedTables {
    /// All tables in the database are marked for deletion
    All,
    /// A list of tables in the database that are marked for deletion
    List(HashSet<TableId>),
}

#[async_trait::async_trait]
impl ObjectDeleter for PersistedFiles {
    async fn delete_database(
        &self,
        db_id: DbId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        {
            let mut inner = self.inner.write();
            inner.deleted_data.insert(db_id, DeletedTables::All);
        }

        // Purge from table index cache if available
        //
        // NOTE(wayne): in theory we could just leave actual purging of tables to individual
        // `delete_table` calls, but that would potentially removing data for tables that are
        // already removed from the catalog and PersistedFiles but not yet removed from the
        // object store, whereas explicitly purging by database through the TableIndexCache
        // ensures that we are deleting all table data from the object store
        if let Some(table_index_cache) = &self.table_index_cache {
            table_index_cache
                .purge_db(&db_id)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)?;
        }

        Ok(())
    }

    async fn delete_table(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        {
            let mut inner = self.inner.write();
            match inner.deleted_data.entry(db_id) {
                hashbrown::hash_map::Entry::Occupied(mut entry) => {
                    match entry.get_mut() {
                        DeletedTables::All => (), // already marked for deletion
                        DeletedTables::List(tables) => {
                            tables.insert(table_id);
                        }
                    }
                }
                hashbrown::hash_map::Entry::Vacant(entry) => {
                    entry.insert(DeletedTables::List(HashSet::from([table_id])));
                }
            }
        }
        if let Some(cache) = &self.table_index_cache {
            cache
                .purge_table(&db_id, &table_id)
                .await
                .map_err(Box::new)?
        }
        Ok(())
    }
}

impl PersistedFiles {
    pub fn new(table_index_cache: Option<TableIndexCache>) -> Self {
        Self {
            table_index_cache,
            ..Default::default()
        }
    }

    /// Create a new `PersistedFiles` from a list of persisted snapshots
    ///
    /// Accepts `Arc<Vec<PersistedSnapshot>>` to allow sharing the snapshot data
    /// between multiple consumers (e.g., PersistedFiles and background checkpoint building)
    /// without cloning the entire Vec.
    pub fn new_from_persisted_snapshots(
        table_index_cache: Option<TableIndexCache>,
        persisted_snapshots: Arc<Vec<PersistedSnapshot>>,
    ) -> Self {
        let inner = Inner::new_from_persisted_snapshots(persisted_snapshots);
        Self {
            table_index_cache,
            inner: RwLock::new(inner),
        }
    }

    /// Create a new `PersistedFiles` from checkpoints and additional snapshots.
    ///
    /// This is the preferred method when checkpoints are available, as it reduces
    /// the amount of data to process during startup:
    /// 1. Merge all checkpoints (one per month, sorted by month)
    /// 2. Apply pending_removed_files from each checkpoint to remove cross-month references
    /// 3. Apply additional snapshots (those newer than the latest checkpoint)
    pub fn new_from_checkpoints_and_snapshots(
        table_index_cache: Option<TableIndexCache>,
        checkpoints: Vec<PersistedSnapshotCheckpoint>,
        additional_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        let inner = Inner::new_from_checkpoints_and_snapshots(checkpoints, additional_snapshots);
        Self {
            table_index_cache,
            inner: RwLock::new(inner),
        }
    }

    /// Add all files from a persisted snapshot to the tracked files.
    ///
    /// Deduplicates against existing files.
    ///
    /// Called from `Replica::reload_snapshots` during replica recovery.
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot);
    }

    /// Add a single parquet file to the tracked files for a specific table.
    ///
    /// Called from `QueryableBuffer` after persistence and from `Replica` during
    /// background cache loading.
    pub fn add_persisted_file(&self, db_id: &DbId, table_id: &TableId, parquet_file: &ParquetFile) {
        let mut inner = self.inner.write();
        inner.add_persisted_file(db_id, table_id, parquet_file);
    }

    /// Get the list of files for a given database and table, always return in descending order of min_time
    pub fn get_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.get_files_filtered(db_id, table_id, &ChunkFilter::default())
    }

    /// Get the list of files for a given database and table, using the provided filter to filter results.
    ///
    /// Always return in descending order of min_time
    pub fn get_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        let inner = self.inner.read();
        let mut files = inner
            .files
            .get(&db_id)
            .and_then(|tables| tables.get(&table_id))
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|file| filter.test_time_stamp_min_max(file.min_time, file.max_time))
            .collect::<Vec<_>>();

        files.sort_by_key(|f| std::cmp::Reverse(f.min_time));

        files
    }

    /// Remove files that are marked for deletion or that violate their retention period.
    pub fn remove_files_for_deletion(
        &self,
        catalog: Arc<Catalog>,
    ) -> SerdeVecMap<DbId, DatabaseTables> {
        let mut removed: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
        let mut removed_paths: HashSet<String> = HashSet::new();
        let mut size = 0;
        let mut row_count = 0;

        // First pass is under a read lock to permit queries running concurrently.
        {
            let mut queue_for_removal = |db_id: DbId, table_id: TableId, file: &ParquetFile| {
                // Guard to prevent adding a file more than once.
                if removed_paths.contains(&file.path) {
                    return;
                }

                size += file.size_bytes;
                row_count += file.row_count;
                removed
                    .entry(db_id)
                    .or_default()
                    .tables
                    .entry(table_id)
                    .or_default()
                    .push(file.clone());
                removed_paths.insert(file.path.clone());
            };

            let guard = self.inner.read();

            // Remove any data marked for hard-deletion.
            for (db_id, deleted) in guard.deleted_data.iter() {
                let Some(tables) = guard.files.get(db_id) else {
                    continue;
                };

                match deleted {
                    DeletedTables::All => {
                        for (table_id, files) in tables {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                    DeletedTables::List(table_ids) => {
                        for (table_id, files) in table_ids.iter().filter_map(|table_id| {
                            tables.get(table_id).map(|file| (table_id, file))
                        }) {
                            for file in files {
                                queue_for_removal(*db_id, *table_id, file);
                            }
                        }
                    }
                }
            }

            let retention_periods = catalog.get_retention_period_cutoff_map();

            for ((db_id, table_id), cutoff) in retention_periods {
                // If the database or table is deleted, the files are already scheduled for deletion.
                match guard.deleted_data.get(&db_id) {
                    Some(DeletedTables::All) => {
                        continue;
                    }
                    Some(DeletedTables::List(tables)) if tables.contains(&table_id) => {
                        continue;
                    }
                    _ => {}
                }

                let Some(files) = guard.files.get(&db_id).and_then(|hm| hm.get(&table_id)) else {
                    continue;
                };
                for file in files {
                    // remove files if their max time (aka newest timestamp) is less than (aka older
                    // than) the cutoff timestamp for the retention period
                    if file.max_time < cutoff {
                        queue_for_removal(db_id, table_id, file);
                    }
                }
            }
        }

        // if no persisted files are found to be in violation of their retention period, then
        // return an empty result to avoid unnecessarily acquiring a write lock
        if removed.is_empty() {
            return removed;
        }

        let mut guard = self.inner.write();
        for (_, tables) in guard.files.iter_mut() {
            for (_, files) in tables.iter_mut() {
                files.retain(|file| !removed_paths.contains(&file.path))
            }
        }

        guard.parquet_files_count = guard
            .parquet_files_count
            .saturating_sub(removed_paths.len() as u64);
        guard.parquet_files_size_mb -= as_mb(size);
        guard.parquet_files_row_count = guard.parquet_files_row_count.saturating_sub(row_count);

        // The deleted data has been processed.
        guard.deleted_data = HashMap::new();

        removed
    }
}

impl ParquetMetrics for PersistedFiles {
    /// Get parquet file metrics, file count, row count and size in MB
    fn get_metrics(&self) -> (u64, f64, u64) {
        let inner = self.inner.read();
        (
            inner.parquet_files_count,
            inner.parquet_files_size_mb,
            inner.parquet_files_row_count,
        )
    }
}

#[derive(Debug, Default)]
struct Inner {
    /// The map of databases to tables to files
    pub files: DatabaseToTables,
    /// Overall count of the parquet files
    pub parquet_files_count: u64,
    /// Total size of all parquet files in MB
    pub parquet_files_size_mb: f64,
    /// Overall row count within the parquet files
    pub parquet_files_row_count: u64,
    /// Data that are marked for deletion.
    pub deleted_data: HashMap<DbId, DeletedTables>,
}

impl Inner {
    /// Create from persisted snapshots via shared Arc reference.
    ///
    /// Uses reference-based iteration to avoid consuming the Arc'd Vec,
    /// allowing the same snapshot data to be shared with other consumers.
    pub(crate) fn new_from_persisted_snapshots(
        persisted_snapshots: Arc<Vec<PersistedSnapshot>>,
    ) -> Self {
        trace!(
            snapshot_count = persisted_snapshots.len(),
            "new_from_persisted_snapshots: starting"
        );
        let mut file_count = 0;
        let mut size_in_mb = 0.0;
        let mut row_count = 0;

        let files = persisted_snapshots.iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_snapshot| {
                size_in_mb += as_mb(persisted_snapshot.parquet_size_bytes);
                row_count += persisted_snapshot.row_count;
                let (parquet_files_added, removed_size, removed_row_count) =
                    update_persisted_files_with_snapshot(true, persisted_snapshot, &mut files);
                file_count += parquet_files_added;
                size_in_mb -= as_mb(removed_size);
                row_count = row_count.saturating_sub(removed_row_count);
                files
            },
        );

        trace!(
            file_count,
            row_count, size_in_mb, "new_from_persisted_snapshots: completed"
        );
        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_row_count: row_count,
            parquet_files_size_mb: size_in_mb,
            deleted_data: HashMap::new(),
        }
    }

    /// Create from checkpoints and additional (newer) snapshots.
    pub(crate) fn new_from_checkpoints_and_snapshots(
        mut checkpoints: Vec<PersistedSnapshotCheckpoint>,
        additional_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        debug!(
            checkpoint_count = checkpoints.len(),
            snapshot_count = additional_snapshots.len(),
            "new_from_checkpoints_and_snapshots: starting"
        );

        // Sort checkpoints by year_month to process in chronological order
        checkpoints.sort_by_key(|c| c.year_month);

        // Merge all checkpoints into one
        let merged_checkpoint = checkpoints.into_iter().reduce(|mut acc, checkpoint| {
            acc.merge(checkpoint);
            acc
        });

        // Convert merged checkpoint to Inner, or start with empty
        let mut inner = merged_checkpoint.map(Inner::from).unwrap_or_default();

        // Apply additional snapshots
        for snapshot in additional_snapshots {
            inner.add_persisted_snapshot(snapshot);
        }

        debug!(
            file_count = inner.parquet_files_count,
            row_count = inner.parquet_files_row_count,
            size_in_mb = inner.parquet_files_size_mb,
            "new_from_checkpoints_and_snapshots: completed"
        );

        inner
    }

    /// Merges all files from a [`PersistedSnapshot`] into the persisted files hierarchy.
    ///
    /// Deduplicates incoming files. Called at runtime (not during initial load).
    pub(crate) fn add_persisted_snapshot(&mut self, persisted_snapshot: PersistedSnapshot) {
        self.parquet_files_row_count += persisted_snapshot.row_count;
        self.parquet_files_size_mb += as_mb(persisted_snapshot.parquet_size_bytes);
        let (file_count, removed_file_size, removed_row_count) =
            update_persisted_files_with_snapshot(false, &persisted_snapshot, &mut self.files);
        self.parquet_files_row_count = self
            .parquet_files_row_count
            .saturating_sub(removed_row_count);
        self.parquet_files_size_mb -= as_mb(removed_file_size);
        self.parquet_files_count += file_count;
    }

    /// Adds a single parquet file to the specified database and table.
    ///
    /// Creates db/table entries if needed. Skips duplicates.
    pub(crate) fn add_persisted_file(
        &mut self,
        db_id: &DbId,
        table_id: &TableId,
        parquet_file: &ParquetFile,
    ) {
        let existing_parquet_files = self
            .files
            .entry(*db_id)
            .or_default()
            .entry(*table_id)
            .or_default();
        if !existing_parquet_files.contains(parquet_file) {
            self.parquet_files_row_count += parquet_file.row_count;
            self.parquet_files_size_mb += as_mb(parquet_file.size_bytes);
            existing_parquet_files.push(parquet_file.clone());
        }
        self.parquet_files_count += 1;
    }
}

impl From<PersistedSnapshotCheckpoint> for Inner {
    fn from(checkpoint: PersistedSnapshotCheckpoint) -> Self {
        let mut files: DatabaseToTables = HashMap::new();
        let mut file_count: u64 = 0;

        for (db_id, db_tables) in checkpoint.databases {
            for (table_id, parquet_files) in db_tables.tables {
                file_count += parquet_files.len() as u64;
                files
                    .entry(db_id)
                    .or_default()
                    .entry(table_id)
                    .or_default()
                    .extend(parquet_files);
            }
        }

        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_size_mb: as_mb(checkpoint.parquet_size_bytes),
            parquet_files_row_count: checkpoint.row_count,
            deleted_data: HashMap::new(),
        }
    }
}

fn as_mb(bytes: u64) -> f64 {
    let factor = (1_000 * 1_000) as f64;
    bytes as f64 / factor
}

/// Merges parquet files from a [`PersistedSnapshot`] into the db/table hierarchy.
///
/// Returns `(file_count_delta, removed_size_bytes, removed_row_count)`.
///
/// When `initial_load=true` (startup): appends without deduplication.
/// When `initial_load=false` (runtime): filters duplicates before appending.
///
/// Also processes `snapshot.removed_files`, removing matching files by ID.
fn update_persisted_files_with_snapshot(
    initial_load: bool,
    persisted_snapshot: &PersistedSnapshot,
    db_to_tables: &mut HashMap<DbId, HashMap<TableId, Vec<ParquetFile>>>,
) -> (u64, u64, u64) {
    let (mut file_count, mut removed_size, mut removed_row_count): (u64, u64, u64) = (0, 0, 0);
    persisted_snapshot
        .databases
        .iter()
        .for_each(|(db_id, tables)| {
            let db_tables: &mut HashMap<TableId, Vec<ParquetFile>> = db_to_tables
                .entry(*db_id)
                .or_insert_with(|| HashMap::with_capacity(tables.tables.len()));

            tables
                .tables
                .iter()
                .for_each(|(table_id, new_parquet_files)| {
                    let table_files = db_tables
                        .entry(*table_id)
                        .or_insert_with(|| Vec::with_capacity(new_parquet_files.len()));
                    if initial_load {
                        file_count += new_parquet_files.len() as u64;
                        table_files.extend(new_parquet_files.iter().cloned());
                    } else {
                        let mut filtered_files: Vec<ParquetFile> = new_parquet_files
                            .iter()
                            .filter(|file| !table_files.contains(file))
                            .cloned()
                            .collect();
                        file_count += filtered_files.len() as u64;
                        table_files.append(&mut filtered_files);
                    }
                });
        });

    // We now remove any files as we load the snapshots if they exist.
    persisted_snapshot
        .removed_files
        .iter()
        .for_each(|(db_id, tables)| {
            let Some(db_tables) = db_to_tables.get_mut(db_id) else {
                // this can happen if the table(s) to remove from PersistedFiles is further back in
                // history than the default number of snapshots loaded during server initialization
                warn!(
                    db_id = ?db_id,
                    "expected to remove tables for db in persisted files"
                );
                return;
            };

            tables
                .tables
                .iter()
                .for_each(|(table_id, remove_parquet_files)| {
                    let Some(table_files) = db_tables.get_mut(table_id) else {
                        // this can happen if the table(s) to remove from PersistedFiles is further back in
                        // history than the default number of snapshots loaded during server initialization
                        warn!(
                            db_id = ?db_id,
                            table_id = ?table_id,
                            "expected to remove table from db in persisted files"
                        );
                        return;
                    };
                    for file in remove_parquet_files {
                        if let Some(idx) = table_files.iter().position(|f| f.id == file.id) {
                            let file = table_files.swap_remove(idx);
                            file_count = file_count.saturating_sub(1);
                            removed_size += file.size_bytes;
                            removed_row_count += file.row_count;
                        }
                    }
                });
        });

    (file_count, removed_size, removed_row_count)
}

#[cfg(test)]
mod tests;
