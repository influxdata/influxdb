//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use std::sync::Arc;

use crate::deleter::ObjectDeleter;
use crate::{ChunkFilter, DatabaseTables};
use crate::{ParquetFile, PersistedSnapshot};
use hashbrown::{HashMap, HashSet};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::TableId;
use influxdb3_id::{DbId, SerdeVecMap};
use influxdb3_telemetry::ParquetMetrics;
use parking_lot::RwLock;

type DatabaseToTables = HashMap<DbId, TableToFiles>;
type TableToFiles = HashMap<TableId, Vec<ParquetFile>>;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
enum DeletedTables {
    /// All tables in the database are marked for deletion
    All,
    /// A list of tables in the database that are marked for deletion
    List(HashSet<TableId>),
}

impl ObjectDeleter for PersistedFiles {
    fn delete_database(&self, db_id: DbId) {
        let mut inner = self.inner.write();
        inner.deleted_data.insert(db_id, DeletedTables::All);
    }

    fn delete_table(&self, db_id: DbId, table_id: TableId) {
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
}

impl PersistedFiles {
    pub fn new() -> Self {
        Default::default()
    }
    /// Create a new `PersistedFiles` from a list of persisted snapshots
    pub fn new_from_persisted_snapshots(persisted_snapshots: Vec<PersistedSnapshot>) -> Self {
        let inner = Inner::new_from_persisted_snapshots(persisted_snapshots);
        Self {
            inner: RwLock::new(inner),
        }
    }

    /// Add all files from a persisted snapshot
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot);
    }

    /// Add single file to a table
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

        files.sort_by(|a, b| b.min_time.cmp(&a.min_time));

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

        guard.parquet_files_count -= removed_paths.len() as u64;
        guard.parquet_files_size_mb -= as_mb(size);
        guard.parquet_files_row_count -= row_count;

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
    pub(crate) fn new_from_persisted_snapshots(
        persisted_snapshots: Vec<PersistedSnapshot>,
    ) -> Self {
        let mut file_count = 0;
        let mut size_in_mb = 0.0;
        let mut row_count = 0;

        let files = persisted_snapshots.into_iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_snapshot| {
                size_in_mb += as_mb(persisted_snapshot.parquet_size_bytes);
                row_count += persisted_snapshot.row_count;
                let (parquet_files_added, removed_size, removed_row_count) =
                    update_persisted_files_with_snapshot(true, persisted_snapshot, &mut files);
                file_count += parquet_files_added;
                size_in_mb -= as_mb(removed_size);
                row_count -= removed_row_count;
                files
            },
        );

        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_row_count: row_count,
            parquet_files_size_mb: size_in_mb,
            deleted_data: HashMap::new(),
        }
    }

    pub(crate) fn add_persisted_snapshot(&mut self, persisted_snapshot: PersistedSnapshot) {
        self.parquet_files_row_count += persisted_snapshot.row_count;
        self.parquet_files_size_mb += as_mb(persisted_snapshot.parquet_size_bytes);
        let (file_count, removed_file_size, removed_row_count) =
            update_persisted_files_with_snapshot(false, persisted_snapshot, &mut self.files);
        self.parquet_files_row_count -= removed_row_count;
        self.parquet_files_size_mb -= as_mb(removed_file_size);
        self.parquet_files_count += file_count;
    }

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

fn as_mb(bytes: u64) -> f64 {
    let factor = (1_000 * 1_000) as f64;
    bytes as f64 / factor
}

fn update_persisted_files_with_snapshot(
    initial_load: bool,
    persisted_snapshot: PersistedSnapshot,
    db_to_tables: &mut HashMap<DbId, HashMap<TableId, Vec<ParquetFile>>>,
) -> (u64, u64, u64) {
    let (mut file_count, mut removed_size, mut removed_row_count) = (0, 0, 0);
    persisted_snapshot
        .databases
        .into_iter()
        .for_each(|(db_id, tables)| {
            let db_tables: &mut HashMap<TableId, Vec<ParquetFile>> =
                db_to_tables.entry(db_id).or_default();

            tables
                .tables
                .into_iter()
                .for_each(|(table_id, mut new_parquet_files)| {
                    let table_files = db_tables.entry(table_id).or_default();
                    if initial_load {
                        file_count += new_parquet_files.len() as u64;
                        table_files.append(&mut new_parquet_files);
                    } else {
                        let mut filtered_files: Vec<ParquetFile> = new_parquet_files
                            .into_iter()
                            .filter(|file| !table_files.contains(file))
                            .collect();
                        file_count += filtered_files.len() as u64;
                        table_files.append(&mut filtered_files);
                    }
                });
        });

    // We now remove any files as we load the snapshots if they exist.
    persisted_snapshot
        .removed_files
        .into_iter()
        .for_each(|(db_id, tables)| {
            let db_tables: &mut HashMap<TableId, Vec<ParquetFile>> =
                db_to_tables.entry(db_id).or_default();

            tables
                .tables
                .into_iter()
                .for_each(|(table_id, remove_parquet_files)| {
                    let table_files = db_tables.entry(table_id).or_default();
                    for file in remove_parquet_files {
                        if let Some(idx) = table_files.iter().position(|f| f.id == file.id) {
                            let file = table_files.remove(idx);
                            file_count -= 1;
                            removed_size -= file.size_bytes;
                            removed_row_count -= file.row_count;
                        }
                    }
                });
        });

    (file_count, removed_size, removed_row_count)
}

#[cfg(test)]
mod tests {

    use crate::ParquetFileId;
    use datafusion::prelude::Expr;
    use datafusion::prelude::col;
    use datafusion::prelude::lit_timestamp_nano;
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_catalog::catalog::TableDefinition;
    use influxdb3_id::ColumnId;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use observability_deps::tracing::info;
    use pretty_assertions::assert_eq;
    use schema::InfluxColumnType;
    use std::sync::Arc;

    use super::*;

    #[test_log::test(test)]
    fn test_get_metrics_after_initial_load() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(10, file_count);
        assert_eq!(0.5, size_in_mb);
        assert_eq!(100, row_count);
    }

    #[test_log::test(test)]
    fn test_get_metrics_after_update() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);
        let parquet_files = build_parquet_files("file_", 5);
        let new_snapshot = build_snapshot(parquet_files, 1, 1, 1);
        persisted_file.add_persisted_snapshot_files(new_snapshot);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(15, file_count);
        assert_eq!(0.75, size_in_mb);
        assert_eq!(150, row_count);
    }

    #[test_log::test(test)]
    fn test_get_metrics_after_update_with_duplicate_file() {
        let all_persisted_snapshot_files = build_persisted_snapshots();
        let already_existing_file = all_persisted_snapshot_files
            .last()
            .unwrap()
            .databases
            .get(&DbId::from(0))
            .unwrap()
            .tables
            .get(&TableId::from(0))
            .unwrap()
            .last()
            .cloned()
            .unwrap();

        let persisted_file =
            PersistedFiles::new_from_persisted_snapshots(all_persisted_snapshot_files);
        let mut parquet_files = build_parquet_files("file_", 4);
        info!(all_persisted_files = ?persisted_file, "Full persisted file");
        info!(already_existing_file = ?already_existing_file, "Existing file");
        parquet_files.push(already_existing_file);

        let new_snapshot = build_snapshot(parquet_files, 1, 1, 1);
        persisted_file.add_persisted_snapshot_files(new_snapshot);

        let (file_count, size_in_mb, row_count) = persisted_file.get_metrics();
        info!(all_persisted_files = ?persisted_file, "Full persisted file after");

        info!(metrics = ?persisted_file.get_metrics(), "All files metrics");
        assert_eq!(14, file_count);
        // TODO: Just tying in TODO within build_snapshot function below. Even though
        //       there are only 14 files added to persisted_file the below 2 metrics
        //       are for 15 files because of using `add_parquet_file` directly which
        //       doesn't check for duplicates
        assert_eq!(0.75, size_in_mb);
        assert_eq!(150, row_count);
    }

    #[test]
    fn test_get_files_with_filters() {
        let parquet_files = (0..100)
            .step_by(10)
            .map(|i| {
                let chunk_time = i;
                ParquetFile {
                    id: ParquetFileId::new(),
                    path: format!("/path/{i:03}.parquet"),
                    size_bytes: 1,
                    row_count: 1,
                    chunk_time,
                    min_time: chunk_time,
                    max_time: chunk_time + 10,
                }
            })
            .collect();
        let persisted_snapshots = vec![build_snapshot(parquet_files, 0, 0, 0)];
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(persisted_snapshots);

        struct TestCase<'a> {
            filter: &'a [Expr],
            expected_n_files: usize,
        }

        let test_cases = [
            TestCase {
                filter: &[],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(0))],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(50))],
                expected_n_files: 5,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(90))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time").gt(lit_timestamp_nano(100))],
                expected_n_files: 0,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(100))],
                expected_n_files: 10,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(50))],
                expected_n_files: 5,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(10))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time").lt(lit_timestamp_nano(0))],
                expected_n_files: 0,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(20))
                    .and(col("time").lt(lit_timestamp_nano(40)))],
                expected_n_files: 2,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(20))
                    .and(col("time").lt(lit_timestamp_nano(30)))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(21))
                    .and(col("time").lt(lit_timestamp_nano(29)))],
                expected_n_files: 1,
            },
            TestCase {
                filter: &[col("time")
                    .gt(lit_timestamp_nano(0))
                    .and(col("time").lt(lit_timestamp_nano(100)))],
                expected_n_files: 10,
            },
        ];

        let table_def = Arc::new(
            TableDefinition::new(
                TableId::from(0),
                "test-tbl".into(),
                vec![(
                    ColumnId::from(0),
                    "time".into(),
                    InfluxColumnType::Timestamp,
                )],
                vec![],
            )
            .unwrap(),
        );

        for t in test_cases {
            let filter = ChunkFilter::new(&table_def, t.filter).unwrap();
            let filtered_files =
                persisted_files.get_files_filtered(DbId::from(0), TableId::from(0), &filter);
            assert_eq!(
                t.expected_n_files,
                filtered_files.len(),
                "wrong number of filtered files:\n\
                result: {filtered_files:?}\n\
                filter provided: {filter:?}"
            );
        }
    }

    fn build_persisted_snapshots() -> Vec<PersistedSnapshot> {
        let mut all_persisted_snapshot_files = Vec::new();
        let parquet_files_1 = build_parquet_files("file_", 5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_1, 1, 1, 1));

        let parquet_files_2 = build_parquet_files("file_", 5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_2, 2, 2, 2));

        all_persisted_snapshot_files
    }

    fn build_snapshot(
        parquet_files: Vec<ParquetFile>,
        snapshot_id: u64,
        wal_id: u64,
        catalog_id: u64,
    ) -> PersistedSnapshot {
        let snap1 = SnapshotSequenceNumber::new(snapshot_id);
        let wal1 = WalFileSequenceNumber::new(wal_id);
        let cat1 = CatalogSequenceNumber::new(catalog_id);
        let mut new_snapshot =
            PersistedSnapshot::new("sample-host-id".to_owned(), snap1, wal1, cat1);
        parquet_files.into_iter().for_each(|file| {
            // TODO: Check why `add_parquet_file` method does not check if file is
            //       already present. This is checked when trying to add a new PersistedSnapshot
            //       as part of snapshotting process.
            new_snapshot.add_parquet_file(DbId::from(0), TableId::from(0), file);
        });
        new_snapshot
    }

    fn build_parquet_files(prefix: &str, num_files: u32) -> Vec<ParquetFile> {
        let parquet_files: Vec<ParquetFile> = (0..num_files)
            .map(|i| ParquetFile {
                id: ParquetFileId::new(),
                path: format!("/random/path/{prefix}_{}.parquet", i),
                size_bytes: 50_000,
                row_count: 10,
                chunk_time: 10,
                min_time: 10,
                max_time: 200,
            })
            .collect();
        parquet_files
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_deleted_database() {
        let parquet_files = build_parquet_files("file_", 5);
        let mut snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);

        // Add files to a second database
        let db_id2 = DbId::from(1);
        let table_id2 = TableId::from(1);
        let more_files = build_parquet_files("db2_", 3);
        for file in more_files.iter() {
            snapshot.add_parquet_file(db_id2, table_id2, file.clone());
        }

        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete the first database
        persisted_files.delete_database(DbId::from(0));

        // Create a mock catalog that returns no retention periods
        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        let removed = persisted_files.remove_files_for_deletion(catalog);

        // Verify all files from database 0 are removed
        assert_eq!(removed.len(), 1);
        let db_tables = removed.get(&DbId::from(0)).unwrap();
        assert_eq!(db_tables.tables.len(), 1);
        let table_files = db_tables.tables.get(&TableId::from(0)).unwrap();
        assert_eq!(table_files.len(), 5);

        // Verify database 1 files remain
        let remaining_files = persisted_files.get_files(db_id2, table_id2);
        assert_eq!(remaining_files.len(), 3);

        // Verify metrics are updated
        let (file_count, size_mb, row_count) = persisted_files.get_metrics();
        assert_eq!(file_count, 3);
        assert!((size_mb - 0.15).abs() < 0.0001); // 3 files * 50_000 bytes (check with epsilon for floating point)
        assert_eq!(row_count, 30); // 3 files * 10 rows
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_deleted_tables() {
        let parquet_files = build_parquet_files("file_", 3);
        let mut snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);

        // Add files to multiple tables in the same database
        let db_id = DbId::from(0);
        let table_id1 = TableId::from(0);
        let table_id2 = TableId::from(1);
        let table_id3 = TableId::from(2);

        let table2_files = build_parquet_files("table2_", 4);
        for file in table2_files.iter() {
            snapshot.add_parquet_file(db_id, table_id2, file.clone());
        }

        let table3_files: Vec<ParquetFile> = build_parquet_files("table_3", 2);
        for file in table3_files.iter() {
            snapshot.add_parquet_file(db_id, table_id3, file.clone());
        }

        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete specific tables
        persisted_files.delete_table(db_id, table_id1);
        persisted_files.delete_table(db_id, table_id3);

        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        let removed = persisted_files.remove_files_for_deletion(catalog);

        // Verify only tables 1 and 3 are removed
        assert_eq!(removed.len(), 1);
        let db_tables = removed.get(&db_id).unwrap();
        assert_eq!(db_tables.tables.len(), 2);

        let table1_removed = db_tables.tables.get(&table_id1).unwrap();
        assert_eq!(table1_removed.len(), 3);

        let table3_removed = db_tables.tables.get(&table_id3).unwrap();
        assert_eq!(table3_removed.len(), 2);

        // Verify table 2 remains
        let table2_remaining = persisted_files.get_files(db_id, table_id2);
        assert_eq!(table2_remaining.len(), 4);

        // Verify metrics
        let (file_count, size_mb, row_count) = persisted_files.get_metrics();
        assert_eq!(file_count, 4);
        assert_eq!(size_mb, 0.2); // 4 files * 50_000 bytes
        assert_eq!(row_count, 40); // 4 files * 10 rows
    }

    #[tokio::test]
    async fn test_remove_files_for_deletion_clears_deleted_data() {
        let parquet_files = build_parquet_files("file_", 3);
        let snapshot = build_snapshot(parquet_files.clone(), 1, 1, 1);
        let persisted_files = PersistedFiles::new_from_persisted_snapshots(vec![snapshot]);

        // Delete a database
        persisted_files.delete_database(DbId::from(0));

        // Verify deleted_data is populated
        {
            let inner = persisted_files.inner.read();
            assert_eq!(inner.deleted_data.len(), 1);
        }

        let catalog = Arc::new(Catalog::new_in_memory("test").await.unwrap());

        persisted_files.remove_files_for_deletion(catalog);

        // Verify deleted_data is cleared after removal
        {
            let inner = persisted_files.inner.read();
            assert_eq!(inner.deleted_data.len(), 0);
        }
    }
}
