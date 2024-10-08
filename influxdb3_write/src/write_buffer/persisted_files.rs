//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use crate::{ParquetFile, PersistedSnapshot};
use hashbrown::HashMap;
use influxdb3_id::DbId;
use influxdb3_id::TableId;
use influxdb3_telemetry::ParquetMetrics;
use parking_lot::RwLock;

type DatabaseToTables = HashMap<DbId, TableToFiles>;
type TableToFiles = HashMap<TableId, Vec<ParquetFile>>;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    inner: RwLock<Inner>,
}

impl PersistedFiles {
    /// Create a new `PersistedFiles` from a list of persisted snapshots
    pub fn new_from_persisted_snapshots(persisted_snapshots: Vec<PersistedSnapshot>) -> Self {
        let inner = Inner::new_from_persisted_snapshots(persisted_snapshots);
        Self {
            inner: RwLock::new(inner),
        }
    }

    /// Add a file to the list of persisted files
    pub fn add_file(&self, db_id: DbId, table_id: TableId, file: ParquetFile) {
        let mut inner = self.inner.write();
        let tables = inner.files.entry(db_id).or_default();
        let table_files = tables.entry(table_id).or_default();
        table_files.push(file);
    }

    /// Add all files from a persisted snapshot
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut inner = self.inner.write();
        inner.add_persisted_snapshot(persisted_snapshot);
    }

    /// Get the list of files for a given database and table, always return in descending order of min_time
    pub fn get_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        let mut files = {
            let inner = self.inner.read();
            inner
                .files
                .get(&db_id)
                .and_then(|tables| tables.get(&table_id))
                .cloned()
                .unwrap_or_default()
        };

        files.sort_by(|a, b| b.min_time.cmp(&a.min_time));

        files
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
}

impl Inner {
    pub fn new_from_persisted_snapshots(persisted_snapshots: Vec<PersistedSnapshot>) -> Self {
        let mut file_count = 0;
        let mut size_in_mb = 0.0;
        let mut row_count = 0;

        let files = persisted_snapshots.into_iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_snapshot| {
                size_in_mb += as_mb(persisted_snapshot.parquet_size_bytes);
                row_count += persisted_snapshot.row_count;
                let parquet_files_added =
                    update_persisted_files_with_snapshot(true, persisted_snapshot, &mut files);
                file_count += parquet_files_added;
                files
            },
        );

        Self {
            files,
            parquet_files_count: file_count,
            parquet_files_row_count: row_count,
            parquet_files_size_mb: size_in_mb,
        }
    }

    pub fn add_persisted_snapshot(&mut self, persisted_snapshot: PersistedSnapshot) {
        self.parquet_files_row_count += persisted_snapshot.row_count;
        self.parquet_files_size_mb += as_mb(persisted_snapshot.parquet_size_bytes);
        let file_count =
            update_persisted_files_with_snapshot(false, persisted_snapshot, &mut self.files);
        self.parquet_files_count += file_count;
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
) -> u64 {
    let mut file_count = 0;
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
    file_count
}

#[cfg(test)]
mod tests {

    use influxdb3_catalog::catalog::SequenceNumber;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
    use observability_deps::tracing::info;
    use pretty_assertions::assert_eq;

    use crate::ParquetFileId;

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
        let parquet_files = build_parquet_files(5);
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
        let mut parquet_files = build_parquet_files(4);
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

    fn build_persisted_snapshots() -> Vec<PersistedSnapshot> {
        let mut all_persisted_snapshot_files = Vec::new();
        let parquet_files_1 = build_parquet_files(5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_1, 1, 1, 1));

        let parquet_files_2 = build_parquet_files(5);
        all_persisted_snapshot_files.push(build_snapshot(parquet_files_2, 2, 2, 2));

        all_persisted_snapshot_files
    }

    fn build_snapshot(
        parquet_files: Vec<ParquetFile>,
        snapshot_id: u64,
        wal_id: u64,
        catalog_id: u32,
    ) -> PersistedSnapshot {
        let snap1 = SnapshotSequenceNumber::new(snapshot_id);
        let wal1 = WalFileSequenceNumber::new(wal_id);
        let cat1 = SequenceNumber::new(catalog_id);
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

    fn build_parquet_files(num_files: u32) -> Vec<ParquetFile> {
        let parquet_files: Vec<ParquetFile> = (0..num_files)
            .map(|_| ParquetFile {
                id: ParquetFileId::new(),
                path: "/random/path/file".to_owned(),
                size_bytes: 50_000,
                row_count: 10,
                chunk_time: 10,
                min_time: 10,
                max_time: 200,
            })
            .collect();
        parquet_files
    }
}
