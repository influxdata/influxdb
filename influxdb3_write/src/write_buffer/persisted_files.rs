//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `QueryableBuffer` with
//! the persisted files to get the full set of data to query.

use crate::{ParquetFile, PersistedSnapshot};
use hashbrown::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    /// The map of databases to tables to files
    files: RwLock<DatabaseToTables>,
}

type DatabaseToTables = HashMap<Arc<str>, TableToFiles>;
type TableToFiles = HashMap<Arc<str>, Vec<ParquetFile>>;

impl PersistedFiles {
    /// Create a new `PersistedFiles` from a list of persisted snapshots
    pub fn new_from_persisted_snapshots(persisted_snapshots: Vec<PersistedSnapshot>) -> Self {
        let files = persisted_snapshots.into_iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_snapshot| {
                persisted_snapshot
                    .databases
                    .into_iter()
                    .for_each(|(db_name, tables)| {
                        let db_tables: &mut HashMap<Arc<str>, Vec<ParquetFile>> =
                            files.entry(db_name).or_default();

                        tables.tables.into_iter().for_each(
                            |(table_name, mut new_parquet_files)| {
                                let table_files = db_tables.entry(table_name).or_default();
                                table_files.append(&mut new_parquet_files);
                            },
                        );
                    });

                files
            },
        );

        Self {
            files: RwLock::new(files),
        }
    }

    /// Add a file to the list of persisted files
    pub fn add_file(&self, db_name: &str, table_name: &str, file: ParquetFile) {
        let mut files = self.files.write();
        let tables = files.entry_ref(db_name).or_default();
        let table_files = tables.entry_ref(table_name).or_default();
        table_files.push(file);
    }

    /// Add all files from a persisted snapshot
    pub fn add_persisted_snapshot_files(&self, persisted_snapshot: PersistedSnapshot) {
        let mut files = self.files.write();
        persisted_snapshot
            .databases
            .into_iter()
            .for_each(|(db_name, tables)| {
                let db_tables = files.entry(db_name).or_default();

                tables
                    .tables
                    .into_iter()
                    .for_each(|(table_name, new_parquet_files)| {
                        let table_files = db_tables.entry(table_name).or_default();
                        table_files.append(
                            &mut new_parquet_files
                                .into_iter()
                                .filter(|file| !table_files.contains(file))
                                .collect(),
                        );
                    });
            });
    }

    /// Get the list of files for a given database and table, always return in descending order of min_time
    pub fn get_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        let mut files = {
            let files = self.files.read();
            files
                .get(db_name)
                .and_then(|tables| tables.get(table_name))
                .cloned()
                .unwrap_or_default()
        };

        files.sort_by(|a, b| b.min_time.cmp(&a.min_time));

        files
    }
}
