//! This tracks what files have been persisted by the write buffer, limited to the last 72 hours.
//! When queries come in they will combine whatever chunks exist from `SegmentState` with
//! the persisted files to get the full set of data to query.

use crate::{ParquetFile, PersistedSegment};
use parking_lot::RwLock;

#[derive(Debug, Default)]
pub struct PersistedFiles {
    /// The map of databases to tables to files
    files: RwLock<hashbrown::HashMap<String, hashbrown::HashMap<String, Vec<ParquetFile>>>>,
}

impl PersistedFiles {
    /// Create a new `PersistedFiles` from a list of persisted segments
    pub fn new_from_persisted_segments(persisted_segments: Vec<PersistedSegment>) -> Self {
        let files = persisted_segments.into_iter().fold(
            hashbrown::HashMap::new(),
            |mut files, persisted_segment| {
                persisted_segment
                    .databases
                    .into_iter()
                    .for_each(|(db_name, tables)| {
                        let db_tables: &mut hashbrown::HashMap<String, Vec<ParquetFile>> =
                            files.entry(db_name).or_default();

                        tables.tables.into_iter().for_each(|(table_name, table)| {
                            let table_files = db_tables.entry(table_name).or_default();
                            table_files.extend(table.parquet_files);
                        });
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

    /// Add all files from a persisted segment
    pub fn add_persisted_segment_files(&self, persisted_segment: PersistedSegment) {
        let mut files = self.files.write();
        persisted_segment
            .databases
            .into_iter()
            .for_each(|(db_name, tables)| {
                let db_tables = files.entry(db_name).or_default();

                tables.tables.into_iter().for_each(|(table_name, table)| {
                    let table_files = db_tables.entry(table_name).or_default();
                    table_files.extend(table.parquet_files);
                });
            });
    }

    /// Get the list of files for a given database and table
    pub fn get_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        let files = self.files.read();
        files
            .get(db_name)
            .and_then(|tables| tables.get(table_name))
            .cloned()
            .unwrap_or_default()
    }
}
