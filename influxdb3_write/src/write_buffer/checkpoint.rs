//! Module for building and managing snapshot checkpoints.
//!
//! Checkpoints aggregate multiple snapshots into a single file per month,
//! reducing the number of files to load during server startup.

use std::collections::HashMap;

use chrono::{Datelike, TimeZone, Utc};
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};

use crate::{DatabaseTables, PersistedSnapshot, PersistedSnapshotCheckpoint, YearMonth};

/// Index mapping file IDs to their database and table location.
/// Used for efficient file lookup during checkpoint updates.
pub type FileIndex = HashMap<ParquetFileId, (DbId, TableId)>;

/// Build an index of file IDs to their (db_id, table_id) location.
pub fn build_file_index(databases: &SerdeVecMap<DbId, DatabaseTables>) -> FileIndex {
    let mut index = HashMap::new();
    for (db_id, db_tables) in databases {
        for (table_id, files) in &db_tables.tables {
            for file in files {
                index.insert(file.id, (*db_id, *table_id));
            }
        }
    }
    index
}

/// Add files from a snapshot to the checkpoint, updating metrics and index.
pub fn add_snapshot_files(
    checkpoint: &mut PersistedSnapshotCheckpoint,
    file_index: &mut FileIndex,
    databases: SerdeVecMap<DbId, DatabaseTables>,
) {
    for (db_id, db_tables) in databases {
        for (table_id, files) in db_tables.tables {
            for file in files {
                file_index.insert(file.id, (db_id, table_id));
                checkpoint.add_file(db_id, table_id, file);
            }
        }
    }
}

/// Process removed files from a snapshot.
/// Files found in the index are removed from the checkpoint.
/// Files not found are added to pending_removed_files.
pub fn process_removed_files(
    checkpoint: &mut PersistedSnapshotCheckpoint,
    file_index: &mut FileIndex,
    removed_files: SerdeVecMap<DbId, DatabaseTables>,
) {
    let mut any_removed = false;

    for (db_id, db_tables) in removed_files {
        for (table_id, files) in db_tables.tables {
            for file in files {
                if let Some((existing_db, existing_table)) = file_index.remove(&file.id) {
                    checkpoint.remove_file(existing_db, existing_table, file.id);
                    any_removed = true;
                } else {
                    checkpoint.add_pending_removed(db_id, table_id, file);
                }
            }
        }
    }

    if any_removed {
        checkpoint.recalculate_time_range();
    }
}

/// Extract year-month from a unix timestamp in milliseconds
pub fn year_month_from_timestamp_ms(timestamp_ms: i64) -> YearMonth {
    let dt = Utc
        .timestamp_millis_opt(timestamp_ms)
        .single()
        .expect("UTC does not observe seasonal time changes and so should always return a single, unambiguous time");
    YearMonth::new_unchecked(dt.year() as u16, dt.month() as u8)
}

/// Group snapshots by year-month based on creation timestamp
pub fn group_snapshots_by_month(
    snapshots: Vec<(PersistedSnapshot, i64)>, // (snapshot, timestamp_ms)
) -> HashMap<YearMonth, Vec<PersistedSnapshot>> {
    let mut grouped: HashMap<YearMonth, Vec<PersistedSnapshot>> = HashMap::new();
    for (snapshot, timestamp_ms) in snapshots {
        let year_month = year_month_from_timestamp_ms(timestamp_ms);
        grouped.entry(year_month).or_default().push(snapshot);
    }
    grouped
}

#[cfg(test)]
mod tests;
