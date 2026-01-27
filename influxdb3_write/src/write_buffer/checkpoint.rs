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
mod tests {
    use super::*;
    use crate::ParquetFile;
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::ParquetFileId;
    use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};

    fn create_test_file(id: ParquetFileId, size_bytes: u64, row_count: u64) -> ParquetFile {
        ParquetFile {
            id,
            path: format!("test/{:?}.parquet", id),
            size_bytes,
            row_count,
            chunk_time: 0,
            min_time: 100,
            max_time: 200,
        }
    }

    fn create_test_snapshot(
        seq: u64,
        databases: SerdeVecMap<DbId, DatabaseTables>,
        removed_files: SerdeVecMap<DbId, DatabaseTables>,
    ) -> PersistedSnapshot {
        PersistedSnapshot {
            node_id: "test-node".to_string(),
            next_file_id: ParquetFileId::new(),
            snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
            wal_file_sequence_number: WalFileSequenceNumber::new(seq),
            catalog_sequence_number: CatalogSequenceNumber::new(seq),
            parquet_size_bytes: 0,
            row_count: 0,
            min_time: i64::MAX,
            max_time: i64::MIN,
            databases,
            removed_files,
            persisted_at: None,
        }
    }

    #[test]
    fn test_add_snapshot_files_to_checkpoint() {
        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file_id = ParquetFileId::new();
        let file = create_test_file(file_id, 1024, 100);

        let mut databases = SerdeVecMap::new();
        let mut db_tables = DatabaseTables::default();
        db_tables.tables.insert(table_id, vec![file]);
        databases.insert(db_id, db_tables);

        let snapshot = create_test_snapshot(1, databases.clone(), SerdeVecMap::new());

        // Build checkpoint using helper functions directly
        let year_month = YearMonth::new_unchecked(2025, 1);
        let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);
        let mut file_index = build_file_index(&checkpoint.databases);

        checkpoint.update_from_snapshot(&snapshot);
        add_snapshot_files(&mut checkpoint, &mut file_index, databases);

        assert_eq!(checkpoint.year_month, year_month);
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 1);
        assert_eq!(checkpoint.parquet_size_bytes, 1024);
        assert_eq!(checkpoint.row_count, 100);
        assert!(checkpoint.databases.contains_key(&db_id));
        assert!(file_index.contains_key(&file_id));
    }

    #[test]
    fn test_add_files_to_existing_checkpoint() {
        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file_id1 = ParquetFileId::new();
        let file_id2 = ParquetFileId::new();

        // Create checkpoint with one file already in it
        let file1 = create_test_file(file_id1, 1024, 100);
        let year_month = YearMonth::new_unchecked(2025, 1);
        let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);
        checkpoint.add_file(db_id, table_id, file1);
        let mut file_index = build_file_index(&checkpoint.databases);

        // Create new snapshot with another file
        let file2 = create_test_file(file_id2, 2048, 200);
        let mut databases = SerdeVecMap::new();
        let mut db_tables = DatabaseTables::default();
        db_tables.tables.insert(table_id, vec![file2]);
        databases.insert(db_id, db_tables);

        let snapshot = create_test_snapshot(2, databases.clone(), SerdeVecMap::new());

        // Apply snapshot to checkpoint
        checkpoint.update_from_snapshot(&snapshot);
        add_snapshot_files(&mut checkpoint, &mut file_index, databases);

        // Should have both files' metrics
        assert_eq!(checkpoint.parquet_size_bytes, 1024 + 2048);
        assert_eq!(checkpoint.row_count, 100 + 200);
        assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 2);

        // Should have 2 files in the table
        let files = &checkpoint.databases[&db_id].tables[&table_id];
        assert_eq!(files.len(), 2);

        // File index should have both files
        assert!(file_index.contains_key(&file_id1));
        assert!(file_index.contains_key(&file_id2));
    }

    #[test]
    fn test_process_removed_files_removes_from_checkpoint() {
        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file_id = ParquetFileId::new();

        // First: add a file to checkpoint
        let file = create_test_file(file_id, 1024, 100);
        let mut databases = SerdeVecMap::new();
        let mut db_tables = DatabaseTables::default();
        db_tables.tables.insert(table_id, vec![file.clone()]);
        databases.insert(db_id, db_tables);
        let snapshot1 = create_test_snapshot(1, databases.clone(), SerdeVecMap::new());

        let year_month = YearMonth::new_unchecked(2025, 1);
        let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);
        let mut file_index = HashMap::new();

        checkpoint.update_from_snapshot(&snapshot1);
        add_snapshot_files(&mut checkpoint, &mut file_index, databases);

        // Verify file was added
        assert_eq!(checkpoint.parquet_size_bytes, 1024);
        assert!(file_index.contains_key(&file_id));

        // Second: process a removal for that file
        let mut removed_files = SerdeVecMap::new();
        let mut rm_db_tables = DatabaseTables::default();
        rm_db_tables.tables.insert(table_id, vec![file]);
        removed_files.insert(db_id, rm_db_tables);
        let snapshot2 = create_test_snapshot(2, SerdeVecMap::new(), removed_files.clone());

        checkpoint.update_from_snapshot(&snapshot2);
        process_removed_files(&mut checkpoint, &mut file_index, removed_files);

        // File should be removed, metrics adjusted
        assert_eq!(checkpoint.parquet_size_bytes, 0);
        assert_eq!(checkpoint.row_count, 0);
        assert!(checkpoint.pending_removed_files.is_empty());
        assert!(!file_index.contains_key(&file_id));
    }

    #[test]
    fn test_process_removed_files_adds_to_pending_when_not_found() {
        let db_id = DbId::new(1);
        let table_id = TableId::new(1);
        let file_id = ParquetFileId::new();

        // Create empty checkpoint (file doesn't exist in it)
        let year_month = YearMonth::new_unchecked(2025, 1);
        let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);
        let mut file_index = HashMap::new();

        // Process removal for a file that doesn't exist in checkpoint
        let file = create_test_file(file_id, 1024, 100);
        let mut removed_files = SerdeVecMap::new();
        let mut rm_db_tables = DatabaseTables::default();
        rm_db_tables.tables.insert(table_id, vec![file]);
        removed_files.insert(db_id, rm_db_tables);

        process_removed_files(&mut checkpoint, &mut file_index, removed_files);

        // File should be in pending_removed_files (not found in current checkpoint)
        assert!(!checkpoint.pending_removed_files.is_empty());
        assert!(checkpoint.pending_removed_files.contains_key(&db_id));
        let pending_files = &checkpoint.pending_removed_files[&db_id].tables[&table_id];
        assert_eq!(pending_files.len(), 1);
        assert_eq!(pending_files[0].id, file_id);
    }

    #[test]
    fn test_year_month_from_timestamp() {
        // January 15, 2025 12:00:00 UTC
        let ts = 1736942400000i64;
        assert_eq!(
            year_month_from_timestamp_ms(ts),
            YearMonth::new_unchecked(2025, 1)
        );

        // December 31, 2024 23:59:59 UTC
        let ts = 1735689599000i64;
        assert_eq!(
            year_month_from_timestamp_ms(ts),
            YearMonth::new_unchecked(2024, 12)
        );
    }

    #[test]
    fn test_group_snapshots_by_month() {
        let snapshot1 = create_test_snapshot(1, SerdeVecMap::new(), SerdeVecMap::new());
        let snapshot2 = create_test_snapshot(2, SerdeVecMap::new(), SerdeVecMap::new());
        let snapshot3 = create_test_snapshot(3, SerdeVecMap::new(), SerdeVecMap::new());

        // January 2025
        let ts_jan = 1736942400000i64;
        // February 2025
        let ts_feb = 1739620800000i64;

        let snapshots = vec![
            (snapshot1, ts_jan),
            (snapshot2, ts_jan),
            (snapshot3, ts_feb),
        ];

        let grouped = group_snapshots_by_month(snapshots);

        let jan_2025 = YearMonth::new_unchecked(2025, 1);
        let feb_2025 = YearMonth::new_unchecked(2025, 2);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[&jan_2025].len(), 2);
        assert_eq!(grouped[&feb_2025].len(), 1);
    }
}
