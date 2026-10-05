use super::*;
use crate::ParquetFile;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::ParquetFileId;
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use std::sync::Arc;

fn create_test_file(id: ParquetFileId, size_bytes: u64, row_count: u64) -> ParquetFile {
    ParquetFile {
        id,
        path: format!("test/{:?}.parquet", id).into(),
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
        node_id: Arc::from("test-node"),
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

/// Files with distinct time ranges starting at `start`, so a test can tell whether the
/// checkpoint's time range was recalculated after a removal.
fn create_test_files(n: i64, start: i64) -> Vec<ParquetFile> {
    (0..n)
        .map(|i| ParquetFile {
            min_time: start + i * 100,
            max_time: start + i * 100 + 50,
            ..create_test_file(ParquetFileId::new(), 1000, 10)
        })
        .collect()
}

fn sorted_ids(files: &[ParquetFile]) -> Vec<ParquetFileId> {
    let mut ids: Vec<ParquetFileId> = files.iter().map(|f| f.id).collect();
    ids.sort_unstable();
    ids
}

#[test]
fn test_process_removed_files_removes_many_across_tables() {
    let db_id = DbId::new(1);
    let table_a = TableId::new(1);
    let table_b = TableId::new(2);
    let files_a = create_test_files(6, 0);
    let files_b = create_test_files(2, 1_000);

    let mut databases = SerdeVecMap::new();
    let mut db_tables = DatabaseTables::default();
    db_tables.tables.insert(table_a, files_a.clone());
    db_tables.tables.insert(table_b, files_b.clone());
    databases.insert(db_id, db_tables);

    let year_month = YearMonth::new_unchecked(2025, 1);
    let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);
    let mut file_index = HashMap::new();
    add_snapshot_files(&mut checkpoint, &mut file_index, databases);
    assert_eq!(8_000, checkpoint.parquet_size_bytes);
    assert_eq!((0, 1_150), (checkpoint.min_time, checkpoint.max_time));

    // Remove non-contiguous files from table A, including the one holding the checkpoint's min
    // time, and table B's file holding the max time. The last entry is not in the checkpoint.
    let absent = create_test_file(ParquetFileId::new(), 1000, 10);
    let mut rm_db_tables = DatabaseTables::default();
    rm_db_tables.tables.insert(
        table_a,
        vec![files_a[0].clone(), files_a[2].clone(), files_a[5].clone()],
    );
    rm_db_tables
        .tables
        .insert(table_b, vec![files_b[1].clone(), absent.clone()]);
    let mut removed_files = SerdeVecMap::new();
    removed_files.insert(db_id, rm_db_tables);

    process_removed_files(&mut checkpoint, &mut file_index, removed_files);

    let tables = &checkpoint.databases[&db_id].tables;
    assert_eq!(
        sorted_ids(&[files_a[1].clone(), files_a[3].clone(), files_a[4].clone()]),
        sorted_ids(&tables[&table_a])
    );
    assert_eq!(sorted_ids(&files_b[..1]), sorted_ids(&tables[&table_b]));
    assert_eq!(4_000, checkpoint.parquet_size_bytes);
    assert_eq!(40, checkpoint.row_count);
    // Left: table A's files 1, 3 and 4 (100 to 450) and table B's file 0 (1_000 to 1_050).
    assert_eq!((100, 1_050), (checkpoint.min_time, checkpoint.max_time));

    for removed in [&files_a[0], &files_a[2], &files_a[5], &files_b[1]] {
        assert!(!file_index.contains_key(&removed.id));
    }
    for kept in [&files_a[1], &files_a[3], &files_a[4]] {
        assert_eq!(Some(&(db_id, table_a)), file_index.get(&kept.id));
    }
    assert_eq!(Some(&(db_id, table_b)), file_index.get(&files_b[0].id));

    let pending = &checkpoint.pending_removed_files[&db_id].tables[&table_b];
    assert_eq!(vec![absent.id], sorted_ids(pending));
}

#[test]
fn test_merge_applies_pending_removals_in_one_pass() {
    let db_id = DbId::new(1);
    let table_id = TableId::new(1);
    let missing_table = TableId::new(2);
    let jan_files = create_test_files(5, 0);

    let mut jan = PersistedSnapshotCheckpoint::new("test-node", YearMonth::new_unchecked(2025, 1));
    for file in &jan_files {
        jan.add_file(db_id, table_id, file.clone());
    }

    // February adds one file and removes January's files 0, 2 and 4. It also carries removals
    // for an id and a table that January does not have, which must change nothing.
    let feb_file = ParquetFile {
        min_time: 1_000,
        max_time: 1_050,
        ..create_test_file(ParquetFileId::new(), 1000, 10)
    };
    let mut feb = PersistedSnapshotCheckpoint::new("test-node", YearMonth::new_unchecked(2025, 2));
    feb.add_file(db_id, table_id, feb_file.clone());
    for i in [0, 2, 4] {
        feb.add_pending_removed(db_id, table_id, jan_files[i].clone());
    }
    feb.add_pending_removed(
        db_id,
        table_id,
        create_test_file(ParquetFileId::new(), 1000, 10),
    );
    feb.add_pending_removed(db_id, missing_table, jan_files[1].clone());

    jan.merge(feb);

    assert_eq!(
        sorted_ids(&[jan_files[1].clone(), jan_files[3].clone(), feb_file]),
        sorted_ids(&jan.databases[&db_id].tables[&table_id])
    );
    assert_eq!(3_000, jan.parquet_size_bytes);
    assert_eq!(30, jan.row_count);
    // January's file 0 held the min time; file 1 now does.
    assert_eq!((100, 1_050), (jan.min_time, jan.max_time));
    assert_eq!(YearMonth::new_unchecked(2025, 2), jan.year_month);
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
