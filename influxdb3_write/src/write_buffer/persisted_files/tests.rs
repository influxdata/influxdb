use crate::{ParquetFileId, YearMonth};
use datafusion::prelude::Expr;
use datafusion::prelude::col;
use datafusion::prelude::lit_timestamp_nano;
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_catalog::catalog::{ColumnDefinition, FieldFamilyMode, TableDefinition};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use observability_deps::tracing::info;
use pretty_assertions::assert_eq;
use std::sync::Arc;

use super::*;

#[test_log::test(test)]
fn test_get_metrics_after_initial_load() {
    let all_persisted_snapshot_files = build_persisted_snapshots();
    let persisted_file =
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(all_persisted_snapshot_files));

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
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(all_persisted_snapshot_files));
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
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(all_persisted_snapshot_files));
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
    let persisted_files =
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(persisted_snapshots));

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
            vec![ColumnDefinition::timestamp(0)],
            vec![],
            vec![],
            FieldFamilyMode::Aware,
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
    let mut new_snapshot = PersistedSnapshot::new("sample-host-id".to_owned(), snap1, wal1, cat1);
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
            path: format!("/random/path/{prefix}_{i}.parquet"),
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

    let persisted_files =
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(vec![snapshot]));

    // Delete the first database
    persisted_files
        .delete_database(DbId::from(0))
        .await
        .unwrap();

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

    let persisted_files =
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(vec![snapshot]));

    // Delete specific tables
    persisted_files
        .delete_table(db_id, table_id1)
        .await
        .unwrap();
    persisted_files
        .delete_table(db_id, table_id3)
        .await
        .unwrap();

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
    let persisted_files =
        PersistedFiles::new_from_persisted_snapshots(None, Arc::new(vec![snapshot]));

    // Delete a database
    persisted_files
        .delete_database(DbId::from(0))
        .await
        .unwrap();

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

fn build_checkpoint(
    year_month: &str,
    parquet_files: Vec<ParquetFile>,
    pending_removed: Vec<ParquetFile>,
) -> PersistedSnapshotCheckpoint {
    let db_id = DbId::from(0);
    let table_id = TableId::from(0);

    let year_month: YearMonth = year_month.parse().unwrap();
    let mut checkpoint = PersistedSnapshotCheckpoint::new("test-node".to_string(), year_month);

    for file in parquet_files {
        checkpoint.add_file(db_id, table_id, file);
    }

    for file in pending_removed {
        checkpoint.add_pending_removed(db_id, table_id, file);
    }

    checkpoint
}

#[test]
fn test_new_from_checkpoints_single_checkpoint() {
    let parquet_files = build_parquet_files("jan_", 5);
    let checkpoint = build_checkpoint("2025-01", parquet_files, vec![]);

    let persisted_files =
        PersistedFiles::new_from_checkpoints_and_snapshots(None, vec![checkpoint], vec![]);

    let (file_count, size_mb, row_count) = persisted_files.get_metrics();
    assert_eq!(5, file_count);
    assert_eq!(0.25, size_mb); // 5 files * 50_000 bytes
    assert_eq!(50, row_count); // 5 files * 10 rows
}

#[test]
fn test_new_from_checkpoints_multiple_months() {
    let jan_files = build_parquet_files("jan_", 3);
    let feb_files = build_parquet_files("feb_", 4);

    let jan_checkpoint = build_checkpoint("2025-01", jan_files, vec![]);
    let feb_checkpoint = build_checkpoint("2025-02", feb_files, vec![]);

    // Pass checkpoints in reverse order to test sorting
    let persisted_files = PersistedFiles::new_from_checkpoints_and_snapshots(
        None,
        vec![feb_checkpoint, jan_checkpoint],
        vec![],
    );

    let (file_count, size_mb, row_count) = persisted_files.get_metrics();
    assert_eq!(7, file_count);
    assert_eq!(0.35, size_mb); // 7 files * 50_000 bytes
    assert_eq!(70, row_count); // 7 files * 10 rows
}

#[test]
fn test_new_from_checkpoints_with_pending_removed() {
    // Create a file that exists in January
    let jan_file = ParquetFile {
        id: ParquetFileId::new(),
        path: "/jan/file.parquet".to_string(),
        size_bytes: 50_000,
        row_count: 10,
        chunk_time: 10,
        min_time: 10,
        max_time: 200,
    };

    let jan_checkpoint = build_checkpoint("2025-01", vec![jan_file.clone()], vec![]);

    // February has pending_removed referencing the January file
    let feb_files = build_parquet_files("feb_", 2);
    let feb_checkpoint = build_checkpoint("2025-02", feb_files, vec![jan_file]);

    let persisted_files = PersistedFiles::new_from_checkpoints_and_snapshots(
        None,
        vec![jan_checkpoint, feb_checkpoint],
        vec![],
    );

    let (file_count, size_mb, row_count) = persisted_files.get_metrics();
    // Jan had 1 file, Feb added 2, then Feb's pending_removed removed 1 from Jan
    assert_eq!(2, file_count);
    assert!((size_mb - 0.1).abs() < 0.0001); // 2 files * 50_000 bytes
    assert_eq!(20, row_count); // 2 files * 10 rows
}

#[test]
fn test_new_from_checkpoints_with_additional_snapshots() {
    let jan_files = build_parquet_files("jan_", 3);
    let jan_checkpoint = build_checkpoint("2025-01", jan_files, vec![]);

    // Additional snapshot with more files
    let snapshot_files = build_parquet_files("snapshot_", 2);
    let snapshot = build_snapshot(snapshot_files, 10, 10, 10);

    let persisted_files = PersistedFiles::new_from_checkpoints_and_snapshots(
        None,
        vec![jan_checkpoint],
        vec![snapshot],
    );

    let (file_count, size_mb, row_count) = persisted_files.get_metrics();
    assert_eq!(5, file_count); // 3 from checkpoint + 2 from snapshot
    assert_eq!(0.25, size_mb);
    assert_eq!(50, row_count);
}

#[test]
fn test_new_from_checkpoints_empty() {
    let persisted_files = PersistedFiles::new_from_checkpoints_and_snapshots(None, vec![], vec![]);

    let (file_count, size_mb, row_count) = persisted_files.get_metrics();
    assert_eq!(0, file_count);
    assert_eq!(0.0, size_mb);
    assert_eq!(0, row_count);
}

#[test]
fn test_new_from_checkpoints_pending_removed_missing_db() {
    // Create a pending_removed that references a non-existent database
    let missing_file = ParquetFile {
        id: ParquetFileId::new(),
        path: "/missing/file.parquet".to_string(),
        size_bytes: 50_000,
        row_count: 10,
        chunk_time: 10,
        min_time: 10,
        max_time: 200,
    };

    // Checkpoint has no files but has pending_removed
    let checkpoint = build_checkpoint("2025-01", vec![], vec![missing_file]);

    // Should not panic, just skip the missing reference
    let persisted_files =
        PersistedFiles::new_from_checkpoints_and_snapshots(None, vec![checkpoint], vec![]);

    let (file_count, _, _) = persisted_files.get_metrics();
    assert_eq!(0, file_count);
}
