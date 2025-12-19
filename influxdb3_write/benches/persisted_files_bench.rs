//! Benchmarks for PersistedFiles.get_files performance.
//!
//! Tests retrieval performance across different file counts:
//! - File count: 10, 100, 1000

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
use influxdb3_write::{DatabaseTables, ParquetFile, PersistedSnapshot};

/// Range of file counts used in benchmarks.
const PARAMETER_RANGE: [usize; 3] = [10, 100, 1000];

/// Number of databases to distribute files across in benchmarks.
const NUM_DBS: usize = 3;

/// Number of tables per database to distribute files across.
const NUM_TABLES: usize = 4;

/// Create a ParquetFile with unique path and varying min_time.
///
/// # Parameters
/// - `idx`: Index used for unique path and time offset
/// - `prefix`: Prefix for unique path naming
fn create_parquet_file(idx: usize, prefix: &str) -> ParquetFile {
    let min_time = idx as i64 * 1000;
    ParquetFile {
        id: ParquetFileId::new(),
        path: format!("/db/table/{prefix}_{idx:08}.parquet"),
        size_bytes: 1024,
        row_count: 100,
        chunk_time: min_time,
        min_time,
        max_time: min_time + 1000,
    }
}

/// Create a PersistedSnapshot with files distributed across multiple dbs/tables.
///
/// Files are distributed using modulo arithmetic:
/// - db_id = file_index % NUM_DBS
/// - table_id = file_index % NUM_TABLES
fn create_snapshot_with_files(
    added_files: Vec<ParquetFile>,
    removed_files: Vec<ParquetFile>,
) -> PersistedSnapshot {
    let mut databases: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
    let mut removed: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();

    // Distribute added files across dbs/tables
    for (idx, file) in added_files.into_iter().enumerate() {
        let db_id = DbId::from((idx % NUM_DBS) as u32);
        let table_id = TableId::from((idx % NUM_TABLES) as u32);

        let db_tables = databases.entry(db_id).or_insert_with(|| DatabaseTables {
            tables: SerdeVecMap::new(),
        });
        db_tables
            .tables
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(file);
    }

    // Distribute removed files across dbs/tables
    for (idx, file) in removed_files.into_iter().enumerate() {
        let db_id = DbId::from((idx % NUM_DBS) as u32);
        let table_id = TableId::from((idx % NUM_TABLES) as u32);

        let db_tables = removed.entry(db_id).or_insert_with(|| DatabaseTables {
            tables: SerdeVecMap::new(),
        });
        db_tables
            .tables
            .entry(table_id)
            .or_insert_with(Vec::new)
            .push(file);
    }

    PersistedSnapshot {
        node_id: "bench-host".to_owned(),
        next_file_id: ParquetFileId::next_id(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        wal_file_sequence_number: influxdb3_wal::WalFileSequenceNumber::new(1),
        catalog_sequence_number: CatalogSequenceNumber::new(1),
        parquet_size_bytes: 0,
        row_count: 0,
        min_time: 0,
        max_time: 0,
        databases,
        removed_files: removed,
    }
}

/// Create a PersistedFiles instance populated with the specified number of files.
///
/// # Parameters
/// - `file_count`: Number of ParquetFile entries to create
fn create_persisted_files(file_count: usize) -> (PersistedFiles, DbId, TableId) {
    let db_id = DbId::from(0);
    let table_id = TableId::from(0);

    let parquet_files: Vec<ParquetFile> = (0..file_count)
        .map(|i| create_parquet_file(i, "file"))
        .collect();

    // Build the snapshot by directly populating public fields
    let mut tables = SerdeVecMap::new();
    tables.insert(table_id, parquet_files.clone());

    let mut databases = SerdeVecMap::new();
    databases.insert(db_id, DatabaseTables { tables });

    // Calculate aggregate stats for the snapshot
    let total_size: u64 = parquet_files.iter().map(|f| f.size_bytes).sum();
    let total_rows: u64 = parquet_files.iter().map(|f| f.row_count).sum();
    let min_time = parquet_files.iter().map(|f| f.min_time).min().unwrap_or(0);
    let max_time = parquet_files.iter().map(|f| f.max_time).max().unwrap_or(0);

    let snapshot = PersistedSnapshot {
        node_id: "bench-host".to_owned(),
        next_file_id: ParquetFileId::next_id(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        wal_file_sequence_number: influxdb3_wal::WalFileSequenceNumber::new(1),
        catalog_sequence_number: CatalogSequenceNumber::new(1),
        parquet_size_bytes: total_size,
        row_count: total_rows,
        min_time,
        max_time,
        databases,
        removed_files: SerdeVecMap::new(),
    };

    let persisted_files = PersistedFiles::new_from_persisted_snapshots(None, vec![snapshot]);

    (persisted_files, db_id, table_id)
}

/// Benchmark: get_files across varying file counts.
///
/// Measures retrieval performance as the number of stored files increases.
fn bench_get_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("persisted_files_get");
    group.sample_size(10);

    for file_count in PARAMETER_RANGE {
        let (persisted_files, db_id, table_id) = create_persisted_files(file_count);

        group.bench_with_input(
            BenchmarkId::new("file_count", file_count),
            &(persisted_files, db_id, table_id),
            |b, (pf, db_id, table_id)| {
                b.iter(|| black_box(pf.get_files(*db_id, *table_id)));
            },
        );
    }

    group.finish();
}

/// Benchmark: add_persisted_snapshot_files with new files.
///
/// Measures performance of adding files via snapshot.
fn bench_add_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("persisted_files_add");
    group.sample_size(10);

    for file_count in PARAMETER_RANGE {
        group.bench_with_input(
            BenchmarkId::new("file_count", file_count),
            &file_count,
            |b, &file_count| {
                b.iter_batched(
                    || {
                        // Setup: create empty PersistedFiles and a snapshot with files to add
                        let persisted_files =
                            PersistedFiles::new_from_persisted_snapshots(None, vec![]);
                        let parquet_files: Vec<ParquetFile> = (0..file_count)
                            .map(|i| create_parquet_file(i, "add"))
                            .collect();
                        let snapshot = create_snapshot_with_files(parquet_files, vec![]);
                        (persisted_files, snapshot)
                    },
                    |(pf, snapshot)| {
                        let _: () = pf.add_persisted_snapshot_files(snapshot);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: add_persisted_snapshot_files with existing db/table structure.
///
/// Unlike bench_add_files which starts empty, this benchmark pre-populates
/// the PersistedFiles with files so the db/table HashMaps already exist.
/// Files are distributed across NUM_DBS databases and NUM_TABLES tables.
fn bench_add_files_existing(c: &mut Criterion) {
    let mut group = c.benchmark_group("persisted_files_add_existing");
    group.sample_size(10);

    for file_count in PARAMETER_RANGE {
        group.bench_with_input(
            BenchmarkId::new("file_count", file_count),
            &file_count,
            |b, &file_count| {
                b.iter_batched(
                    || {
                        // Setup: create PersistedFiles with existing files distributed across dbs/tables
                        let existing_files: Vec<ParquetFile> = (0..file_count)
                            .map(|i| create_parquet_file(i, "existing"))
                            .collect();
                        let initial_snapshot = create_snapshot_with_files(existing_files, vec![]);
                        let persisted_files = PersistedFiles::new_from_persisted_snapshots(
                            None,
                            vec![initial_snapshot],
                        );

                        // New files to add (use offset to avoid overlap)
                        let new_files: Vec<ParquetFile> = (file_count..file_count * 2)
                            .map(|i| create_parquet_file(i, "new"))
                            .collect();
                        let snapshot = create_snapshot_with_files(new_files, vec![]);

                        (persisted_files, snapshot)
                    },
                    |(pf, snapshot)| {
                        let _: () = pf.add_persisted_snapshot_files(snapshot);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: add_persisted_snapshot_files with file removals.
///
/// Measures performance of removing files via snapshot.
fn bench_remove_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("persisted_files_remove");
    group.sample_size(10);

    for file_count in PARAMETER_RANGE {
        // Create files that will be both added initially and removed
        let files_to_remove: Vec<ParquetFile> = (0..file_count)
            .map(|i| create_parquet_file(i, "remove"))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("file_count", file_count),
            &files_to_remove,
            |b, files_to_remove| {
                b.iter_batched(
                    || {
                        // Setup: create PersistedFiles with existing files
                        let initial_snapshot =
                            create_snapshot_with_files(files_to_remove.clone(), vec![]);
                        let persisted_files = PersistedFiles::new_from_persisted_snapshots(
                            None,
                            vec![initial_snapshot],
                        );
                        // Create snapshot with removed_files populated (not databases)
                        let removal_snapshot =
                            create_snapshot_with_files(vec![], files_to_remove.clone());
                        (persisted_files, removal_snapshot)
                    },
                    |(pf, snapshot)| {
                        let _: () = pf.add_persisted_snapshot_files(snapshot);
                        black_box(());
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_get_files,
    bench_add_files,
    bench_add_files_existing,
    bench_remove_files
);
criterion_main!(benches);
