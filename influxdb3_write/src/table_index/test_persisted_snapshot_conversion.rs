use super::*;

use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{DbId, ParquetFileId, SerdeVecMap, TableId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use rstest::rstest;

use crate::{DatabaseTables, ParquetFile, PersistedSnapshot};

#[derive(Default)]
struct CreateTestParquetFileArgs {
    /// Optional file ID; if None, a new ID will be generated
    file_id: Option<ParquetFileId>,
    /// The path for the parquet file
    path: Option<String>,
    /// Size in bytes (default: 100)
    size_bytes: Option<u64>,
    /// Number of rows (default: 10,000)
    row_count: Option<u64>,
    /// Time offset in minutes for calculating timestamps
    time_offset_minutes: i64,
}

// Helper function to create a test ParquetFile
fn create_test_parquet_file(args: CreateTestParquetFileArgs) -> ParquetFile {
    static MINUTE_NS: i64 = 10_i64.pow(9) * 60 * 10;
    let time_offset = args.time_offset_minutes;
    ParquetFile {
        id: args.file_id.unwrap_or_default(),
        path: args.path.unwrap_or_else(|| "whatever".to_string()),
        size_bytes: args.size_bytes.unwrap_or(100),
        row_count: args.row_count.unwrap_or(10_000),
        chunk_time: time_offset * MINUTE_NS,
        min_time: time_offset * MINUTE_NS,
        max_time: (time_offset + 1) * MINUTE_NS,
    }
}

struct CreateTestDatabaseTablesArgs {
    /// Set of (database_id, table_id) tuples to create tables for
    table_ids: HashSet<(DbId, TableId)>,
    /// Number of parquet files to create per table
    files_per_table: usize,
    /// Base time offset in minutes for file timestamps
    base_time_offset_minutes: i64,
}

// Helper function to create a SerdeVecMap with database tables
fn create_database_tables(args: CreateTestDatabaseTablesArgs) -> SerdeVecMap<DbId, DatabaseTables> {
    let mut databases: SerdeVecMap<DbId, DatabaseTables> = SerdeVecMap::new();
    for (db_id, table_id) in &args.table_ids {
        let db_tables = databases.entry(*db_id).or_default();
        let parquet_files = db_tables.tables.entry(*table_id).or_default();
        for _ in 0..args.files_per_table {
            parquet_files.push(create_test_parquet_file(CreateTestParquetFileArgs {
                time_offset_minutes: args.base_time_offset_minutes,
                ..Default::default()
            }));
        }
    }
    databases
}

struct CreateTestPersistedSnapshotArgs {
    /// Snapshot sequence number
    sequence_number: u64,
    /// Node ID (default: "test_host")
    node_id: Option<String>,
    /// Database tables with files to add in this snapshot
    added_tables: Option<SerdeVecMap<DbId, DatabaseTables>>,
    /// Database tables with files to remove in this snapshot
    removed_tables: Option<SerdeVecMap<DbId, DatabaseTables>>,
}

impl Default for CreateTestPersistedSnapshotArgs {
    fn default() -> Self {
        Self {
            sequence_number: 1,
            node_id: None,
            added_tables: None,
            removed_tables: None,
        }
    }
}

// Helper function to create a PersistedSnapshot
fn create_persisted_snapshot(args: CreateTestPersistedSnapshotArgs) -> PersistedSnapshot {
    let databases = args.added_tables.unwrap_or_default();
    let removed_files = args.removed_tables.unwrap_or_default();

    // Calculate min/max times from files
    let mut min_time = i64::MAX;
    let mut max_time = 0;

    for (_, tables) in &databases {
        for (_, files) in &tables.tables {
            for file in files {
                min_time = min_time.min(file.min_time);
                max_time = max_time.max(file.max_time);
            }
        }
    }

    if min_time == i64::MAX {
        min_time = 0;
    }

    PersistedSnapshot {
        node_id: args.node_id.unwrap_or_else(|| "test_host".to_string()),
        next_file_id: ParquetFileId::from(args.sequence_number),
        snapshot_sequence_number: SnapshotSequenceNumber::new(args.sequence_number),
        wal_file_sequence_number: WalFileSequenceNumber::new(args.sequence_number),
        catalog_sequence_number: CatalogSequenceNumber::new(args.sequence_number),
        databases,
        removed_files,
        min_time,
        max_time,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    }
}

#[derive(Debug)]
struct ExpectedTableSnapshot {
    db_id: DbId,
    table_id: TableId,
    expected_file_count: usize,
    expected_removed_file_count: usize,
    expected_metadata: ExpectedMetadata,
}

#[derive(Debug)]
struct ExpectedMetadata {
    parquet_size_bytes: Option<i64>,
    row_count: Option<i64>,
    min_time_check: TimeCheck,
    max_time_check: TimeCheck,
}

#[derive(Debug)]
enum TimeCheck {
    #[allow(dead_code)]
    Exact(i64),
    MatchSnapshot,
    Max, // i64::MAX
    Zero,
}

struct TestCase {
    name: &'static str,
    snapshot: PersistedSnapshot,
    expected_tables: Vec<ExpectedTableSnapshot>,
    additional_validations: Option<fn(&PersistedSnapshot, &[TableIndexSnapshot])>,
}

impl std::fmt::Display for TestCase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestCase")
            .field("name", &self.name)
            .finish()
    }
}

#[rstest]
#[case::simple_single_table_conversion(TestCase {
  name: "simple_single_table_conversion",
  snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
      sequence_number: 1,
      added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (0.into(), 0.into()),
              (0.into(), 1.into()),
              (1.into(), 0.into()),
              (1.into(), 1.into()),
          ]),
          files_per_table: 3,
          base_time_offset_minutes: 1,
      })),
      ..Default::default()
  }),
  expected_tables: vec![
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 0.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 1.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 0.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 1.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
  ],
  additional_validations: None,
})]
#[case::multiple_databases_tables_conversion(TestCase {
  name: "multiple_databases_tables_conversion",
  snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
      sequence_number: 1,
      added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (0.into(), 0.into()),
              (0.into(), 1.into()),
              (1.into(), 0.into()),
              (1.into(), 1.into()),
          ]),
          files_per_table: 3,
          base_time_offset_minutes: 1,
      })),
      ..Default::default()
  }),
  expected_tables: vec![
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 0.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 1.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 0.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 1.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
  ],
  additional_validations: Some(|_snapshot, table_snapshots| {
      // Verify no cross-contamination between tables
      let mut seen_files = HashSet::new();
      for ts in table_snapshots {
          for file in &ts.files {
              assert!(
                  seen_files.insert(file.id),
                  "File {:?} appears in multiple tables",
                  file.id
              );
          }
      }
  }),
})]
#[case::removed_files_handling(TestCase {
  name: "removed_files_handling",
  snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
      sequence_number: 1,
      added_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (3.into(), 3.into()),
              (3.into(), 4.into()),
              (4.into(), 3.into()),
              (4.into(), 4.into()),
          ]),
          files_per_table: 3,
          base_time_offset_minutes: 1,
      })),
      removed_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (0.into(), 0.into()),
              (0.into(), 1.into()),
              (1.into(), 0.into()),
              (1.into(), 1.into()),
          ]),
          files_per_table: 3,
          base_time_offset_minutes: 1,
      })),
      ..Default::default()
  }),
  expected_tables: vec![
      // Tables with added files
      ExpectedTableSnapshot {
          db_id: 3.into(),
          table_id: 3.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 3.into(),
          table_id: 4.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 4.into(),
          table_id: 3.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      ExpectedTableSnapshot {
          db_id: 4.into(),
          table_id: 4.into(),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      // Tables with removed files only
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 0.into(),
          expected_file_count: 0,
          expected_removed_file_count: 3,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-300),
              row_count: Some(-30_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
      ExpectedTableSnapshot {
          db_id: 0.into(),
          table_id: 1.into(),
          expected_file_count: 0,
          expected_removed_file_count: 3,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-300),
              row_count: Some(-30_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 0.into(),
          expected_file_count: 0,
          expected_removed_file_count: 3,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-300),
              row_count: Some(-30_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
      ExpectedTableSnapshot {
          db_id: 1.into(),
          table_id: 1.into(),
          expected_file_count: 0,
          expected_removed_file_count: 3,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-300),
              row_count: Some(-30_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
  ],
  additional_validations: None,
})]
#[case::empty_snapshot_conversion(TestCase {
  name: "empty_snapshot_conversion",
  snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
      sequence_number: 1,
      ..Default::default()
  }),
  expected_tables: vec![],
  additional_validations: None,
})]
#[case::table_with_only_removed_files(TestCase {
  name: "table_with_only_removed_files",
  snapshot: create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
      sequence_number: 1,
      removed_tables: Some(create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (DbId::from(1), TableId::from(1)),
              (DbId::from(1), TableId::from(2)),
          ]),
          files_per_table: 2,
          base_time_offset_minutes: 1,
      })),
      ..Default::default()
  }),
  expected_tables: vec![
      ExpectedTableSnapshot {
          db_id: DbId::from(1),
          table_id: TableId::from(1),
          expected_file_count: 0,
          expected_removed_file_count: 2,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-200),
              row_count: Some(-20_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
      ExpectedTableSnapshot {
          db_id: DbId::from(1),
          table_id: TableId::from(2),
          expected_file_count: 0,
          expected_removed_file_count: 2,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-200),
              row_count: Some(-20_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
  ],
  additional_validations: None,
})]
#[case::overlapping_added_and_removed_files(TestCase {
  name: "overlapping_added_and_removed_files",
  snapshot: {
      // Create both added and removed files - note that they share (1,1) as an overlapping table
      let added = create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (DbId::from(1), TableId::from(1)), // overlapping
              (DbId::from(2), TableId::from(1)), // added only
          ]),
          files_per_table: 3,
          base_time_offset_minutes: 1,
      });
      let removed = create_database_tables(CreateTestDatabaseTablesArgs {
          table_ids: HashSet::from_iter(vec![
              (DbId::from(1), TableId::from(1)), // overlapping
              (DbId::from(1), TableId::from(2)), // removed only
          ]),
          files_per_table: 2,
          base_time_offset_minutes: 1,
      });
      create_persisted_snapshot(CreateTestPersistedSnapshotArgs {
          sequence_number: 1,
          added_tables: Some(added),
          removed_tables: Some(removed),
          ..Default::default()
      })
  },
  expected_tables: vec![
      // Overlapping table
      ExpectedTableSnapshot {
          db_id: DbId::from(1),
          table_id: TableId::from(1),
          expected_file_count: 3,
          expected_removed_file_count: 2,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(100), // 300 (added) - 200 (removed) = 100
              row_count: Some(10_000), // 30000 (added) - 20000 (removed) = 10000
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      // Added only table
      ExpectedTableSnapshot {
          db_id: DbId::from(2),
          table_id: TableId::from(1),
          expected_file_count: 3,
          expected_removed_file_count: 0,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(300),
              row_count: Some(30_000),
              min_time_check: TimeCheck::MatchSnapshot,
              max_time_check: TimeCheck::MatchSnapshot,
          },
      },
      // Removed only table
      ExpectedTableSnapshot {
          db_id: DbId::from(1),
          table_id: TableId::from(2),
          expected_file_count: 0,
          expected_removed_file_count: 2,
          expected_metadata: ExpectedMetadata {
              parquet_size_bytes: Some(-200),
              row_count: Some(-20_000),
              min_time_check: TimeCheck::Max,
              max_time_check: TimeCheck::Zero,
          },
      },
  ],
  additional_validations: Some(|_snapshot, table_snapshots| {
      // Verify all file IDs are unique within each table
      for ts in table_snapshots {
          let file_count = ts.files.len();
          let unique_ids: HashSet<_> = ts.files.iter().map(|f| f.id).collect();
          assert_eq!(
              unique_ids.len(),
              file_count,
              "All file IDs should be unique within table"
          );
      }
  }),
})]
fn validate(#[case] tc: TestCase) {
    println!("running {tc}");
    let snapshot = &tc.snapshot;

    // Handle empty snapshot case
    if tc.expected_tables.is_empty() {
        assert!(
            snapshot.databases.is_empty(),
            "Expected empty persisted snapshot files",
        );
        return;
    }

    // Convert to table index snapshots
    let table_snapshots: Vec<TableIndexSnapshot> = snapshot.clone().into();

    assert_eq!(
        table_snapshots.len(),
        tc.expected_tables.len(),
        "Expected {} table snapshots, got {}",
        tc.expected_tables.len(),
        table_snapshots.len()
    );

    // Verify each expected table
    for expected in &tc.expected_tables {
        println!("verifying {expected:?}");
        let table_snapshot = table_snapshots
            .iter()
            .find(|ts| ts.id.db_id() == expected.db_id && ts.id.table_id() == expected.table_id)
            .expect("Expected to find table");

        // Extract the ParquetFiles from the original PersistedSnapshot for this table
        let original_files = snapshot
            .databases
            .get(&expected.db_id)
            .and_then(|db| db.tables.get(&expected.table_id))
            .map(|files| files.iter().cloned().collect::<HashSet<_>>())
            .unwrap_or_default();

        // Verify that the files in the TableIndexSnapshot match exactly
        assert_eq!(
            table_snapshot.files, original_files,
            "Files don't match exactly for table {:?}/{:?}",
            expected.db_id, expected.table_id
        );

        // Extract removed file IDs from the original PersistedSnapshot
        let original_removed_ids = snapshot
            .removed_files
            .get(&expected.db_id)
            .and_then(|db| db.tables.get(&expected.table_id))
            .map(|files| files.iter().map(|f| f.id).collect::<HashSet<_>>())
            .unwrap_or_default();

        // Verify that the removed file IDs match exactly
        assert_eq!(
            table_snapshot.removed_files, original_removed_ids,
            "Removed file IDs don't match exactly for table {:?}/{:?}",
            expected.db_id, expected.table_id
        );

        // Verify common fields
        assert_eq!(
            table_snapshot.id.node_id(),
            snapshot.node_id,
            "node_id mismatch",
        );
        assert_eq!(
            table_snapshot.snapshot_sequence_number, snapshot.snapshot_sequence_number,
            "snapshot_sequence_number mismatch",
        );

        // Verify file counts
        assert_eq!(
            table_snapshot.files.len(),
            expected.expected_file_count,
            "File count mismatch for table {:?}/{:?}",
            expected.db_id,
            expected.table_id
        );
        assert_eq!(
            table_snapshot.removed_files.len(),
            expected.expected_removed_file_count,
            "Removed file count mismatch for table {:?}/{:?}",
            expected.db_id,
            expected.table_id
        );

        // Verify metadata
        if let Some(expected_bytes) = expected.expected_metadata.parquet_size_bytes {
            assert_eq!(
                table_snapshot.metadata.parquet_size_bytes, expected_bytes,
                "parquet_size_bytes mismatch for table {:?}/{:?}",
                expected.db_id, expected.table_id
            );
        }

        if let Some(expected_rows) = expected.expected_metadata.row_count {
            assert_eq!(
                table_snapshot.metadata.row_count, expected_rows,
                "row_count mismatch for table {:?}/{:?}",
                expected.db_id, expected.table_id
            );
        }

        // Verify times
        match expected.expected_metadata.min_time_check {
            TimeCheck::Exact(time) => {
                assert_eq!(table_snapshot.metadata.min_time, time, "min_time mismatch",)
            }
            TimeCheck::MatchSnapshot => assert!(
                table_snapshot.metadata.min_time >= snapshot.min_time,
                "min_time should be >= snapshot min_time",
            ),
            TimeCheck::Max => assert_eq!(
                table_snapshot.metadata.min_time,
                i64::MAX,
                "min_time should be i64::MAX",
            ),
            TimeCheck::Zero => {
                assert_eq!(table_snapshot.metadata.min_time, 0, "min_time should be 0",)
            }
        }

        match expected.expected_metadata.max_time_check {
            TimeCheck::Exact(time) => {
                assert_eq!(table_snapshot.metadata.max_time, time, "max_time mismatch",)
            }
            TimeCheck::MatchSnapshot => assert!(
                table_snapshot.metadata.max_time <= snapshot.max_time,
                "max_time should be <= snapshot max_time",
            ),
            TimeCheck::Max => assert_eq!(
                table_snapshot.metadata.max_time,
                i64::MAX,
                "max_time should be i64::MAX",
            ),
            TimeCheck::Zero => {
                assert_eq!(table_snapshot.metadata.max_time, 0, "max_time should be 0",)
            }
        }
    }

    // Run additional validations if provided
    if let Some(validation_fn) = tc.additional_validations {
        validation_fn(snapshot, &table_snapshots);
    }
}
