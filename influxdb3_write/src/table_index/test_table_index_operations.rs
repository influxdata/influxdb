use super::*;

use std::collections::BTreeSet;

use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_wal::SnapshotSequenceNumber;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use rstest::rstest;

use crate::{
    ParquetFile,
    paths::{TableIndexPath, TableIndexSnapshotPath},
};

#[derive(Debug, Clone)]
pub(crate) enum SnapshotVariant {
    /// Snapshot with only added files
    AddedFilesOnly { file_count: usize },
    /// Snapshot with both added and removed files
    Mixed {
        added_count: usize,
        removed_ids: Vec<ParquetFileId>,
    },
    /// Empty snapshot
    Empty,
}

pub(crate) struct TableIndexSnapshotBuilder {
    id: TableIndexId,
    sequence: SnapshotSequenceNumber,
    variant: SnapshotVariant,
    metadata_override: Option<IndexMetadata>,
    file_id_start: u64,
    // Store specific file IDs for controlled testing
    specific_file_ids: Option<Vec<ParquetFileId>>,
}

impl TableIndexSnapshotBuilder {
    pub(crate) fn new(id: TableIndexId) -> Self {
        Self {
            id,
            sequence: SnapshotSequenceNumber::new(1),
            variant: SnapshotVariant::Empty,
            metadata_override: None,
            file_id_start: 1,
            specific_file_ids: None,
        }
    }

    pub(crate) fn with_sequence(mut self, seq: SnapshotSequenceNumber) -> Self {
        self.sequence = seq;
        self
    }

    pub(crate) fn with_variant(mut self, variant: SnapshotVariant) -> Self {
        self.variant = variant;
        self
    }

    pub(crate) fn with_file_id_start(mut self, start: u64) -> Self {
        self.file_id_start = start;
        self
    }

    pub(crate) fn with_specific_file_ids(mut self, ids: Vec<ParquetFileId>) -> Self {
        self.specific_file_ids = Some(ids);
        self
    }

    pub(crate) fn build(self) -> TableIndexSnapshot {
        let mut files = HashSet::new();
        let mut removed_files = HashSet::new();
        let mut metadata = self.metadata_override.unwrap_or_else(IndexMetadata::empty);

        match self.variant {
            SnapshotVariant::AddedFilesOnly { file_count } => {
                for i in 0..file_count {
                    let file_id = if let Some(ref ids) = self.specific_file_ids {
                        ids.get(i).cloned().unwrap_or_else(ParquetFileId::new)
                    } else {
                        ParquetFileId::new()
                    };
                    let file = ParquetFile {
                        id: file_id,
                        path: format!("test_file_{i}"),
                        size_bytes: 100 * (i as u64 + 1),
                        row_count: 1000 * (i as u64 + 1),
                        chunk_time: 0,
                        min_time: 1000 + i as i64,
                        max_time: 2000 + i as i64,
                    };

                    // Update metadata if not overridden
                    if self.metadata_override.is_none() {
                        metadata.parquet_size_bytes += (100 * (i + 1)) as i64;
                        metadata.row_count += (1000 * (i + 1)) as i64;
                        metadata.min_time = metadata.min_time.min(1000 + i as i64);
                        metadata.max_time = metadata.max_time.max(2000 + i as i64);
                    }

                    files.insert(file);
                }
            }
            SnapshotVariant::Mixed {
                added_count,
                removed_ids,
            } => {
                // Add files
                for i in 0..added_count {
                    let file_id = if let Some(ref ids) = self.specific_file_ids {
                        ids.get(i).cloned().unwrap_or_else(ParquetFileId::new)
                    } else {
                        ParquetFileId::new()
                    };
                    let file = ParquetFile {
                        id: file_id,
                        path: format!("test_file_{i}"),
                        size_bytes: 100 * (i as u64 + 1),
                        row_count: 1000 * (i as u64 + 1),
                        chunk_time: 0,
                        min_time: 1000 + i as i64,
                        max_time: 2000 + i as i64,
                    };

                    if self.metadata_override.is_none() {
                        metadata.parquet_size_bytes += (100 * (i + 1)) as i64;
                        metadata.row_count += (1000 * (i + 1)) as i64;
                        metadata.min_time = metadata.min_time.min(1000 + i as i64);
                        metadata.max_time = metadata.max_time.max(2000 + i as i64);
                    }

                    files.insert(file);
                }
                // Add removed files
                removed_files.extend(removed_ids);
            }
            SnapshotVariant::Empty => {
                // No files, empty metadata
            }
        }

        TableIndexSnapshot {
            id: self.id.clone(),
            snapshot_sequence_number: self.sequence,
            files,
            removed_files,
            metadata,
        }
    }
}

// Common test constants
const NODE_1: &str = "node-1";
const NODE_2: &str = "node-2";

fn db_1() -> DbId {
    DbId::new(1)
}

fn table_index(
    node_id: &str,
    db_id: DbId,
    table_id: TableId,
    files: BTreeSet<ParquetFile>,
    latest_snapshot_sequence_number: SnapshotSequenceNumber,
    metadata: IndexMetadata,
) -> CoreTableIndex {
    CoreTableIndex {
        id: TableIndexId::new(node_id, db_id, table_id),
        files: files.into_iter().map(Arc::new).collect(),
        latest_snapshot_sequence_number,
        metadata,
    }
}
fn db_2() -> DbId {
    DbId::new(2)
}
fn table_1() -> TableId {
    TableId::new(1)
}
fn table_2() -> TableId {
    TableId::new(2)
}

struct LoadFromObjectStoreTestCase {
    node_id: String,
    db_id: DbId,
    table_id: TableId,
    // Initial index state (if any) to put in object store
    initial_index: Option<CoreTableIndex>,
    // Snapshots to put in object store
    snapshots: Vec<TableIndexSnapshot>,
    // Expected final CoreTableIndex state
    expected: CoreTableIndex,
}

#[rstest]
#[case::no_snapshots_or_index(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: None,
    snapshots: vec![],
    expected: table_index(NODE_1, db_1(), table_1(), BTreeSet::new(), SnapshotSequenceNumber::new(0), IndexMetadata::empty()),
})]
#[case::snapshots_only(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: None,
    snapshots: vec![
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(10))
            .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 2 })
            .with_specific_file_ids(vec![ParquetFileId::from(100), ParquetFileId::from(101)])
            .build(),
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(20))
            .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
            .with_specific_file_ids(vec![ParquetFileId::from(102)])
            .with_file_id_start(2)
            .build(),
    ],
    expected: table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            // Files from first snapshot (seq 10) - 2 files
            files.insert(ParquetFile {
                id: ParquetFileId::from(100),
                path: "test_file_0".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 1000,
                max_time: 2000,
            });
            files.insert(ParquetFile {
                id: ParquetFileId::from(101),
                path: "test_file_1".to_string(),
                size_bytes: 200,
                row_count: 2000,
                chunk_time: 0,
                min_time: 1001,
                max_time: 2001,
            });
            // File from second snapshot (seq 20) - 1 file
            files.insert(ParquetFile {
                id: ParquetFileId::from(102),
                path: "test_file_0".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 1000,
                max_time: 2000,
            });
            files
        }, SnapshotSequenceNumber::new(20), IndexMetadata {
            parquet_size_bytes: 400,
            row_count: 4000,
            min_time: 1000,
            max_time: 2001,
        }),
})]
#[case::index_only(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files
        }, SnapshotSequenceNumber::new(5), IndexMetadata {
            parquet_size_bytes: 500,
            row_count: 5000,
            min_time: 500,
            max_time: 1500,
        })),
    snapshots: vec![],
    expected: table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files
        }, SnapshotSequenceNumber::new(5), IndexMetadata {
            parquet_size_bytes: 500,
            row_count: 5000,
            min_time: 500,
            max_time: 1500,
        }),
})]
#[case::index_and_snapshots(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files
        }, SnapshotSequenceNumber::new(5), IndexMetadata {
            parquet_size_bytes: 500,
            row_count: 5000,
            min_time: 500,
            max_time: 1500,
        })),
    snapshots: vec![
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(15))
            .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
            .with_specific_file_ids(vec![ParquetFileId::from(201)])
            .build(),
    ],
    expected: table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files.insert(ParquetFile {
                id: ParquetFileId::from(201),
                path: "test_file_0".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 1000,
                max_time: 2000,
            });
            files
        }, SnapshotSequenceNumber::new(15), IndexMetadata {
            parquet_size_bytes: 600,
            row_count: 6000,
            min_time: 500,
            max_time: 2000,
        }),
})]
#[case::mix_of_snapshots_with_older_sequence_number(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files
        }, SnapshotSequenceNumber::new(20), IndexMetadata {
            parquet_size_bytes: 500,
            row_count: 5000,
            min_time: 500,
            max_time: 1500,
        })),
    snapshots: vec![
        // Older snapshot - should be ignored
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(10))
            .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
            .with_specific_file_ids(vec![ParquetFileId::from(300)])
            .build(),
        // Newer snapshot - should be applied
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(30))
            .with_variant(SnapshotVariant::AddedFilesOnly { file_count: 1 })
            .with_specific_file_ids(vec![ParquetFileId::from(301)])
            .with_file_id_start(1)
            .build(),
    ],
    expected: table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(200),
                path: "existing_file".to_string(),
                size_bytes: 500,
                row_count: 5000,
                chunk_time: 0,
                min_time: 500,
                max_time: 1500,
            });
            files.insert(ParquetFile {
                id: ParquetFileId::from(301),
                path: "test_file_0".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 1000,
                max_time: 2000,
            });
            files
        }, SnapshotSequenceNumber::new(30), IndexMetadata {
            parquet_size_bytes: 600,
            row_count: 6000,
            min_time: 500,
            max_time: 2000,
        }),
})]
#[case::snapshot_removes_file_from_initial_index(LoadFromObjectStoreTestCase {
    node_id: NODE_1.to_string(),
    db_id: db_1(),
    table_id: table_1(),
    initial_index: Some(table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(100),
                path: "file_to_keep".to_string(),
                size_bytes: 1000,
                row_count: 10000,
                chunk_time: 0,
                min_time: 100,
                max_time: 200,
            });
            files.insert(ParquetFile {
                id: ParquetFileId::from(101),
                path: "file_to_remove".to_string(),
                size_bytes: 2000,
                row_count: 20000,
                chunk_time: 0,
                min_time: 200,
                max_time: 300,
            });
            files
        }, SnapshotSequenceNumber::new(10), IndexMetadata {
            parquet_size_bytes: 3000,
            row_count: 30000,
            min_time: 100,
            max_time: 300,
        })),
    snapshots: vec![
        // Snapshot that removes file 101 and adds file 102
        TableIndexSnapshotBuilder::new(TableIndexId::new(NODE_1, db_1(), table_1()))
            .with_sequence(SnapshotSequenceNumber::new(20))
            .with_variant(SnapshotVariant::Mixed {
                added_count: 1,
                removed_ids: vec![ParquetFileId::from(101)],
            })
            .with_specific_file_ids(vec![ParquetFileId::from(102)])
            .build(),
    ],
    expected: table_index(NODE_1, db_1(), table_1(), {
            let mut files = BTreeSet::new();
            // File 100 remains
            files.insert(ParquetFile {
                id: ParquetFileId::from(100),
                path: "file_to_keep".to_string(),
                size_bytes: 1000,
                row_count: 10000,
                chunk_time: 0,
                min_time: 100,
                max_time: 200,
            });
            // File 101 was removed
            // File 102 was added
            files.insert(ParquetFile {
                id: ParquetFileId::from(102),
                path: "test_file_0".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 1000,
                max_time: 2000,
            });
            files
        }, SnapshotSequenceNumber::new(20), IndexMetadata {
            parquet_size_bytes: 1100,  // 1000 + 100
            row_count: 11000,           // 10000 + 1000
            min_time: 100,
            max_time: 2000,
        }),
})]
#[tokio::test]
async fn load_from_object_store(#[case] tc: LoadFromObjectStoreTestCase) {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Set up initial index if provided
    let path = TableIndexPath::new(&tc.node_id, tc.db_id.get(), tc.table_id.get());
    if let Some(initial) = tc.initial_index {
        let json = serde_json::to_vec_pretty(&initial).unwrap();
        object_store.put(path.as_ref(), json.into()).await.unwrap();
    }

    // Set up snapshots
    for snapshot in tc.snapshots {
        let path = TableIndexSnapshotPath::new(
            &tc.node_id,
            tc.db_id.get(),
            tc.table_id.get(),
            snapshot.snapshot_sequence_number,
        );
        let json = serde_json::to_vec_pretty(&snapshot).unwrap();
        object_store.put(path.as_ref(), json.into()).await.unwrap();
    }

    // Load from object store
    let loaded = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path)
        .await
        .expect("loading from object store should succeed");

    assert_eq!(loaded, tc.expected);
}

#[tokio::test]
async fn load_from_object_store_malformed_index() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let node_id = NODE_1;
    let db_id = db_1();
    let table_id = table_1();

    // Create a malformed index
    let path = TableIndexPath::new(node_id, db_id.get(), table_id.get());
    let malformed_json = b"{ this is not valid json }";
    object_store
        .put(path.as_ref(), malformed_json.to_vec().into())
        .await
        .unwrap();

    // Try to load from object store and expect an error
    let result = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path).await;

    assert!(result.is_err(), "Expected an error but got success");
    let err = result.unwrap_err();
    assert!(
        matches!(err, TableIndexError::DeserializeIndex(_)),
        "Error type mismatch. Got: {err:?}",
    );
}

#[tokio::test]
async fn load_from_object_store_malformed_snapshot() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let node_id = NODE_1;
    let db_id = db_1();
    let table_id = table_1();

    // Create a valid (empty) index first
    let index = table_index(
        node_id,
        db_id,
        table_id,
        BTreeSet::new(),
        SnapshotSequenceNumber::new(0),
        IndexMetadata::empty(),
    );
    let path = TableIndexPath::new(node_id, db_id.get(), table_id.get());
    let json = serde_json::to_vec_pretty(&index).unwrap();
    object_store.put(path.as_ref(), json.into()).await.unwrap();

    // Add a malformed snapshot
    let snapshot_path = TableIndexSnapshotPath::new(
        node_id,
        db_id.get(),
        table_id.get(),
        SnapshotSequenceNumber::new(10),
    );
    let malformed_json = b"{ this is not valid json }";
    object_store
        .put(snapshot_path.as_ref(), malformed_json.to_vec().into())
        .await
        .unwrap();

    // Try to load from object store and expect an error
    let result = CoreTableIndex::from_object_store(Arc::clone(&object_store), &path).await;

    assert!(result.is_err(), "Expected an error but got success");
    let err = result.unwrap_err();
    assert!(
        matches!(err, TableIndexError::DeserializeSnapshot(_)),
        "Error type mismatch. Got: {err:?}",
    );
}

#[rstest]
#[case::matching_identifiers(NODE_1, db_1(), table_1(), NODE_1, db_1(), table_1(), true)]
#[case::different_node(NODE_1, db_1(), table_1(), NODE_2, db_1(), table_1(), false)]
#[case::different_db(NODE_1, db_1(), table_1(), NODE_1, db_2(), table_1(), false)]
#[case::different_table(NODE_1, db_1(), table_1(), NODE_1, db_1(), table_2(), false)]
fn test_merge_validation(
    #[case] node1: &str,
    #[case] db1: DbId,
    #[case] table1: TableId,
    #[case] node2: &str,
    #[case] db2: DbId,
    #[case] table2: TableId,
    #[case] should_succeed: bool,
) {
    let mut index1 = table_index(
        node1,
        db1,
        table1,
        {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(1),
                path: "file1".to_string(),
                size_bytes: 100,
                row_count: 1000,
                chunk_time: 0,
                min_time: 100,
                max_time: 200,
            });
            files
        },
        SnapshotSequenceNumber::new(5),
        IndexMetadata {
            parquet_size_bytes: 100,
            row_count: 1000,
            min_time: 100,
            max_time: 200,
        },
    );

    let index2 = table_index(
        node2,
        db2,
        table2,
        {
            let mut files = BTreeSet::new();
            files.insert(ParquetFile {
                id: ParquetFileId::from(2),
                path: "file2".to_string(),
                size_bytes: 200,
                row_count: 2000,
                chunk_time: 0,
                min_time: 150,
                max_time: 250,
            });
            files
        },
        SnapshotSequenceNumber::new(10),
        IndexMetadata {
            parquet_size_bytes: 200,
            row_count: 2000,
            min_time: 150,
            max_time: 250,
        },
    );

    let result = index1.merge(index2);

    if should_succeed {
        assert!(
            result.is_ok(),
            "Merge should succeed for matching identifiers"
        );
        assert_eq!(index1.files.len(), 2, "Should have both files after merge");
        assert_eq!(
            index1.latest_snapshot_sequence_number,
            SnapshotSequenceNumber::new(10)
        );
        assert_eq!(index1.metadata.parquet_size_bytes, 300);
        assert_eq!(index1.metadata.row_count, 3000);
        assert_eq!(index1.metadata.min_time, 100);
        assert_eq!(index1.metadata.max_time, 250);
    } else {
        assert!(
            result.is_err(),
            "Merge should fail for mismatched identifiers"
        );
        match result.unwrap_err() {
            TableIndexError::MergeMismatch { expected, actual } => {
                assert_eq!(expected.node_id(), node1);
                assert_eq!(actual.node_id(), node2);
                assert_eq!(expected.db_id(), db1);
                assert_eq!(actual.db_id(), db2);
                assert_eq!(expected.table_id(), table1);
                assert_eq!(actual.table_id(), table2);
            }
            err => panic!("Expected MergeMismatch error, got: {err:?}"),
        }
    }
}
