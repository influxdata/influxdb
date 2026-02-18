use super::*;
use crate::{
    DatabaseTables, ParquetFile, ParquetFileId, PersistedSnapshot, PersistedSnapshotCheckpoint,
    PersistedSnapshotCheckpointVersion, PersistedSnapshotVersion,
};
use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_id::{DbId, SerdeVecMap, TableId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;
use pretty_assertions::assert_eq;
use rstest::rstest;
use {
    arrow::array::Int32Array, arrow::datatypes::DataType, arrow::datatypes::Field,
    arrow::datatypes::Schema, chrono::Utc,
    datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder,
    object_store::local::LocalFileSystem,
};

// =========================================================================
// Test Helpers
// =========================================================================

/// Create a test persister with InMemory object store and mock time provider
fn create_test_persister(
    time_nanos: i64,
    checkpoint_interval: Option<Duration>,
) -> (Persister, Arc<MockProvider>) {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(time_nanos)));
    let object_store = Arc::new(InMemory::new());
    let persister = Persister::new(
        object_store,
        "test_host",
        Arc::clone(&time_provider) as _,
        checkpoint_interval,
    );
    (persister, time_provider)
}

/// Create a test checkpoint with minimal data
fn create_test_checkpoint(year_month: YearMonth, seq: u64) -> PersistedSnapshotCheckpoint {
    PersistedSnapshotCheckpoint {
        node_id: "test_host".to_string(),
        year_month,
        last_snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
        next_file_id: Some(ParquetFileId::new()),
        wal_file_sequence_number: WalFileSequenceNumber::new(seq),
        catalog_sequence_number: CatalogSequenceNumber::new(seq),
        parquet_size_bytes: 0,
        row_count: 0,
        min_time: i64::MAX,
        max_time: i64::MIN,
        databases: SerdeVecMap::new(),
        pending_removed_files: SerdeVecMap::new(),
    }
}

/// Create a test snapshot with minimal data
fn create_test_snapshot(seq: u64) -> PersistedSnapshot {
    PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::new(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
        wal_file_sequence_number: WalFileSequenceNumber::new(seq),
        catalog_sequence_number: CatalogSequenceNumber::new(seq),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    }
}

// Nanoseconds for specific dates
const JAN_15_2025_NANOS: i64 = 1_736_899_200_000_000_000; // 2025-01-15 00:00:00 UTC
const FEB_01_2025_NANOS: i64 = 1_738_368_000_000_000_000; // 2025-02-01 00:00:00 UTC

#[tokio::test]
async fn persist_snapshot_info_file() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
    let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(0),
        snapshot_sequence_number: SnapshotSequenceNumber::new(0),
        wal_file_sequence_number: WalFileSequenceNumber::new(0),
        catalog_sequence_number: CatalogSequenceNumber::new(0),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    });

    persister.persist_snapshot(&info_file).await.unwrap();
}

#[tokio::test]
async fn persist_and_load_snapshot_info_files() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
    let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(0),
        snapshot_sequence_number: SnapshotSequenceNumber::new(0),
        wal_file_sequence_number: WalFileSequenceNumber::new(0),
        catalog_sequence_number: CatalogSequenceNumber::default(),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    });
    let info_file_2 = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(1),
        snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        wal_file_sequence_number: WalFileSequenceNumber::new(1),
        catalog_sequence_number: CatalogSequenceNumber::default(),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        max_time: 1,
        min_time: 0,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    });
    let info_file_3 = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(2),
        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
        wal_file_sequence_number: WalFileSequenceNumber::new(2),
        catalog_sequence_number: CatalogSequenceNumber::default(),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    });

    persister.persist_snapshot(&info_file).await.unwrap();
    persister.persist_snapshot(&info_file_2).await.unwrap();
    persister.persist_snapshot(&info_file_3).await.unwrap();

    let snapshots = persister.load_snapshots(2).await.unwrap();
    assert_eq!(snapshots.len(), 2);
    // The most recent files are first
    assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 2);
    assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 2);
    assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 2);
    assert_eq!(snapshots[1].v1_ref().next_file_id.as_u64(), 1);
    assert_eq!(snapshots[1].v1_ref().wal_file_sequence_number.as_u64(), 1);
    assert_eq!(snapshots[1].v1_ref().snapshot_sequence_number.as_u64(), 1);
}

#[tokio::test]
async fn persist_and_load_snapshot_info_files_with_fewer_than_requested() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
    let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::from(0),
        snapshot_sequence_number: SnapshotSequenceNumber::new(0),
        wal_file_sequence_number: WalFileSequenceNumber::new(0),
        catalog_sequence_number: CatalogSequenceNumber::default(),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: 1,
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    });
    persister.persist_snapshot(&info_file).await.unwrap();
    let snapshots = persister.load_snapshots(2).await.unwrap();
    // We asked for the most recent 2 but there should only be 1
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
}

#[tokio::test]
/// This test makes sure that the logic for offset lists works
async fn persist_and_load_over_1000_snapshot_info_files() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
    for id in 0..1001 {
        let info_file = PersistedSnapshotVersion::V1(PersistedSnapshot {
            node_id: "test_host".to_string(),
            next_file_id: ParquetFileId::from(id),
            snapshot_sequence_number: SnapshotSequenceNumber::new(id),
            wal_file_sequence_number: WalFileSequenceNumber::new(id),
            catalog_sequence_number: CatalogSequenceNumber::new(id),
            databases: SerdeVecMap::new(),
            removed_files: SerdeVecMap::new(),
            min_time: 0,
            max_time: 1,
            row_count: 0,
            parquet_size_bytes: 0,
            persisted_at: None,
        });
        persister.persist_snapshot(&info_file).await.unwrap();
    }
    let snapshots = persister.load_snapshots(1500).await.unwrap();
    // We asked for the most recent 1500 so there should be 1001 of them
    assert_eq!(snapshots.len(), 1001);
    assert_eq!(snapshots[0].v1_ref().next_file_id.as_u64(), 1000);
    assert_eq!(
        snapshots[0].v1_ref().wal_file_sequence_number.as_u64(),
        1000
    );
    assert_eq!(
        snapshots[0].v1_ref().snapshot_sequence_number.as_u64(),
        1000
    );
    assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 1000);
}

#[tokio::test]
// This test makes sure that the proper next_file_id is used if a parquet file
// is added
async fn persist_add_parquet_file_and_load_snapshot() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);
    let mut info_file = PersistedSnapshot::new(
        "test_host".to_string(),
        SnapshotSequenceNumber::new(0),
        WalFileSequenceNumber::new(0),
        CatalogSequenceNumber::new(0),
    );

    for _ in 0..=9875 {
        let _id = ParquetFileId::new();
    }

    info_file.add_parquet_file(
        DbId::from(0),
        TableId::from(0),
        crate::ParquetFile {
            // Use a number that will be bigger than what's created in the
            // PersistedSnapshot automatically
            id: ParquetFileId::new(),
            path: "test".into(),
            size_bytes: 5,
            row_count: 5,
            chunk_time: 5,
            min_time: 0,
            max_time: 1,
        },
    );
    persister
        .persist_snapshot(&PersistedSnapshotVersion::V1(info_file))
        .await
        .unwrap();
    let snapshots = persister.load_snapshots(10).await.unwrap();
    assert_eq!(snapshots.len(), 1);

    assert_eq!(snapshots[0].v1_ref().wal_file_sequence_number.as_u64(), 0);
    assert_eq!(snapshots[0].v1_ref().snapshot_sequence_number.as_u64(), 0);
    assert_eq!(snapshots[0].v1_ref().catalog_sequence_number.get(), 0);

    // Should be the next available id after the largest number
    // NOTE(wayne): it's not reasonable to assert on the exact value of a shared process-wide,
    // monotonically-increasing integer.
    assert!(
        snapshots[0].v1_ref().next_file_id.as_u64() >= 9877,
        "parquet file id must be bigger than or equal to"
    );
}

#[tokio::test]
async fn load_snapshot_works_with_no_exising_snapshots() {
    let store = InMemory::new();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(store), "test_host", time_provider, None);

    let snapshots = persister.load_snapshots(100).await.unwrap();
    assert!(snapshots.is_empty());
}

#[tokio::test]
async fn get_parquet_bytes() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

    let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
    let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

    stream_builder.tx().send(Ok(batch1)).await.unwrap();
    stream_builder.tx().send(Ok(batch2)).await.unwrap();

    let parquet = persister
        .serialize_to_parquet(stream_builder.build())
        .await
        .unwrap();

    // Assert we've written all the expected rows
    assert_eq!(parquet.meta_data.num_rows, 10);
}

#[tokio::test]
async fn persist_and_load_parquet_bytes() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider, None);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let stream_builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 5);

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

    let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
    let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(id_array)]).unwrap();

    stream_builder.tx().send(Ok(batch1)).await.unwrap();
    stream_builder.tx().send(Ok(batch2)).await.unwrap();

    let path = ParquetFilePath::new(
        "test_host",
        0,
        0,
        Utc::now().timestamp_nanos_opt().unwrap(),
        WalFileSequenceNumber::new(1),
    );
    let (bytes_written, meta, _) = persister
        .persist_parquet_file(path.clone(), stream_builder.build())
        .await
        .unwrap();

    // Assert we've written all the expected rows
    assert_eq!(meta.num_rows, 10);

    let bytes = persister.load_parquet_file(path).await.unwrap();

    // Assert that we have a file of bytes > 0
    assert!(!bytes.is_empty());
    assert_eq!(bytes.len() as u64, bytes_written);
}

// =========================================================================
// Checkpoint Tests
// =========================================================================

/// Test should_persist_checkpoint timing logic with various scenarios
#[rstest]
#[case::disabled_returns_false(None, false)]
#[case::enabled_no_previous_returns_true(Some(Duration::from_secs(3600)), true)]
fn test_should_persist_checkpoint_basic(
    #[case] checkpoint_interval: Option<Duration>,
    #[case] expected: bool,
) {
    let (persister, _time_provider) = create_test_persister(JAN_15_2025_NANOS, checkpoint_interval);
    assert_eq!(persister.should_persist_checkpoint(), expected);
}

#[tokio::test]
async fn test_should_persist_checkpoint_respects_interval() {
    // Create persister with 1-hour checkpoint interval
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

    // First call should return true (no previous checkpoint)
    assert!(persister.should_persist_checkpoint());

    // Mark checkpoint as created
    persister.mark_checkpoint_created();

    // Should now return false (not enough time elapsed)
    assert!(!persister.should_persist_checkpoint());

    // After the mark, some time passed but not enough
    assert!(persister.get_last_checkpoint_time().is_some());
}

/// Test checkpoint persist and load roundtrip
#[tokio::test]
async fn test_persist_and_load_checkpoint_roundtrip() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Create a checkpoint with some data
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let mut checkpoint = create_test_checkpoint(jan_2025, 5);
    checkpoint.parquet_size_bytes = 1024;
    checkpoint.row_count = 100;
    checkpoint.min_time = 1000;
    checkpoint.max_time = 2000;

    // Persist it
    persister
        .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint.clone()))
        .await
        .unwrap();

    // List checkpoints - should find one
    let paths = persister
        .list_latest_checkpoints_per_month(None)
        .await
        .unwrap();
    assert_eq!(paths.len(), 1);
    assert!(paths[0].as_ref().contains("2025-01"));

    // Load it back
    let loaded = persister.load_checkpoints(paths).await.unwrap();
    assert_eq!(loaded.len(), 1);

    let PersistedSnapshotCheckpointVersion::V1(loaded_checkpoint) = &loaded[0];

    // Verify fields match
    assert_eq!(loaded_checkpoint.node_id, checkpoint.node_id);
    assert_eq!(loaded_checkpoint.year_month, checkpoint.year_month);
    assert_eq!(
        loaded_checkpoint.last_snapshot_sequence_number,
        checkpoint.last_snapshot_sequence_number
    );
    assert_eq!(
        loaded_checkpoint.parquet_size_bytes,
        checkpoint.parquet_size_bytes
    );
    assert_eq!(loaded_checkpoint.row_count, checkpoint.row_count);
    assert_eq!(loaded_checkpoint.min_time, checkpoint.min_time);
    assert_eq!(loaded_checkpoint.max_time, checkpoint.max_time);
}

/// Test list_latest_checkpoints_per_month with various store states
#[rstest]
#[case::empty_store(vec![], 0)]
#[case::single_checkpoint(vec![("2025-01", 1)], 1)]
#[case::multiple_months(vec![("2025-01", 1), ("2025-02", 1)], 2)]
#[case::multiple_per_month_picks_latest(vec![("2025-01", 1), ("2025-01", 2), ("2025-01", 3)], 1)]
#[case::mixed_months_and_sequences(vec![("2025-01", 1), ("2025-01", 5), ("2025-02", 2), ("2025-02", 3)], 2)]
#[tokio::test]
async fn test_list_latest_checkpoints_per_month(
    #[case] checkpoints_to_create: Vec<(&str, u64)>,
    #[case] expected_count: usize,
) {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Persist checkpoints in the given order
    for (year_month_str, seq) in &checkpoints_to_create {
        let year_month: YearMonth = year_month_str.parse().unwrap();
        let checkpoint = create_test_checkpoint(year_month, *seq);
        persister
            .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
            .await
            .unwrap();
    }

    // List and verify
    let paths = persister
        .list_latest_checkpoints_per_month(None)
        .await
        .unwrap();
    assert_eq!(paths.len(), expected_count);

    // If we have multiple per month, verify we got the latest sequence numbers
    if !checkpoints_to_create.is_empty() && expected_count > 0 {
        // Load the checkpoints to verify sequence numbers
        let loaded = persister.load_checkpoints(paths).await.unwrap();

        // Group expected by month and find max seq per month
        let mut expected_by_month: HashMap<YearMonth, u64> = HashMap::new();
        for (month_str, seq) in &checkpoints_to_create {
            let month: YearMonth = month_str.parse().unwrap();
            expected_by_month
                .entry(month)
                .and_modify(|e| *e = (*e).max(*seq))
                .or_insert(*seq);
        }

        // Verify each loaded checkpoint has the expected (max) sequence
        for cp in loaded {
            let PersistedSnapshotCheckpointVersion::V1(cp) = cp;
            let expected_seq = expected_by_month.get(&cp.year_month).unwrap();
            assert_eq!(
                cp.last_snapshot_sequence_number.as_u64(),
                *expected_seq,
                "Expected seq {} for month {}, got {}",
                expected_seq,
                cp.year_month,
                cp.last_snapshot_sequence_number.as_u64()
            );
        }
    }
}

/// Test warm_checkpoint_cache bootstraps the cache from loaded checkpoint
#[tokio::test]
async fn test_warm_checkpoint_cache() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

    // Simulate loading a checkpoint during startup
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let loaded_checkpoint = create_test_checkpoint(jan_2025, 5);
    persister.warm_checkpoint_cache(loaded_checkpoint.clone());

    // Cache should be populated
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.year_month, jan_2025);
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 5);

    // Calling warm again should NOT overwrite (cache already populated)
    let another_checkpoint = create_test_checkpoint(jan_2025, 10);
    persister.warm_checkpoint_cache(another_checkpoint);

    // Should still have original checkpoint
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 5);
}

/// Test that persist_snapshot incrementally updates the cached checkpoint
#[tokio::test]
async fn test_incremental_checkpoint_update_on_persist() {
    // Enable checkpointing with 1 hour interval
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

    // Initially cache is empty
    assert!(persister.get_cached_checkpoint().is_none());

    // Persist first snapshot - should create checkpoint in cache
    let snapshot1 = PersistedSnapshotVersion::V1(create_test_snapshot(1));
    persister.persist_snapshot(&snapshot1).await.unwrap();

    // Cache should now have checkpoint with seq=1
    let cached = persister.get_cached_checkpoint().unwrap();
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    assert_eq!(cached.year_month, jan_2025);
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 1);

    // Persist second snapshot - should update cache incrementally
    let snapshot2 = PersistedSnapshotVersion::V1(create_test_snapshot(2));
    persister.persist_snapshot(&snapshot2).await.unwrap();

    // Cache should now have seq=2
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 2);

    // Persist third snapshot
    let snapshot3 = PersistedSnapshotVersion::V1(create_test_snapshot(3));
    persister.persist_snapshot(&snapshot3).await.unwrap();

    // Cache should now have seq=3
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 3);
}

/// Test that month rollover creates a new checkpoint (incremental path)
#[tokio::test]
async fn test_incremental_checkpoint_month_rollover() {
    let (persister, time_provider) =
        create_test_persister(JAN_15_2025_NANOS, Some(Duration::from_secs(3600)));

    // Persist January snapshot
    let snapshot1 = PersistedSnapshotVersion::V1(create_test_snapshot(1));
    persister.persist_snapshot(&snapshot1).await.unwrap();

    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.year_month, jan_2025);
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 1);

    // Advance to February
    time_provider.set(Time::from_timestamp_nanos(FEB_01_2025_NANOS));

    // Persist February snapshot - should create NEW checkpoint, not update January's
    let snapshot2 = PersistedSnapshotVersion::V1(create_test_snapshot(2));
    persister.persist_snapshot(&snapshot2).await.unwrap();

    let feb_2025 = YearMonth::new_unchecked(2025, 2);
    let cached = persister.get_cached_checkpoint().unwrap();
    assert_eq!(cached.year_month, feb_2025); // New month checkpoint
    assert_eq!(cached.last_snapshot_sequence_number.as_u64(), 2);
}

/// Test that checkpointing disabled means no cache updates
#[tokio::test]
async fn test_checkpoint_disabled_no_cache_updates() {
    // Checkpointing disabled (None interval)
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Persist a snapshot
    let snapshot = PersistedSnapshotVersion::V1(create_test_snapshot(1));
    persister.persist_snapshot(&snapshot).await.unwrap();

    // Cache should still be empty (checkpointing disabled)
    assert!(persister.get_cached_checkpoint().is_none());
}

// =========================================================================
// Tests for build_checkpoints_from_snapshots
// =========================================================================

/// Create a test snapshot with a specific max_time for month grouping tests
fn create_snapshot_with_max_time(seq: u64, max_time_ms: i64) -> PersistedSnapshot {
    PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::new(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
        wal_file_sequence_number: WalFileSequenceNumber::new(seq),
        catalog_sequence_number: CatalogSequenceNumber::new(seq),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: 0,
        max_time: max_time_ms,
        row_count: 100,
        parquet_size_bytes: 1024,
        persisted_at: None,
    }
}

/// Create a snapshot with file data for testing aggregation
fn create_snapshot_with_files(
    seq: u64,
    max_time_ms: i64,
    db_id: DbId,
    table_id: TableId,
    files: Vec<ParquetFile>,
) -> PersistedSnapshot {
    let size_bytes: u64 = files.iter().map(|f| f.size_bytes).sum();
    let row_count: u64 = files.iter().map(|f| f.row_count).sum();
    let min_time = files.iter().map(|f| f.min_time).min().unwrap_or(i64::MAX);
    let max_time = files.iter().map(|f| f.max_time).max().unwrap_or(i64::MIN);

    let mut databases = SerdeVecMap::new();
    let mut db_tables = DatabaseTables::default();
    db_tables.tables.insert(table_id, files);
    databases.insert(db_id, db_tables);

    PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::new(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(seq),
        wal_file_sequence_number: WalFileSequenceNumber::new(seq),
        catalog_sequence_number: CatalogSequenceNumber::new(seq),
        databases,
        removed_files: SerdeVecMap::new(),
        min_time,
        max_time: max_time_ms.max(max_time), // Use provided max_time_ms for month grouping
        row_count,
        parquet_size_bytes: size_bytes,
        persisted_at: None,
    }
}

fn create_test_parquet_file(size_bytes: u64, row_count: u64) -> ParquetFile {
    ParquetFile {
        id: ParquetFileId::new(),
        path: "test/file.parquet".to_string(),
        size_bytes,
        row_count,
        chunk_time: 0,
        min_time: 100,
        max_time: 200,
    }
}

// Milliseconds for specific dates (for max_time in snapshots)
const JAN_15_2025_MS: i64 = 1_736_899_200_000; // 2025-01-15 00:00:00 UTC
const JAN_20_2025_MS: i64 = 1_737_331_200_000; // 2025-01-20 00:00:00 UTC
const FEB_01_2025_MS: i64 = 1_738_368_000_000; // 2025-02-01 00:00:00 UTC
const FEB_15_2025_MS: i64 = 1_739_577_600_000; // 2025-02-15 00:00:00 UTC
const MAR_01_2025_MS: i64 = 1_740_787_200_000; // 2025-03-01 00:00:00 UTC

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_empty() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&[], jan_2025)
        .await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_single_snapshot() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    let db_id = DbId::new(1);
    let table_id = TableId::new(1);
    let file = create_test_parquet_file(1024, 100);
    let snapshot = create_snapshot_with_files(1, JAN_15_2025_MS, db_id, table_id, vec![file]);

    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&[snapshot], jan_2025)
        .await;

    // Should return the January checkpoint (current month)
    let checkpoint = result.expect("should return current month checkpoint");
    assert_eq!(checkpoint.year_month, jan_2025);
    assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 1);
    assert_eq!(checkpoint.row_count, 100);
    assert_eq!(checkpoint.parquet_size_bytes, 1024);
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_multiple_same_month() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    let db_id = DbId::new(1);
    let table_id = TableId::new(1);

    // Two snapshots in January (newest-first order as returned by load_snapshots)
    let snapshots = vec![
        create_snapshot_with_files(
            2,
            JAN_20_2025_MS,
            db_id,
            table_id,
            vec![create_test_parquet_file(1024, 100)],
        ),
        create_snapshot_with_files(
            1,
            JAN_15_2025_MS,
            db_id,
            table_id,
            vec![create_test_parquet_file(1024, 100)],
        ),
    ];

    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
        .await;

    let checkpoint = result.expect("should return current month checkpoint");
    assert_eq!(checkpoint.year_month, jan_2025);
    // Should have the newest snapshot's sequence number
    assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 2);
    // Metrics should be aggregated from both snapshots
    assert_eq!(checkpoint.row_count, 200); // 100 + 100
    assert_eq!(checkpoint.parquet_size_bytes, 2048); // 1024 + 1024
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_multiple_months_returns_current() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    let db_id = DbId::new(1);
    let table_id = TableId::new(1);

    // Helper to create snapshot with a file
    let make_snapshot = |seq, max_time_ms| {
        create_snapshot_with_files(
            seq,
            max_time_ms,
            db_id,
            table_id,
            vec![create_test_parquet_file(1024, 100)],
        )
    };

    // Snapshots across 3 months (newest-first order)
    let snapshots = vec![
        make_snapshot(5, MAR_01_2025_MS),
        make_snapshot(4, FEB_15_2025_MS),
        make_snapshot(3, FEB_01_2025_MS),
        make_snapshot(2, JAN_20_2025_MS),
        make_snapshot(1, JAN_15_2025_MS),
    ];

    // Current month is February - should return February's checkpoint
    let feb_2025 = YearMonth::new_unchecked(2025, 2);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&snapshots, feb_2025)
        .await;

    let checkpoint = result.expect("should return current month checkpoint");
    assert_eq!(checkpoint.year_month, feb_2025);
    // February has 2 snapshots (seq 3 and 4)
    assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 4);
    assert_eq!(checkpoint.row_count, 200);

    // Verify all 3 months' checkpoints were persisted to object store
    let loaded_paths = persister
        .list_latest_checkpoints_per_month(None)
        .await
        .unwrap();
    assert_eq!(
        loaded_paths.len(),
        3,
        "should persist checkpoints for all 3 months"
    );
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_current_month_not_in_snapshots() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    let db_id = DbId::new(1);
    let table_id = TableId::new(1);

    // Only January snapshots
    let snapshots = vec![create_snapshot_with_files(
        1,
        JAN_15_2025_MS,
        db_id,
        table_id,
        vec![create_test_parquet_file(1024, 100)],
    )];

    // Current month is March - not in snapshots
    let mar_2025 = YearMonth::new_unchecked(2025, 3);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&snapshots, mar_2025)
        .await;

    // Should return None since current month isn't in snapshots
    assert!(result.is_none());

    // But January's checkpoint should still be persisted
    let loaded_paths = persister
        .list_latest_checkpoints_per_month(None)
        .await
        .unwrap();
    assert_eq!(loaded_paths.len(), 1);
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_skips_empty_snapshots() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Include a snapshot with max_time = i64::MIN (empty, no data)
    let empty_snapshot = PersistedSnapshot {
        node_id: "test_host".to_string(),
        next_file_id: ParquetFileId::new(),
        snapshot_sequence_number: SnapshotSequenceNumber::new(2),
        wal_file_sequence_number: WalFileSequenceNumber::new(2),
        catalog_sequence_number: CatalogSequenceNumber::new(2),
        databases: SerdeVecMap::new(),
        removed_files: SerdeVecMap::new(),
        min_time: i64::MAX,
        max_time: i64::MIN, // Empty snapshot indicator
        row_count: 0,
        parquet_size_bytes: 0,
        persisted_at: None,
    };

    let snapshots = vec![
        empty_snapshot,
        create_snapshot_with_max_time(1, JAN_15_2025_MS),
    ];

    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
        .await;

    // Should return checkpoint from non-empty snapshot only
    let checkpoint = result.expect("should return checkpoint");
    assert_eq!(checkpoint.last_snapshot_sequence_number.as_u64(), 1);
}

#[tokio::test]
async fn test_build_checkpoints_from_snapshots_with_files() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    let db_id = DbId::new(1);
    let table_id = TableId::new(1);
    let file1 = create_test_parquet_file(1024, 100);
    let file2 = create_test_parquet_file(2048, 200);

    let snapshot = create_snapshot_with_files(1, JAN_15_2025_MS, db_id, table_id, vec![file1]);
    let snapshot2 = create_snapshot_with_files(2, JAN_20_2025_MS, db_id, table_id, vec![file2]);

    // Newest first
    let snapshots = vec![snapshot2, snapshot];

    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let result = persister
        .build_and_persist_checkpoints_from_snapshots(&snapshots, jan_2025)
        .await;

    let checkpoint = result.expect("should return checkpoint");

    // Check that files are present in checkpoint
    assert!(checkpoint.databases.contains_key(&db_id));
    let tables = &checkpoint.databases[&db_id].tables;
    assert!(tables.contains_key(&table_id));
    assert_eq!(tables[&table_id].len(), 2); // Both files
}

// =========================================================================
// Checkpoint Cleanup Tests
// =========================================================================

/// Helper to count checkpoints for a specific month
async fn count_checkpoints_for_month(persister: &Persister, year_month: &YearMonth) -> usize {
    let month_dir =
        SnapshotCheckpointPath::month_dir(&persister.node_identifier_prefix, year_month);
    let list: Vec<_> = persister
        .object_store
        .list(Some(&month_dir))
        .try_collect()
        .await
        .unwrap_or_default();
    list.len()
}

/// Helper to get checkpoint sequence numbers for a month (sorted newest first)
async fn get_checkpoint_sequences_for_month(
    persister: &Persister,
    year_month: &YearMonth,
) -> Vec<u64> {
    let month_dir =
        SnapshotCheckpointPath::month_dir(&persister.node_identifier_prefix, year_month);
    let mut list: Vec<_> = persister
        .object_store
        .list(Some(&month_dir))
        .try_collect()
        .await
        .unwrap_or_default();
    list.sort_by(|a, b| a.location.cmp(&b.location));

    list.iter()
        .filter_map(|meta| {
            SnapshotCheckpointPath::parse_sequence_number(meta.location.as_ref())
                .map(|s| s.as_u64())
        })
        .collect()
}

/// Test that cleanup keeps exactly 2 checkpoints when more than 2 exist
#[tokio::test]
async fn test_checkpoint_cleanup_keeps_two_most_recent() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);

    // Create 4 checkpoints for the same month with increasing sequence numbers
    for seq in [1, 2, 3, 4] {
        let checkpoint = create_test_checkpoint(jan_2025, seq);
        // Use put directly to avoid triggering cleanup (to set up test state)
        let path =
            SnapshotCheckpointPath::new("test_host", &jan_2025, SnapshotSequenceNumber::new(seq));
        let json =
            serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint)).unwrap();
        persister
            .object_store
            .put(path.as_ref(), json.into())
            .await
            .unwrap();
    }

    // Verify we have 4 checkpoints before cleanup
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 4);

    // Trigger cleanup
    Persister::cleanup_old_checkpoints_for_month(
        persister.node_identifier_prefix.clone(),
        Arc::clone(&persister.object_store),
        jan_2025,
    )
    .await;

    // Should have exactly 2 checkpoints remaining
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

    // Verify the 2 most recent (seq 4 and seq 3) are retained
    let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;
    assert_eq!(remaining_seqs, vec![4, 3]);
}

/// Test that no deletion occurs when 2 or fewer checkpoints exist
#[tokio::test]
async fn test_checkpoint_cleanup_no_deletion_when_two_or_fewer() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);

    // Create exactly 2 checkpoints
    for seq in [1, 2] {
        let checkpoint = create_test_checkpoint(jan_2025, seq);
        let path =
            SnapshotCheckpointPath::new("test_host", &jan_2025, SnapshotSequenceNumber::new(seq));
        let json =
            serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint)).unwrap();
        persister
            .object_store
            .put(path.as_ref(), json.into())
            .await
            .unwrap();
    }

    // Verify we have 2 checkpoints before cleanup
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

    // Trigger cleanup - should not delete anything
    Persister::cleanup_old_checkpoints_for_month(
        persister.node_identifier_prefix.clone(),
        Arc::clone(&persister.object_store),
        jan_2025,
    )
    .await;

    // Should still have 2 checkpoints
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

    // Both should still be there
    let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;
    assert_eq!(remaining_seqs, vec![2, 1]);
}

/// Test that cleanup handles empty month gracefully
#[tokio::test]
async fn test_checkpoint_cleanup_handles_empty_month() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);

    // No checkpoints exist - cleanup should not error
    Persister::cleanup_old_checkpoints_for_month(
        persister.node_identifier_prefix.clone(),
        Arc::clone(&persister.object_store),
        jan_2025,
    )
    .await;

    // Should still be 0
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 0);
}

/// Test that persist_checkpoint triggers cleanup automatically
#[tokio::test]
async fn test_persist_checkpoint_triggers_cleanup() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);

    // First, create 2 checkpoints directly (without cleanup)
    for seq in [1, 2] {
        let checkpoint = create_test_checkpoint(jan_2025, seq);
        let path =
            SnapshotCheckpointPath::new("test_host", &jan_2025, SnapshotSequenceNumber::new(seq));
        let json =
            serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint)).unwrap();
        persister
            .object_store
            .put(path.as_ref(), json.into())
            .await
            .unwrap();
    }

    // Verify we have 2 checkpoints
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);

    // Now persist a 3rd checkpoint via the normal method (which triggers cleanup)
    let checkpoint = create_test_checkpoint(jan_2025, 3);
    persister
        .persist_checkpoint(&PersistedSnapshotCheckpointVersion::V1(checkpoint))
        .await
        .unwrap();

    // Cleanup runs asynchronously, so we need to wait for it to complete.
    // Retry with timeout: 100 iterations * 50ms = 5 seconds max.
    let expected_seqs = vec![3, 2];
    let mut attempts = 0;
    loop {
        let count = count_checkpoints_for_month(&persister, &jan_2025).await;
        let remaining_seqs = get_checkpoint_sequences_for_month(&persister, &jan_2025).await;

        if count == 2 && remaining_seqs == expected_seqs {
            break;
        }

        attempts += 1;
        if attempts >= 100 {
            panic!(
                "Cleanup did not complete within 5 seconds. \
                 Expected 2 checkpoints with sequences {:?}, \
                 but found {} checkpoints with sequences {:?}",
                expected_seqs, count, remaining_seqs
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Test that cleanup only affects the specified month
#[tokio::test]
async fn test_checkpoint_cleanup_only_affects_specified_month() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);
    let jan_2025 = YearMonth::new_unchecked(2025, 1);
    let feb_2025 = YearMonth::new_unchecked(2025, 2);

    // Create 4 checkpoints in January
    for seq in [1, 2, 3, 4] {
        let checkpoint = create_test_checkpoint(jan_2025, seq);
        let path =
            SnapshotCheckpointPath::new("test_host", &jan_2025, SnapshotSequenceNumber::new(seq));
        let json =
            serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint)).unwrap();
        persister
            .object_store
            .put(path.as_ref(), json.into())
            .await
            .unwrap();
    }

    // Create 3 checkpoints in February
    for seq in [10, 11, 12] {
        let checkpoint = create_test_checkpoint(feb_2025, seq);
        let path =
            SnapshotCheckpointPath::new("test_host", &feb_2025, SnapshotSequenceNumber::new(seq));
        let json =
            serde_json::to_vec_pretty(&PersistedSnapshotCheckpointVersion::V1(checkpoint)).unwrap();
        persister
            .object_store
            .put(path.as_ref(), json.into())
            .await
            .unwrap();
    }

    // Verify initial state
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 4);
    assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 3);

    // Cleanup January only
    Persister::cleanup_old_checkpoints_for_month(
        persister.node_identifier_prefix.clone(),
        Arc::clone(&persister.object_store),
        jan_2025,
    )
    .await;

    // January should have 2, February should still have 3
    assert_eq!(count_checkpoints_for_month(&persister, &jan_2025).await, 2);
    assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 3);

    // Cleanup February
    Persister::cleanup_old_checkpoints_for_month(
        persister.node_identifier_prefix.clone(),
        Arc::clone(&persister.object_store),
        feb_2025,
    )
    .await;

    // Now both should have 2
    assert_eq!(count_checkpoints_for_month(&persister, &feb_2025).await, 2);
}

// =========================================================================
// list_snapshot_paths_older_than / try_load_snapshot_from_meta Tests
// =========================================================================

#[tokio::test]
async fn test_list_snapshot_paths_older_than_returns_oldest_first() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Persist 5 snapshots (seq 1..=5)
    for seq in 1..=5u64 {
        let snap = PersistedSnapshotVersion::V1(create_test_snapshot(seq));
        persister.persist_snapshot(&snap).await.unwrap();
    }

    // List the 3 oldest snapshot paths older than seq 5
    let paths = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(5), 3)
        .await
        .unwrap();

    assert_eq!(paths.len(), 3);

    // Load each and verify oldest-first order: seq 1, 2, 3
    let s0 = persister
        .try_load_snapshot_from_meta(&paths[0])
        .await
        .unwrap()
        .unwrap();
    let s1 = persister
        .try_load_snapshot_from_meta(&paths[1])
        .await
        .unwrap()
        .unwrap();
    let s2 = persister
        .try_load_snapshot_from_meta(&paths[2])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s0.v1_ref().snapshot_sequence_number.as_u64(), 1);
    assert_eq!(s1.v1_ref().snapshot_sequence_number.as_u64(), 2);
    assert_eq!(s2.v1_ref().snapshot_sequence_number.as_u64(), 3);
}

#[tokio::test]
async fn test_list_snapshot_paths_older_than_limit_larger_than_available() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    for seq in 1..=3u64 {
        let snap = PersistedSnapshotVersion::V1(create_test_snapshot(seq));
        persister.persist_snapshot(&snap).await.unwrap();
    }

    // Ask for 10 but only 2 are older than seq 3
    let paths = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(3), 10)
        .await
        .unwrap();

    assert_eq!(paths.len(), 2);
    let s0 = persister
        .try_load_snapshot_from_meta(&paths[0])
        .await
        .unwrap()
        .unwrap();
    let s1 = persister
        .try_load_snapshot_from_meta(&paths[1])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s0.v1_ref().snapshot_sequence_number.as_u64(), 1);
    assert_eq!(s1.v1_ref().snapshot_sequence_number.as_u64(), 2);
}

#[tokio::test]
async fn test_list_snapshot_paths_older_than_none_older() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    for seq in 1..=3u64 {
        let snap = PersistedSnapshotVersion::V1(create_test_snapshot(seq));
        persister.persist_snapshot(&snap).await.unwrap();
    }

    // threshold=1 → nothing older than seq 1
    let paths = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(1), 10)
        .await
        .unwrap();

    assert!(paths.is_empty());
}

#[tokio::test]
async fn test_list_snapshot_paths_older_than_respects_threshold() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    for seq in 1..=5u64 {
        let snap = PersistedSnapshotVersion::V1(create_test_snapshot(seq));
        persister.persist_snapshot(&snap).await.unwrap();
    }

    // threshold=3 → only seq 1 and 2 are older
    let paths = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(3), 10)
        .await
        .unwrap();

    assert_eq!(paths.len(), 2);
    let s0 = persister
        .try_load_snapshot_from_meta(&paths[0])
        .await
        .unwrap()
        .unwrap();
    let s1 = persister
        .try_load_snapshot_from_meta(&paths[1])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s0.v1_ref().snapshot_sequence_number.as_u64(), 1);
    assert_eq!(s1.v1_ref().snapshot_sequence_number.as_u64(), 2);
}

#[tokio::test]
async fn test_list_snapshot_paths_older_than_keeps_oldest_when_limited() {
    let (persister, _) = create_test_persister(JAN_15_2025_NANOS, None);

    // Persist 4 snapshots. Inverted paths (u64::MAX - seq, 20-digit zero-padded):
    //   seq=1 → test_host/snapshots/18446744073709551614.info.json  (largest path = oldest)
    //   seq=2 → test_host/snapshots/18446744073709551613.info.json
    //   seq=3 → test_host/snapshots/18446744073709551612.info.json
    //   seq=4 → test_host/snapshots/18446744073709551611.info.json  (smallest path = newest)
    //
    // list_with_offset yields ascending: ...611, ...612, ...613, ...614 → seq 4, 3, 2, 1
    // Sliding window (limit=2) keeps last 2: ...613, ...614 → seq 2, 1
    // Reverse → oldest-first: seq 1, seq 2
    for seq in 1..=4u64 {
        let snap = create_test_snapshot(seq);
        persister
            .persist_snapshot(&PersistedSnapshotVersion::V1(snap))
            .await
            .unwrap();
    }

    let paths = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(5), 2)
        .await
        .unwrap();

    assert_eq!(paths.len(), 2);

    // Verify paths: oldest snapshots have the LARGEST inverted stems
    let path0 = paths[0].location.to_string();
    let path1 = paths[1].location.to_string();
    assert_eq!(path0, "test_host/snapshots/18446744073709551614.info.json"); // seq=1
    assert_eq!(path1, "test_host/snapshots/18446744073709551613.info.json"); // seq=2

    // Confirm via content: oldest-first ordering
    let s0 = persister
        .try_load_snapshot_from_meta(&paths[0])
        .await
        .unwrap()
        .unwrap();
    let s1 = persister
        .try_load_snapshot_from_meta(&paths[1])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s0.v1_ref().snapshot_sequence_number.as_u64(), 1);
    assert_eq!(s1.v1_ref().snapshot_sequence_number.as_u64(), 2);
}
