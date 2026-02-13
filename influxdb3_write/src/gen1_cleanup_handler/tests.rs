#![allow(clippy::too_many_arguments)]
use super::*;
use crate::CatalogSequenceNumber;
use crate::{ParquetFile, PersistedSnapshot};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{SnapshotSequenceNumber, WalFileSequenceNumber};
use rstest::rstest;

use crate::PersistedSnapshotVersion;
use crate::paths::TableIndexPath;
use crate::persister::Persister;
use crate::table_index::{CoreTableIndex, IndexMetadata};
use crate::table_index_cache::{TableIndexCache, TableIndexCacheConfig};
use crate::write_buffer::persisted_files::PersistedFiles;
use influxdb3_id::TableIndexId;
use object_store::memory::InMemory;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug)]
struct MockCompactedDataProvider {
    snapshot_marker: Option<(SnapshotSequenceNumber, ParquetFileId)>,
    leftover_ids: HashSet<ParquetFileId>,
    ready: bool,
}

impl MockCompactedDataProvider {
    fn new() -> Self {
        Self {
            snapshot_marker: None,
            leftover_ids: HashSet::new(),
            ready: false,
        }
    }

    fn with_marker(mut self, seq: u64, next_file_id: u64) -> Self {
        self.snapshot_marker = Some((
            SnapshotSequenceNumber::new(seq),
            ParquetFileId::from(next_file_id),
        ));
        self
    }

    fn with_ready(mut self, ready: bool) -> Self {
        self.ready = ready;
        self
    }
}

impl CompactedDataProvider for MockCompactedDataProvider {
    fn get_snapshot_marker_for_node(
        &self,
        _node_id: &str,
    ) -> Option<(SnapshotSequenceNumber, ParquetFileId)> {
        self.snapshot_marker
    }

    fn leftover_file_ids_for_node(&self, _node_id: &str) -> HashSet<ParquetFileId> {
        self.leftover_ids.clone()
    }

    fn is_ready(&self) -> bool {
        self.ready
    }
}

// --- is_file_eligible_for_deletion ---

#[rstest]
#[case::below_threshold(97, 100, vec![], true)]
#[case::just_below_threshold(99, 100, vec![], true)]
#[case::at_threshold(100, 100, vec![], false)]
#[case::above_threshold(101, 100, vec![], false)]
#[case::in_leftover(85, 100, vec![85, 90], false)]
#[case::in_leftover_other(90, 100, vec![85, 90], false)]
#[case::not_in_leftover(95, 100, vec![85, 90], true)]
#[case::above_threshold_even_if_in_leftover(105, 100, vec![105], false)]
fn test_file_eligible_for_deletion(
    #[case] file_id: u64,
    #[case] next_file_id: u64,
    #[case] leftover: Vec<u64>,
    #[case] expected: bool,
) {
    let leftover_ids: HashSet<ParquetFileId> =
        leftover.into_iter().map(ParquetFileId::from).collect();
    assert_eq!(
        is_file_eligible_for_deletion(
            ParquetFileId::from(file_id),
            ParquetFileId::from(next_file_id),
            &leftover_ids,
        ),
        expected,
    );
}

// --- is_snapshot_fully_compacted ---

#[rstest]
#[case::all_below_threshold(vec![(1, 1, vec![10, 20, 30])], 100, vec![], true)]
#[case::file_above_threshold(vec![(1, 1, vec![10, 20, 150])], 100, vec![], false)]
#[case::file_in_leftover(vec![(1, 1, vec![10, 20, 30])], 100, vec![20], false)]
#[case::empty_databases(vec![], 100, vec![], true)]
#[case::multi_table_all_compacted(
    vec![(1, 1, vec![10, 20]), (1, 2, vec![30, 40])], 100, vec![], true
)]
#[case::multi_table_one_has_leftover(
    vec![(1, 1, vec![10, 20]), (1, 2, vec![30, 40])], 100, vec![40], false
)]
fn test_snapshot_fully_compacted(
    #[case] tables: Vec<(u32, u32, Vec<u64>)>,
    #[case] next_file_id: u64,
    #[case] leftover: Vec<u64>,
    #[case] expected: bool,
) {
    let file_ids: Vec<(DbId, TableId, Vec<u64>)> = tables
        .into_iter()
        .map(|(db, tbl, ids)| (DbId::from(db), TableId::from(tbl), ids))
        .collect();
    let snap = make_snapshot("node-A", 1, file_ids);
    let leftover_ids: HashSet<ParquetFileId> =
        leftover.into_iter().map(ParquetFileId::from).collect();
    assert_eq!(
        is_snapshot_fully_compacted(&snap, ParquetFileId::from(next_file_id), &leftover_ids),
        expected,
    );
}

// --- check_eligibility ---

#[rstest]
// All checks pass: correct node, no persisted_at (age check skipped), seq < latest, all files < next_file_id
#[case::eligible(
    "node-A", 1, None, vec![(1, 1, vec![10, 20])],
    "node-A", 1000, 0, 5, 100, vec![],
    SnapshotEligibility::Eligible
)]
// Snapshot belongs to node-B but cleanup runs for node-A → Skip
#[case::wrong_node(
    "node-B", 1, None, vec![(1, 1, vec![10])],
    "node-A", 1000, 0, 5, 100, vec![],
    SnapshotEligibility::Skip { reason: "wrong node" }
)]
// persisted_at=900, now=1000 → age=100ms < min_age=200ms → too recent, Skip
#[case::too_recent(
    "node-A", 1, Some(900), vec![(1, 1, vec![10])],
    "node-A", 1000, 200, 5, 100, vec![],
    SnapshotEligibility::Skip { reason: "too recent" }
)]
// persisted_at=None means age check is skipped entirely (field populated at load time, not in tests).
// Even with a huge min_age, the snapshot is still Eligible.
#[case::no_persisted_at_skips_age_check(
    "node-A", 1, None, vec![(1, 1, vec![10])],
    "node-A", 1000, 9999999, 5, 100, vec![],
    SnapshotEligibility::Eligible
)]
// persisted_at=500, now=1000 → age=500ms >= min_age=200ms → passes age check, Eligible
#[case::old_enough(
    "node-A", 1, Some(500), vec![(1, 1, vec![10])],
    "node-A", 1000, 200, 5, 100, vec![],
    SnapshotEligibility::Eligible
)]
// snapshot seq=5 >= latest_seq=5 → this IS the latest snapshot, must not be deleted → Stop
#[case::is_latest(
    "node-A", 5, None, vec![(1, 1, vec![10])],
    "node-A", 1000, 0, 5, 100, vec![],
    SnapshotEligibility::Stop { reason: "is latest" }
)]
// File 150 >= next_file_id=100 → not yet compacted by the compactor → Stop
#[case::has_uncompacted_files(
    "node-A", 1, None, vec![(1, 1, vec![10, 150])],
    "node-A", 1000, 0, 5, 100, vec![],
    SnapshotEligibility::Stop { reason: "has uncompacted files" }
)]
// File 20 is in the leftover set (compactor skipped it) → must not delete → Stop
#[case::has_leftover_files(
    "node-A", 1, None, vec![(1, 1, vec![10, 20])],
    "node-A", 1000, 0, 5, 100, vec![20],
    SnapshotEligibility::Stop { reason: "has uncompacted files" }
)]
fn test_check_eligibility(
    #[case] snap_node: &str,
    #[case] snap_seq: u64,
    #[case] persisted_at: Option<i64>,
    #[case] tables: Vec<(u32, u32, Vec<u64>)>,
    #[case] node_id: &str,
    #[case] now_ms: i64,
    #[case] min_age_ms: i64,
    #[case] latest_seq: u64,
    #[case] next_file_id: u64,
    #[case] leftover: Vec<u64>,
    #[case] expected: SnapshotEligibility,
) {
    let file_ids = tables
        .into_iter()
        .map(|(db, tbl, ids)| (DbId::from(db), TableId::from(tbl), ids))
        .collect();
    let mut snap = make_snapshot(snap_node, snap_seq, file_ids);
    snap.persisted_at = persisted_at;
    let leftover_ids: HashSet<ParquetFileId> =
        leftover.into_iter().map(ParquetFileId::from).collect();
    assert_eq!(
        check_eligibility(
            &snap,
            node_id,
            now_ms,
            min_age_ms,
            SnapshotSequenceNumber::new(latest_seq),
            ParquetFileId::from(next_file_id),
            &leftover_ids,
        ),
        expected,
    );
}

// --- cleanup_loop integration tests (real Persister with InMemory object store) ---

async fn make_test_handler(
    node_id: &str,
    provider: MockCompactedDataProvider,
    persisted_files: Option<Arc<PersistedFiles>>,
) -> (
    Arc<Gen1CleanupHandler<MockCompactedDataProvider>>,
    Arc<Persister>,
    Arc<PersistedFiles>,
) {
    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let time_provider: Arc<dyn iox_time::TimeProvider> = Arc::new(iox_time::SystemProvider::new());
    let persister = Arc::new(Persister::new(
        Arc::clone(&object_store),
        node_id,
        Arc::clone(&time_provider),
        None,
    ));
    let table_index_cache = TableIndexCache::new(
        node_id.to_string(),
        TableIndexCacheConfig::default(),
        Arc::clone(&object_store),
    );
    let persisted_files = persisted_files.unwrap_or_else(|| Arc::new(PersistedFiles::new(None)));

    let handler = Arc::new(Gen1CleanupHandler::new(
        node_id.to_string(),
        Arc::new(provider),
        table_index_cache,
        Arc::clone(&persister),
        Arc::clone(&persisted_files),
        time_provider,
    ));

    (handler, persister, persisted_files)
}

async fn persist_test_snapshot(
    persister: &Persister,
    node_id: &str,
    seq: u64,
    file_ids: Vec<(DbId, TableId, Vec<u64>)>,
) {
    let snap = make_snapshot(node_id, seq, file_ids);
    persister
        .persist_snapshot(&PersistedSnapshotVersion::V1(snap))
        .await
        .expect("persist snapshot");
}

async fn count_remaining_snapshots(persister: &Persister, node_id: &str) -> usize {
    let latest = persister.get_latest_snapshot_sequence().await.unwrap();
    let Some(latest) = latest else { return 0 };
    let metas = persister
        .list_snapshot_paths_older_than(SnapshotSequenceNumber::new(latest.as_u64() + 1), 1000)
        .await
        .unwrap();
    let mut count = 0;
    for meta in &metas {
        let snap = persister
            .try_load_snapshot_from_meta(meta)
            .await
            .unwrap()
            .unwrap();
        let PersistedSnapshotVersion::V1(ps) = &snap;
        if ps.node_id == node_id {
            count += 1;
        }
    }
    count
}

#[tokio::test]
async fn test_try_start_returns_already_running() {
    let provider = MockCompactedDataProvider::new().with_ready(true);
    let (handler, _, _) = make_test_handler("test-node", provider, None).await;

    handler.is_running.store(true, Ordering::SeqCst);
    let result = handler.try_start_cleanup(Duration::ZERO, 500, 10);
    assert_eq!(result, Gen1CleanupResult::AlreadyRunning);
}

#[tokio::test]
async fn test_try_start_returns_started() {
    // No marker → cleanup_loop exits immediately
    let provider = MockCompactedDataProvider::new().with_ready(true);
    let (handler, _, _) = make_test_handler("test-node", provider, None).await;

    let result = handler.try_start_cleanup(Duration::ZERO, 500, 10);
    assert_eq!(result, Gen1CleanupResult::Started);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!handler.is_running.load(Ordering::SeqCst));
}

/// Snapshots 1,2,3 eligible (all files < threshold); snapshot 4 is latest → only 4 remains.
#[tokio::test]
async fn test_cleanup_deletes_eligible_and_preserves_latest() {
    let node_id = "test-node";
    let provider = MockCompactedDataProvider::new()
        .with_marker(10, 100)
        .with_ready(true);
    let (handler, persister, _) = make_test_handler(node_id, provider, None).await;

    for seq in 1..=4 {
        persist_test_snapshot(
            &persister,
            node_id,
            seq,
            vec![(DbId::from(1), TableId::from(1), vec![seq * 10])],
        )
        .await;
    }

    handler.cleanup_loop(Duration::ZERO, 500, 10).await;

    let latest = persister.get_latest_snapshot_sequence().await.unwrap();
    assert_eq!(latest, Some(SnapshotSequenceNumber::new(4)));

    let remaining = count_remaining_snapshots(&persister, node_id).await;
    assert_eq!(remaining, 1);
}

/// Verifies PersistedFiles index is updated (files removed from query path).
#[tokio::test]
async fn test_cleanup_removes_from_persisted_files() {
    let node_id = "test-node";
    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    let snap1 = make_snapshot(node_id, 1, vec![(db_id, table_id, vec![10, 20])]);
    let snap2 = make_snapshot(node_id, 2, vec![(db_id, table_id, vec![30])]);
    let snap_latest = make_snapshot(node_id, 3, vec![(db_id, table_id, vec![40])]);

    let all_snaps: Vec<PersistedSnapshot> = vec![snap1, snap2, snap_latest];
    let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
        None,
        Arc::new(all_snaps),
    ));

    assert_eq!(persisted_files.get_files(db_id, table_id).len(), 4);

    let provider = MockCompactedDataProvider::new()
        .with_marker(10, 100)
        .with_ready(true);
    let (handler, persister, persisted_files) =
        make_test_handler(node_id, provider, Some(persisted_files)).await;

    for seq in 1..=3 {
        let file_ids = match seq {
            1 => vec![10, 20],
            2 => vec![30],
            3 => vec![40],
            _ => unreachable!(),
        };
        persist_test_snapshot(&persister, node_id, seq, vec![(db_id, table_id, file_ids)]).await;
    }

    handler.cleanup_loop(Duration::ZERO, 500, 10).await;

    let files_after = persisted_files.get_files(db_id, table_id);
    assert_eq!(files_after.len(), 1);
    assert_eq!(files_after[0].id.as_u64(), 40);
}

/// Verifies that cleanup updates the TableIndexCache: only files from processed
/// snapshots are purged, not files from the latest (unprocessed) snapshot.
#[tokio::test]
async fn test_cleanup_updates_table_index_cache() {
    let node_id = "test-node";
    let db_id = DbId::from(1);
    let table_id = TableId::from(1);

    let provider = MockCompactedDataProvider::new()
        .with_marker(10, 100)
        .with_ready(true);
    let (handler, persister, _) = make_test_handler(node_id, provider, None).await;

    // Create a CoreTableIndex with files 10, 20, 30, 40 and persist to object store
    let table_index_id = TableIndexId::new(node_id, db_id, table_id);
    let mut files = BTreeSet::new();
    for id in [10u64, 20, 30, 40] {
        files.insert(Arc::new(ParquetFile {
            id: ParquetFileId::from(id),
            path: format!("{}/dbs/{}/{}/{}.parquet", node_id, db_id, table_id, id),
            size_bytes: 100,
            row_count: 10,
            chunk_time: 1000,
            min_time: 1000,
            max_time: 2000,
        }));
    }
    let index = CoreTableIndex {
        id: table_index_id,
        files,
        latest_snapshot_sequence_number: SnapshotSequenceNumber::new(1),
        metadata: IndexMetadata {
            parquet_size_bytes: 400,
            row_count: 40,
            min_time: 1000,
            max_time: 2000,
        },
    };
    let object_store = persister.object_store();
    index.persist(Arc::clone(&object_store)).await.unwrap();

    // Persist snapshots: snap1 (files 10,20), snap2 (files 30), snap3=latest (files 40)
    persist_test_snapshot(
        &persister,
        node_id,
        1,
        vec![(db_id, table_id, vec![10, 20])],
    )
    .await;
    persist_test_snapshot(&persister, node_id, 2, vec![(db_id, table_id, vec![30])]).await;
    persist_test_snapshot(&persister, node_id, 3, vec![(db_id, table_id, vec![40])]).await;

    // Run cleanup — snap1 and snap2 are processed; snap3 is latest (preserved)
    handler.cleanup_loop(Duration::ZERO, 500, 10).await;

    // Read the CoreTableIndex back from object store
    let table_index_path = TableIndexPath::new(node_id, db_id.get(), table_id.get());
    let updated_index =
        CoreTableIndex::from_object_store(Arc::clone(&object_store), &table_index_path)
            .await
            .unwrap();

    let remaining_ids: Vec<u64> = updated_index.files.iter().map(|f| f.id.as_u64()).collect();
    assert_eq!(
        remaining_ids,
        vec![40],
        "Only file 40 (from latest snapshot) should remain in table index"
    );
}

// --- helpers ---

fn make_snapshot(
    node_id: &str,
    seq: u64,
    file_ids: Vec<(DbId, TableId, Vec<u64>)>,
) -> PersistedSnapshot {
    let mut snap = PersistedSnapshot::new(
        node_id.to_string(),
        SnapshotSequenceNumber::new(seq),
        WalFileSequenceNumber::new(seq),
        CatalogSequenceNumber::new(seq),
    );
    for (db_id, table_id, ids) in file_ids {
        for id in ids {
            snap.databases
                .entry(db_id)
                .or_default()
                .tables
                .entry(table_id)
                .or_default()
                .push(ParquetFile {
                    id: ParquetFileId::from(id),
                    path: format!("{}/dbs/{}/{}/{}.parquet", node_id, db_id, table_id, id),
                    size_bytes: 100,
                    row_count: 10,
                    chunk_time: 1000,
                    min_time: 1000,
                    max_time: 2000,
                });
        }
    }
    snap
}
