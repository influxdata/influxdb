use std::{assert_matches, sync::Arc};

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use crate::format::apply::{serialize_log_file, serialize_snapshot_file};
use crate::format::records::RegisterNode;
use crate::format::records::types::NodeMode;
use crate::format::{FeatureLevel, MakeRecord, Record, file_flags};
use crate::object_store::{CatalogFilePath, PersistCatalogResult};
use crate::{
    catalog::versions::v3::schema::storage::StorageMode, object_store::versions::v3::FastForwardErr,
};
use crate::{
    catalog::{CatalogSequenceNumber, versions::v3::inner::InnerCatalog},
    format::{self, CatalogFile, Header},
};

use super::ObjectStoreCatalog;

fn test_store() -> ObjectStoreCatalog {
    ObjectStoreCatalog::new("prefix", Arc::new(InMemory::new()), StorageMode::default())
}

fn sample_record(sequence: u64) -> Record {
    RegisterNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        instance_id: "inst-1".to_string(),
        registered_time_ns: 1000,
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: [0u8; 16],
        conn_info: None,
        cli_params: None,
        row_delete_predicate_version: 0,
        feature_level: FeatureLevel::ZERO,
    }
    .make_record(sequence)
}

fn add_records_and_serialize_snapshot(sequence: u64, records: &[Record]) -> Bytes {
    serialize_snapshot_file(Uuid::nil(), sequence, records)
}

#[tokio::test]
async fn persist_and_load_log_round_trip() {
    let store = test_store();
    let seq = CatalogSequenceNumber::new(1);
    let bytes = serialize_log_file(Uuid::nil(), seq.get(), &[sample_record(1)]);

    let result = store.persist_log(seq, bytes.clone()).await.unwrap();
    assert!(matches!(result, PersistCatalogResult::Success));

    let loaded = store.load_log(seq).await.unwrap().expect("file present");
    assert_eq!(loaded.header.sequence_number, 1);
    assert_eq!(loaded.record_count(), 1);
}

#[tokio::test]
async fn initialize_and_load_snapshot_round_trip() {
    let store = test_store();
    let bytes = add_records_and_serialize_snapshot(5, &[sample_record(5)]);

    let result = store.initialize_snapshot(bytes).await.unwrap();
    assert!(matches!(result, PersistCatalogResult::Success));

    let (loaded, _size_bytes) = store.load_snapshot().await.unwrap().expect("file present");
    assert_eq!(loaded.header.sequence_number, 5);
    assert_eq!(
        loaded.header.flags & file_flags::SNAPSHOT,
        file_flags::SNAPSHOT
    );
    assert_eq!(loaded.record_count(), 1);
    assert_eq!(loaded.records.len(), 1);
}

#[tokio::test]
async fn load_log_returns_none_when_missing() {
    let store = test_store();
    let loaded = store
        .load_log(CatalogSequenceNumber::new(42))
        .await
        .unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn load_snapshot_returns_none_when_missing() {
    let store = test_store();
    let loaded = store.load_snapshot().await.unwrap();
    assert!(loaded.is_none());
}

#[tokio::test]
async fn persist_log_twice_returns_already_exists() {
    let store = test_store();
    let seq = CatalogSequenceNumber::new(1);
    let bytes = serialize_log_file(Uuid::nil(), seq.get(), &[sample_record(1)]);

    let first = store.persist_log(seq, bytes.clone()).await.unwrap();
    assert!(matches!(first, PersistCatalogResult::Success));

    let second = store.persist_log(seq, bytes).await.unwrap();
    assert!(matches!(second, PersistCatalogResult::AlreadyExists));
}

#[tokio::test]
async fn initialize_snapshot_twice_returns_already_exists() {
    let store = test_store();
    let bytes = add_records_and_serialize_snapshot(1, &[sample_record(1)]);

    let first = store.initialize_snapshot(bytes.clone()).await.unwrap();
    assert!(matches!(first, PersistCatalogResult::Success));

    let second = store.initialize_snapshot(bytes).await.unwrap();
    assert!(matches!(second, PersistCatalogResult::AlreadyExists));
}

#[tokio::test]
async fn update_snapshot_replaces_existing() {
    let store = test_store();
    let initial_bytes = add_records_and_serialize_snapshot(1, &[sample_record(1)]);

    let result = store.initialize_snapshot(initial_bytes).await.unwrap();
    assert!(matches!(result, PersistCatalogResult::Success));

    let update_bytes = add_records_and_serialize_snapshot(7, &[sample_record(1), sample_record(2)]);
    store.update_snapshot(update_bytes).await.unwrap();

    let (loaded, _size_bytes) = store.load_snapshot().await.unwrap().expect("file present");
    assert_eq!(loaded.header.sequence_number, 7);
    assert_eq!(loaded.record_count(), 2);
}

#[tokio::test]
async fn load_catalog_returns_none_for_empty_store() {
    let store = test_store();
    assert!(store.load_catalog().await.unwrap().is_none());
}

#[tokio::test]
async fn load_or_create_initializes_fresh_catalog() {
    let store =
        ObjectStoreCatalog::new("prefix", Arc::new(InMemory::new()), StorageMode::PachaTree);
    let load = store.load_or_create_catalog().await.unwrap();
    assert!(!load.snapshot_needs_rewrite);
    let inner = load.inner;

    assert_eq!(inner.sequence_number(), CatalogSequenceNumber::new(0));
    assert_ne!(inner.catalog_uuid, Uuid::nil());
    // The initial snapshot carries a single SetStorageMode record so the
    // configured storage mode survives reload.
    assert_eq!(inner.ordered_records.len(), 1);
    assert_eq!(inner.storage_mode, StorageMode::PachaTree);

    // A snapshot is persisted at the well-known path.
    let (snapshot, _size_bytes) = store
        .load_snapshot()
        .await
        .unwrap()
        .expect("snapshot present");
    assert_eq!(
        snapshot.header.flags & file_flags::SNAPSHOT,
        file_flags::SNAPSHOT
    );
    assert_eq!(snapshot.header.catalog_uuid, inner.catalog_uuid.as_u128());
    assert_eq!(snapshot.record_count(), 1);
}

#[tokio::test]
async fn load_or_create_is_idempotent() {
    let store = test_store();
    let first = store.load_or_create_catalog().await.unwrap().inner;
    let second = store.load_or_create_catalog().await.unwrap().inner;
    assert_eq!(first.catalog_uuid, second.catalog_uuid);
}

#[tokio::test]
async fn load_or_create_resolves_concurrent_init() {
    // Two `ObjectStoreCatalog`s sharing one underlying object store both
    // attempt to bootstrap. PutMode::Create makes one win; the loser hits
    // PersistCatalogResult::AlreadyExists and reloads the winner's state.
    let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cat_a = ObjectStoreCatalog::new("prefix", Arc::clone(&shared), StorageMode::default());
    let cat_b = ObjectStoreCatalog::new("prefix", shared, StorageMode::default());

    let (a, b) = tokio::join!(
        cat_a.load_or_create_catalog(),
        cat_b.load_or_create_catalog()
    );
    let a = a.unwrap().inner;
    let b = b.unwrap().inner;
    assert_eq!(a.catalog_uuid, b.catalog_uuid);
}

#[tokio::test]
async fn load_catalog_replays_snapshot_and_logs() {
    let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = ObjectStoreCatalog::new("prefix", Arc::clone(&shared), StorageMode::default());

    // Seed: snapshot at sequence 0 with one record, then logs at 1 and 2.
    let snapshot_bytes = add_records_and_serialize_snapshot(0, &[sample_record(0)]);
    store.initialize_snapshot(snapshot_bytes).await.unwrap();

    let log1 = serialize_log_file(
        Uuid::nil(),
        1,
        &[crate::format::records::SetStorageMode {
            mode: crate::format::records::types::StorageMode::PachaTree,
        }
        .make_record(1)],
    );
    store
        .persist_log(CatalogSequenceNumber::new(1), log1)
        .await
        .unwrap();
    let log2 = serialize_log_file(
        Uuid::nil(),
        2,
        &[crate::format::records::SetGenerationDuration {
            level: 0,
            duration_ns: 60_000_000_000,
        }
        .make_record(2)],
    );
    store
        .persist_log(CatalogSequenceNumber::new(2), log2)
        .await
        .unwrap();

    let load = store.load_catalog().await.unwrap().expect("load");
    assert!(!load.snapshot_needs_rewrite);
    let inner = load.inner;
    assert_eq!(inner.sequence_number(), CatalogSequenceNumber::new(2));
    assert_eq!(inner.ordered_records.len(), 3); // snapshot + 2 log records
}

#[test]
fn catalog_file_path_logs_dir_returns_logs_dir() {
    assert_eq!(
        *CatalogFilePath::logs_dir("cats"),
        ObjPath::from("cats/catalog/v3/logs")
    );
}

#[test]
fn catalog_file_path_restore_staging_dir_returns_staging_dir() {
    assert_eq!(
        *CatalogFilePath::restore_staging_dir("cats", "restore-1"),
        ObjPath::from("cats/catalog/restores/restore-1")
    );
}

fn cat_file(
    header_template: &Header,
    num_records: u32,
    cat_sequence: u64,
    rec_sequence_start: u64,
) -> CatalogFile {
    let mut header = *header_template;
    header.record_count = num_records;
    header.sequence_number = cat_sequence;

    CatalogFile {
        header,
        records: (0..num_records)
            .into_iter()
            .map(|rec| sample_record(rec_sequence_start + u64::from(rec)))
            .collect(),
    }
}

fn create_header() -> Header {
    let mut header_bytes = [0u8; Header::SIZE];
    header_bytes[..4].copy_from_slice(&format::MAGIC);
    header_bytes[4..8].copy_from_slice(&Header::CURRENT_VERSION.to_le_bytes());
    // pre-calculated crc
    header_bytes[8..12].copy_from_slice(&3425374128u32.to_le_bytes());

    let mut cursor = std::io::Cursor::new(&header_bytes);
    Header::read_from(&mut cursor).unwrap()
}

#[tokio::test]
async fn snapshot_fast_forwarding_works_in_basic_case() {
    let store = test_store();

    let mut catalog = InnerCatalog::new(Arc::from("catalog"), Uuid::new_v4());
    let header = create_header();

    let first_file = cat_file(&header, 3, 0, 0);
    store
        .fast_forward_inner_with_snapshot(first_file.clone(), &mut catalog)
        .await
        .unwrap();

    let second_file = cat_file(&header, 4, 1, 3);
    store
        .fast_forward_inner_with_snapshot(second_file.clone(), &mut catalog)
        .await
        .unwrap();

    let mut expected_records = first_file
        .records
        .iter()
        .chain(&second_file.records)
        .cloned()
        .collect::<Vec<_>>();

    // first, make sure that applying one file and then the second applies both sets of records
    // without issue.
    assert_eq!(&*catalog.ordered_records, &*expected_records);

    // apply them again, out-of-order. Shouldn't do anything and should be silently ignored
    store
        .fast_forward_inner_with_snapshot(second_file.clone(), &mut catalog)
        .await
        .unwrap();
    store
        .fast_forward_inner_with_snapshot(first_file.clone(), &mut catalog)
        .await
        .unwrap();

    assert_eq!(&*catalog.ordered_records, &*expected_records);

    // for this next part, we want to make a catalog file, then modify it to include some records
    // that were already applied.
    let mut third_file = cat_file(&header, 3, 2, 7);
    let orig_third_files = third_file.records.clone();
    third_file.records = catalog.ordered_records[5..]
        .iter()
        .cloned()
        .chain(third_file.records)
        .collect();
    third_file.header.record_count += 3;
    // we're not going to adjust the header sequence count since it shouldn't matter

    // we apply it, including the files we already had. It should take the records we haven't
    // applied yet
    store
        .fast_forward_inner_with_snapshot(third_file, &mut catalog)
        .await
        .unwrap();

    expected_records.extend(orig_third_files);
    assert_eq!(&*catalog.ordered_records, &*expected_records);

    // then we want to make a set of records that have a gap between the currently-applied records -
    // the catalog shouldn't apply them since doing so would skip over and fail to apply some records
    let disconnected_records = cat_file(&header, 3, 6, 20);
    let err = store
        .fast_forward_inner_with_snapshot(disconnected_records, &mut catalog)
        .await
        .unwrap_err();

    assert_matches!(err, FastForwardErr::WouldCreateGap);
}

#[tokio::test]
async fn snapshots_reject_non_monotonically_increasing_records() {
    let store = test_store();

    let mut catalog = InnerCatalog::new(Arc::from("catalog"), Uuid::new_v4());
    let header = create_header();

    // and now we try to insert something that's not ordered from lowest sequence to highest. It
    // should also fail
    let mut unordered_records = cat_file(&header, 10, 3, 9);
    unordered_records.records[0].header.sequence = 100;

    let err = store
        .fast_forward_inner_with_snapshot(unordered_records, &mut catalog)
        .await
        .unwrap_err();

    assert_matches!(err, FastForwardErr::NotMonotonicallyIncreasing);
}

// TODO(june): fast-forwarding should correctly remove hard-deleted records when it encounters a
// hard-deletion record (once we figure out the exact semantics for that)
