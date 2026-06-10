use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use uuid::Uuid;

use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::format::apply::{serialize_log_file, serialize_snapshot_file};
use crate::format::records::RegisterNode;
use crate::format::records::types::NodeMode;
use crate::format::{FeatureLevel, MakeRecord, Record, file_flags};
use crate::object_store::{CatalogFilePath, PersistCatalogResult};

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
    let inner = store.load_or_create_catalog().await.unwrap();

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
    let first = store.load_or_create_catalog().await.unwrap();
    let second = store.load_or_create_catalog().await.unwrap();
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
    let a = a.unwrap();
    let b = b.unwrap();
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

    let inner = store.load_catalog().await.unwrap().expect("load");
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
