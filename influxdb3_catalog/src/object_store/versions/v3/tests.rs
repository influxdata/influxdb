use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use parking_lot::RwLock;
use pretty_assertions::assert_eq;
use uuid::Uuid;

use crate::{
    catalog::{
        CatalogSequenceNumber,
        versions::v3::{inner::InnerCatalog, schema::storage::StorageMode},
    },
    format::{
        self, CatalogFile, FeatureLevel, Header, MakeRecord, Record,
        apply::{serialize_log_file, serialize_snapshot_file},
        file_flags,
        records::{
            self, CreateDatabase, RegisterNode, SetGenerationDuration, SetStorageMode,
            types::{NodeMode, RetentionPeriod},
        },
    },
    object_store::{
        CatalogFileMeta, CatalogFilePath, LoadedCatalogFile, MaybePutCatalogFile,
        PersistCatalogResult,
    },
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
    std::assert_matches!(result, MaybePutCatalogFile::Success(_));

    let LoadedCatalogFile { file: loaded, .. } =
        store.load_snapshot().await.unwrap().expect("file present");
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
    std::assert_matches!(first, MaybePutCatalogFile::Success(_));

    let second = store.initialize_snapshot(bytes).await.unwrap();
    std::assert_matches!(second, MaybePutCatalogFile::AlreadyExists);
}

#[tokio::test]
async fn update_snapshot_replaces_existing() {
    let store = test_store();
    let initial_bytes = add_records_and_serialize_snapshot(1, &[sample_record(1)]);

    let result = store.initialize_snapshot(initial_bytes).await.unwrap();
    std::assert_matches!(result, MaybePutCatalogFile::Success(_));

    let update_bytes = add_records_and_serialize_snapshot(7, &[sample_record(1), sample_record(2)]);
    store.update_snapshot(update_bytes).await.unwrap();

    let LoadedCatalogFile { file: loaded, .. } =
        store.load_snapshot().await.unwrap().expect("file present");
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
    let LoadedCatalogFile { file: snapshot, .. } = store
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
        &[SetStorageMode {
            mode: records::types::StorageMode::PachaTree,
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
        &[SetGenerationDuration {
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

fn meta(
    etag: impl Into<Option<&'static str>>,
    version: impl Into<Option<&'static str>>,
) -> CatalogFileMeta {
    CatalogFileMeta {
        etag: etag.into().map(ToString::to_string),
        version: version.into().map(ToString::to_string),
    }
}

#[tokio::test]
async fn snapshot_fast_forwarding_works_in_basic_case() {
    let store = test_store();

    let catalog = RwLock::new(InnerCatalog::new(Arc::from("catalog"), Uuid::new_v4()));
    let header = create_header();

    let first_file = cat_file(&header, 3, 0, 0);
    let meta1 = meta("1", None);
    store
        .fast_forward_inner_with_snapshot(first_file.clone(), meta1.clone(), &catalog)
        .await
        .unwrap();

    let second_file = cat_file(&header, 4, 1, 3);
    let meta2 = meta(None, "2");
    store
        .fast_forward_inner_with_snapshot(second_file.clone(), meta2.clone(), &catalog)
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
    assert_eq!(&*catalog.read().ordered_records, &*expected_records);
    assert_eq!(catalog.read().last_snapshot_meta, meta2);

    // apply them again, out-of-order. Shouldn't apply any records, but should change the
    // `last_snapshot_meta` - we have no way of determining which etags came first or second, so if
    // we are able to apply a snapshot without any errors (even if no individual records are
    // applied), we still store the etag as the latest one. See the big comment inside
    // [`ObjectStoreCatalog::fast_forward_inner_with_snapshot`] to see more reasoning.
    store
        .fast_forward_inner_with_snapshot(second_file.clone(), meta2, &catalog)
        .await
        .unwrap();
    store
        .fast_forward_inner_with_snapshot(first_file.clone(), meta1.clone(), &catalog)
        .await
        .unwrap();

    assert_eq!(&*catalog.read().ordered_records, &*expected_records);
    assert_eq!(catalog.read().last_snapshot_meta, meta1);

    // for this next part, we want to make a catalog file, then modify it to include some records
    // that were already applied.
    let mut third_file = cat_file(&header, 3, 2, 7);
    let orig_third_files = third_file.records.clone();
    third_file.records = catalog.read().ordered_records[5..]
        .iter()
        .cloned()
        .chain(third_file.records)
        .collect();
    third_file.header.record_count += 3;
    // we're not going to adjust the header sequence count since it shouldn't matter

    let meta3 = meta("3", "4");

    // we apply it, including the files we already had. It should take the records we haven't
    // applied yet
    store
        .fast_forward_inner_with_snapshot(third_file, meta3.clone(), &catalog)
        .await
        .unwrap();

    expected_records.extend(orig_third_files);
    assert_eq!(&*catalog.read().ordered_records, &*expected_records);
    assert_eq!(catalog.read().last_snapshot_meta, meta3);
}

/// this checks for the condition described at the beginning of
/// [`ObjectStoreCatalog::fast_forward_inner_with_snapshot_without_updating_meta`] -
/// read the description there for more details
#[tokio::test]
async fn catalogs_can_load_out_of_order_snapshots() {
    let store = test_store();

    let catalog = RwLock::new(InnerCatalog::new(Arc::from("catalog"), Uuid::new_v4()));
    let mut header = create_header();

    // this record will be present in the initial grouped snapshot and the one we try to apply after
    let shared_record = CreateDatabase {
        database_id: 0,
        database_name: "db".into(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .make_record(2);

    // emulating a grouped snapshot - containing three node records and one about a specific
    // database. Specifically a situation where the last sequence is not the highest sequence
    let grouped_snapshot = CatalogFile {
        header: {
            let mut header = header;
            header.record_count = 4;
            header.sequence_number = 0;
            header
        },
        // these records have sequence numbers 0, 1, 3, 2 - in that order.
        records: vec![
            sample_record(0),
            sample_record(1),
            sample_record(3),
            shared_record.clone(),
        ],
    };

    let meta1 = meta("a", None);

    store
        .fast_forward_inner_with_snapshot(grouped_snapshot, meta1.clone(), &catalog)
        .await
        .unwrap();

    assert_eq!(catalog.read().ordered_records.len(), 4);
    assert_eq!(catalog.read().sequence.get(), 0);
    assert_eq!(catalog.read().last_snapshot_meta, meta1);

    // 2 records and it's the second snapshot, so sequence 1.
    header.record_count = 2;
    header.sequence_number = 1;
    let normal_snapshot = CatalogFile {
        header,
        records: vec![
            shared_record,
            CreateDatabase {
                database_id: 1,
                database_name: "other_db".into(),
                retention_period: RetentionPeriod::Indefinite,
            }
            .make_record(4),
        ],
    };

    let meta2 = meta("2", "2");

    // make sure that it doesn't error when applying
    store
        .fast_forward_inner_with_snapshot(normal_snapshot, meta2.clone(), &catalog)
        .await
        .unwrap();

    // make sure that we don't double-apply the one that was present in the initial snapshot
    assert_eq!(catalog.read().ordered_records.len(), 5);
    assert_eq!(catalog.read().sequence.get(), 1);
    assert_eq!(catalog.read().last_snapshot_meta, meta2);
}

// TODO(june): fast-forwarding should correctly remove hard-deleted records when it encounters a
// hard-deletion record (once we figure out the exact semantics for that)
