use std::sync::Arc;

use influxdb3_catalog::catalog::CatalogSequenceNumber;
use influxdb3_catalog::format::MakeRecord;
use influxdb3_catalog::format::apply::serialize_log_file;
use influxdb3_catalog::format::apply::serialize_snapshot_file;
use influxdb3_catalog::format::records::CreateDatabase;
use influxdb3_catalog::format::records::types::RetentionPeriod;
use influxdb3_catalog::object_store::CatalogFilePath;
use object_store::{ObjectStore, memory::InMemory, path::Path as ObjPath};
use uuid::Uuid;

use super::{CatalogFormat, CatalogStore};

const PREFIX: &str = "test-cluster";

fn create_db_record(seq: u64, id: u32, name: &str) -> influxdb3_catalog::format::Record {
    CreateDatabase {
        database_id: id,
        database_name: name.to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .make_record(seq)
}

async fn store_with_one_log() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    let record = create_db_record(1, 1, "alpha");
    let bytes = serialize_log_file(Uuid::nil(), 1, std::slice::from_ref(&record));
    let path: ObjPath = CatalogFilePath::log(PREFIX, CatalogSequenceNumber::new(1)).into();
    store.put(&path, bytes.to_vec().into()).await.unwrap();
    store
}

#[tokio::test]
async fn list_reports_one_log() {
    let store = store_with_one_log().await;
    let cs = CatalogStore::new(store, PREFIX);
    let entries = cs.list(false).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].sequence, Some(1));
}

#[tokio::test]
async fn list_render_json_contains_log() {
    let store = store_with_one_log().await;
    let out =
        super::list::render_listing(CatalogStore::new(store, PREFIX), CatalogFormat::Json, false)
            .await
            .unwrap();
    assert!(out.contains("\"sequence\": 1"));
}

/// Store with a snapshot at sequence 5, a pre-snapshot log (seq 3), and a
/// post-snapshot log (seq 7).
async fn store_with_snapshot_and_surrounding_logs() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());

    let records = vec![create_db_record(5, 1, "alpha")];
    let snapshot = serialize_snapshot_file(Uuid::nil(), 5, &records);
    let snapshot_path: ObjPath = CatalogFilePath::snapshot(PREFIX).into();
    store
        .put(&snapshot_path, snapshot.to_vec().into())
        .await
        .unwrap();

    for seq in [3u64, 7u64] {
        let record = create_db_record(seq, seq as u32, "db");
        let bytes = serialize_log_file(Uuid::nil(), seq, std::slice::from_ref(&record));
        let path: ObjPath = CatalogFilePath::log(PREFIX, CatalogSequenceNumber::new(seq)).into();
        store.put(&path, bytes.to_vec().into()).await.unwrap();
    }

    store
}

#[tokio::test]
async fn list_default_excludes_pre_snapshot_logs() {
    let store = store_with_snapshot_and_surrounding_logs().await;
    let cs = CatalogStore::new(store, PREFIX);

    // default: snapshot(seq 5) + log 7, NOT log 3
    let default = cs.list(false).await.unwrap();
    let seqs: Vec<Option<u64>> = default.iter().map(|e| e.sequence).collect();
    assert!(seqs.contains(&Some(7)));
    assert!(!seqs.contains(&Some(3)));
    // snapshot row now carries its sequence
    assert!(
        default
            .iter()
            .any(|e| e.kind == super::CatalogFileKind::Snapshot && e.sequence == Some(5))
    );

    // --all: includes the pre-snapshot log 3 too
    let all = cs.list(true).await.unwrap();
    assert!(all.iter().any(|e| e.sequence == Some(3)));
}

async fn store_with_snapshot() -> Arc<dyn ObjectStore> {
    let store = Arc::new(InMemory::new());
    let records = vec![
        create_db_record(1, 1, "alpha"),
        create_db_record(2, 2, "beta"),
    ];
    let bytes = serialize_snapshot_file(Uuid::nil(), 2, &records);
    let path: ObjPath = CatalogFilePath::snapshot(PREFIX).into();
    store.put(&path, bytes.to_vec().into()).await.unwrap();
    store
}

#[tokio::test]
async fn snapshot_summary_has_histogram() {
    let store = store_with_snapshot().await;
    let out = super::snapshot::render_snapshot(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Json,
        super::snapshot::Render {
            full: false,
            skip_crc: false,
        },
    )
    .await
    .unwrap();
    assert!(out.contains("CreateDatabase"));
    assert!(out.contains("\"snapshot\": true"));
}

#[tokio::test]
async fn snapshot_full_decodes_records() {
    let store = store_with_snapshot().await;
    let out = super::snapshot::render_snapshot(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Json,
        super::snapshot::Render {
            full: true,
            skip_crc: false,
        },
    )
    .await
    .unwrap();
    assert!(
        out.contains("\"records\""),
        "expected decoded output: {out}"
    );
    assert!(out.contains("alpha"));
    assert!(out.contains("beta"));
}

#[tokio::test]
async fn sequence_defaults_to_latest_and_decodes() {
    let store = store_with_one_log().await;
    let out = super::sequence::render_sequence(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Json,
        super::sequence::Render {
            sequence: None,
            headers_only: false,
            skip_crc: false,
        },
    )
    .await
    .unwrap();
    assert!(out.contains("CreateDatabase"));
    assert!(out.contains("alpha"));
}

#[tokio::test]
async fn sequence_headers_only_json_has_histogram() {
    let store = store_with_one_log().await;
    let out = super::sequence::render_sequence(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Json,
        super::sequence::Render {
            sequence: None,
            headers_only: true,
            skip_crc: false,
        },
    )
    .await
    .unwrap();
    assert!(out.contains("\"histogram\""));
    assert!(out.contains("CreateDatabase"));
    // Per-record headers are not rendered.
    assert!(!out.contains("record_headers"));
    assert!(!out.contains("upgrade_safe"));
}

#[tokio::test]
async fn sequence_headers_only_pretty_renders_histogram() {
    let store = store_with_one_log().await;
    let out = super::sequence::render_sequence(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Pretty,
        super::sequence::Render {
            sequence: None,
            headers_only: true,
            skip_crc: false,
        },
    )
    .await
    .unwrap();
    assert!(out.contains("Record types:"));
    assert!(out.contains("CreateDatabase"));
    // Per-record headers are not rendered.
    assert!(!out.contains("upgrade_safe"));
}

#[tokio::test]
async fn sequence_missing_log_errors() {
    let store = Arc::new(InMemory::new());
    let res = super::sequence::render_sequence(
        CatalogStore::new(store, PREFIX),
        CatalogFormat::Json,
        super::sequence::Render {
            sequence: None,
            headers_only: false,
            skip_crc: false,
        },
    )
    .await;
    assert!(res.is_err());
}
