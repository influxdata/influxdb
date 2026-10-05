use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutPayload, PutResult,
};
use uuid::Uuid;

use super::*;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::format::apply::serialize_log_file;
use crate::format::records::types::{
    ColumnDefinition, FieldFamilyMode, NodeMode, RetentionPeriod, TagColumn,
};
use crate::format::records::{AddColumns, RegisterNode, SoftDeleteTable, StopNode};
use crate::format::{FeatureLevel, MakeRecord};
use crate::object_store::versions::v3::ObjectStoreCatalog;

fn register_node(sequence: u64) -> Record {
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

fn create_db(sequence: u64, database_id: u32, name: &str) -> Record {
    CreateDatabase {
        database_id,
        database_name: name.to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .make_record(sequence)
}

fn create_table(sequence: u64, database_id: u32, table_id: u32, name: &str) -> Record {
    CreateTable {
        database_id,
        database_name: "db".to_string(),
        table_name: name.to_string(),
        table_id,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .make_record(sequence)
}

fn add_tag(sequence: u64, database_id: u32, table_id: u32, name: &str) -> Record {
    AddColumns {
        database_id,
        table_id,
        columns: vec![ColumnDefinition::Tag(TagColumn {
            id: 0,
            column_id: Some(0),
            name: name.to_string(),
        })],
        field_families: vec![],
    }
    .make_record(sequence)
}

/// The poisoning sequence: duplicate-name `CreateTable` for table 2, then
/// schema evolution addressed to the orphaned table 1.
fn poison_records() -> Vec<Record> {
    vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_table(3, 0, 2, "cpu"),
        add_tag(4, 0, 1, "host"),
    ]
}

/// Seed a store with a snapshot at sequence 0 and one log file per record.
async fn seed_store(records: Vec<Record>) -> (Arc<dyn ObjectStore>, ObjectStoreCatalog) {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = ObjectStoreCatalog::new("prefix", Arc::clone(&shared), StorageMode::default());
    let snapshot = serialize_snapshot_file(Uuid::nil(), 0, &[register_node(0)]);
    store.initialize_snapshot(snapshot).await.unwrap();
    for record in records {
        let seq = record.sequence();
        let log = serialize_log_file(Uuid::nil(), seq, &[record]);
        store
            .persist_log(CatalogSequenceNumber::new(seq), log)
            .await
            .unwrap();
    }
    (shared, store)
}

#[tokio::test]
async fn dry_run_reports_the_rename_and_writes_nothing() {
    let (shared, store) = seed_store(poison_records()).await;
    let before = store.load_snapshot().await.unwrap().unwrap();

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap();

    assert!(!outcome.executed);
    assert_eq!(outcome.plan.renames.len(), 1);
    let rename = &outcome.plan.renames[0];
    assert_eq!(rename.database_name, "db");
    assert_eq!(rename.table_name, "cpu");
    assert_eq!(rename.kept_table_id, 2);
    assert_eq!(rename.orphaned_table_id, 1);
    assert_eq!(rename.orphaned_table_name, "cpu-orphaned-1");
    assert_eq!(rename.duplicate_create_sequence, 3);
    assert_eq!(outcome.plan.snapshot_sequence, 4);

    let after = store.load_snapshot().await.unwrap().unwrap();
    assert_eq!(
        before.0.header.sequence_number,
        after.0.header.sequence_number
    );
    assert_eq!(before.1, after.1);
}

/// The un-repaired baseline: a persisted poisoned catalog must fail to
/// load with a clean error under the strict insert, never a panic.
#[tokio::test]
async fn unrepaired_poisoned_catalog_fails_load_cleanly() {
    let (_shared, store) = seed_store(poison_records()).await;
    let err = store.load_catalog().await.unwrap_err();
    assert!(err.to_string().contains("already exists"), "got: {err}");
}

#[tokio::test]
async fn execute_makes_the_catalog_loadable() {
    let (shared, store) = seed_store(poison_records()).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);

    // This load panicked in `Repository::id_exists` before the repair.
    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("cpu").unwrap().get(), 2);
    assert_eq!(db.tables.name_to_id("cpu-orphaned-1").unwrap().get(), 1);
    // The AddColumns addressed to the orphaned table applied to it.
    let orphan = db.tables.get_by_id(&TableId::new(1)).unwrap();
    assert!(orphan.tag_columns.contains_name("host"));
}

#[tokio::test]
async fn healthy_catalog_is_a_noop_even_with_execute() {
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        add_tag(3, 0, 1, "host"),
    ];
    let (shared, store) = seed_store(records).await;
    let before = store.load_snapshot().await.unwrap().unwrap();

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();

    assert!(!outcome.executed);
    assert!(outcome.plan.renames.is_empty());
    let after = store.load_snapshot().await.unwrap().unwrap();
    assert_eq!(before.1, after.1);
}

#[tokio::test]
async fn repair_is_idempotent() {
    let (shared, _store) = seed_store(poison_records()).await;

    let first = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(first.executed);

    let second = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(!second.executed);
    assert!(second.plan.renames.is_empty());
}

#[tokio::test]
async fn duplicate_inside_the_snapshot_is_repaired() {
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = ObjectStoreCatalog::new("prefix", Arc::clone(&shared), StorageMode::default());
    let mut records = vec![register_node(0)];
    records.extend(poison_records());
    let snapshot = serialize_snapshot_file(Uuid::nil(), 4, &records);
    store.initialize_snapshot(snapshot).await.unwrap();

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    assert_eq!(outcome.plan.renames.len(), 1);

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("cpu").unwrap().get(), 2);
    assert_eq!(db.tables.name_to_id("cpu-orphaned-1").unwrap().get(), 1);
}

#[tokio::test]
async fn logs_appended_after_the_repair_still_apply() {
    let (shared, store) = seed_store(poison_records()).await;
    repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();

    // An ingester keeps appending after the repaired snapshot.
    let log = serialize_log_file(Uuid::nil(), 5, &[add_tag(5, 0, 2, "region")]);
    store
        .persist_log(CatalogSequenceNumber::new(5), log)
        .await
        .unwrap();

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    let kept = db.tables.get_by_id(&TableId::new(2)).unwrap();
    assert!(kept.tag_columns.contains_name("region"));
}

#[tokio::test]
async fn three_way_duplicate_with_soft_delete_is_repaired() {
    // The third rename must rewrite the second table's CreateTable, which is
    // itself a duplicate record.
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_table(3, 0, 2, "cpu"),
        create_table(4, 0, 3, "cpu"),
        add_tag(5, 0, 1, "host"),
        add_tag(6, 0, 2, "region"),
        SoftDeleteTable {
            database_id: 0,
            table_id: 1,
            deletion_time_ns: 1_000_000_000,
            hard_deletion_time_ns: None,
            hard_delete_scope: None,
        }
        .make_record(7),
    ];
    let (shared, store) = seed_store(records).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    assert_eq!(outcome.plan.renames.len(), 2);
    assert_eq!(outcome.plan.renames[0].orphaned_table_id, 1);
    assert_eq!(outcome.plan.renames[1].orphaned_table_id, 2);

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("cpu").unwrap().get(), 3);
    assert_eq!(db.tables.name_to_id("cpu-orphaned-2").unwrap().get(), 2);
    let second = db.tables.get_by_id(&TableId::new(2)).unwrap();
    assert!(second.tag_columns.contains_name("region"));
    // The soft delete renamed the first orphan again off its repair name.
    let first = db.tables.get_by_id(&TableId::new(1)).unwrap();
    assert!(first.deleted);
    assert!(first.table_name.starts_with("cpu-orphaned-1-"));
    assert!(first.tag_columns.contains_name("host"));
}

#[tokio::test]
async fn execute_backs_up_the_replaced_snapshot() {
    let (shared, _store) = seed_store(poison_records()).await;
    let snapshot_path = CatalogFilePath::snapshot("prefix");
    let original = shared
        .get(&snapshot_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();

    let backup_path = outcome.backup_path.expect("backup path reported");
    assert_eq!(
        backup_path,
        "prefix/catalog/v3/repair/00000000000000000000.snapshot.backup"
    );
    let backup = shared
        .get(&Path::from(backup_path.as_str()))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(backup, original);
}

#[tokio::test]
async fn dry_run_writes_no_backup() {
    let (shared, _store) = seed_store(poison_records()).await;
    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.backup_path.is_none());
}

/// Delegating store whose first `head` of the snapshot path also overwrites
/// the snapshot — a checkpoint landing after the quiesce check's etag read
/// but before the conditional write.
#[derive(Debug)]
struct SwapAfterHead {
    inner: Arc<dyn ObjectStore>,
    snapshot_path: Path,
    swapped: AtomicBool,
}

impl std::fmt::Display for SwapAfterHead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SwapAfterHead({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SwapAfterHead {
    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let result = self.inner.head(location).await;
        if location == &self.snapshot_path && !self.swapped.swap(true, Ordering::SeqCst) {
            self.inner
                .put(
                    &self.snapshot_path,
                    bytes::Bytes::from_static(b"swapped").into(),
                )
                .await?;
        }
        result
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn concurrent_snapshot_change_aborts_the_write() {
    let (shared, _store) = seed_store(poison_records()).await;
    let racing: Arc<dyn ObjectStore> = Arc::new(SwapAfterHead {
        inner: shared,
        snapshot_path: CatalogFilePath::snapshot("prefix").as_ref().clone(),
        swapped: AtomicBool::new(false),
    });

    let err = repair_catalog(racing, "prefix", true, Duration::ZERO, true, &mut |_| {})
        .await
        .unwrap_err();
    assert!(matches!(err, RepairError::SnapshotChanged));
}

#[tokio::test]
async fn execute_on_a_local_filesystem_store_falls_back_to_plain_overwrite() {
    let dir = test_helpers::tmp_dir().unwrap();
    let shared: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let store = ObjectStoreCatalog::new("prefix", Arc::clone(&shared), StorageMode::default());
    let snapshot = serialize_snapshot_file(Uuid::nil(), 0, &[register_node(0)]);
    store.initialize_snapshot(snapshot).await.unwrap();
    for record in poison_records() {
        let seq = record.sequence();
        let log = serialize_log_file(Uuid::nil(), seq, &[record]);
        store
            .persist_log(CatalogSequenceNumber::new(seq), log)
            .await
            .unwrap();
    }

    let mut messages = Vec::new();
    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |m| messages.push(m),
    )
    .await
    .unwrap();

    assert!(outcome.executed);
    assert!(
        messages
            .iter()
            .any(|m| m.contains("plain, non-atomic overwrite"))
    );
    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("cpu").unwrap().get(), 2);
    assert_eq!(db.tables.name_to_id("cpu-orphaned-1").unwrap().get(), 1);
}

fn stop_node(sequence: u64) -> Record {
    StopNode {
        node_catalog_id: 1,
        node_id: "node-a".to_string(),
        stopped_time_ns: 2000,
        process_uuid: [0u8; 16],
    }
    .make_record(sequence)
}

#[tokio::test]
async fn quiesce_confirms_stopped_nodes() {
    let mut records = poison_records();
    records.push(stop_node(5));
    let (shared, _store) = seed_store(records).await;
    let mut messages = Vec::new();
    // No override needed: the registry confirms the only node stopped.
    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        false,
        &mut |m| messages.push(m),
    )
    .await
    .unwrap();

    assert!(outcome.executed);
    assert_eq!(outcome.plan.nodes.len(), 1);
    assert!(outcome.plan.nodes[0].confirmed_stopped);
    assert!(messages.iter().any(|m| m.contains("every node stopped")));
}

#[tokio::test]
async fn quiesce_warns_about_unstopped_nodes_and_states_the_window() {
    let (shared, _store) = seed_store(poison_records()).await;
    let mut messages = Vec::new();
    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |m| messages.push(m),
    )
    .await
    .unwrap();

    // No object-store activity, so the write still proceeds.
    assert!(outcome.executed);
    assert!(!outcome.plan.nodes[0].confirmed_stopped);
    let warning = messages
        .iter()
        .find(|m| m.contains("does not confirm these nodes as stopped"))
        .expect("warning emitted");
    assert!(warning.contains("node-a (running)"));
    assert!(warning.contains("0s"));
}

#[tokio::test]
async fn dry_run_skips_the_quiesce_check() {
    let (shared, _store) = seed_store(poison_records()).await;
    let mut messages = Vec::new();
    // A 60s window would time the test out if the dry run ran the check.
    repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::from_secs(60),
        false,
        &mut |m| messages.push(m),
    )
    .await
    .unwrap();
    assert!(messages.is_empty());
}

/// Delegating store that performs one injected `put` immediately before
/// returning the `nth` listing under `trigger_prefix` — landing the write
/// deterministically inside the quiesce window, load-independent.
#[derive(Debug)]
struct PutOnNthList {
    inner: Arc<dyn ObjectStore>,
    trigger_prefix: Path,
    nth: usize,
    count: std::sync::atomic::AtomicUsize,
    put_path: Path,
    put_bytes: bytes::Bytes,
}

impl PutOnNthList {
    fn listing(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        use futures::FutureExt;
        let fire = prefix.is_some_and(|p| p.as_ref().starts_with(self.trigger_prefix.as_ref()))
            && self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == self.nth;
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let offset = offset.cloned();
        let put = fire.then(|| (self.put_path.clone(), self.put_bytes.clone()));
        async move {
            if let Some((path, bytes)) = put {
                inner.put(&path, bytes.into()).await.expect("injected put");
            }
            match offset {
                Some(offset) => inner.list_with_offset(prefix.as_ref(), &offset),
                None => inner.list(prefix.as_ref()),
            }
        }
        .flatten_stream()
        .boxed()
    }
}

impl std::fmt::Display for PutOnNthList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PutOnNthList({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for PutOnNthList {
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.listing(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.listing(prefix, Some(offset))
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(location).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, bytes, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// The load phase lists the logs dir once; the probe's post-window listing
/// is the second, so `nth: 2` on the logs dir fires inside the window.
fn racing_store(
    shared: Arc<dyn ObjectStore>,
    put_path: Path,
    put_bytes: bytes::Bytes,
) -> Arc<dyn ObjectStore> {
    Arc::new(PutOnNthList {
        inner: shared,
        trigger_prefix: CatalogFilePath::logs_dir("prefix").as_ref().clone(),
        nth: 2,
        count: std::sync::atomic::AtomicUsize::new(0),
        put_path,
        put_bytes,
    })
}

#[tokio::test]
async fn catalog_log_during_quiesce_window_refuses_the_write() {
    let (shared, _store) = seed_store(poison_records()).await;
    let log = serialize_log_file(Uuid::nil(), 5, &[add_tag(5, 0, 2, "region")]);
    let racing = racing_store(
        Arc::clone(&shared),
        CatalogFilePath::log("prefix", CatalogSequenceNumber::new(5))
            .as_ref()
            .clone(),
        log,
    );

    let err = repair_catalog(racing, "prefix", true, Duration::ZERO, true, &mut |_| {})
        .await
        .unwrap_err();

    assert!(matches!(err, RepairError::ClusterActive { .. }));
    assert!(err.to_string().contains("catalog log"));
    // Refused before any write: no backup was taken.
    let mut backups = shared.list(Some(&Path::from("prefix/catalog/v3/repair")));
    assert!(backups.next().await.is_none());
}

#[tokio::test]
async fn wal_object_during_quiesce_window_refuses_the_write() {
    let (shared, _store) = seed_store(poison_records()).await;
    // node-a's WAL dir is listed once for the baseline and once after the
    // window; inject on the second.
    let racing: Arc<dyn ObjectStore> = Arc::new(PutOnNthList {
        inner: Arc::clone(&shared),
        trigger_prefix: Path::from("node-a/wal"),
        nth: 2,
        count: std::sync::atomic::AtomicUsize::new(0),
        put_path: Path::from("node-a/wal/00000000001.wal"),
        put_bytes: bytes::Bytes::from_static(b"wal"),
    });

    let err = repair_catalog(racing, "prefix", true, Duration::ZERO, true, &mut |_| {})
        .await
        .unwrap_err();

    assert!(matches!(err, RepairError::ClusterActive { .. }));
    assert!(err.to_string().contains("wal object"));
}

#[tokio::test]
async fn snapshot_rewrite_during_quiesce_window_refuses_the_write() {
    let (shared, _store) = seed_store(poison_records()).await;
    let racing = racing_store(
        Arc::clone(&shared),
        CatalogFilePath::snapshot("prefix").as_ref().clone(),
        bytes::Bytes::from_static(b"swapped"),
    );

    let err = repair_catalog(racing, "prefix", true, Duration::ZERO, true, &mut |_| {})
        .await
        .unwrap_err();

    assert!(matches!(err, RepairError::ClusterActive { .. }));
    assert!(err.to_string().contains("was rewritten"));
}

#[tokio::test]
async fn unstopped_nodes_refuse_execute_without_the_override() {
    let (shared, store) = seed_store(poison_records()).await;
    let before = store.load_snapshot().await.unwrap().unwrap();

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();

    assert!(matches!(err, RepairError::NodesNotStopped { .. }));
    assert!(err.to_string().contains("node-a (running)"));
    let after = store.load_snapshot().await.unwrap().unwrap();
    assert_eq!(before.1, after.1);
}

#[tokio::test]
async fn duplicate_table_id_is_superseded_by_the_later_create() {
    // Racing writers allocated table id 1 twice; the lenient insert let the
    // later definition replace the earlier, so the repair must too.
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "first"),
        create_table(3, 0, 1, "second"),
        add_tag(4, 0, 1, "host"),
    ];
    let (shared, store) = seed_store(records).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    assert!(outcome.plan.renames.is_empty());
    assert_eq!(outcome.plan.superseded.len(), 1);
    let s = &outcome.plan.superseded[0];
    assert_eq!(s.table_id, Some(1));
    assert_eq!(s.old_name, "first");
    assert_eq!(s.new_name, "second");
    assert_eq!(s.recreate_sequence, 3);

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("second").unwrap().get(), 1);
    assert!(db.tables.name_to_id("first").is_none());
    let table = db.tables.get_by_id(&TableId::new(1)).unwrap();
    assert!(table.tag_columns.contains_name("host"));
}

#[tokio::test]
async fn duplicate_database_id_is_superseded_by_the_later_create() {
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_db(3, 0, "db_two"),
        create_table(4, 0, 1, "mem"),
    ];
    let (shared, store) = seed_store(records).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    // The database supersede also clears its tables, so table id 1's
    // re-create needs no supersede of its own.
    assert_eq!(outcome.plan.superseded.len(), 1);
    let s = &outcome.plan.superseded[0];
    assert_eq!(s.table_id, None);
    assert_eq!(s.old_name, "db");
    assert_eq!(s.new_name, "db_two");

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    assert!(load.inner.databases.get_by_name("db").is_none());
    let db = load.inner.databases.get_by_name("db_two").expect("db_two");
    assert_eq!(db.tables.name_to_id("mem").unwrap().get(), 1);
    assert!(db.tables.name_to_id("cpu").is_none());
}

#[tokio::test]
async fn superseded_id_frees_its_name_for_a_later_create() {
    // From the field: id 5 created as "dc08", re-created as "dc21", then
    // "dc08" created fresh as id 6. The supersede frees the name, so no
    // rename is needed.
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 5, "dc08"),
        create_table(3, 0, 5, "dc21"),
        create_table(4, 0, 6, "dc08"),
    ];
    let (shared, store) = seed_store(records).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    assert!(outcome.plan.renames.is_empty());
    assert_eq!(outcome.plan.superseded.len(), 1);

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("dc21").unwrap().get(), 5);
    assert_eq!(db.tables.name_to_id("dc08").unwrap().get(), 6);
}

/// A checkpoint persists what survives the true-deletion clearing of
/// `ordered_records`; the re-created generation behind a synthetic
/// supersede must survive it.
#[cfg(feature = "true_deletion")]
#[tokio::test]
async fn true_deletion_keeps_superseded_recreates_across_checkpoints() {
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_db(3, 0, "db_two"),
        create_table(4, 0, 1, "mem"),
    ];
    let (shared, store) = seed_store(records).await;
    repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();

    let load = store.load_catalog().await.unwrap().expect("loads");
    let checkpoint = serialize_snapshot_file(Uuid::nil(), 99, &load.inner.ordered_records);
    store.update_snapshot(checkpoint).await.unwrap();

    let reload = store.load_catalog().await.unwrap().expect("reloads");
    let db = reload
        .inner
        .databases
        .get_by_name("db_two")
        .expect("recreated database survives the checkpoint");
    assert_eq!(db.tables.name_to_id("mem").unwrap().get(), 1);
}

#[tokio::test]
async fn same_id_same_name_create_is_refused() {
    // The lenient insert rejected an id reuse whose name was taken, so no
    // node applied the record; the repair must refuse rather than guess.
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_table(3, 0, 1, "cpu"),
    ];
    let (shared, _store) = seed_store(records).await;

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        RepairError::LenientlyRejectedCreate {
            resource: "table",
            id: 1,
            sequence: 3,
            ..
        }
    ));
}

#[tokio::test]
async fn reused_id_with_name_held_elsewhere_is_refused() {
    // id 5 live as "a", id 7 holds "b"; a create of id 5 as "b" was
    // rejected by the lenient insert (name taken under any id).
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 5, "a"),
        create_table(3, 0, 7, "b"),
        create_table(4, 0, 5, "b"),
    ];
    let (shared, _store) = seed_store(records).await;

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        RepairError::LenientlyRejectedCreate {
            resource: "table",
            id: 5,
            sequence: 4,
            ..
        }
    ));
}

#[tokio::test]
async fn same_id_same_name_database_create_is_refused() {
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        create_db(3, 0, "db"),
    ];
    let (shared, _store) = seed_store(records).await;

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        RepairError::LenientlyRejectedCreate {
            resource: "database",
            id: 0,
            sequence: 3,
            ..
        }
    ));
}

#[tokio::test]
async fn cascaded_rename_onto_an_orphan_format_name_verifies() {
    // A user table already holding the orphan-format name forces the same
    // orphan to be renamed twice; verification must track the final name.
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "a"),
        create_table(3, 0, 2, "a"),
        create_table(4, 0, 3, "a-orphaned-1"),
    ];
    let (shared, store) = seed_store(records).await;

    let outcome = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();
    assert!(outcome.executed);
    assert_eq!(outcome.plan.renames.len(), 2);

    let load = store.load_catalog().await.unwrap().expect("catalog loads");
    let db = load.inner.databases.get_by_name("db").expect("db present");
    assert_eq!(db.tables.name_to_id("a").unwrap().get(), 2);
    assert_eq!(db.tables.name_to_id("a-orphaned-1").unwrap().get(), 3);
    assert_eq!(
        db.tables
            .name_to_id("a-orphaned-1-orphaned-1")
            .unwrap()
            .get(),
        1
    );
}

#[tokio::test]
async fn log_sequence_gap_is_refused() {
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "cpu"),
        add_tag(4, 0, 1, "host"),
    ];
    let (shared, _store) = seed_store(records).await;

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(err, RepairError::LogSequenceGap { found: 4, .. }));
}

#[tokio::test]
async fn data_only_scoped_supersede_is_refused() {
    use crate::format::records::types::DeletionScope as WireDeletionScope;
    let records = vec![
        create_db(1, 0, "db"),
        create_table(2, 0, 1, "a"),
        SoftDeleteTable {
            database_id: 0,
            table_id: 1,
            deletion_time_ns: 1_000,
            hard_deletion_time_ns: None,
            hard_delete_scope: Some(WireDeletionScope::DataOnlyKeepResources),
        }
        .make_record(3),
        create_table(4, 0, 1, "b"),
    ];
    let (shared, _store) = seed_store(records).await;

    let err = repair_catalog(
        Arc::clone(&shared),
        "prefix",
        false,
        Duration::ZERO,
        false,
        &mut |_| {},
    )
    .await
    .unwrap_err();
    assert!(matches!(
        err,
        RepairError::SupersededHasDataOnlyScope {
            resource: "table",
            id: 1
        }
    ));
}

/// Table ids are per-database: clearing db 0's superseded table 1 must not
/// touch db 1's table 1 across a checkpoint.
#[cfg(feature = "true_deletion")]
#[tokio::test]
async fn true_deletion_table_clearing_is_database_scoped() {
    let records = vec![
        create_db(1, 0, "db"),
        create_db(2, 1, "other"),
        create_table(3, 0, 1, "first"),
        create_table(4, 1, 1, "keeper"),
        add_tag(5, 1, 1, "host"),
        create_table(6, 0, 1, "second"),
    ];
    let (shared, store) = seed_store(records).await;
    repair_catalog(
        Arc::clone(&shared),
        "prefix",
        true,
        Duration::ZERO,
        true,
        &mut |_| {},
    )
    .await
    .unwrap();

    let load = store.load_catalog().await.unwrap().expect("loads");
    let checkpoint = serialize_snapshot_file(Uuid::nil(), 99, &load.inner.ordered_records);
    store.update_snapshot(checkpoint).await.unwrap();

    let reload = store.load_catalog().await.unwrap().expect("reloads");
    let other = reload.inner.databases.get_by_name("other").expect("other");
    let keeper = other.tables.get_by_id(&TableId::new(1)).unwrap();
    assert_eq!(keeper.table_name.as_ref(), "keeper");
    assert!(keeper.tag_columns.contains_name("host"));
}

#[tokio::test]
async fn restore_records_are_refused() {
    let err = plan_repair(
        Arc::from("prefix"),
        Uuid::nil(),
        &[Record::new(
            record_ids::RESTORE_CATALOG.raw(),
            crate::format::RecordFlags::none(),
            1,
            bytes::Bytes::new(),
        )],
    )
    .unwrap_err();
    assert!(matches!(err, RepairError::RestoreUnsupported));
}
