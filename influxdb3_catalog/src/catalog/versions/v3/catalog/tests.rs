use std::{num::NonZeroU32, sync::Arc, time::Duration};

use iox_time::{MockProvider, Time, TimeProvider};
use metric::Registry;
use object_store::memory::InMemory;
use uuid::Uuid;

use crate::Repository;
use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::v3::catalog::{
    Catalog, CatalogArgs, CheckpointPolicy, HardDeletionTime,
};
use crate::catalog::versions::v3::deletes::DeletionScope;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::node::{RegisterNodeArgs, RegisterNodeOp};
use crate::catalog::versions::v3::schema::column::FieldDataType;
use crate::catalog::versions::v3::schema::node::NodeMode;
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::catalog::versions::v3::transaction::Prompt;
use crate::catalog::versions::v3::usage::CatalogLimits;
use crate::format::records::types::StorageMode as WireStorageMode;
use crate::format::records::{SetGenerationDuration, SetStorageMode};
use crate::object_store::versions::v3::ObjectStoreCatalog;
use crate::resource::CatalogResource;
use influxdb3_id::CatalogId;

fn test_time_provider() -> Arc<dyn TimeProvider> {
    Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)))
}

fn test_store_with_prefix(prefix: &str) -> ObjectStoreCatalog {
    ObjectStoreCatalog::new(prefix, Arc::new(InMemory::new()), StorageMode::default())
}

async fn test_load_or_create(
    prefix: &str,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<Catalog, crate::CatalogError> {
    Catalog::load_or_create(
        prefix,
        None,
        store,
        test_time_provider(),
        Arc::new(Registry::new()),
        Arc::new(CatalogLimits::none()),
        CatalogArgs::default(),
    )
    .await
}

fn test_catalog_with_store(store: ObjectStoreCatalog) -> Catalog {
    test_catalog_with_store_and_policy(store, CheckpointPolicy::default())
}

fn test_catalog_with_store_and_policy(
    store: ObjectStoreCatalog,
    policy: CheckpointPolicy,
) -> Catalog {
    let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    Catalog::from_parts(
        inner,
        store,
        None,
        test_time_provider(),
        Arc::new(Registry::new()),
        NonZeroU32::MIN,
        Catalog::DEFAULT_HARD_DELETE_DURATION,
        policy,
        Arc::new(CatalogLimits::none()),
    )
}

fn test_catalog_with_current_node(prefix: &str, current_node_id: &str) -> Catalog {
    let store = test_store_with_prefix(prefix);
    let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    Catalog::from_parts(
        inner,
        store,
        Some(Arc::from(current_node_id)),
        test_time_provider(),
        Arc::new(Registry::new()),
        NonZeroU32::MIN,
        Catalog::DEFAULT_HARD_DELETE_DURATION,
        CheckpointPolicy::default(),
        Arc::new(CatalogLimits::none()),
    )
}

async fn soft_delete_table_now(catalog: &Catalog, db: &str, table: &str) {
    catalog
        .soft_delete_table(
            db,
            table,
            HardDeletionTime::Now,
            DeletionScope::DataAndCatalog,
        )
        .await
        .unwrap();
}

async fn soft_delete_db_now(catalog: &Catalog, db: &str) {
    catalog
        .soft_delete_database(db, HardDeletionTime::Now, DeletionScope::DataAndCatalog)
        .await
        .unwrap();
}

fn assert_both_ids_live_and_distinct<I, R>(repo: &Repository<I, R>, first: I, second: I)
where
    I: CatalogId,
    R: CatalogResource,
{
    let first_name = repo.id_to_name(&first);
    let second_name = repo.id_to_name(&second);
    assert!(
        first_name.is_some(),
        "first soft-deleted id was evicted from id_name_map"
    );
    assert!(
        second_name.is_some(),
        "second soft-deleted id was evicted from id_name_map"
    );
    assert_ne!(
        first_name, second_name,
        "soft-deleted names collided into a single id_name_map entry"
    );
    assert!(
        repo.get_by_id(&first).is_some(),
        "first id missing from repo"
    );
    assert!(
        repo.get_by_id(&second).is_some(),
        "second id missing from repo"
    );
}

fn register_args(node_id: &str) -> RegisterNodeArgs {
    RegisterNodeArgs {
        node_id: Arc::from(node_id),
        core_count: 4,
        mode: vec![NodeMode::Core],
        process_uuid: Uuid::nil(),
        instance_id: Arc::from("inst-1"),
        conn_info: None,
        cli_params: None,
        registered_time: Time::from_timestamp_nanos(1000),
        row_delete_predicate_version: 0,
    }
}

/// Shared test fixture that owns an in-memory object store.
///
/// - `catalog()` returns a fresh `Catalog` backed by the same store — useful
///   for simulating concurrent writers.
/// - `reload()` loads from persisted state, simulating a node restart.
struct TestCatalog {
    shared: Arc<dyn object_store::ObjectStore>,
}

impl TestCatalog {
    fn new() -> Self {
        Self {
            shared: Arc::new(InMemory::new()),
        }
    }

    fn store(&self) -> Arc<dyn object_store::ObjectStore> {
        Arc::clone(&self.shared)
    }

    /// Return a fresh Catalog backed by the shared store.
    fn catalog(&self) -> Catalog {
        let store = ObjectStoreCatalog::new("p", Arc::clone(&self.shared), StorageMode::default());
        test_catalog_with_store(store)
    }

    /// Simulate a node restart by loading a new Catalog from the shared store.
    async fn reload(&self) -> Catalog {
        test_load_or_create("p", Arc::clone(&self.shared))
            .await
            .unwrap()
    }

    /// Return a fresh Catalog backed by the shared store with automatic
    /// checkpointing suppressed. Use this in tests that call
    /// `force_checkpoint()` manually to control exactly when a snapshot lands.
    fn catalog_no_checkpoint(&self) -> Catalog {
        let store = ObjectStoreCatalog::new("p", Arc::clone(&self.shared), StorageMode::default());
        test_catalog_with_store_and_policy(
            store,
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: std::time::Duration::from_secs(3600),
            },
        )
    }
}

#[tokio::test]
async fn update_persists_applies_and_advances_sequence() {
    let catalog = TestCatalog::new().catalog();
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));

    let node = catalog
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();

    assert_eq!(node.node_id.as_ref(), "node-a");
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(1));
}

#[tokio::test]
async fn update_broadcasts_to_subscribers() {
    let catalog = TestCatalog::new().catalog();
    let mut rx = catalog.subscribe_to_updates("test-subscriber").await;

    let catalog = Arc::new(catalog);
    let update_task = {
        let catalog = Arc::clone(&catalog);
        tokio::spawn(async move {
            catalog
                .update::<RegisterNodeOp>(register_args("node-a"))
                .await
        })
    };

    let received = rx.recv().await.expect("should receive update");
    assert_eq!(received.events().count(), 1);
    drop(received);

    update_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn update_retries_on_already_exists() {
    // Two Catalog instances sharing the same underlying store: simulates two
    // writers racing on the same catalog. The second writer should hit
    // AlreadyExists, load the winning log, and retry on the next sequence.
    let test_catalog = TestCatalog::new();
    let cat_a = test_catalog.catalog();
    let cat_b = test_catalog.catalog();

    cat_a
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();
    // cat_b doesn't know about cat_a's write yet; when it tries to persist at
    // sequence 1, it will hit AlreadyExists, catch up, and then persist at 2.
    cat_b
        .update::<RegisterNodeOp>(register_args("node-b"))
        .await
        .unwrap();

    assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(2));
    let cat_b_inner = cat_b.inner.read();
    assert!(cat_b_inner.nodes.contains_name("node-a"));
    assert!(cat_b_inner.nodes.contains_name("node-b"));
}

#[tokio::test]
async fn catch_up_broadcasts_events_from_other_writer() {
    let test_catalog = TestCatalog::new();
    let cat_a = test_catalog.catalog();
    let cat_b = test_catalog.catalog();
    let mut rx = cat_b.subscribe_to_updates("test-subscriber").await;

    cat_a
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();

    let update_task = tokio::spawn(async move {
        cat_b
            .update::<RegisterNodeOp>(register_args("node-b"))
            .await
    });

    // First update on cat_b's subscriber comes from catch-up of cat_a's write.
    let caught_up = rx.recv().await.expect("should receive caught-up update");
    assert_eq!(caught_up.events().count(), 1);
    drop(caught_up);

    // Second update is cat_b's own write after the retry.
    let own = rx.recv().await.expect("should receive own update");
    assert_eq!(own.events().count(), 1);
    drop(own);

    update_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn transaction_commit_persists_records_and_advances_sequence() {
    let catalog = TestCatalog::new().catalog();
    let mut txn = catalog.begin_transaction();
    assert!(txn.is_empty());
    txn.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    let result = catalog.commit_transaction(txn).await.unwrap();
    let Prompt::Success(seq) = result else {
        panic!("expected Success, got {result:?}")
    };

    assert_eq!(seq, CatalogSequenceNumber::new(1));
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(1));
    assert_eq!(catalog.inner.read().storage_mode, StorageMode::PachaTree);
}

#[tokio::test]
async fn empty_transaction_commit_is_no_op() {
    let catalog = TestCatalog::new().catalog();
    let txn = catalog.begin_transaction();

    let result = catalog.commit_transaction(txn).await.unwrap();
    let Prompt::Success(seq) = result else {
        panic!("expected Success, got {result:?}")
    };
    assert_eq!(seq, CatalogSequenceNumber::new(0));
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));
}

#[tokio::test]
async fn transaction_retries_when_catalog_advanced_after_begin() {
    let catalog = TestCatalog::new().catalog();

    let mut txn = catalog.begin_transaction();
    txn.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    // Another writer (same Catalog, different op path) advances the catalog
    // between begin_transaction() and commit().
    catalog
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();

    let result = catalog.commit_transaction(txn).await.unwrap();
    assert!(
        matches!(result, Prompt::Retry(_)),
        "expected Retry once catalog advanced; got {result:?}"
    );

    // The transaction's records were dropped, not applied: storage_mode is
    // still the default and the racing writer's node is present.
    let inner = catalog.inner.read();
    assert_eq!(inner.storage_mode, StorageMode::default());
    assert!(inner.nodes.contains_name("node-a"));
}

#[tokio::test]
async fn transaction_retries_when_another_writer_takes_the_next_sequence() {
    // Two Catalog instances sharing the same underlying store.
    let test_catalog = TestCatalog::new();
    let cat_a = test_catalog.catalog();
    let cat_b = test_catalog.catalog();

    // Both begin at sequence 0 (each has its own in-memory state; the store is shared).
    let mut txn_a = cat_a.begin_transaction();
    let mut txn_b = cat_b.begin_transaction();
    txn_a.push(&SetStorageMode {
        mode: WireStorageMode::Parquet,
    });
    txn_b.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    // cat_a commits first, claiming sequence 1.
    let a_result = cat_a.commit_transaction(txn_a).await.unwrap();
    assert!(matches!(a_result, Prompt::Success(_)));

    // cat_b's commit hits AlreadyExists at sequence 1 and returns Retry.
    let b_result = cat_b.commit_transaction(txn_b).await.unwrap();
    assert!(
        matches!(b_result, Prompt::Retry(_)),
        "expected Retry on concurrent commit; got {b_result:?}"
    );
    // cat_b has caught up to sequence 1 (cat_a's write was loaded); its own
    // txn was dropped, so storage_mode reflects cat_a's value.
    assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(1));
    assert_eq!(cat_b.inner.read().storage_mode, StorageMode::Parquet);
}

#[tokio::test]
async fn transaction_commit_applies_heterogeneous_record_types() {
    // `push<R: CatalogRecord>` is a method generic, not a struct generic, so
    // each call site monomorphizes independently and a single transaction can
    // mix arbitrary record types.
    let catalog = TestCatalog::new().catalog();
    let mut txn = catalog.begin_transaction();
    txn.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });
    txn.push(&SetGenerationDuration {
        level: 0,
        duration_ns: 60_000_000_000,
    });

    let result = catalog.commit_transaction(txn).await.unwrap();
    assert!(matches!(result, Prompt::Success(_)));

    let inner = catalog.inner.read();
    assert_eq!(inner.storage_mode, StorageMode::PachaTree);
    assert_eq!(
        inner.generation_config.duration_for_level(0),
        Some(std::time::Duration::from_nanos(60_000_000_000)),
    );
}

#[tokio::test]
async fn transaction_concurrent_commits_one_succeeds_other_retries() {
    let catalog = Arc::new(TestCatalog::new().catalog());

    let mut txn_a = catalog.begin_transaction();
    let mut txn_b = catalog.begin_transaction();
    txn_a.push(&SetStorageMode {
        mode: WireStorageMode::Parquet,
    });
    txn_b.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    let cat_a = Arc::clone(&catalog);
    let cat_b = Arc::clone(&catalog);
    let (a, b) = tokio::join!(
        tokio::spawn(async move { cat_a.commit_transaction(txn_a).await.unwrap() }),
        tokio::spawn(async move { cat_b.commit_transaction(txn_b).await.unwrap() }),
    );
    let outcomes = [a.unwrap(), b.unwrap()];

    let successes = outcomes
        .iter()
        .filter(|p| matches!(p, Prompt::Success(_)))
        .count();
    let retries = outcomes
        .iter()
        .filter(|p| matches!(p, Prompt::Retry(_)))
        .count();
    assert_eq!(successes, 1, "exactly one commit should succeed");
    assert_eq!(retries, 1, "the other commit should retry");
    assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(1));
}

#[tokio::test]
async fn commit_retry_catches_up_and_broadcasts_other_writers_events() {
    let test_catalog = TestCatalog::new();
    let cat_a = test_catalog.catalog();
    let cat_b = test_catalog.catalog();
    let mut rx = cat_b.subscribe_to_updates("test-subscriber").await;

    let mut txn_b = cat_b.begin_transaction();
    txn_b.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    cat_a
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();

    // Broadcast inside catch_up_from awaits a per-subscriber ACK, which only
    // fires once the receiver drops the message. Drive commit on a spawned
    // task so the test thread can recv concurrently.
    let cat_b = Arc::new(cat_b);
    let commit_task = {
        let cat_b = Arc::clone(&cat_b);
        tokio::spawn(async move { cat_b.commit_transaction(txn_b).await.unwrap() })
    };

    let caught_up = rx.recv().await.expect("should receive caught-up update");
    assert_eq!(caught_up.events().count(), 1);
    drop(caught_up);

    let result = commit_task.await.unwrap();
    assert!(
        matches!(result, Prompt::Retry(_)),
        "expected Retry; got {result:?}"
    );
}

#[tokio::test]
async fn transaction_pushes_after_side_channel_advance_still_retry() {
    let catalog = TestCatalog::new().catalog();

    let mut txn = catalog.begin_transaction();
    txn.push(&SetStorageMode {
        mode: WireStorageMode::PachaTree,
    });

    // Side-channel advance between two pushes into the same transaction.
    catalog
        .update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();

    txn.push(&SetStorageMode {
        mode: WireStorageMode::ParquetAndPachaTree,
    });

    let result = catalog.commit_transaction(txn).await.unwrap();
    assert!(
        matches!(result, Prompt::Retry(_)),
        "expected Retry once catalog advanced; got {result:?}"
    );
    // Neither push reached the catalog.
    assert_eq!(catalog.inner.read().storage_mode, StorageMode::default());
}

#[tokio::test]
async fn load_or_create_round_trip() {
    // Boot a fresh catalog, perform some writes, then reboot a second catalog
    // against the same object store. The second catalog should reconstruct
    // identical state from the snapshot + logs on disk, including the
    // configured storage mode.
    let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cat1 = Catalog::load_or_create(
        "p",
        None,
        Arc::clone(&shared),
        test_time_provider(),
        Arc::new(Registry::new()),
        Arc::new(CatalogLimits::none()),
        CatalogArgs {
            storage_mode: StorageMode::PachaTree,
            ..CatalogArgs::default()
        },
    )
    .await
    .unwrap();
    let uuid = cat1.catalog_uuid();

    cat1.update::<RegisterNodeOp>(register_args("node-a"))
        .await
        .unwrap();
    cat1.update::<RegisterNodeOp>(register_args("node-b"))
        .await
        .unwrap();

    let cat2 = test_load_or_create("p", shared).await.unwrap();
    assert_eq!(cat2.catalog_uuid(), uuid);
    // Seq 1 is the auto-created `_internal` db; nodes a + b take 2 and 3.
    assert_eq!(cat2.sequence_number(), CatalogSequenceNumber::new(3));
    // The storage mode persisted by `cat1` survives the reload, regardless
    // of the mode passed to `cat2`.
    assert_eq!(cat2.inner.read().storage_mode, StorageMode::PachaTree);
}

// ---------------------------------------------------------------------------
// DatabaseCatalogTransaction tests
// ---------------------------------------------------------------------------

mod database_transaction {
    use super::{CatalogSequenceNumber, TestCatalog};
    use crate::catalog::versions::v3::NUM_FIELDS_PER_FAMILY_LIMIT;
    use crate::catalog::versions::v3::schema::column::ColumnDefinition;
    use crate::catalog::versions::v3::transaction::Prompt;
    use crate::{CatalogError, catalog::CreateTableOptions};
    use schema::{InfluxColumnType, InfluxFieldType};

    #[tokio::test]
    async fn begin_auto_creates_missing_database() {
        let catalog = TestCatalog::new().catalog();

        let txn = catalog.begin_database_transaction("foo").unwrap();
        assert_eq!(txn.db_schema().name.as_ref(), "foo");
        assert!(!txn.is_empty(), "CreateDatabase record should be staged");

        // finalize handled by commit
        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(_) = result else {
            panic!("expected Success");
        };

        let inner_after = catalog.begin_database_transaction("foo").unwrap();
        assert_eq!(inner_after.db_schema().name.as_ref(), "foo");
        assert!(
            inner_after.is_empty(),
            "begin on existing database should not stage CreateDatabase",
        );
    }

    #[tokio::test]
    async fn begin_on_existing_database_does_not_stage_create() {
        let catalog = TestCatalog::new().catalog();
        assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));

        // Create the database via a first commit.
        let txn = catalog.begin_database_transaction("foo").unwrap();
        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(seq) = result else {
            panic!("expected success");
        };
        let expected_seq = CatalogSequenceNumber::new(1);
        assert_eq!(seq, expected_seq);

        // Re-open: should be a no-op transaction.
        let txn = catalog.begin_database_transaction("foo").unwrap();
        assert!(txn.is_empty());
        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(seq) = result else {
            panic!("expected Success");
        };
        // Sequence unchanged because the empty txn is a no-op.
        assert_eq!(seq, expected_seq);
    }

    #[tokio::test]
    async fn table_or_create_returns_same_id_for_repeat_calls() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();

        let id_a = txn.table_or_create("t1").unwrap();
        let id_b = txn.table_or_create("t1").unwrap();
        assert_eq!(id_a, id_b);

        let id_c = txn.table_or_create("t2").unwrap();
        assert_ne!(id_a, id_c);
    }

    #[tokio::test]
    async fn column_or_create_for_each_column_kind() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();

        let time = txn
            .column_or_create("t1", "time", InfluxColumnType::Timestamp)
            .unwrap();
        assert!(matches!(time, ColumnDefinition::Timestamp(_)));

        let tag = txn
            .column_or_create("t1", "host", InfluxColumnType::Tag)
            .unwrap();
        assert!(matches!(tag, ColumnDefinition::Tag(_)));

        let field = txn
            .column_or_create(
                "t1",
                "value",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
        assert!(matches!(field, ColumnDefinition::Field(_)));

        // Repeat calls return the same column instance.
        let field_again = txn
            .column_or_create(
                "t1",
                "value",
                InfluxColumnType::Field(InfluxFieldType::Float),
            )
            .unwrap();
        assert_eq!(field, field_again);
    }

    #[tokio::test]
    async fn column_type_conflict_returns_invalid_column_type() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();

        txn.column_or_create("t1", "host", InfluxColumnType::Tag)
            .unwrap();

        let err = txn
            .column_or_create(
                "t1",
                "host",
                InfluxColumnType::Field(InfluxFieldType::String),
            )
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidColumnType { .. }),
            "expected InvalidColumnType, got {err:?}",
        );
    }

    #[tokio::test]
    async fn empty_transaction_commit_is_a_no_op() {
        let catalog = TestCatalog::new().catalog();

        // Begin against an existing database so no CreateDatabase is staged.
        catalog
            .commit(catalog.begin_database_transaction("foo").unwrap())
            .await
            .unwrap();
        let seq_after_create = catalog.sequence_number();

        let txn = catalog.begin_database_transaction("foo").unwrap();
        assert!(txn.is_empty());

        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(seq) = result else {
            panic!("expected Success");
        };
        assert_eq!(seq, seq_after_create);
        assert_eq!(catalog.sequence_number(), seq_after_create);
    }

    #[tokio::test]
    async fn end_to_end_begin_add_finalize_commit_applies_records() {
        let catalog = TestCatalog::new().catalog();

        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        txn.column_or_create("t1", "time", InfluxColumnType::Timestamp)
            .unwrap();
        txn.column_or_create("t1", "host", InfluxColumnType::Tag)
            .unwrap();
        txn.column_or_create(
            "t1",
            "value",
            InfluxColumnType::Field(InfluxFieldType::Float),
        )
        .unwrap();

        // Add a second table.
        txn.column_or_create("t2", "time", InfluxColumnType::Timestamp)
            .unwrap();

        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(_) = result else {
            panic!("expected Success");
        };

        // Begin a fresh transaction to inspect the resulting state.
        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let db = snapshot.db_schema();
        let t1 = db.table_definition("t1").expect("t1 exists");
        assert!(t1.column_definition("time").is_some());
        assert!(t1.column_definition("host").is_some());
        assert!(t1.column_definition("value").is_some());
        assert_eq!(t1.num_columns(), 3);

        let t2 = db.table_definition("t2").expect("t2 exists");
        assert_eq!(t2.num_columns(), 1);
    }

    // -----------------------------------------------------------------------
    // Field family handling (Aware mode)
    // -----------------------------------------------------------------------

    use crate::catalog::versions::v3::schema::column::{FieldFamilyMode, FieldFamilyName};

    #[tokio::test]
    async fn aware_mode_routes_qualified_field_to_named_family() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware);
        let table_id = txn.create_table_with_opts("t", opts).unwrap();
        let tx = txn.table_tx_or_create("t").unwrap();
        assert_eq!(tx.table_id(), table_id);

        tx.field_or_create("cpu::user", InfluxFieldType::Float)
            .unwrap();
        tx.field_or_create("cpu::system", InfluxFieldType::Float)
            .unwrap();
        tx.field_or_create("mem::used", InfluxFieldType::UInteger)
            .unwrap();

        let result = catalog.commit(txn).await.unwrap();
        assert!(matches!(result, Prompt::Success(_)));

        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let table = snapshot.db_schema().table_definition("t").unwrap();
        let cpu = table
            .field_families
            .get_by_name("cpu")
            .expect("cpu family exists");
        let mem = table
            .field_families
            .get_by_name("mem")
            .expect("mem family exists");
        assert!(matches!(&cpu.name, FieldFamilyName::User(n) if n.as_ref() == "cpu"));
        assert!(matches!(&mem.name, FieldFamilyName::User(n) if n.as_ref() == "mem"));
        assert_ne!(cpu.id, mem.id);
    }

    #[tokio::test]
    async fn aware_mode_unqualified_field_uses_auto_family() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware);
        txn.create_table_with_opts("t", opts).unwrap();
        let tx = txn.table_tx_or_create("t").unwrap();

        tx.field_or_create("bare", InfluxFieldType::Float).unwrap();

        catalog.commit(txn).await.unwrap();

        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let table = snapshot.db_schema().table_definition("t").unwrap();
        assert!(
            table
                .field_families
                .resource_iter()
                .all(|ff| matches!(&ff.name, FieldFamilyName::Auto(_))),
            "unqualified field should not create a named family",
        );
    }

    #[tokio::test]
    async fn aware_mode_malformed_qualifier_treated_as_unqualified() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware);
        txn.create_table_with_opts("t", opts).unwrap();
        let tx = txn.table_tx_or_create("t").unwrap();

        tx.field_or_create("::only_field", InfluxFieldType::Float)
            .unwrap();
        tx.field_or_create("only_family::", InfluxFieldType::Float)
            .unwrap();

        catalog.commit(txn).await.unwrap();

        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let table = snapshot.db_schema().table_definition("t").unwrap();
        assert!(
            table
                .field_families
                .resource_iter()
                .all(|ff| matches!(&ff.name, FieldFamilyName::Auto(_))),
            "malformed qualifiers should not create named families",
        );
    }

    #[tokio::test]
    async fn auto_mode_does_not_parse_double_colon() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        // The write-path default (`table_or_create`) is now Aware, so create an
        // Auto-mode table explicitly to exercise the no-parse behavior.
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Auto);
        txn.create_table_with_opts("t", opts).unwrap();
        let tx = txn.table_tx_or_create("t").unwrap();

        tx.field_or_create("cpu::user", InfluxFieldType::Float)
            .unwrap();

        catalog.commit(txn).await.unwrap();

        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let table = snapshot.db_schema().table_definition("t").unwrap();
        assert!(table.column_definition("cpu::user").is_some());
        assert!(table.field_families.get_by_name("cpu").is_none());
    }

    #[tokio::test]
    async fn aware_mode_named_family_enforces_field_limit() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware);
        txn.create_table_with_opts("t", opts).unwrap();
        let tx = txn.table_tx_or_create("t").unwrap();

        for i in 0..NUM_FIELDS_PER_FAMILY_LIMIT {
            let name = format!("grp::f{i}");
            tx.field_or_create(&name, InfluxFieldType::Integer).unwrap();
        }

        let err = tx
            .field_or_create("grp::too_many", InfluxFieldType::Integer)
            .unwrap_err();
        assert!(
            matches!(
                err,
                CatalogError::TooManyFields { ref field_family, limit }
                    if field_family == "grp" && limit == NUM_FIELDS_PER_FAMILY_LIMIT
            ),
            "expected TooManyFields on grp, got {err:?}",
        );
    }
}

// ---------------------------------------------------------------------------
// TableTransaction tests
// ---------------------------------------------------------------------------

mod table_transaction {
    use super::TestCatalog;
    use crate::CatalogError;
    use crate::catalog::versions::v3::NUM_FIELDS_PER_FAMILY_LIMIT;
    use crate::catalog::versions::v3::transaction::Prompt;
    use schema::InfluxFieldType;

    #[tokio::test]
    async fn table_tx_or_create_returns_same_handle_for_repeat_calls() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();

        let id_a = txn.table_tx_or_create("t1").unwrap().table_id();
        let id_b = txn.table_tx_or_create("t1").unwrap().table_id();
        assert_eq!(id_a, id_b);

        let id_c = txn.table_tx_or_create("t2").unwrap().table_id();
        assert_ne!(id_a, id_c);
    }

    #[tokio::test]
    async fn time_or_create_is_idempotent() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        assert_eq!(tx.num_columns(), 0);
        let a = tx.time_or_create().unwrap();
        let b = tx.time_or_create().unwrap();
        assert_eq!(a, b);
        assert_eq!(tx.num_columns(), 1);
    }

    #[tokio::test]
    async fn tag_or_create_is_idempotent() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        let a = tx.tag_or_create("host").unwrap();
        let b = tx.tag_or_create("host").unwrap();
        assert_eq!(a, b);
        assert_eq!(tx.num_columns(), 1);
    }

    #[tokio::test]
    async fn tag_or_create_rejects_reserved_name() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        let err = tx.tag_or_create("time").unwrap_err();
        assert!(
            matches!(err, CatalogError::ReservedColumn(_)),
            "expected ReservedColumn, got {err:?}",
        );
    }

    #[tokio::test]
    async fn tag_or_create_rejects_name_already_used_by_field() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        tx.field_or_create("v", InfluxFieldType::Float).unwrap();
        let err = tx.tag_or_create("v").unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidColumnType { .. }),
            "expected InvalidColumnType, got {err:?}",
        );
    }

    #[tokio::test]
    async fn field_or_create_is_idempotent_for_same_type() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        let a = tx.field_or_create("v", InfluxFieldType::Float).unwrap();
        let b = tx.field_or_create("v", InfluxFieldType::Float).unwrap();
        assert_eq!(a, b);
        assert_eq!(tx.num_columns(), 1);
    }

    #[tokio::test]
    async fn field_or_create_rejects_different_type_for_same_name() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        tx.field_or_create("v", InfluxFieldType::Float).unwrap();
        let err = tx
            .field_or_create("v", InfluxFieldType::Integer)
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidColumnType { .. }),
            "expected InvalidColumnType, got {err:?}",
        );
    }

    #[tokio::test]
    async fn field_or_create_rejects_reserved_name() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        let tx = txn.table_tx_or_create("t1").unwrap();

        let err = tx
            .field_or_create("time", InfluxFieldType::Float)
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::ReservedColumn(_)),
            "expected ReservedColumn, got {err:?}",
        );
    }

    #[tokio::test]
    async fn field_family_rolls_over_when_full() {
        let catalog = TestCatalog::new().catalog();
        let mut txn = catalog.begin_database_transaction("foo").unwrap();
        {
            let tx = txn.table_tx_or_create("t1").unwrap();
            // Fill the first auto family to capacity.
            for i in 0..NUM_FIELDS_PER_FAMILY_LIMIT {
                let name = format!("f{i}");
                let f = tx.field_or_create(&name, InfluxFieldType::Float).unwrap();
                assert_eq!(f.id.0.get(), 0, "field {name} should be in family 0");
            }
            // The next field rolls over into a new auto family.
            let overflow = tx
                .field_or_create("overflow", InfluxFieldType::Float)
                .unwrap();
            assert_eq!(overflow.id.0.get(), 1);
        }

        let result = catalog.commit(txn).await.unwrap();
        let Prompt::Success(_) = result else {
            panic!("expected Success");
        };

        // After commit, the table reflects two field families.
        let snapshot = catalog.begin_database_transaction("foo").unwrap();
        let t1 = snapshot
            .db_schema()
            .table_definition("t1")
            .expect("t1 exists");
        assert_eq!(t1.num_field_families(), 2);
        assert_eq!(t1.num_field_columns(), NUM_FIELDS_PER_FAMILY_LIMIT + 1);
    }
}

// ---------------------------------------------------------------------------
// Background checkpointing
// ---------------------------------------------------------------------------

mod checkpointing {
    use super::*;
    use pretty_assertions::assert_eq;

    fn catalog_with_policy(
        shared: Arc<dyn object_store::ObjectStore>,
        policy: CheckpointPolicy,
    ) -> Catalog {
        let store = ObjectStoreCatalog::new("p", shared, StorageMode::default());
        let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
        Catalog::from_parts(
            inner,
            store,
            None,
            test_time_provider(),
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            policy,
            Arc::new(CatalogLimits::none()),
        )
    }

    /// Wait for any in-flight background checkpoint to finish by acquiring the
    /// single permit; release it immediately so later assertions aren't
    /// blocked.
    async fn await_checkpoint(catalog: &Catalog) {
        let _ = catalog.checkpoint_slot.acquire().await.unwrap();
    }

    #[tokio::test]
    async fn skips_below_threshold() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            shared,
            CheckpointPolicy {
                log_interval: 100,
                time_interval: Duration::from_secs(3600),
            },
        );

        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .unwrap();

        // Direct call at seq=1: delta < log_interval, time well below
        // time_interval, so nothing spawns.
        assert!(
            catalog
                .maybe_background_checkpoint(CatalogSequenceNumber::new(1))
                .is_none()
        );
    }

    #[tokio::test]
    async fn skips_when_no_records() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            shared,
            CheckpointPolicy {
                log_interval: 1,
                time_interval: Duration::from_millis(0),
            },
        );

        // No updates performed — ordered_records is empty. Even with
        // thresholds met, the empty catalog is skipped.
        assert!(
            catalog
                .maybe_background_checkpoint(CatalogSequenceNumber::new(5))
                .is_none()
        );
    }

    #[tokio::test]
    async fn fires_at_log_threshold_and_persists_snapshot() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            Arc::clone(&shared),
            CheckpointPolicy {
                log_interval: 2,
                time_interval: Duration::from_secs(3600),
            },
        );

        // Two updates: seq=1 internal check skips (delta=1 < 2); seq=2 fires.
        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .unwrap();
        catalog
            .update::<RegisterNodeOp>(register_args("b"))
            .await
            .unwrap();

        await_checkpoint(&catalog).await;

        let last = *catalog.last_checkpoint.lock();
        assert_eq!(last.sequence, CatalogSequenceNumber::new(2));

        let (snapshot, _size_bytes) = catalog.store.load_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.header.sequence_number, 2);
        assert_eq!(snapshot.record_count(), 2);
    }

    #[tokio::test]
    async fn fires_at_time_threshold() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            Arc::clone(&shared),
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_millis(50),
            },
        );

        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(80)).await;

        catalog
            .update::<RegisterNodeOp>(register_args("b"))
            .await
            .unwrap();

        await_checkpoint(&catalog).await;

        let last = *catalog.last_checkpoint.lock();
        assert_eq!(last.sequence, CatalogSequenceNumber::new(2));
    }

    #[tokio::test]
    async fn state_resets_after_success() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            Arc::clone(&shared),
            CheckpointPolicy {
                log_interval: 1,
                time_interval: Duration::from_secs(3600),
            },
        );

        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .unwrap();
        await_checkpoint(&catalog).await;

        // After success, delta from last checkpoint is 0 and time is near-zero,
        // so an immediate re-attempt at the same sequence must skip.
        assert!(
            catalog
                .maybe_background_checkpoint(CatalogSequenceNumber::new(1))
                .is_none()
        );
    }

    /// A forced post-restore checkpoint must serialize against the same slot
    /// `maybe_background_checkpoint` uses. Otherwise a background snapshot task
    /// spawned just before the restore — holding an older sequence and
    /// pre-restore records — can land its `put` after the forced write and
    /// clobber the post-restore snapshot with a stale image, defeating the
    /// point of forcing the checkpoint.
    #[tokio::test]
    async fn force_checkpoint_serializes_against_inflight_background_checkpoint() {
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = Arc::new(catalog_with_policy(
            Arc::clone(&shared),
            // Suppress the automatic background checkpoint; we drive the slot by
            // hand so the test is deterministic.
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_secs(3600),
            },
        ));

        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .unwrap();

        // Stand in for an in-flight background checkpoint that captured an
        // older, pre-restore image and has not yet landed its `put`: hold the
        // single checkpoint slot.
        let permit = Arc::clone(&catalog.checkpoint_slot)
            .acquire_owned()
            .await
            .unwrap();

        // The forced checkpoint must block on the slot rather than racing the
        // (still in-flight) background write.
        let handle = {
            let catalog = Arc::clone(&catalog);
            tokio::spawn(async move { catalog.force_checkpoint().await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !handle.is_finished(),
            "force_checkpoint must block while a background checkpoint holds the slot",
        );

        // Once the in-flight checkpoint releases the slot, the forced
        // checkpoint proceeds and writes last, so the live (post-restore)
        // sequence wins.
        drop(permit);
        handle.await.unwrap().unwrap();

        let live_seq = catalog.inner.read().sequence_number();
        let (snapshot, _size_bytes) = catalog.store.load_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.header.sequence_number, live_seq.get());
    }

    /// Reproducer for #4019, symptom A: a snapshot containing a hard-deleted
    /// database that had tables fails to load — cold start fails outright.
    #[tokio::test]
    async fn probe_hard_deleted_db_snapshot_reloads() {
        let test_catalog = TestCatalog::new();
        let catalog = catalog_with_policy(
            test_catalog.store(),
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_secs(3600),
            },
        );
        catalog.create_database("keep").await.unwrap();
        let dropped = catalog.create_database("dropped").await.unwrap();
        catalog
            .create_table("dropped", "t", &["host"], &[("temp", FieldDataType::Float)])
            .await
            .unwrap();
        catalog
            .soft_delete_database(
                "dropped",
                HardDeletionTime::Now,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap();
        catalog.hard_delete_database(&dropped.id).await.unwrap();

        catalog.force_checkpoint().await.unwrap();

        let reloaded = test_load_or_create("p", test_catalog.store()).await;
        assert!(reloaded.is_ok(), "reload failed: {:?}", reloaded.err());
    }

    /// Reproducer for #4019, symptom B: a snapshot containing a soft-deleted
    /// database that had tables loads, but the table state silently diverges
    /// from the live state that produced it.
    #[tokio::test]
    async fn probe_soft_deleted_db_snapshot_reload_state() {
        let test_catalog = TestCatalog::new();
        let catalog = catalog_with_policy(
            test_catalog.store(),
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_secs(3600),
            },
        );
        catalog.create_database("db").await.unwrap();
        catalog
            .create_table("db", "t", &["host"], &[("temp", FieldDataType::Float)])
            .await
            .unwrap();
        catalog
            .soft_delete_database("db", HardDeletionTime::Never, DeletionScope::DataAndCatalog)
            .await
            .unwrap();

        // Live state: the soft delete cascaded to the table.
        let live_db = catalog
            .inner
            .read()
            .databases
            .resource_iter()
            .next()
            .cloned()
            .unwrap();
        let live_table = live_db.tables.resource_iter().next().cloned().unwrap();
        assert!(live_db.deleted);
        assert!(live_table.deleted, "live table not marked deleted");

        catalog.force_checkpoint().await.unwrap();
        let reloaded = test_catalog.reload().await;
        let reloaded_db = reloaded
            .db_schema_by_id(&live_db.id)
            .expect("database present after reload");
        let reloaded_table = reloaded_db.tables.resource_iter().next().cloned().unwrap();
        assert_eq!(reloaded_db.name, live_db.name, "db name diverged");
        assert_eq!(
            reloaded_table.deleted, live_table.deleted,
            "table deleted flag diverged: live={} reloaded={}",
            live_table.deleted, reloaded_table.deleted,
        );
        assert_eq!(
            reloaded_table.table_name, live_table.table_name,
            "table name diverged",
        );
    }

    /// Reproducer for #4019, symptom C: a snapshot containing a database
    /// hard-deleted with `DataOnlyRemoveTables` scope loads, but the cleared
    /// tables are resurrected.
    #[tokio::test]
    async fn probe_remove_tables_hard_delete_snapshot_reload_state() {
        let test_catalog = TestCatalog::new();
        let catalog = catalog_with_policy(
            test_catalog.store(),
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_secs(3600),
            },
        );
        let db = catalog.create_database("db").await.unwrap();
        catalog
            .create_table("db", "t", &["host"], &[("temp", FieldDataType::Float)])
            .await
            .unwrap();
        catalog
            .soft_delete_database(
                "db",
                HardDeletionTime::Now,
                DeletionScope::DataOnlyRemoveTables,
            )
            .await
            .unwrap();
        catalog.hard_delete_database(&db.id).await.unwrap();

        // Live state: the database survives, its tables were cleared.
        let live_db = catalog.db_schema_by_id(&db.id).unwrap();
        assert_eq!(live_db.tables.len(), 0, "live tables not cleared");

        catalog.force_checkpoint().await.unwrap();
        let reloaded = test_catalog.reload().await;
        let reloaded_db = reloaded.db_schema_by_id(&db.id).unwrap();
        assert_eq!(
            reloaded_db.tables.len(),
            0,
            "cleared tables resurrected on reload"
        );
    }

    #[cfg(feature = "true_deletion")]
    #[test_log::test(tokio::test)]
    async fn soft_deletion_cascades_and_removes_all_references_from_snapshot() {
        use crate::format::{
            CatalogFile, MakeRecord, record_ids,
            records::{
                AddColumns, CreateDatabase, CreateTable, NextIdScope, SetNextId,
                types::{
                    ColumnDefinition, FieldColumn, FieldFamilyDefinition, FieldFamilyMode,
                    FieldFamilyName, FieldIdentifier, RetentionPeriod, TagColumn, TimestampColumn,
                },
            },
        };

        fn get_snapshot_catalog_file(catalog: &Catalog) -> CatalogFile {
            let bytes = catalog.inner.write().create_snapshot();
            let mut cursor = std::io::Cursor::new(bytes.as_ref());
            CatalogFile::read_from(&mut cursor).expect("reader parses snapshot")
        }

        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = catalog_with_policy(
            Arc::clone(&shared),
            CheckpointPolicy {
                log_interval: 1_000_000,
                time_interval: Duration::from_secs(3600),
            },
        );

        let db = catalog.create_database("db").await.unwrap();
        let table = catalog
            .create_table("db", "t", &["host"], &[("temp", FieldDataType::Float)])
            .await
            .unwrap();

        let file = get_snapshot_catalog_file(&catalog);
        let expected_records = [
            CreateDatabase {
                database_id: db.id.get(),
                database_name: "db".to_string(),
                retention_period: RetentionPeriod::Indefinite,
            }
            .make_record(1),
            CreateTable {
                database_id: db.id.get(),
                database_name: "db".to_string(),
                table_name: "t".to_string(),
                table_id: table.table_id.get(),
                retention_period: RetentionPeriod::Indefinite,
                field_family_mode: FieldFamilyMode::Aware,
            }
            .make_record(2),
            AddColumns {
                database_id: db.id.get(),
                table_id: table.table_id.get(),
                columns: vec![
                    ColumnDefinition::Tag(TagColumn {
                        id: 0,
                        column_id: Some(0),
                        name: "host".to_string(),
                    }),
                    ColumnDefinition::Field(FieldColumn {
                        id: FieldIdentifier {
                            family_id: 0,
                            field_id: 0,
                        },
                        column_id: Some(1),
                        name: "temp".to_string(),
                        data_type: crate::format::records::types::FieldDataType::Float,
                    }),
                    ColumnDefinition::Timestamp(TimestampColumn {
                        column_id: Some(2),
                        name: "time".to_string(),
                    }),
                ],
                field_families: vec![FieldFamilyDefinition {
                    id: 0,
                    name: FieldFamilyName::Auto(0),
                }],
            }
            .make_record(2),
        ];

        // make sure that we have all of the records for the creation and such...
        assert_eq!(file.records, expected_records);

        // todo: test different deletion scopes
        catalog
            .soft_delete_database("db", HardDeletionTime::Now, DeletionScope::DataAndCatalog)
            .await
            .unwrap();

        let file = get_snapshot_catalog_file(&catalog);

        assert_eq!(file.records[..file.records.len() - 1], expected_records);
        let soft_delete_record = file.records.last().unwrap();
        assert_eq!(soft_delete_record.id(), record_ids::SOFT_DELETE_DATABASE);

        // normally, the deleter or smth would handle actually doing the hard-deleting, but since
        // this is a catalog-only test, we have to manually call the delete ourselves.
        catalog.hard_delete_database(&db.id).await.unwrap();

        let file = get_snapshot_catalog_file(&catalog);

        // and now make sure that none of the creation events are there
        assert_eq!(
            file.records,
            [SetNextId {
                id: 0,
                scope: NextIdScope::Databases
            }
            .make_record(1)]
        );
    }
}

// ---------------------------------------------------------------------------
// Legacy grouped snapshot rewrite at startup
// ---------------------------------------------------------------------------

mod startup_snapshot_rewrite {
    use std::io::Cursor;

    use super::*;
    use crate::format::records::{CreateDatabase, CreateTable, RegisterNode};
    use crate::format::{CatalogFile, FeatureLevel, Header, MakeRecord, Record, file_flags};
    use crate::object_store::CatalogFilePath;

    /// Byte length of one legacy group-index entry.
    const LEGACY_ENTRY_SIZE: usize = 24;

    /// Encode one legacy group-index entry: group_type, group_key,
    /// record_count, and byte_length as u32 LE, then byte_offset as u64 LE.
    fn legacy_index_entry(
        group_type: u32,
        group_key: u32,
        record_count: u32,
        byte_length: u32,
        byte_offset: u64,
    ) -> [u8; LEGACY_ENTRY_SIZE] {
        let mut entry = [0u8; LEGACY_ENTRY_SIZE];
        entry[0..4].copy_from_slice(&group_type.to_le_bytes());
        entry[4..8].copy_from_slice(&group_key.to_le_bytes());
        entry[8..12].copy_from_slice(&record_count.to_le_bytes());
        entry[12..16].copy_from_slice(&byte_length.to_le_bytes());
        entry[16..24].copy_from_slice(&byte_offset.to_le_bytes());
        entry
    }

    /// Hand-build a legacy multi-group snapshot file: header, then one
    /// 24-byte index entry per (group, records) pair — Global first — then
    /// the record bytes of every group concatenated in order.
    fn build_legacy_grouped_snapshot(
        sequence_number: u64,
        groups: &[(u32, u32, &[Record])],
    ) -> Vec<u8> {
        let mut index = Vec::new();
        let mut record_bytes = Vec::new();
        let mut record_count = 0u32;
        for (group_type, group_key, records) in groups {
            let byte_offset = record_bytes.len() as u64;
            for record in *records {
                record_bytes.extend_from_slice(&record.to_bytes());
            }
            index.extend_from_slice(&legacy_index_entry(
                *group_type,
                *group_key,
                records.len() as u32,
                (record_bytes.len() as u64 - byte_offset) as u32,
                byte_offset,
            ));
            record_count += records.len() as u32;
        }
        let payload: Vec<u8> = index.into_iter().chain(record_bytes).collect();
        let header = Header {
            format_version: Header::CURRENT_VERSION,
            flags: file_flags::SNAPSHOT,
            catalog_uuid: Uuid::nil().as_u128(),
            sequence_number,
            record_count,
            group_count: groups.len() as u32,
            payload_len: payload.len() as u64,
            payload_crc: crc32fast::hash(&payload),
        };
        let mut file = Vec::with_capacity(Header::SIZE + payload.len());
        file.extend_from_slice(&header.to_bytes());
        file.extend_from_slice(&payload);
        file
    }

    /// Hand-build an indexless flat snapshot file as written between
    /// #4026 and the compat-entry writer: header with `group_count: 0`,
    /// then the record bytes with no index.
    fn build_flat_indexless_snapshot(sequence_number: u64, records: &[Record]) -> Vec<u8> {
        let mut payload = Vec::new();
        for record in records {
            payload.extend_from_slice(&record.to_bytes());
        }
        let header = Header {
            format_version: Header::CURRENT_VERSION,
            flags: file_flags::SNAPSHOT,
            catalog_uuid: Uuid::nil().as_u128(),
            sequence_number,
            record_count: records.len() as u32,
            group_count: 0,
            payload_len: payload.len() as u64,
            payload_crc: crc32fast::hash(&payload),
        };
        let mut file = Vec::with_capacity(Header::SIZE + payload.len());
        file.extend_from_slice(&header.to_bytes());
        file.extend_from_slice(&payload);
        file
    }

    /// Three records — register-node, create-database, create-table — used
    /// to populate the hand-built snapshot fixtures.
    fn sample_records() -> [Record; 3] {
        let register = RegisterNode {
            node_catalog_id: 1,
            node_id: "node-a".to_string(),
            instance_id: "inst-1".to_string(),
            registered_time_ns: 1000,
            core_count: 4,
            mode: vec![crate::format::records::types::NodeMode::Core],
            process_uuid: [0u8; 16],
            conn_info: None,
            cli_params: None,
            row_delete_predicate_version: 0,
            feature_level: FeatureLevel::ZERO,
        }
        .make_record(1);
        let create_db = CreateDatabase {
            database_id: 10,
            database_name: "db-10".to_string(),
            retention_period: crate::format::records::types::RetentionPeriod::Indefinite,
        }
        .make_record(2);
        let create_table = CreateTable {
            database_id: 10,
            database_name: "db-10".to_string(),
            table_name: "table-100".to_string(),
            table_id: 100,
            retention_period: crate::format::records::types::RetentionPeriod::Indefinite,
            field_family_mode: crate::format::records::types::FieldFamilyMode::Auto,
        }
        .make_record(3);
        [register, create_db, create_table]
    }

    async fn read_stored_snapshot(
        store: &Arc<dyn object_store::ObjectStore>,
        prefix: &str,
    ) -> CatalogFile {
        let bytes = store
            .get(&CatalogFilePath::snapshot(prefix))
            .await
            .expect("snapshot object present")
            .bytes()
            .await
            .unwrap();
        let mut cursor = Cursor::new(bytes.as_ref());
        CatalogFile::read_from(&mut cursor).expect("parse stored snapshot")
    }

    #[tokio::test]
    async fn legacy_grouped_snapshot_rewritten_at_startup() {
        let test_catalog = TestCatalog::new();
        let store = test_catalog.store();

        // Hand-build a legacy multi-group snapshot: Global group with a
        // register-node and a create-database record, plus a per-database
        // group holding the create-table record.
        let [register, create_db, create_table] = sample_records();

        let bytes = build_legacy_grouped_snapshot(
            3,
            &[
                (0, 0, &[register, create_db]), // Global group
                (1, 10, &[create_table]),       // Database group for db 10
            ],
        );
        store
            .put(&CatalogFilePath::snapshot("p"), bytes.into())
            .await
            .unwrap();
        assert_eq!(
            read_stored_snapshot(&store, "p").await.header.group_count,
            2
        );

        let catalog = test_catalog.reload().await;

        // The loaded state contains the db + table from the legacy file.
        let db = catalog.db_schema("db-10").expect("db loaded");
        assert!(db.tables.get_by_name("table-100").is_some());

        // The stored snapshot was rewritten in the current single-group
        // form, preserving record count and sequence.
        let snapshot = read_stored_snapshot(&store, "p").await;
        assert_eq!(snapshot.header.group_count, 1);
        assert_eq!(snapshot.header.sequence_number, 3);
        assert_eq!(snapshot.record_count(), 3);
    }

    #[tokio::test]
    async fn flat_indexless_snapshot_rewritten_at_startup() {
        let test_catalog = TestCatalog::new();
        let store = test_catalog.store();

        // Hand-build a flat indexless snapshot (group_count == 0) as written
        // between #4026 and the compat-entry writer. Pre-flat readers reject
        // this layout, so startup must rewrite it even though this binary
        // reads it fine.
        let records = sample_records();
        let bytes = build_flat_indexless_snapshot(3, &records);
        store
            .put(&CatalogFilePath::snapshot("p"), bytes.into())
            .await
            .unwrap();
        assert_eq!(
            read_stored_snapshot(&store, "p").await.header.group_count,
            0
        );

        let catalog = test_catalog.reload().await;

        // The loaded state contains the db + table from the flat file.
        let db = catalog.db_schema("db-10").expect("db loaded");
        assert!(db.tables.get_by_name("table-100").is_some());

        // The stored snapshot was rewritten in the current single-group
        // form, preserving record count and sequence.
        let snapshot = read_stored_snapshot(&store, "p").await;
        assert_eq!(snapshot.header.group_count, 1);
        assert_eq!(snapshot.header.sequence_number, 3);
        assert_eq!(snapshot.record_count(), 3);
    }

    #[tokio::test]
    async fn current_snapshot_not_rewritten_at_startup() {
        let test_catalog = TestCatalog::new();
        let store = test_catalog.store();
        {
            let catalog = test_catalog.reload().await;
            catalog.create_database("db").await.unwrap();
            catalog.force_checkpoint().await.unwrap();
        }
        let path = CatalogFilePath::snapshot("p");
        let before = store.head(&path).await.unwrap();
        assert_eq!(
            read_stored_snapshot(&store, "p").await.header.group_count,
            1
        );

        let reloaded = test_catalog.reload().await;
        assert!(reloaded.db_schema("db").is_some());

        // A single-group snapshot is already the target form; startup must
        // not write it again.
        let after = store.head(&path).await.unwrap();
        assert_eq!(before.e_tag, after.e_tag);
        assert_eq!(before.last_modified, after.last_modified);
    }

    #[tokio::test]
    async fn fresh_catalog_writes_only_initial_snapshot() {
        let test_catalog = TestCatalog::new();
        let store = test_catalog.store();
        let catalog = test_catalog.reload().await;

        // Pin the existing fresh-boot behavior: initialization writes an
        // initial single-group snapshot at sequence 0 holding only the
        // SetStorageMode record. No rewrite runs — the `_internal` database
        // created after init lands in a log file, not the snapshot.
        let snapshot = read_stored_snapshot(&store, "p").await;
        assert_eq!(snapshot.header.group_count, 1);
        assert_eq!(snapshot.header.sequence_number, 0);
        assert_eq!(snapshot.record_count(), 1);
        assert!(catalog.db_schema("_internal").is_some());
    }
}

// ---------------------------------------------------------------------------
// DDL CreateTable field families
// ---------------------------------------------------------------------------

mod ddl_create_table {
    use super::*;

    use crate::catalog::{
        CreateTableOptions,
        versions::v3::schema::column::{FieldDataType, FieldFamilyMode, FieldFamilyName},
    };

    #[tokio::test]
    async fn aware_mode_ddl_routes_qualified_fields_to_named_families() {
        let catalog = TestCatalog::new().catalog();

        let tags: [&str; 0] = [];
        let table = catalog
            .create_table_opts(
                "foo",
                "t",
                &tags,
                &[
                    ("cpu::user", FieldDataType::Float),
                    ("cpu::system", FieldDataType::Float),
                    ("mem::used", FieldDataType::UInteger),
                ],
                CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware),
            )
            .await
            .unwrap();

        // Aware mode must parse `family::field` and group fields by family,
        // matching the write path and v2's DDL path — not dump every field
        // into a single family named after the first field.
        assert!(
            table.field_families.get_by_name("cpu").is_some(),
            "expected a `cpu` field family",
        );
        assert!(
            table.field_families.get_by_name("mem").is_some(),
            "expected a `mem` field family",
        );
        assert!(
            table.field_families.get_by_name("cpu::user").is_none(),
            "field family must not be named after the first field",
        );
        assert_eq!(table.num_field_families(), 2);
    }

    #[tokio::test]
    async fn aware_mode_ddl_unqualified_field_uses_auto_family() {
        let catalog = TestCatalog::new().catalog();

        let tags: [&str; 0] = [];
        let table = catalog
            .create_table_opts(
                "foo",
                "t",
                &tags,
                &[
                    ("cpu::user", FieldDataType::Float),
                    ("bare", FieldDataType::Float),
                ],
                CreateTableOptions::default().field_family_mode(FieldFamilyMode::Aware),
            )
            .await
            .unwrap();

        // Qualified field -> named family; unqualified field -> the shared
        // auto family (mirrors the write path).
        assert!(table.field_families.get_by_name("cpu").is_some());
        assert_eq!(table.num_field_families(), 2);
        let auto_families = table
            .field_families
            .resource_iter()
            .filter(|ff| matches!(&ff.name, FieldFamilyName::Auto(_)))
            .count();
        assert_eq!(
            auto_families, 1,
            "unqualified field should land in an auto family",
        );
    }
}

// ---------------------------------------------------------------------------
// Limit enforcement
// ---------------------------------------------------------------------------

mod limits {
    use super::*;

    use crate::CatalogError;
    use crate::catalog::versions::v3::ops::database::{CreateDatabaseArgs, CreateDatabaseOp};
    use crate::catalog::versions::v3::schema::column::FieldFamilyMode;
    use crate::catalog::{CreateTableOptions, FieldDataType, TableDefinition};

    fn catalog_with_limits(limits: CatalogLimits) -> Catalog {
        let store = ObjectStoreCatalog::new(
            "p",
            Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>,
            StorageMode::default(),
        );
        let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
        Catalog::from_parts(
            inner,
            store,
            None,
            test_time_provider(),
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(limits),
        )
    }

    fn create_db_args(name: &str) -> CreateDatabaseArgs {
        CreateDatabaseArgs {
            name: name.to_string(),
            retention_period: None,
        }
    }

    async fn create_table(
        cat: &Catalog,
        db: &str,
        table: &str,
    ) -> Result<Arc<TableDefinition>, CatalogError> {
        let tags: [&str; 0] = [];
        let fields: [(&str, FieldDataType); 0] = [];
        let opts = CreateTableOptions::default().field_family_mode(FieldFamilyMode::Auto);
        cat.create_table_opts(db, table, &tags, &fields, opts).await
    }

    #[tokio::test]
    async fn create_database_rejected_at_db_limit() {
        let catalog = catalog_with_limits(CatalogLimits::new(2, 100, 100));

        catalog
            .update::<CreateDatabaseOp>(create_db_args("a"))
            .await
            .unwrap();
        catalog
            .update::<CreateDatabaseOp>(create_db_args("b"))
            .await
            .unwrap();

        let err = catalog
            .update::<CreateDatabaseOp>(create_db_args("c"))
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::TooManyDbs(2)));
    }

    #[tokio::test]
    async fn create_table_rejected_at_table_limit() {
        let catalog = catalog_with_limits(CatalogLimits::new(10, 1, 100));
        create_table(&catalog, "db1", "t1").await.unwrap();
        let err = create_table(&catalog, "db1", "t2").await.unwrap_err();
        assert!(matches!(
            err,
            CatalogError::TooManyTables {
                current: 1,
                limit: 1
            }
        ));
    }

    #[tokio::test]
    async fn create_table_auto_create_db_rejected_at_db_limit() {
        let catalog = catalog_with_limits(CatalogLimits::new(1, 100, 100));

        // First db auto-created via CreateTableOp succeeds.
        create_table(&catalog, "db1", "t1").await.unwrap();

        // Second db (different name) would push us past the db limit.
        let err = create_table(&catalog, "db2", "t2").await.unwrap_err();
        assert!(matches!(err, CatalogError::TooManyDbs(1)));
    }

    #[tokio::test]
    async fn duplicate_create_at_db_limit_returns_already_exists() {
        // Re-creating an existing database while at the db limit must
        // surface AlreadyExists, not TooManyDbs — prepare runs ahead of
        // limits_check.
        let catalog = catalog_with_limits(CatalogLimits::new(1, 100, 100));

        catalog
            .update::<CreateDatabaseOp>(create_db_args("db1"))
            .await
            .unwrap();

        let err = catalog
            .update::<CreateDatabaseOp>(create_db_args("db1"))
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists));
    }

    #[tokio::test]
    async fn duplicate_create_at_table_limit_returns_already_exists() {
        let catalog = catalog_with_limits(CatalogLimits::new(10, 1, 100));
        create_table(&catalog, "db1", "t1").await.unwrap();
        let err = create_table(&catalog, "db1", "t1").await.unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists));
    }

    #[tokio::test]
    async fn begin_database_transaction_rejected_at_db_limit() {
        let catalog = catalog_with_limits(CatalogLimits::new(1, 100, 100));

        catalog
            .update::<CreateDatabaseOp>(create_db_args("db1"))
            .await
            .unwrap();

        // Existing db: succeeds.
        catalog.begin_database_transaction("db1").unwrap();

        // New db over the limit: rejected before staging the CreateDatabase
        // record.
        let err = catalog.begin_database_transaction("db2").unwrap_err();
        assert!(matches!(err, CatalogError::TooManyDbs(1)));
    }

    #[tokio::test]
    async fn table_transaction_rejects_new_column_at_column_limit_with_details() {
        let catalog = catalog_with_limits(CatalogLimits::new(10, 100, 2));
        let mut txn = catalog.begin_database_transaction("db1").unwrap();
        let tx = txn.table_tx_or_create("cpu").unwrap();

        tx.tag_or_create("tag0").unwrap();
        tx.field_or_create("field0", FieldDataType::String.into())
            .unwrap();

        let err = tx.tag_or_create("tag1").unwrap_err();
        assert!(
            matches!(
                err,
                CatalogError::TooManyColumns {
                    ref table_name,
                    attempted: 3,
                    limit: 2,
                } if table_name.inner() == "cpu"
            ),
            "expected detailed TooManyColumns, got {err:?}",
        );
    }

    #[tokio::test]
    async fn table_transaction_allows_increased_tag_limit_and_rejects_next_tag_with_details() {
        use crate::catalog::versions::v3::NUM_TAG_COLUMNS_LIMIT;

        let catalog = catalog_with_limits(CatalogLimits::new(10, 100, NUM_TAG_COLUMNS_LIMIT + 1));
        let mut txn = catalog.begin_database_transaction("db1").unwrap();
        let tx = txn.table_tx_or_create("cpu").unwrap();

        for i in 0..NUM_TAG_COLUMNS_LIMIT {
            tx.tag_or_create(&format!("tag{i}")).unwrap();
        }
        assert_eq!(tx.num_columns(), NUM_TAG_COLUMNS_LIMIT);

        let err = tx.tag_or_create("tag_too_many").unwrap_err();
        assert!(
            matches!(
                err,
                CatalogError::TooManyTagColumns {
                    ref table_name,
                    attempted,
                    limit,
                } if table_name.inner() == "cpu"
                    && attempted == NUM_TAG_COLUMNS_LIMIT + 1
                    && limit == NUM_TAG_COLUMNS_LIMIT
            ),
            "expected detailed TooManyTagColumns, got {err:?}",
        );
    }

    #[tokio::test]
    async fn check_write_column_limits_reports_projected_total() {
        let catalog = catalog_with_limits(CatalogLimits::new(10, 100, 5));
        create_table(&catalog, "db1", "cpu").await.unwrap();

        let mut txn = catalog.begin_database_transaction("db1").unwrap();
        let incoming_tags = ["tag1", "tag2"];
        let incoming_fields = [
            "field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8",
        ];

        let err = txn
            .check_write_column_limits("cpu", &incoming_tags, &incoming_fields)
            .unwrap_err();
        assert!(
            matches!(
                err,
                CatalogError::TooManyColumns {
                    ref table_name,
                    attempted: 10,
                    limit: 5,
                } if table_name.inner() == "cpu"
            ),
            "expected projected TooManyColumns, got {err:?}",
        );
    }

    #[tokio::test]
    async fn check_write_column_limits_reports_projected_tag_total() {
        const TAG_COLUMN_LIMIT: usize = 10;
        let catalog = catalog_with_limits(
            CatalogLimits::new(10, 100, usize::MAX)
                .with_tag_columns_per_table_limit(TAG_COLUMN_LIMIT),
        );
        let mut txn = catalog.begin_database_transaction("db1").unwrap();
        let tx = txn.table_tx_or_create("cpu").unwrap();

        for i in 0..TAG_COLUMN_LIMIT - 2 {
            tx.tag_or_create(&format!("tag{i}")).unwrap();
        }

        let incoming_tags: Vec<String> = (0..5).map(|i| format!("new_tag{i}")).collect();
        let incoming_tag_refs: Vec<&str> = incoming_tags.iter().map(String::as_str).collect();
        let err = txn
            .check_write_column_limits("cpu", &incoming_tag_refs, &[])
            .unwrap_err();
        assert!(
            matches!(
                err,
                CatalogError::TooManyTagColumns {
                    ref table_name,
                    attempted,
                    limit,
                } if table_name.inner() == "cpu"
                    && attempted == TAG_COLUMN_LIMIT + 3
                    && limit == TAG_COLUMN_LIMIT
            ),
            "expected projected TooManyTagColumns, got {err:?}",
        );
    }

    #[tokio::test]
    async fn check_write_column_limits_counts_duplicate_keys_once() {
        let catalog = catalog_with_limits(CatalogLimits::new(10, 100, 5));
        create_table(&catalog, "db1", "cpu").await.unwrap();

        let mut txn = catalog.begin_database_transaction("db1").unwrap();
        // 5 unique new columns, exactly at the limit; the repeated keys must
        // not be double-counted or this would be spuriously rejected.
        let incoming_tags = ["tag1", "tag1"];
        let incoming_fields = ["field1", "field1", "field2", "field3", "field4"];

        txn.check_write_column_limits("cpu", &incoming_tags, &incoming_fields)
            .expect("duplicate keys within a line should count once toward the limit");
    }
}
// ---------------------------------------------------------------------------
// Write-path feature-level gate
// ---------------------------------------------------------------------------

mod feature_level_gate {
    use super::*;
    use crate::CatalogError;
    use crate::format::FeatureLevel;

    #[tokio::test]
    async fn rejects_record_above_committed_level() {
        // Floor the committed level so RegisterNode (record_id well above
        // 0) is gated. The op is otherwise valid; the gate rejects it
        // before persistence.
        let catalog = TestCatalog::new().catalog();
        catalog.inner.write().committed_feature_level = FeatureLevel::ZERO;

        let err = catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .expect_err("gate must reject record above committed level");
        let CatalogError::RecordExceedsCommittedFeatureLevel {
            record_id,
            committed,
        } = err
        else {
            panic!("expected RecordExceedsCommittedFeatureLevel, got {err:?}");
        };
        assert!(record_id > 0);
        assert_eq!(committed, FeatureLevel::ZERO);

        // The gate runs before persistence, so the catalog must not have
        // advanced its sequence.
        assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));
    }

    #[tokio::test]
    async fn allows_record_within_committed_level() {
        // Default-constructed catalog commits to the locally-derived
        // level, so every locally-known record is within bounds.
        let catalog = TestCatalog::new().catalog();
        catalog
            .update::<RegisterNodeOp>(register_args("a"))
            .await
            .expect("record at the committed level must be allowed");
    }

    #[tokio::test]
    async fn update_catches_up_when_gate_fails_on_stale_view() {
        // Two `Catalog`s share an object store. We floor both their
        // in-memory `committed_feature_level`s so any non-trivial record
        // would hit the gate, then advance the cluster from `cat2` —
        // landing an `AdvanceFeatureLevel` log file that `cat1` has not
        // yet applied. From `cat1`'s POV the local committed is now
        // stale: the cluster will accept records beyond `cat1`'s view.
        //
        // The point of the test: `cat1.update::<RegisterNodeOp>` must
        // not surface `RecordExceedsCommittedFeatureLevel` based on the
        // stale snapshot. The catch-up retry should load `cat2`'s log,
        // refresh `cat1`'s committed level, and persist the registration
        // at the next free sequence.
        use crate::catalog::versions::v3::ops::feature_level::{
            AdvanceFeatureLevelArgs, AdvanceFeatureLevelOp,
        };
        use crate::format::derive_feature_level;

        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.reload().await;
        let cat2 = test_catalog.reload().await;

        // Floor both `committed_feature_level`s so AdvanceFeatureLevel
        // (id 1) is the only record either catalog accepts via the
        // gate; stays low for `cat1` for the rest of the test.
        let stale = FeatureLevel {
            core: 1,
            enterprise: 0,
        };
        cat1.inner.write().committed_feature_level = stale;
        cat2.inner.write().committed_feature_level = stale;

        // `cat2` advances the cluster version; `cat1` still holds the
        // original value in memory.
        let derived = derive_feature_level();
        cat2.update::<AdvanceFeatureLevelOp>(AdvanceFeatureLevelArgs { committed: derived })
            .await
            .expect("cat2 must advance committed");

        // `cat1.update` would otherwise reject `RegisterNode` (id 2)
        // against the stale committed. The catch-up retry should
        // observe `cat2`'s AdvanceFeatureLevel log, refresh, and
        // persist at the next free sequence after that.
        let cat2_post_advance_seq = cat2.sequence_number();
        cat1.update::<RegisterNodeOp>(register_args("a"))
            .await
            .expect("update must catch up past the stale gate failure");

        assert_eq!(cat1.sequence_number(), cat2_post_advance_seq.next());
        assert_eq!(cat1.inner.read().committed_feature_level, derived);
    }
}

// ---------------------------------------------------------------------------
// Startup fast-fail
// ---------------------------------------------------------------------------

mod startup_fast_fail {
    use super::*;
    use crate::CatalogError;
    use crate::catalog::versions::v3::ops::feature_level::{
        AdvanceFeatureLevelArgs, AdvanceFeatureLevelOp,
    };
    use crate::format::{FeatureLevel, derive_feature_level};

    #[tokio::test]
    async fn load_or_create_rejects_committed_above_local() {
        // Persist a catalog whose committed feature level exceeds what
        // the local binary derives — the simulated case of an
        // "older" node booting against a cluster that has already
        // upgraded.
        let test_catalog = TestCatalog::new();
        let derived = derive_feature_level();
        let elevated = FeatureLevel {
            core: derived.core + 1,
            enterprise: derived.enterprise,
        };

        {
            let cat = test_catalog.reload().await;
            cat.update::<AdvanceFeatureLevelOp>(AdvanceFeatureLevelArgs {
                committed: elevated,
            })
            .await
            .expect("advancing committed to derived+1 must succeed");
        }

        let err = test_load_or_create("p", test_catalog.store())
            .await
            .expect_err("reload must fail when committed exceeds local");
        let CatalogError::NodeBelowCommittedFeatureLevel { committed, local } = err else {
            panic!("expected NodeBelowCommittedFeatureLevel, got {err:?}");
        };
        assert_eq!(committed, elevated);
        assert_eq!(local, derived);
    }

    #[tokio::test]
    async fn load_or_create_succeeds_when_committed_equals_local() {
        // A fresh catalog should initialize with the local process's
        // derived feature level.
        let catalog = TestCatalog::new().reload().await;
        let committed = catalog.inner.read().committed_feature_level;
        assert_eq!(derive_feature_level(), committed);
    }

    #[tokio::test]
    async fn load_or_create_migrates_v2_catalog_in_place() {
        // A v2 catalog at the prefix is converted to v3 by the
        // migration runner invoked from `load_or_create`. Subsequent
        // loads from the same prefix find the v3 snapshot directly.
        use crate::catalog::versions::v2::InnerCatalog as V2InnerCatalog;
        use crate::catalog::versions::v2::Snapshot as V2Snapshot;
        use crate::log::versions::v4::StorageMode as V2StorageMode;
        use crate::object_store::versions as ostore;

        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let prefix: Arc<str> = Arc::from("v2-prefix");
        let v2_uuid = Uuid::new_v4();

        // Seed a v2 catalog at the prefix.
        let v2_inner = V2InnerCatalog::new(Arc::clone(&prefix), v2_uuid);
        let v2_store = ostore::v2::ObjectStoreCatalog::new(
            Arc::clone(&prefix),
            u64::MAX,
            Arc::clone(&shared),
            V2StorageMode::default(),
        );
        v2_store
            .persist_catalog_checkpoint(&v2_inner.snapshot())
            .await
            .unwrap();

        // First load_or_create runs the migration.
        let cat = test_load_or_create("v2-prefix", Arc::clone(&shared))
            .await
            .expect("load_or_create must run the migration");
        assert_eq!(cat.catalog_uuid(), v2_uuid);

        // Second load_or_create finds the v3 snapshot already and is a
        // no-op on the migration side.
        let cat2 = test_load_or_create("v2-prefix", shared)
            .await
            .expect("second load_or_create after migration");
        assert_eq!(cat2.catalog_uuid(), v2_uuid);
    }
}

// ---------------------------------------------------------------------------
// Read-only accessors
// ---------------------------------------------------------------------------

mod read_only {
    use super::*;

    #[tokio::test]
    async fn identity_accessors_round_trip_construction_values() {
        let store = ObjectStoreCatalog::new(
            "store-prefix",
            Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>,
            StorageMode::default(),
        );
        let inner = InnerCatalog::new(Arc::from("cluster-1"), Uuid::nil());
        let catalog = Catalog::from_parts(
            inner,
            store,
            None,
            test_time_provider(),
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        );

        assert_eq!(catalog.catalog_id().as_ref(), "cluster-1");
        assert_eq!(catalog.catalog_uuid(), Uuid::nil());
        assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));
        assert_eq!(catalog.shard_count(), NonZeroU32::MIN);
        assert_eq!(catalog.object_store_prefix().as_ref(), "store-prefix");
    }

    #[tokio::test]
    async fn time_provider_returns_shared_instance() {
        let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let time_provider: Arc<dyn TimeProvider> = Arc::clone(&mock) as _;
        let store = test_store_with_prefix("p");
        let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
        let catalog = Catalog::from_parts(
            inner,
            store,
            None,
            time_provider,
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        );

        assert_eq!(catalog.time_provider().now(), mock.now());
        mock.set(Time::from_timestamp_nanos(42));
        assert_eq!(catalog.time_provider().now().timestamp_nanos(), 42);
    }

    #[tokio::test]
    async fn object_store_returns_underlying_handle() {
        let test_catalog = TestCatalog::new();
        let catalog = test_catalog.catalog();

        let returned = catalog.object_store();
        // Same pointer: the catalog hands back the very `Arc` it was
        // constructed with, not a clone of the underlying store.
        assert!(Arc::ptr_eq(&returned, &test_catalog.store()));
    }

    #[tokio::test]
    async fn license_paths_derive_from_catalog_id() {
        let store = test_store_with_prefix("p");
        let inner = InnerCatalog::new(Arc::from("cluster-x"), Uuid::nil());
        let catalog = Catalog::from_parts(
            inner,
            store,
            None,
            test_time_provider(),
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        );

        assert_eq!(
            catalog.commercial_license_file_path().as_ref(),
            "cluster-x/commercial_license"
        );
        assert_eq!(
            catalog.trial_or_home_license_file_path().as_ref(),
            "cluster-x/trial_or_home_license"
        );
    }

    #[tokio::test]
    async fn clone_inner_returns_disconnected_copy() {
        let catalog = TestCatalog::new().catalog();
        let snapshot = catalog.clone_inner();
        assert_eq!(snapshot.catalog_id.as_ref(), "test");
        assert_eq!(snapshot.sequence, CatalogSequenceNumber::new(0));
    }

    #[tokio::test]
    async fn minimum_row_delete_predicate_version_is_none_without_nodes() {
        let catalog = TestCatalog::new().catalog();
        assert!(
            catalog
                .minimum_supported_row_delete_predicate_version()
                .is_none()
        );
    }

    #[tokio::test]
    async fn list_namespaces_is_empty_without_databases() {
        let catalog = TestCatalog::new().catalog();
        assert!(catalog.list_namespaces().is_empty());
    }

    #[tokio::test]
    async fn set_state_shutdown_is_idempotent_and_advisory() {
        // The signal is advisory; subsequent reads should still work.
        let catalog = TestCatalog::new().catalog();
        catalog.set_state_shutdown();
        catalog.set_state_shutdown();
        assert_eq!(catalog.sequence_number(), CatalogSequenceNumber::new(0));
    }
}

// ---------------------------------------------------------------------------
// background_update — periodic catch-up loop
// ---------------------------------------------------------------------------

mod background_update {
    use super::*;
    use influxdb3_shutdown::ShutdownManager;

    /// Build a Catalog backed by `store`, parameterized with the given
    /// time provider so the test can drive sleep wakeups deterministically.
    fn catalog_with_time(store: ObjectStoreCatalog, time: Arc<dyn TimeProvider>) -> Catalog {
        let inner = InnerCatalog::new(Arc::from("test"), Uuid::nil());
        Catalog::from_parts(
            inner,
            store,
            None,
            time,
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        )
    }

    #[tokio::test]
    async fn catches_up_after_remote_writer_lands_a_log() {
        // cat_a runs the background loop; cat_b is a separate process
        // sharing the same object store that writes a log. Use real
        // tokio time with a short cadence so the test resolves quickly
        // without juggling mock-time wakeups against an async catch-up.
        let test_catalog = TestCatalog::new();
        let store_a = ObjectStoreCatalog::new("p", test_catalog.store(), StorageMode::default());

        let cat_a = Arc::new(catalog_with_time(
            store_a,
            Arc::new(iox_time::SystemProvider::new()) as Arc<dyn TimeProvider>,
        ));
        let cat_b = test_catalog.catalog();

        let manager = ShutdownManager::new_testing();
        let token = manager.register("background-update-test");

        let cat_a_clone = Arc::clone(&cat_a);
        let task = tokio::spawn(async move {
            cat_a_clone
                .background_update(Duration::from_millis(20), token)
                .await;
        });

        cat_b
            .update::<RegisterNodeOp>(register_args("node-from-b"))
            .await
            .unwrap();
        assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(1));

        let caught_up = tokio::time::timeout(Duration::from_secs(5), async {
            while cat_a.sequence_number() == CatalogSequenceNumber::new(0) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(caught_up.is_ok(), "background loop did not catch up");
        assert_eq!(cat_a.sequence_number(), CatalogSequenceNumber::new(1));
        assert!(cat_a.inner.read().nodes.contains_name("node-from-b"));

        manager.shutdown();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop should exit on shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_token_exits_loop_during_sleep() {
        let store = test_store_with_prefix("p");
        let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Arc::new(catalog_with_time(
            store,
            Arc::clone(&mock) as Arc<dyn TimeProvider>,
        ));

        let manager = ShutdownManager::new_testing();
        let token = manager.register("shutdown-during-sleep");

        let cat_clone = Arc::clone(&catalog);
        let task = tokio::spawn(async move {
            cat_clone
                .background_update(Duration::from_secs(3600), token)
                .await;
        });

        // Yield once to ensure the loop is parked in `sleep_until`.
        tokio::task::yield_now().await;
        manager.shutdown();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("loop should exit promptly on shutdown")
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// update_to_sequence_number — bounded catch-up
// ---------------------------------------------------------------------------

mod bounded_catch_up {
    use super::*;

    #[tokio::test]
    async fn applies_logs_up_to_target_then_stops() {
        // Two catalogs sharing one object store. cat_a writes three
        // logs; cat_b is told to catch up only to seq 2. The third
        // log (seq 3) must remain unread on cat_b.
        let test_catalog = TestCatalog::new();
        let cat_a = test_catalog.catalog();
        let cat_b = test_catalog.catalog();

        for name in ["node-a", "node-b", "node-c"] {
            cat_a
                .update::<RegisterNodeOp>(register_args(name))
                .await
                .unwrap();
        }
        assert_eq!(cat_a.sequence_number(), CatalogSequenceNumber::new(3));
        assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(0));

        cat_b
            .update_to_sequence_number(CatalogSequenceNumber::new(2))
            .await
            .unwrap();
        assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(2));
        // Scope the read guard so it is dropped before the await below —
        // parking_lot guards are !Send.
        {
            let inner = cat_b.inner.read();
            assert!(inner.nodes.contains_name("node-a"));
            assert!(inner.nodes.contains_name("node-b"));
            assert!(
                !inner.nodes.contains_name("node-c"),
                "log 3 must not be applied",
            );
        }

        // Calling again with a target at or below the current
        // sequence is a no-op.
        cat_b
            .update_to_sequence_number(CatalogSequenceNumber::new(1))
            .await
            .unwrap();
        assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(2));
    }

    #[tokio::test]
    async fn broadcasts_each_applied_log_to_subscribers() {
        // cat_b subscribes BEFORE catching up; each log applied via
        // bounded catch-up should arrive as its own CatalogUpdate.
        // Caches, processing engine, and the deleter rely on this
        // path for updates that arrive via background poll, not via
        // the local write path.
        let test_catalog = TestCatalog::new();
        let cat_a = test_catalog.catalog();
        let cat_b = Arc::new(test_catalog.catalog());

        for name in ["node-a", "node-b"] {
            cat_a
                .update::<RegisterNodeOp>(register_args(name))
                .await
                .unwrap();
        }

        let mut rx = cat_b.subscribe_to_updates("catchup-observer").await;

        // Catch up runs in a task because each broadcast waits for
        // the subscriber to ACK before moving to the next log.
        let cat_b_clone = Arc::clone(&cat_b);
        let catch_up_task = tokio::spawn(async move {
            cat_b_clone
                .update_to_sequence_number(CatalogSequenceNumber::new(2))
                .await
                .unwrap();
        });

        let mut received_batches = 0;
        while let Some(msg) = rx.recv().await {
            received_batches += 1;
            // Dropping ACKs the batch so the next applied log can broadcast.
            drop(msg);
            if received_batches == 2 {
                break;
            }
        }
        catch_up_task.await.unwrap();
        assert_eq!(
            received_batches, 2,
            "each applied log produces its own update"
        );
        assert_eq!(cat_b.sequence_number(), CatalogSequenceNumber::new(2));
    }
}

// ---------------------------------------------------------------------------
// Node mutations
// ---------------------------------------------------------------------------

mod node_mutations {
    use super::*;
    use crate::CatalogError;
    use influxdb3_process::ProcessUuidWrapper;

    fn process_uuid_getter() -> Arc<dyn influxdb3_process::ProcessUuidGetter> {
        Arc::new(ProcessUuidWrapper::new())
    }

    #[tokio::test]
    async fn register_then_stop_round_trip() {
        let catalog = TestCatalog::new().catalog();

        let registered = catalog
            .register_node(
                "node-a",
                4,
                vec![NodeMode::Core],
                process_uuid_getter(),
                Arc::from("inst-1"),
                Some("localhost:8080".to_string()),
                Some("--verbose".to_string()),
                3,
            )
            .await
            .unwrap();
        assert_eq!(registered.node_id().as_ref(), "node-a");
        assert!(registered.is_running());
        assert_eq!(registered.row_delete_predicate_version(), 3);

        // Graceful stop records the calling process's UUID.
        let stopped = catalog
            .update_node_state_stopped("node-a", process_uuid_getter())
            .await
            .unwrap();
        assert!(stopped.state().is_stopped());

        // Re-registering after a stop succeeds (allowed by op).
        catalog
            .register_node(
                "node-a",
                8,
                vec![NodeMode::All],
                process_uuid_getter(),
                Arc::from("inst-2"),
                None,
                None,
                3,
            )
            .await
            .unwrap();
        let again = catalog.node("node-a").unwrap();
        assert!(again.is_running());
        assert_eq!(again.instance_id().as_ref(), "inst-2");
    }

    #[tokio::test]
    async fn admin_stop_uses_nil_process_uuid() {
        // The administrative path doesn't carry a calling process UUID;
        // the operator simply orders the node to stop. The behavior we
        // observe externally is the state transition; the nil UUID is
        // an implementation detail of the StopNode record.
        let catalog = TestCatalog::new().catalog();
        catalog
            .register_node(
                "node-a",
                4,
                vec![NodeMode::Core],
                process_uuid_getter(),
                Arc::from("inst-1"),
                None,
                None,
                0,
            )
            .await
            .unwrap();
        let stopped = catalog.stop_node("node-a").await.unwrap();
        assert!(stopped.state().is_stopped());

        // Stopping a missing node surfaces NotFound.
        let err = catalog.stop_node("ghost").await.unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(name) if name.contains("ghost")),
            "got {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Current node accessors
// ---------------------------------------------------------------------------

mod current_node {
    use super::*;
    use crate::CatalogError;
    use crate::catalog::versions::v3::schema::node::NodeSpec;
    use influxdb3_id::NodeId;
    use influxdb3_process::ProcessUuidWrapper;

    fn process_uuid_getter() -> Arc<dyn influxdb3_process::ProcessUuidGetter> {
        Arc::new(ProcessUuidWrapper::new())
    }

    async fn register(catalog: &Catalog, node_id: &str, cores: u64) {
        catalog
            .register_node(
                node_id,
                cores,
                vec![NodeMode::Core],
                process_uuid_getter(),
                Arc::from(format!("inst-{node_id}")),
                None,
                None,
                0,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unset_falls_back_to_catalog_id() {
        let catalog = TestCatalog::new().catalog();
        // Inner catalog uses "test" as the catalog_id; current_node_id falls
        // back to that since no node was configured at construction.
        assert_eq!(catalog.current_node_id().as_ref(), "test");
        let err = catalog.current_node().unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(detail) if detail.contains("current node")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn resolves_registered_node() {
        let catalog = test_catalog_with_current_node("cluster-1", "node-a");
        register(&catalog, "node-a", 4).await;

        assert_eq!(catalog.current_node_id().as_ref(), "node-a");
        let node = catalog.current_node().unwrap();
        assert_eq!(node.node_id().as_ref(), "node-a");
    }

    #[tokio::test]
    async fn current_node_errors_when_unregistered() {
        let catalog = test_catalog_with_current_node("cluster-1", "node-a");
        // current_node_id is set, but no RegisterNode landed yet.
        let err = catalog.current_node().unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(detail) if detail.contains("node-a")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn matches_node_spec_all_and_explicit() {
        let catalog = test_catalog_with_current_node("cluster-1", "node-a");
        register(&catalog, "node-a", 4).await;
        register(&catalog, "node-b", 8).await;
        let me = catalog.current_node().unwrap().node_catalog_id();
        let other = catalog.node("node-b").unwrap().node_catalog_id();

        assert!(catalog.matches_node_spec(&NodeSpec::All).unwrap());
        assert!(
            catalog
                .matches_node_spec(&NodeSpec::Nodes(vec![me]))
                .unwrap()
        );
        assert!(
            !catalog
                .matches_node_spec(&NodeSpec::Nodes(vec![other]))
                .unwrap()
        );
    }

    #[tokio::test]
    async fn matches_node_spec_explicit_without_current_node_errors() {
        let catalog = TestCatalog::new().catalog();
        let err = catalog
            .matches_node_spec(&NodeSpec::Nodes(vec![NodeId::new(0)]))
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(detail) if detail.contains("current node")),
            "got {err:?}"
        );
        // `All` short-circuits and never touches the current-node state.
        assert!(catalog.matches_node_spec(&NodeSpec::All).unwrap());
    }

    #[tokio::test]
    async fn cores_sums_running_nodes_only() {
        let catalog = test_catalog_with_current_node("cluster-1", "node-a");
        register(&catalog, "node-a", 4).await;
        register(&catalog, "node-b", 8).await;
        register(&catalog, "node-c", 16).await;

        // All nodes running: full sum.
        assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 28);
        // Excluding the current node (node-a, 4 cores).
        assert_eq!(catalog.total_active_cores_in_use_by_other_nodes(), 24);

        // Stop node-b. Both totals exclude its 8 cores.
        catalog.stop_node("node-b").await.unwrap();
        assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 20);
        assert_eq!(catalog.total_active_cores_in_use_by_other_nodes(), 16);
    }
}

// ---------------------------------------------------------------------------
// Database read accessors
// ---------------------------------------------------------------------------

mod db_lookups {
    use super::*;
    use crate::catalog::DeletionStatus;
    use crate::catalog::versions::v3::ops::database::{
        CreateDatabaseArgs, CreateDatabaseOp, SoftDeleteDatabaseArgs, SoftDeleteDatabaseOp,
    };
    use crate::resource::CatalogResource;
    use influxdb3_id::DbId;

    struct Seeded {
        catalog: Catalog,
        mock: Arc<MockProvider>,
        alpha_id: DbId,
        beta_id: DbId,
        gamma_id: DbId,
    }

    /// Build a catalog seeded with two nodes and three databases.
    /// `beta` is soft-deleted with a hard-delete time of t=2000 — note
    /// that soft-delete renames the database in the catalog, so the
    /// original "beta" name no longer resolves; tests look it up via
    /// the captured `beta_id`.
    async fn seeded_catalog() -> Seeded {
        let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
        let time_provider: Arc<dyn TimeProvider> = Arc::clone(&mock) as _;
        let catalog = Catalog::from_parts(
            InnerCatalog::new(Arc::from("test"), Uuid::nil()),
            test_store_with_prefix("p"),
            None,
            time_provider,
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            Catalog::DEFAULT_HARD_DELETE_DURATION,
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        );

        catalog
            .update::<RegisterNodeOp>(register_args("node-a"))
            .await
            .unwrap();
        catalog
            .update::<RegisterNodeOp>(register_args("node-b"))
            .await
            .unwrap();

        let alpha = catalog
            .update::<CreateDatabaseOp>(CreateDatabaseArgs {
                name: "alpha".to_string(),
                retention_period: None,
            })
            .await
            .unwrap();
        let beta = catalog
            .update::<CreateDatabaseOp>(CreateDatabaseArgs {
                name: "beta".to_string(),
                retention_period: None,
            })
            .await
            .unwrap();
        let gamma = catalog
            .update::<CreateDatabaseOp>(CreateDatabaseArgs {
                name: "gamma".to_string(),
                retention_period: None,
            })
            .await
            .unwrap();

        catalog
            .update::<SoftDeleteDatabaseOp>(SoftDeleteDatabaseArgs {
                database_name: "beta".to_string(),
                deletion_time: Time::from_timestamp_nanos(1000),
                hard_delete_time: Some(Time::from_timestamp_nanos(2000)),
                hard_delete_scope: None,
            })
            .await
            .unwrap();

        Seeded {
            catalog,
            mock,
            alpha_id: alpha.id(),
            beta_id: beta.id(),
            gamma_id: gamma.id(),
        }
    }

    #[tokio::test]
    async fn db_and_node_lookups() {
        let Seeded {
            catalog,
            alpha_id,
            beta_id,
            gamma_id,
            ..
        } = seeded_catalog().await;

        // `alpha` and `gamma` are live; `beta` is soft-deleted (and
        // therefore tombstone-renamed in the id_name_map), but
        // resolvable by id.
        assert!(catalog.db_exists(alpha_id));
        assert!(catalog.db_exists(beta_id));
        assert_eq!(catalog.db_name_to_id("alpha"), Some(alpha_id));
        assert_eq!(catalog.db_name_to_id("beta"), None);
        assert_eq!(catalog.db_id_to_name(&alpha_id).as_deref(), Some("alpha"));
        assert_eq!(catalog.db_schema("alpha").map(|d| d.id()), Some(alpha_id));
        assert_eq!(
            catalog.db_schema_by_id(&gamma_id).map(|d| d.name()),
            Some(Arc::from("gamma"))
        );
        assert_eq!(catalog.database_count(), 3);
        assert_eq!(catalog.table_count(), 0);

        // db_names filters soft-deleted entries; list_db_schema and
        // list_namespaces return everything.
        let mut db_names = catalog.db_names();
        db_names.sort();
        assert_eq!(db_names, vec!["alpha".to_string(), "gamma".to_string()]);
        assert_eq!(catalog.list_db_schema().len(), 3);
        assert_eq!(catalog.list_namespaces().len(), 3);

        let allocated_ids = [alpha_id, beta_id, gamma_id];
        assert!(allocated_ids.iter().all(|id| *id < catalog.next_db_id()));
    }

    #[tokio::test]
    async fn deletion_status_and_hard_deleted_listing() {
        let Seeded {
            catalog,
            mock,
            alpha_id,
            beta_id,
            ..
        } = seeded_catalog().await;

        // At t=1000, beta is soft-deleted (hard-delete time is 2000, not
        // yet reached), alpha is live.
        assert_eq!(catalog.database_deletion_status(alpha_id), None);
        assert_eq!(
            catalog.database_deletion_status(beta_id),
            Some(DeletionStatus::Soft)
        );
        assert!(catalog.list_hard_deleted_dbs_tables(10).is_empty());

        // Advance past the hard-delete threshold. `Hard` carries the
        // duration since the hard-delete time.
        mock.set(Time::from_timestamp_nanos(5_000));
        let beta_status = catalog.database_deletion_status(beta_id).unwrap();
        assert!(matches!(beta_status, DeletionStatus::Hard(_)));
        let hard = catalog.list_hard_deleted_dbs_tables(10);
        assert_eq!(hard, vec![(beta_id, None)]);

        // A non-existent db reports NotFound.
        let missing = DbId::new(9_999);
        assert_eq!(
            catalog.database_deletion_status(missing),
            Some(DeletionStatus::NotFound)
        );
    }
}

// ---------------------------------------------------------------------------
// Database mutation methods
// ---------------------------------------------------------------------------

mod db_mutations {
    use super::*;
    use crate::CatalogError;
    use crate::catalog::INTERNAL_DB_NAME;
    use crate::catalog::versions::v3::catalog::{CreateDatabaseOptions, HardDeletionTime};
    use crate::catalog::versions::v3::deletes::DeletionScope;
    use crate::resource::CatalogResource;
    use influxdb3_id::DbId;
    use std::ops::Add;

    async fn create_delete_recreate_db(catalog: &Catalog, name: &str) -> (DbId, DbId) {
        let first_id = catalog.create_database(name).await.unwrap().id();
        soft_delete_db_now(catalog, name).await;
        let second_id = catalog.create_database(name).await.unwrap().id();
        (first_id, second_id)
    }

    #[tokio::test]
    async fn create_and_opts_variants_roundtrip() {
        use crate::catalog::versions::v3::schema::retention::RetentionPeriod;

        let catalog = TestCatalog::new().catalog();

        let foo = catalog.create_database("foo").await.unwrap();
        assert_eq!(foo.name().as_ref(), "foo");
        assert!(matches!(foo.retention_period, RetentionPeriod::Indefinite));

        // Re-creating an existing database surfaces AlreadyExists.
        let dup = catalog.create_database("foo").await.unwrap_err();
        assert!(matches!(dup, CatalogError::AlreadyExists), "got {dup:?}");

        // Options-bearing variant carries the retention period through
        // to the database schema.
        let bar = catalog
            .create_database_opts(
                "bar",
                CreateDatabaseOptions::default().retention_period(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        assert_eq!(
            bar.retention_period,
            RetentionPeriod::Duration(Duration::from_secs(60))
        );
        assert_eq!(catalog.list_db_schema().len(), 2);
    }

    #[tokio::test]
    async fn soft_delete_same_second_db_name_collision_keeps_repo_consistent() {
        let catalog = TestCatalog::new().catalog();
        let (first_id, second_id) = create_delete_recreate_db(&catalog, "sales").await;

        // second delete stamps the same-second name the first delete already used
        soft_delete_db_now(&catalog, "sales").await;

        // both deleted databases must remain consistent in the databases repo
        let inner = catalog.inner.read();
        assert_both_ids_live_and_distinct(&inner.databases, first_id, second_id);
    }

    #[tokio::test]
    async fn soft_delete_then_hard_delete() {
        let mock = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1_000)));
        let time_provider: Arc<dyn TimeProvider> = Arc::clone(&mock) as _;
        let catalog = Catalog::from_parts(
            InnerCatalog::new(Arc::from("test"), Uuid::nil()),
            test_store_with_prefix("p"),
            None,
            time_provider,
            Arc::new(Registry::new()),
            NonZeroU32::MIN,
            // Short default so HardDeletionTime::Default is observable.
            Duration::from_secs(60),
            CheckpointPolicy::default(),
            Arc::new(CatalogLimits::none()),
        );

        let target = catalog.create_database("doomed").await.unwrap();
        let target_id = target.id();

        // Soft delete with the catalog default duration. The resolved
        // hard-delete time should be `now + default_duration`.
        let deleted = catalog
            .soft_delete_database(
                "doomed",
                HardDeletionTime::Default,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap();
        assert_eq!(deleted.id(), target_id);
        assert!(deleted.deleted);
        assert_eq!(
            deleted.hard_delete_time.unwrap().timestamp_nanos(),
            Time::from_timestamp_nanos(1_000)
                .add(Duration::from_secs(60))
                .timestamp_nanos(),
        );

        // Soft-delete renames the database so the original name no
        // longer resolves; a repeat call with the same name surfaces
        // NotFound from the underlying op.
        let dup = catalog
            .soft_delete_database(
                "doomed",
                HardDeletionTime::Default,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&dup, CatalogError::NotFound(name) if name == "doomed"),
            "got {dup:?}"
        );

        // Hard-delete drops the database from the catalog.
        catalog.hard_delete_database(&target_id).await.unwrap();
        assert!(!catalog.db_exists(target_id));
    }

    #[tokio::test]
    async fn soft_delete_internal_database_is_rejected() {
        let catalog = TestCatalog::new().catalog();
        let err = catalog
            .soft_delete_database(
                INTERNAL_DB_NAME,
                HardDeletionTime::Now,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, CatalogError::CannotDeleteInternalDatabase),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn retention_period_set_and_clear() {
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("logs").await.unwrap();

        catalog
            .set_retention_period_for_database("logs", Duration::from_secs(3_600))
            .await
            .unwrap();

        catalog
            .clear_retention_period_for_database("logs")
            .await
            .unwrap();

        // After soft-delete the database is renamed in the catalog,
        // so retention ops keyed by the original name surface
        // NotFound rather than AlreadyDeleted.
        catalog
            .soft_delete_database("logs", HardDeletionTime::Now, DeletionScope::DataAndCatalog)
            .await
            .unwrap();
        let err = catalog
            .set_retention_period_for_database("logs", Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(name) if name == "logs"),
            "got {err:?}"
        );

        // Missing-database lookup also returns NotFound.
        let err = catalog
            .clear_retention_period_for_database("nope")
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(name) if name == "nope"),
            "got {err:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Table mutation methods
// ---------------------------------------------------------------------------

mod table_mutations {
    use super::*;
    use crate::CatalogError;
    use crate::catalog::versions::v3::catalog::{CreateTableOptions, HardDeletionTime};
    use crate::catalog::versions::v3::deletes::DeletionScope;
    use crate::catalog::versions::v3::schema::column::FieldDataType;
    use crate::resource::CatalogResource;
    use influxdb3_id::{DbId, TableId};

    fn columns() -> (&'static [&'static str], Vec<(&'static str, FieldDataType)>) {
        (
            &["host", "region"],
            vec![
                ("temp", FieldDataType::Float),
                ("status", FieldDataType::String),
            ],
        )
    }

    async fn create_delete_recreate_table(
        catalog: &Catalog,
        db: &str,
        table: &str,
    ) -> (DbId, TableId, TableId) {
        let (tags, fields) = columns();
        let first_id = catalog
            .create_table(db, table, tags, &fields)
            .await
            .unwrap()
            .id();
        let db_id = catalog.db_name_to_id(db).unwrap();
        soft_delete_table_now(catalog, db, table).await;
        let second_id = catalog
            .create_table(db, table, tags, &fields)
            .await
            .unwrap()
            .id();
        (db_id, first_id, second_id)
    }

    #[tokio::test]
    async fn create_and_opts_variants_roundtrip() {
        let catalog = TestCatalog::new().catalog();
        let (tags, fields) = columns();

        // create_table auto-creates the database (schema-on-write).
        let tbl = catalog
            .create_table("metrics", "cpu", tags, &fields)
            .await
            .unwrap();
        assert_eq!(tbl.table_name.as_ref(), "cpu");
        assert!(catalog.db_name_to_id("metrics").is_some());

        // Re-creating an existing table is rejected.
        let err = catalog
            .create_table("metrics", "cpu", tags, &fields)
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::AlreadyExists), "got {err:?}");

        // Options-bearing variant carries its retention period through.
        let _other = catalog
            .create_table_opts(
                "metrics",
                "cpu_short",
                tags,
                &fields,
                CreateTableOptions::default().retention_period(std::time::Duration::from_secs(60)),
            )
            .await
            .unwrap();
        let metrics = catalog.db_schema("metrics").unwrap();
        assert_eq!(metrics.tables().count(), 2);
        assert_eq!(catalog.table_count(), 2);
    }

    #[tokio::test]
    async fn soft_delete_then_hard_delete() {
        let catalog = TestCatalog::new().catalog();
        let (tags, fields) = columns();
        let tbl = catalog
            .create_table("metrics", "cpu", tags, &fields)
            .await
            .unwrap();
        let tbl_id = tbl.id();
        let db_id = catalog.db_name_to_id("metrics").unwrap();

        let deleted = catalog
            .soft_delete_table(
                "metrics",
                "cpu",
                HardDeletionTime::Now,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap();
        assert_eq!(deleted.id(), tbl_id);
        assert!(deleted.deleted);

        // The soft-deleted table is observable via table_deletion_status.
        assert!(catalog.table_deletion_status(db_id, tbl_id).is_some());

        // Hard-delete drops the table from the catalog.
        catalog.hard_delete_table(&db_id, &tbl_id).await.unwrap();
        let metrics = catalog.db_schema_by_id(&db_id).unwrap();
        assert!(metrics.table_definition_by_id(&tbl_id).is_none());
    }

    #[tokio::test]
    async fn soft_delete_same_second_name_collision_keeps_repo_consistent() {
        let catalog = TestCatalog::new().catalog();
        let (db_id, first_id, second_id) =
            create_delete_recreate_table(&catalog, "metrics", "cpu").await;

        // second delete stamps the same-second name the first delete already used
        soft_delete_table_now(&catalog, "metrics", "cpu").await;

        // both deleted tables stay addressable; pre-fix the first was evicted
        let db = catalog.db_schema_by_id(&db_id).unwrap();
        assert_both_ids_live_and_distinct(&db.tables, first_id, second_id);
    }

    #[tokio::test]
    async fn soft_delete_database_cascade_table_name_collision_keeps_repo_consistent() {
        let catalog = TestCatalog::new().catalog();
        let (db_id, first_id, second_id) =
            create_delete_recreate_table(&catalog, "metrics", "cpu").await;

        // cascading db delete renames the live table into the already-used deleted name
        soft_delete_db_now(&catalog, "metrics").await;

        // the cascade-renamed table must not evict the earlier deleted one
        let inner = catalog.inner.read();
        let db = inner.databases.get_by_id(&db_id).unwrap();
        assert_both_ids_live_and_distinct(&db.tables, first_id, second_id);
    }

    #[tokio::test]
    async fn soft_delete_same_second_collision_survives_reload() {
        let test_catalog = TestCatalog::new();
        let catalog = test_catalog.reload().await;
        let (db_id, first_id, second_id) =
            create_delete_recreate_table(&catalog, "metrics", "cpu").await;
        soft_delete_table_now(&catalog, "metrics", "cpu").await;
        drop(catalog);

        // replaying the persisted log must reproduce the disambiguated names, not the desync
        let reloaded = test_catalog.reload().await;
        let db = reloaded.db_schema_by_id(&db_id).unwrap();
        assert_both_ids_live_and_distinct(&db.tables, first_id, second_id);
    }

    #[tokio::test]
    async fn soft_delete_missing_table_returns_table_not_found() {
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("metrics").await.unwrap();

        let err = catalog
            .soft_delete_table(
                "metrics",
                "ghost",
                HardDeletionTime::Now,
                DeletionScope::DataAndCatalog,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                CatalogError::TableNotFound { db_name, table_name }
                    if db_name.as_ref() == "metrics" && table_name.as_ref() == "ghost"
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn hard_delete_unknown_db_or_table_returns_not_found() {
        let catalog = TestCatalog::new().catalog();
        let err = catalog
            .hard_delete_table(
                &influxdb3_id::DbId::new(9_999),
                &influxdb3_id::TableId::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::DatabaseNotFound { db_name } if db_name.as_ref() == "db_id=9999"),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn retention_period_set_and_clear() {
        let catalog = TestCatalog::new().catalog();
        let (tags, fields) = columns();
        catalog
            .create_table("metrics", "cpu", tags, &fields)
            .await
            .unwrap();

        catalog
            .set_retention_period_for_table("metrics", "cpu", std::time::Duration::from_secs(3_600))
            .await
            .unwrap();

        catalog
            .clear_retention_period_for_table("metrics", "cpu")
            .await
            .unwrap();

        // Missing table surfaces TableNotFound.
        let err = catalog
            .set_retention_period_for_table("metrics", "ghost", std::time::Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                CatalogError::TableNotFound { db_name, table_name }
                    if db_name.as_ref() == "metrics" && table_name.as_ref() == "ghost"
            ),
            "got {err:?}"
        );
    }
}

mod cluster_config {
    use super::*;
    use crate::CatalogError;

    #[tokio::test]
    async fn set_gen1_duration_round_trip() {
        let catalog = TestCatalog::new().catalog();

        catalog
            .set_gen1_duration(Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            catalog.get_generation_duration(1),
            Some(Duration::from_secs(60))
        );

        // Same duration is a no-op.
        let err = catalog
            .set_gen1_duration(Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NoCatalogChange { .. }),
            "got {err:?}"
        );

        // A different duration is rejected.
        let err = catalog
            .set_gen1_duration(Duration::from_secs(120))
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                CatalogError::CannotChangeGenerationDuration { level: 1, .. }
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn set_all_generation_durations_aligned_multi_level() {
        let catalog = TestCatalog::new().catalog();

        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),
                Duration::from_secs(600),
                Duration::from_secs(3600),
            ])
            .await
            .unwrap();
        assert_eq!(
            catalog.get_generation_duration(1),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            catalog.get_generation_duration(2),
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            catalog.get_generation_duration(3),
            Some(Duration::from_secs(3600))
        );

        // Re-issuing the same set is `AlreadyExists`.
        let err = catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),
                Duration::from_secs(600),
                Duration::from_secs(3600),
            ])
            .await
            .unwrap_err();
        assert!(matches!(&err, CatalogError::AlreadyExists), "got {err:?}");

        // Extending to a new level on top of existing levels is allowed.
        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),
                Duration::from_secs(600),
                Duration::from_secs(3600),
                Duration::from_secs(86400),
            ])
            .await
            .unwrap();
        assert_eq!(
            catalog.get_generation_duration(4),
            Some(Duration::from_secs(86400))
        );
    }

    #[tokio::test]
    async fn set_all_generation_durations_empty_is_noop() {
        let catalog = TestCatalog::new().catalog();
        catalog.set_all_generation_durations(&[]).await.unwrap();
        assert_eq!(catalog.get_generation_duration(1), None);
    }

    #[tokio::test]
    async fn set_all_generation_durations_misaligned_rejected() {
        let catalog = TestCatalog::new().catalog();

        // 90 is not a multiple of 60.
        let err = catalog
            .set_all_generation_durations(&[Duration::from_secs(60), Duration::from_secs(90)])
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                CatalogError::Enterprise(
                    crate::error::enterprise::EnterpriseCatalogError::MisalignedGenerations
                )
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn set_all_generation_durations_conflicting_level_rejected() {
        let catalog = TestCatalog::new().catalog();
        catalog
            .set_gen1_duration(Duration::from_secs(60))
            .await
            .unwrap();

        // gen1 is already set to 60s; attempting 120s is rejected.
        let err = catalog
            .set_all_generation_durations(&[Duration::from_secs(120)])
            .await
            .unwrap_err();
        assert!(
            matches!(
                &err,
                CatalogError::CannotChangeGenerationDuration { level: 1, .. }
            ),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn storage_mode_forward_transitions_and_downgrade() {
        let catalog = TestCatalog::new().catalog();
        assert_eq!(catalog.storage_mode(), StorageMode::Parquet);

        // Re-setting current mode is a no-op.
        let err = catalog
            .set_storage_mode(StorageMode::Parquet)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NoCatalogChange { .. }),
            "got {err:?}"
        );

        // Forward transitions: Parquet -> ParquetAndPachaTree -> PachaTree.
        catalog
            .set_storage_mode(StorageMode::ParquetAndPachaTree)
            .await
            .unwrap();
        assert_eq!(catalog.storage_mode(), StorageMode::ParquetAndPachaTree);

        catalog
            .set_storage_mode(StorageMode::PachaTree)
            .await
            .unwrap();
        assert_eq!(catalog.storage_mode(), StorageMode::PachaTree);

        // Backward transition through `set_storage_mode` is rejected.
        let err = catalog
            .set_storage_mode(StorageMode::Parquet)
            .await
            .unwrap_err();
        assert!(matches!(&err, CatalogError::Internal { .. }), "got {err:?}");

        // Operator downgrade jumps back to Parquet from any state.
        catalog.downgrade_storage_mode_to_parquet().await.unwrap();
        assert_eq!(catalog.storage_mode(), StorageMode::Parquet);

        // Re-downgrading from Parquet is a no-op.
        let err = catalog
            .downgrade_storage_mode_to_parquet()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::NoCatalogChange { .. }),
            "got {err:?}"
        );
    }
}

mod tokens {
    use super::*;
    use crate::CatalogError;

    #[tokio::test]
    async fn admin_token_create_regenerate_delete() {
        let catalog = TestCatalog::new().catalog();

        // Initial create succeeds.
        let (info, raw) = catalog.create_admin_token(false).await.unwrap();
        assert!(!raw.is_empty());
        let original_id = info.id;
        let original_hash = info.hash.clone();

        // Re-creating without regenerate hits the duplicate-name guard.
        let err = catalog.create_admin_token(false).await.unwrap_err();
        assert!(
            matches!(&err, CatalogError::TokenNameAlreadyExists(name) if name == "_admin"),
            "got {err:?}"
        );

        // Regenerate rotates the hash on the existing entry; id stays.
        let (regen, _raw2) = catalog.create_admin_token(true).await.unwrap();
        assert_eq!(regen.id, original_id);
        assert_ne!(regen.hash, original_hash);

        // Deleting the operator token via the public path is rejected.
        let err = catalog.delete_token("_admin").await.unwrap_err();
        assert!(
            matches!(err, CatalogError::CannotDeleteOperatorToken),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn token_with_permission_validates_against_catalog() {
        use influxdb3_authz::permissions::PermissionDetailsSpec;
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("metrics").await.unwrap();

        // Happy path: db permission referencing an existing database.
        catalog
            .create_token_with_permission_and_hash(
                vec![PermissionDetailsSpec {
                    resource_type: "db".to_string(),
                    resource_identifier: vec!["metrics".to_string()],
                    actions: vec!["read".to_string()],
                }],
                "scoped-a".to_string(),
                vec![1, 2, 3],
                None,
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_tokens()
                .iter()
                .any(|t| t.name.as_ref() == "scoped-a")
        );

        // Db permission referencing a missing database is rejected. Matches v2:
        // the unknown name surfaces from resource resolution as
        // `CannotParsePermissionForToken(invalid resource name ...)`.
        let err = catalog
            .create_token_with_permission_and_hash(
                vec![PermissionDetailsSpec {
                    resource_type: "db".to_string(),
                    resource_identifier: vec!["ghost".to_string()],
                    actions: vec!["read".to_string()],
                }],
                "scoped-b".to_string(),
                vec![1, 2, 3],
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::CannotParsePermissionForToken(msg)
                if msg.contains("invalid resource name") && msg.contains("ghost")),
            "got {err:?}"
        );

        // Unknown resource type is rejected with a parse error.
        let err = catalog
            .create_token_with_permission_and_hash(
                vec![PermissionDetailsSpec {
                    resource_type: "garbage".to_string(),
                    resource_identifier: vec![],
                    actions: vec!["read".to_string()],
                }],
                "scoped-c".to_string(),
                vec![1, 2, 3],
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::CannotParsePermissionForToken(_)),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn named_admin_token_with_hash() {
        let catalog = TestCatalog::new().catalog();

        catalog
            .create_named_admin_token_with_hash("team-a".to_string(), vec![1, 2, 3, 4], None)
            .await
            .unwrap();
        assert!(
            catalog
                .get_tokens()
                .iter()
                .any(|t| t.name.as_ref() == "team-a")
        );

        // Delete works on non-operator tokens.
        let deleted = catalog.delete_token("team-a").await.unwrap();
        assert_eq!(deleted.name.as_ref(), "team-a");
        assert!(
            !catalog
                .get_tokens()
                .iter()
                .any(|t| t.name.as_ref() == "team-a")
        );

        // Deleting a missing token surfaces NotFound.
        let err = catalog.delete_token("missing").await.unwrap_err();
        assert!(
            matches!(&err, CatalogError::NotFound(name) if name == "missing"),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn get_permission_resolves_db_scoped_grant() {
        use influxdb3_authz::permissions::PermissionDetailsSpec;
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("metrics").await.unwrap();
        catalog.create_database("logs").await.unwrap();

        let scoped_hash = vec![10u8; 32];
        catalog
            .create_token_with_permission_and_hash(
                vec![PermissionDetailsSpec {
                    resource_type: "db".to_string(),
                    resource_identifier: vec!["metrics".to_string()],
                    actions: vec!["read".to_string()],
                }],
                "scoped".to_string(),
                scoped_hash.clone(),
                None,
            )
            .await
            .unwrap();

        // Grant resolves for the scoped database.
        let granted = catalog
            .get_permission("metrics", scoped_hash.clone())
            .expect("permission should resolve");
        assert_ne!(granted.actions(), 0);

        // Different database — no grant.
        assert!(
            catalog
                .get_permission("logs", scoped_hash.clone())
                .is_none()
        );

        // Unknown database — no grant even with a valid token.
        assert!(catalog.get_permission("ghost", scoped_hash).is_none());

        // Unknown token hash — no grant.
        assert!(catalog.get_permission("metrics", vec![99u8; 32]).is_none());
    }

    #[tokio::test]
    async fn get_permission_admin_token_matches_any_database() {
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("metrics").await.unwrap();

        let (info, _raw) = catalog.create_admin_token(false).await.unwrap();
        let admin_hash = info.hash.clone();

        // Admin token's wildcard grant resolves for any registered db
        // via the resource-type/identifier wildcard fallback in the
        // permission lookup.
        assert!(catalog.get_permission("metrics", admin_hash).is_some());
    }

    #[tokio::test]
    async fn get_permission_returns_none_after_token_delete() {
        use influxdb3_authz::permissions::PermissionDetailsSpec;
        let catalog = TestCatalog::new().catalog();
        catalog.create_database("metrics").await.unwrap();

        let scoped_hash = vec![20u8; 32];
        catalog
            .create_token_with_permission_and_hash(
                vec![PermissionDetailsSpec {
                    resource_type: "db".to_string(),
                    resource_identifier: vec!["metrics".to_string()],
                    actions: vec!["read".to_string()],
                }],
                "doomed".to_string(),
                scoped_hash.clone(),
                None,
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_permission("metrics", scoped_hash.clone())
                .is_some()
        );

        catalog.delete_token("doomed").await.unwrap();
        assert!(catalog.get_permission("metrics", scoped_hash).is_none());
    }
}

// ---------------------------------------------------------------------------
// Cache and trigger wrapper methods
// ---------------------------------------------------------------------------

mod caches_and_triggers {
    use super::*;
    use crate::CatalogError;
    use crate::catalog::versions::v3::catalog::ApiNodeSpec;
    use crate::catalog::versions::v3::schema::cache::{
        LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality,
    };
    use crate::catalog::versions::v3::schema::column::FieldDataType;
    use crate::catalog::versions::v3::schema::trigger::{TriggerSettings, ValidPluginFilename};
    use influxdb3_process::ProcessUuidWrapper;

    fn process_uuid_getter() -> Arc<dyn influxdb3_process::ProcessUuidGetter> {
        Arc::new(ProcessUuidWrapper::new())
    }

    async fn seeded() -> Catalog {
        let catalog = TestCatalog::new().catalog();
        catalog
            .register_node(
                "node-a",
                4,
                vec![NodeMode::All],
                process_uuid_getter(),
                Arc::from("inst-1"),
                None,
                None,
                0,
            )
            .await
            .unwrap();
        catalog
            .create_table(
                "metrics",
                "cpu",
                &["host"],
                &[("temp", FieldDataType::Float)],
            )
            .await
            .unwrap();
        catalog
    }

    #[tokio::test]
    async fn distinct_cache_create_delete_with_node_name() {
        let catalog = seeded().await;

        let created = catalog
            .create_distinct_cache(
                "metrics",
                "cpu",
                ApiNodeSpec::Nodes(vec!["node-a".to_string()]),
                Some("by_host"),
                &["host"],
                MaxCardinality::default(),
                MaxAge::default(),
            )
            .await
            .unwrap();
        assert_eq!(created.cache_name.as_ref(), "by_host");

        catalog
            .delete_distinct_cache("metrics", "cpu", "by_host")
            .await
            .unwrap();
    }

    /// The cache-create `_committed` variants report the sequence and time
    /// the op was actually applied at.
    #[tokio::test]
    async fn cache_create_committed_reports_applied_metadata() {
        let catalog = seeded().await;
        let pre_seq = catalog.sequence_number();

        let committed = catalog
            .create_distinct_cache_committed(
                "metrics",
                "cpu",
                ApiNodeSpec::Nodes(vec!["node-a".to_string()]),
                Some("by_host"),
                &["host"],
                MaxCardinality::default(),
                MaxAge::default(),
            )
            .await
            .unwrap();

        assert_eq!(committed.sequence, pre_seq.next());
        assert_eq!(
            committed.time_ns,
            // catalog mock clock never changed so we can just assert on its `now()`
            catalog.time_provider().now().timestamp_nanos()
        );

        // A later write advances the live catalog past the cache op's
        // sequence.
        catalog
            .create_last_cache(
                "metrics",
                "cpu",
                ApiNodeSpec::Nodes(vec!["node-a".to_string()]),
                Some("recent"),
                None::<&[&str]>,
                None::<&[&str]>,
                LastCacheSize::default(),
                LastCacheTtl::default(),
            )
            .await
            .unwrap();
        assert!(catalog.sequence_number() > committed.sequence);
    }

    #[tokio::test]
    async fn last_cache_unknown_node_name_is_rejected() {
        let catalog = seeded().await;

        // Reference a node that doesn't exist; the wrapper's name→id
        // resolution rejects with InvalidNodeName before the op runs.
        let err = catalog
            .create_last_cache(
                "metrics",
                "cpu",
                ApiNodeSpec::Nodes(vec!["ghost".to_string()]),
                Some("recent"),
                None::<&[&str]>,
                None::<&[&str]>,
                LastCacheSize::default(),
                LastCacheTtl::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::InvalidNodeName(name) if name == "ghost"),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn trigger_create_disable_enable_delete() {
        let catalog = seeded().await;
        // Trigger needs a plugin file reference; the validation is on
        // the caller side (ValidPluginFilename), so a synthetic value
        // is fine.
        let trigger = catalog
            .create_processing_engine_trigger(
                "metrics",
                "trig-a",
                ValidPluginFilename::from_validated_name("plugin.py"),
                ApiNodeSpec::All,
                "every:1m",
                TriggerSettings::default(),
                &None,
                false,
            )
            .await
            .unwrap();
        assert_eq!(trigger.trigger_name.as_ref(), "trig-a");
        assert!(!trigger.disabled);

        let disabled = catalog
            .disable_processing_engine_trigger("metrics", "trig-a")
            .await
            .unwrap();
        assert!(disabled.disabled);

        let enabled = catalog
            .enable_processing_engine_trigger("metrics", "trig-a")
            .await
            .unwrap();
        assert!(!enabled.disabled);

        // Deleting a running trigger without `force` is rejected.
        let err = catalog
            .delete_processing_engine_trigger("metrics", "trig-a", false)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, CatalogError::ProcessingEngineTriggerRunning { .. }),
            "got {err:?}"
        );

        // Disabling first, then deleting (without force), works.
        catalog
            .disable_processing_engine_trigger("metrics", "trig-a")
            .await
            .unwrap();
        catalog
            .delete_processing_engine_trigger("metrics", "trig-a", false)
            .await
            .unwrap();
    }
}

mod subscriptions {
    use super::*;

    #[tokio::test]
    async fn unsubscribe_drops_the_named_subscription() {
        let catalog = TestCatalog::new().catalog();

        // First registration is fine.
        let _rx = catalog.subscribe_to_updates("dropped").await;
        // unsubscribe_from_updates removes the entry so re-subscribing
        // by the same name no longer hits the duplicate-name assert.
        catalog.unsubscribe_from_updates("dropped").await;
        let _rx2 = catalog.subscribe_to_updates("dropped").await;

        // Unknown name is a no-op (does not panic).
        catalog.unsubscribe_from_updates("not-registered").await;

        // prune_subscriptions runs cleanly even when there's no work.
        catalog.prune_subscriptions().await;
    }
}

mod backup {
    use super::*;

    use object_store::path::Path;
    use test_helpers::assert_contains;

    use crate::catalog::versions::v3::backup::CatalogRestoreSource;
    use crate::object_store::versions::v3::CatalogFilePath;

    /// Build a catalog over a fresh in-memory store and return it together
    /// with the store so tests can stage backup images directly.
    async fn backup_test_catalog(prefix: &str) -> (Catalog, Arc<dyn object_store::ObjectStore>) {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let catalog = Catalog::new_with_checkpoint_interval(
            prefix,
            Arc::clone(&store),
            test_time_provider(),
            Arc::new(Registry::new()),
            // A large interval keeps the background checkpoint task from
            // racing the snapshot the test writes by hand.
            1_000,
        )
        .await
        .unwrap();
        (catalog, store)
    }

    /// Serialize the catalog's current in-memory state as a snapshot file and
    /// write it to the live snapshot (checkpoint) path. Mirrors the v2 test
    /// helper that PUT a serialized `CatalogSnapshot` to the checkpoint path.
    async fn write_snapshot_to_live_path(catalog: &Catalog) -> Path {
        let snapshot_path = CatalogFilePath::snapshot(catalog.object_store_prefix().as_ref());
        let snapshot_bytes = catalog.clone_inner().create_snapshot();
        catalog
            .object_store()
            .put(&snapshot_path, snapshot_bytes.into())
            .await
            .unwrap();
        snapshot_path.into()
    }

    #[test_log::test(tokio::test)]
    async fn backup_view_returns_checkpoint_bytes_and_following_logs() {
        let (catalog, _store) = backup_test_catalog("backup-view-host").await;

        catalog.create_database("db_checkpoint").await.unwrap();
        let checkpoint_sequence = catalog.sequence_number();
        let checkpoint_path = write_snapshot_to_live_path(&catalog).await;
        let checkpoint_bytes = catalog
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        catalog.create_database("db_after_1").await.unwrap();
        let after_1_sequence = catalog.sequence_number();
        catalog.create_database("db_after_2").await.unwrap();
        let after_2_sequence = catalog.sequence_number();

        let view = catalog.backup_view().await.unwrap();

        assert_eq!(view.checkpoint.sequence, checkpoint_sequence);
        assert_eq!(view.checkpoint.path, checkpoint_path);
        assert_eq!(view.checkpoint.bytes, checkpoint_bytes);
        assert_eq!(view.through_sequence, after_2_sequence);
        assert_eq!(
            view.log_files
                .iter()
                .map(|f| f.sequence)
                .collect::<Vec<_>>(),
            vec![after_1_sequence, after_2_sequence]
        );
        assert_eq!(
            view.log_files
                .iter()
                .map(|f| f.path.to_string())
                .collect::<Vec<_>>(),
            vec![
                CatalogFilePath::log(catalog.object_store_prefix().as_ref(), after_1_sequence,)
                    .to_string(),
                CatalogFilePath::log(catalog.object_store_prefix().as_ref(), after_2_sequence,)
                    .to_string(),
            ]
        );
    }

    #[test_log::test(tokio::test)]
    async fn backup_view_keeps_checkpoint_bytes_stable_after_live_checkpoint_changes() {
        let (catalog, _store) = backup_test_catalog("backup-view-stable-host").await;

        catalog.create_database("db_checkpoint").await.unwrap();
        let checkpoint_sequence = catalog.sequence_number();
        let checkpoint_path = write_snapshot_to_live_path(&catalog).await;
        let checkpoint_bytes = catalog
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let view = catalog.backup_view().await.unwrap();

        // Advance the catalog and overwrite the live snapshot; the previously
        // captured view must keep its original checkpoint sequence and bytes.
        catalog.create_database("db_after").await.unwrap();
        let after_sequence = catalog.sequence_number();
        write_snapshot_to_live_path(&catalog).await;
        let overwritten_checkpoint_bytes = catalog
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        assert_eq!(view.checkpoint.sequence, checkpoint_sequence);
        assert_eq!(view.checkpoint.bytes, checkpoint_bytes);
        assert_ne!(view.checkpoint.bytes, overwritten_checkpoint_bytes);
        assert_eq!(catalog.sequence_number(), after_sequence);
    }

    #[test_log::test(tokio::test)]
    async fn restore_uses_selected_backup_paths() {
        let (catalog, _store) = backup_test_catalog("restore-selected-backup-host").await;

        catalog.create_database("db_checkpoint").await.unwrap();
        let checkpoint_sequence = catalog.sequence_number();
        let checkpoint_path = write_snapshot_to_live_path(&catalog).await;

        catalog.create_database("db_after_1").await.unwrap();
        let after_1_sequence = catalog.sequence_number();
        catalog.create_database("db_after_2").await.unwrap();
        let after_2_sequence = catalog.sequence_number();

        // Copy the live snapshot + logs into a separate backup prefix.
        let backup_checkpoint_path = Path::from("selected-backup/catalog/v3/snapshot");
        let backup_checkpoint_bytes = catalog
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        catalog
            .object_store()
            .put(&backup_checkpoint_path, backup_checkpoint_bytes.into())
            .await
            .unwrap();

        let mut backup_log_paths = Vec::new();
        for sequence in [after_1_sequence, after_2_sequence] {
            let live_log_path =
                CatalogFilePath::log(catalog.object_store_prefix().as_ref(), sequence);
            let filename = live_log_path.filename().unwrap();
            let backup_log_path = Path::from(format!("selected-backup/catalog/v3/logs/{filename}"));
            let backup_log_bytes = catalog
                .object_store()
                .get(&live_log_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            catalog
                .object_store()
                .put(&backup_log_path, backup_log_bytes.into())
                .await
                .unwrap();
            backup_log_paths.push(backup_log_path);
        }

        catalog.create_database("db_newer").await.unwrap();
        let newer_sequence = catalog.sequence_number();

        // Subscribe before triggering restore so the broadcast is observable.
        // The broadcaster blocks on the subscriber's ACK (drop of the
        // CatalogUpdateMessage), so we must receive concurrently with
        // restore() rather than after it.
        let mut sub = catalog
            .subscribe_to_updates("restore-test-subscriber")
            .await;
        let restore_fut = catalog.restore(
            Arc::from("restore-1"),
            CatalogRestoreSource {
                checkpoint_path: backup_checkpoint_path,
                log_paths: backup_log_paths,
            },
        );
        let recv_fut = async {
            let msg = sub.recv().await.expect("broadcast delivered");
            msg.is_restore()
        };
        let (report, saw_restore) = tokio::join!(restore_fut, recv_fut);
        let report = report.unwrap();

        // The restore record persists at sequence newer_sequence + 1.
        let restore_sequence = newer_sequence.next();
        assert_eq!(report.restored_sequence, restore_sequence);
        assert_eq!(report.restore_id.as_ref(), "restore-1");
        assert_eq!(catalog.sequence_number(), restore_sequence);

        // Restored state: databases from the backup window are present,
        // db_newer (created after the backup was taken) is gone.
        assert!(catalog.db_name_to_id("db_checkpoint").is_some());
        assert!(catalog.db_name_to_id("db_after_1").is_some());
        assert!(catalog.db_name_to_id("db_after_2").is_some());
        assert_eq!(catalog.db_name_to_id("db_newer"), None);

        // Subscribers observe a restore broadcast.
        assert!(saw_restore, "subscriber should observe is_restore()");

        // Sequence-creation ordering invariant.
        assert_eq!(checkpoint_sequence.next(), after_1_sequence);
        assert_eq!(after_1_sequence.next(), after_2_sequence);
        assert_eq!(after_2_sequence.next(), newer_sequence);
    }

    #[test_log::test(tokio::test)]
    async fn restore_errors_on_missing_replayed_sequence() {
        let (catalog, _store) = backup_test_catalog("restore-missing-log-host").await;

        catalog.create_database("db_checkpoint").await.unwrap();
        let checkpoint_sequence = catalog.sequence_number();
        let checkpoint_path = write_snapshot_to_live_path(&catalog).await;

        catalog.create_database("db_after_1").await.unwrap();
        catalog.create_database("db_after_2").await.unwrap();
        let after_2_sequence = catalog.sequence_number();

        let backup_checkpoint_path = Path::from("selected-backup/catalog/v3/snapshot");
        let backup_checkpoint_bytes = catalog
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        catalog
            .object_store()
            .put(&backup_checkpoint_path, backup_checkpoint_bytes.into())
            .await
            .unwrap();

        // Copy only the second log file, leaving a gap after the checkpoint.
        let live_log_path =
            CatalogFilePath::log(catalog.object_store_prefix().as_ref(), after_2_sequence);
        let filename = live_log_path.filename().unwrap();
        let backup_log_path = Path::from(format!("selected-backup/catalog/v3/logs/{filename}"));
        let backup_log_bytes = catalog
            .object_store()
            .get(&live_log_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        catalog
            .object_store()
            .put(&backup_log_path, backup_log_bytes.into())
            .await
            .unwrap();

        let err = catalog
            .restore(
                Arc::from("restore-bad-sequence"),
                CatalogRestoreSource {
                    checkpoint_path: backup_checkpoint_path,
                    log_paths: vec![backup_log_path],
                },
            )
            .await
            .unwrap_err();

        assert_contains!(
            err.to_string(),
            format!(
                "catalog replay sequence mismatch for selected-backup/catalog/v3/logs/{filename}: expected {}, got {}",
                checkpoint_sequence.next().get(),
                after_2_sequence.get(),
            )
            .as_str()
        );
    }

    #[test_log::test(tokio::test)]
    async fn backup_view_errors_when_checkpoint_is_ahead_of_live_sequence() {
        let (catalog, _store) = backup_test_catalog("backup-view-behind-host").await;

        // Capture a live snapshot at a lower sequence, then advance, write the
        // higher-sequence snapshot to the checkpoint path, and finally roll the
        // in-memory catalog back to the lower sequence so the persisted
        // checkpoint is ahead of the live catalog.
        catalog.create_database("db_live").await.unwrap();
        let live_sequence = catalog.sequence_number();
        let live_inner = catalog.clone_inner();

        catalog.create_database("db_checkpoint").await.unwrap();
        let checkpoint_sequence = catalog.sequence_number();
        write_snapshot_to_live_path(&catalog).await;

        // Roll the in-memory catalog back to the lower sequence. The test
        // module has private access to `inner`, mirroring v2's
        // `update_from_snapshot(live_snapshot)` rollback.
        *catalog.inner.write() = live_inner;
        assert_eq!(catalog.sequence_number(), live_sequence);

        let err = catalog.backup_view().await.unwrap_err();
        assert_contains!(
            err.to_string(),
            format!(
                "persisted catalog checkpoint sequence {} is ahead of live catalog sequence {}",
                checkpoint_sequence.get(),
                live_sequence.get(),
            )
            .as_str()
        );
    }

    /// Restore on node A persists a `RestoreCatalog` log file. Node B picks
    /// it up via the catch-up path, replays the load+swap against its own
    /// `InnerCatalog`, and broadcasts `is_restore()` to its own subscribers.
    #[test_log::test(tokio::test)]
    async fn restore_propagates_via_catch_up_to_peer() {
        let prefix = "restore-catch-up-host";
        let shared: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let cat_a = Catalog::new_with_checkpoint_interval(
            prefix,
            Arc::clone(&shared),
            test_time_provider(),
            Arc::new(Registry::new()),
            1_000,
        )
        .await
        .unwrap();
        let cat_b = Catalog::new_with_checkpoint_interval(
            prefix,
            Arc::clone(&shared),
            test_time_provider(),
            Arc::new(Registry::new()),
            1_000,
        )
        .await
        .unwrap();

        // Seed shared state both catalogs see: a database, plus a snapshot
        // written at that sequence to use as the backup checkpoint.
        cat_a.create_database("db_baseline").await.unwrap();
        let baseline_sequence = cat_a.sequence_number();
        let checkpoint_path = write_snapshot_to_live_path(&cat_a).await;

        // Stage the backup image under a separate prefix so the restore
        // record's paths point at stable, copied-out bytes.
        let backup_checkpoint_path = Path::from("catch-up-backup/catalog/v3/snapshot");
        let checkpoint_bytes = cat_a
            .object_store()
            .get(&checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        cat_a
            .object_store()
            .put(&backup_checkpoint_path, checkpoint_bytes.into())
            .await
            .unwrap();

        // Bring cat_b up to baseline so it has the same starting point as A,
        // then create a database on A that we expect to vanish after restore.
        cat_b
            .update_to_sequence_number(baseline_sequence)
            .await
            .unwrap();
        cat_a.create_database("db_will_disappear").await.unwrap();
        let pre_restore_sequence = cat_a.sequence_number();

        // Restore on A.
        let mut sub_a = cat_a.subscribe_to_updates("sub-a").await;
        let restore_fut = cat_a.restore(
            Arc::from("propagation-test"),
            CatalogRestoreSource {
                checkpoint_path: backup_checkpoint_path,
                log_paths: vec![],
            },
        );
        let recv_a = async {
            let msg = sub_a.recv().await.expect("A broadcast");
            msg.is_restore()
        };
        let (report, a_saw_restore) = tokio::join!(restore_fut, recv_a);
        let report = report.unwrap();
        let restore_sequence = report.restored_sequence;
        assert!(a_saw_restore);
        assert_eq!(restore_sequence, pre_restore_sequence.next());

        // Catch B up: it should see the same restore broadcast and end with
        // the same restored state.
        let mut sub_b = cat_b.subscribe_to_updates("sub-b").await;
        let catch_up_fut = cat_b.update_to_sequence_number(restore_sequence);
        let mut events_seen = 0;
        let mut b_saw_restore = false;
        let drain_b = async {
            while let Some(msg) = sub_b.recv().await {
                if msg.is_restore() {
                    b_saw_restore = true;
                }
                events_seen += 1;
                drop(msg);
                if events_seen >= 2 {
                    // Two updates expected: the `db_will_disappear` create
                    // applied during catch-up, then the restore that wipes
                    // it. After both, stop draining.
                    break;
                }
            }
        };
        let (catch_up_res, ()) = tokio::join!(catch_up_fut, drain_b);
        catch_up_res.unwrap();
        assert!(b_saw_restore, "B's subscriber should observe a restore");

        // B's restored state matches A's.
        assert_eq!(cat_b.sequence_number(), restore_sequence);
        assert!(cat_b.db_name_to_id("db_baseline").is_some());
        assert_eq!(cat_b.db_name_to_id("db_will_disappear"), None);
    }

    /// Regression test for the restore-checkpoint race.
    ///
    /// `Catalog::restore` releases the write permit when its `update_committed`
    /// returns, then calls `force_checkpoint`. A writer can commit in that
    /// window, advancing `inner` past the restore sequence. The persisted
    /// snapshot must stay internally consistent regardless of where that write
    /// lands relative to the checkpoint: `force_checkpoint` reads the header
    /// sequence and the record body together under a single `inner` lock, so it
    /// never stamps a snapshot whose header understates its contents. A peer
    /// cold-starting from the store therefore always loads cleanly — it either
    /// finds the concurrent write already folded into the snapshot, or replays
    /// it as the single straggler log on top — and never double-applies a
    /// record the snapshot already holds.
    ///
    /// We exercise the real `restore()` path with a genuinely concurrent write
    /// via `tokio::join!`. `restore_fut` is polled first, so it acquires the
    /// write permit and commits at `restore_sequence`; the concurrent write is
    /// serialized after it at `restore_sequence + 1`. Whichever side of the
    /// checkpoint that write lands on, the cold-start convergence below holds.
    #[test_log::test(tokio::test)]
    async fn restore_checkpoint_stays_consistent_under_concurrent_write() {
        let (catalog, store) = backup_test_catalog("restore-force-checkpoint-race-host").await;

        // Seed baseline state and stage it as the backup image under a
        // stable, copied-out prefix.
        catalog.create_database("db_baseline").await.unwrap();
        let live_snapshot_path = write_snapshot_to_live_path(&catalog).await;
        let backup_checkpoint_path = Path::from("race-backup/catalog/v3/snapshot");
        let checkpoint_bytes = catalog
            .object_store()
            .get(&live_snapshot_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        catalog
            .object_store()
            .put(&backup_checkpoint_path, checkpoint_bytes.into())
            .await
            .unwrap();

        // A database created after the backup window; the restore removes it.
        catalog.create_database("db_will_vanish").await.unwrap();
        let pre_restore_sequence = catalog.sequence_number();
        let restore_sequence = pre_restore_sequence.next();
        let concurrent_sequence = restore_sequence.next();
        assert_eq!(restore_sequence.get(), concurrent_sequence.get() - 1);

        let restore_fut = catalog.restore(
            Arc::from("race-restore"),
            CatalogRestoreSource {
                checkpoint_path: backup_checkpoint_path,
                log_paths: vec![],
            },
        );

        // Polled second, this commits once `restore`'s `update_committed` has
        // released the write permit — at `restore_sequence + 1`, advancing
        // `inner`. It races `restore`'s `force_checkpoint`; the snapshot stays
        // consistent on either ordering.
        let concurrent_write = async {
            catalog.create_database("db_concurrent").await.unwrap();
        };

        let (report, ()) = tokio::join!(restore_fut, concurrent_write);
        let report = report.unwrap();

        // The restore persisted at `restore_sequence`; the concurrent write
        // advanced `inner` to the next sequence.
        assert_eq!(report.restored_sequence, restore_sequence);
        assert_eq!(catalog.sequence_number(), concurrent_sequence);
        assert!(catalog.db_name_to_id("db_concurrent").is_some());
        assert_eq!(catalog.db_name_to_id("db_will_vanish"), None);

        // `force_checkpoint` overwrote the live snapshot, and it reflects the
        // restored baseline: `db_baseline` is present and the post-backup
        // `db_will_vanish` is gone. (Whether `db_concurrent` is folded in
        // depends on which side of the checkpoint the write landed — that is
        // the race, and either outcome is consistent, so we don't pin it.)
        let reconstructed = catalog
            .store
            .load_catalog_from_paths(&live_snapshot_path, &[])
            .await
            .unwrap()
            .expect("snapshot loads");
        assert!(reconstructed.databases.name_to_id("db_baseline").is_some());
        assert_eq!(reconstructed.databases.name_to_id("db_will_vanish"), None);

        // The decisive check. A peer cold-starting from this object store runs
        // the standard load (`load_or_create_catalog` -> `load_catalog`): it
        // applies the snapshot, then replays any logs after the snapshot
        // sequence. Pre-fix, the snapshot header could understate its body, so
        // the straggler log re-applied a record the snapshot already held and
        // the load failed the duplicate apply. With the header and body read
        // together, the load always succeeds and converges to the live state.
        let peer = Catalog::new_with_checkpoint_interval(
            "restore-force-checkpoint-race-host",
            Arc::clone(&store),
            test_time_provider(),
            Arc::new(Registry::new()),
            1_000,
        )
        .await
        .expect("cold-start load must not replay an already-snapshotted log");
        assert_eq!(peer.sequence_number(), concurrent_sequence);
        assert!(peer.db_name_to_id("db_baseline").is_some());
        assert!(peer.db_name_to_id("db_concurrent").is_some());
        assert_eq!(peer.db_name_to_id("db_will_vanish"), None);
    }

    /// A restore replaces the catalog's data but must keep the live cluster's
    /// identity. The outer `Catalog.catalog_uuid` (used for every subsequent
    /// log) is fixed at construction, so the post-restore snapshot must adopt
    /// the live uuid too — otherwise snapshot and logs would silently disagree
    /// on identity and a cold-start load would stitch a mixed catalog together.
    #[test_log::test(tokio::test)]
    async fn restore_keeps_live_identity_when_backup_uuid_differs() {
        let (catalog, store) = backup_test_catalog("restore-identity-live-host").await;
        let live_uuid = catalog.catalog_uuid();
        catalog.create_database("db_live_only").await.unwrap();

        // Build the backup image from a *different* catalog instance so its
        // snapshot carries a different catalog uuid (cross-cluster / DR style).
        let (backup_src, _backup_store) = backup_test_catalog("restore-identity-backup-host").await;
        let backup_uuid = backup_src.catalog_uuid();
        assert_ne!(backup_uuid, live_uuid, "test needs distinct uuids");
        backup_src.create_database("db_from_backup").await.unwrap();
        let backup_snapshot_bytes = backup_src.clone_inner().create_snapshot();

        // Stage the differing-uuid snapshot in the live store as the backup
        // image and restore from it.
        let backup_checkpoint_path = Path::from("identity-backup/catalog/v3/snapshot");
        catalog
            .object_store()
            .put(&backup_checkpoint_path, backup_snapshot_bytes.into())
            .await
            .unwrap();
        catalog
            .restore(
                Arc::from("identity-restore"),
                CatalogRestoreSource {
                    checkpoint_path: backup_checkpoint_path,
                    log_paths: vec![],
                },
            )
            .await
            .unwrap();

        // Data came from the backup, but identity stayed live.
        assert!(catalog.db_name_to_id("db_from_backup").is_some());
        assert_eq!(catalog.db_name_to_id("db_live_only"), None);
        assert_eq!(catalog.catalog_uuid(), live_uuid);

        // The forced post-restore snapshot is stamped with the live uuid, not
        // the backup's, so it agrees with the live-uuid logs that follow it.
        let (snapshot, _size_bytes) = catalog.store.load_snapshot().await.unwrap().unwrap();
        assert_eq!(Uuid::from_u128(snapshot.header.catalog_uuid), live_uuid);

        // A write after restore lands a log on top of that snapshot...
        catalog.create_database("db_after_restore").await.unwrap();
        let post_restore_sequence = catalog.sequence_number();

        // ...and a peer cold-starting from the store loads cleanly: the
        // snapshot and the following log agree on the (live) identity. A split
        // uuid would trip the load-path agreement check.
        let peer = Catalog::new_with_checkpoint_interval(
            "restore-identity-live-host",
            Arc::clone(&store),
            test_time_provider(),
            Arc::new(Registry::new()),
            1_000,
        )
        .await
        .expect("cold-start load must not see a split catalog uuid");
        assert_eq!(peer.catalog_uuid(), live_uuid);
        assert_eq!(peer.sequence_number(), post_restore_sequence);
        assert!(peer.db_name_to_id("db_from_backup").is_some());
        assert!(peer.db_name_to_id("db_after_restore").is_some());
        assert_eq!(peer.db_name_to_id("db_live_only"), None);
    }
}

// ---------------------------------------------------------------------------
// Query group persistence
// ---------------------------------------------------------------------------

mod query_group_persistence {
    use std::num::NonZeroUsize;

    use influxdb3_id::{NodeId, QueryGroupId};

    use super::TestCatalog;
    use crate::CatalogError;
    use crate::catalog::versions::v3::schema::query_group::QueryGroupDefinition;

    // --------------------------------
    // Helper functions
    // --------------------------------

    fn rf(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("non-zero")
    }

    /// Assert that two query group definitions are equal across all fields.
    ///
    /// `QueryGroupDefinition` derives `PartialEq + Debug`, so this gives a
    /// full struct diff on failure without needing field-by-field asserts.
    fn assert_query_group(expected: &QueryGroupDefinition, actual: &QueryGroupDefinition) {
        assert_eq!(expected, actual);
    }

    // Test that a query group written after the initial snapshot is visible
    // in a second catalog that loads the same object store prefix.
    #[tokio::test]
    async fn query_group_survives_log_replay() {
        let test_catalog = TestCatalog::new();
        // Seed the prefix through load_or_create so startup has a valid
        // snapshot, then keep the query group only in following logs.
        let cat1 = test_catalog.reload().await;
        let nodes = cat1.register_query_nodes(3).await;
        let created = cat1
            .create_query_group("analytics", vec![nodes[0], nodes[1], nodes[2]], rf(2))
            .await
            .unwrap();

        // Boot a second catalog from the same store without force_checkpoint,
        // so it must replay the query-group log after the snapshot.
        let cat2 = test_catalog.reload().await;

        assert_query_group(
            &created,
            &cat2
                .query_group_by_name("analytics")
                .expect("group must survive log replay"),
        );
    }

    #[tokio::test]
    async fn query_group_survives_snapshot_reload() {
        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.catalog_no_checkpoint();
        // Use a non-sequential order to detect any sorting applied on persist/reload.
        let nodes = cat1.register_query_nodes(3).await;
        let created = cat1
            .create_query_group("g1", vec![nodes[2], nodes[0], nodes[1]], rf(1))
            .await
            .unwrap();
        cat1.force_checkpoint().await.unwrap();

        let cat2 = test_catalog.reload().await;

        assert_query_group(
            &created,
            &cat2
                .query_group_by_name("g1")
                .expect("group must survive snapshot reload"),
        );
    }

    #[tokio::test]
    async fn query_group_member_order_preserved_after_reload() {
        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.catalog_no_checkpoint();
        // Deliberately out-of-natural-order to detect sorting bugs.
        let nodes = cat1.register_query_nodes(4).await;
        let created = cat1
            .create_query_group(
                "ordered",
                vec![nodes[3], nodes[1], nodes[2], nodes[0]],
                rf(2),
            )
            .await
            .unwrap();
        cat1.force_checkpoint().await.unwrap();

        let cat2 = test_catalog.reload().await;

        assert_query_group(
            &created,
            &cat2
                .query_group_by_name("ordered")
                .expect("group must survive reload"),
        );
    }

    #[tokio::test]
    async fn multiple_query_groups_survive_reload() {
        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.catalog_no_checkpoint();
        let nodes = cat1.register_query_nodes(3).await;
        let g1 = cat1
            .create_query_group("g1", vec![nodes[0]], rf(1))
            .await
            .unwrap();
        let g2 = cat1
            .create_query_group("g2", vec![nodes[1], nodes[2]], rf(1))
            .await
            .unwrap();
        cat1.force_checkpoint().await.unwrap();

        let cat2 = test_catalog.reload().await;

        assert_eq!(
            cat2.list_query_groups().len(),
            2,
            "both groups must be present after reload"
        );
        assert_query_group(
            &g1,
            &cat2.query_group_by_name("g1").expect("g1 must be present"),
        );
        assert_query_group(
            &g2,
            &cat2.query_group_by_name("g2").expect("g2 must be present"),
        );
    }

    #[tokio::test]
    async fn query_group_by_id_survives_reload() {
        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.catalog_no_checkpoint();
        let nodes = cat1.register_query_nodes(1).await;
        let group = cat1
            .create_query_group("g1", vec![nodes[0]], rf(1))
            .await
            .unwrap();
        let id = group.id;
        cat1.force_checkpoint().await.unwrap();

        let cat2 = test_catalog.reload().await;
        cat2.query_group_by_id(&id)
            .expect("group must be retrievable by ID after reload");
    }

    #[tokio::test]
    async fn query_group_by_name_returns_none_for_unknown_name() {
        let cat = TestCatalog::new().reload().await;
        assert!(
            cat.query_group_by_name("nonexistent").is_none(),
            "expected None for unknown name"
        );
    }

    #[tokio::test]
    async fn query_group_by_id_returns_none_for_unknown_id() {
        let cat = TestCatalog::new().reload().await;
        assert!(
            cat.query_group_by_id(&QueryGroupId::new(999)).is_none(),
            "expected None for unknown ID"
        );
    }

    // The name uniqueness check runs before member validation, so the second
    // call returns AlreadyExists even though its member list is unregistered.
    #[tokio::test]
    async fn create_query_group_duplicate_name_returns_already_exists() {
        let cat = TestCatalog::new().reload().await;
        let nodes = cat.register_query_nodes(1).await;
        cat.create_query_group("g1", vec![nodes[0]], rf(1))
            .await
            .unwrap();

        // Use an unregistered node ID so this call would fail member validation
        // if the name check didn't short-circuit first.
        let result = cat
            .create_query_group("g1", vec![NodeId::new(9999)], rf(1))
            .await;
        assert!(
            matches!(result, Err(CatalogError::AlreadyExists)),
            "expected AlreadyExists, got {result:?}"
        );
    }

    // Test that a deleted query group is absent after replaying the log from
    // object store. Ensures the DeleteQueryGroup record is persisted and replayed.
    #[tokio::test]
    async fn delete_query_group_absent_after_log_replay() {
        let test_catalog = TestCatalog::new();
        // Seed the prefix through load_or_create so startup has a valid
        // snapshot, then keep create/delete records only in following logs.
        let cat1 = test_catalog.reload().await;
        let nodes = cat1.register_query_nodes(1).await;
        let group = cat1
            .create_query_group("g1", vec![nodes[0]], rf(1))
            .await
            .unwrap();
        assert!(
            cat1.query_group_by_name("g1").is_some(),
            "query group must be present before deletion"
        );

        cat1.delete_query_group(&group.id).await.unwrap();

        let cat2 = test_catalog.reload().await;
        assert!(
            cat2.query_group_by_name("g1").is_none(),
            "deleted group must be absent after log replay"
        );
    }

    // Test that a deleted query group is absent after snapshot reload.
    // Ensures the delete is included in the checkpoint and not resurrected.
    #[tokio::test]
    async fn delete_query_group_absent_after_snapshot_reload() {
        let test_catalog = TestCatalog::new();
        let cat1 = test_catalog.catalog_no_checkpoint();
        let nodes = cat1.register_query_nodes(1).await;
        let group = cat1
            .create_query_group("g1", vec![nodes[0]], rf(1))
            .await
            .unwrap();
        assert!(
            cat1.query_group_by_name("g1").is_some(),
            "query group must be present before deletion"
        );

        cat1.delete_query_group(&group.id).await.unwrap();
        cat1.force_checkpoint().await.unwrap();

        let cat2 = test_catalog.reload().await;
        assert!(
            cat2.query_group_by_name("g1").is_none(),
            "deleted group must be absent after snapshot reload"
        );
    }

    // Test that delete returns the removed definition so callers
    // (e.g. node-stop safety) can inspect which members were in the group.
    #[tokio::test]
    async fn delete_query_group_returns_removed_definition() {
        let cat = TestCatalog::new().reload().await;
        let nodes = cat.register_query_nodes(2).await;
        let group = cat
            .create_query_group("g1", vec![nodes[0], nodes[1]], rf(1))
            .await
            .unwrap();

        let removed = cat.delete_query_group(&group.id).await.unwrap();
        assert_query_group(&group, &removed);
    }

    // Test that deleting a nonexistent query group returns a typed NotFound error
    #[tokio::test]
    async fn delete_query_group_nonexistent_returns_not_found() {
        let cat = TestCatalog::new().reload().await;

        let result = cat.delete_query_group(&QueryGroupId::new(999)).await;
        assert!(
            matches!(result, Err(CatalogError::NotFound(_))),
            "expected NotFound, got {result:?}"
        );
    }
}
