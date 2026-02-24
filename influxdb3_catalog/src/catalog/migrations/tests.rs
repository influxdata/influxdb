use super::*;
use crate::catalog::CatalogSequenceNumber;
use crate::catalog::versions::{v1, v2};
use crate::log::versions::v3::FieldDataType;
use crate::serialize::versions::{v1 as serialize_v1, v2 as serialize_v2};
use futures::StreamExt;
use influxdb3_test_helpers::object_store::mock;
use influxdb3_test_helpers::object_store::mock::{MockCall, MockStore, PutPayloadWrapper};
use iox_time::{MockProvider, Time};
use object_store::PutResult;
use object_store::memory::InMemory;

/// Helper to create a test v1 catalog with sample data
async fn create_test_v1_catalog() -> v1::InnerCatalog {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
    let catalog = v1::Catalog::new_in_memory_with_args(
        "test-catalog",
        time_provider,
        v1::CatalogArgs::default(),
    )
    .await
    .unwrap();

    // Add some test data
    catalog.create_database("test_db").await.unwrap();
    catalog
        .create_table(
            "test_db",
            "test_table",
            &["tag1"],
            &[] as &[(&str, FieldDataType)],
        )
        .await
        .unwrap();

    catalog.inner.read().clone()
}

/// Helper to write a v1 catalog to object store
async fn write_v1_catalog_to_store(
    catalog: &v1::InnerCatalog,
    store: Arc<dyn ObjectStore>,
    prefix: Arc<str>,
) {
    use crate::catalog::versions::v1::Snapshot;
    let checkpoint_path = ostore::v1::CatalogFilePath::checkpoint(&prefix);
    let snapshot = catalog.snapshot();
    let serialized =
        serialize_v1::serialize_catalog_file(&snapshot).expect("serialize v1 snapshot");

    store
        .put(&checkpoint_path, serialized.into())
        .await
        .expect("write v1 checkpoint");
}

/// Helper to write a v2 checkpoint to object store
async fn write_v2_checkpoint_to_store(store: Arc<dyn ObjectStore>, prefix: Arc<str>) {
    use crate::catalog::versions::v2::Snapshot;
    let checkpoint_path = ostore::v2::CatalogFilePath::checkpoint(&prefix);

    // Create minimal v2 catalog
    let v2_inner = v2::InnerCatalog::new(Arc::from("test-catalog"), uuid::Uuid::new_v4());
    let snapshot = v2_inner.snapshot();
    let serialized =
        serialize_v2::serialize_catalog_file(&snapshot).expect("serialize v2 snapshot");

    store
        .put(&checkpoint_path, serialized.into())
        .await
        .expect("write v2 checkpoint");
}

/// Helper to verify v2 checkpoint exists
async fn verify_v2_checkpoint_exists(store: Arc<dyn ObjectStore>, prefix: Arc<str>) -> bool {
    let checkpoint_path = ostore::v2::CatalogFilePath::checkpoint(&prefix);
    store.head(&checkpoint_path).await.is_ok()
}

/// Helper to verify UpgradedLog exists in v1 catalog
async fn verify_upgraded_log_exists(
    store: Arc<dyn ObjectStore>,
    prefix: Arc<str>,
    sequence: CatalogSequenceNumber,
) -> bool {
    use ostore::v1::CatalogFilePath;
    let path = CatalogFilePath::log(prefix.as_ref(), sequence);

    if let Ok(result) = store.get(&path).await
        && let Ok(bytes) = result.bytes().await
    {
        // Try to deserialize - if it fails with UpgradedLog error, that means it IS an UpgradedLog
        match serialize_v1::verify_and_deserialize_catalog_file(bytes) {
            Err(crate::object_store::ObjectStoreCatalogError::UpgradedLog) => return true,
            _ => return false,
        }
    }
    false
}

#[tokio::test]
async fn test_successful_v1_to_v2_migration() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Setup: Create and write v1 catalog
    let v1_catalog = create_test_v1_catalog().await;
    write_v1_catalog_to_store(&v1_catalog, Arc::clone(&store) as _, Arc::clone(&prefix)).await;

    // Action: Perform migration
    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Migration succeeded
    assert!(result.is_ok());

    // Verify: v2 checkpoint exists
    assert!(verify_v2_checkpoint_exists(Arc::clone(&store) as _, Arc::clone(&prefix)).await);

    // Verify: UpgradedLog was written
    let next_sequence = v1_catalog.sequence_number().next();
    assert!(
        verify_upgraded_log_exists(Arc::clone(&store) as _, Arc::clone(&prefix), next_sequence)
            .await
    );

    // Verify: Can load v2 catalog
    let v2_catalog = serialize_v2::load_catalog(Arc::clone(&prefix), store as Arc<dyn ObjectStore>)
        .await
        .expect("load v2 catalog")
        .expect("v2 catalog should exist");

    // Verify: Data was migrated
    assert!(v2_catalog.databases.get_by_name("test_db").is_some());
}

#[tokio::test]
async fn test_v2_already_exists_no_migration() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Setup: Write v2 checkpoint
    write_v2_checkpoint_to_store(Arc::clone(&store) as _, Arc::clone(&prefix)).await;

    // Record initial object count
    let initial_objects: Vec<_> = store.list(None).map(|r| r.unwrap()).collect().await;

    // Action: Perform migration
    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Migration succeeded without doing anything
    assert!(result.is_ok());

    // Verify: No new objects written
    let final_objects: Vec<_> = store.list(None).map(|r| r.unwrap()).collect().await;
    assert_eq!(initial_objects.len(), final_objects.len());
}

#[tokio::test]
async fn test_no_v1_catalog_exists() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Action: Perform migration on empty store
    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Migration succeeded without doing anything
    assert!(result.is_ok());

    // Verify: No objects written
    let objects: Vec<_> = store.list(None).map(|r| r.unwrap()).collect().await;
    assert!(objects.is_empty());
}

#[tokio::test]
async fn test_v1_already_upgraded() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Setup: Create v1 catalog
    let v1_catalog = create_test_v1_catalog().await;

    // Write the catalog
    write_v1_catalog_to_store(&v1_catalog, Arc::clone(&store) as _, Arc::clone(&prefix)).await;

    // Write an UpgradedLog to simulate that the catalog has already been upgraded
    use ostore::v1::CatalogFilePath;
    let upgraded_log = serialize_v1::serialize_catalog_file(&crate::log::UpgradedLog)
        .expect("UpgradedLog should have serialized");
    let next_sequence = v1_catalog.sequence_number().next();
    let path = CatalogFilePath::log(prefix.as_ref(), next_sequence);
    store.put(&path, upgraded_log.into()).await.unwrap();

    // Action: Perform migration
    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Migration succeeded
    assert!(result.is_ok());

    // Verify: v2 checkpoint exists
    assert!(verify_v2_checkpoint_exists(Arc::clone(&store) as _, Arc::clone(&prefix)).await);

    // Verify: UpgradedLog exists (it was written before migration)
    let next_sequence = v1_catalog.sequence_number().next();
    assert!(
        verify_upgraded_log_exists(
            store as Arc<dyn ObjectStore>,
            Arc::clone(&prefix),
            next_sequence
        )
        .await
    );
}

async fn create_mock_store(prefix: Arc<str>) -> Arc<MockStore> {
    // Setup: Create and write v1 catalog
    let v1_catalog = create_test_v1_catalog().await;

    let v1_checkpoint_path = ostore::v1::CatalogFilePath::checkpoint(&prefix);
    let v2_checkpoint_path = ostore::v2::CatalogFilePath::checkpoint(&prefix);

    // Get copy of the serialized data for the MockStore
    let v1_checkpoint_data = {
        let in_mem = Arc::new(InMemory::new());
        write_v1_catalog_to_store(&v1_catalog, Arc::clone(&in_mem) as _, Arc::clone(&prefix)).await;
        in_mem
            .get(&v1_checkpoint_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    };

    MockStore::new()
        // The v2 checkpoint does not exist
        .mock_next(MockCall::Head {
            params: v2_checkpoint_path.clone().into(),
            barriers: vec![],
            res: Err(mock::not_found("")),
        })
        // The v1 checkpoint does exist
        .mock_next(MockCall::Head {
            params: v1_checkpoint_path.clone().into(),
            barriers: vec![],
            res: Ok(mock::object_meta()),
        })
        // Load the v1 catalog
        .mock_next_multi(vec![
            MockCall::Get {
                params: v1_checkpoint_path.clone().into(),
                barriers: vec![],
                res: Ok(mock::get_result(v1_checkpoint_data, &v1_checkpoint_path)),
            },
            MockCall::ListWithOffset {
                params: (
                    Some(ostore::v1::CatalogFilePath::dir(&prefix).into()),
                    ostore::v1::CatalogFilePath::log(&prefix, 3.into()).into(),
                ),
                barriers: vec![],
                res: futures::stream::empty().boxed().into(),
            },
        ])
}

#[tokio::test]
async fn test_race_condition_upgraded_log_already_exists() {
    let prefix = Arc::from("test");
    let v1_upgrade_log_path = ostore::v1::CatalogFilePath::log(&prefix, 4.into());
    let store = create_mock_store(Arc::clone(&prefix))
        .await
        // Return that the UpgradeLog already exists,
        .mock_next(MockCall::PutOpts {
            params: (
                v1_upgrade_log_path.clone().into(),
                PutPayloadWrapper::Ignore,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            ),
            barriers: vec![],
            res: Err(object_store::Error::AlreadyExists {
                path: "".to_string(),
                source: "foo".into(),
            }),
        });

    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), store.as_store()).await;

    // Verify: Migration failed with expected error
    assert!(matches!(
        result,
        Err(MigrationError::UpgradeLogAlreadyExists)
    ));

    // Verify: Error is retryable
    if let Err(e) = result {
        assert!(e.is_retryable());
    }
}

#[tokio::test]
async fn test_race_condition_v2_checkpoint_already_exists() {
    let prefix = Arc::from("test");
    let v1_upgrade_log_path = ostore::v1::CatalogFilePath::log(&prefix, 4.into());
    let v2_checkpoint_path = ostore::v2::CatalogFilePath::checkpoint(&prefix);
    let store = create_mock_store(Arc::clone(&prefix))
        .await
        // Store the UpgradeLog to the v1 catalog,
        .mock_next(MockCall::PutOpts {
            params: (
                v1_upgrade_log_path.clone().into(),
                PutPayloadWrapper::Ignore,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            ),
            barriers: vec![],
            res: Ok(PutResult {
                e_tag: None,
                version: None,
            }),
        })
        // Return that the v2 checkpoint already exists
        .mock_next(MockCall::PutOpts {
            params: (
                v2_checkpoint_path.clone().into(),
                PutPayloadWrapper::Ignore,
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            ),
            barriers: vec![],
            res: Err(object_store::Error::AlreadyExists {
                path: "".to_string(),
                source: "foo".into(),
            }),
        });

    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), store.as_store()).await;
    // Verify: Migration succeeded (another node completed it)
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_migration_with_complex_catalog() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Setup: Create complex v1 catalog
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(1000)));
    let catalog = v1::Catalog::new_in_memory_with_args(
        "test-catalog",
        time_provider,
        v1::CatalogArgs::default(),
    )
    .await
    .unwrap();

    // Add multiple databases and tables
    catalog.create_database("db1").await.unwrap();
    catalog.create_database("db2").await.unwrap();

    catalog
        .create_table(
            "db1",
            "table1",
            &["tag1", "tag2"],
            &[] as &[(&str, FieldDataType)],
        )
        .await
        .unwrap();
    catalog
        .create_table("db1", "table2", &["tag3"], &[] as &[(&str, FieldDataType)])
        .await
        .unwrap();
    catalog
        .create_table(
            "db2",
            "table3",
            &["tag4", "tag5"],
            &[] as &[(&str, FieldDataType)],
        )
        .await
        .unwrap();

    // Add a token
    catalog.create_admin_token(false).await.unwrap();

    let v1_catalog = catalog.inner.read().clone();
    write_v1_catalog_to_store(&v1_catalog, Arc::clone(&store) as _, Arc::clone(&prefix)).await;

    // Action: Perform migration
    let result = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Migration succeeded
    assert!(result.is_ok());

    // Verify: v2 catalog has all data
    let v2_catalog = serialize_v2::load_catalog(Arc::clone(&prefix), store as Arc<dyn ObjectStore>)
        .await
        .expect("load v2 catalog")
        .expect("v2 catalog should exist");

    assert!(v2_catalog.databases.get_by_name("db1").is_some());
    assert!(v2_catalog.databases.get_by_name("db2").is_some());
    // Should have the created admin token
    let token_count = v2_catalog.tokens.repo().len();
    assert_eq!(token_count, 1, "Expected 1 token, got {}", token_count);
}

#[tokio::test]
async fn test_idempotent_migration() {
    let store = Arc::new(InMemory::new());
    let prefix = Arc::from("test");

    // Setup: Create and write v1 catalog
    let v1_catalog = create_test_v1_catalog().await;
    write_v1_catalog_to_store(&v1_catalog, Arc::clone(&store) as _, Arc::clone(&prefix)).await;

    // Action: Perform migration twice
    let result1 = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;
    let result2 = check_and_migrate_v1_to_v2(Arc::clone(&prefix), Arc::clone(&store) as _).await;

    // Verify: Both migrations succeeded
    assert!(result1.is_ok());
    assert!(result2.is_ok());

    // Verify: v2 checkpoint still exists and is valid
    assert!(verify_v2_checkpoint_exists(store as Arc<dyn ObjectStore>, Arc::clone(&prefix)).await);
}
