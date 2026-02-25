use super::*;
use crate::table_index_cache::TableIndexCacheConfig;
use influxdb3_shutdown::ShutdownManager;
use iox_time::{MockProvider, Time};
use metric::Registry;
use object_store::{ObjectStore, memory::InMemory};

#[tokio::test]
async fn test_retention_handler_no_policies() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let registry = Arc::new(Registry::default());
    let catalog = Arc::new(
        Catalog::new(
            "test_host".to_string(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Arc::clone(&registry),
        )
        .await
        .unwrap(),
    );

    let table_index_cache = TableIndexCache::new(
        "test_host".to_string(),
        TableIndexCacheConfig::default(),
        object_store,
    );

    let handler = Arc::new(RetentionPeriodHandler::new(
        table_index_cache.clone(),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        Duration::from_secs(30),
        "test_host".to_string(),
    ));

    // Check retention with no policies configured
    handler.check_and_enforce_retention().await;
    // Should complete without errors
}

#[tokio::test]
async fn test_retention_handler_shutdown() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let registry = Arc::new(Registry::default());
    let catalog = Arc::new(
        Catalog::new(
            "test_host".to_string(),
            Arc::clone(&object_store),
            Arc::clone(&time_provider) as _,
            Arc::clone(&registry),
        )
        .await
        .unwrap(),
    );

    let table_index_cache = TableIndexCache::new(
        "test_host".to_string(),
        TableIndexCacheConfig::default(),
        object_store,
    );

    let handler = Arc::new(RetentionPeriodHandler::new(
        table_index_cache.clone(),
        Arc::clone(&catalog),
        Arc::clone(&time_provider) as _,
        Duration::from_millis(100), // Short interval for testing
        "test_host".to_string(),
    ));

    let shutdown_manager = ShutdownManager::new_testing();
    let shutdown_token = shutdown_manager.register();

    // Start the background task
    let task_handle = tokio::spawn(handler.background_task(shutdown_token));

    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown
    shutdown_manager.shutdown();

    // Task should complete
    task_handle.await.unwrap();
}
