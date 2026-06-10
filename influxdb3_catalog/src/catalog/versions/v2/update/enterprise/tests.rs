use std::{sync::Arc, time::Duration};

use iox_time::{MockProvider, Time, TimeProvider};
use object_store::{ObjectStore, memory::InMemory};

use crate::{
    CatalogError,
    catalog::versions::v2::{Catalog, CatalogLimits},
    error::enterprise::EnterpriseCatalogError,
};

async fn catalog() -> Arc<Catalog> {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let time: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    catalog_with_store_and_time(store, time).await
}

async fn catalog_with_store_and_time(
    store: Arc<dyn ObjectStore>,
    time: Arc<dyn TimeProvider>,
) -> Arc<Catalog> {
    Catalog::new_enterprise(
        "test-node",
        "test-cluster",
        store,
        time,
        Default::default(),
        Arc::new(CatalogLimits::default()),
        Default::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_set_all_generation_durations_empty() {
    let catalog = catalog().await;
    assert!(
        catalog
            .set_all_generation_durations(&[])
            .await
            .is_ok_and(|r| r.is_none())
    );
}

#[tokio::test]
async fn test_set_all_generation_durations_too_many() {
    let catalog = catalog().await;
    let durations: Vec<Duration> = (0..=u8::MAX).map(|_| Duration::default()).collect();
    assert!(matches!(
        catalog
            .set_all_generation_durations(&durations)
            .await
            .unwrap_err(),
        CatalogError::Enterprise(EnterpriseCatalogError::TooManyCompactedGenerations { .. })
    ));
}

#[tokio::test]
async fn test_set_all_generation_durations_gen1_only() {
    let catalog = catalog().await;
    assert!(
        catalog
            .set_all_generation_durations(&[Duration::from_secs(60)])
            .await
            .is_ok()
    );
    // Verify gen1 was set
    let inner = catalog.inner.read();
    assert_eq!(
        inner.generation_config.duration_for_level(1),
        Some(Duration::from_secs(60))
    );
}

#[tokio::test]
async fn test_set_all_generation_durations_multiple() {
    let catalog = catalog().await;
    assert!(
        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),
                Duration::from_secs(120),
                Duration::from_secs(240),
            ])
            .await
            .is_ok()
    );
    // Verify all generations were set
    let inner = catalog.inner.read();
    assert_eq!(
        inner.generation_config.duration_for_level(1),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        inner.generation_config.duration_for_level(2),
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        inner.generation_config.duration_for_level(3),
        Some(Duration::from_secs(240))
    );
}

#[tokio::test]
async fn test_set_all_generation_durations_misaligned() {
    let catalog = catalog().await;
    assert!(matches!(
        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),
                Duration::from_secs(100), // Not a multiple of 60
            ])
            .await
            .unwrap_err(),
        CatalogError::Enterprise(EnterpriseCatalogError::MisalignedGenerations)
    ));
}

#[tokio::test]
async fn test_set_all_generation_durations_cannot_change_existing() {
    let catalog = catalog().await;
    // First set
    catalog
        .set_all_generation_durations(&[Duration::from_secs(60), Duration::from_secs(120)])
        .await
        .unwrap();
    // Try to change gen1
    assert!(matches!(
        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(90), // Different gen1
                Duration::from_secs(180),
            ])
            .await
            .unwrap_err(),
        CatalogError::CannotChangeGenerationDuration { level: 1, .. }
    ));
}

#[tokio::test]
async fn test_set_all_generation_durations_can_add_to_existing() {
    let catalog = catalog().await;
    // First set gen1 and gen2
    catalog
        .set_all_generation_durations(&[Duration::from_secs(60), Duration::from_secs(120)])
        .await
        .unwrap();
    // Add gen3
    assert!(
        catalog
            .set_all_generation_durations(&[
                Duration::from_secs(60),  // Same gen1
                Duration::from_secs(120), // Same gen2
                Duration::from_secs(240), // New gen3
            ])
            .await
            .is_ok()
    );
    // Verify gen3 was added
    let inner = catalog.inner.read();
    assert_eq!(
        inner.generation_config.duration_for_level(3),
        Some(Duration::from_secs(240))
    );
}

#[tokio::test]
async fn test_set_all_generation_durations_no_change_returns_already_exists() {
    let catalog = catalog().await;
    catalog
        .set_all_generation_durations(&[Duration::from_secs(60), Duration::from_secs(120)])
        .await
        .unwrap();
    // Try to set same durations again
    assert!(matches!(
        catalog
            .set_all_generation_durations(&[Duration::from_secs(60), Duration::from_secs(120),])
            .await
            .unwrap_err(),
        CatalogError::AlreadyExists
    ));
}

#[tokio::test]
async fn test_set_all_generation_durations_atomic_operation() {
    let catalog = catalog().await;
    // Try to set durations where gen2 is invalid
    // This should fail and no durations should be set
    let result = catalog
        .set_all_generation_durations(&[
            Duration::from_secs(60),
            Duration::from_secs(100), // Invalid - not a multiple of 60
            Duration::from_secs(200), // This would also be invalid
        ])
        .await;
    assert!(result.is_err());

    // Verify nothing was set
    let inner = catalog.inner.read();
    assert_eq!(inner.generation_config.duration_for_level(1), None);
    assert_eq!(inner.generation_config.duration_for_level(2), None);
    assert_eq!(inner.generation_config.duration_for_level(3), None);
}
