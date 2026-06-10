use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::generation::{SetGenerationDuration, SetStorageMode};
use crate::format::records::types::StorageMode;

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

#[test]
fn set_generation_duration_round_trip() {
    assert_roundtrip!(
        SetGenerationDuration {
            level: 2,
            duration_ns: 3_600_000_000_000,
        },
        "020000a0b83046030000"
    );
}

#[test]
fn set_storage_mode_round_trip() {
    assert_roundtrip!(
        SetStorageMode {
            mode: StorageMode::Parquet
        },
        "00"
    );
    assert_roundtrip!(
        SetStorageMode {
            mode: StorageMode::PachaTree
        },
        "01"
    );
    assert_roundtrip!(
        SetStorageMode {
            mode: StorageMode::ParquetAndPachaTree
        },
        "02"
    );
}

#[test]
fn record_ids() {
    assert_eq!(SetGenerationDuration::ID.raw(), 24);
    assert!(!SetGenerationDuration::FLAGS.is_upgrade_safe());

    assert_eq!(SetStorageMode::ID.raw(), 25);
    assert!(!SetStorageMode::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_set_generation_duration() {
    let mut catalog = test_catalog();
    SetGenerationDuration {
        level: 1,
        duration_ns: 3_600_000_000_000, // 1 hour
    }
    .apply(&mut catalog)
    .unwrap();

    let duration = catalog.generation_config.duration_for_level(1).unwrap();
    assert_eq!(duration, std::time::Duration::from_secs(3600));
}

#[test]
fn apply_set_storage_mode() {
    let mut catalog = test_catalog();
    SetStorageMode {
        mode: StorageMode::PachaTree,
    }
    .apply(&mut catalog)
    .unwrap();

    assert!(matches!(
        catalog.storage_mode,
        crate::catalog::versions::v3::schema::storage::StorageMode::PachaTree
    ));
}
