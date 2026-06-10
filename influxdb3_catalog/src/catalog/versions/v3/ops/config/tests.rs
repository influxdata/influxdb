use std::time::Duration;

use crate::CatalogError;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{apply_batch, test_catalog};
use crate::catalog::versions::v3::schema::storage::StorageMode;
use crate::format::RecordBatch;

use super::{
    SetGenerationDurationArgs, SetGenerationDurationOp, SetStorageModeArgs, SetStorageModeOp,
};

#[test]
fn prepare_set_generation_duration() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    SetGenerationDurationOp::prepare(
        &SetGenerationDurationArgs {
            level: 1,
            duration: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    let mut catalog = catalog;
    apply_batch(&batch, &mut catalog);
    assert_eq!(
        catalog.generation_config.duration_for_level(1),
        Some(Duration::from_secs(3600))
    );
}

#[test]
fn prepare_set_generation_duration_conflict() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    SetGenerationDurationOp::prepare(
        &SetGenerationDurationArgs {
            level: 1,
            duration: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = SetGenerationDurationOp::prepare(
        &SetGenerationDurationArgs {
            level: 1,
            duration: Duration::from_secs(7200),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(
        result,
        Err(CatalogError::CannotChangeGenerationDuration { .. })
    ));
}

#[test]
fn prepare_set_generation_duration_same_value() {
    let mut catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    SetGenerationDurationOp::prepare(
        &SetGenerationDurationArgs {
            level: 1,
            duration: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = SetGenerationDurationOp::prepare(
        &SetGenerationDurationArgs {
            level: 1,
            duration: Duration::from_secs(3600),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::NoCatalogChange { .. })));
}

#[test]
fn prepare_set_storage_mode() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    SetStorageModeOp::prepare(
        &SetStorageModeArgs {
            storage_mode: StorageMode::ParquetAndPachaTree,
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    let mut catalog = catalog;
    apply_batch(&batch, &mut catalog);
    assert_eq!(catalog.storage_mode, StorageMode::ParquetAndPachaTree);
}

#[test]
fn prepare_set_storage_mode_invalid_transition() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = SetStorageModeOp::prepare(
        &SetStorageModeArgs {
            storage_mode: StorageMode::PachaTree,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::Internal { .. })));
}

#[test]
fn prepare_set_storage_mode_no_change() {
    let catalog = test_catalog();
    let mut batch = RecordBatch::new(1);

    let result = SetStorageModeOp::prepare(
        &SetStorageModeArgs {
            storage_mode: StorageMode::Parquet,
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::NoCatalogChange { .. })));
}
