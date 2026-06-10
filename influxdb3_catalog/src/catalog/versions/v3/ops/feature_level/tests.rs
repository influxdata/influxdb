use std::sync::Arc;

use uuid::Uuid;

use super::{AdvanceFeatureLevelArgs, AdvanceFeatureLevelOp};
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::format::{FeatureLevel, RecordBatch};

fn fresh_catalog_with_committed(committed: FeatureLevel) -> InnerCatalog {
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    catalog.committed_feature_level = committed;
    catalog
}

#[test]
fn prepare_pushes_record_when_strictly_higher() {
    let catalog = fresh_catalog_with_committed(FeatureLevel {
        core: 1,
        enterprise: 0,
    });
    let mut batch = RecordBatch::new(1);

    AdvanceFeatureLevelOp::prepare(
        &AdvanceFeatureLevelArgs {
            committed: FeatureLevel {
                core: 5,
                enterprise: 2,
            },
        },
        &catalog,
        &mut batch,
    )
    .expect("higher target advances");

    assert_eq!(batch.len(), 1);
}

#[test]
fn prepare_rejects_equal_target_as_no_change() {
    let level = FeatureLevel {
        core: 5,
        enterprise: 2,
    };
    let catalog = fresh_catalog_with_committed(level);
    let mut batch = RecordBatch::new(1);

    let err = AdvanceFeatureLevelOp::prepare(
        &AdvanceFeatureLevelArgs { committed: level },
        &catalog,
        &mut batch,
    )
    .expect_err("equal target is a no-op");

    assert!(matches!(err, CatalogError::NoCatalogChange { .. }));
    assert_eq!(batch.len(), 0);
}

#[test]
fn prepare_rejects_regression() {
    let catalog = fresh_catalog_with_committed(FeatureLevel {
        core: 5,
        enterprise: 2,
    });
    let mut batch = RecordBatch::new(1);

    let err = AdvanceFeatureLevelOp::prepare(
        &AdvanceFeatureLevelArgs {
            committed: FeatureLevel {
                core: 4,
                enterprise: 2,
            },
        },
        &catalog,
        &mut batch,
    )
    .expect_err("regressing target is rejected");

    assert!(matches!(err, CatalogError::Internal { .. }));
    assert_eq!(batch.len(), 0);
}
