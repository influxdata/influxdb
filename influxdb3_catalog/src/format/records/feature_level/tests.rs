use std::sync::Arc;

use uuid::Uuid;

use super::AdvanceFeatureLevel;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::records::assert_roundtrip;
use crate::format::{CatalogRecord, FeatureLevel, derive_feature_level};

#[test]
fn record_id() {
    assert_eq!(AdvanceFeatureLevel::ID.raw(), 1);
    assert!(!AdvanceFeatureLevel::FLAGS.is_upgrade_safe());
}

#[test]
fn round_trip() {
    assert_roundtrip!(
        AdvanceFeatureLevel {
            committed: FeatureLevel {
                core: 25,
                enterprise: 3,
            },
        },
        "19000300"
    );
}

#[test]
fn apply_sets_committed_feature_level() {
    let mut catalog = InnerCatalog::new(Arc::from("test"), Uuid::nil());
    // Fresh catalog defaults to the local binary's level.
    assert_eq!(catalog.committed_feature_level, derive_feature_level());

    let target = FeatureLevel {
        core: 99,
        enterprise: 7,
    };
    AdvanceFeatureLevel { committed: target }
        .apply(&mut catalog)
        .unwrap();
    assert_eq!(catalog.committed_feature_level, target);
}
