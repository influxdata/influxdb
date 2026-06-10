use super::{FeatureLevel, derive_feature_level};
use crate::format::RecordId;

#[test]
fn zero_allows_nothing() {
    assert!(!FeatureLevel::ZERO.allows(RecordId::core(1)));
    assert!(!FeatureLevel::ZERO.allows(RecordId::enterprise(1)));
}

#[test]
fn allows_core_enterprise_independently() {
    let level = FeatureLevel {
        core: 5,
        enterprise: 2,
    };

    // Core sequences within bounds.
    assert!(level.allows(RecordId::core(1)));
    assert!(level.allows(RecordId::core(5)));
    assert!(!level.allows(RecordId::core(6)));

    // Enterprise sequences within bounds (independent of core).
    assert!(level.allows(RecordId::enterprise(1)));
    assert!(level.allows(RecordId::enterprise(2)));
    assert!(!level.allows(RecordId::enterprise(3)));
    // Enterprise records do not borrow core's level.
    assert!(!level.allows(RecordId::enterprise(5)));
}

#[test]
fn derive_matches_compiled_registry() {
    // Ensure that the registered level for core and enterprise is > 0 to
    // catch a potential regression where the registry is not loaded at all
    let level = derive_feature_level();
    assert!(level.core > 0, "derived core feature level should be > 0");
    assert!(
        level.enterprise > 0,
        "derived enterprise feature level should be > 0"
    );
}

#[test]
fn partial_ord_component_wise() {
    use std::cmp::Ordering;
    let a = FeatureLevel {
        core: 5,
        enterprise: 2,
    };
    let b = FeatureLevel {
        core: 5,
        enterprise: 2,
    };
    let is_lower = FeatureLevel {
        core: 4,
        enterprise: 1,
    };
    let is_higher = FeatureLevel {
        core: 6,
        enterprise: 3,
    };
    let is_neither_lower_or_higher = FeatureLevel {
        core: 6,
        enterprise: 1,
    };

    // Equality.
    assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));

    // higher/lower-comparable cases use `Less` / `Greater`.
    assert_eq!(a.partial_cmp(&is_lower), Some(Ordering::Greater));
    assert_eq!(a.partial_cmp(&is_higher), Some(Ordering::Less));

    // Crossed components are incomparable — neither binary can apply
    // the other's full record set.
    assert!(a.partial_cmp(&is_neither_lower_or_higher).is_none());
    assert!(is_neither_lower_or_higher.partial_cmp(&a).is_none());
}

#[test]
fn feature_level_encoding_is_stable() {
    assert_encoding_stable!(
        FeatureLevel {
            core: 25,
            enterprise: 3,
        },
        "19000300"
    );
}
