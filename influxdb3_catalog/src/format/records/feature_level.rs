//! Cluster-wide feature-level advancement (record_id 1).

use super::impl_bitcode_encoding;
use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::FeatureLevel;
use crate::format::apply::ApplyError;
use crate::format::{CatalogRecord, RecordFlags, RecordId, RegisteredRecord, record_ids};

/// Advance the cluster's committed feature level. Once applied, records
/// up to `committed` may be written by any node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, bitcode::Encode, bitcode::Decode)]
pub struct AdvanceFeatureLevel {
    /// The new committed feature level.
    pub committed: FeatureLevel,
}

impl CatalogRecord for AdvanceFeatureLevel {
    const ID: RecordId = record_ids::ADVANCE_FEATURE_LEVEL;
    const FLAGS: RecordFlags = RecordFlags::none();
    const NAME: &'static str = "AdvanceFeatureLevel";

    fn apply(&self, catalog: &mut InnerCatalog) -> Result<(), ApplyError> {
        catalog.committed_feature_level = self.committed;
        Ok(())
    }

    fn event(&self) -> CatalogEvent {
        CatalogEvent::FeatureLevelAdvanced {
            committed: self.committed,
        }
    }
}

inventory::submit! {
    RegisteredRecord::new::<AdvanceFeatureLevel>()
}

impl_bitcode_encoding!(AdvanceFeatureLevel);

#[cfg(test)]
mod tests;
