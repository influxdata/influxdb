//! Cluster-wide feature-level advancement (record_id 1).

use influxdb3_catalog_macros::catalog_record;

use crate::catalog::versions::v3::events::CatalogEvent;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::FeatureLevel;
use crate::format::apply::ApplyError;
use crate::format::{RecordApply, record_ids};

/// Advance the cluster's committed feature level. Once applied, records
/// up to `committed` may be written by any node in the cluster.
#[catalog_record(id = record_ids::ADVANCE_FEATURE_LEVEL, shape = 0x0dee8f6e)]
#[derive(Copy)]
pub struct AdvanceFeatureLevel {
    /// The new committed feature level.
    pub committed: FeatureLevel,
}

impl RecordApply for AdvanceFeatureLevel {
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

#[cfg(test)]
mod tests;
