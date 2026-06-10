//! `AdvanceFeatureLevelOp` — raise the committed feature level
//! independently of a node register operation.

use super::CatalogOp;
use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::FeatureLevel;
use crate::format::RecordBatch;
use crate::format::records::AdvanceFeatureLevel;

#[derive(Debug)]
pub(crate) struct AdvanceFeatureLevelArgs {
    pub committed: FeatureLevel,
}

#[derive(Debug)]
pub(crate) struct AdvanceFeatureLevelOp;

impl CatalogOp for AdvanceFeatureLevelOp {
    type Input = AdvanceFeatureLevelArgs;
    type Output = ();

    fn prepare(
        args: &Self::Input,
        catalog: &InnerCatalog,
        records: &mut RecordBatch,
    ) -> Result<Self, CatalogError> {
        // Reject regressions: the target must be at-or-above the current
        // level in every partition. Equality is a no-op.
        let current = catalog.committed_feature_level;
        let target = args.committed;
        if target.core < current.core || target.enterprise < current.enterprise {
            return Err(CatalogError::Internal {
                details: format!(
                    "AdvanceFeatureLevel regression: current ({}, {}), target ({}, {})",
                    current.core, current.enterprise, target.core, target.enterprise,
                ),
            });
        }
        if target == current {
            return Err(CatalogError::NoCatalogChange {
                details: format!(
                    "feature level already at ({}, {})",
                    current.core, current.enterprise,
                ),
            });
        }

        records.push(&AdvanceFeatureLevel { committed: target });
        Ok(Self)
    }

    fn output(&self, _catalog: &InnerCatalog) -> Self::Output {}
}

#[cfg(test)]
mod tests;
