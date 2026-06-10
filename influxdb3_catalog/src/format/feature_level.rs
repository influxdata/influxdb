//! Cluster-wide feature level for forward-compatibility gating.

use std::cmp::Ordering;

use bitcode::{Decode, Encode};

use super::RecordId;
use super::record_id::RecordIdKind;
use super::registry::REGISTRY;

/// A cluster feature level: the highest sequential record ID a node
/// understands for Core and Enterprise catalog features, respectively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, Encode, Decode)]
pub struct FeatureLevel {
    pub core: u16,
    pub enterprise: u16,
}

impl Default for FeatureLevel {
    fn default() -> Self {
        Self::ZERO
    }
}

impl FeatureLevel {
    /// A node with `FeatureLevel::ZERO` is a node that was registered
    /// by a version of the software that did not support this feature.
    pub const ZERO: Self = Self {
        core: 0,
        enterprise: 0,
    };

    /// Whether `record_id` is compatible with this feature level.
    pub fn allows(self, record_id: RecordId) -> bool {
        if let Some(enterprise) = record_id.as_enterprise() {
            enterprise <= self.enterprise
        } else {
            record_id.raw() <= self.core
        }
    }

    pub fn min(&self, other: &Self) -> Self {
        Self {
            core: self.core.min(other.core),
            enterprise: self.enterprise.min(other.enterprise),
        }
    }
}

/// Component-wise partial order on feature levels.
///
/// Returns `Some(Less)` / `Some(Greater)` only when *both* `core` and
/// `enterprise` components agree on the direction (or are equal). Two
/// levels with crossed components, i.e., one higher and the other lower,
/// are genuinely incomparable (returns `None`), since neither binary can
/// apply the other's full record set.
///
/// This is intentionally not the lexicographic order;
/// `#[derive(PartialOrd)]` would produce: lex order would imply
/// `(3, 5) < (5, 3)`, masking the incomparability that the
/// forward-compatibility checks need to surface.
///
/// `Ord` is deliberately not implemented — the order is partial, and a
/// total-order extension would silently flatten the incomparable cases
/// into one direction or the other.
impl PartialOrd for FeatureLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (
            self.core.cmp(&other.core),
            self.enterprise.cmp(&other.enterprise),
        ) {
            (Ordering::Equal, Ordering::Equal) => Some(Ordering::Equal),
            (Ordering::Less | Ordering::Equal, Ordering::Less | Ordering::Equal) => {
                Some(Ordering::Less)
            }
            (Ordering::Greater | Ordering::Equal, Ordering::Greater | Ordering::Equal) => {
                Some(Ordering::Greater)
            }
            _ => None,
        }
    }
}

/// Derive the local feature level from the compiled record registry.
///
/// Walks every `RegisteredRecord` in `inventory` and tracks the
/// maximum sequence for each of `core` and `enterprise`.
pub fn derive_feature_level() -> FeatureLevel {
    let mut level = FeatureLevel::ZERO;
    for entry in REGISTRY.all() {
        match entry.id.kind() {
            RecordIdKind::Core(core) => {
                level.core = level.core.max(core);
            }
            RecordIdKind::Enterprise(enterprise) => {
                level.enterprise = level.enterprise.max(enterprise);
            }
        }
    }
    level
}

#[cfg(test)]
mod tests;
