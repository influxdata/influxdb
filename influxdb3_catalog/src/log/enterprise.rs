use std::collections::btree_set;

use super::{NodeMode, NodeModes};

impl NodeModes {
    pub fn is_compactor(&self) -> bool {
        self.0.contains(&NodeMode::Compact) || self.0.contains(&NodeMode::All)
    }

    pub fn is_ingester(&self) -> bool {
        self.0.contains(&NodeMode::Ingest) || self.0.contains(&NodeMode::All)
    }

    pub fn is_querier(&self) -> bool {
        self.0.contains(&NodeMode::Query)
            || self.0.contains(&NodeMode::All)
            || self.0.contains(&NodeMode::Process)
    }

    pub fn contains(&self, mode: &NodeMode) -> bool {
        self.0.contains(mode)
    }

    pub fn contains_only(&self, mode: &NodeMode) -> bool {
        self.0.len() == 1 && self.0.contains(mode)
    }

    pub fn into_iter(&self) -> btree_set::IntoIter<NodeMode> {
        self.0.clone().into_iter()
    }
}
