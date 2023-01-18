use std::sync::Arc;

use self::{file_filter::FileFilter, partition_filter::PartitionFilter};

pub mod file_filter;
pub mod partition_filter;

#[derive(Debug)]
pub struct Rules {
    pub file_filters: Vec<Arc<dyn FileFilter>>,
    pub partition_filters: Vec<Arc<dyn PartitionFilter>>,
}

/// Get hardcoded rules.
///
/// TODO: make this a runtime config
pub fn hardcoded_rules() -> Arc<Rules> {
    Arc::new(Rules {
        file_filters: vec![],
        partition_filters: vec![],
    })
}
