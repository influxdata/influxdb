use std::{collections::HashSet, fmt::Display, time::Duration};

use data_types::PartitionId;

/// Default threshold for hot partitions
const DEFAULT_PARTITION_MINUTE_THRESHOLD: u64 = 10;

/// Partitions source config.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionsSourceConfig {
    /// For "hot" compaction: use the catalog to determine which partitions have recently received
    /// writes, defined as having a new Parquet file created within the last `threshold`.
    CatalogRecentWrites {
        /// The amount of time ago to look for Parquet file creations
        threshold: Duration,
    },

    /// Use all partitions from the catalog.
    ///
    /// This does NOT consider if/when a partition received any writes.
    CatalogAll,

    /// Use a fixed set of partitions.
    ///
    /// This is mostly useful for debugging.
    Fixed(HashSet<PartitionId>),
}

impl Display for PartitionsSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatalogRecentWrites { threshold } => {
                write!(f, "catalog_recent_writes({threshold:?})")
            }
            Self::CatalogAll => write!(f, "catalog_all"),
            Self::Fixed(p_ids) => {
                let mut p_ids = p_ids.iter().copied().collect::<Vec<_>>();
                p_ids.sort();
                write!(f, "fixed({p_ids:?})")
            }
        }
    }
}

impl Default for PartitionsSourceConfig {
    fn default() -> Self {
        Self::CatalogRecentWrites {
            threshold: Duration::from_secs(DEFAULT_PARTITION_MINUTE_THRESHOLD * 60),
        }
    }
}
