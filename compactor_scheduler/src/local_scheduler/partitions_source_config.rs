use std::{collections::HashSet, fmt::Display, time::Duration};

use clap_blocks::compactor_scheduler::PartitionSourceConfigForLocalScheduler;
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

impl PartitionsSourceConfig {
    /// Create a new [`PartitionsSourceConfig`] from the CLI|env config.
    pub fn from_config(config: PartitionSourceConfigForLocalScheduler) -> PartitionsSourceConfig {
        let PartitionSourceConfigForLocalScheduler {
            partition_filter,
            process_all_partitions,
            compaction_partition_minute_threshold,
        } = config;

        match (partition_filter, process_all_partitions) {
            (None, false) => PartitionsSourceConfig::CatalogRecentWrites {
                threshold: Duration::from_secs(compaction_partition_minute_threshold * 60),
            },
            (None, true) => PartitionsSourceConfig::CatalogAll,
            (Some(ids), false) => {
                PartitionsSourceConfig::Fixed(ids.iter().cloned().map(PartitionId::new).collect())
            }
            (Some(_), true) => panic!(
                "provided partition ID filter and specific 'process all', this does not make sense"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic(
        expected = "provided partition ID filter and specific 'process all', this does not make sense"
    )]
    fn process_all_and_partition_filter_incompatible() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: Some(vec![1, 7]),
            process_all_partitions: true,
        };
        PartitionsSourceConfig::from_config(config);
    }

    #[test]
    fn fixed_list_of_partitions() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: Some(vec![1, 7]),
            process_all_partitions: false,
        };
        let partitions_source_config = PartitionsSourceConfig::from_config(config);

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::Fixed([PartitionId::new(1), PartitionId::new(7)].into())
        );
    }

    #[test]
    fn all_in_the_catalog() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: None,
            process_all_partitions: true,
        };
        let partitions_source_config = PartitionsSourceConfig::from_config(config);

        assert_eq!(partitions_source_config, PartitionsSourceConfig::CatalogAll,);
    }

    #[test]
    fn normal_compaction() {
        let config = PartitionSourceConfigForLocalScheduler {
            compaction_partition_minute_threshold: 10,
            partition_filter: None,
            process_all_partitions: false,
        };
        let partitions_source_config = PartitionsSourceConfig::from_config(config);

        assert_eq!(
            partitions_source_config,
            PartitionsSourceConfig::CatalogRecentWrites {
                threshold: Duration::from_secs(600)
            },
        );
    }
}
