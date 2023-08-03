use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;

use super::PartitionsSubsetSource;

#[derive(Debug)]
pub(crate) struct SkippedPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl SkippedPartitionsSource {
    pub(crate) fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for SkippedPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "skipped_partitions_catalog")
    }
}

#[async_trait]
impl PartitionsSubsetSource for SkippedPartitionsSource {
    async fn fetch(&self, partitions: &[PartitionId]) -> Vec<PartitionId> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("skipped_compaction_of_given_partitions", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_in_skipped_compactions(partitions)
                    .await
            })
            .await
            .expect("retry forever")
            .iter()
            .map(|sc| sc.partition_id)
            .collect()
    }
}
