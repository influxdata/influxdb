use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{PartitionId, SkippedCompaction};
use iox_catalog::interface::Catalog;

use super::SkippedCompactionsSource;

#[derive(Debug)]
pub struct CatalogSkippedCompactionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogSkippedCompactionsSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogSkippedCompactionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl SkippedCompactionsSource for CatalogSkippedCompactionsSource {
    async fn fetch(&self, partition: PartitionId) -> Option<SkippedCompaction> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("skipped_compaction_of_given_partition", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_in_skipped_compaction(partition)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
