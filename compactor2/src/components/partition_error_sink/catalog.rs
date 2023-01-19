use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;

use super::PartitionErrorSink;

#[derive(Debug)]
pub struct CatalogPartitionErrorSink {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogPartitionErrorSink {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogPartitionErrorSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl PartitionErrorSink for CatalogPartitionErrorSink {
    async fn record(&self, partition: PartitionId, msg: &str) {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("store partition error in catalog", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    // TODO: remove stats from the catalog since the couple the catalog state to the algorithm implementation
                    .record_skipped_compaction(partition, msg, 0, 0, 0, 0, 0)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
