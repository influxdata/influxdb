use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;

use crate::error::DynError;

use super::PartitionDoneSink;

#[derive(Debug)]
pub struct CatalogPartitionDoneSink {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogPartitionDoneSink {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogPartitionDoneSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl PartitionDoneSink for CatalogPartitionDoneSink {
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) {
        if let Err(e) = res {
            let msg = e.to_string();

            Backoff::new(&self.backoff_config)
                .retry_all_errors("store partition error in catalog", || async {
                    self.catalog
                        .repositories()
                        .await
                        .partitions()
                        // TODO: remove stats from the catalog since the couple the catalog state to the algorithm implementation
                        .record_skipped_compaction(partition, &msg, 0, 0, 0, 0, 0)
                        .await
                })
                .await
                .expect("retry forever");
        }
    }
}
