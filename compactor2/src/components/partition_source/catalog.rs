use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Partition, PartitionId};
use iox_catalog::interface::Catalog;

use super::PartitionSource;

#[derive(Debug)]
pub struct CatalogPartitionSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogPartitionSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogPartitionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl PartitionSource for CatalogPartitionSource {
    async fn fetch_by_id(&self, partition_id: PartitionId) -> Option<Partition> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("partition_by_id", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .get_by_id(partition_id)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
