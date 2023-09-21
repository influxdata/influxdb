use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Partition, PartitionId, TransitionPartitionId};
use iox_catalog::{interface::Catalog, partition_lookup};

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
                let mut repos = self.catalog.repositories().await;
                let id = TransitionPartitionId::Deprecated(partition_id);
                partition_lookup(repos.as_mut(), &id).await
            })
            .await
            .expect("retry forever")
    }
}
