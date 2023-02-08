use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::PartitionId;
use iox_catalog::interface::Catalog;

use super::PartitionsSource;

#[derive(Debug)]
/// Returns all partitions in the catalog, regardless of any other condition
pub struct CatalogAllPartitionsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogAllPartitionsSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogAllPartitionsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog_all")
    }
}

#[async_trait]
impl PartitionsSource for CatalogAllPartitionsSource {
    async fn fetch(&self) -> Vec<PartitionId> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("list_ids", || async {
                self.catalog
                    .repositories()
                    .await
                    .partitions()
                    .list_ids()
                    .await
            })
            .await
            .expect("retry forever")
    }
}
