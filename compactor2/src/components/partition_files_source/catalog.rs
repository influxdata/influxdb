use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{ParquetFile, PartitionId};
use iox_catalog::interface::Catalog;

use super::PartitionFilesSource;

#[derive(Debug)]
pub struct CatalogPartitionFilesSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogPartitionFilesSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogPartitionFilesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl PartitionFilesSource for CatalogPartitionFilesSource {
    async fn fetch(&self, partition: PartitionId) -> Vec<ParquetFile> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("parquet_files_of_given_partition", || async {
                self.catalog
                    .repositories()
                    .await
                    .parquet_files()
                    .list_by_partition_not_to_delete(partition)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
