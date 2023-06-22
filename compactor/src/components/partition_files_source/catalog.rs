use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{ParquetFile, PartitionId};
use iox_catalog::interface::Catalog;

use super::{rate_limit::QueryRateLimit, PartitionFilesSource};

#[async_trait]
pub(crate) trait CatalogQuerier: Send + Sync + Debug {
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error>;
}

#[async_trait]
impl CatalogQuerier for Arc<dyn Catalog> {
    async fn get_partitions(
        &self,
        partition_id: PartitionId,
    ) -> Result<Vec<ParquetFile>, iox_catalog::interface::Error> {
        self.repositories()
            .await
            .parquet_files()
            .list_by_partition_not_to_delete(partition_id)
            .await
    }
}

#[derive(Debug)]
pub struct CatalogPartitionFilesSource<T = QueryRateLimit<Arc<dyn Catalog>>> {
    backoff_config: BackoffConfig,
    catalog: T,
}

impl<T> CatalogPartitionFilesSource<T> {
    pub fn new(backoff_config: BackoffConfig, catalog: T) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl<T> Display for CatalogPartitionFilesSource<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl<T> PartitionFilesSource for CatalogPartitionFilesSource<T>
where
    T: CatalogQuerier,
{
    async fn fetch(&self, partition_id: PartitionId) -> Vec<ParquetFile> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("parquet_files_of_given_partition", || async {
                self.catalog.get_partitions(partition_id).await
            })
            .await
            .expect("retry forever")
    }
}
