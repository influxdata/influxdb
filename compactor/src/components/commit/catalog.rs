use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use iox_catalog::interface::Catalog;

use super::Commit;

#[derive(Debug)]
pub struct CatalogCommit {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogCommit {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl Commit for CatalogCommit {
    async fn commit(
        &self,
        _partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        // Either upgrade or (delete & create) must not empty
        assert!(!upgrade.is_empty() || (!delete.is_empty() && !create.is_empty()));

        let upgrade = upgrade.iter().map(|f| f.id).collect::<Vec<_>>();

        Backoff::new(&self.backoff_config)
            .retry_all_errors("commit parquet file changes", || async {
                let mut txn = self.catalog.start_transaction().await?;

                let parquet_files = txn.parquet_files();

                for file in delete {
                    parquet_files.flag_for_delete(file.id).await?;
                }

                parquet_files
                    .update_compaction_level(&upgrade, target_level)
                    .await?;

                let mut ids = Vec::with_capacity(create.len());
                for file in create {
                    let res = parquet_files.create(file.clone()).await?;
                    ids.push(res.id);
                }

                txn.commit().await?;

                Ok::<_, iox_catalog::interface::Error>(ids)
            })
            .await
            .expect("retry forever")
    }
}
