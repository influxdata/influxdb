use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use iox_catalog::interface::Catalog;

use crate::{commit::Error, Commit};

#[derive(Debug)]
pub(crate) struct CatalogCommit {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogCommit {
    pub(crate) fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
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
    ) -> Result<Vec<ParquetFileId>, Error> {
        let is_upgrade_commit = !upgrade.is_empty();
        let is_replacement_commit = !delete.is_empty() || !create.is_empty();
        let replacement_commit_is_ok = !delete.is_empty() && !create.is_empty();

        match (is_upgrade_commit, is_replacement_commit) {
            (false, false) => {
                return Err(Error::BadRequest("commit must have files to upgrade, and/or a set of files to replace (delete and create)".into()));
            }
            (_, true) if !replacement_commit_is_ok => {
                return Err(Error::BadRequest(
                    "replacement commits must have both files to delete and files to create".into(),
                ));
            }
            _ => {} // is ok
        }

        let delete = delete.iter().map(|f| f.id).collect::<Vec<_>>();
        let upgrade = upgrade.iter().map(|f| f.id).collect::<Vec<_>>();

        let result = Backoff::new(&self.backoff_config)
            .retry_all_errors("commit parquet file changes", || async {
                let mut repos = self.catalog.repositories().await;
                let parquet_files = repos.parquet_files();
                let ids = parquet_files
                    .create_upgrade_delete(&delete, &upgrade, create, target_level)
                    .await?;

                Ok::<_, iox_catalog::interface::Error>(ids)
            })
            .await
            .expect("retry forever");

        if result.len() != create.len() {
            return Err(Error::InvalidCatalogResult(format!(
                "Number of created parquet files is invalid: expected {} but found {}",
                create.len(),
                result.len()
            )));
        }

        return Ok(result);
    }
}
