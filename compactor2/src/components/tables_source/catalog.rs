use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Table, TableId};
use iox_catalog::interface::Catalog;

use super::TablesSource;

#[derive(Debug)]
pub struct CatalogTablesSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogTablesSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogTablesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl TablesSource for CatalogTablesSource {
    async fn fetch(&self, table: TableId) -> Option<Table> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("table_of_given_table_id", || async {
                self.catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_id(table)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
