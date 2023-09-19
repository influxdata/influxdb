use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Column, TableId};
use iox_catalog::interface::Catalog;

use super::ColumnsSource;

#[derive(Debug)]
pub struct CatalogColumnsSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogColumnsSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogColumnsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl ColumnsSource for CatalogColumnsSource {
    async fn fetch(&self, table: TableId) -> Vec<Column> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("table_of_given_table_id", || async {
                self.catalog
                    .repositories()
                    .await
                    .columns()
                    .list_by_table_id(table)
                    .await
            })
            .await
            .expect("retry forever")
    }
}
