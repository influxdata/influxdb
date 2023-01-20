use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{NamespaceId, NamespaceSchema};
use iox_catalog::interface::{get_schema_by_id, Catalog};

use super::NamespacesSource;

#[derive(Debug)]
pub struct CatalogNamespacesSource {
    _backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogNamespacesSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            _backoff_config: backoff_config,
            catalog,
        }
    }
}

impl Display for CatalogNamespacesSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "catalog")
    }
}

#[async_trait]
impl NamespacesSource for CatalogNamespacesSource {
    async fn fetch(&self, ns: NamespaceId) -> Option<NamespaceSchema> {
        let mut repos = self.catalog.repositories().await;

        // todos:
        //   1. Make this method perform retries.
        //   2. If we do not plan to cache all table of a namespace, we should remove this function
        //      and instead read and build TableSchema for a given TableId.
        let ns = get_schema_by_id(ns, repos.as_mut()).await;

        if let Ok(ns) = ns {
            return Some(ns);
        } else {
            return None;
        }
    }
}
