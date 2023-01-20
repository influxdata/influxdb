use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Namespace, NamespaceId, NamespaceSchema};
use iox_catalog::interface::{get_schema_by_id, Catalog};

use super::NamespacesSource;

#[derive(Debug)]
pub struct CatalogNamespacesSource {
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl CatalogNamespacesSource {
    pub fn new(backoff_config: BackoffConfig, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            backoff_config,
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
    async fn fetch_by_id(&self, ns: NamespaceId) -> Option<Namespace> {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("namespace_of_given_namespace_id", || async {
                self.catalog
                    .repositories()
                    .await
                    .namespaces()
                    .get_by_id(ns)
                    .await
            })
            .await
            .expect("retry forever")
    }

    async fn fetch_schema_by_id(&self, ns: NamespaceId) -> Option<NamespaceSchema> {
        let mut repos = self.catalog.repositories().await;

        // todos:
        //   1. Make this method perform retries.
        //   2. If we do not plan to cache all table of a namespace, we should remove this function
        //      and instead read and build TableSchema for a given TableId.
        let ns = get_schema_by_id(ns, repos.as_mut()).await;

        ns.ok()
    }
}
