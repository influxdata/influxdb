use std::{sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::TableId;
use iox_catalog::interface::Catalog;

use super::TableMetadata;
use crate::deferred_load::DeferredLoad;

/// An abstract provider of a [`DeferredLoad`] configured to fetch the
/// catalog [`TableMetadata`] of the specified [`TableId`].
pub(crate) trait TableProvider: Send + Sync + std::fmt::Debug {
    fn for_table(&self, id: TableId) -> DeferredLoad<TableMetadata>;
}

#[derive(Debug)]
pub(crate) struct TableResolver {
    max_smear: Duration,
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
    metrics: Arc<metric::Registry>,
}

impl TableResolver {
    pub(crate) fn new(
        max_smear: Duration,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
        metrics: Arc<metric::Registry>,
    ) -> Self {
        Self {
            max_smear,
            catalog,
            backoff_config,
            metrics,
        }
    }

    /// Fetch the [`TableMetadata`] from the [`Catalog`] for specified
    /// `table_id`, retrying endlessly when errors occur.
    pub(crate) async fn fetch(
        table_id: TableId,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> TableMetadata {
        Backoff::new(&backoff_config)
            .retry_all_errors("fetch table", || async {
                let table = catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_id(table_id)
                    .await?
                    .unwrap_or_else(|| {
                        panic!("resolving table name for non-existent table id {table_id}")
                    })
                    .into();

                Result::<_, iox_catalog::interface::Error>::Ok(table)
            })
            .await
            .expect("retry forever")
    }
}

impl TableProvider for TableResolver {
    fn for_table(&self, id: TableId) -> DeferredLoad<TableMetadata> {
        DeferredLoad::new(
            self.max_smear,
            Self::fetch(id, Arc::clone(&self.catalog), self.backoff_config.clone()),
            &self.metrics,
        )
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;

    #[derive(Debug)]
    pub(crate) struct MockTableProvider {
        table: TableMetadata,
    }

    impl MockTableProvider {
        pub(crate) fn new(table: impl Into<TableMetadata>) -> Self {
            Self {
                table: table.into(),
            }
        }
    }

    impl Default for MockTableProvider {
        fn default() -> Self {
            Self::new(TableMetadata::new_for_testing(
                "bananas".into(),
                Default::default(),
            ))
        }
    }

    impl TableProvider for MockTableProvider {
        fn for_table(&self, _id: TableId) -> DeferredLoad<TableMetadata> {
            let table = self.table.clone();
            DeferredLoad::new(
                Duration::from_secs(1),
                async { table },
                &metric::Registry::default(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use test_helpers::timeout::FutureTimeout;

    use super::*;
    use crate::test_util::populate_catalog;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";

    #[tokio::test]
    async fn test_fetch() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the namespace / table
        let (_ns_id, table_id) = populate_catalog(&*catalog, NAMESPACE_NAME, TABLE_NAME).await;

        let fetcher = Arc::new(TableResolver::new(
            Duration::from_secs(10),
            Arc::clone(&catalog),
            backoff_config.clone(),
            metrics,
        ));

        let got = fetcher
            .for_table(table_id)
            .get()
            .with_timeout_panic(Duration::from_secs(5))
            .await;
        assert_eq!(got.name(), TABLE_NAME);
    }
}
