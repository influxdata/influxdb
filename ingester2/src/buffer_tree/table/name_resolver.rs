use std::{sync::Arc, time::Duration};

use backoff::{Backoff, BackoffConfig};
use data_types::TableId;
use iox_catalog::interface::Catalog;

use super::TableName;
use crate::deferred_load::DeferredLoad;

/// An abstract provider of a [`DeferredLoad`] configured to fetch the
/// [`TableName`] of the specified [`TableId`].
pub(crate) trait TableNameProvider: Send + Sync + std::fmt::Debug {
    fn for_table(&self, id: TableId) -> DeferredLoad<TableName>;
}

#[derive(Debug)]
pub(crate) struct TableNameResolver {
    max_smear: Duration,
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
}

impl TableNameResolver {
    pub(crate) fn new(
        max_smear: Duration,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            max_smear,
            catalog,
            backoff_config,
        }
    }

    /// Fetch the [`TableName`] from the [`Catalog`] for specified
    /// `table_id`, retrying endlessly when errors occur.
    pub(crate) async fn fetch(
        table_id: TableId,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> TableName {
        Backoff::new(&backoff_config)
            .retry_all_errors("fetch table name", || async {
                let s = catalog
                    .repositories()
                    .await
                    .tables()
                    .get_by_id(table_id)
                    .await?
                    .expect("resolving table name for non-existent table id")
                    .name
                    .into();

                Result::<_, iox_catalog::interface::Error>::Ok(s)
            })
            .await
            .expect("retry forever")
    }
}

impl TableNameProvider for TableNameResolver {
    fn for_table(&self, id: TableId) -> DeferredLoad<TableName> {
        DeferredLoad::new(
            self.max_smear,
            Self::fetch(id, Arc::clone(&self.catalog), self.backoff_config.clone()),
        )
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;

    #[derive(Debug)]
    pub(crate) struct MockTableNameProvider {
        name: TableName,
    }

    impl MockTableNameProvider {
        pub(crate) fn new(name: impl Into<TableName>) -> Self {
            Self { name: name.into() }
        }
    }

    impl Default for MockTableNameProvider {
        fn default() -> Self {
            Self::new("bananas")
        }
    }

    impl TableNameProvider for MockTableNameProvider {
        fn for_table(&self, _id: TableId) -> DeferredLoad<TableName> {
            let name = self.name.clone();
            DeferredLoad::new(Duration::from_secs(1), async { name })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::ShardIndex;
    use test_helpers::timeout::FutureTimeout;

    use super::*;
    use crate::test_util::populate_catalog;

    const SHARD_INDEX: ShardIndex = ShardIndex::new(24);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";

    #[tokio::test]
    async fn test_fetch() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (_shard_id, _ns_id, table_id) =
            populate_catalog(&*catalog, SHARD_INDEX, NAMESPACE_NAME, TABLE_NAME).await;

        let fetcher = Arc::new(TableNameResolver::new(
            Duration::from_secs(10),
            Arc::clone(&catalog),
            backoff_config.clone(),
        ));

        let got = fetcher
            .for_table(table_id)
            .get()
            .with_timeout_panic(Duration::from_secs(5))
            .await;
        assert_eq!(&**got, TABLE_NAME);
    }
}
