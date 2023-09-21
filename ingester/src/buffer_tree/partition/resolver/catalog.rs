//! A [`PartitionProvider`] implementation that hits the [`Catalog`] to resolve
//! the partition id and persist offset.

use std::sync::Arc;

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use data_types::{Column, NamespaceId, Partition, PartitionKey, TableId};
use iox_catalog::interface::Catalog;
use observability_deps::tracing::debug;
use parking_lot::Mutex;

use super::r#trait::PartitionProvider;
use crate::{
    buffer_tree::{
        namespace::NamespaceName,
        partition::{
            counter::PartitionCounter, resolver::build_sort_key_from_sort_key_ids_and_columns,
            PartitionData, SortKeyState,
        },
        table::metadata::TableMetadata,
    },
    deferred_load::DeferredLoad,
};

/// A [`PartitionProvider`] implementation that hits the [`Catalog`] to resolve
/// the partition id and persist offset, returning an initialised
/// [`PartitionData`].
#[derive(Debug)]
pub(crate) struct CatalogPartitionResolver {
    catalog: Arc<dyn Catalog>,
    backoff_config: BackoffConfig,
}

impl CatalogPartitionResolver {
    /// Construct a [`CatalogPartitionResolver`] that looks up partitions in
    /// `catalog`.
    pub(crate) fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            backoff_config: Default::default(),
        }
    }

    async fn get(
        &self,
        partition_key: PartitionKey,
        table_id: TableId,
    ) -> Result<Partition, iox_catalog::interface::Error> {
        self.catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(partition_key, table_id)
            .await
    }

    async fn get_columns(
        &self,
        table_id: TableId,
    ) -> Result<Vec<Column>, iox_catalog::interface::Error> {
        self.catalog
            .repositories()
            .await
            .columns()
            .list_by_table_id(table_id)
            .await
    }
}

#[async_trait]
impl PartitionProvider for CatalogPartitionResolver {
    async fn get_partition(
        &self,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table: Arc<DeferredLoad<TableMetadata>>,
        partition_counter: Arc<PartitionCounter>,
    ) -> Arc<Mutex<PartitionData>> {
        debug!(
            %partition_key,
            %table_id,
            %table,
            "upserting partition in catalog"
        );
        let p = Backoff::new(&self.backoff_config)
            .retry_all_errors("resolve partition", || {
                self.get(partition_key.clone(), table_id)
            })
            .await
            .expect("retry forever");

        let p_sort_key = p.sort_key();
        let p_sort_key_ids = p.sort_key_ids_none_if_empty();

        // fetch columns of the table to build sort_key from sort_key_ids
        let columns = Backoff::new(&self.backoff_config)
            .retry_all_errors("resolve partition's table columns", || {
                self.get_columns(table_id)
            })
            .await
            .expect("retry forever");

        // build sort_key from sort_key_ids and columns
        let sort_key =
            build_sort_key_from_sort_key_ids_and_columns(&p_sort_key_ids, columns.into_iter());

        // This is here to catch bugs and will be removed once the sort_key is removed from the partition
        assert_eq!(sort_key, p_sort_key);

        Arc::new(Mutex::new(PartitionData::new(
            p.transition_partition_id(),
            // Use the caller's partition key instance, as it MAY be shared with
            // other instance, but the instance returned from the catalog
            // definitely has no other refs.
            partition_key,
            namespace_id,
            namespace_name,
            table_id,
            table,
            SortKeyState::Provided(sort_key, p_sort_key_ids.cloned()),
            partition_counter,
        )))
    }
}

#[cfg(test)]
mod tests {
    // Harmless in tests - saves a bunch of extra vars.
    #![allow(clippy::await_holding_lock)]

    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use iox_catalog::{
        partition_lookup,
        test_helpers::{arbitrary_namespace, arbitrary_table},
    };

    use super::*;
    use crate::buffer_tree::table::metadata::TableName;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "ns-bananas";
    const PARTITION_KEY: &str = "platanos";

    #[tokio::test]
    async fn test_resolver() {
        let metrics = Arc::new(metric::Registry::default());
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        let (namespace_id, table_id) = {
            let mut repos = catalog.repositories().await;
            let ns = arbitrary_namespace(&mut *repos, NAMESPACE_NAME).await;

            let table = arbitrary_table(&mut *repos, TABLE_NAME, &ns).await;

            (ns.id, table.id)
        };

        let callers_partition_key = PartitionKey::from(PARTITION_KEY);
        let table_name = TableName::from(TABLE_NAME);
        let resolver = CatalogPartitionResolver::new(Arc::clone(&catalog));
        let got = resolver
            .get_partition(
                callers_partition_key.clone(),
                namespace_id,
                Arc::new(DeferredLoad::new(
                    Duration::from_secs(1),
                    async { NamespaceName::from(NAMESPACE_NAME) },
                    &metrics,
                )),
                table_id,
                Arc::new(DeferredLoad::new(
                    Duration::from_secs(1),
                    async {
                        TableMetadata::new_for_testing(
                            TableName::from(TABLE_NAME),
                            Default::default(),
                        )
                    },
                    &metrics,
                )),
                Arc::new(PartitionCounter::new(NonZeroUsize::new(1).unwrap())),
            )
            .await;

        // Ensure the table name is available.
        let _ = got.lock().table().get().await.name();

        assert_eq!(got.lock().namespace_id(), namespace_id);
        assert_eq!(
            got.lock().table().get().await.name().to_string(),
            table_name.to_string()
        );
        assert_matches!(got.lock().sort_key(), SortKeyState::Provided(None, None));
        assert!(got.lock().partition_key.ptr_eq(&callers_partition_key));

        let mut repos = catalog.repositories().await;
        let id = got.lock().partition_id.clone();
        let got = partition_lookup(repos.as_mut(), &id)
            .await
            .unwrap()
            .expect("partition not created");
        assert_eq!(got.table_id, table_id);
        assert_eq!(got.partition_key, PartitionKey::from(PARTITION_KEY));
    }
}
