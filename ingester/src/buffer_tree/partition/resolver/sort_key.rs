//! A optimised resolver of a partition [`SortKey`].

use std::sync::Arc;

use backoff::{Backoff, BackoffConfig};
use data_types::{Column, PartitionKey, SortedColumnSet, TableId};
use iox_catalog::interface::Catalog;
use schema::sort::SortKey;

/// A resolver of [`SortKey`] from the catalog for a given [`PartitionKey`]/[`TableId`] pair.
#[derive(Debug)]
pub(crate) struct SortKeyResolver {
    partition_key: PartitionKey,
    table_id: TableId,
    backoff_config: BackoffConfig,
    catalog: Arc<dyn Catalog>,
}

impl SortKeyResolver {
    pub(crate) fn new(
        partition_key: PartitionKey,
        table_id: TableId,
        catalog: Arc<dyn Catalog>,
        backoff_config: BackoffConfig,
    ) -> Self {
        Self {
            partition_key,
            table_id,
            backoff_config,
            catalog,
        }
    }

    /// Fetch the [`SortKey`] and its corresponding sort key ids from the from the [`Catalog`]
    /// for `partition_id`, retrying endlessly when errors occur.
    pub(crate) async fn fetch(self) -> (Option<SortKey>, Option<SortedColumnSet>) {
        Backoff::new(&self.backoff_config)
            .retry_all_errors("fetch partition sort key", || async {
                let mut repos = self.catalog.repositories().await;

                // fetch partition
                let partition = repos
                    .partitions()
                    .create_or_get(self.partition_key.clone(), self.table_id)
                    .await?;

                let (p_sort_key, p_sort_key_ids) =
                    (partition.sort_key(), partition.sort_key_ids_none_if_empty());

                // fetch partition's table columns
                let columns = repos.columns().list_by_table_id(self.table_id).await?;

                // build sort_key from sort_key_ids and columns
                let sort_key = build_sort_key_from_sort_key_ids_and_columns(
                    &p_sort_key_ids,
                    columns.into_iter(),
                );

                // This is here to catch bugs and will be removed once the sort_key is removed from the partition
                assert_eq!(sort_key, p_sort_key);

                Result::<_, iox_catalog::interface::Error>::Ok((sort_key, p_sort_key_ids))
            })
            .await
            .expect("retry forever")
    }
}

// build sort_key from sort_key_ids and columns
// panic if the sort_key_ids are not found in the columns
pub(crate) fn build_sort_key_from_sort_key_ids_and_columns<I>(
    sort_key_ids: &Option<SortedColumnSet>,
    columns: I,
) -> Option<SortKey>
where
    I: Iterator<Item = Column>,
{
    let mut column_names = columns
        .map(|c| (c.id, c.name))
        .collect::<hashbrown::HashMap<_, _>>();
    sort_key_ids.as_ref().map(|ids| {
        SortKey::from_columns(ids.iter().map(|id| {
            column_names
                .remove(id)
                .unwrap_or_else(|| panic!("cannot find column names for sort key id {}", id.get()))
        }))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use data_types::{ColumnId, ColumnType, SortedColumnSet};

    use super::*;
    use crate::test_util::populate_catalog_with_table_columns;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const PARTITION_KEY: &str = "platanos";

    #[tokio::test]
    async fn test_fetch() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the namespace, table & columns
        let (_ns_id, table_id, col_ids) = populate_catalog_with_table_columns(
            &*catalog,
            NAMESPACE_NAME,
            TABLE_NAME,
            &["bananas", "uno", "dos", "tres"],
        )
        .await;

        let partition = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get(PARTITION_KEY.into(), table_id)
            .await
            .expect("should create");

        // Test: sort_key_ids from create_or_get which is empty
        assert!(partition.sort_key_ids().unwrap().is_empty());

        let fetcher = SortKeyResolver::new(
            PARTITION_KEY.into(),
            table_id,
            Arc::clone(&catalog),
            backoff_config.clone(),
        );

        // Set the sort key
        let catalog_state = catalog
            .repositories()
            .await
            .partitions()
            .cas_sort_key(
                &partition.transition_partition_id(),
                None,
                None,
                &["uno", "dos", "bananas"],
                &SortedColumnSet::from(vec![col_ids[1].get(), col_ids[2].get(), col_ids[0].get()]),
            )
            .await
            .expect("should update existing partition key");

        // Test: sort_key_ids from cas_sort_key
        // fetch sort key for the partition from the catalog
        let (fetched_sort_key, fetched_sort_key_ids) = fetcher.fetch().await;
        assert_eq!(fetched_sort_key, catalog_state.sort_key());
        assert_eq!(
            fetched_sort_key_ids,
            catalog_state.sort_key_ids_none_if_empty()
        );
    }

    // panic if the sort_key_ids are not found in the columns
    #[tokio::test]
    #[should_panic(expected = "cannot find column names for sort key id 3")]
    async fn test_panic_build_sort_key_from_sort_key_ids_and_columns() {
        // table columns
        let columns = vec![
            Column {
                name: "uno".into(),
                id: ColumnId::new(1),
                column_type: ColumnType::Tag,
                table_id: TableId::new(1),
            },
            Column {
                name: "dos".into(),
                id: ColumnId::new(2),
                column_type: ColumnType::Tag,
                table_id: TableId::new(1),
            },
        ];

        // sort_key_ids include some columns that are not in the columns
        let sort_key_ids = Some(SortedColumnSet::from([2, 3]));
        let _sort_key =
            build_sort_key_from_sort_key_ids_and_columns(&sort_key_ids, columns.into_iter());
    }

    #[tokio::test]
    async fn test_build_sort_key_from_sort_key_ids_and_columns() {
        // table columns
        let columns = vec![
            Column {
                name: "uno".into(),
                id: ColumnId::new(1),
                column_type: ColumnType::Tag,
                table_id: TableId::new(1),
            },
            Column {
                name: "dos".into(),
                id: ColumnId::new(2),
                column_type: ColumnType::Tag,
                table_id: TableId::new(1),
            },
            Column {
                name: "tres".into(),
                id: ColumnId::new(3),
                column_type: ColumnType::Tag,
                table_id: TableId::new(1),
            },
        ];

        // sort_key_ids is None
        let sort_key_ids = None;
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        assert_eq!(sort_key, None);

        // sort_key_ids is empty
        let sort_key_ids = Some(SortedColumnSet::new(vec![]));
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        let vec: Vec<&str> = vec![];
        assert_eq!(sort_key, Some(SortKey::from_columns(vec)));

        // sort_key_ids include all columns and in the same order
        let sort_key_ids = Some(SortedColumnSet::from([1, 2, 3]));
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        assert_eq!(
            sort_key,
            Some(SortKey::from_columns(vec!["uno", "dos", "tres"]))
        );

        // sort_key_ids include all columns but in different order
        let sort_key_ids = Some(SortedColumnSet::from([2, 3, 1]));
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        assert_eq!(
            sort_key,
            Some(SortKey::from_columns(vec!["dos", "tres", "uno"]))
        );

        // sort_key_ids include some columns
        let sort_key_ids = Some(SortedColumnSet::from([2, 3]));
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        assert_eq!(sort_key, Some(SortKey::from_columns(vec!["dos", "tres"])));

        // sort_key_ids include some columns in different order
        let sort_key_ids = Some(SortedColumnSet::from([3, 1]));
        let sort_key = build_sort_key_from_sort_key_ids_and_columns(
            &sort_key_ids,
            columns.clone().into_iter(),
        );
        assert_eq!(sort_key, Some(SortKey::from_columns(vec!["tres", "uno"])));
    }
}
