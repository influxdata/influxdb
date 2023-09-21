use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::PartitionId;
use schema::sort::SortKey;

use crate::{
    components::{
        columns_source::ColumnsSource, namespaces_source::NamespacesSource,
        partition_source::PartitionSource, tables_source::TablesSource,
    },
    error::DynError,
    partition_info::PartitionInfo,
};

use super::PartitionInfoSource;

#[derive(Debug)]
pub struct SubSourcePartitionInfoSource<C, P, T, N>
where
    C: ColumnsSource,
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    columns_source: C,
    partition_source: P,
    tables_source: T,
    namespaces_source: N,
}

impl<C, P, T, N> SubSourcePartitionInfoSource<C, P, T, N>
where
    C: ColumnsSource,
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    pub fn new(
        columns_source: C,
        partition_source: P,
        tables_source: T,
        namespaces_source: N,
    ) -> Self {
        Self {
            columns_source,
            partition_source,
            tables_source,
            namespaces_source,
        }
    }
}

impl<C, P, T, N> Display for SubSourcePartitionInfoSource<C, P, T, N>
where
    C: ColumnsSource,
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "sub_sources(partition={}, tables={}, namespaces={})",
            self.partition_source, self.tables_source, self.namespaces_source
        )
    }
}

#[async_trait]
impl<C, P, T, N> PartitionInfoSource for SubSourcePartitionInfoSource<C, P, T, N>
where
    C: ColumnsSource,
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    async fn fetch(&self, partition_id: PartitionId) -> Result<Arc<PartitionInfo>, DynError> {
        // Get info for the partition
        let partition = self
            .partition_source
            .fetch_by_id(partition_id)
            .await
            .ok_or_else::<DynError, _>(|| String::from("Cannot find partition info").into())?;

        let table = self
            .tables_source
            .fetch(partition.table_id)
            .await
            .ok_or_else::<DynError, _>(|| String::from("Cannot find table").into())?;

        // TODO: after we have catalog function to read table schema, we should use it
        // and avoid reading namespace schema
        let namespace = self
            .namespaces_source
            .fetch_by_id(table.namespace_id)
            .await
            .ok_or_else::<DynError, _>(|| String::from("Cannot find namespace").into())?;

        let namespace_schema = self
            .namespaces_source
            .fetch_schema_by_id(table.namespace_id)
            .await
            .ok_or_else::<DynError, _>(|| String::from("Cannot find namespace schema").into())?;

        let table_schema = namespace_schema
            .tables
            .get(&table.name)
            .ok_or_else::<DynError, _>(|| String::from("Cannot find table schema").into())?;

        // fetch table columns to get column names for the partition's sort_key_ids
        let columns = self.columns_source.fetch(table.id).await;

        // sort_key_ids of the partition
        let sort_key_ids = partition.sort_key_ids_none_if_empty();
        // sort_key of the partition. This will be removed but until then, use it to validate the
        // sort_key computed by mapping sort_key_ids to column names
        let p_sort_key = partition.sort_key();

        // convert column ids to column names
        let sort_key = sort_key_ids.as_ref().map(|ids| {
            let names = ids
                .iter()
                .map(|id| {
                    columns
                        .iter()
                        .find(|c| c.id == *id)
                        .map(|c| c.name.clone())
                        .ok_or_else::<DynError, _>(|| {
                            format!(
                                "Cannot find column with id {} for table {}",
                                id.get(),
                                table.name
                            )
                            .into()
                        })
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("Cannot find column names for sort key ids");

            SortKey::from_columns(names.iter().map(|s| &**s))
        });

        // This is here to catch bugs if any while mapping sort_key_ids to column names
        // This wil be removed once sort_key is removed from partition
        assert_eq!(sort_key, p_sort_key);

        Ok(Arc::new(PartitionInfo {
            partition_id,
            partition_hash_id: partition.hash_id().cloned(),
            namespace_id: table.namespace_id,
            namespace_name: namespace.name,
            table: Arc::new(table),
            table_schema: Arc::new(table_schema.clone()),
            sort_key,
            partition_key: partition.partition_key,
        }))
    }
}
