use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::PartitionId;

use crate::{
    components::{
        namespaces_source::NamespacesSource, partition_source::PartitionSource,
        tables_source::TablesSource,
    },
    error::DynError,
    partition_info::PartitionInfo,
};

use super::PartitionInfoSource;

#[derive(Debug)]
pub struct SubSourcePartitionInfoSource<P, T, N>
where
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    partition_source: P,
    tables_source: T,
    namespaces_source: N,
}

impl<P, T, N> SubSourcePartitionInfoSource<P, T, N>
where
    P: PartitionSource,
    T: TablesSource,
    N: NamespacesSource,
{
    pub fn new(partition_source: P, tables_source: T, namespaces_source: N) -> Self {
        Self {
            partition_source,
            tables_source,
            namespaces_source,
        }
    }
}

impl<P, T, N> Display for SubSourcePartitionInfoSource<P, T, N>
where
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
impl<P, T, N> PartitionInfoSource for SubSourcePartitionInfoSource<P, T, N>
where
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

        Ok(Arc::new(PartitionInfo {
            partition_id,
            partition_hash_id: partition.hash_id().cloned(),
            namespace_id: table.namespace_id,
            namespace_name: namespace.name,
            table: Arc::new(table),
            table_schema: Arc::new(table_schema.clone()),
            sort_key: partition.sort_key(),
            partition_key: partition.partition_key,
        }))
    }
}
