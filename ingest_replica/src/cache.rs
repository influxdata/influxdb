//! A cache of table schemas and partition sort keys for us with the buffer to answer Flight
//! requests.

use data_types::{NamespaceId, PartitionId, PartitionKey, ShardId, TableId, TableSchema};
use iox_catalog::interface::{
    get_table_schema_by_id, list_schemas, Catalog, Error as CatalogError,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, ops::DerefMut, sync::Arc};
use thiserror::Error;

/// Errors that occur during the use of the cache.
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("namespace {id:?} not found")]
    NamespaceNotFound { id: NamespaceId },

    #[error("table {id:?} not found")]
    TableNotFound { id: TableId },

    #[error("partition for table {table_id:?} and partition key {partition_key:?} not found")]
    PartitionNotFound {
        table_id: TableId,
        partition_key: PartitionKey,
    },

    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

#[derive(Debug)]
pub(crate) struct SchemaCache {
    state: RwLock<State>,
    catalog: Arc<dyn Catalog>,
    transition_shard_id: ShardId,
}

#[derive(Debug, Default)]
struct State {
    partition_ids: BTreeMap<(TableId, PartitionKey), PartitionId>,
    table_schemas: BTreeMap<TableId, Arc<TableSchema>>,
}

const RECENT_PARTITION_COUNT_TO_WARM: usize = 40000;

impl SchemaCache {
    pub async fn warm(&self) -> Result<(), CacheError> {
        let namespaces = list_schemas(&*self.catalog).await?.collect::<Vec<_>>();
        let partitions = self
            .catalog
            .repositories()
            .await
            .partitions()
            .most_recent_n(RECENT_PARTITION_COUNT_TO_WARM)
            .await?;

        let mut state = self.state.write();

        for (_namespace, schema) in namespaces {
            for (_table_name, table_schema) in schema.tables {
                state
                    .table_schemas
                    .insert(table_schema.id, Arc::new(table_schema));
            }
        }

        for partition in partitions {
            state
                .partition_ids
                .insert((partition.table_id, partition.partition_key), partition.id);
        }

        Ok(())
    }

    pub fn new(catalog: Arc<dyn Catalog>, transition_shard_id: ShardId) -> Self {
        Self {
            catalog,
            state: Default::default(),
            transition_shard_id,
        }
    }

    pub async fn get_table_schema(
        &self,
        table_id: TableId,
    ) -> Result<Arc<TableSchema>, CacheError> {
        match self.get_table_schema_from_cache(&table_id) {
            Some(t) => Ok(t),
            None => {
                let table_schema = {
                    let mut repos = self.catalog.repositories().await;
                    get_table_schema_by_id(table_id, repos.deref_mut()).await?
                };
                let table_schema = Arc::new(table_schema);
                let mut s = self.state.write();
                s.table_schemas.insert(table_id, Arc::clone(&table_schema));

                Ok(table_schema)
            }
        }
    }

    fn get_table_schema_from_cache(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        let s = self.state.read();
        s.table_schemas.get(table_id).cloned()
    }

    pub async fn get_table_schema_from_catalog(
        &self,
        table_id: TableId,
    ) -> Result<Arc<TableSchema>, CacheError> {
        let table_schema = {
            let mut repos = self.catalog.repositories().await;
            get_table_schema_by_id(table_id, repos.deref_mut()).await?
        };

        let table_schema = Arc::new(table_schema);
        let mut s = self.state.write();
        s.table_schemas.insert(table_id, Arc::clone(&table_schema));

        Ok(table_schema)
    }

    pub async fn get_partition_id(
        &self,
        table_id: TableId,
        partition_key: PartitionKey,
    ) -> Result<PartitionId, CacheError> {
        let id = match self.get_partition_id_from_cache(table_id, partition_key.clone()) {
            Some(k) => k,
            None => {
                let partition = self
                    .catalog
                    .repositories()
                    .await
                    .partitions()
                    .create_or_get(partition_key.clone(), self.transition_shard_id, table_id)
                    .await?;
                let mut s = self.state.write();
                s.partition_ids
                    .insert((table_id, partition_key), partition.id);
                partition.id
            }
        };

        Ok(id)
    }

    fn get_partition_id_from_cache(
        &self,
        table_id: TableId,
        partition_key: PartitionKey,
    ) -> Option<PartitionId> {
        let s = self.state.read();
        s.partition_ids.get(&(table_id, partition_key)).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{ColumnType, Namespace, Partition, Table};
    use iox_catalog::create_or_get_default_records;
    use iox_catalog::mem::MemCatalog;
    use metric::Registry;

    const NAMESPACE_NAME: &str = "foo";
    const TABLE_NAME: &str = "bar";
    const COLUMN_NAME: &str = "time";
    const PARTITION_KEY: &str = "2023-01-08";

    #[tokio::test]
    async fn warms_cache() {
        let (catalog, shard_id, _namespace, table, partition) = get_test_data().await;

        let cache = SchemaCache::new(catalog, shard_id);
        assert!(cache.get_table_schema_from_cache(&table.id).is_none());
        assert!(cache
            .get_partition_id_from_cache(table.id, partition.partition_key.clone())
            .is_none());

        cache.warm().await.unwrap();
        assert_eq!(
            cache.get_table_schema_from_cache(&table.id).unwrap().id,
            table.id
        );
        assert_eq!(
            cache
                .get_partition_id_from_cache(table.id, partition.partition_key)
                .unwrap(),
            partition.id
        );
    }

    #[tokio::test]
    async fn gets_table_schema_and_partition_id_from_catalog_if_not_in_cache() {
        let (catalog, shard_id, _namespace, table, partition) = get_test_data().await;

        let cache = SchemaCache::new(catalog, shard_id);
        assert!(cache.get_table_schema_from_cache(&table.id).is_none());
        assert!(cache
            .get_partition_id_from_cache(table.id, partition.partition_key.clone())
            .is_none());

        assert_eq!(cache.get_table_schema(table.id).await.unwrap().id, table.id);
        assert_eq!(
            cache
                .get_partition_id(table.id, partition.partition_key)
                .await
                .unwrap(),
            partition.id
        );
    }

    async fn get_test_data() -> (Arc<dyn Catalog>, ShardId, Namespace, Table, Partition) {
        let catalog = MemCatalog::new(Arc::new(Registry::new()));

        let mut txn = catalog.start_transaction().await.unwrap();
        let (topic, query_pool, shards) = create_or_get_default_records(1, txn.deref_mut())
            .await
            .unwrap();

        let shard_id = *shards.keys().next().unwrap();
        let namespace = txn
            .namespaces()
            .create(NAMESPACE_NAME, None, topic.id, query_pool.id)
            .await
            .unwrap();
        let table = txn
            .tables()
            .create_or_get(TABLE_NAME, namespace.id)
            .await
            .unwrap();
        let _ = txn
            .columns()
            .create_or_get(COLUMN_NAME, table.id, ColumnType::Time)
            .await
            .unwrap();
        let partition = txn
            .partitions()
            .create_or_get(PARTITION_KEY.into(), shard_id, table.id)
            .await
            .unwrap();

        txn.commit().await.unwrap();

        (Arc::new(catalog), shard_id, namespace, table, partition)
    }
}
