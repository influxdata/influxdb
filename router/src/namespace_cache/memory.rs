use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use super::NamespaceCache;

/// An error type indicating that `namespace` is not present in the cache.
#[derive(Debug, Error)]
#[error("namespace {namespace} not found in cache")]
pub struct CacheMissErr {
    pub(super) namespace: NamespaceName<'static>,
}

/// An in-memory cache of [`NamespaceSchema`] backed by a hashmap protected with
/// a read-write mutex.
#[derive(Debug, Default)]
pub struct MemoryNamespaceCache {
    cache: RwLock<HashMap<NamespaceName<'static>, Arc<NamespaceSchema>>>,
}

#[async_trait]
impl NamespaceCache for Arc<MemoryNamespaceCache> {
    type ReadError = CacheMissErr;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        self.cache
            .read()
            .get(namespace)
            .ok_or(CacheMissErr {
                namespace: namespace.clone(),
            })
            .map(Arc::clone)
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: impl Into<Arc<NamespaceSchema>>,
    ) -> Option<Arc<NamespaceSchema>> {
        let mut guard = self.cache.write();
        let new_ns = schema.into();
        match guard.get(&namespace) {
            Some(old_ns) => {
                // If the previous tenant has a different ID then take the new
                // schema. The old may have been replaced.
                if old_ns.id != new_ns.id {
                    return guard.insert(namespace, new_ns);
                }

                let mut new_ns = (*new_ns).clone();
                for (table_name, new_table) in &mut new_ns.tables {
                    let old_columns = match old_ns.tables.get(table_name) {
                        Some(v) => &v.columns,
                        None => continue,
                    };
                    for (column_name, column) in old_columns {
                        if !new_table.columns.contains_key(column_name) {
                            new_table.columns.insert(column_name.clone(), *column);
                        }
                    }
                }

                guard.insert(namespace, Arc::new(new_ns))
            }
            None => guard.insert(namespace, new_ns),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert_matches::assert_matches;
    use data_types::{
        Column, ColumnId, ColumnType, NamespaceId, QueryPoolId, TableId, TableSchema, TopicId,
    };

    use super::*;

    #[tokio::test]
    async fn test_put_get() {
        let ns = NamespaceName::new("test").expect("namespace name is valid");
        let cache = Arc::new(MemoryNamespaceCache::default());

        assert_matches!(
            cache.get_schema(&ns).await,
            Err(CacheMissErr { namespace: got_ns }) => {
                assert_eq!(got_ns, ns);
            }
        );

        let schema1 = NamespaceSchema {
            id: NamespaceId::new(42),
            topic_id: TopicId::new(24),
            query_pool_id: QueryPoolId::new(1234),
            tables: Default::default(),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: Some(876),
        };
        assert!(cache.put_schema(ns.clone(), schema1.clone()).is_none());
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema1
        );

        let schema2 = NamespaceSchema {
            id: NamespaceId::new(2),
            topic_id: TopicId::new(2),
            query_pool_id: QueryPoolId::new(2),
            tables: Default::default(),
            max_columns_per_table: 10,
            max_tables: 42,
            retention_period_ns: Some(876),
        };

        assert_eq!(
            *cache
                .put_schema(ns.clone(), schema2.clone())
                .expect("should have existing schema"),
            schema1
        );
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema2
        );
    }

    #[tokio::test]
    async fn test_put_racy_additive_merge() {
        let ns = NamespaceName::new("arán").expect("namespace name is valid");
        let table_name = "arán";
        let table_id = TableId::new(1);

        // Create two distinct namespace schema to put in the cache to simulate
        // a pair of racy writes with different column additions.
        let column_1 = Column {
            id: ColumnId::new(1),
            table_id,
            name: String::from("brötchen"),
            column_type: ColumnType::String,
        };
        let column_2 = Column {
            id: ColumnId::new(2),
            table_id,
            name: String::from("pain"),
            column_type: ColumnType::String,
        };

        let mut table_schema_1 = TableSchema::new(table_id);
        table_schema_1.add_column(&column_1);
        let mut table_schema_2 = TableSchema::new(table_id);
        table_schema_2.add_column(&column_2);

        assert_ne!(table_schema_1, table_schema_2); // These MUST always be different

        let schema_update_1 = NamespaceSchema {
            id: NamespaceId::new(42),
            topic_id: TopicId::new(76),
            query_pool_id: QueryPoolId::new(64),
            tables: BTreeMap::from([(String::from(table_name), table_schema_1)]),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: None,
        };
        let schema_update_2 = NamespaceSchema {
            tables: BTreeMap::from([(String::from(table_name), table_schema_2)]),
            ..schema_update_1
        };

        let want_namespace_schema = {
            let mut want_table_schema = TableSchema::new(table_id);
            want_table_schema.add_column(&column_1);
            want_table_schema.add_column(&column_2);
            NamespaceSchema {
                tables: BTreeMap::from([(String::from(table_name), want_table_schema)]),
                ..schema_update_1
            }
        };

        // Set up the cache and ensure there are no entries for the namespace.
        let cache = Arc::new(MemoryNamespaceCache::default());
        assert_matches!(
            cache.get_schema(&ns).await,
            Err(CacheMissErr { namespace: got_ns })  => {
                assert_eq!(got_ns, ns);
            }
        );

        let cache_clone = Arc::clone(&cache);
        let ns_clone = ns.clone();
        tokio::task::spawn(async move {
            cache_clone.put_schema(ns_clone.clone(), schema_update_1);
            cache_clone.put_schema(ns_clone.clone(), schema_update_2);
        })
        .await
        .unwrap();

        let got_namespace_schema = cache
            .get_schema(&ns)
            .await
            .expect("a namespace schema should be found");

        assert_eq!(
            *got_namespace_schema, want_namespace_schema,
            "table schema for left hand side should contain columns from both writes",
        );
    }
}
