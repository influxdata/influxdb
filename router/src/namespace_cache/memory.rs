use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use hashbrown::HashMap;
use parking_lot::RwLock;
use thiserror::Error;

use super::{ChangeStats, NamespaceCache};

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
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        let old = self
            .cache
            .read()
            .get(&namespace)
            // The existing Arc is cloned to allow the merge to be performed without holding
            // the read-lock on the cache
            .map(Arc::clone);

        let (merged_schema, change_stats) = match old {
            Some(old) => merge_schema_additive(schema, old),
            None => {
                let change_stats = ChangeStats {
                    new_columns: schema
                        .tables
                        .values()
                        .map(|v| v.columns.len())
                        .sum::<usize>(),
                    new_tables: schema.tables.len(),
                    did_update: false,
                };
                (schema, change_stats)
            }
        };

        let ret = Arc::new(merged_schema);
        self.cache.write().insert(namespace, Arc::clone(&ret));
        (ret, change_stats)
    }
}

/// Merges into `new_ns` any table or column schema which are
/// present in `old_ns` but missing in `new_ns`. The newer namespace schema is
/// prioritised in the case of any conflicting schema definitions.
fn merge_schema_additive(
    mut new_ns: NamespaceSchema,
    old_ns: Arc<NamespaceSchema>,
) -> (NamespaceSchema, ChangeStats) {
    // invariant: Namespace ID should never change for a given name
    assert_eq!(old_ns.id, new_ns.id);

    let old_table_count = old_ns.tables.len();
    let mut old_column_count = 0;
    // Table schema missing from the new schema are added from the old. If the
    // table exists in both the new and the old namespace schema then any column
    // schema missing from the new table schema are added from the old.
    //
    // This code performs get_mut() & insert() operations to populate `new_ns`,
    // instead of using the BTreeMap's entry() API. This allows this loop to
    // avoid allocating/cloning the table / column name string to give an owned
    // string to the entry() call for every table/column, where the vast
    // majority will likely be already present in the map, wasting the
    // allocation. Instead this block prefers to perform the additional lookup
    // for the insert() call, knowing these cases will be far fewer, amortising
    // to 0 as the schemas become fully populated, leaving the common path free
    // of overhead.
    for (old_table_name, old_table) in &old_ns.tables {
        old_column_count += old_table.columns.len();
        match new_ns.tables.get_mut(old_table_name) {
            Some(new_table) => {
                for (column_name, column) in &old_table.columns {
                    if !new_table.columns.contains_key(column_name) {
                        new_table.columns.insert(column_name.to_owned(), *column);
                    }
                }
            }
            None => {
                new_ns
                    .tables
                    .insert(old_table_name.to_owned(), old_table.to_owned());
            }
        }
    }

    // To compute the change stats for the merge it is still necessary to iterate
    // over the tables present in the new schema. The new schema may have
    // introduced additional tables that won't be visited by the merge logic's logic.
    let change_stats = ChangeStats {
        new_tables: new_ns.tables.len() - old_table_count,
        new_columns: new_ns
            .tables
            .values()
            .map(|v| v.columns.len())
            .sum::<usize>()
            - old_column_count,
        did_update: true,
    };
    (new_ns, change_stats)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert_matches::assert_matches;
    use data_types::{
        Column, ColumnId, ColumnType, NamespaceId, QueryPoolId, TableId, TableSchema, TopicId,
    };

    use super::*;

    const TEST_NAMESPACE_ID: NamespaceId = NamespaceId::new(42);

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
            id: TEST_NAMESPACE_ID,
            topic_id: TopicId::new(24),
            query_pool_id: QueryPoolId::new(1234),
            tables: Default::default(),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: Some(876),
        };
        assert_matches!(cache.put_schema(ns.clone(), schema1.clone()), (new, s) => {
            assert_eq!(*new, schema1);
            assert_eq!(s.new_tables, 0);
        });
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema1
        );

        let schema2 = NamespaceSchema {
            id: TEST_NAMESPACE_ID,
            topic_id: TopicId::new(2),
            query_pool_id: QueryPoolId::new(2),
            tables: Default::default(),
            max_columns_per_table: 10,
            max_tables: 42,
            retention_period_ns: Some(876),
        };

        assert_matches!(cache.put_schema(ns.clone(), schema2.clone()), (new, s) => {
            assert_eq!(*new, schema2);
            assert_eq!(s.new_tables, 0);
        });
        assert_eq!(
            *cache.get_schema(&ns).await.expect("lookup failure"),
            schema2
        );
    }

    #[tokio::test]
    async fn test_put_additive_merge_columns() {
        let ns = NamespaceName::new("arán").expect("namespace name is valid");
        let table_name = "arán";
        let table_id = TableId::new(1);

        // Create two distinct namespace schema to put in the cache to simulate
        // a pair of writes with different column additions.
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

        let mut first_write_table_schema = TableSchema::new(table_id);
        first_write_table_schema.add_column(&column_1);
        let mut second_write_table_schema = TableSchema::new(table_id);
        second_write_table_schema.add_column(&column_2);

        assert_ne!(first_write_table_schema, second_write_table_schema); // These MUST always be different

        let schema_update_1 = NamespaceSchema {
            id: NamespaceId::new(42),
            topic_id: TopicId::new(76),
            query_pool_id: QueryPoolId::new(64),
            tables: BTreeMap::from([(String::from(table_name), first_write_table_schema)]),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: None,
        };
        let schema_update_2 = NamespaceSchema {
            tables: BTreeMap::from([(String::from(table_name), second_write_table_schema)]),
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

        assert_matches!(cache.put_schema(ns.clone(), schema_update_1.clone()), (new_schema, new_stats) => {
            assert_eq!(*new_schema, schema_update_1);
            assert_eq!(new_stats, ChangeStats{ new_tables: 1, new_columns: 1, did_update: false});
        });
        assert_matches!(cache.put_schema(ns.clone(), schema_update_2), (new_schema, new_stats) => {
            assert_eq!(*new_schema, want_namespace_schema);
            assert_eq!(new_stats, ChangeStats{ new_tables: 0, new_columns: 1, did_update: true});
        });

        let got_namespace_schema = cache
            .get_schema(&ns)
            .await
            .expect("a namespace schema should be found");

        assert_eq!(
            *got_namespace_schema, want_namespace_schema,
            "table schema for left hand side should contain columns from both writes",
        );
    }

    #[tokio::test]
    async fn test_put_additive_merge_tables() {
        let ns = NamespaceName::new("arán").expect("namespace name is valid");
        // Create two distinct namespace schema to put in the cache to simulate
        // a pair of writes with different table additions.
        //
        // Each table has been given a column to assert the table merge logic
        // produces the correct metrics.
        let mut table_1 = TableSchema::new(TableId::new(1));
        table_1.add_column(&Column {
            id: ColumnId::new(1),
            table_id: TableId::new(1),
            name: "column_a".to_string(),
            column_type: ColumnType::String,
        });
        let mut table_2 = TableSchema::new(TableId::new(2));
        table_2.add_column(&Column {
            id: ColumnId::new(2),
            table_id: TableId::new(2),
            name: "column_b".to_string(),
            column_type: ColumnType::String,
        });
        let mut table_3 = TableSchema::new(TableId::new(3));
        table_3.add_column(&Column {
            id: ColumnId::new(3),
            table_id: TableId::new(3),
            name: "column_c".to_string(),
            column_type: ColumnType::String,
        });

        let schema_update_1 = NamespaceSchema {
            id: NamespaceId::new(42),
            topic_id: TopicId::new(76),
            query_pool_id: QueryPoolId::new(64),
            tables: BTreeMap::from([
                (String::from("table_1"), table_1.to_owned()),
                (String::from("table_2"), table_2.to_owned()),
            ]),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: None,
        };
        let schema_update_2 = NamespaceSchema {
            tables: BTreeMap::from([
                (String::from("table_1"), table_1.to_owned()),
                (String::from("table_3"), table_3.to_owned()),
            ]),
            ..schema_update_1
        };

        let want_namespace_schema = NamespaceSchema {
            tables: BTreeMap::from([
                (String::from("table_1"), table_1),
                (String::from("table_2"), table_2),
                (String::from("table_3"), table_3),
            ]),
            ..schema_update_1
        };

        // Set up the cache and ensure there are no entries for the namespace.
        let cache = Arc::new(MemoryNamespaceCache::default());
        assert_matches!(
            cache.get_schema(&ns).await,
            Err(CacheMissErr { namespace: got_ns })  => {
                assert_eq!(got_ns, ns);
            }
        );

        assert_matches!(cache.put_schema(ns.clone(), schema_update_1.clone()), (new_schema, new_stats) => {
            assert_eq!(*new_schema, schema_update_1);
            assert_eq!(new_stats, ChangeStats{ new_tables: 2, new_columns: 2, did_update: false});
        });
        assert_matches!(cache.put_schema(ns.clone(), schema_update_2), (new_schema, new_stats) => {
            assert_eq!(*new_schema, want_namespace_schema);
            assert_eq!(new_stats, ChangeStats{ new_tables: 1, new_columns: 1, did_update: true});
        });

        let got_namespace_schema = cache
            .get_schema(&ns)
            .await
            .expect("a namespace schema should be found");

        assert_eq!(
            *got_namespace_schema, want_namespace_schema,
            "table schema for left hand side should contain tables from both writes",
        );
    }
}
