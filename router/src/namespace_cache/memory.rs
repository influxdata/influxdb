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
                        .map(|v| v.column_count())
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
    // invariant: Namespace partition template override should never change for a given name
    assert_eq!(old_ns.partition_template, new_ns.partition_template);

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
        old_column_count += old_table.column_count();
        match new_ns.tables.get_mut(old_table_name) {
            Some(new_table) => {
                for (column_name, column) in old_table.columns.iter() {
                    if !new_table.contains_column_name(column_name) {
                        new_table.add_column_schema(column_name.to_string(), *column);
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
            .map(|v| v.column_count())
            .sum::<usize>()
            - old_column_count,
        did_update: true,
    };
    (new_ns, change_stats)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use assert_matches::assert_matches;
    use data_types::{
        Column, ColumnId, ColumnSchema, ColumnType, ColumnsByName, NamespaceId, TableId,
        TableSchema,
    };
    use proptest::{prelude::*, prop_compose, proptest};

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
            tables: Default::default(),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: Some(876),
            partition_template: Default::default(),
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
            tables: Default::default(),
            max_columns_per_table: 10,
            max_tables: 42,
            retention_period_ns: Some(876),
            partition_template: Default::default(),
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

    // In production code, a `TableSchema` should come from a `Table` that came from the catalog,
    // but these tests are independent of the catalog.
    fn empty_table_schema(id: TableId) -> TableSchema {
        TableSchema {
            id,
            partition_template: Default::default(),
            columns: ColumnsByName::new([]),
        }
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

        let mut first_write_table_schema = empty_table_schema(table_id);
        first_write_table_schema.add_column(column_1.clone());
        let mut second_write_table_schema = empty_table_schema(table_id);
        second_write_table_schema.add_column(column_2.clone());

        // These MUST always be different
        assert_ne!(first_write_table_schema, second_write_table_schema);

        let schema_update_1 = NamespaceSchema {
            id: NamespaceId::new(42),
            tables: BTreeMap::from([(String::from(table_name), first_write_table_schema)]),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: None,
            partition_template: Default::default(),
        };
        let schema_update_2 = NamespaceSchema {
            tables: BTreeMap::from([(String::from(table_name), second_write_table_schema)]),
            ..schema_update_1.clone()
        };

        let want_namespace_schema = {
            let mut want_table_schema = empty_table_schema(table_id);
            want_table_schema.add_column(column_1);
            want_table_schema.add_column(column_2);
            NamespaceSchema {
                tables: BTreeMap::from([(String::from(table_name), want_table_schema)]),
                ..schema_update_1.clone()
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

        assert_matches!(
            cache.put_schema(ns.clone(), schema_update_1.clone()),
            (new_schema, new_stats) => {
                assert_eq!(*new_schema, schema_update_1);
                assert_eq!(
                    new_stats,
                    ChangeStats { new_tables: 1, new_columns: 1, did_update: false }
                );
            }
        );
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
        let mut table_1 = empty_table_schema(TableId::new(1));
        table_1.add_column(Column {
            id: ColumnId::new(1),
            table_id: TableId::new(1),
            name: "column_a".to_string(),
            column_type: ColumnType::String,
        });
        let mut table_2 = empty_table_schema(TableId::new(2));
        table_2.add_column(Column {
            id: ColumnId::new(2),
            table_id: TableId::new(2),
            name: "column_b".to_string(),
            column_type: ColumnType::String,
        });
        let mut table_3 = empty_table_schema(TableId::new(3));
        table_3.add_column(Column {
            id: ColumnId::new(3),
            table_id: TableId::new(3),
            name: "column_c".to_string(),
            column_type: ColumnType::String,
        });

        let schema_update_1 = NamespaceSchema {
            id: NamespaceId::new(42),
            tables: BTreeMap::from([
                (String::from("table_1"), table_1.to_owned()),
                (String::from("table_2"), table_2.to_owned()),
            ]),
            max_columns_per_table: 50,
            max_tables: 24,
            retention_period_ns: None,
            partition_template: Default::default(),
        };
        let schema_update_2 = NamespaceSchema {
            tables: BTreeMap::from([
                (String::from("table_1"), table_1.to_owned()),
                (String::from("table_3"), table_3.to_owned()),
            ]),
            ..schema_update_1.clone()
        };

        let want_namespace_schema = NamespaceSchema {
            tables: BTreeMap::from([
                (String::from("table_1"), table_1),
                (String::from("table_2"), table_2),
                (String::from("table_3"), table_3),
            ]),
            ..schema_update_1.clone()
        };

        // Set up the cache and ensure there are no entries for the namespace.
        let cache = Arc::new(MemoryNamespaceCache::default());
        assert_matches!(
            cache.get_schema(&ns).await,
            Err(CacheMissErr { namespace: got_ns })  => {
                assert_eq!(got_ns, ns);
            }
        );

        assert_matches!(
            cache.put_schema(ns.clone(), schema_update_1.clone()),
            (new_schema, new_stats) => {
                assert_eq!(*new_schema, schema_update_1);
                assert_eq!(
                    new_stats,
                    ChangeStats { new_tables: 2, new_columns: 2, did_update: false }
                );
            }
        );
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

    /// A set of table and column names from which arbitrary names are selected
    /// in prop tests, instead of using random values that have a low
    /// probability of overlap.
    const TEST_TABLE_NAME_SET: &[&str] = &["bananas", "quiero", "un", "platano"];
    const TEST_COLUMN_NAME_SET: &[&str] = &["A", "B", "C", "D", "E", "F"];

    prop_compose! {
        fn arbitrary_column_schema()(id in any::<i64>(), disctim in 1_i16..=7) -> ColumnSchema {
            let col_type = ColumnType::try_from(disctim).expect("valid discriminator range");
            ColumnSchema { id: ColumnId::new(id), column_type: col_type }
        }
    }

    prop_compose! {
        /// Generate an arbitrary TableSchema with up to 10 columns.
        fn arbitrary_table_schema()(
            id in any::<i64>(),
            columns in proptest::collection::btree_map(
                proptest::sample::select(TEST_COLUMN_NAME_SET).prop_map(ToString::to_string),
                arbitrary_column_schema(),
                (0, 10) // Set size range
            ),
        ) -> TableSchema {
            let columns = ColumnsByName::from(columns);
            TableSchema {
                id: TableId::new(id),
                partition_template: Default::default(),
                columns,
            }
        }
    }

    prop_compose! {
        fn arbitrary_namespace_schema()(
            tables in proptest::collection::btree_map(
                proptest::sample::select(TEST_TABLE_NAME_SET),
                arbitrary_table_schema(),
                (0, 10) // Set size range
            ),
            max_columns_per_table in any::<usize>(),
            max_tables in any::<usize>(),
            retention_period_ns in any::<Option<i64>>(),
        ) -> NamespaceSchema {
            let tables = tables.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
            NamespaceSchema {
                id: TEST_NAMESPACE_ID,
                tables,
                max_columns_per_table,
                max_tables,
                retention_period_ns,
                partition_template: Default::default(),
            }
        }
    }

    /// Reduce `ns` into a set of `(table_name, column_name)` for all tables &
    /// columns.
    fn into_set(ns: &NamespaceSchema) -> HashSet<(String, String)> {
        ns.tables
            .iter()
            .flat_map(|(table_name, col_set)| {
                // Build a set of tuples in the form (table_name, column_name)
                col_set
                    .columns
                    .names()
                    .into_iter()
                    .map(|col_name| (table_name.to_string(), col_name.to_string()))
            })
            .collect()
    }

    proptest! {
        #[test]
        fn prop_schema_merge(
                a in arbitrary_namespace_schema(),
                b in arbitrary_namespace_schema()
            ) {
            // Convert inputs into sets
            let known_a = into_set(&a);
            let known_b = into_set(&b);

            // Compute the union set of the input schema sets.
            //
            // This is the expected result of the cache merging operation.
            let want = known_a.union(&known_b).map(|v| v.to_owned()).collect::<HashSet<_>>();

            // Merge the schemas using the cache merge logic.
            let name = NamespaceName::try_from("bananas").unwrap();
            let cache = Arc::new(MemoryNamespaceCache::default());
            let (got, stats_1) = cache.put_schema(name.clone(), a.clone());
            assert_eq!(*got, a); // The new namespace should be unchanged

            // Drive the merging logic
            let (got, stats_2) = cache.put_schema(name, b.clone());

            // Reduce the merged schema into a comparable set.
            let got_set = into_set(&got);

            // Assert the table/column sets merged by the known good hashset
            // union implementation, and the cache merging logic are the same.
            assert_eq!(got_set, want);

            // Assert the "last writer wins" in terms of all other namespace
            // values.
            assert_eq!(got.max_columns_per_table, b.max_columns_per_table);
            assert_eq!(got.max_tables, b.max_tables);
            assert_eq!(got.retention_period_ns, b.retention_period_ns);

            // Finally, assert the reported "newly added" statistics sum to the
            // total of the inputs.
            assert_eq!(stats_1.new_columns + stats_2.new_columns, want.len());

            let tables = a.tables.keys().chain(b.tables.keys()).collect::<HashSet<_>>();
            assert_eq!(stats_1.new_tables + stats_2.new_tables, tables.len());
        }
    }
}
