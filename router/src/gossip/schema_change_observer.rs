//! A schema change observer that gossips schema diffs to other peers.

use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{ColumnsByName, NamespaceName, NamespaceSchema};
use generated_types::influxdata::iox::gossip::v1::{
    schema_message::Event, Column, NamespaceCreated, TableCreated, TableUpdated,
};

use crate::namespace_cache::{ChangeStats, NamespaceCache};

use super::traits::SchemaBroadcast;

/// A [`NamespaceCache`] decorator implementing cluster-wide, best-effort
/// propagation of local schema changes via the gossip subsystem.
///
/// This type sits in the call chain of schema cache operations and gossips any
/// observed diffs applied to the local cache.
///
/// # Broadcasting Local Changes
///
/// Updated [`NamespaceSchema`] passed through the
/// [`NamespaceCache::put_schema()`] method are broadcast to gossip peers.
///
/// Instead of gossiping the entire schema, the new schema elements described by
/// the [`ChangeStats`] are transmitted on a best-effort basis.
///
/// Gossip [`Event`] are populated within the call to
/// [`NamespaceCache::put_schema()`] but packed & serialised into gossip frames
/// off-path in a background task to minimise the latency overhead.
///
/// Processing of the diffs happens partially on the cache request path, and
/// partially off of it - the actual message content is generated on the cache
/// request path, but framing, serialisation and dispatch happen asynchronously.
#[derive(Debug)]
pub struct SchemaChangeObserver<T, U> {
    tx: U,
    inner: T,
}

/// A pass-through [`NamespaceCache`] implementation that gossips new schema
/// additions.
#[async_trait]
impl<T, U> NamespaceCache for SchemaChangeObserver<T, U>
where
    T: NamespaceCache,
    U: SchemaBroadcast,
{
    type ReadError = T::ReadError;

    /// Pass through get operations.
    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        self.inner.get_schema(namespace).await
    }

    /// Pass through put requests, gossiping the content of the returned
    /// [`ChangeStats`], if any.
    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        let (schema, diff) = self.inner.put_schema(namespace.clone(), schema);

        // Gossip the cache content diff.
        //
        // This causes cache content deltas to be gossiped around the cluster.
        self.observe_diff(&namespace, &schema, &diff);

        (schema, diff)
    }
}

impl<T, U> SchemaChangeObserver<T, U>
where
    U: SchemaBroadcast,
{
    /// Construct a new [`SchemaChangeObserver`] that publishes gossip messages
    /// over `gossip`, and delegates cache operations to `inner`.
    pub fn new(inner: T, gossip: U) -> Self {
        Self { tx: gossip, inner }
    }

    /// Derive a set of gossip event messages from `c`, and dispatch them to
    /// cluster peers.
    fn observe_diff(
        &self,
        name: &NamespaceName<'static>,
        schema: &NamespaceSchema,
        c: &ChangeStats,
    ) {
        // Is this namespace new?
        if !c.did_update {
            self.handle_new_namespace(name, schema);
        }

        // Dispatch "new table" messages
        if !c.new_tables.is_empty() {
            self.handle_new_tables(name, &c.new_tables);
        }

        // Dispatch any "new column" messages for existing tables
        if !c.new_columns_per_table.is_empty() {
            self.handle_table_update(name, schema, &c.new_columns_per_table);
        }
    }

    /// Gossip that a new namespace named `namespace_name` exists and is
    /// associated with `schema`.
    fn handle_new_namespace(
        &self,
        namespace_name: &NamespaceName<'static>,
        schema: &NamespaceSchema,
    ) {
        let msg = NamespaceCreated {
            namespace_name: namespace_name.to_string(),
            namespace_id: schema.id.get(),
            partition_template: schema.partition_template.as_proto().cloned(),
            max_columns_per_table: schema.max_columns_per_table as u64,
            max_tables: schema.max_tables as u64,
            retention_period_ns: schema.retention_period_ns,
        };

        self.tx.broadcast(Event::NamespaceCreated(msg));
    }

    /// Gossip that the provided set of `new_tables` have been added to the
    /// `namespace_name` namespace.
    fn handle_new_tables(
        &self,
        namespace_name: &NamespaceName<'static>,
        new_tables: &BTreeMap<String, data_types::TableSchema>,
    ) {
        for (table_name, schema) in new_tables {
            let msg = TableCreated {
                table: Some(TableUpdated {
                    table_name: table_name.to_owned(),
                    namespace_name: namespace_name.to_string(),
                    table_id: schema.id.get(),
                    columns: schema
                        .columns
                        .iter()
                        .map(|(col_name, col_schema)| Column {
                            column_id: col_schema.id.get(),
                            name: col_name.to_owned(),
                            column_type: col_schema.column_type as i32,
                        })
                        .collect(),
                }),
                partition_template: schema.partition_template.as_proto().cloned(),
            };

            self.tx.broadcast(Event::TableCreated(msg));
        }
    }

    /// Gossip that the provided set of `new_columns_per_table` (keyed by table
    /// name) have been added to the `namespace_name` namespace.
    fn handle_table_update(
        &self,
        namespace_name: &NamespaceName<'static>,
        schema: &NamespaceSchema,
        new_columns_per_table: &BTreeMap<String, ColumnsByName>,
    ) {
        for (table_name, cols) in new_columns_per_table {
            let table_schema = schema
                .tables
                .get(table_name)
                .expect("diff table must exist");

            let msg = TableUpdated {
                table_name: table_name.to_owned(),
                namespace_name: namespace_name.to_string(),
                table_id: table_schema.id.get(),
                columns: cols
                    .iter()
                    .map(|(col_name, col_schema)| {
                        debug_assert_eq!(
                            table_schema.columns.get(col_name).unwrap().id,
                            col_schema.id
                        );
                        Column {
                            column_id: col_schema.id.get(),
                            name: col_name.to_owned(),
                            column_type: col_schema.column_type as i32,
                        }
                    })
                    .collect(),
            };

            self.tx.broadcast(Event::TableUpdated(msg));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        gossip::mock_schema_broadcast::MockSchemaBroadcast, namespace_cache::MemoryNamespaceCache,
    };

    use super::*;

    use assert_matches::assert_matches;
    use data_types::{
        partition_template::{test_table_partition_override, NamespacePartitionTemplateOverride},
        ColumnId, NamespaceId, TableId, TableSchema,
    };
    use generated_types::influxdata::iox::gossip::v1::column::ColumnType;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const TABLE_ID: i64 = 42;

    const DEFAULT_NAMESPACE_PARTITION_TEMPLATE: NamespacePartitionTemplateOverride =
        NamespacePartitionTemplateOverride::const_default();
    const DEFAULT_NAMESPACE: NamespaceSchema = NamespaceSchema {
        id: NamespaceId::new(4242),
        tables: BTreeMap::new(),
        max_columns_per_table: 1,
        max_tables: 2,
        retention_period_ns: None,
        partition_template: DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
    };

    macro_rules! test_observe {
        (
            $name:ident,
            diff = $diff:expr,             // Diff to be evaluated
            schema = $schema:expr,         // Schema to be evaluated
            want_count = $want_count:expr, // Wait for this many messages to be broadcast
            want = $($want:tt)+            // Pattern match over slice of messages
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_observe_ $name>]() {
                    let gossip = Arc::new(MockSchemaBroadcast::default());
                    let observer = SchemaChangeObserver::new(
                        MemoryNamespaceCache::default(),
                        Arc::clone(&gossip)
                    );

                    // Evaluate the given diff
                    observer.observe_diff(
                        &NAMESPACE_NAME.try_into().unwrap(),
                        &$schema,
                        &$diff,
                    );

                    gossip.wait_for_messages($want_count).await;

                    // And inspect the messages sent.
                    let msg = gossip.messages();
                    assert_matches::assert_matches!(msg.as_slice(), $($want)+);
                }
            }
        }
    }

    // A new, empty namespace is observed. A new (empty) schema was created.
    test_observe!(
        new_namespace,
        diff = ChangeStats {
            new_tables: Default::default(),
            new_columns_per_table: Default::default(),
            num_new_columns: Default::default(),
            did_update: false,
        },
        schema = DEFAULT_NAMESPACE,
        want_count = 1,
        want = [Event::NamespaceCreated(NamespaceCreated {
            namespace_name,
            namespace_id,
            partition_template,
            max_columns_per_table,
            max_tables,
            retention_period_ns
        })] => {
            assert_eq!(namespace_name, NAMESPACE_NAME);
            assert_eq!(*namespace_id, DEFAULT_NAMESPACE.id.get());
            assert_eq!(*partition_template, DEFAULT_NAMESPACE.partition_template.as_proto().cloned());
            assert_eq!(*max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table as u64);
            assert_eq!(*max_tables, DEFAULT_NAMESPACE.max_tables as u64);
            assert_eq!(*retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
        }
    );

    // A new namespace that was immediately populated with a table containing a
    // column.
    test_observe!(
        new_namespace_new_table,
        diff = ChangeStats {
            new_tables: new_map(&[
                (TABLE_NAME, TableSchema {
                    id: TableId::new(TABLE_ID),
                    columns: ColumnsByName::new([
                        data_types::Column {
                            name: "platanos".to_string(),
                            column_type: data_types::ColumnType::String,
                            id: ColumnId::new(2442),
                            table_id: TableId::new(TABLE_ID)
                        },
                    ]),
                    partition_template: test_table_partition_override(vec![
                        data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                    ]),
                })
            ]),
            new_columns_per_table: Default::default(),
            num_new_columns: Default::default(),
            did_update: false,
        },
        schema = DEFAULT_NAMESPACE,
        want_count = 1,
        want = [Event::NamespaceCreated(NamespaceCreated {
            namespace_name,
            namespace_id,
            partition_template,
            max_columns_per_table,
            max_tables,
            retention_period_ns
        }),
        Event::TableCreated(TableCreated { table, partition_template: table_template })] => {
            // Validate the namespace create message

            assert_eq!(namespace_name, NAMESPACE_NAME);
            assert_eq!(*namespace_id, DEFAULT_NAMESPACE.id.get());
            assert_eq!(*partition_template, DEFAULT_NAMESPACE.partition_template.as_proto().cloned());
            assert_eq!(*max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table as u64);
            assert_eq!(*max_tables, DEFAULT_NAMESPACE.max_tables as u64);
            assert_eq!(*retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);

            // Validate the table message

            let meta = table.as_ref().expect("must have metadata");

            assert_eq!(meta.table_name, TABLE_NAME);
            assert_eq!(meta.namespace_name, NAMESPACE_NAME);
            assert_eq!(meta.table_id, TABLE_ID);
            assert_matches!(meta.columns.as_slice(), [c] => {
                assert_eq!(c.name, "platanos");
                assert_eq!(c.column_id, 2442);
                assert_eq!(c.column_type(), ColumnType::String);
            });

            let want = test_table_partition_override(vec![
                data_types::partition_template::TemplatePart::TagValue("bananatastic"),
            ]);
            assert_eq!(table_template.as_ref(), want.as_proto());
        }
    );

    // A new table for an existing namespace.
    test_observe!(
        existing_namespace_new_table,
        diff = ChangeStats {
            new_tables: new_map(&[
                (TABLE_NAME, TableSchema {
                    id: TableId::new(TABLE_ID),
                    columns: ColumnsByName::new([
                        data_types::Column {
                            name: "platanos".to_string(),
                            column_type: data_types::ColumnType::String,
                            id: ColumnId::new(2442),
                            table_id: TableId::new(TABLE_ID)
                        },
                    ]),
                    partition_template: test_table_partition_override(vec![
                        data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                    ]),
                })
            ]),
            new_columns_per_table: Default::default(),
            num_new_columns: Default::default(),
            did_update: true,
        },
        schema = DEFAULT_NAMESPACE,
        want_count = 1,
        want = [Event::TableCreated(TableCreated { table, partition_template: table_template })] => {
            let meta = table.as_ref().expect("must have metadata");

            assert_eq!(meta.table_name, TABLE_NAME);
            assert_eq!(meta.namespace_name, NAMESPACE_NAME);
            assert_eq!(meta.table_id, TABLE_ID);
            assert_matches!(meta.columns.as_slice(), [c] => {
                assert_eq!(c.name, "platanos");
                assert_eq!(c.column_id, 2442);
                assert_eq!(c.column_type(), ColumnType::String);
            });

            let want = test_table_partition_override(vec![
                data_types::partition_template::TemplatePart::TagValue("bananatastic"),
            ]);
            assert_eq!(table_template.as_ref(), want.as_proto());
        }
    );

    // A new table for an existing namespace, and an update to an existing
    // table.
    test_observe!(
        existing_namespace_existing_table,
        diff = ChangeStats {
            new_tables: new_map(&[
                (TABLE_NAME, TableSchema {
                    id: TableId::new(TABLE_ID),
                    columns: ColumnsByName::new([
                        data_types::Column {
                            name: "platanos".to_string(),
                            column_type: data_types::ColumnType::String,
                            id: ColumnId::new(2442),
                            table_id: TableId::new(TABLE_ID)
                        },
                    ]),
                    partition_template: test_table_partition_override(vec![
                        data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                    ]),
                })
            ]),
            new_columns_per_table: new_map(&[
                ("more-bananas", ColumnsByName::new([
                    data_types::Column {
                        name: "platanos".to_string(),
                        column_type: data_types::ColumnType::Tag,
                        id: ColumnId::new(1234),
                        table_id: TableId::new(4321)
                    },
                ]))
            ]),
            num_new_columns: Default::default(),
            did_update: true,
        },
        schema = {
            // insert the existing table & column that was added in this diff
            let mut ns = DEFAULT_NAMESPACE.clone();
            ns.tables.insert("more-bananas".to_string(), TableSchema {
                id: TableId::new(4321),
                partition_template:  test_table_partition_override(vec![]),
                columns: ColumnsByName::new([
                    data_types::Column {
                        name: "platanos".to_string(),
                        column_type: data_types::ColumnType::Tag,
                        id: ColumnId::new(1234),
                        table_id: TableId::new(4321)
                    },
                ]),
            });
            ns
        },
        want_count = 1,
        want = [
                Event::TableCreated(created),
                Event::TableUpdated(updated),
            ] => {
            // Table create validation
            let meta = created.table.as_ref().expect("must have metadata");

            assert_eq!(meta.table_name, TABLE_NAME);
            assert_eq!(meta.namespace_name, NAMESPACE_NAME);
            assert_eq!(meta.table_id, TABLE_ID);
            assert_matches!(meta.columns.as_slice(), [c] => {
                assert_eq!(c.name, "platanos");
                assert_eq!(c.column_id, 2442);
                assert_eq!(c.column_type(), ColumnType::String);
            });

            let want = test_table_partition_override(vec![
                data_types::partition_template::TemplatePart::TagValue("bananatastic"),
            ]);
            assert_eq!(created.partition_template.as_ref(), want.as_proto());

            // Table update validation
            assert_eq!(updated.table_name, "more-bananas");
            assert_eq!(updated.namespace_name, NAMESPACE_NAME);
            assert_eq!(updated.table_id, 4321);
            assert_matches!(updated.columns.as_slice(), [c] => {
                assert_eq!(c.name, "platanos");
                assert_eq!(c.column_id, 1234);
                assert_eq!(c.column_type(), ColumnType::Tag);
            });
        }
    );

    fn new_map<T>(v: &[(&str, T)]) -> BTreeMap<String, T>
    where
        T: Clone,
    {
        let mut out = BTreeMap::new();
        for (name, table) in v {
            out.insert(name.to_string(), table.clone());
        }
        out
    }
}
