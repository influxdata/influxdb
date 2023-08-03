//! [`NamespaceCache`] decorator to gossip schema changes.

use std::{borrow::Cow, collections::BTreeMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    ColumnSchema, ColumnsByName, NamespaceId, NamespaceName, NamespaceNameError, NamespaceSchema,
    TableId, TableSchema,
};
use generated_types::influxdata::iox::gossip::v1::{
    gossip_message::Msg, NamespaceCreated, TableCreated, TableUpdated,
};
use observability_deps::tracing::{debug, error, trace, warn};
use thiserror::Error;

use crate::namespace_cache::{CacheMissErr, NamespaceCache};

use super::dispatcher::GossipMessageHandler;

/// Errors caused by incoming schema gossip messages from cluster peers.
#[derive(Debug, Error)]
enum Error {
    #[error("table create contains no table information")]
    MissingTableUpdate,

    #[error("message contains invalid namespace name: {0}")]
    InvalidNamespaceName(#[from] NamespaceNameError),

    #[error("invalid partition template: {0}")]
    PartitionTemplate(#[from] data_types::partition_template::ValidationError),

    #[error("invalid column schema: {0}")]
    ColumnSchema(Box<dyn std::error::Error>),

    #[error("lookup failed: {0}")]
    Lookup(Box<dyn std::error::Error>),

    /// An update message was received for a table that the local peer is
    /// currently unaware of.
    ///
    /// This indicates an undelivered/reordered preceding "create" message is
    /// yet to be processed.
    #[error("received update for unknown table {0}")]
    TableNotFound(String),
}

/// A [`NamespaceCache`] decorator applying incoming schema change notifications
/// via the [`gossip`] subsystem.
///
/// Any schema additions received from peers are applied to the decorated
/// [`NamespaceCache`], helping to keep the peers approximately in-sync on a
/// best-effort basis.
///
/// # Applying Peer Changes
///
/// Other peers participating in schema gossiping send changes made to their
/// local state - this allows this local node (and all the other peers) to
/// populate their cache before a write request is received by the local node
/// that would cause a cache miss resulting in a catalog query, and the
/// associated latency penalty and catalog load that comes with it.
///
/// This type implements the [`GossipMessageHandler`] which is invoked with the
/// [`Msg`] received from an opaque peer by the [`gossip`] subsystem (off of the
/// hot path), which when processed causes the cache contents to be updated if
/// appropriate through the usual [`NamespaceCache::get_schema()`] and
/// [`NamespaceCache::put_schema()`] abstraction.
///
/// # Peer Trust, Immutability, and Panic
///
/// Certain values are immutable for the lifetime of the associated entity; for
/// example, the data type of a column must never change.
///
/// If a peer gossips an event that contradicts the local state w.r.t an
/// immutable value, the handler will panic. This is designed to bring down the
/// local node and cause it to rebuild the cache state from the source of truth
/// (the catalog) at startup to converge any differences.
///
/// This requires trusted peers within the network this node is operating
/// within. A malicious peer can trivially panic peers by gossiping malicious
/// schema updates. This lays outside the threat model of the gossip system
/// which explicitly trusts all gossip peers by design.
#[derive(Debug)]
pub struct NamespaceSchemaGossip<C> {
    inner: C,
}

/// A handler of incoming gossip events.
///
/// Merges the content of each event with the existing [`NamespaceCache`]
/// contents, if any.
#[async_trait]
impl<C> GossipMessageHandler for Arc<NamespaceSchemaGossip<C>>
where
    C: NamespaceCache<ReadError = CacheMissErr>,
{
    async fn handle(&self, message: Msg) {
        trace!(?message, "received schema message");

        let res = match message {
            Msg::NamespaceCreated(v) => self.handle_namespace_created(v).await,
            Msg::TableCreated(v) => self.handle_table_created(v).await,
            Msg::TableUpdated(v) => self.handle_updated_table(v).await,
        };

        if let Err(error) = res {
            warn!(%error, "failed to apply gossip schema operation");
        }
    }
}

impl<C> NamespaceSchemaGossip<C>
where
    C: NamespaceCache<ReadError = CacheMissErr>,
{
    /// Construct this [`NamespaceSchemaGossip`] decorator, using `inner` for
    /// schema storage.
    pub fn new(inner: C) -> Self {
        Self { inner }
    }

    /// Handle a namespace creation event, inserting it into the
    /// [`NamespaceCache`].
    ///
    /// If the local state contains the gossiped namespace, this is a no-op,
    /// otherwise it is inserted into the [`NamespaceCache`].
    ///
    /// # Panics
    ///
    /// This method panics if the gossiped immutable values do not match the
    /// local state.
    async fn handle_namespace_created(&self, note: NamespaceCreated) -> Result<(), Error> {
        // Check if this namespace exists already
        let namespace_name = NamespaceName::try_from(note.namespace_name)?;

        // Resolve the namespace partition template.
        //
        // If specified, it should be used to construct the
        // NamespacePartitionTemplateOverride, otherwise the default
        // implementation should be used.
        //
        // NOTE: the lack of a field presence confirmation means removal of this
        // field could cause the server to interpret the lack of the field as
        // "use the default" which would be incorrect.
        let partition_template = note
            .partition_template
            .map(NamespacePartitionTemplateOverride::try_from)
            .transpose()?
            .unwrap_or_default();

        // Insert the namespace or do nothing if it exists.
        match self.inner.get_schema(&namespace_name).await {
            Ok(v) => {
                // It does! This is a no-op.

                // Invariant: name -> ID mappings & partition templates MUST be
                // immutable and consistent across the cluster.
                assert_eq!(v.id.get(), note.namespace_id);
                assert_eq!(v.partition_template, partition_template);

                return Ok(());
            }
            Err(CacheMissErr {
                namespace: namespace_name,
            }) => {
                debug!(
                    namespace_id = note.namespace_id,
                    max_columns_per_table = note.max_columns_per_table,
                    max_tables = note.max_tables,
                    retention_period_ns = note.retention_period_ns,
                    ?partition_template,
                    "discovered new namespace via gossip"
                );
                // It does not - initialise the namespace schema and place it
                // into the cache.
                //
                // If another thread has populated the cache since the above
                // check, this becomes a merge operation.
                self.inner.put_schema(
                    namespace_name,
                    NamespaceSchema {
                        id: NamespaceId::new(note.namespace_id),
                        tables: Default::default(),
                        max_columns_per_table: note.max_columns_per_table as _,
                        max_tables: note.max_tables as _,
                        retention_period_ns: note.retention_period_ns,
                        partition_template,
                    },
                );
            }
        };

        Ok(())
    }

    /// Handle a gossip event for a table schema update.
    ///
    /// The local peer MAY or MAY NOT already know about this table and
    /// namespace. If the peer is unaware of either, this is a no-op.
    ///
    /// # Panics
    ///
    /// This method panics if the gossiped immutable values do not match the
    /// local state.
    async fn handle_updated_table(&self, update: TableUpdated) -> Result<(), Error> {
        let table_name = update.table_name.clone();

        // Obtain the existing namespace to modify.
        let namespace_name = NamespaceName::try_from(update.namespace_name.clone())?;
        let ns = self
            .inner
            .get_schema(&namespace_name)
            .await
            .map_err(|v| Error::Lookup(Box::from(v)))?;

        // Load the table from it.
        //
        // If the table does not exist, then it cannot be updated - return
        // early (with an error for logging purposes).
        let table = ns
            .tables
            .get(&update.table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        // Invariant: name -> ID mappings MUST be immutable and consistent
        // across the cluster.
        assert_eq!(table.id.get(), update.table_id);

        if let Some(table) = update_table(table, update)? {
            upsert_cached_namespace(&self.inner, &ns, namespace_name, table, table_name);
        }

        Ok(())
    }

    /// Handle a gossip event for a newly created table.
    ///
    /// The local peer MAY or MAY NOT already know about this table and
    /// namespace.
    ///
    /// If the local peer does not know of this namespace, this is a no-op.
    ///
    /// If the local peer already knows of this table, the contents are merged,
    /// and the immutable fields are verified to be identical.
    ///
    /// # Panics
    ///
    /// This method panics if the immutable table fields differ.
    async fn handle_table_created(&self, v: TableCreated) -> Result<(), Error> {
        // Extract the table update from the message.
        let update = v.table.ok_or(Error::MissingTableUpdate)?;

        let table_id = TableId::new(update.table_id);
        let table_name = update.table_name.clone();

        // Obtain the existing namespace to modify.
        let namespace_name = NamespaceName::try_from(update.namespace_name.clone())?;
        let ns = self
            .inner
            .get_schema(&namespace_name)
            .await
            .map_err(|v| Error::Lookup(Box::from(v)))?;

        let partition_template =
            TablePartitionTemplateOverride::try_new(v.partition_template, &ns.partition_template)?;

        // Attempt to apply this update.
        //
        // If this update results in no update to the local state (no new schema
        // elements in the message) then this block returns None.
        //
        // If the table is created or modified below, it is returned and MUST be
        // inserted into an updated NamespaceSchema and ultimately placed into
        // the NamespaceCache for later reuse.
        let table = match ns.tables.get(&update.table_name) {
            Some(v) => {
                // Invariant: name -> ID mappings & partition templates MUST be
                // immutable and consistent across the cluster.
                assert_eq!(v.id, table_id);
                assert_eq!(v.partition_template, partition_template);

                update_table(v, update)?
            }
            None => {
                // Decode the columns within this update
                let columns = update
                    .columns
                    .into_iter()
                    .map(|v| {
                        let schema = ColumnSchema::try_from(&v)?;
                        Ok((v.name, schema))
                    })
                    .collect::<Result<BTreeMap<String, _>, _>>()
                    .map_err(Error::ColumnSchema)?;

                debug!(
                    table_name=%update.table_name,
                    %table_id,
                    n_columns=columns.len(),
                    "discovered new table via gossip"
                );

                Some(TableSchema {
                    id: table_id,
                    partition_template,
                    columns: ColumnsByName::from(columns),
                })
            }
        };

        // If the table was updated/created, it must be inserted into the
        // namespace (which itself now needs to be lazily cloned).
        if let Some(table) = table {
            upsert_cached_namespace(&self.inner, &ns, namespace_name, table, table_name);
        }

        Ok(())
    }
}

/// Apply `update` to `table`, returning an updated copy, if any.
fn update_table(table: &TableSchema, update: TableUpdated) -> Result<Option<TableSchema>, Error> {
    let table_id = TableId::new(update.table_id);

    // Invariant: name -> ID mappings MUST be immutable and consistent across
    // the cluster.
    assert_eq!(update.table_id, table_id.get());

    // Defer cloning unless it's necessary
    let mut table = Cow::Borrowed(table);

    // Union the gossiped columns with the existing table's columns.
    for v in update.columns {
        let column = ColumnSchema::try_from(&v).map_err(Error::ColumnSchema)?;
        let name = v.name;

        if let Some(v) = table.columns.get(&name) {
            // Invariant: name -> ID mappings MUST be immutable and
            // consistent across the cluster.
            assert_eq!(v.id, column.id);
            // Invariant: name -> data type mappings MUST be
            // immutable and consistent across the cluster.
            assert_eq!(v.column_type, column.column_type);
            continue;
        }

        debug!(
            table_name=&update.table_name,
            table_id = %table.id,
            column_name=name,
            column_id=column.id.get(),
            column_data_type=%column.column_type,
            "discovered new column via gossip"
        );

        // This column is missing in the schema and must be added.
        table.to_mut().columns.add_column(name, column);
    }

    // Hide the CoW complexity from the caller - it cares only about the
    // presence of an update (or not).
    match table {
        Cow::Borrowed(_) => Ok(None),
        Cow::Owned(v) => Ok(Some(v)),
    }
}

/// Update `cache`, upserting `table` into `namespace`.
fn upsert_cached_namespace<'a, T>(
    cache: &T,
    namespace: &NamespaceSchema,
    namespace_name: NamespaceName<'static>,
    table: TableSchema,
    table_name: String,
) where
    T: NamespaceCache + 'a,
{
    // Clone the namespace to obtain a mutable copy
    let mut schema = namespace.clone();

    // Insert the updated/created table
    schema.tables.insert(table_name.to_string(), table);
    // (note there may be an old value returned in case of an update)

    // The new, updated namespace must be inserted into the cache.
    let _ = cache.put_schema(namespace_name, schema);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{
        partition_template::{NamespacePartitionTemplateOverride, PARTITION_BY_DAY_PROTO},
        ColumnId, ColumnType, NamespaceId,
    };

    use crate::namespace_cache::{CacheMissErr, MemoryNamespaceCache};

    use super::*;

    const NAMESPACE_NAME: &str = "ns_bananas";
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

    /// Generate a test that processes the provided gossip message, and asserts
    /// the state of the namespace named [`NAMESPACE_NAME`] in the cache after.
    macro_rules! test_handle_gossip_message_ {
        (
            $name:ident,
            existing = $existing:expr, // Optional existing schema entry, if any.
            message = $message:expr,  // Gossip message to process.
            want = $($want:tt)+       // The result of looking up NAMESPACE_NAME
                                      // in the cache after the handler ran.
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_handle_gossip_message_ $name>]() {
                    let inner = Arc::new(MemoryNamespaceCache::default());
                    let layer = Arc::new(NamespaceSchemaGossip::new(
                        Arc::clone(&inner),
                    ));
                    let name = NamespaceName::try_from(NAMESPACE_NAME).unwrap();

                    // Optionally pre-populate the NamespaceSchema before the
                    // handler is invoked.
                    if let Some(ns) = $existing {
                        inner.put_schema(name.clone(), ns);
                    }

                    // Process the gossip message
                    layer.handle($message).await;

                    // Inspect the resulting cache state
                    let got = inner
                        .get_schema(&name)
                        .await;

                    assert_matches!(got, $($want)+);
                }
            }
        }
    }

    // A TableCreated message is received for a namespace that does not yet
    // exist on the local node.
    test_handle_gossip_message_!(
        table_created_namespace_miss,
        existing = None,
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![],
            }),
            partition_template: None,
        }),
        want = Err(CacheMissErr { .. })
    );

    // A minimal table create request with no tables to populate.
    //
    // This test focuses on the table metadata.
    test_handle_gossip_message_!(
        table_created_no_columns,
        existing = Some(DEFAULT_NAMESPACE.clone()),
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![],
            }),
            partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 1);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    Some((**PARTITION_BY_DAY_PROTO).clone()),
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());
                assert_eq!(columns.column_count(), 0);
            });
        }
    );

    // A previously unknown table with columns is created in response to a
    // TableCreated message.
    test_handle_gossip_message_!(
        table_created,
        existing = Some(DEFAULT_NAMESPACE.clone()),
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![
                    generated_types::influxdata::iox::gossip::v1::Column{
                        name: "c1".to_string(),
                        column_id: 101,
                        column_type: ColumnType::U64 as _,
                    }
                ],
            }),
            partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 1);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    Some((**PARTITION_BY_DAY_PROTO).clone()),
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());
                assert_eq!(columns.column_count(), 1);

                assert_eq!(*columns.get("c1").unwrap(), ColumnSchema {
                    id: ColumnId::new(101),
                    column_type: ColumnType::U64,
                });
            });
        }
    );

    // Table create with no specified partition template
    test_handle_gossip_message_!(
        table_created_no_partition_template,
        existing = Some(DEFAULT_NAMESPACE.clone()),
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![],
            }),
            partition_template: None,
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 1);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    None,
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());
                assert_eq!(columns.column_count(), 0);
            });
        }
    );

    // Table create arrives for a table that doesn't previously exist and is
    // merged with the existing, different table (instead of replacing).
    test_handle_gossip_message_!(
        table_created_merge_existing_tables,
        // Create an existing table in the namespace
        existing = Some({
            let mut ns = DEFAULT_NAMESPACE.clone();

            let table = TableSchema{
                id: TableId::new(1234), // Same table ID
                partition_template: TablePartitionTemplateOverride::default(),
                columns: ColumnsByName::new(vec![]),
            };

            ns.tables.insert("platanos".to_string(), table);

            ns
        }),
        // Send a gossip message adding a new column to the above
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![
                    generated_types::influxdata::iox::gossip::v1::Column{
                        name: "c2".to_string(),
                        column_id: 101,
                        column_type: ColumnType::Tag as _,
                    }
                ],
            }),
            partition_template: None,
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 2);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            // The original table still exists
            assert_matches!(ns.tables.get("platanos"), Some(TableSchema { id, .. }) => {
                assert_eq!(id.get(), 1234);
            });

            // The new table was merged in
            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    None,
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());

                // The table now must contain both columns
                assert_eq!(columns.column_count(), 1);
            });
        }
    );

    // Table create with columns, for an existing table that contains unrelated
    // columns.
    test_handle_gossip_message_!(
        table_created_merge_existing_columns,
        // Create an existing table / column in the namespace
        existing = Some({
            let mut ns = DEFAULT_NAMESPACE.clone();

            let mut table = TableSchema{
                id: TableId::new(42), // Same table ID
                partition_template: TablePartitionTemplateOverride::default(),
                columns: ColumnsByName::new(vec![]),
            };

            table.add_column_schema("c1".to_string(), ColumnSchema {
                id: ColumnId::new(100),
                column_type: ColumnType::String,
            });

            ns.tables.insert("bananas".to_string(), table);

            ns
        }),
        // Send a gossip message adding a new column to the above
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: 42,
                columns: vec![
                    generated_types::influxdata::iox::gossip::v1::Column{
                        name: "c2".to_string(),
                        column_id: 101,
                        column_type: ColumnType::Tag as _,
                    }
                ],
            }),
            partition_template: None,
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 1);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    None,
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());

                // The table now must contain both columns
                assert_eq!(columns.column_count(), 2);
                assert_eq!(*columns.get("c1").unwrap(), ColumnSchema {
                    id: ColumnId::new(100),
                    column_type: ColumnType::String,
                });
                assert_eq!(*columns.get("c2").unwrap(), ColumnSchema {
                    id: ColumnId::new(101),
                    column_type: ColumnType::Tag,
                });
            });
        }
    );

    // A create message arrives without an "update" payload embedded.
    test_handle_gossip_message_!(
        table_created_missing_update,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::TableCreated(TableCreated {
            table: None, // No inner content!
            partition_template: None,
        }),
        want = Ok(ns) => {
            assert_eq!(*ns, DEFAULT_NAMESPACE); // Unmodified
        }
    );

    // A create message arrives that doesn't include the namespace name.
    test_handle_gossip_message_!(
        table_created_missing_namespace_name,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::TableCreated(TableCreated {
            table: Some(TableUpdated {
                table_name: "bananas".to_string(),
                namespace_name: "".to_string(), // empty is missing in proto
                table_id: 42,
                columns: vec![],
            }),
            partition_template: None,
        }),
        want = Ok(ns) => {
            assert_eq!(*ns, DEFAULT_NAMESPACE); // Unmodified
        }
    );

    // An update message arrives that doesn't include the namespace name.
    test_handle_gossip_message_!(
        table_updated_missing_namespace_name,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::TableUpdated(TableUpdated {
            table_name: "bananas".to_string(),
            namespace_name: "".to_string(), // empty is missing in proto
            table_id: 42,
            columns: vec![],
        }),
        want = Ok(ns) => {
            assert_eq!(*ns, DEFAULT_NAMESPACE); // Unmodified
        }
    );

    // An update message arrives for an unknown namespace.
    test_handle_gossip_message_!(
        table_updated_missing_namespace,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::TableUpdated(TableUpdated {
            table_name: "bananas".to_string(),
            namespace_name: "another".to_string(), // not known locally
            table_id: 42,
            columns: vec![],
        }),
        want = Ok(ns) => {
            assert_eq!(*ns, DEFAULT_NAMESPACE); // Unmodified
        }
    );

    // An update message arrives for an unknown table.
    test_handle_gossip_message_!(
        table_updated_missing_table,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::TableUpdated(TableUpdated {
            table_name: "bananas".to_string(), // Table not known locally
            namespace_name: NAMESPACE_NAME.to_string(),
            table_id: 42,
            columns: vec![],
        }),
        want = Ok(ns) => {
            assert_eq!(*ns, DEFAULT_NAMESPACE); // Unmodified
        }
    );

    // An update message arrives that contains a mix of already-known columns
    // and new columns which are union-ed together.
    test_handle_gossip_message_!(
        table_updated_existing_column,
        // Create an existing table and column in the namespace
        existing = Some({
            let mut ns = DEFAULT_NAMESPACE.clone();

            let mut table = TableSchema{
                id: TableId::new(42), // Same table ID
                partition_template: TablePartitionTemplateOverride::default(),
                columns: ColumnsByName::new(vec![]),
            };

            table.add_column_schema("c1".to_string(), ColumnSchema {
                id: ColumnId::new(101),
                column_type: ColumnType::String,
            });

            ns.tables.insert("bananas".to_string(), table);

            ns
        }),
        message = Msg::TableUpdated(TableUpdated {
            table_name: "bananas".to_string(), // Table not known locally
            namespace_name: NAMESPACE_NAME.to_string(),
            table_id: 42,
            columns: vec![
                generated_types::influxdata::iox::gossip::v1::Column{
                    name: "c1".to_string(),
                    column_id: 101,
                    column_type: ColumnType::String as _,
                },
                generated_types::influxdata::iox::gossip::v1::Column{
                    name: "c2".to_string(),
                    column_id: 102,
                    column_type: ColumnType::Tag as _,
                },
            ],
        }),
        want = Ok(ns) => {
            assert_eq!(ns.id, DEFAULT_NAMESPACE.id);
            assert_eq!(ns.tables.len(), 1);
            assert_eq!(ns.max_columns_per_table, DEFAULT_NAMESPACE.max_columns_per_table);
            assert_eq!(ns.max_tables, DEFAULT_NAMESPACE.max_tables);
            assert_eq!(ns.retention_period_ns, DEFAULT_NAMESPACE.retention_period_ns);
            assert_eq!(ns.partition_template, DEFAULT_NAMESPACE.partition_template);

            assert_matches!(ns.tables.get("bananas"), Some(TableSchema { id, partition_template, columns }) => {
                assert_eq!(id.get(), 42);
                assert_eq!(*partition_template, TablePartitionTemplateOverride::try_new(
                    None,
                    &DEFAULT_NAMESPACE_PARTITION_TEMPLATE,
                ).unwrap());

                // The table now must contain both columns
                assert_eq!(columns.column_count(), 2);
                assert_eq!(*columns.get("c1").unwrap(), ColumnSchema {
                    id: ColumnId::new(101),
                    column_type: ColumnType::String,
                });
                assert_eq!(*columns.get("c2").unwrap(), ColumnSchema {
                    id: ColumnId::new(102),
                    column_type: ColumnType::Tag,
                });
            });
        }
    );

    // A create message arrives without an "update" payload embedded.
    test_handle_gossip_message_!(
        namespace_created_missing_name,
        existing = None,
        message = Msg::NamespaceCreated(NamespaceCreated {
            namespace_name: "".to_string(), // missing in proto
            namespace_id: DEFAULT_NAMESPACE.id.get(),
            partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
            max_columns_per_table: DEFAULT_NAMESPACE.max_columns_per_table as _,
            max_tables: DEFAULT_NAMESPACE.max_tables as _,
            retention_period_ns: DEFAULT_NAMESPACE.retention_period_ns,
        }),
        want = Err(CacheMissErr { .. })
    );

    // A create message arrive for a new namespace.
    test_handle_gossip_message_!(
        namespace_created,
        existing = None,
        message = Msg::NamespaceCreated(NamespaceCreated {
            namespace_name: NAMESPACE_NAME.to_string(),
            namespace_id: DEFAULT_NAMESPACE.id.get(),
            partition_template: None,
            max_columns_per_table: DEFAULT_NAMESPACE.max_columns_per_table as _,
            max_tables: DEFAULT_NAMESPACE.max_tables as _,
            retention_period_ns: DEFAULT_NAMESPACE.retention_period_ns,
        }),
        want = Ok(v) => {
            assert_eq!(*v, DEFAULT_NAMESPACE);
        }
    );

    // A create message arrives for an unknown namespace with a specified
    // partition template.
    test_handle_gossip_message_!(
        namespace_created_specified_partition_template,
        existing = None,
        message = Msg::NamespaceCreated(NamespaceCreated {
            namespace_name: NAMESPACE_NAME.to_string(), // missing in proto
            namespace_id: DEFAULT_NAMESPACE.id.get(),
            partition_template: Some((**PARTITION_BY_DAY_PROTO).clone()),
            max_columns_per_table: DEFAULT_NAMESPACE.max_columns_per_table as _,
            max_tables: DEFAULT_NAMESPACE.max_tables as _,
            retention_period_ns: DEFAULT_NAMESPACE.retention_period_ns,
        }),
        want = Ok(v) => {
            let mut want = DEFAULT_NAMESPACE.clone();
            want.partition_template = NamespacePartitionTemplateOverride::try_from((**PARTITION_BY_DAY_PROTO).clone()).unwrap();
            assert_eq!(*v, want);
        }
    );

    // A create message arrives for an unknown namespace.
    test_handle_gossip_message_!(
        namespace_created_existing,
        existing = Some(DEFAULT_NAMESPACE),
        message = Msg::NamespaceCreated(NamespaceCreated {
            namespace_name: NAMESPACE_NAME.to_string(), // missing in proto
            namespace_id: DEFAULT_NAMESPACE.id.get(),

            // The partition template is not allowed to change over the lifetime
            // of the namespace.
            partition_template: DEFAULT_NAMESPACE.partition_template.as_proto().cloned(),

            // But these fields can change.
            //
            // They will be ignored, and the local values used instead.
            max_columns_per_table: 123456,
            max_tables: 123456,
            retention_period_ns: Some(123456),
        }),
        want = Ok(v) => {
            // Mutable values remain unmodified
            assert_eq!(*v, DEFAULT_NAMESPACE);
        }
    );
}
