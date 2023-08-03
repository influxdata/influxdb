//! A schema change observer that gossips schema diffs to other peers.

use std::{collections::BTreeMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{ColumnsByName, NamespaceName, NamespaceSchema};
use generated_types::{
    influxdata::iox::gossip::v1::{
        gossip_message::Msg, Column, GossipMessage, NamespaceCreated, TableCreated, TableUpdated,
    },
    prost::Message,
};
use gossip::MAX_USER_PAYLOAD_BYTES;
use observability_deps::tracing::{debug, error, warn};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinHandle,
};

use crate::namespace_cache::{ChangeStats, NamespaceCache};

use super::traits::SchemaBroadcast;

/// A [`NamespaceCache`] decorator implementing cluster-wide, best-effort
/// propagation of local schema changes via the [`gossip`] subsystem.
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
/// Gossip [`Msg`] are populated within the call to
/// [`NamespaceCache::put_schema()`] but packed & serialised into [`gossip`]
/// frames off-path in a background task to minimise the latency overhead.
///
/// Processing of the diffs happens partially on the cache request path, and
/// partially off of it - the actual message content is generated on the cache
/// request path, but framing, serialisation and dispatch happen asynchronously.
#[derive(Debug)]
pub struct SchemaChangeObserver<T> {
    tx: mpsc::Sender<Msg>,
    task: JoinHandle<()>,
    inner: T,
}

impl<T> Drop for SchemaChangeObserver<T> {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A pass-through [`NamespaceCache`] implementation that gossips new schema
/// additions.
#[async_trait]
impl<T> NamespaceCache for SchemaChangeObserver<T>
where
    T: NamespaceCache,
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

impl<T> SchemaChangeObserver<T> {
    /// Construct a new [`SchemaChangeObserver`] that publishes gossip messages
    /// over `gossip`, and delegates cache operations to `inner`.
    pub fn new<U>(inner: T, gossip: U) -> Self
    where
        U: SchemaBroadcast + 'static,
    {
        let (tx, rx) = mpsc::channel(100);

        let task = tokio::spawn(actor_loop(rx, gossip));

        Self { tx, task, inner }
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

        self.enqueue(Msg::NamespaceCreated(msg));
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

            self.enqueue(Msg::TableCreated(msg));
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

            self.enqueue(Msg::TableUpdated(msg));
        }
    }

    /// Serialise, pack & gossip `msg` asynchronously.
    fn enqueue(&self, msg: Msg) {
        debug!(?msg, "sending schema message");
        match self.tx.try_send(msg) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => panic!("schema serialisation actor not running"),
            Err(TrySendError::Full(_)) => {
                warn!("schema serialisation queue full, dropping message")
            }
        }
    }
}

/// A background task loop that pulls [`Msg`] from `rx`, serialises / packs them
/// into a single gossip frame, and broadcasts the result over `gossip`.
async fn actor_loop<T>(mut rx: mpsc::Receiver<Msg>, gossip: T)
where
    T: SchemaBroadcast,
{
    while let Some(msg) = rx.recv().await {
        let frames = match msg {
            v @ Msg::NamespaceCreated(_) => vec![v],
            Msg::TableCreated(v) => serialise_table_create_frames(v),
            Msg::TableUpdated(v) => {
                // Split the frame up into N frames, sized as big as the gossip
                // transport will allow.
                let mut frames = Vec::with_capacity(1);
                if !serialise_table_update_frames(v, MAX_USER_PAYLOAD_BYTES, &mut frames) {
                    warn!("dropping oversized columns in table update");
                    // Continue sending the columns that were packed
                    // successfully
                }

                // It's possible all columns were oversized and therefore
                // dropped during the split.
                if frames.is_empty() {
                    warn!("dropping empty table update message");
                    continue;
                }

                frames.into_iter().map(Msg::TableUpdated).collect()
            }
        };

        for frame in frames {
            let msg = GossipMessage { msg: Some(frame) };
            gossip.broadcast(msg.encode_to_vec()).await
        }
    }

    debug!("stopping schema serialisation actor");
}

/// Serialise `msg` into one or more frames and append them to `out`.
///
/// If when `msg` is serialised it is less than or equal to `max_frame_bytes` in
/// length, this method appends a single frame. If `msg` is too large, it is
/// split into `N` multiple smaller frames and each appended individually.
///
/// If any [`Column`] within the message is too large to fit into an update
/// containing only itself, then this method returns `false` indicating
/// oversized columns were dropped from the output.
fn serialise_table_update_frames(
    mut msg: TableUpdated,
    max_frame_bytes: usize,
    out: &mut Vec<TableUpdated>,
) -> bool {
    // Does this frame fit within the maximum allowed frame size?
    if msg.encoded_len() <= max_frame_bytes {
        // Never send empty update messages.
        if !msg.columns.is_empty() {
            out.push(msg);
        }
        return true;
    }

    // This message is too large to be sent as a single message.
    debug!(
        n_bytes = msg.encoded_len(),
        "splitting oversized table update message"
    );

    // Find the midpoint in the column list.
    //
    // If the column list is down to one candidate, then the TableUpdate
    // containing only this column is too large to be sent, so this column must
    // be dropped from the output.
    if msg.columns.len() <= 1 {
        // Return false to indicate some columns were dropped.
        return false;
    }
    let mid = msg.columns.len() / 2;

    // Split up the columns in the message into two.
    let right = msg.columns.drain(mid..).collect();

    // msg now contains the left-most half of the columns.
    //
    // Construct the frame for the right-most half of the columns.
    let other = TableUpdated {
        columns: right,
        table_name: msg.table_name.clone(),
        namespace_name: msg.namespace_name.clone(),
        table_id: msg.table_id,
    };

    // Recursively split the frames until they fit, at which point they'll
    // be sent within this call.
    serialise_table_update_frames(msg, max_frame_bytes, out)
        && serialise_table_update_frames(other, max_frame_bytes, out)
}

/// Split `msg` into a [`TableCreated`] and a series of [`TableUpdated`] frames,
/// each sized less than [`MAX_USER_PAYLOAD_BYTES`].
fn serialise_table_create_frames(mut msg: TableCreated) -> Vec<Msg> {
    // If it fits, do nothing.
    if msg.encoded_len() <= MAX_USER_PAYLOAD_BYTES {
        return vec![Msg::TableCreated(msg)];
    }

    // If not, split the message into a single create, followed by N table
    // updates.
    let columns = std::mem::take(&mut msg.table.as_mut().unwrap().columns);

    // Check that on its own, without columns, it'll be send-able.
    if msg.encoded_len() > MAX_USER_PAYLOAD_BYTES {
        error!("dropping oversized table create message");
        return vec![];
    }

    // Recreate the new TableUpdate containing all the columns
    let mut update = msg.table.as_ref().unwrap().clone();
    update.columns = columns;

    let mut updates = Vec::with_capacity(1);
    if !serialise_table_update_frames(update, MAX_USER_PAYLOAD_BYTES, &mut updates) {
        warn!("dropping oversized columns in table update");
        // Continue sending the columns that were packed
        // successfully
    }

    // Return the table creation, followed by the updates containing columns.
    std::iter::once(Msg::TableCreated(msg))
        .chain(updates.into_iter().map(Msg::TableUpdated))
        .collect()
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
    use generated_types::influxdata::iox::{
        gossip::v1::column::ColumnType,
        partition_template::v1::{template_part::Part, PartitionTemplate, TemplatePart},
    };
    use proptest::prelude::*;

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

    proptest! {
        /// Assert that overly-large [`TableUpdated`] frames are correctly split
        /// into multiple smaller frames that are under the provided maximum
        /// size.
        #[test]
        fn prop_table_update_frame_splitting(
            max_frame_bytes in 0_usize..1_000,
            n_columns in 0_i64..1_000,
        ) {
            let mut msg = TableUpdated {
                table_name: TABLE_NAME.to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: TABLE_ID,
                columns: (0..n_columns).map(|v| {
                    Column {
                        name: "ignored".to_string(),
                        column_id: v,
                        column_type: 42,
                    }
                }).collect(),
            };

            let mut out = Vec::new();
            let success = serialise_table_update_frames(
                msg.clone(),
                max_frame_bytes,
                &mut out
            );

            // For all inputs, either:
            //
            //  - The return value indicates a frame could not be split
            //    entirely, in which case at least one column must exceed
            //    max_frame_bytes when packed in an update message, preventing
            //    it from being split down to N=1, and that column is absent
            //    from the output messages.
            //
            // or
            //
            //  - The message may have been split into more than one message of
            //    less-than-or-equal-to max_frame_bytes in size, in which case
            //    all columns must appear exactly once (validated by checking
            //    the column ID values reduce to the input set)
            //
            // All output messages must contain identical table metadata and be
            // non-empty.

            // Otherwise validate the successful case.
            for v in &out {
                // Invariant: the split frames must be less than or equal to the
                // desired maximum encoded frame size
                assert!(v.encoded_len() <= max_frame_bytes);

                // Invariant: all messages must contain the same metadata
                assert_eq!(v.table_name, msg.table_name);
                assert_eq!(v.namespace_name, msg.namespace_name);
                assert_eq!(v.table_id, msg.table_id);

                // Invariant: there should always be at least one column per
                // message (no empty update frames).
                assert!(!v.columns.is_empty());
            }

            let got_ids = into_sorted_vec(out.iter()
                .flat_map(|v| v.columns.iter().map(|v| v.column_id)));

            // Build the set of IDs that should appear.
            let want_ids = if success {
                // All column IDs must appear in the output as the splitting was
                // successful.
                (0..n_columns).collect::<Vec<_>>()
            } else {
                // Splitting failed.
                //
                // Build the set of column IDs expected to be in the output
                // (those that are under the maximum size on their own).
                let cols = std::mem::take(&mut msg.columns);
                let want_ids = into_sorted_vec(cols.into_iter().filter_map(|v| {
                        let column_id = v.column_id;
                        msg.columns = vec![v];
                        if msg.encoded_len() > max_frame_bytes {
                            return None;
                        }
                        Some(column_id)
                    }));

                // Assert at least one column must be too large to be packed
                // into an update containing only that column if one was
                // provided.
                if n_columns != 0 {
                    assert_ne!(want_ids.len(), n_columns as usize);
                }

                want_ids
            };

            // The ordered iterator of observed IDs must match the input
            // interval of [0, n_columns)
            assert!(want_ids.into_iter().eq(got_ids.into_iter()));
        }

        #[test]
        fn prop_table_create_frame_splitting(
            n_columns in 0..MAX_USER_PAYLOAD_BYTES as i64,
            partition_template_size in 0..MAX_USER_PAYLOAD_BYTES
        ) {
            let mut msg = TableCreated {
                table: Some(TableUpdated {
                    table_name: TABLE_NAME.to_string(),
                    namespace_name: NAMESPACE_NAME.to_string(),
                    table_id: TABLE_ID,
                    columns: (0..n_columns).map(|v| {
                        Column {
                            name: "ignored".to_string(),
                            column_id: v,
                            column_type: 42,
                        }
                    }).collect(),
                }),
                // Generate a potentially outrageously big partition template.
                // It's probably invalid, but that's fine for this test.
                partition_template: Some(PartitionTemplate{ parts: vec![
                    TemplatePart{ part: Some(Part::TagValue(
                        "b".repeat(partition_template_size).to_string()
                    ))
                }]}),
            };

            let frames = serialise_table_create_frames(msg.clone());

            // This TableCreated message is in one of three states:
            //
            //   1. Small enough to be sent in a one-er
            //   2. Big enough to need splitting into a create + updates
            //   3. Too big to send the create message at all
            //
            // For 1 and 2, all columns should be observed, and should follow
            // the create message. For 3, nothing should be sent, as those
            // updates are likely to go unused by peers who likely don't know
            // about this table.

            // Validate 3 first.
            if frames.is_empty() {
                // Then it MUST be too large to send even with no columns.
                //
                // Remove them and assert the size.
                msg.table.as_mut().unwrap().columns = vec![];
                assert!(msg.encoded_len() > MAX_USER_PAYLOAD_BYTES);
                return Ok(());
            }

            // Both 1 and 2 require all columns to eventually be observed, as
            // well as the create message.

            let mut iter = frames.into_iter();
            let mut columns = assert_matches!(iter.next(), Some(Msg::TableCreated(v)) => {
                // Invariant: table metadata must be correct
                assert_eq!(v.partition_template, msg.partition_template);

                let update = v.table.unwrap();
                assert_eq!(update.table_name, TABLE_NAME);
                assert_eq!(update.namespace_name, NAMESPACE_NAME);
                assert_eq!(update.table_id, TABLE_ID);

                // Return the columns in this create message, if any.
                update.columns
            });

            // Combine the columns from above, with any subsequent update
            // messages
            columns.extend(iter.flat_map(|v| {
                assert_matches!(v, Msg::TableUpdated(v) => {
                    // Invariant: metadata in follow-on updates must also match
                    assert_eq!(v.table_name, TABLE_NAME);
                    assert_eq!(v.namespace_name, NAMESPACE_NAME);
                    assert_eq!(v.table_id, TABLE_ID);

                    v.columns
                })
            }));

            // Columns now contains all the columns, across all the output
            // messages.
            let got_ids = into_sorted_vec(columns.into_iter().map(|v| v.column_id));

            // Which should match the full input set of column IDs
            assert!((0..n_columns).eq(got_ids.into_iter()));
        }
    }

    /// Generate a `Vec` of sorted `T`, preserving duplicates, if any.
    fn into_sorted_vec<T>(v: impl IntoIterator<Item = T>) -> Vec<T>
    where
        T: Ord,
    {
        let mut v = v.into_iter().collect::<Vec<_>>();
        v.sort_unstable();
        v
    }

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
                        Arc::new(MemoryNamespaceCache::default()),
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
        want = [Msg::NamespaceCreated(NamespaceCreated {
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
        want = [Msg::NamespaceCreated(NamespaceCreated {
            namespace_name,
            namespace_id,
            partition_template,
            max_columns_per_table,
            max_tables,
            retention_period_ns
        }),
        Msg::TableCreated(TableCreated { table, partition_template: table_template })] => {
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
        want = [Msg::TableCreated(TableCreated { table, partition_template: table_template })] => {
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
                Msg::TableCreated(created),
                Msg::TableUpdated(updated),
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
