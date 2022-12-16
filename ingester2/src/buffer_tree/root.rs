use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, ShardId, TableId};
use dml::DmlOperation;
use metric::U64Counter;
use parking_lot::Mutex;
use trace::span::Span;

use super::{
    namespace::{name_resolver::NamespaceNameProvider, NamespaceData},
    partition::{resolver::PartitionProvider, PartitionData},
    post_write::PostWriteObserver,
    table::name_resolver::TableNameProvider,
};
use crate::{
    arcmap::ArcMap,
    dml_sink::DmlSink,
    query::{response::QueryResponse, tracing::QueryExecTracing, QueryError, QueryExec},
};

/// A [`BufferTree`] is the root of an in-memory tree of many [`NamespaceData`]
/// containing one or more child [`TableData`] nodes, which in turn contain one
/// or more [`PartitionData`] nodes:
///
/// ```text
///
///                        ╔════════════════╗
///                        ║   BufferTree   ║
///                        ╚═══════╦════════╝
///                                ▼
///                         ┌────────────┐
///                         │ Namespace  ├┐
///                         └┬───────────┘├┐
///                          └┬───────────┘│
///                           └────┬───────┘
///                                ▼
///                         ┌────────────┐
///                         │   Table    ├┐
///                         └┬───────────┘├┐
///                          └┬───────────┘│
///                           └────┬───────┘
///                                ▼
///                         ┌────────────┐
///                         │ Partition  ├┐
///                         └┬───────────┘├┐
///                          └┬───────────┘│
///                           └────────────┘
/// ```
///
/// A buffer tree is a mutable data structure that implements [`DmlSink`] to
/// apply successive [`DmlOperation`] to its internal state, and makes the
/// materialised result available through a streaming [`QueryExec`] execution.
///
/// # Read Consistency
///
/// When [`BufferTree::query_exec()`] is called for a given table, a snapshot of
/// the table's current set of partitions is created and the data within these
/// partitions will be streamed to the client as they consume the response. New
/// partitions that are created concurrently to the query execution do not ever
/// become visible.
///
/// Concurrent writes during query execution to a partition that forms part of
/// this snapshot will be visible iff the write has been fully applied to the
/// partition's data buffer before the query stream reads the data from that
/// partition. Once a partition has been read, the data within it is immutable
/// from the caller's perspective, and subsequent writes DO NOT become visible.
///
/// [`TableData`]: crate::buffer_tree::table::TableData
/// [`PartitionData`]: crate::buffer_tree::partition::PartitionData
#[derive(Debug)]
pub(crate) struct BufferTree<O> {
    /// The resolver of `(table_id, partition_key)` to [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    /// A set of namespaces this [`BufferTree`] instance has processed
    /// [`DmlOperation`]'s for.
    ///
    /// The [`NamespaceNameProvider`] acts as a [`DeferredLoad`] constructor to
    /// resolve the [`NamespaceName`] for new [`NamespaceData`] out of the hot
    /// path.
    ///
    /// [`DeferredLoad`]: crate::deferred_load::DeferredLoad
    /// [`NamespaceName`]: data_types::NamespaceName
    namespaces: ArcMap<NamespaceId, NamespaceData<O>>,
    namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
    /// The [`TableName`] provider used by [`NamespaceData`] to initialise a
    /// [`TableData`].
    ///
    /// [`TableName`]: crate::buffer_tree::table::TableName
    /// [`TableData`]: crate::buffer_tree::table::TableData
    table_name_resolver: Arc<dyn TableNameProvider>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,

    post_write_observer: Arc<O>,
    transition_shard_id: ShardId,
}

impl<O> BufferTree<O>
where
    O: Send + Sync + Debug,
{
    /// Initialise a new [`BufferTree`] that emits metrics to `metrics`.
    pub(crate) fn new(
        namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
        table_name_resolver: Arc<dyn TableNameProvider>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
        metrics: Arc<metric::Registry>,
        transition_shard_id: ShardId,
    ) -> Self {
        let namespace_count = metrics
            .register_metric::<U64Counter>(
                "ingester_namespaces",
                "Number of namespaces known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespaces: Default::default(),
            namespace_name_resolver,
            table_name_resolver,
            metrics,
            partition_provider,
            post_write_observer,
            namespace_count,
            transition_shard_id,
        }
    }

    /// Gets the namespace data out of the map
    pub(crate) fn namespace(&self, namespace_id: NamespaceId) -> Option<Arc<NamespaceData<O>>> {
        self.namespaces.get(&namespace_id)
    }

    /// Iterate over a snapshot of [`PartitionData`] in the tree.
    ///
    /// This iterator will iterate over a consistent snapshot of namespaces
    /// taken at the time this fn was called, recursing into each table &
    /// partition incrementally. Each time a namespace is read, a snapshot of
    /// tables is taken, and these are then iterated on. Likewise the first read
    /// of a table causes a snapshot of partitions to be taken, and it is those
    /// partitions that are read.
    ///
    /// Because of this, concurrent writes may add new data to partitions/tables
    /// and these MAY be readable depending on the progress of the iterator
    /// through the tree.
    pub(crate) fn partitions(&self) -> impl Iterator<Item = Arc<Mutex<PartitionData>>> + Send {
        self.namespaces
            .values()
            .into_iter()
            .flat_map(|v| v.tables())
            .flat_map(|v| v.partitions())
    }
}

#[async_trait]
impl<O> DmlSink for BufferTree<O>
where
    O: PostWriteObserver,
{
    type Error = mutable_batch::Error;

    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        let namespace_id = op.namespace_id();
        let namespace_data = self.namespaces.get_or_insert_with(&namespace_id, || {
            // Increase the metric that records the number of namespaces
            // buffered in this ingester instance.
            self.namespace_count.inc(1);

            Arc::new(NamespaceData::new(
                namespace_id,
                self.namespace_name_resolver.for_namespace(namespace_id),
                Arc::clone(&self.table_name_resolver),
                Arc::clone(&self.partition_provider),
                Arc::clone(&self.post_write_observer),
                &self.metrics,
                self.transition_shard_id,
            ))
        });

        namespace_data.apply(op).await
    }
}

#[async_trait]
impl<O> QueryExec for BufferTree<O>
where
    O: Send + Sync + Debug,
{
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        // Extract the namespace if it exists.
        let inner = self
            .namespace(namespace_id)
            .ok_or(QueryError::NamespaceNotFound(namespace_id))?;

        // Delegate query execution to the namespace, wrapping the execution in
        // a tracing delegate to emit a child span.
        QueryExecTracing::new(inner, "namespace")
            .query_exec(namespace_id, table_id, columns, span)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{PartitionId, PartitionKey};
    use datafusion::{assert_batches_eq, assert_batches_sorted_eq};
    use futures::{StreamExt, TryStreamExt};
    use metric::{Attributes, Metric};

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::{
                name_resolver::mock::MockNamespaceNameProvider, NamespaceData, NamespaceName,
            },
            partition::{resolver::mock::MockPartitionProvider, PartitionData, SortKeyState},
            post_write::mock::MockPostWriteObserver,
            table::{name_resolver::mock::MockTableNameProvider, TableName},
        },
        deferred_load::{self, DeferredLoad},
        query::partition_response::PartitionResponse,
        test_util::make_write_op,
    };

    const TABLE_ID: TableId = TableId::new(44);
    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const NAMESPACE_ID: NamespaceId = NamespaceId::new(42);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    #[tokio::test]
    async fn test_namespace_init_table() {
        let metrics = Arc::new(metric::Registry::default());

        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("banana-split"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
        ));

        // Init the namespace
        let ns = NamespaceData::new(
            NAMESPACE_ID,
            DeferredLoad::new(Duration::from_millis(1), async { NAMESPACE_NAME.into() }),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            &metrics,
            TRANSITION_SHARD_ID,
        );

        // Assert the namespace name was stored
        let name = ns.namespace_name().to_string();
        assert!(
            (name == NAMESPACE_NAME) || (name == deferred_load::UNRESOLVED_DISPLAY_STRING),
            "unexpected namespace name: {name}"
        );

        // Assert the namespace does not contain the test data
        assert!(ns.table(TABLE_ID).is_none());

        // Write some test data
        ns.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("banana-split"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,city=Madrid day="sun",temp=55 22"#,
        )))
        .await
        .expect("buffer op should succeed");

        // Referencing the table should succeed
        assert!(ns.table(TABLE_ID).is_some());

        // And the table counter metric should increase
        let tables = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_tables")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(tables, 1);

        // Ensure the deferred namespace name is loaded.
        let name = ns.namespace_name().get().await;
        assert_eq!(&**name, NAMESPACE_NAME);
        assert_eq!(ns.namespace_name().to_string(), NAMESPACE_NAME);
    }

    /// Generate a test that performs a set of writes and assert the data within
    /// the table with TABLE_ID in the namespace with NAMESPACE_ID.
    macro_rules! test_write_query {
        (
            $name:ident,
            partitions = [$($partition:expr), +], // The set of PartitionData for the mock partition provider
            writes = [$($write:expr), *],         // The set of DmlWrite to apply()
            want = $want:expr                     // The expected results of querying NAMESPACE_ID and TABLE_ID
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_write_query_ $name>]() {
                    // Configure the mock partition provider with the provided
                    // partitions.
                    let partition_provider = Arc::new(MockPartitionProvider::default()
                        $(
                            .with_partition($partition)
                        )+
                    );

                    // Init the buffer tree
                    let buf = BufferTree::new(
                        Arc::new(MockNamespaceNameProvider::default()),
                        Arc::new(MockTableNameProvider::new(TABLE_NAME)),
                        partition_provider,
                        Arc::new(MockPostWriteObserver::default()),
                        Arc::new(metric::Registry::default()),
                        TRANSITION_SHARD_ID,
                    );

                    // Write the provided DmlWrites
                    $(
                        buf.apply(DmlOperation::Write($write))
                            .await
                            .expect("failed to perform write");
                    )*

                    // Execute the query against NAMESPACE_ID and TABLE_ID
                    let batches = buf
                        .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
                        .await
                        .expect("query should succeed")
                        .into_record_batches()
                        .try_collect::<Vec<_>>()
                        .await
                        .expect("query failed");

                    // Assert the contents of NAMESPACE_ID and TABLE_ID
                    assert_batches_sorted_eq!(
                        $want,
                        &batches
                    );
                }
            }
        };
    }

    // A simple "read your writes" test.
    test_write_query!(
        read_writes,
        partitions = [PartitionData::new(
            PartitionId::new(0),
            PartitionKey::from("p1"),
            NAMESPACE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NamespaceName::from(NAMESPACE_NAME)
            })),
            TABLE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        )],
        writes = [make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        )],
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Asturias | 35   | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    // A query that ensures the data across multiple partitions within a single
    // table are returned.
    test_write_query!(
        multiple_partitions,
        partitions = [
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("p1"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
            PartitionData::new(
                PartitionId::new(1),
                PartitionKey::from("p2"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            )
        ],
        writes = [
            make_write_op(
                &PartitionKey::from("p1"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"bananas,region=Madrid temp=35 4242424242"#,
            ),
            make_write_op(
                &PartitionKey::from("p2"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"bananas,region=Asturias temp=25 4242424242"#,
            )
        ],
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Madrid   | 35   | 1970-01-01T00:00:04.242424242 |",
            "| Asturias | 25   | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    // A query that ensures the data across multiple namespaces is correctly
    // filtered to return only the queried table.
    test_write_query!(
        filter_multiple_namespaces,
        partitions = [
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("p1"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
            PartitionData::new(
                PartitionId::new(1),
                PartitionKey::from("p2"),
                NamespaceId::new(4321), // A different namespace ID.
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TableId::new(1234), // A different table ID.
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            )
        ],
        writes = [
            make_write_op(
                &PartitionKey::from("p1"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"bananas,region=Madrid temp=25 4242424242"#,
            ),
            make_write_op(
                &PartitionKey::from("p2"),
                NamespaceId::new(4321), // A different namespace ID.
                TABLE_NAME,
                TableId::new(1234), // A different table ID
                0,
                r#"bananas,region=Asturias temp=35 4242424242"#,
            )
        ],
        want = [
            "+--------+------+-------------------------------+",
            "| region | temp | time                          |",
            "+--------+------+-------------------------------+",
            "| Madrid | 25   | 1970-01-01T00:00:04.242424242 |",
            "+--------+------+-------------------------------+",
        ]
    );

    // A query that ensures the data across multiple tables (with the same table
    // name!) is correctly filtered to return only the queried table.
    test_write_query!(
        filter_multiple_tabls,
        partitions = [
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("p1"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
            PartitionData::new(
                PartitionId::new(1),
                PartitionKey::from("p2"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TableId::new(1234), // A different table ID.
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            )
        ],
        writes = [
            make_write_op(
                &PartitionKey::from("p1"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"bananas,region=Madrid temp=25 4242424242"#,
            ),
            make_write_op(
                &PartitionKey::from("p2"),
                NAMESPACE_ID,
                TABLE_NAME,
                TableId::new(1234), // A different table ID
                0,
                r#"bananas,region=Asturias temp=35 4242424242"#,
            )
        ],
        want = [
            "+--------+------+-------------------------------+",
            "| region | temp | time                          |",
            "+--------+------+-------------------------------+",
            "| Madrid | 25   | 1970-01-01T00:00:04.242424242 |",
            "+--------+------+-------------------------------+",
        ]
    );

    // Assert that no dedupe operations are performed when querying a partition
    // that contains duplicate rows for a single series/primary key, but the
    // operations maintain their ordering (later writes appear after earlier
    // writes).
    test_write_query!(
        duplicate_writes,
        partitions = [PartitionData::new(
            PartitionId::new(0),
            PartitionKey::from("p1"),
            NAMESPACE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NamespaceName::from(NAMESPACE_NAME)
            })),
            TABLE_ID,
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from(TABLE_NAME)
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        )],
        writes = [
            make_write_op(
                &PartitionKey::from("p1"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                0,
                r#"bananas,region=Asturias temp=35 4242424242"#,
            ),
            make_write_op(
                &PartitionKey::from("p1"),
                NAMESPACE_ID,
                TABLE_NAME,
                TABLE_ID,
                1,
                r#"bananas,region=Asturias temp=12 4242424242"#,
            )
        ],
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Asturias | 35   | 1970-01-01T00:00:04.242424242 |",
            "| Asturias | 12   | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    /// Assert that multiple writes to a single namespace/table results in a
    /// single namespace being created, and matching metrics.
    #[tokio::test]
    async fn test_metrics() {
        // Configure the mock partition provider to return a single partition, named
        // p1.
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(PartitionData::new(
                    PartitionId::new(0),
                    PartitionKey::from("p1"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                ))
                .with_partition(PartitionData::new(
                    PartitionId::new(0),
                    PartitionKey::from("p2"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                )),
        );

        let metrics = Arc::new(metric::Registry::default());

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::default()),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::clone(&metrics),
            TRANSITION_SHARD_ID,
        );

        // Write data to partition p1, in table "bananas".
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write initial data");

        // Write a duplicate record with the same series key & timestamp, but a
        // different temp value.
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p2"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            1,
            r#"bananas,region=Asturias temp=12 4242424242"#,
        )))
        .await
        .expect("failed to overwrite data");

        // Validate namespace count
        assert_eq!(buf.namespaces.values().len(), 1);
        let m = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_namespaces")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to find metric with attributes")
            .fetch();
        assert_eq!(m, 1, "namespace counter mismatch");

        // Validate table count
        let m = metrics
            .get_instrument::<Metric<U64Counter>>("ingester_tables")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[]))
            .expect("failed to find metric with attributes")
            .fetch();
        assert_eq!(m, 1, "tables counter mismatch");
    }

    /// Assert that multiple writes to a single namespace/table results in a
    /// single namespace being created, and matching metrics.
    #[tokio::test]
    async fn test_partition_iter() {
        const TABLE2_ID: TableId = TableId::new(1234321);

        // Configure the mock partition provider to return a single partition, named
        // p1.
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(PartitionData::new(
                    PartitionId::new(0),
                    PartitionKey::from("p1"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                ))
                .with_partition(PartitionData::new(
                    PartitionId::new(1),
                    PartitionKey::from("p2"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                ))
                .with_partition(PartitionData::new(
                    PartitionId::new(2),
                    PartitionKey::from("p3"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE2_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from("another_table")
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                )),
        );

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::default()),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::clone(&Arc::new(metric::Registry::default())),
            TRANSITION_SHARD_ID,
        );

        assert_eq!(buf.partitions().count(), 0);

        // Write data to partition p1, in table "bananas".
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write initial data");

        assert_eq!(buf.partitions().count(), 1);

        // Write data to partition p2, in table "bananas".
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p2"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write initial data");

        assert_eq!(buf.partitions().count(), 2);

        // Write data to partition p3, in the second table
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p3"),
            NAMESPACE_ID,
            "another_table",
            TABLE2_ID,
            0,
            r#"another_table,region=Asturias temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write initial data");

        // Iterate over the partitions and ensure they were all visible.
        let mut ids = buf
            .partitions()
            .map(|p| p.lock().partition_id().get())
            .collect::<Vec<_>>();

        ids.sort_unstable();

        assert_matches!(*ids, [0, 1, 2]);
    }

    /// Assert the correct "not found" errors are generated for missing
    /// table/namespaces, and that querying an entirely empty buffer tree
    /// returns no data (as opposed to panicking, etc).
    #[tokio::test]
    async fn test_not_found() {
        let partition_provider = Arc::new(MockPartitionProvider::default().with_partition(
            PartitionData::new(
                PartitionId::new(0),
                PartitionKey::from("p1"),
                NAMESPACE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    NamespaceName::from(NAMESPACE_NAME)
                })),
                TABLE_ID,
                Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                    TableName::from(TABLE_NAME)
                })),
                SortKeyState::Provided(None),
                TRANSITION_SHARD_ID,
            ),
        ));

        // Init the BufferTree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::default()),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
            TRANSITION_SHARD_ID,
        );

        // Query the empty tree
        let err = buf
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect_err("query should fail");
        assert_matches!(err, QueryError::NamespaceNotFound(ns) => {
            assert_eq!(ns, NAMESPACE_ID);
        });

        // Write data to partition p1, in table "bananas".
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Asturias temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write data");

        // Ensure an unknown table errors
        let err = buf
            .query_exec(NAMESPACE_ID, TableId::new(1234), vec![], None)
            .await
            .expect_err("query should fail");
        assert_matches!(err, QueryError::TableNotFound(ns, t) => {
            assert_eq!(ns, NAMESPACE_ID);
            assert_eq!(t, TableId::new(1234));
        });

        // Ensure a valid namespace / table does not error
        buf.query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("namespace / table should exist");
    }

    /// This test asserts the read consistency properties defined in the
    /// [`BufferTree`] type docs.
    ///
    /// Specifically, this test ensures:
    ///
    ///  * A read snapshot of the set of partitions is created during the
    ///    construction of the query stream. New partitions added (or existing
    ///    partitions removed) do not change the query results once the stream
    ///    has been initialised.
    ///  * Concurrent writes to partitions that form part of the read snapshot
    ///    become visible if they are ordered/applied before the acquisition of
    ///    the partition data by the query stream. Writes ordered after the
    ///    partition lock acquisition do not become readable.
    ///
    /// All writes use the same write timestamp as it is not a factor in
    /// ordering of writes.
    #[tokio::test]
    async fn test_read_consistency() {
        // Configure the mock partition provider to return two partitions, named
        // p1 and p2.
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(PartitionData::new(
                    PartitionId::new(0),
                    PartitionKey::from("p1"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                ))
                .with_partition(PartitionData::new(
                    PartitionId::new(1),
                    PartitionKey::from("p2"),
                    NAMESPACE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        NamespaceName::from(NAMESPACE_NAME)
                    })),
                    TABLE_ID,
                    Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                        TableName::from(TABLE_NAME)
                    })),
                    SortKeyState::Provided(None),
                    TRANSITION_SHARD_ID,
                )),
        );

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::default()),
            Arc::new(MockTableNameProvider::new(TABLE_NAME)),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
            TRANSITION_SHARD_ID,
        );

        // Write data to partition p1, in table "bananas".
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            0,
            r#"bananas,region=Madrid temp=35 4242424242"#,
        )))
        .await
        .expect("failed to write initial data");

        // Execute a query of the buffer tree, generating the result stream, but
        // DO NOT consume it.
        let stream = buf
            .query_exec(NAMESPACE_ID, TABLE_ID, vec![], None)
            .await
            .expect("query should succeed")
            .into_partition_stream();

        // Perform a write concurrent to the consumption of the query stream
        // that creates a new partition (p2) in the same table.
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p2"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            1,
            r#"bananas,region=Asturias temp=20 4242424242"#,
        )))
        .await
        .expect("failed to perform concurrent write to new partition");

        // Perform another write that hits the partition within the query
        // results snapshot (p1) before the partition is read.
        buf.apply(DmlOperation::Write(make_write_op(
            &PartitionKey::from("p1"),
            NAMESPACE_ID,
            TABLE_NAME,
            TABLE_ID,
            2,
            r#"bananas,region=Murcia temp=30 4242424242"#,
        )))
        .await
        .expect("failed to perform concurrent write to existing partition");

        // Consume the set of partitions within the query stream.
        //
        // Under the specified query consistency guarantees, both the first and
        // third writes (both to p1) should be visible. The second write to p2
        // should not be visible.
        let mut partitions: Vec<PartitionResponse> = stream.collect().await;
        assert_eq!(partitions.len(), 1); // only p1, not p2
        let partition = partitions.pop().unwrap();

        // Perform the partition read
        let batches = datafusion::physical_plan::common::collect(
            partition.into_record_batch_stream().unwrap(),
        )
        .await
        .expect("failed to collate query results");

        // Assert the contents of p1 contains both the initial write, and the
        // 3rd write in a single RecordBatch.
        assert_batches_eq!(
            [
                "+--------+------+-------------------------------+",
                "| region | temp | time                          |",
                "+--------+------+-------------------------------+",
                "| Madrid | 35   | 1970-01-01T00:00:04.242424242 |",
                "| Murcia | 30   | 1970-01-01T00:00:04.242424242 |",
                "+--------+------+-------------------------------+",
            ],
            &batches
        );
    }
}
