use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{NamespaceId, TableId};
use metric::U64Counter;
use parking_lot::Mutex;
use predicate::Predicate;
use trace::span::Span;

use super::{
    namespace::{name_resolver::NamespaceNameProvider, NamespaceData},
    partition::{resolver::PartitionProvider, PartitionData},
    post_write::PostWriteObserver,
    table::metadata_resolver::TableProvider,
};
use crate::{
    arcmap::ArcMap,
    dml_payload::IngestOp,
    dml_sink::DmlSink,
    partition_iter::PartitionIter,
    query::{
        projection::OwnedProjection, response::QueryResponse, tracing::QueryExecTracing,
        QueryError, QueryExec,
    },
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
/// apply successive [`IngestOp`] to its internal state, and makes the
/// materialised result available through a streaming [`QueryExec`] execution.
///
/// The tree is populated lazily/on-demand as [`IngestOp`] are applied or
/// the data is accessed, but some information is pre-cached and made available
/// to the [`BufferTree`] for performance reasons (see [`PartitionCache`]).
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
/// [`PartitionCache`]: crate::buffer_tree::partition::resolver::PartitionCache
#[derive(Debug)]
pub(crate) struct BufferTree<O> {
    /// The resolver of `(table_id, partition_key)` to [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    /// A set of namespaces this [`BufferTree`] instance has processed
    /// [`IngestOp`]'s for.
    ///
    /// The [`NamespaceNameProvider`] acts as a [`DeferredLoad`] constructor to
    /// resolve the [`NamespaceName`] for new [`NamespaceData`] out of the hot
    /// path.
    ///
    /// [`DeferredLoad`]: crate::deferred_load::DeferredLoad
    /// [`NamespaceName`]: data_types::NamespaceName
    namespaces: ArcMap<NamespaceId, NamespaceData<O>>,
    namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
    /// The [`TableMetadata`] provider used by [`NamespaceData`] to initialise a
    /// [`TableData`].
    ///
    /// [`TableMetadata`]: crate::buffer_tree::table::TableMetadata
    /// [`TableData`]: crate::buffer_tree::table::TableData
    table_resolver: Arc<dyn TableProvider>,

    metrics: Arc<metric::Registry>,
    namespace_count: U64Counter,

    post_write_observer: Arc<O>,
}

impl<O> BufferTree<O>
where
    O: Send + Sync + Debug,
{
    /// Initialise a new [`BufferTree`] that emits metrics to `metrics`.
    pub(crate) fn new(
        namespace_name_resolver: Arc<dyn NamespaceNameProvider>,
        table_resolver: Arc<dyn TableProvider>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
        metrics: Arc<metric::Registry>,
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
            table_resolver,
            metrics,
            partition_provider,
            post_write_observer,
            namespace_count,
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

    async fn apply(&self, op: IngestOp) -> Result<(), Self::Error> {
        let namespace_id = op.namespace();
        let namespace_data = self.namespaces.get_or_insert_with(&namespace_id, || {
            // Increase the metric that records the number of namespaces
            // buffered in this ingester instance.
            self.namespace_count.inc(1);

            Arc::new(NamespaceData::new(
                namespace_id,
                Arc::new(self.namespace_name_resolver.for_namespace(namespace_id)),
                Arc::clone(&self.table_resolver),
                Arc::clone(&self.partition_provider),
                Arc::clone(&self.post_write_observer),
                &self.metrics,
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
        projection: OwnedProjection,
        span: Option<Span>,
        predicate: Option<Predicate>,
    ) -> Result<Self::Response, QueryError> {
        // Extract the namespace if it exists.
        let inner = self
            .namespace(namespace_id)
            .ok_or(QueryError::NamespaceNotFound(namespace_id))?;

        // Delegate query execution to the namespace, wrapping the execution in
        // a tracing delegate to emit a child span.
        QueryExecTracing::new(inner, "namespace")
            .query_exec(namespace_id, table_id, projection, span, predicate)
            .await
    }
}

impl<O> PartitionIter for crate::buffer_tree::BufferTree<O>
where
    O: Send + Sync + Debug + 'static,
{
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
        Box::new(self.partitions())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arrow::datatypes::DataType;
    use assert_matches::assert_matches;
    use data_types::{
        partition_template::{test_table_partition_override, TemplatePart},
        PartitionHashId, PartitionKey, TransitionPartitionId,
    };
    use datafusion::{
        assert_batches_eq, assert_batches_sorted_eq,
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use futures::StreamExt;
    use lazy_static::lazy_static;
    use metric::{Attributes, Metric};
    use predicate::Predicate;
    use test_helpers::maybe_start_logging;

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::{name_resolver::mock::MockNamespaceNameProvider, NamespaceData},
            partition::resolver::mock::MockPartitionProvider,
            post_write::mock::MockPostWriteObserver,
            table::{metadata_resolver::mock::MockTableProvider, TableMetadata},
        },
        deferred_load::{self, DeferredLoad},
        query::partition_response::PartitionResponse,
        test_util::{
            defer_namespace_name_1_ms, make_write_op, PartitionDataBuilder,
            ARBITRARY_CATALOG_PARTITION_ID, ARBITRARY_NAMESPACE_ID, ARBITRARY_NAMESPACE_NAME,
            ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID, ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_PROVIDER, ARBITRARY_TRANSITION_PARTITION_ID,
        },
    };

    const TABLE2_ID: TableId = TableId::new(1234321);
    const TABLE2_NAME: &str = "another_table";

    const NAMESPACE2_ID: NamespaceId = NamespaceId::new(4321);

    lazy_static! {
        static ref PARTITION2_KEY: PartitionKey = PartitionKey::from("p2");
        static ref PARTITION3_KEY: PartitionKey = PartitionKey::from("p3");
        static ref ARBITRARY_TABLE_PARTITION2: TransitionPartitionId =
            TransitionPartitionId::Deterministic(PartitionHashId::new(
                ARBITRARY_TABLE_ID,
                &PARTITION2_KEY
            ));
        static ref TABLE2_PARTITION2: TransitionPartitionId =
            TransitionPartitionId::Deterministic(PartitionHashId::new(TABLE2_ID, &PARTITION2_KEY));
    }

    #[tokio::test]
    async fn test_namespace_init_table() {
        let metrics = Arc::new(metric::Registry::default());

        // Configure the mock partition provider to return a partition for this
        // table ID.
        let partition_provider = Arc::new(
            MockPartitionProvider::default().with_partition(PartitionDataBuilder::new().build()),
        );

        // Init the namespace
        let ns = NamespaceData::new(
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_ms(),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            &metrics,
        );

        // Assert the namespace name was stored
        let name = ns.namespace_name().to_string();
        assert!(
            (name.as_str() == &***ARBITRARY_NAMESPACE_NAME)
                || (name == deferred_load::UNRESOLVED_DISPLAY_STRING),
            "unexpected namespace name: {name}"
        );

        // Assert the namespace does not contain the test data
        assert!(ns.table(ARBITRARY_TABLE_ID).is_none());

        // Write some test data
        ns.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},city=Madrid day="sun",temp=55 22"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("buffer op should succeed");

        // Referencing the table should succeed
        assert!(ns.table(ARBITRARY_TABLE_ID).is_some());

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
        assert_eq!(&**name, &***ARBITRARY_NAMESPACE_NAME);
        assert_eq!(
            ns.namespace_name().to_string().as_str(),
            &***ARBITRARY_NAMESPACE_NAME
        );
    }

    /// Generate a test that performs a set of writes and assert the data within
    /// the table with ARBITRARY_TABLE_ID in the namespace with ARBITRARY_NAMESPACE_ID.
    macro_rules! test_write_query {
        (
            $name:ident,
            $(table_provider = $table_provider:expr,)? // An optional table provider
            $(projection = $projection:expr,)?    // An optional OwnedProjection
            partitions = [$($partition:expr), +], // The set of PartitionData for the mock
                                                  // partition provider
            writes = [$($write:expr), *],         // The set of WriteOperation to apply()
            predicate = $predicate:expr,          // An optional predicate to use for the query
            want = $want:expr                     // The expected results of querying
                                                  // ARBITRARY_NAMESPACE_ID and ARBITRARY_TABLE_ID
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_write_query_ $name>]() {
                    maybe_start_logging();

                    // Configure the mock partition provider with the provided
                    // partitions.
                    let partition_provider = Arc::new(MockPartitionProvider::default()
                        $(
                            .with_partition($partition)
                        )+
                    );

                    #[allow(unused_variables)]
                    let table_provider = Arc::clone(&*ARBITRARY_TABLE_PROVIDER);
                    $(
                        let table_provider: Arc<dyn TableProvider> = $table_provider;
                    )?

                    // Init the buffer tree
                    let buf = BufferTree::new(
                        Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
                        table_provider,
                        partition_provider,
                        Arc::new(MockPostWriteObserver::default()),
                        Arc::new(metric::Registry::default()),
                    );

                    // Write the provided WriteOperation
                    $(
                        buf.apply(IngestOp::Write($write).into())
                            .await
                            .expect("failed to perform write");
                    )*

                    #[allow(unused_variables)]
                    let projection = OwnedProjection::default();
                    $(
                        let projection = $projection;
                    )?

                    // Execute the query against ARBITRARY_NAMESPACE_ID and ARBITRARY_TABLE_ID
                    let batches = buf
                        .query_exec(
                            ARBITRARY_NAMESPACE_ID,
                            ARBITRARY_TABLE_ID,
                            projection,
                            None,
                            $predicate
                        )
                        .await
                        .expect("query should succeed")
                        .into_partition_stream()
                        .flat_map(|ps| futures::stream::iter(ps.into_record_batches()))
                        .collect::<Vec<_>>()
                        .await;

                    // Assert the contents of ARBITRARY_NAMESPACE_ID and ARBITRARY_TABLE_ID
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
        partitions = [PartitionDataBuilder::new()
            .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
            .build()],
        writes = [make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )],
        predicate = None,
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Asturias | 35.0 | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    // Projection support
    test_write_query!(
        projection,
        projection = OwnedProjection::from(vec!["time", "region"]),
        partitions = [PartitionDataBuilder::new()
            .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
            .build()],
        writes = [make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )],
        predicate = None,
        want = [
            "+-------------------------------+----------+",
            "| time                          | region   |",
            "+-------------------------------+----------+",
            "| 1970-01-01T00:00:04.242424242 | Asturias |",
            "+-------------------------------+----------+",
        ]
    );

    // Projection support
    test_write_query!(
        projection_without_time,
        projection = OwnedProjection::from(vec!["region"]),
        partitions = [PartitionDataBuilder::new()
            .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
            .build()],
        writes = [make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )],
        predicate = None,
        want = [
            "+----------+",
            "| region   |",
            "+----------+",
            "| Asturias |",
            "+----------+",
        ]
    );

    // A query that ensures the data across multiple partitions within a single
    // table are returned.
    test_write_query!(
        multiple_partitions,
        partitions = [
            PartitionDataBuilder::new()
                .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                .build(),
            PartitionDataBuilder::new()
                .with_partition_key(PARTITION2_KEY.clone())
                .build()
        ],
        writes = [
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region=Madrid temp=35 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            ),
            make_write_op(
                &PARTITION2_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region=Asturias temp=25 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            )
        ],
        predicate = None,
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Madrid   | 35.0 | 1970-01-01T00:00:04.242424242 |",
            "| Asturias | 25.0 | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    // A query that ensures the data across multiple namespaces is correctly
    // filtered to return only the queried table.
    test_write_query!(
        filter_multiple_namespaces,
        partitions = [
            PartitionDataBuilder::new()
                .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                .build(),
            PartitionDataBuilder::new()
                .with_partition_key(PARTITION2_KEY.clone())
                .with_namespace_id(NAMESPACE2_ID) // A different namespace ID.
                .with_table_id(TABLE2_ID) // A different table ID.
                .build()
        ],
        writes = [
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region=Madrid temp=25 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            ),
            make_write_op(
                &PARTITION2_KEY,
                NAMESPACE2_ID, // A different namespace ID.
                &ARBITRARY_TABLE_NAME,
                TABLE2_ID, // A different table ID
                0,
                &format!(
                    r#"{},region=Asturias temp=35 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            )
        ],
        predicate = None,
        want = [
            "+--------+------+-------------------------------+",
            "| region | temp | time                          |",
            "+--------+------+-------------------------------+",
            "| Madrid | 25.0 | 1970-01-01T00:00:04.242424242 |",
            "+--------+------+-------------------------------+",
        ]
    );

    // A query that ensures the data across multiple tables (with the same table
    // name!) is correctly filtered to return only the queried table.
    test_write_query!(
        filter_multiple_tables,
        partitions = [
            PartitionDataBuilder::new()
                .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                .build(),
            PartitionDataBuilder::new()
                .with_partition_key(PARTITION2_KEY.clone())
                .with_table_id(TABLE2_ID) // A different table ID.
                .build()
        ],
        writes = [
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region=Madrid temp=25 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            ),
            make_write_op(
                &PARTITION2_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                TABLE2_ID, // A different table ID
                0,
                &format!(
                    r#"{},region=Asturias temp=35 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            )
        ],
        predicate = None,
        want = [
            "+--------+------+-------------------------------+",
            "| region | temp | time                          |",
            "+--------+------+-------------------------------+",
            "| Madrid | 25.0 | 1970-01-01T00:00:04.242424242 |",
            "+--------+------+-------------------------------+",
        ]
    );

    // Assert that no dedupe operations are performed when querying a partition
    // that contains duplicate rows for a single series/primary key, but the
    // operations maintain their ordering (later writes appear after earlier
    // writes).
    test_write_query!(
        duplicate_writes,
        partitions = [PartitionDataBuilder::new()
            .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
            .build()],
        writes = [
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region=Asturias temp=35 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            ),
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                1,
                &format!(
                    r#"{},region=Asturias temp=12 4242424242"#,
                    &*ARBITRARY_TABLE_NAME
                ),
                None,
            )
        ],
        predicate = None,
        want = [
            "+----------+------+-------------------------------+",
            "| region   | temp | time                          |",
            "+----------+------+-------------------------------+",
            "| Asturias | 35.0 | 1970-01-01T00:00:04.242424242 |",
            "| Asturias | 12.0 | 1970-01-01T00:00:04.242424242 |",
            "+----------+------+-------------------------------+",
        ]
    );

    // This test asserts that the results returned from a query to the
    // [`BufferTree`] filters rows from the result as directed by the
    // query's [`Predicate`].
    //
    // It makes sure that for a [`BufferTree`] with a set of partitions split
    // by some key a query with a predicate `<partition key column> == <arbitrary literal>`
    // returns partition data that has been filtered to contain only rows which
    // contain the specified value in that partition key column.
    test_write_query!(
        filter_by_predicate_partition_key,
        table_provider = Arc::new(MockTableProvider::new(TableMetadata::new_for_testing(
            ARBITRARY_TABLE_NAME.clone(),
            test_table_partition_override(vec![TemplatePart::TagValue("region")])
        ))),
        partitions = [
            PartitionDataBuilder::new()
                .with_partition_key(ARBITRARY_PARTITION_KEY.clone()) // "platanos"
                .build(),
            PartitionDataBuilder::new()
                .with_partition_key(PARTITION2_KEY.clone()) // "p2"
                .build()
        ],
        writes = [
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                0,
                &format!(
                    r#"{},region={} temp=35 4242424242"#,
                    &*ARBITRARY_TABLE_NAME, &*ARBITRARY_PARTITION_KEY
                ),
                None,
            ),
            make_write_op(
                &ARBITRARY_PARTITION_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                1,
                &format!(
                    r#"{},region={} temp=12 4242424242"#,
                    &*ARBITRARY_TABLE_NAME, &*ARBITRARY_PARTITION_KEY
                ),
                None,
            ),
            make_write_op(
                &PARTITION2_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                2,
                &format!(
                    r#"{},region={} temp=17 7676767676"#,
                    &*ARBITRARY_TABLE_NAME, *PARTITION2_KEY
                ),
                None,
            ),
            make_write_op(
                &PARTITION2_KEY,
                ARBITRARY_NAMESPACE_ID,
                &ARBITRARY_TABLE_NAME,
                ARBITRARY_TABLE_ID,
                3,
                &format!(
                    r#"{},region={} temp=13 7676767676"#,
                    &*ARBITRARY_TABLE_NAME, *PARTITION2_KEY,
                ),
                None,
            )
        ],
        // NOTE: The querier will coerce the type of the predicates correctly, so the ingester does NOT need to perform
        //       type coercion. This type should reflect that.
        predicate = Some(Predicate::new().with_expr(col("region").eq(lit(
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from(PARTITION2_KEY.inner()))
            )
        )))),
        want = [
            "+--------+------+-------------------------------+",
            "| region | temp | time                          |",
            "+--------+------+-------------------------------+",
            "| p2     | 13.0 | 1970-01-01T00:00:07.676767676 |",
            "| p2     | 17.0 | 1970-01-01T00:00:07.676767676 |",
            "+--------+------+-------------------------------+",
        ]
    );

    /// Ensure partition pruning during query execution also prunes metadata
    /// frames.
    ///
    /// Individual frames are fast to serialise, but large numbers of frames can
    /// add significant query overhead, particularly for queries returning small
    /// numbers of rows where the metadata becomes a significant portion of the
    /// response.
    #[tokio::test]
    async fn test_partition_metadata_pruning() {
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key("madrid".into())
                        .build(),
                )
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key("asturias".into())
                        .build(),
                ),
        );

        // Construct a partition template suitable for pruning on the "region"
        // tag.
        let table_provider = Arc::new(MockTableProvider::new(TableMetadata::new_for_testing(
            ARBITRARY_TABLE_NAME.clone(),
            test_table_partition_override(vec![TemplatePart::TagValue("region")]),
        )));

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            table_provider,
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
        );

        // Write to two regions
        buf.apply(IngestOp::Write(make_write_op(
            &PartitionKey::from("madrid"),
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=madrid temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to perform write");

        buf.apply(IngestOp::Write(make_write_op(
            &PartitionKey::from("asturias"),
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to perform write");

        // Construct a predicate suitable for pruning partitions based on the
        // region / partition template.
        let predicate = Some(Predicate::new().with_expr(col("region").eq(lit(
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::from("asturias")),
            ),
        ))));

        // Execute the query and count the number of partitions that are
        // returned (either data, or metadata).
        let partition_count = buf
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                predicate,
            )
            .await
            .expect("query should succeed")
            .into_partition_stream()
            .count()
            .await;

        // Because the data in the "madrid" partition was pruned out, the
        // metadata should not be sent either.
        assert_eq!(partition_count, 1);
    }

    /// Assert that multiple writes to a single namespace/table results in a
    /// single namespace being created, and matching metrics.
    #[tokio::test]
    async fn test_metrics() {
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                        .build(),
                )
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(PARTITION2_KEY.clone())
                        .build(),
                ),
        );

        let metrics = Arc::new(metric::Registry::default());

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::clone(&metrics),
        );

        // Write data to the arbitrary partition, in the arbitrary table
        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write initial data");

        // Write a duplicate record with the same series key & timestamp, but a
        // different temp value.
        buf.apply(IngestOp::Write(make_write_op(
            &PARTITION2_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            1,
            &format!(
                r#"{},region=Asturias temp=12 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
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

    #[tokio::test]
    async fn test_partition_iter() {
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                        .build(),
                )
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(PARTITION2_KEY.clone())
                        .build(),
                )
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(PARTITION3_KEY.clone())
                        .with_table_id(TABLE2_ID)
                        .with_table_loader(Arc::new(DeferredLoad::new(
                            Duration::from_secs(1),
                            async move {
                                TableMetadata::new_for_testing(
                                    TABLE2_NAME.into(),
                                    Default::default(),
                                )
                            },
                            &metric::Registry::default(),
                        )))
                        .build(),
                ),
        );

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::clone(&Arc::new(metric::Registry::default())),
        );

        assert_eq!(buf.partitions().count(), 0);

        // Write data to the arbitrary partition, in the arbitrary table
        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write initial data");

        assert_eq!(buf.partitions().count(), 1);

        // Write data to partition2, in the arbitrary table
        buf.apply(IngestOp::Write(make_write_op(
            &PARTITION2_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write initial data");

        assert_eq!(buf.partitions().count(), 2);

        // Write data to partition3, in the second table
        buf.apply(IngestOp::Write(make_write_op(
            &PARTITION3_KEY,
            ARBITRARY_NAMESPACE_ID,
            TABLE2_NAME,
            TABLE2_ID,
            0,
            &format!(r#"{},region=Asturias temp=35 4242424242"#, TABLE2_NAME),
            None,
        )))
        .await
        .expect("failed to write initial data");

        // Iterate over the partitions and ensure they were all visible.
        let mut ids = buf
            .partitions()
            .map(|p| p.lock().partition_id().clone())
            .collect::<Vec<_>>();
        ids.sort_unstable();

        let mut expected = [
            ARBITRARY_TRANSITION_PARTITION_ID.clone(),
            TransitionPartitionId::new(ARBITRARY_TABLE_ID, &PARTITION2_KEY),
            TransitionPartitionId::new(TABLE2_ID, &PARTITION3_KEY),
        ];
        expected.sort_unstable();

        assert_eq!(ids, expected);
    }

    /// Assert the correct "not found" errors are generated for missing
    /// table/namespaces, and that querying an entirely empty buffer tree
    /// returns no data (as opposed to panicking, etc).
    #[tokio::test]
    async fn test_not_found() {
        let partition_provider = Arc::new(
            MockPartitionProvider::default().with_partition(
                PartitionDataBuilder::new()
                    .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                    .build(),
            ),
        );

        // Init the BufferTree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
        );

        // Query the empty tree
        let err = buf
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect_err("query should fail");
        assert_matches!(err, QueryError::NamespaceNotFound(ns) => {
            assert_eq!(ns, ARBITRARY_NAMESPACE_ID);
        });

        // Write data to the arbitrary partition, in the arbitrary table
        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Asturias temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write data");

        // Ensure an unknown table errors
        let err = buf
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                TABLE2_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect_err("query should fail");
        assert_matches!(err, QueryError::TableNotFound(ns, t) => {
            assert_eq!(ns, ARBITRARY_NAMESPACE_ID);
            assert_eq!(t, TABLE2_ID);
        });

        // Ensure a valid namespace / table does not error
        buf.query_exec(
            ARBITRARY_NAMESPACE_ID,
            ARBITRARY_TABLE_ID,
            OwnedProjection::default(),
            None,
            None,
        )
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
        // Configure the mock partition provider to return two partitions.
        let partition_provider = Arc::new(
            MockPartitionProvider::default()
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                        .build(),
                )
                .with_partition(
                    PartitionDataBuilder::new()
                        .with_partition_key(PARTITION2_KEY.clone())
                        .build(),
                ),
        );

        // Init the buffer tree
        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
        );

        // Write data to the arbitrary partition, in the arbitrary table
        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Madrid temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write initial data");

        // Execute a query of the buffer tree, generating the result stream, but
        // DO NOT consume it.
        let stream = buf
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect("query should succeed")
            .into_partition_stream();

        // Perform a write concurrent to the consumption of the query stream
        // that creates a new partition2 in the same table.
        buf.apply(IngestOp::Write(make_write_op(
            &PARTITION2_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            1,
            &format!(
                r#"{},region=Asturias temp=20 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to perform concurrent write to new partition");

        // Perform another write that hits the arbitrary partition within the query
        // results snapshot before the partition is read.
        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            2,
            &format!(
                r#"{},region=Murcia temp=30 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to perform concurrent write to existing partition");

        // Consume the set of partitions within the query stream.
        //
        // Under the specified query consistency guarantees, both the first and
        // third writes (both to the arbitrary partition) should be visible. The second write to
        // partition2 should not be visible.
        let mut partitions: Vec<PartitionResponse> = stream.collect().await;
        assert_eq!(partitions.len(), 1); // only p1, not p2
        let partition = partitions.pop().unwrap();

        // Ensure the partition hash ID is sent.
        assert_eq!(partition.id(), &*ARBITRARY_TRANSITION_PARTITION_ID);

        // Perform the partition read
        let batches = partition.into_record_batches();

        // Assert the contents of the arbitrary partition contains both the initial write, and the
        // 3rd write in a single RecordBatch.
        assert_batches_eq!(
            [
                "+--------+------+-------------------------------+",
                "| region | temp | time                          |",
                "+--------+------+-------------------------------+",
                "| Madrid | 35.0 | 1970-01-01T00:00:04.242424242 |",
                "| Murcia | 30.0 | 1970-01-01T00:00:04.242424242 |",
                "+--------+------+-------------------------------+",
            ],
            &batches
        );
    }

    // If the catalog doesn't have a PartitionHashId as represented by the PartitionProvider, don't
    // send it to the querier.
    #[tokio::test]
    async fn dont_send_partition_hash_id_when_not_in_catalog() {
        let partition_provider = Arc::new(
            MockPartitionProvider::default().with_partition(
                PartitionDataBuilder::new()
                    .with_deprecated_partition_id(ARBITRARY_CATALOG_PARTITION_ID)
                    .with_partition_key(ARBITRARY_PARTITION_KEY.clone())
                    .build(),
            ),
        );

        let buf = BufferTree::new(
            Arc::new(MockNamespaceNameProvider::new(&**ARBITRARY_NAMESPACE_NAME)),
            Arc::clone(&*ARBITRARY_TABLE_PROVIDER),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
            Arc::new(metric::Registry::default()),
        );

        buf.apply(IngestOp::Write(make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            0,
            &format!(
                r#"{},region=Madrid temp=35 4242424242"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        )))
        .await
        .expect("failed to write initial data");

        let stream = buf
            .query_exec(
                ARBITRARY_NAMESPACE_ID,
                ARBITRARY_TABLE_ID,
                OwnedProjection::default(),
                None,
                None,
            )
            .await
            .expect("query should succeed")
            .into_partition_stream();

        let mut partitions: Vec<PartitionResponse> = stream.collect().await;
        let partition = partitions.pop().unwrap();

        // Ensure the partition hash ID is NOT sent.
        assert_eq!(
            partition.id(),
            &TransitionPartitionId::Deprecated(ARBITRARY_CATALOG_PARTITION_ID),
        );
    }
}
