//! Namespace level data buffer structures.

pub(crate) mod name_resolver;

use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceId, ShardId, TableId};
use dml::DmlOperation;
use metric::U64Counter;
use observability_deps::tracing::warn;
use trace::span::Span;

use super::{
    partition::resolver::PartitionProvider,
    post_write::PostWriteObserver,
    table::{name_resolver::TableNameProvider, TableData},
};
use crate::{
    arcmap::ArcMap,
    deferred_load::DeferredLoad,
    dml_sink::DmlSink,
    query::{response::QueryResponse, tracing::QueryExecTracing, QueryError, QueryExec},
};

/// The string name / identifier of a Namespace.
///
/// A reference-counted, cheap clone-able string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NamespaceName(Arc<str>);

impl<T> From<T> for NamespaceName
where
    T: AsRef<str>,
{
    fn from(v: T) -> Self {
        Self(Arc::from(v.as_ref()))
    }
}

impl std::ops::Deref for NamespaceName {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Data of a Namespace that belongs to a given Shard
#[derive(Debug)]
pub(crate) struct NamespaceData<O> {
    namespace_id: NamespaceId,
    namespace_name: Arc<DeferredLoad<NamespaceName>>,

    /// A set of tables this [`NamespaceData`] instance has processed
    /// [`DmlOperation`]'s for.
    ///
    /// The [`TableNameProvider`] acts as a [`DeferredLoad`] constructor to
    /// resolve the [`TableName`] for new [`TableData`] out of the hot path.
    ///
    /// [`TableName`]: crate::buffer_tree::table::TableName
    tables: ArcMap<TableId, TableData<O>>,
    table_name_resolver: Arc<dyn TableNameProvider>,
    /// The count of tables initialised in this Ingester so far, across all
    /// namespaces.
    table_count: U64Counter,

    /// The resolver of `(table_id, partition_key)` to [`PartitionData`].
    ///
    /// [`PartitionData`]: super::partition::PartitionData
    partition_provider: Arc<dyn PartitionProvider>,

    post_write_observer: Arc<O>,

    transition_shard_id: ShardId,
}

impl<O> NamespaceData<O> {
    /// Initialize new tables with default partition template of daily
    pub(super) fn new(
        namespace_id: NamespaceId,
        namespace_name: DeferredLoad<NamespaceName>,
        table_name_resolver: Arc<dyn TableNameProvider>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
        metrics: &metric::Registry,
        transition_shard_id: ShardId,
    ) -> Self {
        let table_count = metrics
            .register_metric::<U64Counter>(
                "ingester_tables",
                "Number of tables known to the ingester",
            )
            .recorder(&[]);

        Self {
            namespace_id,
            namespace_name: Arc::new(namespace_name),
            tables: Default::default(),
            table_name_resolver,
            table_count,
            partition_provider,
            post_write_observer,
            transition_shard_id,
        }
    }

    /// Return the table data by ID.
    pub(crate) fn table(&self, table_id: TableId) -> Option<Arc<TableData<O>>> {
        self.tables.get(&table_id)
    }

    /// Return the [`NamespaceId`] this [`NamespaceData`] belongs to.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Returns the [`NamespaceName`] for this namespace.
    pub(crate) fn namespace_name(&self) -> &DeferredLoad<NamespaceName> {
        &self.namespace_name
    }

    /// Obtain a snapshot of the tables within this [`NamespaceData`].
    ///
    /// NOTE: the snapshot is an atomic / point-in-time snapshot of the set of
    /// [`NamespaceData`], but the tables (and partitions) within them may
    /// change as they continue to buffer DML operations.
    pub(super) fn tables(&self) -> Vec<Arc<TableData<O>>> {
        self.tables.values()
    }
}

#[async_trait]
impl<O> DmlSink for NamespaceData<O>
where
    O: PostWriteObserver,
{
    type Error = mutable_batch::Error;

    async fn apply(&self, op: DmlOperation) -> Result<(), Self::Error> {
        let sequence_number = op
            .meta()
            .sequence()
            .expect("applying unsequenced op")
            .sequence_number;

        match op {
            DmlOperation::Write(write) => {
                // Extract the partition key derived by the router.
                let partition_key = write.partition_key().clone();

                for (table_id, b) in write.into_tables() {
                    // Grab a reference to the table data, or insert a new
                    // TableData for it.
                    let table_data = self.tables.get_or_insert_with(&table_id, || {
                        self.table_count.inc(1);
                        Arc::new(TableData::new(
                            table_id,
                            self.table_name_resolver.for_table(table_id),
                            self.namespace_id,
                            Arc::clone(&self.namespace_name),
                            Arc::clone(&self.partition_provider),
                            Arc::clone(&self.post_write_observer),
                            self.transition_shard_id,
                        ))
                    });

                    table_data
                        .buffer_table_write(sequence_number, b, partition_key.clone())
                        .await?;
                }
            }
            DmlOperation::Delete(delete) => {
                // Deprecated delete support:
                // https://github.com/influxdata/influxdb_iox/issues/5825
                warn!(
                    namespace_name=%self.namespace_name,
                    namespace_id=%self.namespace_id,
                    table_name=?delete.table_name(),
                    sequence_number=?delete.meta().sequence(),
                    "discarding unsupported delete op"
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<O> QueryExec for NamespaceData<O>
where
    O: Send + Sync + std::fmt::Debug,
{
    type Response = QueryResponse;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        span: Option<Span>,
    ) -> Result<Self::Response, QueryError> {
        assert_eq!(
            self.namespace_id, namespace_id,
            "buffer tree index inconsistency"
        );

        // Extract the table if it exists.
        let inner = self
            .table(table_id)
            .ok_or(QueryError::TableNotFound(namespace_id, table_id))?;

        // Delegate query execution to the namespace, wrapping the execution in
        // a tracing delegate to emit a child span.
        Ok(QueryResponse::new(
            QueryExecTracing::new(inner, "table")
                .query_exec(namespace_id, table_id, columns, span)
                .await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use data_types::{PartitionId, PartitionKey, ShardId};
    use metric::{Attributes, Metric};

    use super::*;
    use crate::{
        buffer_tree::{
            namespace::NamespaceData,
            partition::{resolver::mock::MockPartitionProvider, PartitionData, SortKeyState},
            post_write::mock::MockPostWriteObserver,
            table::{name_resolver::mock::MockTableNameProvider, TableName},
        },
        deferred_load::{self, DeferredLoad},
        test_util::make_write_op,
    };

    const TABLE_NAME: &str = "bananas";
    const TABLE_ID: TableId = TableId::new(44);
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
            r#"bananas,city=Medford day="sun",temp=55 22"#,
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
}
