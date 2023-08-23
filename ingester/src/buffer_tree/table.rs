//! Table level data buffer structures.

pub(crate) mod metadata_resolver;

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{
    partition_template::{build_column_values, ColumnValue, TablePartitionTemplateOverride},
    NamespaceId, PartitionKey, SequenceNumber, Table, TableId,
};
use datafusion::{prelude::Expr, scalar::ScalarValue};
use iox_query::{
    chunk_statistics::{create_chunk_statistics, ColumnRange},
    pruning::prune_summaries,
    QueryChunk,
};
use mutable_batch::MutableBatch;
use parking_lot::Mutex;
use predicate::Predicate;
use trace::span::{Span, SpanRecorder};

use super::{
    namespace::NamespaceName,
    partition::{resolver::PartitionProvider, PartitionData},
    post_write::PostWriteObserver,
};
use crate::{
    arcmap::ArcMap,
    deferred_load::DeferredLoad,
    query::{
        partition_response::PartitionResponse, projection::OwnedProjection,
        response::PartitionStream, QueryError, QueryExec,
    },
    query_adaptor::QueryAdaptor,
};

/// Metadata from the catalog for a table
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableMetadata {
    name: TableName,
    partition_template: TablePartitionTemplateOverride,
}

impl TableMetadata {
    #[cfg(test)]
    pub fn new_for_testing(
        name: TableName,
        partition_template: TablePartitionTemplateOverride,
    ) -> Self {
        Self {
            name,
            partition_template,
        }
    }

    pub(crate) fn name(&self) -> &TableName {
        &self.name
    }

    pub(crate) fn partition_template(&self) -> &TablePartitionTemplateOverride {
        &self.partition_template
    }
}

impl From<Table> for TableMetadata {
    fn from(t: Table) -> Self {
        Self {
            name: t.name.into(),
            partition_template: t.partition_template,
        }
    }
}

impl std::fmt::Display for TableMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.name, f)
    }
}

/// The string name / identifier of a Table.
///
/// A reference-counted, cheap clone-able string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TableName(Arc<str>);

impl<T> From<T> for TableName
where
    T: AsRef<str>,
{
    fn from(v: T) -> Self {
        Self(Arc::from(v.as_ref()))
    }
}

impl From<TableName> for Arc<str> {
    fn from(v: TableName) -> Self {
        v.0
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for TableName {
    type Target = Arc<str>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        &*self.0 == other
    }
}

/// Data of a Table in a given Namesapce
#[derive(Debug)]
pub(crate) struct TableData<O> {
    table_id: TableId,
    catalog_table: Arc<DeferredLoad<TableMetadata>>,

    /// The catalog ID of the namespace this table is being populated from.
    namespace_id: NamespaceId,
    namespace_name: Arc<DeferredLoad<NamespaceName>>,

    /// An abstract constructor of [`PartitionData`] instances for a given
    /// `(key, table)` tuple.
    partition_provider: Arc<dyn PartitionProvider>,

    // Map of partition key to its data
    partition_data: ArcMap<PartitionKey, Mutex<PartitionData>>,

    post_write_observer: Arc<O>,
}

impl<O> TableData<O> {
    /// Initialize new table buffer identified by [`TableId`] in the catalog.
    ///
    /// The partition provider is used to instantiate a [`PartitionData`]
    /// instance when this [`TableData`] instance observes an op for a partition
    /// for the first time.
    pub(super) fn new(
        table_id: TableId,
        catalog_table: Arc<DeferredLoad<TableMetadata>>,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        partition_provider: Arc<dyn PartitionProvider>,
        post_write_observer: Arc<O>,
    ) -> Self {
        Self {
            table_id,
            catalog_table,
            namespace_id,
            namespace_name,
            partition_data: Default::default(),
            partition_provider,
            post_write_observer,
        }
    }

    /// Return a mutable reference to all partitions buffered for this table.
    ///
    /// # Ordering
    ///
    /// The order of [`PartitionData`] in the iterator is arbitrary and should
    /// not be relied upon.
    ///
    /// # Snapshot
    ///
    /// The set of [`PartitionData`] returned is an atomic / point-in-time
    /// snapshot of the set of [`PartitionData`] at the time this function is
    /// invoked, but the data within them may change as they continue to buffer
    /// DML operations.
    pub(crate) fn partitions(&self) -> Vec<Arc<Mutex<PartitionData>>> {
        self.partition_data.values()
    }

    /// Returns the table ID for this partition.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the catalog data for this table.
    pub(crate) fn catalog_table(&self) -> &Arc<DeferredLoad<TableMetadata>> {
        &self.catalog_table
    }

    /// Return the [`NamespaceId`] this table is a part of.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

impl<O> TableData<O>
where
    O: PostWriteObserver,
{
    // buffers the table write and returns true if the lifecycle manager indicates that
    // ingest should be paused.
    pub(super) async fn buffer_table_write(
        &self,
        sequence_number: SequenceNumber,
        batch: MutableBatch,
        partition_key: PartitionKey,
    ) -> Result<(), mutable_batch::Error> {
        let p = self.partition_data.get(&partition_key);
        let partition_data = match p {
            Some(p) => p,
            None => {
                let p = self
                    .partition_provider
                    .get_partition(
                        partition_key.clone(),
                        self.namespace_id,
                        Arc::clone(&self.namespace_name),
                        self.table_id,
                        Arc::clone(&self.catalog_table),
                    )
                    .await;
                // Add the partition to the map.
                //
                // This MAY return a different instance than `p` if another
                // thread has already initialised the partition.
                self.partition_data.get_or_insert_with(&partition_key, || p)
            }
        };

        // Obtain the partition lock.
        let mut p = partition_data.lock();

        // Enqueue the write, returning any error.
        p.buffer_write(batch, sequence_number)?;

        // If successful, allow the observer to inspect the partition.
        self.post_write_observer
            .observe(Arc::clone(&partition_data), p);

        Ok(())
    }
}

#[async_trait]
impl<O> QueryExec for TableData<O>
where
    O: Send + Sync + Debug,
{
    type Response = PartitionStream;

    async fn query_exec(
        &self,
        namespace_id: NamespaceId,
        table_id: TableId,
        projection: OwnedProjection,
        span: Option<Span>,
        predicate: Option<Predicate>,
    ) -> Result<Self::Response, QueryError> {
        assert_eq!(self.table_id, table_id, "buffer tree index inconsistency");
        assert_eq!(
            self.namespace_id, namespace_id,
            "buffer tree index inconsistency"
        );

        let table_partition_template = self.catalog_table.get().await.partition_template;
        let filters = predicate
            .map(|p| p.filter_expr().into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        // Gather the partition data from all of the partitions in this table.
        let span = SpanRecorder::new(span);
        let partitions = self.partitions().into_iter().filter_map(move |p| {
            let mut span = span.child("partition read");

            let (id, completed_persistence_count, data, partition_key) = {
                let mut p = p.lock();
                (
                    p.partition_id().clone(),
                    p.completed_persistence_count(),
                    p.get_query_data(&projection),
                    p.partition_key().clone(),
                )
            };

            let ret = match data {
                Some(data) => {
                    assert_eq!(&id, data.partition_id());

                    // Potentially prune out this partition if the partition
                    // template & derived partition key can be used to match
                    // against the filters.
                    if !keep_after_pruning_partition_key(
                        &table_partition_template,
                        &partition_key,
                        &filters,
                        &data,
                    ) {
                        // This partition will never contain any data that would
                        // form part of the query response.
                        //
                        // Because this is true of buffered data, it is also
                        // true of the persisted data, and therefore sending the
                        // persisted file count metadata is useless because the
                        // querier would never utilise the persisted files as
                        // part of this query.
                        //
                        // This avoids sending O(n) metadata frames for queries
                        // that may only touch one or two actual frames. The N
                        // partition count grows over the lifetime of the
                        // ingester as more partitions are created, and while
                        // fast to serialise individually, the sequentially-sent
                        // N metadata frames add up.
                        return None;
                    }

                    PartitionResponse::new(
                        data.into_record_batches(),
                        id,
                        completed_persistence_count,
                    )
                }
                None => PartitionResponse::new(vec![], id, completed_persistence_count),
            };

            span.ok("read partition data");
            Some(ret)
        });

        Ok(PartitionStream::new(futures::stream::iter(partitions)))
    }
}

/// Return true if `data` contains one or more rows matching `predicate`,
/// pruning based on the `partition_key` and `template`.
///
/// Returns false iff it can be proven that all of data does not match the
/// predicate.
fn keep_after_pruning_partition_key(
    table_partition_template: &TablePartitionTemplateOverride,
    partition_key: &PartitionKey,
    filters: &[Expr],
    data: &QueryAdaptor,
) -> bool {
    // Construct a set of per-column min/max statistics based on the partition
    // key values.
    let column_ranges = Arc::new(
        build_column_values(table_partition_template, partition_key.inner())
            .filter_map(|(col, val)| {
                let range = match val {
                    ColumnValue::Identity(s) => {
                        let s = Arc::new(ScalarValue::from(s.as_ref()));
                        ColumnRange {
                            min_value: Arc::clone(&s),
                            max_value: s,
                        }
                    }
                    ColumnValue::Prefix(p) if p.is_empty() => return None,
                    ColumnValue::Prefix(p) => {
                        // If the partition only has a prefix of the tag value
                        // (it was truncated) then form a conservative range:
                        //
                        // # Minimum
                        // Use the prefix itself.
                        //
                        // Note that the minimum is inclusive.
                        //
                        // All values in the partition are either:
                        //
                        // - identical to the prefix, in which case they are
                        //   included by the inclusive minimum
                        //
                        // - have the form `"<prefix><s>"`, and it holds that
                        //   `"<prefix><s>" > "<prefix>"` for all strings
                        //   `"<s>"`.
                        //
                        // # Maximum
                        // Use `"<prefix_excluding_last_char><char::max>"`.
                        //
                        // Note that the maximum is inclusive.
                        //
                        // All strings in this partition must be smaller than
                        // this constructed maximum, because string comparison
                        // is front-to-back and the
                        // `"<prefix_excluding_last_char><char::max>" >
                        // "<prefix>"`.

                        let min_value = Arc::new(ScalarValue::from(p.as_ref()));

                        let mut chars = p.as_ref().chars().collect::<Vec<_>>();
                        *chars.last_mut().expect("checked that prefix is not empty") =
                            std::char::MAX;
                        let max_value = Arc::new(ScalarValue::from(
                            chars.into_iter().collect::<String>().as_str(),
                        ));

                        ColumnRange {
                            min_value,
                            max_value,
                        }
                    }
                };

                Some((Arc::from(col), range))
            })
            .collect::<HashMap<_, _>>(),
    );

    let chunk_statistics = Arc::new(create_chunk_statistics(
        data.num_rows(),
        data.schema(),
        data.ts_min_max(),
        &column_ranges,
    ));

    prune_summaries(
        data.schema(),
        &[(chunk_statistics, data.schema().as_arrow())],
        filters,
    )
    // Errors are logged by `iox_query` and sometimes fine, e.g. for not
    // implemented DataFusion features or upstream bugs. The querier uses the
    // same strategy. Pruning is a mere optimization and should not lead to
    // crashes or unreadable data.
    .ok()
    .map(|vals| {
        vals.into_iter()
            .next()
            .expect("one chunk in, one chunk out")
    })
    .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mutable_batch_lp::lines_to_batches;

    use super::*;
    use crate::{
        buffer_tree::{
            partition::resolver::mock::MockPartitionProvider,
            post_write::mock::MockPostWriteObserver,
        },
        test_util::{
            defer_namespace_name_1_sec, defer_table_metadata_1_sec, PartitionDataBuilder,
            ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID,
            ARBITRARY_TABLE_NAME,
        },
    };

    #[tokio::test]
    async fn test_partition_init() {
        // Configure the mock partition provider to return a partition for a table ID.
        let partition_provider = Arc::new(
            MockPartitionProvider::default().with_partition(PartitionDataBuilder::new().build()),
        );

        let table = TableData::new(
            ARBITRARY_TABLE_ID,
            defer_table_metadata_1_sec(),
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_sec(),
            partition_provider,
            Arc::new(MockPostWriteObserver::default()),
        );

        let batch = lines_to_batches(
            &format!(r#"{},bat=man value=24 42"#, &*ARBITRARY_TABLE_NAME),
            0,
        )
        .unwrap()
        .remove(&***ARBITRARY_TABLE_NAME)
        .unwrap();

        // Assert the table does not contain the test partition
        assert!(table.partition_data.get(&ARBITRARY_PARTITION_KEY).is_none());

        // Write some test data
        table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                ARBITRARY_PARTITION_KEY.clone(),
            )
            .await
            .expect("buffer op should succeed");

        // Referencing the partition should succeed
        assert!(table.partition_data.get(&ARBITRARY_PARTITION_KEY).is_some());
    }
}
