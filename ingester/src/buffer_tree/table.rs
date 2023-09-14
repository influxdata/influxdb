//! Table level data buffer structures.

pub(crate) mod metadata;
pub(crate) mod metadata_resolver;

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{
    partition_template::{build_column_values, ColumnValue, TablePartitionTemplateOverride},
    NamespaceId, PartitionKey, SequenceNumber, TableId,
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

use self::metadata::TableMetadata;

use super::{
    namespace::NamespaceName,
    partition::{counter::PartitionCounter, resolver::PartitionProvider, PartitionData},
    post_write::PostWriteObserver,
    BufferWriteError,
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

const MAX_NAMESPACE_PARTITION_COUNT: usize = usize::MAX;

/// Data of a Table in a given Namespace
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

    /// A counter tracking the approximate number of partitions currently
    /// buffered.
    ///
    /// This counter is NOT atomically incremented w.r.t creation of the
    /// partitions it tracks, and therefore is susceptible to "overrun",
    /// breaching the configured partition count limit by a relatively small
    /// degree.
    partition_count: Arc<PartitionCounter>,

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
        partition_count: Arc<PartitionCounter>,
        post_write_observer: Arc<O>,
    ) -> Self {
        Self {
            table_id,
            catalog_table,
            namespace_id,
            namespace_name,
            partition_data: Default::default(),
            partition_provider,
            partition_count,
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
    ) -> Result<(), BufferWriteError> {
        let p = self.partition_data.get(&partition_key);
        let partition_data = match p {
            Some(p) => p,
            None if self.partition_count.is_maxed() => {
                // This namespace has exceeded the upper bound on partitions.
                //
                // This counter is approximate, but monotonic - the count may be
                // over the desired limit.
                return Err(BufferWriteError::PartitionLimit {
                    count: self.partition_count.read(),
                });
            }
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
                self.partition_data.get_or_insert_with(&partition_key, || {
                    self.partition_count.inc();
                    p
                })
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

        let table_partition_template = self.catalog_table.get().await.partition_template().clone();
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
                    ColumnValue::Datetime { .. } => {
                        // not yet supported
                        return None;
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
    use std::{num::NonZeroUsize, sync::Arc};

    use assert_matches::assert_matches;
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
        let partition_provider =
            Arc::new(MockPartitionProvider::default().with_partition(PartitionDataBuilder::new()));

        let partition_counter = Arc::new(PartitionCounter::new(NonZeroUsize::new(42).unwrap()));

        let table = TableData::new(
            ARBITRARY_TABLE_ID,
            defer_table_metadata_1_sec(),
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_sec(),
            partition_provider,
            Arc::clone(&partition_counter),
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

        // The partition should have been recorded in the partition count.
        assert_eq!(partition_counter.read(), 1);
    }

    /// Ensure the partition limit is respected.
    #[tokio::test]
    async fn test_partition_limit() {
        // Configure the mock partition provider to return a partition for a table ID.
        let partition_provider =
            Arc::new(MockPartitionProvider::default().with_partition(PartitionDataBuilder::new()));

        const N: usize = 42;

        // Configure the counter that has already exceeded the maximum limit.
        let partition_counter = Arc::new(PartitionCounter::new(NonZeroUsize::new(N).unwrap()));
        partition_counter.set(N);

        let table = TableData::new(
            ARBITRARY_TABLE_ID,
            defer_table_metadata_1_sec(),
            ARBITRARY_NAMESPACE_ID,
            defer_namespace_name_1_sec(),
            partition_provider,
            Arc::clone(&partition_counter),
            Arc::new(MockPostWriteObserver::default()),
        );

        let batch = lines_to_batches(
            &format!(r#"{},bat=man value=24 42"#, &*ARBITRARY_TABLE_NAME),
            0,
        )
        .unwrap()
        .remove(&***ARBITRARY_TABLE_NAME)
        .unwrap();

        // Write some test data
        let err = table
            .buffer_table_write(
                SequenceNumber::new(42),
                batch,
                ARBITRARY_PARTITION_KEY.clone(),
            )
            .await
            .expect_err("buffer op should hit partition limit");

        assert_matches!(err, BufferWriteError::PartitionLimit { count: N });

        // The partition should not have been created
        assert_eq!(table.partition_data.values().len(), 0);

        // The partition counter should be unchanged
        assert_eq!(partition_counter.read(), N);
    }
}
