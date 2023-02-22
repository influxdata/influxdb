//! Partition level data buffer structures.

use std::{collections::VecDeque, sync::Arc};

use data_types::{
    sequence_number_set::SequenceNumberSet, NamespaceId, PartitionId, PartitionKey, SequenceNumber,
    ShardId, TableId,
};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use schema::sort::SortKey;

use self::{
    buffer::{traits::Queryable, BufferState, DataBuffer, Persisting},
    persisting::{BatchIdent, PersistingData},
};
use super::{namespace::NamespaceName, table::TableName};
use crate::{deferred_load::DeferredLoad, query_adaptor::QueryAdaptor};

mod buffer;
pub(crate) mod persisting;
pub(crate) mod resolver;

/// The load state of the [`SortKey`] for a given partition.
#[derive(Debug, Clone)]
pub(crate) enum SortKeyState {
    /// The [`SortKey`] has not yet been fetched from the catalog, and will be
    /// lazy loaded (or loaded in the background) by a call to
    /// [`DeferredLoad::get()`].
    Deferred(Arc<DeferredLoad<Option<SortKey>>>),
    /// The sort key is known and specified.
    Provided(Option<SortKey>),
}

impl SortKeyState {
    pub(crate) async fn get(&self) -> Option<SortKey> {
        match self {
            Self::Deferred(v) => v.get().await,
            Self::Provided(v) => v.clone(),
        }
    }
}

/// Data of an IOx Partition of a given Table of a Namespace that belongs to a
/// given Shard
#[derive(Debug)]
pub struct PartitionData {
    /// The catalog ID of the partition this buffer is for.
    partition_id: PartitionId,
    /// The string partition key for this partition.
    partition_key: PartitionKey,

    /// The sort key of this partition.
    ///
    /// This can known, in which case this field will contain a
    /// [`SortKeyState::Provided`] with the [`SortKey`], or unknown with a value
    /// of [`SortKeyState::Deferred`] causing it to be loaded from the catalog
    /// (potentially) in the background or at read time.
    ///
    /// Callers should use [`Self::sort_key()`] to be abstracted away from these
    /// fetch details.
    sort_key: SortKeyState,

    /// The namespace this partition is part of.
    namespace_id: NamespaceId,
    /// The name of the namespace this partition is part of, potentially
    /// unresolved / deferred.
    namespace_name: Arc<DeferredLoad<NamespaceName>>,

    /// The catalog ID for the table this partition is part of.
    table_id: TableId,
    /// The name of the table this partition is part of, potentially unresolved
    /// / deferred.
    table_name: Arc<DeferredLoad<TableName>>,

    /// A [`DataBuffer`] for incoming writes.
    buffer: DataBuffer,

    /// The currently persisting [`DataBuffer`] instances, if any.
    ///
    /// This queue is ordered from newest at the head, to oldest at the tail -
    /// forward iteration order matches write order.
    ///
    /// The [`BatchIdent`] is a generational counter that is used to tag each
    /// persisting with a unique, opaque identifier.
    persisting: VecDeque<(BatchIdent, BufferState<Persisting>)>,

    /// The number of persist operations started over the lifetime of this
    /// [`PartitionData`].
    started_persistence_count: BatchIdent,

    /// The number of persist operations completed over the lifetime of this
    /// [`PartitionData`].
    completed_persistence_count: u64,

    transition_shard_id: ShardId,
}

impl PartitionData {
    /// Initialize a new partition data buffer
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: PartitionId,
        partition_key: PartitionKey,
        namespace_id: NamespaceId,
        namespace_name: Arc<DeferredLoad<NamespaceName>>,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
        sort_key: SortKeyState,
        transition_shard_id: ShardId,
    ) -> Self {
        Self {
            partition_id: id,
            partition_key,
            sort_key,
            namespace_id,
            namespace_name,
            table_id,
            table_name,
            buffer: DataBuffer::default(),
            persisting: VecDeque::with_capacity(1),
            started_persistence_count: BatchIdent::default(),
            completed_persistence_count: 0,
            transition_shard_id,
        }
    }

    /// Buffer the given [`MutableBatch`] in memory.
    pub(crate) fn buffer_write(
        &mut self,
        mb: MutableBatch,
        sequence_number: SequenceNumber,
    ) -> Result<(), mutable_batch::Error> {
        // Buffer the write.
        self.buffer.buffer_write(mb, sequence_number)?;

        trace!(
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            "buffered write"
        );

        Ok(())
    }

    pub(crate) fn persist_cost_estimate(&self) -> usize {
        self.buffer.persist_cost_estimate()
    }

    /// Return all data for this partition, ordered by the calls to
    /// [`PartitionData::buffer_write()`].
    pub(crate) fn get_query_data(&mut self) -> Option<QueryAdaptor> {
        // Extract the buffered data, if any.
        let buffered_data = self.buffer.get_query_data();

        // Prepend any currently persisting batches.
        //
        // The persisting RecordBatch instances MUST be ordered before the
        // buffered data to preserve the ordering of writes such that updates to
        // existing rows materialise to the correct output.
        let data = self
            .persisting
            .iter()
            .flat_map(|(_, b)| b.get_query_data())
            .chain(buffered_data)
            .collect::<Vec<_>>();

        trace!(
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            n_batches = data.len(),
            "read partition data"
        );

        if data.is_empty() {
            return None;
        }

        // Construct the query adaptor over the partition data.
        //
        // `data` MUST contain at least one row, or the constructor panics. This
        // is upheld by the FSM, which ensures only non-empty snapshots /
        // RecordBatch are generated. Because `data` contains at least one
        // RecordBatch, this invariant holds.
        Some(QueryAdaptor::new(self.partition_id, data))
    }

    /// Snapshot and mark all buffered data as persisting.
    ///
    /// This method returns [`None`] if no data is buffered in [`Self`].
    ///
    /// A reference to the persisting data is retained until a corresponding
    /// call to [`Self::mark_persisted()`] is made to release it.
    ///
    /// Additionally each persistence MAY update the partition sort key, which
    /// is not a commutative operations, requiring partition persistence to be
    /// serialised (unless it can be known in advance no sort key update is
    /// necessary for a given persistence).
    pub(crate) fn mark_persisting(&mut self) -> Option<PersistingData> {
        let fsm = std::mem::take(&mut self.buffer).into_persisting()?;

        // From this point on, all code MUST be infallible or the buffered data
        // contained within persisting may be dropped.

        // Increment the "started persist" counter.
        //
        // This is used to cheaply identify batches given to the
        // mark_persisted() call.
        let batch_ident = self.started_persistence_count.next();

        debug!(
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            %batch_ident,
            "marking partition as persisting"
        );

        // Wrap the persisting data in the type wrapper
        let data = PersistingData::new(
            QueryAdaptor::new(self.partition_id, fsm.get_query_data()),
            batch_ident,
        );

        // Push the new buffer to the back of the persisting queue, so that
        // iterating from back to front during queries iterates over writes from
        // oldest to newest.
        self.persisting.push_back((batch_ident, fsm));

        Some(data)
    }

    /// Mark this partition as having completed persistence of the specified
    /// `batch`.
    ///
    /// All internal references to the data in `batch` are released.
    ///
    /// # Panics
    ///
    /// This method panics if [`Self`] is not marked as undergoing a persist
    /// operation, or `batch` is not currently being persisted.
    pub(crate) fn mark_persisted(&mut self, batch: PersistingData) -> SequenceNumberSet {
        // Find the batch in the persisting queue.
        let idx = self
            .persisting
            .iter()
            .position(|(old, _)| *old == batch.batch_ident())
            .expect("no currently persisting batch");

        // Remove the batch from the queue, preserving the order of the queue
        // for batch iteration during queries.
        let (old_ident, fsm) = self.persisting.remove(idx).unwrap();
        assert_eq!(old_ident, batch.batch_ident());

        self.completed_persistence_count += 1;

        debug!(
            batch_ident = %old_ident,
            persistence_count = %self.completed_persistence_count,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            batch_ident = %batch.batch_ident(),
            "marking partition persistence complete"
        );

        // Return the set of IDs this buffer contained.
        fsm.into_sequence_number_set()
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Return the count of persisted Parquet files for this [`PartitionData`] instance.
    pub(crate) fn completed_persistence_count(&self) -> u64 {
        self.completed_persistence_count
    }

    /// Return the name of the table this [`PartitionData`] is buffering writes
    /// for.
    pub(crate) fn table_name(&self) -> &Arc<DeferredLoad<TableName>> {
        &self.table_name
    }

    /// Return the table ID for this partition.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Return the partition key for this partition.
    pub(crate) fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the transition_shard_id for this partition.
    pub(crate) fn transition_shard_id(&self) -> ShardId {
        self.transition_shard_id
    }

    /// Return the [`NamespaceId`] this partition is a part of.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Return the [`NamespaceName`] this partition is a part of, potentially
    /// deferred / not yet resolved.
    ///
    /// NOTE: this MAY involve querying the catalog with unbounded retries.
    pub(crate) fn namespace_name(&self) -> &Arc<DeferredLoad<NamespaceName>> {
        &self.namespace_name
    }

    /// Return the [`SortKey`] for this partition.
    ///
    /// NOTE: this MAY involve querying the catalog with unbounded retries.
    pub(crate) fn sort_key(&self) -> &SortKeyState {
        &self.sort_key
    }

    /// Set the cached [`SortKey`] to the specified value.
    ///
    /// All subsequent calls to [`Self::sort_key`] will return
    /// [`SortKeyState::Provided`]  with the `new`.
    pub(crate) fn update_sort_key(&mut self, new: Option<SortKey>) {
        self.sort_key = SortKeyState::Provided(new);
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Deref, time::Duration};

    use arrow::compute::SortOptions;
    use arrow_util::assert_batches_eq;
    use assert_matches::assert_matches;
    use backoff::BackoffConfig;
    use data_types::ShardIndex;
    use datafusion::{
        physical_expr::PhysicalSortExpr,
        physical_plan::{expressions::col, memory::MemoryExec, ExecutionPlan},
    };
    use datafusion_util::test_collect;
    use iox_catalog::interface::Catalog;
    use lazy_static::lazy_static;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;

    use super::*;
    use crate::{buffer_tree::partition::resolver::SortKeyResolver, test_util::populate_catalog};

    const PARTITION_ID: PartitionId = PartitionId::new(1);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    lazy_static! {
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("platanos");
        static ref TABLE_NAME: TableName = TableName::from("bananas");
        static ref NAMESPACE_NAME: NamespaceName = NamespaceName::from("namespace-bananas");
    }

    // Write some data and read it back from the buffer.
    //
    // This ensures the sequence range, progress API, buffering, snapshot
    // generation & query all work as intended.
    #[tokio::test]
    async fn test_write_read() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // And no data should be returned when queried.
        assert!(p.get_query_data().is_none());

        // Perform a single write.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // The data should be readable.
        {
            let data = p.get_query_data().expect("should return data");
            assert_eq!(data.partition_id(), PARTITION_ID);

            let expected = [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2.0    | millions | 1970-01-01T00:00:00.000000010Z |",
                "+--------+--------+----------+--------------------------------+",
            ];
            assert_batches_eq!(
                expected,
                &*data
                    .record_batches()
                    .iter()
                    .map(Deref::deref)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }

        // Perform a another write, adding data to the existing queryable data
        // snapshot.
        let mb = lp_to_mutable_batch(r#"bananas,city=Madrid people=4,pigeons="none" 20"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        // And finally both writes should be readable.
        {
            let data = p.get_query_data().expect("should contain data");
            assert_eq!(data.partition_id(), PARTITION_ID);

            let expected = [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2.0    | millions | 1970-01-01T00:00:00.000000010Z |",
                "| Madrid | 4.0    | none     | 1970-01-01T00:00:00.000000020Z |",
                "+--------+--------+----------+--------------------------------+",
            ];
            assert_batches_eq!(
                expected,
                &*data
                    .record_batches()
                    .iter()
                    .map(Deref::deref)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }

    // Test persist operations against the partition, ensuring data is readable
    // both before, during, and after a persist takes place.
    #[tokio::test]
    async fn test_persist() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        assert!(p.get_query_data().is_none());

        // Perform a single write.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // Ensure the batch ident hasn't been increased yet.
        assert_eq!(p.started_persistence_count.get(), 0);
        assert_eq!(p.completed_persistence_count, 0);

        // Begin persisting the partition.
        let persisting_data = p.mark_persisting().expect("must contain existing data");
        // And validate the data being persisted.
        assert_eq!(persisting_data.partition_id(), PARTITION_ID);
        assert_eq!(persisting_data.record_batches().len(), 1);
        let expected = [
            "+--------+--------+----------+--------------------------------+",
            "| city   | people | pigeons  | time                           |",
            "+--------+--------+----------+--------------------------------+",
            "| London | 2.0    | millions | 1970-01-01T00:00:00.000000010Z |",
            "+--------+--------+----------+--------------------------------+",
        ];
        assert_batches_eq!(
            expected,
            &*persisting_data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Ensure the started batch ident is increased after a persist call, but not the completed
        // batch ident.
        assert_eq!(p.started_persistence_count.get(), 1);
        assert_eq!(p.completed_persistence_count, 0);
        // And the batch is correctly identified
        assert_eq!(persisting_data.batch_ident().get(), 1);

        // Buffer another write during an ongoing persist.
        let mb = lp_to_mutable_batch(r#"bananas,city=Madrid people=4,pigeons="none" 20"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        // Which must be readable, alongside the ongoing persist data.
        {
            let data = p.get_query_data().expect("must have data");
            assert_eq!(data.partition_id(), PARTITION_ID);
            assert_eq!(data.record_batches().len(), 2);
            let expected = [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2.0    | millions | 1970-01-01T00:00:00.000000010Z |",
                "| Madrid | 4.0    | none     | 1970-01-01T00:00:00.000000020Z |",
                "+--------+--------+----------+--------------------------------+",
            ];
            assert_batches_eq!(
                expected,
                &*data
                    .record_batches()
                    .iter()
                    .map(Deref::deref)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }

        // The persist now "completes".
        let set = p.mark_persisted(persisting_data);
        assert_eq!(set.len(), 1);
        assert!(set.contains(SequenceNumber::new(1)));

        // Ensure the started batch ident isn't increased when a persist completes, but the
        // completed count is increased.
        assert_eq!(p.started_persistence_count.get(), 1);
        assert_eq!(p.completed_persistence_count, 1);

        // Querying the buffer should now return only the second write.
        {
            let data = p.get_query_data().expect("must have data");
            assert_eq!(data.partition_id(), PARTITION_ID);
            assert_eq!(data.record_batches().len(), 1);
            let expected = [
                "+--------+--------+---------+--------------------------------+",
                "| city   | people | pigeons | time                           |",
                "+--------+--------+---------+--------------------------------+",
                "| Madrid | 4.0    | none    | 1970-01-01T00:00:00.000000020Z |",
                "+--------+--------+---------+--------------------------------+",
            ];
            assert_batches_eq!(
                expected,
                &*data
                    .record_batches()
                    .iter()
                    .map(Deref::deref)
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }

    // Ensure the ordering of snapshots & persisting data is preserved such that
    // updates resolve correctly, and batch identifiers are correctly allocated
    // and validated in mark_persisted() calls which return the correct
    // SequenceNumberSet instances.
    #[tokio::test]
    async fn test_record_batch_ordering() {
        // A helper function to dedupe the record batches in [`QueryAdaptor`]
        // and assert the resulting batch contents.
        async fn assert_deduped(expect: &[&str], batch: QueryAdaptor) {
            let batch = batch
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>();

            let sort_keys = vec![PhysicalSortExpr {
                expr: col("time", &batch[0].schema()).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }];

            // Setup in memory stream
            let schema = batch[0].schema();
            let projection = None;
            let input = Arc::new(MemoryExec::try_new(&[batch], schema, projection).unwrap());

            // Create and run the deduplicator
            let exec = Arc::new(iox_query::provider::DeduplicateExec::new(input, sort_keys));
            let got = test_collect(Arc::clone(&exec) as Arc<dyn ExecutionPlan>).await;

            assert_batches_eq!(expect, &*got);
        }

        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // Perform the initial write.
        //
        // In the next series of writes this test will overwrite the value of x
        // and assert the deduped resulting state.
        let mb = lp_to_mutable_batch(r#"bananas x=1 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Write an update
        let mb = lp_to_mutable_batch(r#"bananas x=2 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 2.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Ensure the batch ident is increased after a persist call.
        assert_eq!(p.started_persistence_count.get(), 0);

        // Begin persisting the data, moving the buffer to the persisting state.

        let persisting_data1 = p.mark_persisting().unwrap();
        assert_eq!(persisting_data1.record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 2.0 |",
                "+--------------------------------+-----+",
            ],
            (*persisting_data1).clone(),
        )
        .await;

        // Ensure the batch ident is increased after a persist call.
        assert_eq!(p.started_persistence_count.get(), 1);
        // And the batch is correctly identified
        assert_eq!(persisting_data1.batch_ident().get(), 1);

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=3 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(3))
            .expect("write should succeed");

        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 2);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 3.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Persist again, moving the last write to the persisting state and
        // adding it to the persisting queue.

        let persisting_data2 = p.mark_persisting().unwrap();
        assert_eq!(persisting_data2.record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 3.0 |",
                "+--------------------------------+-----+",
            ],
            (*persisting_data2).clone(),
        )
        .await;

        // Ensure the batch ident is increased after a persist call.
        assert_eq!(p.started_persistence_count.get(), 2);
        // And the batch is correctly identified
        assert_eq!(persisting_data1.batch_ident().get(), 1);
        assert_eq!(persisting_data2.batch_ident().get(), 2);

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=4 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(3))
            .expect("write should succeed");

        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 3);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Finish persisting the first batch.
        let set = p.mark_persisted(persisting_data1);
        assert_eq!(set.len(), 2);
        assert!(set.contains(SequenceNumber::new(1)));
        assert!(set.contains(SequenceNumber::new(2)));

        // And assert the correct value remains.
        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 2);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Finish persisting the second batch.
        let set = p.mark_persisted(persisting_data2);
        assert_eq!(set.len(), 1);
        assert!(set.contains(SequenceNumber::new(3)));

        // And assert the correct value remains.
        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        assert_eq!(p.started_persistence_count.get(), 2);
    }

    // Ensure the ordering of snapshots & persisting data is preserved such that
    // updates resolve correctly when queried, and batch identifiers are
    // correctly allocated, validated, and removed in mark_persisted() calls
    // which return the correct SequenceNumberSet instances.
    #[tokio::test]
    async fn test_out_of_order_persist() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // Perform the initial write.
        //
        // In the next series of writes this test will overwrite the value of x
        // and assert the deduped resulting state.
        let mb = lp_to_mutable_batch(r#"bananas x=1 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // Begin persisting the data, moving the buffer to the persisting state.

        let persisting_data1 = p.mark_persisting().unwrap();

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=2 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(3))
            .expect("write should succeed");

        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 2.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Persist again, moving the last write to the persisting state and
        // adding it to the persisting queue.

        let persisting_data2 = p.mark_persisting().unwrap();

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=3 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(4))
            .expect("write should succeed");

        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 2.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 3.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Persist again, moving the last write to the persisting state and
        // adding it to the persisting queue ordered such that querying returns
        // the correctly ordered rows (newest rows last).
        let persisting_data3 = p.mark_persisting().unwrap();

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=4 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(5))
            .expect("write should succeed");

        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 2.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 3.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Finish persisting the second batch out-of-order! The middle entry,
        // ensuring the first and last entries remain ordered.
        let set = p.mark_persisted(persisting_data2);
        assert_eq!(set.len(), 1);
        assert!(set.contains(SequenceNumber::new(3)));

        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 3.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Finish persisting the last batch.
        let set = p.mark_persisted(persisting_data3);
        assert_eq!(set.len(), 1);
        assert!(set.contains(SequenceNumber::new(4)));

        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 1.0 |",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );

        // Finish persisting the first batch.
        let set = p.mark_persisted(persisting_data1);
        assert_eq!(set.len(), 1);
        assert!(set.contains(SequenceNumber::new(1)));

        // Assert only the buffered data remains
        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------------------------------+-----+",
                "| time                           | x   |",
                "+--------------------------------+-----+",
                "| 1970-01-01T00:00:00.000000042Z | 4.0 |",
                "+--------------------------------+-----+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );
    }

    // Ensure an updated sort key is returned.
    #[tokio::test]
    async fn test_update_provided_sort_key() {
        let starting_state =
            SortKeyState::Provided(Some(SortKey::from_columns(["banana", "time"])));

        let mut p = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            NamespaceId::new(42),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(1),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from("platanos")
            })),
            starting_state,
            TRANSITION_SHARD_ID,
        );

        let want = Some(SortKey::from_columns(["banana", "platanos", "time"]));
        p.update_sort_key(want.clone());

        assert_matches!(p.sort_key(), SortKeyState::Provided(_));
        assert_eq!(p.sort_key().get().await, want);
    }

    // Test loading a deferred sort key from the catalog on demand.
    #[tokio::test]
    async fn test_update_deferred_sort_key() {
        let metrics = Arc::new(metric::Registry::default());
        let backoff_config = BackoffConfig::default();
        let catalog: Arc<dyn Catalog> =
            Arc::new(iox_catalog::mem::MemCatalog::new(Arc::clone(&metrics)));

        // Populate the catalog with the shard / namespace / table
        let (shard_id, _ns_id, table_id) =
            populate_catalog(&*catalog, ShardIndex::new(1), "bananas", "platanos").await;

        let partition_id = catalog
            .repositories()
            .await
            .partitions()
            .create_or_get("test".into(), shard_id, table_id)
            .await
            .expect("should create")
            .id;

        catalog
            .repositories()
            .await
            .partitions()
            .cas_sort_key(partition_id, None, &["terrific"])
            .await
            .unwrap();

        // Read the just-created sort key (None)
        let fetcher = Arc::new(DeferredLoad::new(
            Duration::from_nanos(1),
            SortKeyResolver::new(partition_id, Arc::clone(&catalog), backoff_config.clone())
                .fetch(),
        ));

        let starting_state = SortKeyState::Deferred(fetcher);

        let mut p = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            NamespaceId::new(42),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(1),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from("platanos")
            })),
            starting_state,
            shard_id,
        );

        let want = Some(SortKey::from_columns(["banana", "platanos", "time"]));
        p.update_sort_key(want.clone());

        assert_matches!(p.sort_key(), SortKeyState::Provided(_));
        assert_eq!(p.sort_key().get().await, want);
    }

    // Perform writes with non-monotonic sequence numbers.
    #[tokio::test]
    async fn test_non_monotonic_writes() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // Perform out of order writes.
        p.buffer_write(
            lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1,
            SequenceNumber::new(2),
        )
        .expect("write should succeed");
        let _ = p.buffer_write(
            lp_to_mutable_batch(r#"bananas,city=Madrid people=2,pigeons="none" 11"#).1,
            SequenceNumber::new(1),
        );

        // Nothing should explode, data should be readable.
        let data = p.get_query_data().unwrap();
        assert_batches_eq!(
            [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2.0    | millions | 1970-01-01T00:00:00.000000010Z |",
                "| Madrid | 2.0    | none     | 1970-01-01T00:00:00.000000011Z |",
                "+--------+--------+----------+--------------------------------+",
            ],
            &*data
                .record_batches()
                .iter()
                .map(Deref::deref)
                .cloned()
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn test_mark_persisting_no_data() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        assert!(p.mark_persisting().is_none());
    }

    #[tokio::test]
    async fn test_mark_persisting_twice() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        assert!(p.mark_persisting().is_some());
        assert!(p.mark_persisting().is_none());
    }

    // Ensure an empty PartitionData does not panic due to constructing an empty
    // QueryAdaptor.
    #[tokio::test]
    async fn test_empty_partition_no_queryadaptor_panic() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        assert!(p.get_query_data().is_none());
    }
}
