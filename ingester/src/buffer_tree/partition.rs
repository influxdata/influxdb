//! Partition level data buffer structures.

use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use schema::sort::SortKey;
use thiserror::Error;
use write_summary::ShardProgress;

use self::buffer::{traits::Queryable, BufferState, DataBuffer, Persisting};
use super::table::TableName;
use crate::{
    deferred_load::DeferredLoad, query_adaptor::QueryAdaptor, sequence_range::SequenceNumberRange,
};

mod buffer;
pub mod resolver;

/// Errors that occur during DML operation buffering.
#[derive(Debug, Error)]
pub(crate) enum BufferError {
    /// The op being applied has already been previously persisted.
    #[error("skipped applying already persisted op")]
    SkipPersisted,

    /// An error occurred writing the data to the [`MutableBatch`].
    #[error("failed to apply DML op: {0}")]
    BufferError(#[from] mutable_batch::Error),
}

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

    /// The shard, namespace & table IDs for this partition.
    shard_id: ShardId,
    namespace_id: NamespaceId,
    table_id: TableId,
    /// The name of the table this partition is part of, potentially unresolved
    /// / deferred.
    table_name: Arc<DeferredLoad<TableName>>,

    /// A buffer for incoming writes.
    buffer: DataBuffer,

    /// The currently persisting [`DataBuffer`], if any.
    persisting: Option<BufferState<Persisting>>,

    /// The max_persisted_sequence number for any parquet_file in this
    /// partition.
    max_persisted_sequence_number: Option<SequenceNumber>,
}

impl PartitionData {
    /// Initialize a new partition data buffer
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: PartitionId,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: Arc<DeferredLoad<TableName>>,
        sort_key: SortKeyState,
        max_persisted_sequence_number: Option<SequenceNumber>,
    ) -> Self {
        Self {
            partition_id: id,
            partition_key,
            sort_key,
            shard_id,
            namespace_id,
            table_id,
            table_name,
            buffer: DataBuffer::default(),
            persisting: None,
            max_persisted_sequence_number,
        }
    }

    /// Buffer the given [`MutableBatch`] in memory, ordered by the specified
    /// [`SequenceNumber`].
    ///
    /// This method returns [`BufferError::SkipPersisted`] if `sequence_number`
    /// falls in the range of previously persisted data (where `sequence_number`
    /// is strictly less than the value of
    /// [`Self::max_persisted_sequence_number()`]).
    ///
    /// # Panics
    ///
    /// This method panics if `sequence_number` is not strictly greater than
    /// previous calls. This is not enforced for writes before the persist mark.
    pub(super) fn buffer_write(
        &mut self,
        mb: MutableBatch,
        sequence_number: SequenceNumber,
    ) -> Result<(), BufferError> {
        // Skip any ops that have already been applied.
        if let Some(min) = self.max_persisted_sequence_number {
            if sequence_number <= min {
                trace!(
                    shard_id=%self.shard_id,
                    op_sequence_number=?sequence_number,
                    "skipping already-persisted write"
                );
                return Err(BufferError::SkipPersisted);
            }
        }

        // Buffer the write, which ensures monotonicity of writes within the
        // buffer itself.
        self.buffer.buffer_write(mb, sequence_number)?;

        trace!(
            shard_id = %self.shard_id,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            min_sequence_number=?self.buffer.sequence_number_range().inclusive_min(),
            max_sequence_number=?self.buffer.sequence_number_range().inclusive_max(),
            "buffered write"
        );

        Ok(())
    }

    /// Return all data for this partition, ordered by the [`SequenceNumber`]
    /// from which it was buffered with.
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
            .flat_map(|b| b.get_query_data())
            .chain(buffered_data)
            .collect::<Vec<_>>();

        trace!(
            shard_id = %self.shard_id,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            min_sequence_number=?self.buffer.sequence_number_range().inclusive_min(),
            max_sequence_number=?self.buffer.sequence_number_range().inclusive_max(),
            max_persisted=?self.max_persisted_sequence_number(),
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

    /// Return the range of [`SequenceNumber`] currently queryable by calling
    /// [`PartitionData::get_query_data()`].
    ///
    /// This includes buffered data, snapshots, and currently persisting data.
    pub(crate) fn sequence_number_range(&self) -> SequenceNumberRange {
        self.persisting
            .as_ref()
            .map(|v| v.sequence_number_range().clone())
            .unwrap_or_default()
            .merge(self.buffer.sequence_number_range())
    }

    /// Return the progress from this Partition
    pub(crate) fn progress(&self) -> ShardProgress {
        let mut p = ShardProgress::default();

        let range = self.buffer.sequence_number_range();
        // Observe both the min & max, as the ShardProgress tracks both.
        if let Some(v) = range.inclusive_min() {
            p = p.with_buffered(v);
            p = p.with_buffered(range.inclusive_max().unwrap());
        }

        // Observe the buffered state, if any.
        if let Some(range) = self.persisting.as_ref().map(|p| p.sequence_number_range()) {
            // Observe both the min & max, as the ShardProgress tracks both.
            //
            // All persisting batches MUST contain data. This is an invariant
            // upheld by the state machine.
            p = p.with_buffered(range.inclusive_min().unwrap());
            p = p.with_buffered(range.inclusive_max().unwrap());
        }

        // And finally report the persist watermark for this partition.
        if let Some(v) = self.max_persisted_sequence_number() {
            p = p.with_persisted(v)
        }

        trace!(
            shard_id = %self.shard_id,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            progress = ?p,
            "progress query"
        );

        p
    }

    /// Snapshot and mark all buffered data as persisting.
    ///
    /// This method returns [`None`] if no data is buffered in [`Self`].
    ///
    /// A reference to the persisting data is retained until a corresponding
    /// call to [`Self::mark_persisted()`] is made to release it.
    ///
    /// # Panics
    ///
    /// This method panics if [`Self`] contains data already an ongoing persist
    /// operation. All calls to [`Self::mark_persisting()`] must be followed by
    /// a matching call to [`Self::mark_persisted()`] before a new persist can
    /// begin.
    pub(crate) fn mark_persisting(&mut self) -> Option<QueryAdaptor> {
        // Assert that there is at most one persist operation per partition
        // ongoing at any one time.
        //
        // This is not a system invariant, however the system MUST make
        // persisted partitions visible in monotonic order w.r.t their sequence
        // numbers.
        assert!(
            self.persisting.is_none(),
            "starting persistence on partition in persisting state"
        );

        let persisting = std::mem::take(&mut self.buffer).into_persisting()?;

        // From this point on, all code MUST be infallible or the buffered data
        // contained within persisting may be dropped.

        debug!(
            shard_id = %self.shard_id,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            current_max_persisted_sequence_number = ?self.max_persisted_sequence_number,
            persisting_min_sequence_number = ?persisting.sequence_number_range().inclusive_min(),
            persisting_max_sequence_number = ?persisting.sequence_number_range().inclusive_max(),
            "marking partition as persisting"
        );

        let data = persisting.get_query_data();
        self.persisting = Some(persisting);

        Some(QueryAdaptor::new(self.partition_id, data))
    }

    /// Mark this partition as having completed persistence up to, and
    /// including, the specified [`SequenceNumber`].
    ///
    /// All references to actively persisting are released.
    ///
    /// # Panics
    ///
    /// This method panics if [`Self`] is not marked as undergoing a persist
    /// operation. All calls to [`Self::mark_persisted()`] must be preceded by a
    /// matching call to [`Self::mark_persisting()`].
    pub(crate) fn mark_persisted(&mut self, sequence_number: SequenceNumber) {
        // Assert there is a batch marked as persisting in self, that it has a
        // non-empty sequence number range, and that the persisted upper bound
        // matches the data in the batch being dropped.
        //
        // TODO: once this has been deployed without issue (the assert does not
        // fire), passing the sequence number is redundant and can be removed.
        let persisting_max = self
            .persisting
            .as_ref()
            .expect("must be a persisting batch when marking complete")
            .sequence_number_range()
            .inclusive_max()
            .expect("persisting batch must contain sequence numbers");
        assert_eq!(
            persisting_max, sequence_number,
            "marking {:?} as persisted but persisting batch max is {:?}",
            sequence_number, persisting_max
        );

        // Additionally assert the persisting batch is ordered strictly before
        // the data in the buffer, if any.
        //
        // This asserts writes are monotonically applied.
        if let Some(buffer_min) = self.buffer.sequence_number_range().inclusive_min() {
            assert!(persisting_max < buffer_min, "monotonicity violation");
        }

        // It is an invariant that partitions are persisted in order so that
        // both the per-shard, and per-partition watermarks are correctly
        // advanced and accurate.
        if let Some(last_persist) = self.max_persisted_sequence_number() {
            assert!(
                sequence_number > last_persist,
                "out of order partition persistence, persisting {}, previously persisted {}",
                sequence_number.get(),
                last_persist.get(),
            );
        }

        self.max_persisted_sequence_number = Some(sequence_number);
        self.persisting = None;

        debug!(
            shard_id = %self.shard_id,
            namespace_id = %self.namespace_id,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            current_max_persisted_sequence_number = ?self.max_persisted_sequence_number,
            "marking partition persistence complete"
        );
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Return the [`SequenceNumber`] that forms the (inclusive) persistence
    /// watermark for this partition.
    pub(crate) fn max_persisted_sequence_number(&self) -> Option<SequenceNumber> {
        self.max_persisted_sequence_number
    }

    /// Return the name of the table this [`PartitionData`] is buffering writes
    /// for.
    pub(crate) fn table_name(&self) -> &Arc<DeferredLoad<TableName>> {
        &self.table_name
    }

    /// Return the shard ID for this partition.
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Return the table ID for this partition.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Return the partition key for this partition.
    pub(crate) fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the [`NamespaceId`] this partition is a part of.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
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

    lazy_static! {
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("platanos");
        static ref TABLE_NAME: TableName = TableName::from("bananas");
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
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        // No writes should report no sequence offsets.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), None);
            assert_eq!(range.inclusive_max(), None);
        }

        // The progress API should indicate there is no progress status.
        assert!(p.progress().is_empty());

        // And no data should be returned when queried.
        assert!(p.get_query_data().is_none());

        // Perform a single write.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // The sequence range should now cover the single write.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), Some(SequenceNumber::new(1)));
            assert_eq!(range.inclusive_max(), Some(SequenceNumber::new(1)));
        }

        // The progress API should indicate there is some data buffered, but not
        // persisted.
        {
            let progress = p.progress();
            assert!(progress.readable(SequenceNumber::new(1)));
            assert!(!progress.persisted(SequenceNumber::new(1)));
        }

        // The data should be readable.
        {
            let data = p.get_query_data().expect("should return data");
            assert_eq!(data.partition_id(), PARTITION_ID);

            let expected = [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2      | millions | 1970-01-01T00:00:00.000000010Z |",
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

        // The sequence range should now cover both writes.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), Some(SequenceNumber::new(1)));
            assert_eq!(range.inclusive_max(), Some(SequenceNumber::new(2)));
        }

        // The progress API should indicate there is more data buffered, but not
        // persisted.
        {
            let progress = p.progress();
            assert!(progress.readable(SequenceNumber::new(1)));
            assert!(progress.readable(SequenceNumber::new(2)));
            assert!(!progress.persisted(SequenceNumber::new(1)));
            assert!(!progress.persisted(SequenceNumber::new(2)));
        }

        // And finally both writes should be readable.
        {
            let data = p.get_query_data().expect("should contain data");
            assert_eq!(data.partition_id(), PARTITION_ID);

            let expected = [
                "+--------+--------+----------+--------------------------------+",
                "| city   | people | pigeons  | time                           |",
                "+--------+--------+----------+--------------------------------+",
                "| London | 2      | millions | 1970-01-01T00:00:00.000000010Z |",
                "| Madrid | 4      | none     | 1970-01-01T00:00:00.000000020Z |",
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
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        assert!(p.max_persisted_sequence_number().is_none());
        assert!(p.get_query_data().is_none());

        // Perform a single write.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // Begin persisting the partition.
        let persisting_data = p.mark_persisting().expect("must contain existing data");
        // And validate the data being persisted.
        assert_eq!(persisting_data.partition_id(), PARTITION_ID);
        assert_eq!(persisting_data.record_batches().len(), 1);
        let expected = [
            "+--------+--------+----------+--------------------------------+",
            "| city   | people | pigeons  | time                           |",
            "+--------+--------+----------+--------------------------------+",
            "| London | 2      | millions | 1970-01-01T00:00:00.000000010Z |",
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

        // The sequence range should now cover the single persisting write.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), Some(SequenceNumber::new(1)));
            assert_eq!(range.inclusive_max(), Some(SequenceNumber::new(1)));
        }

        // The progress API should indicate there is some data buffered, but not
        // yet persisted.
        {
            let progress = p.progress();
            assert!(progress.readable(SequenceNumber::new(1)));
            assert!(!progress.persisted(SequenceNumber::new(1)));
        }

        // And the max_persisted_sequence_number should not have changed.
        assert!(p.max_persisted_sequence_number().is_none());

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
                "| London | 2      | millions | 1970-01-01T00:00:00.000000010Z |",
                "| Madrid | 4      | none     | 1970-01-01T00:00:00.000000020Z |",
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

        // The sequence range should still cover both writes.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), Some(SequenceNumber::new(1)));
            assert_eq!(range.inclusive_max(), Some(SequenceNumber::new(2)));
        }

        // The progress API should indicate that both writes are still data
        // buffered.
        {
            let progress = p.progress();
            assert!(progress.readable(SequenceNumber::new(1)));
            assert!(progress.readable(SequenceNumber::new(2)));
            assert!(!progress.persisted(SequenceNumber::new(1)));
            assert!(!progress.persisted(SequenceNumber::new(2)));
        }

        // And the max_persisted_sequence_number should not have changed.
        assert!(p.max_persisted_sequence_number().is_none());

        // The persist now "completes".
        p.mark_persisted(SequenceNumber::new(1));

        // The sequence range should now cover only the second remaining
        // buffered write.
        {
            let range = p.sequence_number_range();
            assert_eq!(range.inclusive_min(), Some(SequenceNumber::new(2)));
            assert_eq!(range.inclusive_max(), Some(SequenceNumber::new(2)));
        }

        // The progress API should indicate that the writes are readable
        // (somewhere, not necessarily in the ingester), and the first write is
        // persisted.
        {
            let progress = p.progress();
            assert!(progress.readable(SequenceNumber::new(1)));
            assert!(progress.readable(SequenceNumber::new(2)));
            assert!(progress.persisted(SequenceNumber::new(1)));
            assert!(!progress.persisted(SequenceNumber::new(2)));
        }

        // And the max_persisted_sequence_number should reflect the completed
        // persist op.
        assert_eq!(
            p.max_persisted_sequence_number(),
            Some(SequenceNumber::new(1))
        );

        // Querying the buffer should now return only the second write.
        {
            let data = p.get_query_data().expect("must have data");
            assert_eq!(data.partition_id(), PARTITION_ID);
            assert_eq!(data.record_batches().len(), 1);
            let expected = [
                "+--------+--------+---------+--------------------------------+",
                "| city   | people | pigeons | time                           |",
                "+--------+--------+---------+--------------------------------+",
                "| Madrid | 4      | none    | 1970-01-01T00:00:00.000000020Z |",
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
    // updates resolve correctly.
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
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
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
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000042Z | 1 |",
                "+--------------------------------+---+",
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
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000042Z | 2 |",
                "+--------------------------------+---+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Begin persisting the data, moving the buffer to the persisting state.
        {
            let batches = p.mark_persisting().unwrap();
            assert_eq!(batches.record_batches().len(), 1);
            assert_deduped(
                &[
                    "+--------------------------------+---+",
                    "| time                           | x |",
                    "+--------------------------------+---+",
                    "| 1970-01-01T00:00:00.000000042Z | 2 |",
                    "+--------------------------------+---+",
                ],
                batches,
            )
            .await;
        }

        // Buffer another write, and generate a snapshot by querying it.
        let mb = lp_to_mutable_batch(r#"bananas x=3 42"#).1;
        p.buffer_write(mb, SequenceNumber::new(3))
            .expect("write should succeed");

        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 2);
        assert_deduped(
            &[
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000042Z | 3 |",
                "+--------------------------------+---+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;

        // Finish persisting.
        p.mark_persisted(SequenceNumber::new(2));
        assert_eq!(
            p.max_persisted_sequence_number(),
            Some(SequenceNumber::new(2))
        );

        // And assert the correct value remains.
        assert_eq!(p.get_query_data().unwrap().record_batches().len(), 1);
        assert_deduped(
            &[
                "+--------------------------------+---+",
                "| time                           | x |",
                "+--------------------------------+---+",
                "| 1970-01-01T00:00:00.000000042Z | 3 |",
                "+--------------------------------+---+",
            ],
            p.get_query_data().unwrap(),
        )
        .await;
    }

    // Ensure an updated sort key is returned.
    #[tokio::test]
    async fn test_update_provided_sort_key() {
        let starting_state =
            SortKeyState::Provided(Some(SortKey::from_columns(["banana", "time"])));

        let mut p = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from("platanos")
            })),
            starting_state,
            None,
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
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TableName::from("platanos")
            })),
            starting_state,
            None,
        );

        let want = Some(SortKey::from_columns(["banana", "platanos", "time"]));
        p.update_sort_key(want.clone());

        assert_matches!(p.sort_key(), SortKeyState::Provided(_));
        assert_eq!(p.sort_key().get().await, want);
    }

    // Perform writes with non-monotonic sequence numbers.
    #[tokio::test]
    #[should_panic(expected = "monotonicity violation")]
    async fn test_non_monotonic_writes() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        // Perform out of order writes.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb.clone(), SequenceNumber::new(2))
            .expect("write should succeed");
        let _ = p.buffer_write(mb, SequenceNumber::new(1));
    }

    #[tokio::test]
    #[should_panic(expected = "must be a persisting batch when marking complete")]
    async fn test_mark_persisted_not_persisting() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        p.mark_persisted(SequenceNumber::new(1));
    }

    #[tokio::test]
    async fn test_mark_persisting_no_data() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        assert!(p.mark_persisting().is_none());
    }

    #[tokio::test]
    #[should_panic(expected = "starting persistence on partition in persisting state")]
    async fn test_mark_persisting_twice() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        assert!(p.mark_persisting().is_some());

        p.mark_persisting();
    }

    #[tokio::test]
    #[should_panic(
        expected = "marking SequenceNumber(42) as persisted but persisting batch max is SequenceNumber(2)"
    )]
    async fn test_mark_persisted_wrong_sequence_number() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        assert!(p.mark_persisting().is_some());

        p.mark_persisted(SequenceNumber::new(42));
    }

    // Because persisting moves the data out of the "hot" buffer, the sequence
    // numbers are not validated as being monotonic (the new buffer has no
    // sequence numbers to compare against).
    //
    // Instead this check is performed when marking the persist op as complete.
    #[tokio::test]
    #[should_panic(expected = "monotonicity violation")]
    async fn test_non_monotonic_writes_with_persistence() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb.clone(), SequenceNumber::new(42))
            .expect("write should succeed");

        assert!(p.mark_persisting().is_some());

        // This succeeds due to a new buffer being in place that cannot track
        // previous sequence numbers.
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("out of order write should succeed");

        // The assert on non-monotonic writes moves to here instead.
        p.mark_persisted(SequenceNumber::new(42));
    }

    // As above, the sequence numbers are not tracked between buffer instances.
    //
    // This test ensures that a partition can tolerate replayed ops prior to the
    // persist marker when first initialising. However once a partition has
    // buffered beyond the persist marker, it cannot re-buffer ops after it.
    #[tokio::test]
    #[should_panic(expected = "monotonicity violation")]
    async fn test_non_monotonic_writes_after_persistence() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            None,
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb.clone(), SequenceNumber::new(42))
            .expect("write should succeed");

        assert!(p.mark_persisting().is_some());
        p.mark_persisted(SequenceNumber::new(42));

        // This should fail as the write "goes backwards".
        let err = p
            .buffer_write(mb.clone(), SequenceNumber::new(1))
            .expect_err("out of order write should succeed");

        // This assert ensures replay is tolerated, with the previously
        // persisted ops skipping instead of being applied.
        assert_matches!(err, BufferError::SkipPersisted);

        // Until a write is accepted.
        p.buffer_write(mb.clone(), SequenceNumber::new(100))
            .expect("out of order write should succeed");

        // At which point a write between the persist marker and the maximum
        // applied sequence number is a hard error.
        let _ = p.buffer_write(mb, SequenceNumber::new(50));
    }

    // As above, but with a pre-configured persist marker greater than the
    // sequence number being wrote.
    #[tokio::test]
    async fn test_non_monotonic_writes_persist_marker() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            Some(SequenceNumber::new(42)),
        );
        assert_eq!(
            p.max_persisted_sequence_number(),
            Some(SequenceNumber::new(42))
        );

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;

        // This should fail as the write "goes backwards".
        let err = p
            .buffer_write(mb, SequenceNumber::new(1))
            .expect_err("out of order write should not succeed");

        assert_matches!(err, BufferError::SkipPersisted);
    }

    // Restoring a persist marker is included in progress reports.
    #[tokio::test]
    async fn test_persist_marker_progress() {
        let p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            Some(SequenceNumber::new(42)),
        );
        assert_eq!(
            p.max_persisted_sequence_number(),
            Some(SequenceNumber::new(42))
        );

        // Sequence number ranges cover buffered data only.
        assert!(p.sequence_number_range().inclusive_min().is_none());
        assert!(p.sequence_number_range().inclusive_max().is_none());

        // Progress API returns that the op is persisted and readable (not on
        // the ingester, but via object storage)
        assert!(p.progress().readable(SequenceNumber::new(42)));
        assert!(p.progress().persisted(SequenceNumber::new(42)));
    }

    // Ensure an empty PartitionData does not panic due to constructing an empty
    // QueryAdaptor.
    #[tokio::test]
    async fn test_empty_partition_no_queryadaptor_panic() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            ShardId::new(2),
            NamespaceId::new(3),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            Some(SequenceNumber::new(42)),
        );

        assert!(p.get_query_data().is_none());
    }
}
