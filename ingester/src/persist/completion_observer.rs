use std::{fmt::Debug, sync::Arc, time::Duration};

use async_trait::async_trait;
use data_types::{
    sequence_number_set::SequenceNumberSet, NamespaceId, ParquetFileParams, TableId,
    TransitionPartitionId,
};

use crate::wal::reference_tracker::WalReferenceHandle;

/// An abstract observer of persistence completion events.
///
/// This call is made synchronously by the persist worker, after
/// [`PartitionData::mark_persisted()`] has been called, but before the persist
/// job is logged as complete / waiter notification fired / job permit dropped.
///
/// Implementers SHOULD be be relatively quick to return to the caller, avoiding
/// unnecessary slowdown of persistence system.
///
/// [`PartitionData::mark_persisted()`]:
///     crate::buffer_tree::partition::PartitionData::mark_persisted()
#[async_trait]
pub trait PersistCompletionObserver: Send + Sync + Debug {
    /// Observe the [`CompletedPersist`] notification for the newly persisted
    /// data.
    async fn persist_complete(&self, note: Arc<CompletedPersist>);
}

/// A set of details describing the persisted data.
#[derive(Debug)]
pub struct CompletedPersist {
    /// The catalog metadata for the persist operation.
    meta: ParquetFileParams,

    /// The [`SequenceNumberSet`] of the persisted data.
    sequence_numbers: SequenceNumberSet,
}

impl CompletedPersist {
    /// Construct a new completion notification.
    pub(crate) fn new(meta: ParquetFileParams, sequence_numbers: SequenceNumberSet) -> Self {
        Self {
            meta,
            sequence_numbers,
        }
    }

    /// Returns the [`NamespaceId`] of the persisted data.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.meta.namespace_id
    }

    /// Returns the [`TableId`] of the persisted data.
    pub(crate) fn table_id(&self) -> TableId {
        self.meta.table_id
    }

    /// Returns the [`TransitionPartitionId`] of the persisted data.
    pub(crate) fn partition_id(&self) -> &TransitionPartitionId {
        &self.meta.partition_id
    }

    /// Returns the [`SequenceNumberSet`] of the persisted data.
    pub(crate) fn sequence_numbers(&self) -> &SequenceNumberSet {
        &self.sequence_numbers
    }

    /// Consume `self`, returning ownership of the inner [`SequenceNumberSet`].
    pub(crate) fn into_sequence_numbers(self) -> SequenceNumberSet {
        self.sequence_numbers
    }

    /// Obtain an owned inner [`SequenceNumberSet`] from an [`Arc`] wrapped
    /// [`CompletedPersist`] in the most memory-efficient way possible at call
    /// time.
    ///
    /// This method attempts to unwrap an [`Arc`]-wrapped [`CompletedPersist`]
    /// if `self `is the only reference, otherwise the shared set is cloned.
    pub(crate) fn owned_sequence_numbers(self: Arc<Self>) -> SequenceNumberSet {
        Arc::try_unwrap(self)
            .map(|v| v.into_sequence_numbers())
            .unwrap_or_else(|v| v.sequence_numbers().clone())
    }

    /// The number of rows persisted.
    pub fn row_count(&self) -> usize {
        self.meta.row_count as _
    }

    /// The number of columns persisted.
    pub fn column_count(&self) -> usize {
        self.meta.column_set.len()
    }

    /// The byte size of the generated Parquet file.
    pub fn parquet_file_bytes(&self) -> usize {
        self.meta.file_size_bytes as _
    }

    /// The duration of time covered by this file (difference between min
    /// timestamp, and max timestamp).
    pub fn timestamp_range(&self) -> Duration {
        let min = iox_time::Time::from(self.meta.min_time);
        let max = iox_time::Time::from(self.meta.max_time);

        max.checked_duration_since(min)
            .expect("parquet min/max file timestamp difference is negative")
    }
}

/// A no-op implementation of the [`PersistCompletionObserver`] trait.
#[derive(Clone, Copy, Debug, Default)]
pub struct NopObserver;

#[async_trait]
impl PersistCompletionObserver for NopObserver {
    async fn persist_complete(&self, _note: Arc<CompletedPersist>) {
        // the goggles do nothing!
    }
}

#[async_trait]
impl<T> PersistCompletionObserver for Arc<T>
where
    T: PersistCompletionObserver,
{
    async fn persist_complete(&self, note: Arc<CompletedPersist>) {
        (**self).persist_complete(note).await
    }
}

#[async_trait]
impl PersistCompletionObserver for WalReferenceHandle {
    async fn persist_complete(&self, note: Arc<CompletedPersist>) {
        self.enqueue_persist_notification(note).await
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    /// A mock observer that captures the calls it receives.
    #[derive(Debug, Default)]
    pub(crate) struct MockCompletionObserver {
        calls: Mutex<Vec<Arc<CompletedPersist>>>,
    }

    impl MockCompletionObserver {
        pub(crate) fn calls(&self) -> Vec<Arc<CompletedPersist>> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl PersistCompletionObserver for MockCompletionObserver {
        async fn persist_complete(&self, note: Arc<CompletedPersist>) {
            self.calls.lock().push(Arc::clone(&note));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{
        ARBITRARY_NAMESPACE_ID, ARBITRARY_TABLE_ID, ARBITRARY_TRANSITION_PARTITION_ID,
    };
    use data_types::{ColumnId, ColumnSet, SequenceNumber, Timestamp};

    fn arbitrary_file_meta() -> ParquetFileParams {
        ParquetFileParams {
            namespace_id: ARBITRARY_NAMESPACE_ID,
            table_id: ARBITRARY_TABLE_ID,
            partition_id: ARBITRARY_TRANSITION_PARTITION_ID.clone(),
            object_store_id: Default::default(),
            min_time: Timestamp::new(42),
            max_time: Timestamp::new(42),
            file_size_bytes: 42424242,
            row_count: 24,
            compaction_level: data_types::CompactionLevel::Initial,
            created_at: Timestamp::new(1234),
            column_set: ColumnSet::new([1, 2, 3, 4].into_iter().map(ColumnId::new)),
            max_l0_created_at: Timestamp::new(42),
        }
    }

    #[test]
    fn test_owned_sequence_numbers_only_ref() {
        let orig_set = [SequenceNumber::new(42)]
            .into_iter()
            .collect::<SequenceNumberSet>();

        let note = Arc::new(CompletedPersist::new(
            arbitrary_file_meta(),
            orig_set.clone(),
        ));

        assert_eq!(orig_set, note.owned_sequence_numbers())
    }

    #[test]
    fn test_owned_sequence_numbers_many_ref() {
        let orig_set = [SequenceNumber::new(42)]
            .into_iter()
            .collect::<SequenceNumberSet>();

        let note = Arc::new(CompletedPersist::new(
            arbitrary_file_meta(),
            orig_set.clone(),
        ));

        let note2 = Arc::clone(&note);

        assert_eq!(orig_set, note.owned_sequence_numbers());
        assert_eq!(orig_set, note2.owned_sequence_numbers());
    }

    #[test]
    fn test_accessors() {
        let meta = arbitrary_file_meta();

        let note = CompletedPersist::new(meta.clone(), Default::default());

        assert_eq!(note.namespace_id(), meta.namespace_id);
        assert_eq!(note.table_id(), meta.table_id);
        assert_eq!(note.partition_id(), &meta.partition_id);

        assert_eq!(note.column_count(), meta.column_set.len());
        assert_eq!(note.row_count(), meta.row_count as usize);
        assert_eq!(note.parquet_file_bytes(), meta.file_size_bytes as usize);
    }

    #[test]
    fn test_timestamp_range() {
        const RANGE: Duration = Duration::from_secs(42);

        let min = iox_time::Time::from_timestamp_nanos(0);
        let max = min.checked_add(RANGE).unwrap();

        let mut meta = arbitrary_file_meta();
        meta.min_time = Timestamp::from(min);
        meta.max_time = Timestamp::from(max);

        let note = CompletedPersist::new(meta, Default::default());

        assert_eq!(note.timestamp_range(), RANGE);
    }

    #[test]
    #[should_panic(expected = "parquet min/max file timestamp difference is negative")]
    fn test_timestamp_range_negative() {
        const RANGE: Duration = Duration::from_secs(42);

        let min = iox_time::Time::from_timestamp_nanos(0);
        let max = min.checked_add(RANGE).unwrap();

        let mut meta = arbitrary_file_meta();

        // Values are the wrong way around!
        meta.max_time = Timestamp::from(min);
        meta.min_time = Timestamp::from(max);

        let note = CompletedPersist::new(meta, Default::default());
        let _ = note.timestamp_range();
    }
}
