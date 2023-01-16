use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{sequence_number_set::SequenceNumberSet, NamespaceId, PartitionId, TableId};

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
pub(crate) trait PersistCompletionObserver: Send + Sync + Debug {
    /// Observe the [`CompletedPersist`] notification for the newly persisted
    /// data.
    async fn persist_complete(&self, note: Arc<CompletedPersist>);
}

/// A set of details describing the persisted data.
#[derive(Debug, Clone)]
pub struct CompletedPersist {
    /// The catalog identifiers for the persisted partition.
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,

    /// The [`SequenceNumberSet`] of the persisted data.
    sequence_numbers: SequenceNumberSet,
}

impl CompletedPersist {
    /// Construct a new completion notification.
    pub(super) fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: PartitionId,
        sequence_numbers: SequenceNumberSet,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            partition_id,
            sequence_numbers,
        }
    }

    /// Returns the [`NamespaceId`] of the persisted data.
    pub(crate) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    /// Returns the [`TableId`] of the persisted data.
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the [`PartitionId`] of the persisted data.
    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Returns the [`SequenceNumberSet`] of the persisted data.
    pub(crate) fn sequence_numbers(&self) -> &SequenceNumberSet {
        &self.sequence_numbers
    }
}

/// A no-op implementation of the [`PersistCompletionObserver`] trait.
#[derive(Debug, Default)]
pub(crate) struct NopObserver;

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
