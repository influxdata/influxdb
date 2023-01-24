use std::sync::Arc;

use data_types::{NamespaceId, PartitionId, PartitionKey, ShardId, TableId};
use observability_deps::tracing::*;
use parking_lot::Mutex;
use schema::sort::SortKey;
use thiserror::Error;
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit},
    time::Instant,
};
use uuid::Uuid;

use crate::{
    buffer_tree::{
        namespace::NamespaceName,
        partition::{persisting::PersistingData, PartitionData, SortKeyState},
        table::TableName,
    },
    deferred_load::DeferredLoad,
    persist::completion_observer::CompletedPersist,
};

use super::completion_observer::PersistCompletionObserver;

/// Errors a persist can experience.
#[derive(Debug, Error)]
pub(super) enum PersistError {
    /// A concurrent sort key update was observed and the sort key update was
    /// aborted. The newly observed sort key is returned.
    #[error("detected concurrent sort key update")]
    ConcurrentSortKeyUpdate(SortKey),
}

/// An internal type that contains all necessary information to run a persist
/// task.
#[derive(Debug)]
pub(super) struct PersistRequest {
    complete: oneshot::Sender<()>,
    partition: Arc<Mutex<PartitionData>>,
    data: PersistingData,
    enqueued_at: Instant,
    permit: OwnedSemaphorePermit,
}

impl PersistRequest {
    /// Construct a [`PersistRequest`] for `data` from `partition`, recording
    /// the current timestamp as the "enqueued at" point.
    pub(super) fn new(
        partition: Arc<Mutex<PartitionData>>,
        data: PersistingData,
        permit: OwnedSemaphorePermit,
        enqueued_at: Instant,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                complete: tx,
                partition,
                data,
                enqueued_at,
                permit,
            },
            rx,
        )
    }

    /// Return the partition ID of the persisting data.
    pub(super) fn partition_id(&self) -> PartitionId {
        self.data.partition_id()
    }
}

/// The context of a persist job, containing the data to be persisted and
/// associated metadata.
///
/// This type caches various read-only (or read-mostly) fields on the
/// [`PartitionData`] itself, avoiding unnecessary lock contention. It also
/// carries timing metadata, and the completion notification channel for the
/// enqueuer notification.
pub(super) struct Context {
    partition: Arc<Mutex<PartitionData>>,
    data: PersistingData,

    /// IDs loaded from the partition at construction time.
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: PartitionId,

    transition_shard_id: ShardId,

    // The partition key for this partition
    partition_key: PartitionKey,

    /// Deferred strings needed for persistence.
    ///
    /// These [`DeferredLoad`] are given a pre-fetch hint when this [`Context`]
    /// is constructed to load them in the background (if not already resolved)
    /// in order to avoid incurring the query latency when the values are
    /// needed.
    namespace_name: Arc<DeferredLoad<NamespaceName>>,
    table_name: Arc<DeferredLoad<TableName>>,

    /// The [`SortKey`] for the [`PartitionData`] at the time of [`Context`]
    /// construction.
    ///
    /// The [`SortKey`] MUST NOT change during persistence, as updates to the
    /// sort key are not commutative and thus require serialising. This
    /// precludes parallel persists of partitions, unless they can be proven not
    /// to need to update the sort key.
    sort_key: SortKeyState,

    /// A notification signal to indicate to the caller that this partition has
    /// persisted.
    complete: oneshot::Sender<()>,

    /// Timing statistics tracking the timestamp this persist job was first
    /// enqueued, and the timestamp this [`Context`] was constructed (signifying
    /// the start of active persist work, as opposed to passive time spent in
    /// the queue).
    enqueued_at: Instant,
    dequeued_at: Instant,

    /// The persistence permit for this work.
    ///
    /// This permit MUST be retained for the entire duration of the persistence
    /// work, and MUST be released at the end of the persistence AFTER any
    /// references to the persisted data are released.
    permit: OwnedSemaphorePermit,
}

impl Context {
    /// Construct a persistence job [`Context`] from `req`.
    ///
    /// Locks the [`PartitionData`] in `req` to read various properties which
    /// are then cached in the [`Context`].
    pub(super) fn new(req: PersistRequest) -> Self {
        let partition_id = req.data.partition_id();

        // Obtain the partition lock and load the immutable values that will be
        // used during this persistence.
        let s = {
            let PersistRequest {
                complete,
                partition,
                data,
                enqueued_at,
                permit,
            } = req;

            let p = Arc::clone(&partition);
            let guard = p.lock();

            assert_eq!(partition_id, guard.partition_id());

            Self {
                partition,
                data,
                namespace_id: guard.namespace_id(),
                table_id: guard.table_id(),
                partition_id,
                partition_key: guard.partition_key().clone(),
                namespace_name: Arc::clone(guard.namespace_name()),
                table_name: Arc::clone(guard.table_name()),

                // Technically the sort key isn't immutable, but MUST NOT be
                // changed by an external actor (by something other than code in
                // this Context) during the execution of this persist, otherwise
                // sort key update serialisation is violated.
                sort_key: guard.sort_key().clone(),

                complete,
                enqueued_at,
                dequeued_at: Instant::now(),
                permit,
                transition_shard_id: guard.transition_shard_id(),
            }
        };

        // Pre-fetch the deferred values in a background thread (if not already
        // resolved)
        s.namespace_name.prefetch_now();
        s.table_name.prefetch_now();
        if let SortKeyState::Deferred(ref d) = s.sort_key {
            d.prefetch_now();
        }

        s
    }

    /// Replace the cached sort key in the [`PartitionData`] with the specified
    /// new sort key.
    ///
    /// # Panics
    ///
    /// This method panics if the cached sort key in the [`PartitionData`] has
    /// diverged from the cached sort key in `self`, indicating concurrent
    /// persist jobs that update the sort key for the same [`PartitionData`]
    /// running in this ingester process.
    pub(super) async fn set_partition_sort_key(&mut self, new_sort_key: SortKey) {
        // Invalidate the sort key in the partition.
        let old_key;
        {
            let mut guard = self.partition.lock();
            old_key = guard.sort_key().clone();
            guard.update_sort_key(Some(new_sort_key.clone()));
        };

        // Assert the internal (to this ingester2 instance) serialisation of
        // sort key updates.
        //
        // Both of these get() should not block due to both of the values having
        // been previously resolved / used in the Context constructor.
        assert_eq!(old_key.get().await, self.sort_key.get().await);

        // Update the cached copy of the sort key in this Context.
        self.sort_key = SortKeyState::Provided(Some(new_sort_key));
    }

    // Call [`PartitionData::mark_complete`] to finalise the persistence job,
    // emit a log for the user, and notify the observer of this persistence
    // task, if any.
    pub(super) async fn mark_complete<O>(self, object_store_id: Uuid, completion_observer: &O)
    where
        O: PersistCompletionObserver,
    {
        // Mark the partition as having completed persistence, causing it to
        // release the reference to the in-flight persistence data it is
        // holding.
        //
        // This SHOULD cause the data to be dropped, but there MAY be ongoing
        // queries that currently hold a reference to the data. In either case,
        // the persisted data will be dropped "shortly".
        let sequence_numbers = self.partition.lock().mark_persisted(self.data);
        let n_writes = sequence_numbers.len();

        // Dispatch the completion notification into the observer chain before
        // completing the persist operation.
        completion_observer
            .persist_complete(Arc::new(CompletedPersist::new(
                self.namespace_id,
                self.table_id,
                self.partition_id,
                sequence_numbers,
            )))
            .await;

        let now = Instant::now();

        info!(
            %object_store_id,
            namespace_id = %self.namespace_id,
            namespace_name = %self.namespace_name,
            table_id = %self.table_id,
            table_name = %self.table_name,
            partition_id = %self.partition_id,
            partition_key = %self.partition_key,
            total_persist_duration = ?now.duration_since(self.enqueued_at),
            active_persist_duration = ?now.duration_since(self.dequeued_at),
            queued_persist_duration = ?self.dequeued_at.duration_since(self.enqueued_at),
            n_writes,
            "persisted partition"
        );

        // Explicitly drop the permit before notifying the caller, so that if
        // there's no headroom in the queue, the caller that is woken by the
        // notification is able to push into the queue immediately.
        drop(self.permit);

        // Notify the observer of this persistence task, if any.
        let _ = self.complete.send(());
    }

    pub(super) fn enqueued_at(&self) -> Instant {
        self.enqueued_at
    }

    pub(super) fn sort_key(&self) -> &SortKeyState {
        &self.sort_key
    }

    pub(super) fn data(&self) -> &PersistingData {
        &self.data
    }

    pub(super) fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }

    pub(super) fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(super) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub(super) fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    pub(super) fn namespace_name(&self) -> &DeferredLoad<NamespaceName> {
        self.namespace_name.as_ref()
    }

    pub(super) fn table_name(&self) -> &DeferredLoad<TableName> {
        self.table_name.as_ref()
    }

    pub(super) fn transition_shard_id(&self) -> ShardId {
        self.transition_shard_id
    }
}
