use std::{fmt::Debug, sync::Arc};

use data_types::{sequence_number_set::SequenceNumberSet, SequenceNumber};
use observability_deps::tracing::warn;
use tokio::sync::mpsc::{self, error::TrySendError};
use wal::SegmentId;

use crate::{
    persist::completion_observer::CompletedPersist, wal::reference_tracker::WalFileDeleter,
};

use super::WalReferenceActor;

/// A WAL file reference-count tracker handle.
///
/// The [`WalReferenceHandle`] feeds three inputs to the [`WalReferenceActor`]:
///
///   * The [`SequenceNumberSet`] and ID of rotated out WAL segment files
///   * The [`SequenceNumberSet`] of each completed persistence task
///   * All [`SequenceNumber`] of writes that failed to buffer
///
/// ```text
///             ┌  Write Processing ─ ─ ─ ─ ─ ─ ─ ─ ─
///                                                  │
///             │  ┌────────────┐   ┌─────────────┐
///                │ WAL Rotate │   │ WAL DmlSink │  │
///             │  └────────────┘   └─────────────┘
///                       │                │         │
///             │      IDs in              │
///                    rotated          Failed       │
///             │      segment         write IDs
///                     file               │         │
///             │         │                │
///              ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ┘
///                       ▼                ▼
///             ┌────────────────────────────────────┐
///             │                                    │
///             │         WalReferenceActor          │─ ─▶  Delete Files
///             │                                    │
///             └────────────────────────────────────┘
///                                ▲
///                                │
///             ┌  Persist System ─│─ ─ ─ ─ ─ ─ ─ ─ ─
///                                │                 │
///             │        ┌──────────────────┐
///                      │    Completed     │        │
///             │        │   Persistence    │
///                      │     Observer     │        │
///             │        └──────────────────┘
///              ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
/// ```
///
/// Using these three streams of information, the [`WalReferenceActor`] computes
/// the number of unpersisted operations referenced in each WAL segment file,
/// and updates this count as more persist operations complete.
///
/// Once all the operations in a given WAL file have been observed as persisted
/// (or failed to apply), the WAL file is no longer required (all data it
/// contains is durable in the object store) and it is deleted.
///
/// The [`WalReferenceActor`] is tolerant of out-of-order events - that is, a
/// "persisted" event can be received and processed before the WAL file the data
/// is in is known. This is necessary to handle "hot partition persistence"
/// where data is persisted before the WAL file is rotated.
///
/// The [`WalReferenceActor`] gracefully stops once all [`WalReferenceHandle`]
/// instances to it are dropped.
#[derive(Debug, Clone)]
pub(crate) struct WalReferenceHandle {
    /// A stream of newly rotated segment files and the set of
    /// [`SequenceNumber`] within them.
    file_tx: mpsc::Sender<(SegmentId, SequenceNumberSet)>,

    /// A steam of persist notifications - the [`SequenceNumberSet`] of the
    /// persisted data that is now durable in object storage, and which no
    /// longer requires WAL entries for.
    persist_tx: mpsc::Sender<Arc<CompletedPersist>>,

    /// A stream of [`SequenceNumber`] identifying operations that have been (or
    /// will be) added to the WAL, but failed to buffer/complete. These should
    /// be treated as if they were "persisted", as they will never be persisted,
    /// and are not expected to remain durable (user did not get an ACK).
    unbuffered_tx: mpsc::Sender<SequenceNumber>,
}

impl WalReferenceHandle {
    /// Construct a new [`WalReferenceActor`] and [`WalReferenceHandle`] pair.
    ///
    /// The returned [`WalReferenceActor`] SHOULD be
    /// [`WalReferenceActor::run()`] before the handle is used to avoid
    /// potential deadlocks.
    pub(crate) fn new<T>(wal: T, metrics: &metric::Registry) -> (Self, WalReferenceActor<T>)
    where
        T: WalFileDeleter,
    {
        let (file_tx, file_rx) = mpsc::channel(5);
        let (persist_tx, persist_rx) = mpsc::channel(50);
        let (unbuffered_tx, unbuffered_rx) = mpsc::channel(50);

        let actor = WalReferenceActor::new(wal, file_rx, persist_rx, unbuffered_rx, metrics);

        (
            Self {
                file_tx,
                persist_tx,
                unbuffered_tx,
            },
            actor,
        )
    }

    /// Enqueue a new file rotation event, providing the [`SegmentId`] of the
    /// WAL file and the [`SequenceNumberSet`] the WAL segment contains.
    pub(crate) async fn enqueue_rotated_file(&self, segment_id: SegmentId, set: SequenceNumberSet) {
        Self::send(&self.file_tx, (segment_id, set)).await
    }

    /// Enqueue a persist completion notification for newly persisted data.
    pub(crate) async fn enqueue_persist_notification(&self, note: Arc<CompletedPersist>) {
        Self::send(&self.persist_tx, note).await
    }

    /// Enqueue a notification that a write appearing in some WAL segment will
    /// not be buffered/persisted (either the active, not-yet-rotated segment or
    /// a prior, already-rotated segment).
    ///
    /// This can happen when a write is added to the WAL segment and
    /// subsequently fails to be applied to the in-memory buffer. It is
    /// important to track these unusual cases to ensure the WAL file is not
    /// kept forever due to an outstanding reference, waiting for the unbuffered
    /// write to be persisted (which it never will).
    pub(crate) async fn enqueue_unbuffered_write(&self, id: SequenceNumber) {
        Self::send(&self.unbuffered_tx, id).await
    }

    /// Send `val` over `chan`, logging a warning if `chan` is at capacity.
    async fn send<T>(chan: &mpsc::Sender<T>, val: T)
    where
        T: Debug + Send,
    {
        match chan.try_send(val) {
            Ok(()) => {}
            Err(TrySendError::Full(val)) => {
                warn!(?val, "notification buffer is full");
                chan.send(val).await.expect("wal reference actor stopped");
            }
            Err(TrySendError::Closed(_)) => panic!("wal reference actor stopped"),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use data_types::{NamespaceId, PartitionId, TableId};
    use futures::Future;
    use metric::{assert_counter, U64Gauge};
    use parking_lot::Mutex;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::Notify;

    use super::*;

    /// A mock file deleter that records the IDs it was asked to delete.
    #[derive(Debug, Default)]
    struct MockWalDeleter {
        notify: Notify,
        calls: Mutex<Vec<SegmentId>>,
    }

    impl MockWalDeleter {
        /// Return the set of [`SegmentId`] that have been deleted.
        fn calls(&self) -> Vec<SegmentId> {
            self.calls.lock().clone()
        }
        /// Return a future that completes when a file is subsequently deleted,
        /// or panics if no file is deleted within 5 seconds.
        fn waker(&self) -> impl Future<Output = ()> + '_ {
            self.notify
                .notified()
                .with_timeout_panic(Duration::from_secs(5))
        }
    }

    #[async_trait]
    impl WalFileDeleter for Arc<MockWalDeleter> {
        async fn delete_file(&self, id: SegmentId) {
            self.calls.lock().push(id);
            self.notify.notify_waiters();
        }
    }

    /// Return a [`SequenceNumberSet`] containing `vals`.
    fn new_set<T>(vals: T) -> SequenceNumberSet
    where
        T: IntoIterator<Item = i64>,
    {
        vals.into_iter().map(SequenceNumber::new).collect()
    }

    /// Return a persist completion notification with the given
    /// [`SequenceNumberSet`] values.
    fn new_note<T>(vals: T) -> Arc<CompletedPersist>
    where
        T: IntoIterator<Item = i64>,
    {
        Arc::new(CompletedPersist::new(
            NamespaceId::new(1),
            TableId::new(2),
            PartitionId::new(3),
            new_set(vals),
        ))
    }

    /// Test in-order notifications:
    ///
    ///   * WAL file is rotated and the tracker notified
    ///   * Multiple persists complete, and an unbuffered notification, draining
    ///     the references to the file
    ///   * The file is deleted when refs == 0
    ///   * Dropping the handle stops the actor
    #[tokio::test]
    async fn test_rotate_persist_delete() {
        const SEGMENT_ID: SegmentId = SegmentId::new(42);

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        // Add a file with IDs 1 through 5
        handle
            .enqueue_rotated_file(SEGMENT_ID, new_set([1, 2, 3, 4, 5]))
            .await;

        // Submit a persist notification that removes refs 1 & 2.
        handle.enqueue_persist_notification(new_note([1, 2])).await;

        // Ensure the file was not deleted
        assert!(wal.calls().is_empty());

        // Enqueue a unbuffered notification (out of order)
        handle
            .enqueue_unbuffered_write(SequenceNumber::new(5))
            .await;

        // Ensure the file was not deleted
        assert!(wal.calls().is_empty());

        // Finally release the last IDs
        let waker = wal.waker();
        handle.enqueue_persist_notification(new_note([3, 4])).await;

        // Wait for it to be processed
        waker.await;

        // Validate the correct ID was deleted
        assert_matches!(wal.calls().as_slice(), &[v] if v == SEGMENT_ID);

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_file_count",
            value = 0,
        );

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_file_op_reference_count",
            value = 0,
        );

        // Assert clean shutdown behaviour.
        drop(handle);
        actor_task
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("actor task should stop cleanly")
    }

    /// Test in-order notifications:
    ///
    ///   * Multiple persists complete
    ///   * A WAL file notification is received containing a subset of the
    ///     already persisted IDs
    ///   * The file is deleted because refs == 0
    ///   * A WAL file notification for a superset of the remaining persisted
    ///     IDs
    ///   * The remaining references are persisted/unbuffered
    ///   * The second WAL file is deleted
    ///   * Dropping the handle stops the actor
    #[tokio::test]
    async fn test_persist_all_rotate_delete() {
        const SEGMENT_ID_1: SegmentId = SegmentId::new(42);
        const SEGMENT_ID_2: SegmentId = SegmentId::new(24);

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        // Submit a persist notification for the entire set of IDs [1,2,3,4] in
        // the upcoming first WAL, and partially the second WAL
        handle.enqueue_persist_notification(new_note([2])).await;
        handle.enqueue_persist_notification(new_note([1])).await;
        handle.enqueue_persist_notification(new_note([3, 4])).await;

        // Add a file with IDs 1, 2, 3
        let waker = wal.waker();
        handle
            .enqueue_rotated_file(SEGMENT_ID_1, new_set([1, 2, 3]))
            .await;

        // Wait for it to be processed
        waker.await;

        // Validate the correct ID was deleted
        assert_matches!(wal.calls().as_slice(), &[v] if v == SEGMENT_ID_1);

        // Enqueue the second WAL, covering 4
        handle
            .enqueue_rotated_file(SEGMENT_ID_2, new_set([4, 5, 6]))
            .await;

        // At this point, the second WAL still has references outstanding (5, 6)
        // and should not have been deleted.
        assert_eq!(wal.calls().len(), 1);

        // Release one of the remaining two refs
        handle.enqueue_persist_notification(new_note([6])).await;

        // Still no deletion
        assert_eq!(wal.calls().len(), 1);

        // And finally release the last ref via an unbuffered notification
        let waker = wal.waker();
        handle
            .enqueue_unbuffered_write(SequenceNumber::new(5))
            .await;
        waker.await;

        // Validate the correct ID was deleted
        assert_matches!(wal.calls().as_slice(), &[a, b] => {
            assert_eq!(a, SEGMENT_ID_1);
            assert_eq!(b, SEGMENT_ID_2);
        });

        // Assert clean shutdown behaviour.
        drop(handle);
        actor_task
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("actor task should stop cleanly")
    }

    #[tokio::test]
    async fn test_empty_file_set() {
        const SEGMENT_ID: SegmentId = SegmentId::new(42);

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        // Notifying the actor of a WAL file with no operations in it should not
        // cause a panic, and should cause the file to be immediately deleted.
        let waker = wal.waker();
        handle
            .enqueue_rotated_file(SEGMENT_ID, SequenceNumberSet::default())
            .await;

        // Wait for the file deletion.
        waker.await;
        assert_matches!(wal.calls().as_slice(), &[v] if v == SEGMENT_ID);

        // Assert clean shutdown behaviour.
        drop(handle);
        actor_task
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("actor task should stop cleanly")
    }

    #[tokio::test]
    #[should_panic(expected = "duplicate segment ID")]
    async fn test_duplicate_segment_ids() {
        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        // Enqueuing a notification before the actor is running should succeed
        // because of the channel buffer capacity.
        handle
            .enqueue_rotated_file(SegmentId::new(42), new_set([1, 2]))
            .with_timeout_panic(Duration::from_secs(5))
            .await;

        handle
            .enqueue_rotated_file(SegmentId::new(42), new_set([3, 4]))
            .with_timeout_panic(Duration::from_secs(5))
            .await;

        // This should panic after processing the second file.
        actor.run().with_timeout_panic(Duration::from_secs(5)).await;
    }

    /// Enqueue two segment files, enqueue persist notifications for the second
    /// file and wait for it to be deleted to synchronise the state (so it's not
    /// a racy test).
    ///
    /// Then assert the metric values for the known state.
    #[tokio::test]
    async fn test_metrics() {
        const SEGMENT_ID_1: SegmentId = SegmentId::new(42);
        const SEGMENT_ID_2: SegmentId = SegmentId::new(24);

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        // Add a file with 4 references
        handle
            .enqueue_rotated_file(SEGMENT_ID_1, new_set([1, 2, 3, 4, 5]))
            .await;

        // Reduce the reference count for file 1 (leaving 3 references)
        handle.enqueue_persist_notification(new_note([1, 2])).await;

        // Enqueue the second file.
        handle
            .enqueue_rotated_file(SEGMENT_ID_2, new_set([6]))
            .await;

        // Release the references to file 2
        let waker = wal.waker();
        handle.enqueue_persist_notification(new_note([6])).await;

        waker.await;

        //
        // At this point, the actor has deleted the second file, which means it
        // has already processed the first enqueued file, and the first persist
        // notification that relates to the first file.
        //
        // A non-racy assert can now be made against this known state.
        //

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_file_count",
            value = 1,
        );

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_file_op_reference_count",
            value = 3, // 5 initial, reduced by 2 via persist notification
        );

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_min_id",
            value = SEGMENT_ID_1.get(),
        );

        // Assert clean shutdown behaviour.
        drop(handle);
        actor_task
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("actor task should stop cleanly")
    }
}
