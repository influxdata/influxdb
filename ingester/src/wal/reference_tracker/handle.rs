use std::{fmt::Debug, sync::Arc};

use data_types::sequence_number_set::SequenceNumberSet;
use futures::Future;
use observability_deps::tracing::warn;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    oneshot, Notify,
};
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
///   * All [`data_types::SequenceNumber`] of writes that failed to buffer
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
    /// [`data_types::SequenceNumber`] within them.
    ///
    /// The provided channel should be sent on once the file has been processed.
    file_tx: mpsc::Sender<(SegmentId, SequenceNumberSet, oneshot::Sender<()>)>,

    /// A steam of persist notifications - the [`SequenceNumberSet`] of the
    /// persisted data that is now durable in object storage, and which no
    /// longer requires WAL entries for.
    persist_tx: mpsc::Sender<Arc<CompletedPersist>>,

    /// A stream of [`SequenceNumberSet`] identifying operations that have been (or
    /// will be) added to the WAL, but failed to buffer/complete. These should
    /// be treated as if they were "persisted", as they will never be persisted,
    /// and are not expected to remain durable (user did not get an ACK).
    unbuffered_tx: mpsc::Sender<SequenceNumberSet>,

    /// A semaphore to wake tasks waiting for the number inactive WAL segement
    /// files reaches 0.
    ///
    /// This can happen multiple times during the lifecycle of an ingester.
    empty_waker: Arc<Notify>,
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
        let empty_waker = Default::default();

        let actor = WalReferenceActor::new(
            wal,
            file_rx,
            persist_rx,
            unbuffered_rx,
            Arc::clone(&empty_waker),
            metrics,
        );

        (
            Self {
                file_tx,
                persist_tx,
                unbuffered_tx,
                empty_waker,
            },
            actor,
        )
    }

    /// Enqueue a new file rotation event, providing the [`SegmentId`] of the
    /// WAL file and the [`SequenceNumberSet`] the WAL segment contains.
    pub(crate) async fn enqueue_rotated_file(
        &self,
        segment_id: SegmentId,
        set: SequenceNumberSet,
    ) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        Self::send(&self.file_tx, (segment_id, set, tx)).await;
        rx
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
    pub(crate) async fn enqueue_unbuffered_write(&self, set: SequenceNumberSet) {
        Self::send(&self.unbuffered_tx, set).await
    }

    /// A future that resolves when there are no partially persisted / inactive
    /// WAL segment files known to the reference tracker.
    ///
    /// NOTE: the active WAL segment file may contain unpersisted operations!
    /// The tracker is only aware of inactive/rotated-out WAL files.
    ///
    /// NOTE: the number of references may reach 0 multiple times over the
    /// lifetime of an ingester.
    ///
    /// # Ordering
    ///
    /// Calling this method must happen-before the number of files reaches 0.
    /// The number of files reaching zero happens-before the wakers are woken.
    ///
    /// A caller must call this method before the number of inactive files
    /// reaches 0 to be woken, otherwise the future will resolve the next time 0
    /// is reached.
    ///
    /// Calls to [`WalReferenceHandle::enqueue_rotated_file()`] are executed
    /// asynchronously; callers should use the returned completion handle when
    /// enqueuing to order processing of the file before emptying of the
    /// inactive file set (this notification future).
    pub(crate) fn empty_inactive_notifier(&self) -> impl Future<Output = ()> + '_ {
        self.empty_waker.notified()
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
    use super::*;
    use crate::test_util::new_persist_notification;
    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use data_types::SequenceNumber;
    use futures::{task::Context, Future, FutureExt};
    use metric::{assert_counter, U64Gauge};
    use parking_lot::Mutex;
    use std::{pin::Pin, task::Poll, time::Duration};
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::Notify;

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
        fn deleted_file_waker(&self) -> impl Future<Output = ()> + '_ {
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
        T: IntoIterator<Item = u64>,
    {
        vals.into_iter().map(SequenceNumber::new).collect()
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
            .enqueue_rotated_file(SEGMENT_ID, new_set([1, 2, 3, 4, 5, 6]))
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // Submit a persist notification that removes refs 1 & 2.
        handle
            .enqueue_persist_notification(new_persist_notification([1, 2]))
            .await;

        // Ensure the file was not deleted
        assert!(wal.calls().is_empty());

        // Enqueue a unbuffered notification (out of order)
        handle.enqueue_unbuffered_write(new_set([5, 6])).await;

        // Ensure the file was not deleted
        assert!(wal.calls().is_empty());

        // Finally release the last IDs
        let deleted_file_waker = wal.deleted_file_waker();
        handle
            .enqueue_persist_notification(new_persist_notification([3, 4]))
            .await;

        // Wait for it to be processed
        deleted_file_waker.await;

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
        handle
            .enqueue_persist_notification(new_persist_notification([2]))
            .await;
        handle
            .enqueue_persist_notification(new_persist_notification([1]))
            .await;
        handle
            .enqueue_persist_notification(new_persist_notification([3, 4]))
            .await;

        // Add a file with IDs 1, 2, 3
        let deleted_file_waker = wal.deleted_file_waker();
        handle
            .enqueue_rotated_file(SEGMENT_ID_1, new_set([1, 2, 3]))
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // Wait for it to be processed
        deleted_file_waker.await;

        // Validate the correct ID was deleted
        assert_matches!(wal.calls().as_slice(), &[v] if v == SEGMENT_ID_1);

        // Enqueue the second WAL, covering 4
        handle
            .enqueue_rotated_file(SEGMENT_ID_2, new_set([4, 5, 6]))
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // At this point, the second WAL still has references outstanding (5, 6)
        // and should not have been deleted.
        assert_eq!(wal.calls().len(), 1);

        // Release one of the remaining two refs
        handle
            .enqueue_persist_notification(new_persist_notification([6]))
            .await;

        // Still no deletion
        assert_eq!(wal.calls().len(), 1);

        // And finally release the last ref via an unbuffered notification
        let empty_waker = handle.empty_inactive_notifier();
        let deleted_file_waker = wal.deleted_file_waker();
        handle.enqueue_unbuffered_write(new_set([5])).await;
        deleted_file_waker.await;

        // Validate the correct ID was deleted
        assert_matches!(wal.calls().as_slice(), &[a, b] => {
            assert_eq!(a, SEGMENT_ID_1);
            assert_eq!(b, SEGMENT_ID_2);
        });

        // Wait for the "it's empty!" waker to fire, indicating the tracker has
        // no references to unpersisted data.
        empty_waker.with_timeout_panic(Duration::from_secs(5)).await;

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
        let empty_waker = handle.empty_inactive_notifier();
        let deleted_file_waker = wal.deleted_file_waker();
        handle
            .enqueue_rotated_file(SEGMENT_ID, SequenceNumberSet::default())
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // Wait for the file deletion.
        deleted_file_waker.await;
        assert_matches!(wal.calls().as_slice(), &[v] if v == SEGMENT_ID);

        // Wait for the "it's empty!" waker to fire, indicating the tracker has
        // no references to unpersisted data.
        empty_waker.with_timeout_panic(Duration::from_secs(5)).await;

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
        assert!(SEGMENT_ID_2 < SEGMENT_ID_1); // Test invariant

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        // Add a file with 4 references
        handle
            .enqueue_rotated_file(SEGMENT_ID_1, new_set([1, 2, 3, 4, 5]))
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // Reduce the reference count for file 1 (leaving 3 references)
        handle
            .enqueue_persist_notification(new_persist_notification([1, 2]))
            .await;

        // Enqueue the second file.
        handle
            .enqueue_rotated_file(SEGMENT_ID_2, new_set([6]))
            .with_timeout_panic(Duration::from_secs(5))
            .flatten()
            .await
            .expect("did not receive file processed notification");

        // The second file was completed processed, so the minimum segment ID
        // MUST now be 24.

        assert_counter!(
            metrics,
            U64Gauge,
            "ingester_wal_inactive_min_id",
            value = SEGMENT_ID_2.get(),
        );

        // Release the references to file 2
        let deleted_file_waker = wal.deleted_file_waker();
        handle
            .enqueue_persist_notification(new_persist_notification([6]))
            .await;

        deleted_file_waker.await;

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

    /// Ensure the empty notification only fires when there are no referenced
    /// WAL files and no entries in the persisted set.
    #[tokio::test]
    async fn test_empty_notifications() {
        const SEGMENT_ID_1: SegmentId = SegmentId::new(24);
        const SEGMENT_ID_2: SegmentId = SegmentId::new(42);

        let metrics = metric::Registry::default();
        let wal = Arc::new(MockWalDeleter::default());
        let (handle, actor) = WalReferenceHandle::new(Arc::clone(&wal), &metrics);

        let actor_task = tokio::spawn(actor.run());

        {
            let empty_waker = handle.empty_inactive_notifier();

            // Allow the waker future to be polled explicitly.
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            futures::pin_mut!(empty_waker);

            // Add a file
            handle
                .enqueue_rotated_file(SEGMENT_ID_1, new_set([1, 2, 3, 4]))
                .with_timeout_panic(Duration::from_secs(5))
                .flatten()
                .await
                .expect("did not receive file processed notification");

            // Remove some file references, leaving 1 reference
            handle
                .enqueue_persist_notification(new_persist_notification([1, 2]))
                .await;

            // The tracker is not empty, so the future must not resolve.
            assert_matches!(Pin::new(&mut empty_waker).poll(&mut cx), Poll::Pending);

            // Add a persist notification, populating the "persisted" set.
            handle
                .enqueue_persist_notification(new_persist_notification([5]))
                .await;

            // Release the file reference, leaving only the above reference
            // remaining (id=5)
            handle.enqueue_unbuffered_write(new_set([3, 4])).await;

            // The tracker is not empty, so the future must not resolve.
            assert_matches!(Pin::new(&mut empty_waker).poll(&mut cx), Poll::Pending);

            // Add a new file with two references, releasing id=5.
            handle
                .enqueue_rotated_file(SEGMENT_ID_2, new_set([5, 6]))
                .with_timeout_panic(Duration::from_secs(5))
                .flatten()
                .await
                .expect("did not receive file processed notification");

            // The tracker is not empty, so the future must not resolve.
            assert_matches!(Pin::new(&mut empty_waker).poll(&mut cx), Poll::Pending);

            // Finally release the last reference (id=6)
            handle.enqueue_unbuffered_write(new_set([6])).await;

            // The future MUST now resolve.
            empty_waker.with_timeout_panic(Duration::from_secs(5)).await;
        }

        // Assert clean shutdown behaviour.
        drop(handle);
        actor_task
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("actor task should stop cleanly")
    }
}
