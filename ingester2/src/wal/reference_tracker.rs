//! A WAL file reference tracker, responsible for deleting files that contain
//! entirely persisted data.

use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use data_types::{
    sequence_number_set::{self, SequenceNumberSet},
    SequenceNumber,
};
use hashbrown::HashMap;
use metric::U64Gauge;
use observability_deps::tracing::{debug, info, warn};
use tokio::{
    select,
    sync::mpsc::{self, error::TrySendError},
};
use wal::SegmentId;

use crate::persist::completion_observer::CompletedPersist;

/// An abstraction defining the ability of an implementer to delete WAL segment
/// files by ID.
#[async_trait]
pub(crate) trait WalFileDeleter: Debug + Send + Sync + 'static {
    /// Delete the WAL segment with the specified [`SegmentId`], or panic if
    /// deletion fails.
    async fn delete_file(&self, id: SegmentId);
}

#[async_trait]
impl WalFileDeleter for Arc<wal::Wal> {
    async fn delete_file(&self, id: SegmentId) {
        self.delete(id).await.expect("failed to drop wal segment");
    }
}

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

/// A WAL file reference-count tracker.
///
/// See [`WalReferenceHandle`].
#[derive(Debug)]
pub(crate) struct WalReferenceActor<T = Arc<wal::Wal>> {
    wal: T,

    /// The set of IDs of persisted data that do not yet appear in
    /// `wal_files`, the set of WAL files rotated out of active use. This is
    /// an intermediate buffer necessary to tolerate out-of-order persist
    /// notifications w.r.t file notifications.
    ///
    /// IDs that appear in this set are most likely part of the active WAL
    /// segment file and should be reconciled when it rotates.
    persisted: SequenceNumberSet,

    /// The set of closed WAL segment files, and the set of unpersisted
    /// [`SequenceNumber`] they contain.
    ///
    /// These [`SequenceNumberSet`] are slowly drained / have IDs removed in
    /// response to persisted data notifications. Once the set is of length 0,
    /// the file can be deleted as all the entries the file contains has been
    /// persisted.
    ///
    /// Invariant: sets in this map are always non-empty.
    wal_files: HashMap<wal::SegmentId, SequenceNumberSet>,

    /// Channels for input from the [`WalReferenceHandle`].
    file_rx: mpsc::Receiver<(SegmentId, SequenceNumberSet)>,
    persist_rx: mpsc::Receiver<Arc<CompletedPersist>>,
    unbuffered_rx: mpsc::Receiver<SequenceNumber>,

    /// A metric tracking the number of rotated WAL files being reference
    /// tracked.
    num_files: U64Gauge,
    /// The minimum [`SegmentId`] in `wal_files`, the set of old (rotated out)
    /// files that will eventually be deleted.
    ///
    /// If this value never changes over the lifetime of an ingester, it is an
    /// indication of a reference leak bug, causing a WAL file to never be
    /// deleted.
    min_id: U64Gauge,
    /// The number of references to unpersisted operations remaining in the old
    /// (rotated out) WAL files, decreasing as persistence completes, and
    /// increasing as non-empty WAL files are rotated into `wal_files`.
    referenced_ops: U64Gauge,
}

impl<T> WalReferenceActor<T>
where
    T: WalFileDeleter,
{
    fn new(
        wal: T,
        file_rx: mpsc::Receiver<(SegmentId, SequenceNumberSet)>,
        persist_rx: mpsc::Receiver<Arc<CompletedPersist>>,
        unbuffered_rx: mpsc::Receiver<SequenceNumber>,
        metrics: &metric::Registry,
    ) -> Self {
        let num_files = metrics
            .register_metric::<U64Gauge>(
                "ingester_wal_inactive_file_count",
                "number of WAL files that are not being actively wrote to, but contain unpersisted data"
            )
            .recorder(&[]);

        let min_id = metrics
            .register_metric::<U64Gauge>(
                "ingester_wal_inactive_min_id",
                "the segment ID of the oldest inactive wal file",
            )
            .recorder(&[]);

        let referenced_ops = metrics
            .register_metric::<U64Gauge>(
                "ingester_wal_inactive_file_op_reference_count",
                "the number of unpersisted operations referenced in inactive WAL files",
            )
            .recorder(&[]);

        Self {
            wal,
            persisted: SequenceNumberSet::default(),
            wal_files: HashMap::with_capacity(3),
            file_rx,
            persist_rx,
            unbuffered_rx,
            num_files,
            min_id,
            referenced_ops,
        }
    }

    /// Execute the actor task.
    ///
    /// This task exits once the sender side of the input channels have been
    /// dropped.
    pub(crate) async fn run(mut self) {
        loop {
            select! {
                // Prefer polling the channels in the specified order.
                //
                // By consuming file_rx first, there's a greater chance that
                // subsequent persist/ignore events can be applied directly to
                // the file sets, rather than having to wait in the intermediate
                // "persisted" set, reducing memory utilisation.
                biased;

                Some((id, f)) = self.file_rx.recv() => self.handle_new_file(id, f).await,
                Some(p) = self.persist_rx.recv() => self.handle_persisted(p).await,
                Some(i) = self.unbuffered_rx.recv() => self.handle_unbuffered(i).await,
                else => break
            }

            // After each action is processed, update the metrics.
            self.update_metrics();
        }

        debug!("stopping wal reference counter task");
    }

    /// Update the metrics to match the internal state.
    fn update_metrics(&self) {
        let num_files = self.wal_files.len();

        // Build a set of (id, set_len) tuples for debug logging.
        let id_lens = self
            .wal_files
            .iter()
            .map(|(id, set)| (*id, set.len()))
            .collect::<Vec<_>>();

        // Emit a log for debugging purposes, showing the current state.
        debug!(
            num_files,
            files=?id_lens,
            persisted_set_len=self.persisted.len(),
            "updated reference state"
        );

        // Reduce (id, set_len) tuples to the min ID and sum of the set lengths,
        // defaulting to 0 for the length and u64::MAX for the ID if the file
        // set is empty.
        let (min_id, referenced_ops) =
            id_lens
                .into_iter()
                .fold((u64::MAX, 0), |(id_min, len_sum), e| {
                    assert!(e.1 > 0); // Invariant: sets in file map are never empty
                    (id_min.min(e.0.get()), len_sum + e.1)
                });

        // And update the various exported metrics.
        self.num_files.set(num_files as _);
        self.min_id.set(min_id);
        self.referenced_ops.set(referenced_ops);
    }

    /// Track a newly rotated WAL segment, with the given [`SegmentId`] and
    /// containing the operations specified in [`SequenceNumberSet`].
    ///
    /// This method tolerates an empty `set`.
    async fn handle_new_file(&mut self, segment_id: SegmentId, mut set: SequenceNumberSet) {
        debug!(
            %segment_id,
            sequence_number_set = ?set,
            "notified of new segment file"
        );

        // Clear the overlap between the "persisted" set, and this new file from
        // both.
        let n = clear_intersection(&mut self.persisted, &mut set);
        if n > 0 {
            debug!(n, "released previously persisted IDs");
        }

        // If the file set is now completely empty, it can be immediately
        // deleted.
        if set.is_empty() {
            debug!(n, "immediately dropping empty segment file");
            return delete_file(&self.wal, segment_id).await;
        }

        // Otherwise, retain this file for later persist notifications.
        //
        // Run-optimise the bitmap to minimise memory utilisation of this set.
        // This is a relatively fast operation, and the file sets are expected
        // to be highly suitable for RLE compression due to the monotonic
        // sequence number assignments.
        set.run_optimise();

        // Insert the file set into the files being tracked
        assert!(!set.is_empty()); // Invariant: sets in file map are never empty
        assert!(
            self.wal_files.insert(segment_id, set).is_none(),
            "duplicate segment ID"
        );
    }

    /// Process a persistence completion notification, decreasing the reference
    /// counts against tracked WAL files, and holding any remaining IDs (in the
    /// untracked active WAL segment) in a temporary "persisted" buffer.
    async fn handle_persisted(&mut self, note: Arc<CompletedPersist>) {
        debug!(
            namespace_id = %note.namespace_id(),
            table_id = %note.table_id(),
            partition_id = %note.partition_id(),
            sequence_number_set = ?note.sequence_numbers(),
            "notified of persisted data"
        );

        self.remove(note.owned_sequence_numbers()).await;
    }

    /// Handle a write that has been added to the WAL, but that did not complete
    /// / buffer.
    ///
    /// Because the write was added to the WAL, its ID will be part of the WAL
    /// file's [`SequenceNumberSet`], but because the write was not buffered, it
    /// will never be persisted and therefore the WAL set will always have an
    /// outstanding reference unless it is accounted for here.
    async fn handle_unbuffered(&mut self, id: SequenceNumber) {
        debug!(sequence_number = id.get(), "notified of unbuffered write");

        // Delegate to the same code as persisted by presenting this ID as a set
        // - the same behaviour is required.
        let mut set = SequenceNumberSet::with_capacity(1);
        set.add(id);

        self.remove(set).await;
    }

    /// Remove the intersection of `set` from all the sets in `self` (file sets,
    /// and the untracked / "persisted" buffer set).
    ///
    /// Deletes all WAL files that are no longer referenced / have unpersisted
    /// entries.
    async fn remove(&mut self, mut set: SequenceNumberSet) {
        // First remove this set from the "persisted" / file-less set.
        let n = clear_intersection(&mut set, &mut self.persisted);
        if n > 0 {
            debug!(n, "released previously persisted IDs");
        }

        if set.is_empty() {
            debug!(n, "fully matched previously persisted IDs");
            return;
        }

        // And then walk the WAL file sets.
        let mut remove_ids = Vec::with_capacity(0);
        for (id, file_set) in self.wal_files.iter_mut() {
            // Invariant: files in the file set always have at least 1 reference
            assert!(!file_set.is_empty());

            // Early exit the loop if possible.
            if set.is_empty() {
                break;
            }

            // Clear the intersection of both sets.
            let n = clear_intersection(&mut set, file_set);
            if n == 0 {
                continue;
            }

            debug!(n, segment_id=%id, "matched file IDs");

            // At least 1 element was removed from the file set, it may now be
            // empty.
            if file_set.is_empty() {
                remove_ids.push(*id);
            }
        }

        // Union whatever IDs remain with the file-less persisted set.
        if !set.is_empty() {
            debug!(n = set.len(), "retaining file-less IDs");
            self.persisted.add_set(&set);
        }

        // And delete any newly empty files
        for id in remove_ids {
            let file_set = self
                .wal_files
                .remove(&id)
                .expect("id was obtained during iter");

            // Invariant: the file being removed always has no references.
            assert!(file_set.is_empty());

            delete_file(&self.wal, id).await
        }
    }
}

/// Remove the intersection of `a` and `b`, from both `a` and `b`, and return
/// the cardinality of the intersection.
fn clear_intersection(a: &mut SequenceNumberSet, b: &mut SequenceNumberSet) -> usize {
    let intersection = sequence_number_set::intersect(a, b);

    a.remove_set(&intersection);
    b.remove_set(&intersection);

    intersection.len() as _
}

/// Delete the specified WAL segment from `wal`, and log it at info.
async fn delete_file<T>(wal: &T, id: SegmentId)
where
    T: WalFileDeleter,
{
    info!(
        %id,
        "deleted fully-persisted wal segment"
    );

    wal.delete_file(id).await
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionId, TableId};
    use futures::Future;
    use metric::assert_counter;
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
