use std::sync::Arc;

use data_types::sequence_number_set::{self, SequenceNumberSet};
use hashbrown::HashMap;
use metric::U64Gauge;
use observability_deps::tracing::{debug, info};
use tokio::{
    select,
    sync::{mpsc, oneshot, Notify},
};
use wal::SegmentId;

use crate::{
    persist::completion_observer::CompletedPersist, wal::reference_tracker::WalFileDeleter,
};

/// A WAL file reference-count tracker.
///
/// See [`WalReferenceHandle`].
///
/// [`WalReferenceHandle`]: super::WalReferenceHandle
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
    /// [`data_types::SequenceNumber`] they contain.
    ///
    /// These [`SequenceNumberSet`] are slowly drained / have IDs removed in
    /// response to persisted data notifications. Once the set is of length 0,
    /// the file can be deleted as all the entries the file contains has been
    /// persisted.
    ///
    /// Invariant: sets in this map are always non-empty.
    wal_files: HashMap<wal::SegmentId, SequenceNumberSet>,

    /// A semaphore to wake tasks waiting for the number of inactive
    /// (old/rotated) WAL segments (in wal_files) to reach 0.
    ///
    /// This can happen multiple times during the lifecycle of an ingester.
    empty_waker: Arc<Notify>,

    /// Channels for input from the [`WalReferenceHandle`].
    ///
    /// [`WalReferenceHandle`]: super::WalReferenceHandle
    file_rx: mpsc::Receiver<(SegmentId, SequenceNumberSet, oneshot::Sender<()>)>,
    persist_rx: mpsc::Receiver<Arc<CompletedPersist>>,
    unbuffered_rx: mpsc::Receiver<SequenceNumberSet>,

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
    pub(super) fn new(
        wal: T,
        file_rx: mpsc::Receiver<(SegmentId, SequenceNumberSet, oneshot::Sender<()>)>,
        persist_rx: mpsc::Receiver<Arc<CompletedPersist>>,
        unbuffered_rx: mpsc::Receiver<SequenceNumberSet>,
        empty_waker: Arc<Notify>,
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
            empty_waker,
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

                Some((id, f, done)) = self.file_rx.recv() => {
                    self.handle_new_file(id, f).await;
                    let _ = done.send(()); // There may be no listener.
                },
                Some(p) = self.persist_rx.recv() => self.handle_persisted(p).await,
                Some(i) = self.unbuffered_rx.recv() => self.handle_unbuffered(i).await,
                else => break
            }

            // After each action is processed, update the metrics and wake any
            // waiters if the tracker has no unpersisted operations.
            self.update_metrics();
            self.notify_empty_wakers();
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
    /// Because the write was added to the WAL, its ID set will be part of the WAL
    /// file's [`SequenceNumberSet`], but because the write was not buffered, it
    /// will never be persisted and therefore the WAL set will always have an
    /// outstanding reference unless it is accounted for here.
    async fn handle_unbuffered(&mut self, set: SequenceNumberSet) {
        debug!(sequence_number_set = ?set, "notified of unbuffered write");

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

    /// Wake the waiters when the last inactive WAL segment file is deleted.
    fn notify_empty_wakers(&self) {
        if self.wal_files.is_empty() {
            self.empty_waker.notify_waiters();
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

// Tests in actor.rs
