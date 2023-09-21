use std::sync::Arc;

use async_trait::async_trait;
use observability_deps::tracing::*;
use tokio::{
    sync::mpsc,
    sync::{mpsc::error::TrySendError, watch::Receiver},
};
use tracker::DiskSpaceSnapshot;

use super::reference_tracker::WalReferenceHandle;
use crate::{
    ingest_state::{IngestState, IngestStateError},
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};

/// The usage threshold (as a percentage [0,1.0]) at which the disk full
/// protection must kick in and stop new writes while the active WAL segment
/// is rotated, persisted and given the time to be dropped.
const DISK_USAGE_RATIO_THRESHOLD: f64 = 0.9;

/// A type that opaquely handles the persistence and tidying of outstanding
/// data taking up disk space.
#[async_trait]
pub(crate) trait PersistingCleaner {
    async fn persist_and_clean(&self);
}

/// A [`wal::Wal`] and [`PartitionIter`] based implementation of a
/// [`PersistingCleaner`], capable of rotating the active WAL segment,
/// persisting in-memory data and waiting for segments to be removed from disk.
#[derive(Debug)]
pub(crate) struct WalPersister<T, P> {
    wal: Arc<wal::Wal>,
    wal_reference_handle: WalReferenceHandle,
    buffer: T,
    persist: P,
}

impl<T, P> WalPersister<T, P>
where
    T: PartitionIter + Sync,
    P: PersistQueue + Clone,
{
    /// Create a new [`wal::Wal`] based [`PersistingCleaner`] that uses the given
    /// [`WalReferenceHandle`] to control tidying up of fully persisted WAL
    /// segments.
    pub fn new(
        wal: Arc<wal::Wal>,
        wal_reference_handle: WalReferenceHandle,
        buffer: T,
        persist: P,
    ) -> Self {
        Self {
            wal,
            wal_reference_handle,
            buffer,
            persist,
        }
    }
}

#[async_trait]
impl<T, P> PersistingCleaner for WalPersister<T, P>
where
    T: PartitionIter + Sync,
    P: PersistQueue + Clone,
{
    /// Rotate the WAL, notifying the reference tracker and persisting the buffer-tree.
    ///
    /// This function waits for the set of inactive WAL files to be empty before returning.
    async fn persist_and_clean(&self) {
        let (closed_segment, sequence_number_set) =
            self.wal.rotate().expect("failed to rotate wal");
        info!(
            closed_id = %closed_segment.id(),
            segment_bytes = closed_segment.size(),
            n_ops = sequence_number_set.len(),
            "rotated wal to allow disk clean-up to occur",
        );

        _ = self
            .wal_reference_handle
            .enqueue_rotated_file(closed_segment.id(), sequence_number_set)
            .await
            .await;

        persist_partitions(self.buffer.partition_iter(), &self.persist).await;
        info!(closed_id = %closed_segment.id(), "partitions persisted to allow disk clean-up to occur");
    }
}

/// A simple actor loop which invokes the provided [`PersistingCleaner`] for
/// each enqueued notification, exiting when the paired [`mpsc::Sender`] is
/// dropped. At most one persist and clean job will be in progress at once,
/// subsequent notifications queue up on `notify_rx`.
async fn persisting_cleaner_actor_loop<P>(mut notify_rx: mpsc::Receiver<()>, persisting_cleaner: P)
where
    P: PersistingCleaner + Send + Sync,
{
    while notify_rx.recv().await.is_some() {
        info!("received request to persist buffered writes and clean up persisted files from the disk");
        persisting_cleaner.persist_and_clean().await;
    }

    info!("stopping disk protection persisting cleaner actor loop");
}

/// Protect the ingester from the disk watched by `disk_snapshot_rx` filling up,
/// marking the provided `ingest_state` with an error while persisting
/// outstanding partitions until enough space is free to clear the [`IngestStateError`].
///
/// This task runs until it detects the [`Receiver`] is disconnected.
pub(crate) async fn guard_disk_capacity<P>(
    mut disk_snapshot_rx: Receiver<DiskSpaceSnapshot>,
    ingest_state: Arc<IngestState>,
    persisting_cleaner: P,
) where
    P: PersistingCleaner + Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(persisting_cleaner_actor_loop(rx, persisting_cleaner));

    // Listen for new disk space snapshots in a loop, assessing the usage ratio,
    // setting ingest state and enqueuing clean-up action if needed.
    while disk_snapshot_rx.changed().await.is_ok() {
        let snapshot = *disk_snapshot_rx.borrow();
        let observed_disk_usage_ratio = snapshot.disk_usage_ratio();

        if observed_disk_usage_ratio < DISK_USAGE_RATIO_THRESHOLD {
            if ingest_state.unset(IngestStateError::DiskFull) {
                info!(
                    observed_disk_usage_ratio,
                    DISK_USAGE_RATIO_THRESHOLD,
                    "wal disk usage ratio back within safe threshold, re-enabling ingest"
                );
            }
            continue;
        }

        // Usage threshold for disk capacity has been breached, block the ingest
        // and rotate the WAL to protect the durability of the data.
        if ingest_state.set(IngestStateError::DiskFull) {
            warn!(
                observed_disk_usage_ratio,
                DISK_USAGE_RATIO_THRESHOLD,
                available_disk_space=snapshot.available_disk_space(),
                total_disk_space=snapshot.total_disk_space(),
                "safe wal disk usage ratio threshold exceeded, blocking ingest until capacity is freed up"
            );
        }

        // Enqueue a cleanup task to persist outstanding data and clean up the
        // disk. It is fine if the queue is full, as another clean up will happen
        // despite the message being dropped.
        match tx.try_send(()) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => {
                panic!("disk protection persisting cleaner actor not running")
            }
            Err(TrySendError::Full(_)) => {
                debug!("existing ongoing persist and clean job queued")
            }
        }
    }

    // If the sender has disconnected, this task cannot do anything
    // meaningful and is likely a signal that shutdown has been invoked
    // - abort the task.
    info!("stopping disk full protection task");
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::future::pending;
    use futures::{future::ready, Future};
    use parking_lot::Mutex;
    use tempfile::tempdir;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::{oneshot, watch};
    use tracker::DiskSpaceSnapshot;

    use crate::dml_payload::IngestOp;
    use crate::persist::queue::mock::MockPersistQueue;
    use crate::test_util::{
        make_write_op, PartitionDataBuilder, ARBITRARY_NAMESPACE_ID, ARBITRARY_PARTITION_KEY,
        ARBITRARY_TABLE_ID, ARBITRARY_TABLE_NAME, ARBITRARY_TRANSITION_PARTITION_ID,
    };
    use crate::wal::traits::WalAppender;

    use super::*;

    #[tokio::test]
    async fn test_wal_persister() {
        let write_op = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            1,
            &format!(
                r#"{},city=Edinburgh laughs="plenny" 10"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );

        let mut p = PartitionDataBuilder::new().build();
        write_op.tables().for_each(|(_, table_data)| {
            p.buffer_write(
                table_data.partitioned_data().data().clone(),
                table_data.partitioned_data().sequence_number(),
            )
            .expect("write should be ok")
        });
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        let tmp_dir = tempdir().expect("no temp dir available");
        let wal = wal::Wal::new(tmp_dir.path())
            .await
            .expect("failed to initialise WAL");
        assert_eq!(wal.closed_segments().len(), 0);

        wal.append(&IngestOp::Write(write_op))
            .changed()
            .await
            .expect("should be able to append to WAL");

        let (wal_reference_handle, wal_reference_actor) =
            WalReferenceHandle::new(Arc::clone(&wal), &metric::Registry::default());
        let persist_handle = Arc::new(MockPersistQueue::new_with_observer(
            wal_reference_handle.clone(),
        ));
        tokio::spawn(wal_reference_actor.run());

        let wal_persister = WalPersister::new(
            Arc::clone(&wal),
            wal_reference_handle.clone(),
            vec![Arc::clone(&p)],
            Arc::clone(&persist_handle),
        );

        // Register an empty waker then call `persist_and_tidy` before ensuring
        // that the waker notifies.
        let empty_waker = wal_reference_handle.empty_inactive_notifier();
        wal_persister
            .persist_and_clean()
            .with_timeout_panic(Duration::from_secs(5))
            .await;
        empty_waker.with_timeout_panic(Duration::from_secs(1)).await;

        // Should be no closed segments remaining after persist and tidy,
        // and the outstanding partition must have been persisted
        assert!(wal.closed_segments().is_empty());
        assert_matches!(persist_handle.calls().as_slice(), [got] => {
            assert_eq!(got.lock().partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
        });
    }

    #[derive(Default)]
    struct MockPersistingCleaner {
        calls: AtomicUsize,
        futures: Mutex<VecDeque<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    }

    impl MockPersistingCleaner {
        fn with_process_futures(
            self,
            futures: impl IntoIterator<Item = Pin<Box<dyn Future<Output = ()> + Send>>>,
        ) -> Self {
            self.futures.lock().extend(futures);
            self
        }
    }

    #[async_trait]
    impl PersistingCleaner for Arc<MockPersistingCleaner> {
        async fn persist_and_clean(&self) {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let f: Pin<Box<dyn Future<Output = ()> + Send>> = self
                .futures
                .lock()
                .pop_front()
                .unwrap_or(Box::pin(ready(())));
            f.await;
        }
    }

    const ARBITRARY_DISK_SIZE: f64 = 1000.0;
    const BUSY_WAIT_SLEEP: u64 = 50;

    async fn wait_for_ok_state(ingest_state: &Arc<IngestState>) {
        loop {
            tokio::time::sleep(Duration::from_millis(BUSY_WAIT_SLEEP)).await;
            if ingest_state.read().is_ok() {
                return;
            }
        }
    }

    async fn wait_for_err_state(ingest_state: &Arc<IngestState>) {
        loop {
            tokio::time::sleep(Duration::from_millis(BUSY_WAIT_SLEEP)).await;
            if ingest_state.read().is_err() {
                return;
            }
        }
    }

    // This test just makes an assertion that the actor loop performs a notified
    // persist and clean if it carrying out one at the time it is notified and
    // that dropping the parent notifier causes the actor to exit.
    #[tokio::test]
    async fn test_actor_queues_request() {
        // Set up the mock cleaner to only unblock when notified for two calls.
        let (tx_unblock, rx_unblock) = oneshot::channel();
        let persisting_cleaner = Arc::new(MockPersistingCleaner::default().with_process_futures([
            Box::pin(async move {
                rx_unblock.await.expect("should receive unblock ok");
            }) as Pin<Box<dyn Future<Output = _> + Send>>,
        ]));

        // Set up the actor loop to process notifications
        let (tx_notify, rx_notify) = mpsc::channel(1);
        let actor_loop_handle = tokio::spawn(persisting_cleaner_actor_loop(
            rx_notify,
            Arc::clone(&persisting_cleaner),
        ));

        // Enqueue two persist & cleans, then make sure the third is not enqueued.
        tx_notify
            .try_send(())
            .expect("first trigger must be picked up");
        tx_notify
            .send_timeout((), Duration::from_millis(500))
            .await
            .expect("second trigger must be queued");
        tx_notify
            .send_timeout((), Duration::from_millis(500))
            .await
            .expect_err("third trigger must be dropped");

        // Check that the first call was made, then unblock it and check the
        // second is made after the loop quit
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);
        tx_unblock.send(()).expect("send should be received");

        drop(tx_notify);
        actor_loop_handle
            .await
            .expect("actor should exit without error during shutdown");

        // Wait for the loop to exit and ensure the second, queued call
        // was processed
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 2);
    }

    // This test just ensures that disk snapshots showing a healthy amount of
    // available disk space don't affect the ingest state or normal write
    // appending to the WAL or persisting from the buffer tree.
    #[tokio::test]
    async fn test_no_action_taken() {
        let persisting_cleaner = Arc::new(MockPersistingCleaner::default());
        let (tx, rx) = watch::channel(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ));
        let ingest_state = Arc::new(IngestState::default());

        let task_handle = tokio::spawn(guard_disk_capacity(
            rx,
            Arc::clone(&ingest_state),
            Arc::clone(&persisting_cleaner),
        ));
        // Ensure the initial ingest state is OK
        assert!(ingest_state.read().is_ok());

        let usage_ratios = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.85];

        // Send a few snapshots that simmer below the threshold and ensure the state never is changed.
        for usage in usage_ratios {
            assert!(usage < DISK_USAGE_RATIO_THRESHOLD);
            let avail_ratio = 1.0 - usage;
            tx.send(DiskSpaceSnapshot::new(
                (avail_ratio * ARBITRARY_DISK_SIZE) as u64,
                ARBITRARY_DISK_SIZE as u64,
            ))
            .expect("should be able to send snapshot");
            assert!(ingest_state.read().is_ok());
        }

        drop(tx);
        task_handle
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("should exit");
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 0);
    }

    // This test starts the guard task, sends a snapshot over the limit, waits
    // for writes to be blocked and then "clears" the disk and ensures writes
    // are unblocked.
    #[tokio::test]
    async fn test_persist_and_clean() {
        let persisting_cleaner = Arc::new(MockPersistingCleaner::default());
        let (tx, rx) = watch::channel(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ));
        let ingest_state = Arc::new(IngestState::default());

        let task_handle = tokio::spawn(guard_disk_capacity(
            rx,
            Arc::clone(&ingest_state),
            Arc::clone(&persisting_cleaner),
        ));
        assert!(ingest_state.read().is_ok());

        // Send a snapshot indicating that the disk usage has reached the threshold.
        let avail_ratio = 1.0 - DISK_USAGE_RATIO_THRESHOLD;
        tx.send(DiskSpaceSnapshot::new(
            (avail_ratio * ARBITRARY_DISK_SIZE) as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Wait for the ingest state to be set to error and ensure persist and tidy was called
        wait_for_err_state(&ingest_state)
            .with_timeout_panic(Duration::from_secs(1))
            .await;
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);

        // Send a snapshot with the "cleaned" disk space available
        tx.send(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Wait for the ingest state to be cleared before dropping the sender
        // and asserting no more calls to the persisting cleaner were made
        wait_for_ok_state(&ingest_state)
            .with_timeout_panic(Duration::from_secs(1))
            .await;

        drop(tx);
        task_handle
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("should exit");
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);
    }

    // This test ensures that the ingest error state can be cleared if disk
    // space is freed up while a call to persist and clean up the WAL is still
    // in progress.
    #[tokio::test]
    async fn test_blocked_persist_and_clean_allows_clearing_of_ingest_state() {
        // Create a blocking cleaner.
        let persisting_cleaner = Arc::new(MockPersistingCleaner::default().with_process_futures([
            Box::pin(pending()) as Pin<Box<dyn Future<Output = _> + Send>>,
        ]));
        let (tx, rx) = watch::channel(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ));
        let ingest_state = Arc::new(IngestState::default());

        let _task_handle = tokio::spawn(guard_disk_capacity(
            rx,
            Arc::clone(&ingest_state),
            Arc::clone(&persisting_cleaner),
        ));
        assert!(ingest_state.read().is_ok());

        let avail_ratio = 1.0 - DISK_USAGE_RATIO_THRESHOLD;
        tx.send(DiskSpaceSnapshot::new(
            (avail_ratio * ARBITRARY_DISK_SIZE) as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Wait for the ingest state to be set to error, and check that the cleaner has been called.
        wait_for_err_state(&ingest_state)
            .with_timeout_panic(Duration::from_secs(1))
            .await;
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);

        tx.send(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Wait for the state to be reset.
        wait_for_ok_state(&ingest_state)
            .with_timeout_panic(Duration::from_secs(1))
            .await;
    }

    // This test ensures that repeated snapshots over the limit do not cause new
    // calls to the persisting cleaner while there is a call in progress.
    #[tokio::test]
    async fn test_existing_persist_prevents_additional_job() {
        // Create a blocking cleaner.
        let persisting_cleaner = Arc::new(MockPersistingCleaner::default().with_process_futures([
            Box::pin(pending()) as Pin<Box<dyn Future<Output = _> + Send>>,
        ]));
        let (tx, rx) = watch::channel(DiskSpaceSnapshot::new(
            ARBITRARY_DISK_SIZE as u64,
            ARBITRARY_DISK_SIZE as u64,
        ));
        let ingest_state = Arc::new(IngestState::default());

        let task_handle = tokio::spawn(guard_disk_capacity(
            rx,
            Arc::clone(&ingest_state),
            Arc::clone(&persisting_cleaner),
        ));
        assert!(ingest_state.read().is_ok());

        let avail_ratio = 1.0 - DISK_USAGE_RATIO_THRESHOLD;
        tx.send(DiskSpaceSnapshot::new(
            (avail_ratio * ARBITRARY_DISK_SIZE) as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Wait for the ingest state to be set to error, and check that the cleaner has been called.
        wait_for_err_state(&ingest_state)
            .with_timeout_panic(Duration::from_secs(1))
            .await;
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);

        tx.send(DiskSpaceSnapshot::new(
            (avail_ratio * ARBITRARY_DISK_SIZE) as u64,
            ARBITRARY_DISK_SIZE as u64,
        ))
        .expect("should be able to send snapshot");

        // Drop the sender and wait for the task to finish before asserting
        // the error state is still set and no more calls were made.
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(tx);
        task_handle
            .with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("should exit");

        assert!(ingest_state.read().is_err());
        assert_eq!(persisting_cleaner.calls.load(Ordering::Relaxed), 1);
    }
}
