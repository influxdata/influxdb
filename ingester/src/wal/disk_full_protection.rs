use std::sync::Arc;

use async_trait::async_trait;
use observability_deps::tracing::*;
use tokio::{sync::watch::Receiver, task::JoinHandle};
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
    async fn persist_and_tidy(&self);
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
    async fn persist_and_tidy(&self) {
        let (closed_segment, sequence_number_set) =
            self.wal.rotate().expect("failed to rotate wal");
        debug!(
            closed_id = %closed_segment.id(),
            segment_bytes = closed_segment.size(),
            n_ops = sequence_number_set.len(),
            "rotated wal to allow disk clean-up",
        );

        let empty_waker = self.wal_reference_handle.empty_inactive_notifier();

        _ = self
            .wal_reference_handle
            .enqueue_rotated_file(closed_segment.id(), sequence_number_set)
            .await
            .await;

        persist_partitions(self.buffer.partition_iter(), &self.persist).await;
        debug!(closed_id = %closed_segment.id(), "partitions persisted to allow disk clean-up");

        empty_waker.await;
    }
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
    let mut cleanup_handle: Option<JoinHandle<()>> = Default::default();
    let persisting_cleaner = Arc::new(persisting_cleaner);

    // Listen for new disk space snapshots in a loop, assessing the usage ratio
    // and taking protective action if needed.
    loop {
        // If the sender has disconnected, this task cannot do anything
        // meaningful and is likely a signal that shutdown has been invoked
        // - abort the task..
        if disk_snapshot_rx.changed().await.is_err() {
            return;
        }

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
                "safe wal disk usage ratio threshold exceeded, blocking ingest until capacity is freed up"
            );
        }

        if let Some(existing_cleanup) = &cleanup_handle {
            // An existing cleanup task may still be in progress, wait for it
            // to finish before starting another one.
            if !existing_cleanup.is_finished() {
                continue;
            }
        }

        // Start a cleanup task to persist outstanding data and tidy up the
        // disk, keeping a handle to ensure only one is running at any given
        // time.
        cleanup_handle = Some(tokio::spawn({
            info!("persisting outstanding writes and tidying up old files");
            let persisting_cleaner = Arc::clone(&persisting_cleaner);
            async move {
                persisting_cleaner.persist_and_tidy().await;
            }
        }));
    }
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
    use tokio::sync::watch;
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
            .persist_and_tidy()
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
        async fn persist_and_tidy(&self) {
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
