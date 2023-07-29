use observability_deps::tracing::*;
use std::{sync::Arc, time::Duration};

use crate::{
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
    wal::reference_tracker::WalReferenceHandle,
};

/// Rotate the `wal` segment file every `period` duration of time, notifying
/// the [`WalReferenceHandle`].
pub(crate) async fn periodic_rotation<T, P>(
    wal: Arc<wal::Wal>,
    period: Duration,
    wal_reference_handle: WalReferenceHandle,
    buffer: T,
    persist: P,
) where
    T: PartitionIter + Sync + 'static,
    P: PersistQueue + Clone + 'static,
{
    let mut interval = tokio::time::interval(period);

    // The first tick completes immediately. We want to wait one interval before rotating the wal
    // and persisting for the first time, so tick once outside the loop first.
    interval.tick().await;

    loop {
        interval.tick().await;
        info!("rotating wal file");

        let (stats, ids) = wal.rotate().expect("failed to rotate WAL");
        debug!(
            closed_id = %stats.id(),
            segment_bytes = stats.size(),
            n_ops = ids.len(),
            "rotated wal"
        );
        wal_reference_handle
            .enqueue_rotated_file(stats.id(), ids)
            .await;

        // Do not block the ticker while partitions are persisted to ensure
        // timely ticking.
        //
        // This ticker loop MUST make progress and periodically enqueue
        // partition data to the persist system - this ensures that given a
        // blocked persist system (i.e. object store outage) the persist queue
        // grows until it reaches saturation and the ingester rejects writes. If
        // the persist ticker does not make progress, the buffer tree grows
        // instead of the persist queue, until the ingester OOMs.
        //
        // There's no need to retain a handle to the task here; any panics are
        // fatal, and nothing needs to wait for completion (except graceful
        // shutdown, which works without having to wait for notifications from
        // in-flight persists, but rather evaluates the buffer tree state to
        // determine completeness instead). The spawned task eventually deletes
        // the right WAL segment regardless of concurrent tasks.
        tokio::spawn({
            let persist = persist.clone();
            let iter = buffer.partition_iter();
            async move {
                // Drain the BufferTree of partition data and persist each one.
                //
                // Writes that landed into the partition buffer after the rotation but
                // before the partition data is read will be included in the parquet
                // file, but this is not a problem in the happy case (they will not
                // appear in the next persist too).
                //
                // In the case of an ingester crash after these partitions (with their
                // extra writes) have been persisted, the ingester will replay them and
                // re-persist them, causing a small number of duplicate writes to be
                // present in object storage that must be asynchronously compacted later
                // - a small price to pay for not having to block ingest while the WAL
                // is rotated, all outstanding writes + queries complete, and all then
                // partitions are marked as persisting.

                persist_partitions(iter, &persist).await;

                debug!(
                    closed_id = %stats.id(),
                    "partitions persisted"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use async_trait::async_trait;
    use parking_lot::Mutex;
    use tempfile::tempdir;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::oneshot;
    use wal::WriteResult;

    use super::*;
    use crate::{
        buffer_tree::partition::{persisting::PersistingData, PartitionData},
        dml_payload::IngestOp,
        persist::queue::mock::MockPersistQueue,
        test_util::{
            make_write_op, new_persist_notification, PartitionDataBuilder, ARBITRARY_NAMESPACE_ID,
            ARBITRARY_PARTITION_KEY, ARBITRARY_TABLE_ID, ARBITRARY_TABLE_NAME,
            ARBITRARY_TRANSITION_PARTITION_ID,
        },
        wal::traits::WalAppender,
    };

    const TICK_INTERVAL: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn test_notify_rotate_persist() {
        let metrics = metric::Registry::default();

        // Create a write operation to stick in the WAL, and create a partition
        // iter from the data within it to mock out the buffer tree.
        let write_op = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            1,
            &format!(
                r#"{},city=London people=2,pigeons="millions" 10"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );

        let mut p = PartitionDataBuilder::new().build();
        for (_, table_data) in write_op.tables() {
            let partitioned_data = table_data.partitioned_data();
            p.buffer_write(
                partitioned_data.data().clone(),
                partitioned_data.sequence_number(),
            )
            .expect("write should succeed");
        }
        // Wrap the partition in the lock.
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        // Initialise a mock persist queue to inspect the calls made to the
        // persist subsystem.
        let persist_handle = Arc::new(MockPersistQueue::default());

        // Initialise the WAL, write the operation to it
        let tmp_dir = tempdir().expect("no temp dir available");
        let wal = wal::Wal::new(tmp_dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 0);

        let mut write_result = wal.append(&IngestOp::Write(write_op));

        write_result
            .changed()
            .await
            .expect("should be able to get WAL write result");

        assert_matches!(
            write_result
                .borrow()
                .as_ref()
                .expect("WAL should always return result"),
            WriteResult::Ok(_),
            "test write should succeed"
        );

        let (wal_reference_handle, wal_reference_actor) =
            WalReferenceHandle::new(Arc::clone(&wal), &metrics);
        tokio::spawn(wal_reference_actor.run());

        // Start the rotation task
        let rotate_task_handle = tokio::spawn(periodic_rotation(
            Arc::clone(&wal),
            TICK_INTERVAL,
            wal_reference_handle.clone(),
            vec![Arc::clone(&p)],
            Arc::clone(&persist_handle),
        ));

        tokio::time::pause();
        tokio::time::advance(TICK_INTERVAL).await;
        tokio::time::resume();

        // Wait for the WAL to rotate, causing 1 closed segment to exist.
        async {
            loop {
                if !wal.closed_segments().is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // There should be exactly 1 segment.
        let mut segments = wal.closed_segments();
        assert_eq!(segments.len(), 1);
        let closed_segment = segments.pop().unwrap();

        // Send a persistence notification to allow the actor to delete
        // the WAL file
        wal_reference_handle
            .enqueue_persist_notification(new_persist_notification([1]))
            .await;

        // Wait for the closed segment to no longer appear in the WAL,
        // indicating deletion
        async {
            loop {
                if wal
                    .closed_segments()
                    .iter()
                    .all(|s| s.id() != closed_segment.id())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Stop the task and assert the state of the persist queue
        rotate_task_handle.abort();

        assert_matches!(persist_handle.calls().as_slice(), [got] => {
            let guard = got.lock();
            assert_eq!(guard.partition_id(), &*ARBITRARY_TRANSITION_PARTITION_ID);
        })
    }

    /// A [`PersistQueue`] implementation that never completes a persist task
    /// and therefore never signals completion of any persist task.
    ///
    /// This simulates a persist system where all workers cannot make progress;
    /// for example, an object store outage or catalog unavailability.
    #[derive(Debug, Default)]
    struct BlockedPersistQueue {
        /// Observed PartitionData instances.
        calls: Mutex<Vec<Arc<Mutex<PartitionData>>>>,
        // The tx handles that callers are blocked waiting on.
        tx: Mutex<Vec<oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl PersistQueue for BlockedPersistQueue {
        #[allow(clippy::async_yields_async)]
        async fn enqueue(
            &self,
            partition: Arc<Mutex<PartitionData>>,
            _data: PersistingData,
        ) -> oneshot::Receiver<()> {
            self.calls.lock().push(Arc::clone(&partition));
            let (tx, rx) = oneshot::channel();
            self.tx.lock().push(tx);
            rx
        }
    }

    #[tokio::test]
    async fn test_persist_ticks_when_blocked() {
        let metrics = metric::Registry::default();

        // Create a write operation to stick in the WAL, and create a partition
        // iter from the data within it to mock out the buffer tree.
        let write_op = make_write_op(
            &ARBITRARY_PARTITION_KEY,
            ARBITRARY_NAMESPACE_ID,
            &ARBITRARY_TABLE_NAME,
            ARBITRARY_TABLE_ID,
            1,
            &format!(
                r#"{},city=London people=2,pigeons="millions" 10"#,
                &*ARBITRARY_TABLE_NAME
            ),
            None,
        );

        let mut p = PartitionDataBuilder::new().build();
        for (_, table_data) in write_op.tables() {
            let partitioned_data = table_data.partitioned_data();
            p.buffer_write(
                partitioned_data.data().clone(),
                partitioned_data.sequence_number(),
            )
            .expect("write should succeed");
        }
        // Wrap the partition in the lock.
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        // Initialise a mock persist queue that never completes.
        let persist_handle = Arc::new(BlockedPersistQueue::default());

        // Initialise the WAL
        let tmp_dir = tempdir().expect("no temp dir available");
        let wal = wal::Wal::new(tmp_dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 0);

        let mut write_result = wal.append(&IngestOp::Write(write_op.clone()));

        write_result
            .changed()
            .await
            .expect("should be able to get WAL write result");

        assert_matches!(
            write_result
                .borrow()
                .as_ref()
                .expect("WAL should always return result"),
            WriteResult::Ok(_),
            "test write should succeed"
        );

        let (wal_reference_handle, wal_reference_actor) =
            WalReferenceHandle::new(Arc::clone(&wal), &metrics);
        tokio::spawn(wal_reference_actor.run());

        // Start the rotation task
        let rotate_task_handle = tokio::spawn(periodic_rotation(
            Arc::clone(&wal),
            TICK_INTERVAL,
            wal_reference_handle,
            vec![Arc::clone(&p)],
            Arc::clone(&persist_handle),
        ));

        tokio::time::pause();
        tokio::time::advance(TICK_INTERVAL).await;
        tokio::time::resume();

        // Wait for the WAL to rotate, causing 1 closed segment to exist.
        async {
            loop {
                if !wal.closed_segments().is_empty() {
                    return;
                }

                tokio::task::yield_now().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // There should be exactly 1 segment.
        let mut segment = wal.closed_segments();
        assert_eq!(segment.len(), 1);
        let segment = segment.pop().unwrap();

        // Move past the hacky sleep.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::resume();

        // Wait for the WAL segment to be deleted, indicating the end of
        // processing of the first loop.
        async {
            loop {
                match wal.closed_segments().pop() {
                    Some(closed) if closed.id() != segment.id() => {
                        // Rotation has occurred.
                        break closed;
                    }
                    // Rotation has not yet occurred.
                    Some(_) => tokio::task::yield_now().await,
                    // The old file was deleted and no new one has taken its
                    // place.
                    None => unreachable!(),
                }
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Pause the ticker loop and buffer another write in the partition.
        for (i, (_, table_data)) in write_op.tables().enumerate() {
            let partitioned_data = table_data.partitioned_data();
            p.lock()
                .buffer_write(
                    partitioned_data.data().clone(),
                    partitioned_data.sequence_number() + i as u64 + 1,
                )
                .expect("write should succeed");
        }

        // Cause another tick to occur, driving the loop again.
        tokio::time::pause();
        tokio::time::advance(TICK_INTERVAL).await;
        tokio::time::resume();

        // Wait the second tick to complete.
        async {
            loop {
                if persist_handle.calls.lock().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Stop the worker and assert the state of the persist queue.
        rotate_task_handle.abort();

        let calls = persist_handle.calls.lock().clone();
        assert_matches!(calls.as_slice(), [got1, got2] => {
            assert!(Arc::ptr_eq(got1, got2));
        })
    }
}
