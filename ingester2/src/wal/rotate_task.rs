use observability_deps::tracing::*;
use std::{sync::Arc, time::Duration};

use crate::{
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};

/// Rotate the `wal` segment file every `period` duration of time.
pub(crate) async fn periodic_rotation<T, P>(
    wal: Arc<wal::Wal>,
    period: Duration,
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

        let stats = wal.rotate().expect("failed to rotate WAL");
        debug!(
            closed_id = %stats.id(),
            segment_bytes = stats.size(),
            "rotated wal"
        );

        // TEMPORARY HACK: wait 5 seconds for in-flight writes to the old WAL
        // segment to complete before draining the partitions.
        //
        // This can occur because writes to the WAL & buffer tree are not atomic
        // (avoiding a serialising mutex in the write path).
        //
        // A flawed solution would be to have this code read the current
        // SequenceNumber after rotation, and then wait until at least that
        // sequence number has been buffered in the BufferTree. This may work in
        // most cases, but is racy / not deterministic - writes are not ordered,
        // so sequence number 5 might be buffered before sequence number 1.
        //
        // As a temporary hack, wait 5 seconds for in-flight writes to complete
        // (which should be more than enough time) before proceeding under the
        // assumption that they have indeed completed, and all writes from the
        // previous WAL segment are now buffered. Because they're buffered, the
        // persist operation performed next will persist all the writes that
        // were in the previous WAL segment, and therefore at the end of the
        // persist operation the WAL segment can be dropped.
        //
        // The potential downside of this hack is that in the very unlikely
        // situation that an in-flight write has not completed before the
        // persist operation starts (after the 5 second sleep) and the WAL entry
        // for it is dropped - we then reduce the durability of that write until
        // it is persisted next time, or it is lost after an ingester crash
        // before the next rotation.
        //
        // In the future, a proper fix will be to keep the set of sequence
        // numbers wrote to each partition buffer, and each WAL segment as a
        // bitmap, and after persistence submit the partition's bitmap to the
        // WAL for it to do a set difference to derive the remaining sequence
        // IDs, and therefore number of references to the WAL segment. Once the
        // set of remaining IDs is empty (all data is persisted), the segment is
        // safe to delete. This content-addressed reference counting technique
        // has the added advantage of working even with parallel / out-of-order
        // / hot partition persists that span WAL segments, and means there's no
        // special code path between "hot partition persist" and "wal rotation
        // persist" - it all works the same way!
        //
        //      https://github.com/influxdata/influxdb_iox/issues/6566
        //
        // TODO: this properly as described above.

        tokio::time::sleep(Duration::from_secs(5)).await;

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
            let wal = Arc::clone(&wal);
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

                wal.delete(stats.id())
                    .await
                    .expect("failed to drop wal segment");

                info!(
                    closed_id = %stats.id(),
                    "dropped persisted wal segment"
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
    use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
    use lazy_static::lazy_static;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use parking_lot::Mutex;
    use tempfile::tempdir;
    use test_helpers::timeout::FutureTimeout;
    use tokio::sync::oneshot;

    use crate::{
        buffer_tree::{
            namespace::NamespaceName,
            partition::PartitionData,
            partition::{persisting::PersistingData, SortKeyState},
            table::TableName,
        },
        deferred_load::DeferredLoad,
        persist::queue::mock::MockPersistQueue,
    };

    use super::*;

    const PARTITION_ID: PartitionId = PartitionId::new(1);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);
    const TICK_INTERVAL: Duration = Duration::from_millis(10);

    lazy_static! {
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("platanos");
        static ref TABLE_NAME: TableName = TableName::from("bananas");
        static ref NAMESPACE_NAME: NamespaceName = NamespaceName::from("namespace-bananas");
    }

    #[tokio::test]
    async fn test_persist() {
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // Perform a single write to populate the partition.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb, SequenceNumber::new(1))
            .expect("write should succeed");

        // Wrap the partition in the lock.
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        // Initialise a mock persist queue to inspect the calls made to the
        // persist subsystem.
        let persist = Arc::new(MockPersistQueue::default());

        // Initialise the WAL
        let tmp_dir = tempdir().expect("no temp dir available");
        let wal = wal::Wal::new(tmp_dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 0);

        // Start the rotation task
        let handle = tokio::spawn(periodic_rotation(
            Arc::clone(&wal),
            TICK_INTERVAL,
            vec![Arc::clone(&p)],
            Arc::clone(&persist),
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
        // processing.
        async {
            loop {
                match wal.closed_segments().pop() {
                    Some(p) if p.id() != segment.id() => {
                        // Rotation has occurred.
                        break;
                    }
                    // Rotation has not yet occurred.
                    Some(_) => tokio::task::yield_now().await,
                    // The old file was deleted and no new one has yet taken its
                    // place.
                    None => break,
                }
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Stop the worker and assert the state of the persist queue.
        handle.abort();

        assert_matches!(persist.calls().as_slice(), [got] => {
            let guard = got.lock();
            assert_eq!(guard.partition_id(), PARTITION_ID);
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
        let mut p = PartitionData::new(
            PARTITION_ID,
            PARTITION_KEY.clone(),
            NamespaceId::new(3),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                NAMESPACE_NAME.clone()
            })),
            TableId::new(4),
            Arc::new(DeferredLoad::new(Duration::from_secs(1), async {
                TABLE_NAME.clone()
            })),
            SortKeyState::Provided(None),
            TRANSITION_SHARD_ID,
        );

        // Perform a single write to populate the partition.
        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        p.buffer_write(mb.clone(), SequenceNumber::new(1))
            .expect("write should succeed");

        // Wrap the partition in the lock.
        assert_eq!(p.completed_persistence_count(), 0);
        let p = Arc::new(Mutex::new(p));

        // Initialise a mock persist queue that never completes.
        let persist = Arc::new(BlockedPersistQueue::default());

        // Initialise the WAL
        let tmp_dir = tempdir().expect("no temp dir available");
        let wal = wal::Wal::new(tmp_dir.path())
            .await
            .expect("failed to initialise WAL");

        assert_eq!(wal.closed_segments().len(), 0);

        // Start the rotation task
        let handle = tokio::spawn(periodic_rotation(
            Arc::clone(&wal),
            TICK_INTERVAL,
            vec![Arc::clone(&p)],
            Arc::clone(&persist),
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
        p.lock()
            .buffer_write(mb, SequenceNumber::new(2))
            .expect("write should succeed");

        // Cause another tick to occur, driving the loop again.
        tokio::time::pause();
        tokio::time::advance(TICK_INTERVAL).await;
        tokio::time::resume();

        // Move past the sleep.
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::resume();

        // Wait the second tick to complete.
        async {
            loop {
                if persist.calls.lock().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        .with_timeout_panic(Duration::from_secs(5))
        .await;

        // Stop the worker and assert the state of the persist queue.
        handle.abort();

        let calls = persist.calls.lock().clone();
        assert_matches!(calls.as_slice(), [got1, got2] => {
            assert!(Arc::ptr_eq(got1, got2));
        })
    }
}
