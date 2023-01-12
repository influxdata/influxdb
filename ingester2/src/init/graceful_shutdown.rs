use std::{sync::Arc, time::Duration};

use futures::Future;
use observability_deps::tracing::*;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    ingest_state::{IngestState, IngestStateError},
    partition_iter::PartitionIter,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};

/// Defines how often the shutdown task polls the partition buffers for
/// emptiness.
///
/// Polls faster in tests to avoid unnecessary delay.
#[cfg(test)]
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Awaits `fut`, before blocking ingest and persisting all data.
///
/// Returns once all outstanding persist jobs have completed (regardless of what
/// started them) and all buffered data has been flushed to object store.
///
/// Correctly accounts for persist jobs that have been started (by a call to
/// [`PartitionData::mark_persisting()`] but not yet enqueued).
///
/// Ingest is blocked by setting [`IngestStateError::GracefulStop`] in the
/// [`IngestState`].
///
/// [`PartitionData::mark_persisting()`]:
///     crate::buffer_tree::partition::PartitionData::mark_persisting()
pub(super) async fn graceful_shutdown_handler<F, T, P>(
    fut: F,
    complete: oneshot::Sender<()>,
    ingest_state: Arc<IngestState>,
    buffer: T,
    persist: P,
    wal: Arc<wal::Wal>,
) where
    F: Future<Output = CancellationToken> + Send,
    T: PartitionIter + Sync,
    P: PersistQueue + Clone,
{
    // Obtain the cancellation token that stops the RPC server.
    let rpc_server_stop = fut.await;
    info!("gracefully stopping ingester");

    // Reject RPC writes.
    //
    // There MAY be writes ongoing that started before this state was set.
    ingest_state.set(IngestStateError::GracefulStop);

    info!("persisting all data before shutdown");

    // Drain the buffer tree, persisting all data.
    //
    // Returns once the persist jobs it starts have complete.
    persist_partitions(buffer.partition_iter(), &persist).await;

    // There may have been concurrent persist jobs started previously by hot
    // partition persistence or WAL rotation (or some other, arbitrary persist
    // source) that have not yet completed (this is unlikely). There may also be
    // late arriving writes that started before ingest was blocked, but did not
    // buffer until after the persist was completed above (also unlikely).
    //
    // Wait until there is no data in the buffer at all before proceeding,
    // therefore ensuring those concurrent persist operations have completed and
    // no late arriving data remains buffered.
    //
    // NOTE: There is a small race in which a late arriving write starts before
    // ingest is blocked, is then stalled the entire time partitions are
    // persisted, remains stalled while this "empty" check occurs, and then
    // springs to life and buffers in the buffer tree after this check has
    // completed - I think this is extreme enough to accept as a theoretical
    // possibility that doesn't need covering off in practice.
    while buffer
        .partition_iter()
        .any(|p| p.lock().get_query_data().is_some())
    {
        if persist_partitions(buffer.partition_iter(), &persist).await != 0 {
            // Late arriving writes needed persisting.
            debug!("re-persisting late arriving data");
        } else {
            // At least one partition is returning data, and there is no data to
            // start persisting, therefore there is an outstanding persist
            // operation that hasn't yet been marked as complete.
            debug!("waiting for concurrent persist to complete");
        }

        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }

    // There is now no data buffered in the ingester - all data has been
    // persisted to object storage.
    //
    // Therefore there are no ops that need replaying to rebuild the (now empty)
    // buffer state, therefore all WAL segments can be deleted to prevent
    // spurious replay and re-uploading of the same data.
    //
    // This should be made redundant by persist-driven WAL dropping:
    //
    //  https://github.com/influxdata/influxdb_iox/issues/6566
    //
    wal.rotate().expect("failed to rotate wal");
    for file in wal.closed_segments() {
        if let Err(error) = wal.delete(file.id()).await {
            // This MAY occur due to concurrent segment deletion driven by the
            // WAL rotation task.
            //
            // If this is a legitimate failure to delete (not a "not found")
            // then this causes the data to be re-uploaded - an acceptable
            // outcome, and preferable to panicking here and not dropping the
            // rest of the deletable files.
            warn!(%error, "failed to drop WAL segment");
        }
    }

    info!("persisted all data - stopping ingester");

    // Stop the RPC server (and therefore stop accepting new queries)
    rpc_server_stop.cancel();
    // And signal the ingester has stopped.
    let _ = complete.send(());
}

#[cfg(test)]
mod tests {
    use std::{future::ready, sync::Arc, task::Poll};

    use assert_matches::assert_matches;
    use data_types::{NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId};
    use futures::FutureExt;
    use lazy_static::lazy_static;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use parking_lot::Mutex;
    use test_helpers::timeout::FutureTimeout;

    use crate::{
        buffer_tree::{
            namespace::NamespaceName, partition::PartitionData, partition::SortKeyState,
            table::TableName,
        },
        deferred_load::DeferredLoad,
        persist::queue::mock::MockPersistQueue,
    };

    use super::*;

    const PARTITION_ID: PartitionId = PartitionId::new(1);
    const TRANSITION_SHARD_ID: ShardId = ShardId::new(84);

    lazy_static! {
        static ref PARTITION_KEY: PartitionKey = PartitionKey::from("platanos");
        static ref TABLE_NAME: TableName = TableName::from("bananas");
        static ref NAMESPACE_NAME: NamespaceName = NamespaceName::from("namespace-bananas");
    }

    // Initialise a partition containing buffered data.
    fn new_partition() -> Arc<Mutex<PartitionData>> {
        let mut partition = PartitionData::new(
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

        let mb = lp_to_mutable_batch(r#"bananas,city=London people=2,pigeons="millions" 10"#).1;
        partition
            .buffer_write(mb, SequenceNumber::new(1))
            .expect("failed to write dummy data");

        Arc::new(Mutex::new(partition))
    }

    // Initialise a WAL with > 1 segment.
    async fn new_wal() -> (tempfile::TempDir, Arc<wal::Wal>) {
        let dir = tempfile::tempdir().expect("failed to get temporary WAL directory");
        let wal = wal::Wal::new(dir.path())
            .await
            .expect("failed to initialise WAL to write");

        wal.rotate().expect("failed to rotate WAL");

        (dir, wal)
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let persist = Arc::new(MockPersistQueue::default());
        let ingest_state = Arc::new(IngestState::default());
        let (_tempdir, wal) = new_wal().await;
        let partition = new_partition();

        let rpc_stop = CancellationToken::new();
        let (tx, rx) = oneshot::channel();
        graceful_shutdown_handler(
            ready(rpc_stop.clone()),
            tx,
            ingest_state,
            vec![Arc::clone(&partition)],
            Arc::clone(&persist),
            Arc::clone(&wal),
        )
        .await;

        // Wait for the shutdown to complete.
        rx.with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("shutdown task panicked");

        assert!(rpc_stop.is_cancelled());

        // Assert the data was persisted
        let persist_calls = persist.calls();
        assert_matches!(&*persist_calls, [p] => {
            assert!(Arc::ptr_eq(p, &partition));
        });

        // Assert there are now no WAL segment files that will be replayed
        assert!(wal.closed_segments().is_empty());
    }

    #[tokio::test]
    async fn test_graceful_shutdown_concurrent_persist() {
        let persist = Arc::new(MockPersistQueue::default());
        let ingest_state = Arc::new(IngestState::default());
        let (_tempdir, wal) = new_wal().await;
        let partition = new_partition();

        // Mark the partition as persisting
        let persist_job = partition
            .lock()
            .mark_persisting()
            .expect("non-empty partition should begin persisting");

        // Start the graceful shutdown job in another thread, as it SHOULD block
        // until the persist job is marked as complete.
        let rpc_stop = CancellationToken::new();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(graceful_shutdown_handler(
            ready(rpc_stop.clone()),
            tx,
            ingest_state,
            vec![Arc::clone(&partition)],
            Arc::clone(&persist),
            Arc::clone(&wal),
        ));

        // Wait a small duration of time for the first buffer emptiness check to
        // fire.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Assert the shutdown hasn't completed.
        //
        // This is racy, but will fail false negative and will not flake in CI.
        // If this fails in CI, it is a legitimate bug (shutdown task should not
        // have stopped).
        let rx = rx.shared();
        assert_matches!(futures::poll!(rx.clone()), Poll::Pending);

        // And because the shutdown is still ongoing, the RPC server must not
        // have been signalled to stop.
        assert!(!rpc_stop.is_cancelled());

        // Mark the persist job as having completed, unblocking the shutdown
        // task.
        partition.lock().mark_persisted(persist_job);

        // Wait for the shutdown to complete.
        rx.with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("shutdown task panicked");

        assert!(rpc_stop.is_cancelled());

        assert!(handle
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .is_ok());

        // Assert the data was not passed to the persist task (it couldn't have
        // been, as this caller held the PersistData)
        assert!(persist.calls().is_empty());

        // Assert there are now no WAL segment files that will be replayed
        assert!(wal.closed_segments().is_empty());
    }

    /// An implementation of [`PartitionIter`] that yields an extra new,
    /// non-empty partition each time [`PartitionIter::partition_iter()`] is
    /// called.
    #[derive(Debug)]
    struct SneakyPartitionBuffer {
        max: usize,
        partitions: Mutex<Vec<Arc<Mutex<PartitionData>>>>,
    }

    impl SneakyPartitionBuffer {
        fn new(max: usize) -> Self {
            Self {
                max,
                partitions: Default::default(),
            }
        }

        fn partitions(&self) -> Vec<Arc<Mutex<PartitionData>>> {
            self.partitions.lock().clone()
        }
    }

    impl PartitionIter for SneakyPartitionBuffer {
        fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
            let mut partitions = self.partitions.lock();

            // If this hasn't reached the maximum number of times to be sneaky,
            // add another partition.
            if partitions.len() != self.max {
                partitions.push(new_partition());
            }

            Box::new(partitions.clone().into_iter())
        }
    }

    #[tokio::test]
    async fn test_graceful_shutdown_concurrent_new_writes() {
        let persist = Arc::new(MockPersistQueue::default());
        let ingest_state = Arc::new(IngestState::default());
        let (_tempdir, wal) = new_wal().await;

        // Initialise a buffer that keeps yielding more and more newly wrote
        // data, up until the maximum.
        const MAX_NEW_PARTITIONS: usize = 3;
        let buffer = Arc::new(SneakyPartitionBuffer::new(MAX_NEW_PARTITIONS));

        // Start the graceful shutdown job in another thread, as it SHOULD block
        // until the persist job is marked as complete.
        let rpc_stop = CancellationToken::new();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(graceful_shutdown_handler(
            ready(rpc_stop.clone()),
            tx,
            ingest_state,
            Arc::clone(&buffer),
            Arc::clone(&persist),
            Arc::clone(&wal),
        ));

        // Wait for the shutdown to complete.
        rx.with_timeout_panic(Duration::from_secs(5))
            .await
            .expect("shutdown task panicked");

        assert!(rpc_stop.is_cancelled());

        assert!(handle
            .with_timeout_panic(Duration::from_secs(1))
            .await
            .is_ok());

        // Assert all the data yielded by the sneaky buffer was passed to the
        // persist task.
        let persist_calls = persist.calls();
        let must_have_persisted = |p: &Arc<Mutex<PartitionData>>| {
            for call in &persist_calls {
                if Arc::ptr_eq(call, p) {
                    return true;
                }
            }
            false
        };
        if !buffer.partitions().iter().all(must_have_persisted) {
            panic!("at least one sneaky buffer was not passed to the persist system");
        }

        // Assert there are now no WAL segment files that will be replayed
        assert!(wal.closed_segments().is_empty());
    }
}
