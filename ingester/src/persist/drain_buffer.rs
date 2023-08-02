use std::{future, sync::Arc};

use futures::{stream, StreamExt};
use observability_deps::tracing::debug;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::buffer_tree::partition::PartitionData;

use super::queue::PersistQueue;

/// [`PERSIST_ENQUEUE_CONCURRENCY`] defines the parallelism used when acquiring
/// partition locks and marking the partition as persisting.
const PERSIST_ENQUEUE_CONCURRENCY: usize = 5;

// Persist a set of [`PartitionData`], blocking for completion of all enqueued
// persist jobs and returning the number of partitions that were persisted.
//
// This call is not atomic, partitions are marked for persistence incrementally.
// Writes that landed into the partition buffer after this call, but before the
// partition data is read will be included in the persisted data.
pub(crate) async fn persist_partitions<T, P>(iter: T, persist: &P) -> usize
where
    T: Iterator<Item = Arc<Mutex<PartitionData>>> + Send,
    P: PersistQueue + Clone,
{
    let notifications = stream::iter(iter)
        .filter_map(|p| async move {
            let t = Instant::now();

            // Skip this partition if there is no data to persist
            let data = p.lock().mark_persisting()?;

            debug!(
                partition_id=%data.partition_id(),
                lock_wait=?Instant::now().duration_since(t),
                "read data for persistence"
            );

            // Enqueue the partition for persistence.
            //
            // The persist task will call mark_persisted() on the partition
            // once complete.
            // Some(future::ready(persist.queue_persist(p, data).await))
            Some(future::ready((p, data)))
        })
        // Concurrently attempt to obtain partition locks and mark them as
        // persisting. This will hide the latency of individual lock
        // acquisitions.
        .buffer_unordered(PERSIST_ENQUEUE_CONCURRENCY)
        // Serialise adding partitions to the persist queue (a fast
        // operation that doesn't benefit from contention at all).
        .then(|(p, data)| {
            let persist = persist.clone();

            // Enqueue and retain the notification receiver, which will be
            // awaited later.
            #[allow(clippy::async_yields_async)]
            async move {
                persist.enqueue(p, data).await
            }
        })
        .collect::<Vec<_>>()
        .await;

    debug!(
        n_partitions = notifications.len(),
        "queued all non-empty partitions for persist"
    );

    let count = notifications.len();

    // Wait for all the persist completion notifications.
    for n in notifications {
        n.await.expect("persist worker task panic");
    }

    count
}
