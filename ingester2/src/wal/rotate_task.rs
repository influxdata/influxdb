use futures::{stream, StreamExt};
use observability_deps::tracing::*;
use std::{future, sync::Arc, time::Duration};

use crate::{buffer_tree::BufferTree, persist::handle::PersistHandle};

/// [`PERSIST_ENQUEUE_CONCURRENCY`] defines the parallelism used when acquiring
/// partition locks and marking the partition as persisting.
const PERSIST_ENQUEUE_CONCURRENCY: usize = 10;

/// Rotate the `wal` segment file every `period` duration of time.
pub(crate) async fn periodic_rotation(
    wal: wal::Wal,
    period: Duration,
    buffer: Arc<BufferTree>,
    persist: PersistHandle,
) {
    let handle = wal.rotation_handle();
    let mut interval = tokio::time::interval(period);

    loop {
        interval.tick().await;
        info!("rotating wal file");

        let stats = handle.rotate().await.expect("failed to rotate WAL");
        debug!(
            closed_id = %stats.id(),
            segment_bytes = stats.size(),
            "rotated wal"
        );

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

        let notifications = stream::iter(buffer.partitions())
            .filter_map(|p| {
                async move {
                    // Skip this partition if there is no data to persist
                    let data = p.lock().mark_persisting()?;

                    // Enqueue the partition for persistence.
                    //
                    // The persist task will call mark_persisted() on the partition
                    // once complete.
                    // Some(future::ready(persist.queue_persist(p, data).await))
                    Some(future::ready((p, data)))
                }
            })
            // Concurrently attempt to obtain partition locks and mark them as
            // persisting. This will hide the latency of individual lock
            // acquisitions.
            .buffer_unordered(PERSIST_ENQUEUE_CONCURRENCY)
            // Serialise adding partitions to the persist queue (a fast
            // operation that doesn't benefit from contention at all).
            .then(|(p, data)| {
                let persist = persist.clone();
                async move { persist.queue_persist(p, data).await }
            })
            .collect::<Vec<_>>()
            .await;

        debug!(
            n_partitions = notifications.len(),
            closed_id = %stats.id(),
            "queued partitions for persist"
        );

        // Wait for all the persist completion notifications.
        for n in notifications {
            n.notified().await;
        }

        debug!(
            closed_id = %stats.id(),
            "partitions persisted"
        );
    }
}

// TODO(test): rotate task
