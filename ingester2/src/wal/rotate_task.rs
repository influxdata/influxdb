use futures::{stream, StreamExt};
use observability_deps::tracing::*;
use std::{future, sync::Arc, time::Duration};
use tokio::time::Instant;

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
        // TODO: this properly as described above.

        tokio::time::sleep(Duration::from_secs(5)).await;

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
                    let t = Instant::now();

                    // Skip this partition if there is no data to persist
                    let data = p.lock().mark_persisting()?;

                    debug!(
                        partition_id=data.partition_id().get(),
                        lock_wait=?Instant::now().duration_since(t),
                        "read data for persistence"
                    );

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

        handle
            .delete(stats.id())
            .await
            .expect("failed to drop wal segment");

        info!(
            closed_id = %stats.id(),
            "dropped persisted wal segment"
        );
    }
}

// TODO(test): rotate task
