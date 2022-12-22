use observability_deps::tracing::*;
use parking_lot::Mutex;
use std::{fmt::Debug, sync::Arc, time::Duration};

use crate::{
    buffer_tree::partition::PartitionData,
    persist::{drain_buffer::persist_partitions, queue::PersistQueue},
};

/// An abstraction over any type that can yield an iterator of (potentially
/// empty) [`PartitionData`].
pub trait PartitionIter: Send + Debug {
    /// Return the set of partitions in `self`.
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send>;
}

impl<T> PartitionIter for Arc<T>
where
    T: PartitionIter + Send + Sync,
{
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
        (**self).partition_iter()
    }
}

impl<O> PartitionIter for crate::buffer_tree::BufferTree<O>
where
    O: Send + Sync + Debug + 'static,
{
    fn partition_iter(&self) -> Box<dyn Iterator<Item = Arc<Mutex<PartitionData>>> + Send> {
        Box::new(self.partitions())
    }
}

/// Rotate the `wal` segment file every `period` duration of time.
pub(crate) async fn periodic_rotation<T, P>(
    wal: Arc<wal::Wal>,
    period: Duration,
    buffer: T,
    persist: P,
) where
    T: PartitionIter + Sync,
    P: PersistQueue + Clone,
{
    let mut interval = tokio::time::interval(period);

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
        persist_partitions(buffer.partition_iter(), persist.clone()).await;

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
}

// TODO(test): rotate task
