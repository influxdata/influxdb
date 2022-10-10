//! Data for the lifecycle of the Ingester

use std::sync::Arc;

use data_types::{PartitionId, SequenceNumber, ShardId, TableId};
use mutable_batch::MutableBatch;
use schema::selection::Selection;
use snafu::ResultExt;
use uuid::Uuid;
use write_summary::ShardProgress;

use crate::data::table::TableName;

use super::{PersistingBatch, QueryableBatch, SnapshotBatch};

/// Data of an IOx partition split into batches
/// ┌────────────────────────┐        ┌────────────────────────┐      ┌─────────────────────────┐
/// │         Buffer         │        │       Snapshots        │      │       Persisting        │
/// │  ┌───────────────────┐ │        │                        │      │                         │
/// │  │  ┌───────────────┐│ │        │ ┌───────────────────┐  │      │  ┌───────────────────┐  │
/// │  │ ┌┴──────────────┐│├─┼────────┼─┼─▶┌───────────────┐│  │      │  │  ┌───────────────┐│  │
/// │  │┌┴──────────────┐├┘│ │        │ │ ┌┴──────────────┐││  │      │  │ ┌┴──────────────┐││  │
/// │  ││  BufferBatch  ├┘ │ │        │ │┌┴──────────────┐├┘│──┼──────┼─▶│┌┴──────────────┐├┘│  │
/// │  │└───────────────┘  │ │    ┌───┼─▶│ SnapshotBatch ├┘ │  │      │  ││ SnapshotBatch ├┘ │  │
/// │  └───────────────────┘ │    │   │ │└───────────────┘  │  │      │  │└───────────────┘  │  │
/// │          ...           │    │   │ └───────────────────┘  │      │  └───────────────────┘  │
/// │  ┌───────────────────┐ │    │   │                        │      │                         │
/// │  │  ┌───────────────┐│ │    │   │          ...           │      │           ...           │
/// │  │ ┌┴──────────────┐││ │    │   │                        │      │                         │
/// │  │┌┴──────────────┐├┘│─┼────┘   │ ┌───────────────────┐  │      │  ┌───────────────────┐  │
/// │  ││  BufferBatch  ├┘ │ │        │ │  ┌───────────────┐│  │      │  │  ┌───────────────┐│  │
/// │  │└───────────────┘  │ │        │ │ ┌┴──────────────┐││  │      │  │ ┌┴──────────────┐││  │
/// │  └───────────────────┘ │        │ │┌┴──────────────┐├┘│──┼──────┼─▶│┌┴──────────────┐├┘│  │
/// │                        │        │ ││ SnapshotBatch ├┘ │  │      │  ││ SnapshotBatch ├┘ │  │
/// │          ...           │        │ │└───────────────┘  │  │      │  │└───────────────┘  │  │
/// │                        │        │ └───────────────────┘  │      │  └───────────────────┘  │
/// └────────────────────────┘        └────────────────────────┘      └─────────────────────────┘
#[derive(Debug, Default)]
pub(crate) struct DataBuffer {
    /// Buffer of incoming writes
    pub(crate) buffer: Option<BufferBatch>,

    /// Data in `buffer` will be moved to a `snapshot` when one of these happens:
    ///  . A background persist is called
    ///  . A read request from Querier
    /// The `buffer` will be empty when this happens.
    pub(crate) snapshots: Vec<Arc<SnapshotBatch>>,
    /// When a persist is called, data in `buffer` will be moved to a `snapshot`
    /// and then all `snapshots` will be moved to a `persisting`.
    /// Both `buffer` and 'snaphots` will be empty when this happens.
    pub(crate) persisting: Option<Arc<PersistingBatch>>,
    // Extra Notes:
    //  . In MVP, we will only persist a set of snapshots at a time.
    //    In later version, multiple persisting operations may be happening concurrently but
    //    their persisted info must be added into the Catalog in their data
    //    ingesting order.
    //  . When a read request comes from a Querier, all data from `snapshots`
    //    and `persisting` must be sent to the Querier.
    //  . After the `persisting` data is persisted and successfully added
    //    into the Catalog, it will be removed from this Data Buffer.
    //    This data might be added into an extra cache to serve up to
    //    Queriers that may not have loaded the parquet files from object
    //    storage yet. But this will be decided after MVP.
}

impl DataBuffer {
    /// If a [`BufferBatch`] exists, convert it to a [`SnapshotBatch`] and add
    /// it to the list of snapshots.
    ///
    /// Does nothing if there is no [`BufferBatch`].
    pub(crate) fn generate_snapshot(&mut self) -> Result<(), mutable_batch::Error> {
        let snapshot = self.copy_buffer_to_snapshot()?;
        if let Some(snapshot) = snapshot {
            self.snapshots.push(snapshot);
            self.buffer = None;
        }

        Ok(())
    }

    /// Returns snapshot of the buffer but keeps data in the buffer
    fn copy_buffer_to_snapshot(&self) -> Result<Option<Arc<SnapshotBatch>>, mutable_batch::Error> {
        if let Some(buf) = &self.buffer {
            return Ok(Some(Arc::new(SnapshotBatch {
                min_sequence_number: buf.min_sequence_number,
                max_sequence_number: buf.max_sequence_number,
                data: Arc::new(buf.data.to_arrow(Selection::All)?),
            })));
        }

        Ok(None)
    }

    /// Snapshots the buffer and make a QueryableBatch for all the snapshots
    /// Both buffer and snapshots will be empty after this
    pub(super) fn snapshot_to_queryable_batch(
        &mut self,
        table_name: &TableName,
        partition_id: PartitionId,
    ) -> Option<QueryableBatch> {
        self.generate_snapshot()
            .expect("This mutable batch snapshot error should be impossible.");

        let mut data = vec![];
        std::mem::swap(&mut data, &mut self.snapshots);

        // only produce batch if there is any data
        if data.is_empty() {
            None
        } else {
            Some(QueryableBatch::new(table_name.clone(), partition_id, data))
        }
    }

    /// Returns all existing snapshots plus data in the buffer
    /// This only read data. Data in the buffer will be kept in the buffer
    pub(super) fn buffer_and_snapshots(
        &self,
    ) -> Result<Vec<Arc<SnapshotBatch>>, crate::data::Error> {
        // Existing snapshots
        let mut snapshots = self.snapshots.clone();

        // copy the buffer to a snapshot
        let buffer_snapshot = self
            .copy_buffer_to_snapshot()
            .context(crate::data::BufferToSnapshotSnafu)?;
        snapshots.extend(buffer_snapshot);

        Ok(snapshots)
    }

    /// Snapshots the buffer and moves snapshots over to the `PersistingBatch`.
    ///
    /// # Panic
    ///
    /// Panics if there is already a persisting batch.
    pub(super) fn snapshot_to_persisting(
        &mut self,
        shard_id: ShardId,
        table_id: TableId,
        partition_id: PartitionId,
        table_name: &TableName,
    ) -> Option<Arc<PersistingBatch>> {
        if self.persisting.is_some() {
            panic!("Unable to snapshot while persisting. This is an unexpected state.")
        }

        if let Some(queryable_batch) = self.snapshot_to_queryable_batch(table_name, partition_id) {
            let persisting_batch = Arc::new(PersistingBatch {
                shard_id,
                table_id,
                partition_id,
                object_store_id: Uuid::new_v4(),
                data: Arc::new(queryable_batch),
            });

            self.persisting = Some(Arc::clone(&persisting_batch));

            Some(persisting_batch)
        } else {
            None
        }
    }

    /// Return a QueryableBatch of the persisting batch after applying new tombstones
    pub(super) fn get_persisting_data(&self) -> Option<QueryableBatch> {
        let persisting = match &self.persisting {
            Some(p) => p,
            None => return None,
        };

        // persisting data
        Some((*persisting.data).clone())
    }

    /// Return the progress in this DataBuffer
    pub(super) fn progress(&self) -> ShardProgress {
        let progress = ShardProgress::new();

        let progress = if let Some(buffer) = &self.buffer {
            progress.combine(buffer.progress())
        } else {
            progress
        };

        let progress = self.snapshots.iter().fold(progress, |progress, snapshot| {
            progress.combine(snapshot.progress())
        });

        if let Some(persisting) = &self.persisting {
            persisting
                .data
                .data
                .iter()
                .fold(progress, |progress, snapshot| {
                    progress.combine(snapshot.progress())
                })
        } else {
            progress
        }
    }

    #[cfg(test)]
    pub(super) fn get_snapshots(&self) -> &[Arc<SnapshotBatch>] {
        self.snapshots.as_ref()
    }

    pub(crate) fn mark_persisted(&mut self) {
        self.persisting = None;
    }
}

/// BufferBatch is a MutableBatch with its ingesting order, sequence_number, that helps the
/// ingester keep the batches of data in their ingesting order
#[derive(Debug)]
pub(crate) struct BufferBatch {
    /// Sequence number of the first write in this batch
    pub(crate) min_sequence_number: SequenceNumber,
    /// Sequence number of the last write in this batch
    pub(super) max_sequence_number: SequenceNumber,
    /// Ingesting data
    pub(super) data: MutableBatch,
}

impl BufferBatch {
    /// Return the progress in this DataBuffer
    fn progress(&self) -> ShardProgress {
        ShardProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

#[cfg(test)]
mod tests {
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;

    use super::*;

    #[test]
    fn snapshot_empty_buffer_adds_no_snapshots() {
        let mut data_buffer = DataBuffer::default();

        data_buffer.generate_snapshot().unwrap();

        assert!(data_buffer.snapshots.is_empty());
    }

    #[test]
    fn snapshot_buffer_batch_moves_to_snapshots() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        let (_, mutable_batch1) =
            lp_to_mutable_batch(r#"foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            min_sequence_number: seq_num1,
            max_sequence_number: seq_num1,
            data: mutable_batch1,
        };
        let record_batch1 = buffer_batch1.data.to_arrow(Selection::All).unwrap();
        data_buffer.buffer = Some(buffer_batch1);

        data_buffer.generate_snapshot().unwrap();

        assert!(data_buffer.buffer.is_none());
        assert_eq!(data_buffer.snapshots.len(), 1);

        let snapshot = &data_buffer.snapshots[0];
        assert_eq!(snapshot.min_sequence_number, seq_num1);
        assert_eq!(snapshot.max_sequence_number, seq_num1);
        assert_eq!(&*snapshot.data, &record_batch1);
    }
}
