//! Partition level data buffer structures.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::{
    NamespaceId, PartitionId, PartitionKey, SequenceNumber, ShardId, TableId, Tombstone,
};
use iox_query::exec::Executor;
use mutable_batch::MutableBatch;
use schema::selection::Selection;
use snafu::ResultExt;
use uuid::Uuid;
use write_summary::ShardProgress;

use self::buffer::{BufferBatch, DataBuffer};
use crate::{data::query_dedup::query, query::QueryableBatch};

mod buffer;
pub mod resolver;

/// Read only copy of the unpersisted data for a partition in the ingester for a specific partition.
#[derive(Debug)]
pub(crate) struct UnpersistedPartitionData {
    pub(crate) partition_id: PartitionId,
    pub(crate) non_persisted: Vec<Arc<SnapshotBatch>>,
    pub(crate) persisting: Option<QueryableBatch>,
    pub(crate) partition_status: PartitionStatus,
}

/// Status of a partition that has unpersisted data.
///
/// Note that this structure is specific to a partition (which itself is bound to a table and
/// shard)!
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct PartitionStatus {
    /// Max sequence number persisted
    pub parquet_max_sequence_number: Option<SequenceNumber>,

    /// Max sequence number for a tombstone
    pub tombstone_max_sequence_number: Option<SequenceNumber>,
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct PersistingBatch {
    /// Shard id of the data
    pub(crate) shard_id: ShardId,

    /// Table id of the data
    pub(crate) table_id: TableId,

    /// Partition Id of the data
    pub(crate) partition_id: PartitionId,

    /// Id of to-be-created parquet file of this data
    pub(crate) object_store_id: Uuid,

    /// data
    pub(crate) data: Arc<QueryableBatch>,
}

impl PersistingBatch {
    pub(crate) fn object_store_id(&self) -> Uuid {
        self.object_store_id
    }

    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    pub(crate) fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

/// SnapshotBatch contains data of many contiguous BufferBatches
#[derive(Debug, PartialEq)]
pub(crate) struct SnapshotBatch {
    /// Min sequence number of its combined BufferBatches
    pub(crate) min_sequence_number: SequenceNumber,
    /// Max sequence number of its combined BufferBatches
    pub(crate) max_sequence_number: SequenceNumber,
    /// Data of its combined BufferBatches kept in one RecordBatch
    pub(crate) data: Arc<RecordBatch>,
}

impl SnapshotBatch {
    /// Return only data of the given columns
    pub(crate) fn scan(
        &self,
        selection: Selection<'_>,
    ) -> Result<Option<Arc<RecordBatch>>, super::Error> {
        Ok(match selection {
            Selection::All => Some(Arc::clone(&self.data)),
            Selection::Some(columns) => {
                let schema = self.data.schema();

                let indices = columns
                    .iter()
                    .filter_map(|&column_name| {
                        match schema.index_of(column_name) {
                            Ok(idx) => Some(idx),
                            _ => None, // this batch does not include data of this column_name
                        }
                    })
                    .collect::<Vec<_>>();
                if indices.is_empty() {
                    None
                } else {
                    Some(Arc::new(
                        self.data
                            .project(&indices)
                            .context(super::FilterColumnSnafu {})?,
                    ))
                }
            }
        })
    }

    /// Return progress in this data
    fn progress(&self) -> ShardProgress {
        ShardProgress::new()
            .with_buffered(self.min_sequence_number)
            .with_buffered(self.max_sequence_number)
    }
}

/// Data of an IOx Partition of a given Table of a Namesapce that belongs to a given Shard
#[derive(Debug)]
pub struct PartitionData {
    /// The catalog ID of the partition this buffer is for.
    id: PartitionId,
    /// The string partition key for this partition.
    partition_key: PartitionKey,

    /// The shard, namespace & table IDs for this partition.
    shard_id: ShardId,
    namespace_id: NamespaceId,
    table_id: TableId,
    /// The name of the table this partition is part of.
    table_name: Arc<str>,

    pub(super) data: DataBuffer,

    /// The max_persisted_sequence number for any parquet_file in this
    /// partition.
    max_persisted_sequence_number: Option<SequenceNumber>,
}

impl PartitionData {
    /// Initialize a new partition data buffer
    pub(crate) fn new(
        id: PartitionId,
        partition_key: PartitionKey,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        table_name: Arc<str>,
        max_persisted_sequence_number: Option<SequenceNumber>,
    ) -> Self {
        Self {
            id,
            partition_key,
            shard_id,
            namespace_id,
            table_id,
            table_name,
            data: Default::default(),
            max_persisted_sequence_number,
        }
    }

    /// Snapshot anything in the buffer and move all snapshot data into a persisting batch
    pub(super) fn snapshot_to_persisting_batch(&mut self) -> Option<Arc<PersistingBatch>> {
        self.data
            .snapshot_to_persisting(self.shard_id, self.table_id, self.id, &self.table_name)
    }

    /// Snapshot whatever is in the buffer and return a new vec of the
    /// arc cloned snapshots
    #[cfg(test)]
    fn snapshot(&mut self) -> Result<Vec<Arc<SnapshotBatch>>, super::Error> {
        self.data
            .generate_snapshot()
            .context(super::SnapshotSnafu)?;
        Ok(self.data.get_snapshots().to_vec())
    }

    /// Return non persisting data
    pub(super) fn get_non_persisting_data(&self) -> Result<Vec<Arc<SnapshotBatch>>, super::Error> {
        self.data.buffer_and_snapshots()
    }

    /// Return persisting data
    pub(super) fn get_persisting_data(&self) -> Option<QueryableBatch> {
        self.data.get_persisting_data()
    }

    /// Write the given mb in the buffer
    pub(super) fn buffer_write(
        &mut self,
        sequence_number: SequenceNumber,
        mb: MutableBatch,
    ) -> Result<(), super::Error> {
        match &mut self.data.buffer {
            Some(buf) => {
                buf.max_sequence_number = sequence_number.max(buf.max_sequence_number);
                buf.data.extend_from(&mb).context(super::BufferWriteSnafu)?;
            }
            None => {
                self.data.buffer = Some(BufferBatch {
                    min_sequence_number: sequence_number,
                    max_sequence_number: sequence_number,
                    data: mb,
                })
            }
        }

        Ok(())
    }

    /// Buffers a new tombstone:
    ///   . All the data in the `buffer` and `snapshots` will be replaced with one
    ///     tombstone-applied snapshot
    ///   . The tombstone is only added in the `deletes_during_persisting` if the `persisting`
    ///     exists
    pub(super) async fn buffer_tombstone(&mut self, executor: &Executor, tombstone: Tombstone) {
        self.data.add_tombstone(tombstone.clone());

        // ----------------------------------------------------------
        // First apply the tombstone on all in-memory & non-persisting data
        // Make a QueryableBatch for all buffer + snapshots + the given tombstone
        let max_sequence_number = tombstone.sequence_number;
        let query_batch = match self.data.snapshot_to_queryable_batch(
            &self.table_name,
            self.id,
            Some(tombstone.clone()),
        ) {
            Some(query_batch) if !query_batch.is_empty() => query_batch,
            _ => {
                // No need to proceed further
                return;
            }
        };

        let (min_sequence_number, _) = query_batch.min_max_sequence_numbers();
        assert!(min_sequence_number <= max_sequence_number);

        // Run query on the QueryableBatch to apply the tombstone.
        let stream = match query(executor, Arc::new(query_batch)).await {
            Err(e) => {
                // this should never error out. if it does, we need to crash hard so
                // someone can take a look.
                panic!("unable to apply tombstones on snapshots: {:?}", e);
            }
            Ok(stream) => stream,
        };
        let record_batches = match datafusion::physical_plan::common::collect(stream).await {
            Err(e) => {
                // this should never error out. if it does, we need to crash hard so
                // someone can take a look.
                panic!("unable to collect record batches: {:?}", e);
            }
            Ok(batches) => batches,
        };

        // Merge all result record batches into one record batch
        // and make a snapshot for it
        let snapshot = if !record_batches.is_empty() {
            let record_batch =
                arrow::compute::concat_batches(&record_batches[0].schema(), &record_batches)
                    .unwrap_or_else(|e| {
                        panic!("unable to concat record batches: {:?}", e);
                    });
            let snapshot = SnapshotBatch {
                min_sequence_number,
                max_sequence_number,
                data: Arc::new(record_batch),
            };

            Some(Arc::new(snapshot))
        } else {
            None
        };

        // ----------------------------------------------------------
        // Add the tombstone-applied data back in as one snapshot
        if let Some(snapshot) = snapshot {
            self.data.snapshots.push(snapshot);
        }
    }

    /// Return the progress from this Partition
    pub(super) fn progress(&self) -> ShardProgress {
        self.data.progress()
    }

    pub(super) fn id(&self) -> PartitionId {
        self.id
    }

    /// Return the [`SequenceNumber`] that forms the (inclusive) persistence
    /// watermark for this partition.
    pub(super) fn max_persisted_sequence_number(&self) -> Option<SequenceNumber> {
        self.max_persisted_sequence_number
    }

    /// Mark this partition as having completed persistence up to, and
    /// including, the specified [`SequenceNumber`].
    pub(super) fn mark_persisted(&mut self, sequence_number: SequenceNumber) {
        self.max_persisted_sequence_number = Some(sequence_number);
        self.data.mark_persisted();
    }

    /// Return the name of the table this [`PartitionData`] is buffering writes
    /// for.
    #[cfg(test)]
    pub(crate) fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    /// Return the shard ID for this partition.
    #[cfg(test)]
    pub(crate) fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Return the table ID for this partition.
    #[cfg(test)]
    pub(crate) fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Return the partition key for this partition.
    pub fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Return the [`NamespaceId`] this partition is a part of.
    pub fn namespace_id(&self) -> NamespaceId {
        self.namespace_id
    }
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_sorted_eq;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;

    use super::*;
    use crate::test_util::create_tombstone;

    #[test]
    fn snapshot_buffer_different_but_compatible_schemas() {
        let mut partition_data = PartitionData::new(
            PartitionId::new(1),
            "bananas".into(),
            ShardId::new(1),
            NamespaceId::new(42),
            TableId::new(1),
            "foo".into(),
            None,
        );

        let seq_num1 = SequenceNumber::new(1);
        // Missing tag `t1`
        let (_, mut mutable_batch1) =
            lp_to_mutable_batch(r#"foo iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        partition_data
            .buffer_write(seq_num1, mutable_batch1.clone())
            .unwrap();

        let seq_num2 = SequenceNumber::new(2);
        // Missing field `iv`
        let (_, mutable_batch2) =
            lp_to_mutable_batch(r#"foo,t1=aoeu uv=1u,fv=12.0,bv=false,sv="bye" 10000"#);

        partition_data
            .buffer_write(seq_num2, mutable_batch2.clone())
            .unwrap();
        partition_data.data.generate_snapshot().unwrap();

        assert!(partition_data.data.buffer.is_none());
        assert_eq!(partition_data.data.snapshots.len(), 1);

        let snapshot = &partition_data.data.snapshots[0];
        assert_eq!(snapshot.min_sequence_number, seq_num1);
        assert_eq!(snapshot.max_sequence_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    // Test deletes mixed with writes on a single parittion
    #[tokio::test]
    async fn writes_and_deletes() {
        // Make a partition with empty DataBuffer
        let s_id = 1;
        let t_id = 1;
        let p_id = 1;
        let mut p = PartitionData::new(
            PartitionId::new(p_id),
            "bananas".into(),
            ShardId::new(s_id),
            NamespaceId::new(42),
            TableId::new(t_id),
            "restaurant".into(),
            None,
        );
        let exec = Executor::new(1);

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 1
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Boston day="fri",temp=50 10"#);
        p.buffer_write(SequenceNumber::new(1), mb).unwrap();

        // --- seq_num: 2
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="thu",temp=44 15"#);

        p.buffer_write(SequenceNumber::new(2), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(1)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(2)
        );
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.deletes_during_persisting().len(), 0);
        assert_eq!(p.data.persisting, None);

        // ------------------------------------------
        // Delete
        // --- seq_num: 3
        let ts = create_tombstone(
            1,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            3,         // delete's seq_number
            0,         // min time of data to get deleted
            20,        // max time of data to get deleted
            "day=thu", // delete predicate
        );
        // one row will get deleted, the other is moved to snapshot
        p.buffer_tombstone(&exec, ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // one snpashot if there is data
        assert_eq!(p.data.deletes_during_persisting().len(), 0);
        assert_eq!(p.data.persisting, None);
        // snapshot only has one row since the other one got deleted
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+--------+-----+------+--------------------------------+",
            "| city   | day | temp | time                           |",
            "+--------+-----+------+--------------------------------+",
            "| Boston | fri | 50   | 1970-01-01T00:00:00.000000010Z |",
            "+--------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 3);

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 4
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Medford day="sun",temp=55 22
                restaurant,city=Boston day="sun",temp=57 24
            "#,
        );
        p.buffer_write(SequenceNumber::new(4), mb).unwrap();

        // --- seq_num: 5
        let (_, mb) = lp_to_mutable_batch(r#"restaurant,city=Andover day="tue",temp=56 30"#);

        p.buffer_write(SequenceNumber::new(5), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(4)
        );
        assert_eq!(
            p.data.buffer.as_ref().unwrap().max_sequence_number,
            SequenceNumber::new(5)
        );
        assert_eq!(p.data.snapshots.len(), 1); // existing sanpshot
        assert_eq!(p.data.deletes_during_persisting().len(), 0);
        assert_eq!(p.data.persisting, None);

        // ------------------------------------------
        // Delete
        // --- seq_num: 6
        let ts = create_tombstone(
            2,             // tombstone id
            t_id,          // table id
            s_id,          // shard id
            6,             // delete's seq_number
            10,            // min time of data to get deleted
            50,            // max time of data to get deleted
            "city=Boston", // delete predicate
        );
        // two rows will get deleted, one from existing snapshot, one from the buffer being moved
        // to snpashot
        p.buffer_tombstone(&exec, ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // one snpashot
        assert_eq!(p.data.deletes_during_persisting().len(), 0);
        assert_eq!(p.data.persisting, None);
        // snapshot only has two rows since the other 2 rows with city=Boston have got deleted
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+---------+-----+------+--------------------------------+",
            "| city    | day | temp | time                           |",
            "+---------+-----+------+--------------------------------+",
            "| Andover | tue | 56   | 1970-01-01T00:00:00.000000030Z |",
            "| Medford | sun | 55   | 1970-01-01T00:00:00.000000022Z |",
            "+---------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 1);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 6);

        // ------------------------------------------
        // Persisting
        let p_batch = p.snapshot_to_persisting_batch().unwrap();

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after issuing persit
        assert_eq!(p.data.snapshots.len(), 0); // always empty after issuing persit
        assert_eq!(p.data.deletes_during_persisting().len(), 0); // deletes not happen yet
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Delete
        // --- seq_num: 7
        let ts = create_tombstone(
            3,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            7,         // delete's seq_number
            10,        // min time of data to get deleted
            50,        // max time of data to get deleted
            "temp=55", // delete predicate
        );
        // if a query come while persisting, the row with temp=55 will be deleted before
        // data is sent back to Querier
        p.buffer_tombstone(&exec, ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
                                          // no snpashots becasue buffer has not data yet and the
                                          // snapshot was empty too
        assert_eq!(p.data.snapshots.len(), 0);
        assert_eq!(p.data.deletes_during_persisting().len(), 1); // tombstone added since data is
                                                                 // persisting
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Fill `buffer`
        // --- seq_num: 8
        let (_, mb) = lp_to_mutable_batch(
            r#"
                restaurant,city=Wilmington day="sun",temp=55 35
                restaurant,city=Boston day="sun",temp=60 36
                restaurant,city=Boston day="sun",temp=62 38
            "#,
        );
        p.buffer_write(SequenceNumber::new(8), mb).unwrap();

        // verify data
        assert_eq!(
            p.data.buffer.as_ref().unwrap().min_sequence_number,
            SequenceNumber::new(8)
        ); // 1 newly added mutable batch of 3 rows of data
        assert_eq!(p.data.snapshots.len(), 0); // still empty
        assert_eq!(p.data.deletes_during_persisting().len(), 1);
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));

        // ------------------------------------------
        // Take snapshot of the `buffer`
        p.snapshot().unwrap();
        // verify data
        assert!(p.data.buffer.is_none()); // empty after snapshot
        assert_eq!(p.data.snapshots.len(), 1); // data moved from buffer
        assert_eq!(p.data.deletes_during_persisting().len(), 1);
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));
        // snapshot has three rows moved from buffer
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Wilmington | sun | 55   | 1970-01-01T00:00:00.000000035Z |",
            "| Boston     | sun | 60   | 1970-01-01T00:00:00.000000036Z |",
            "| Boston     | sun | 62   | 1970-01-01T00:00:00.000000038Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 8);

        // ------------------------------------------
        // Delete
        // --- seq_num: 9
        let ts = create_tombstone(
            4,         // tombstone id
            t_id,      // table id
            s_id,      // shard id
            9,         // delete's seq_number
            10,        // min time of data to get deleted
            50,        // max time of data to get deleted
            "temp=60", // delete predicate
        );
        // the row with temp=60 will be removed from the sanphot
        p.buffer_tombstone(&exec, ts).await;

        // verify data
        assert!(p.data.buffer.is_none()); // always empty after delete
        assert_eq!(p.data.snapshots.len(), 1); // new snapshot of the existing with delete applied
        assert_eq!(p.data.deletes_during_persisting().len(), 2); // one more tombstone added make it 2
        assert_eq!(p.data.persisting, Some(Arc::clone(&p_batch)));
        // snapshot has only 2 rows because the row with tem=60 was removed
        let data = (*p.data.snapshots[0].data).clone();
        let expected = vec![
            "+------------+-----+------+--------------------------------+",
            "| city       | day | temp | time                           |",
            "+------------+-----+------+--------------------------------+",
            "| Wilmington | sun | 55   | 1970-01-01T00:00:00.000000035Z |",
            "| Boston     | sun | 62   | 1970-01-01T00:00:00.000000038Z |",
            "+------------+-----+------+--------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &[data]);
        assert_eq!(p.data.snapshots[0].min_sequence_number.get(), 8);
        assert_eq!(p.data.snapshots[0].max_sequence_number.get(), 9);

        exec.join().await;
    }
}
