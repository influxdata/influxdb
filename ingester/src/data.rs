//! Data for the lifecycle of the Ingester

use crate::handler::IngestHandlerImpl;
use arrow::record_batch::RecordBatch;
use data_types::delete_predicate::DeletePredicate;
use iox_catalog::interface::{
    KafkaPartition, KafkaTopicId, NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId,
    Tombstone,
};
use mutable_batch::MutableBatch;
use parking_lot::RwLock;
use schema::selection::Selection;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Error while reading Topic {}", name))]
    ReadTopic {
        source: iox_catalog::interface::Error,
        name: String,
    },

    #[snafu(display("Error while reading Kafka Partition id {}", id.get()))]
    ReadSequencer {
        source: iox_catalog::interface::Error,
        id: KafkaPartition,
    },

    #[snafu(display(
        "Sequencer record not found for kafka_topic_id {} and kafka_partition {}",
        kafka_topic_id,
        kafka_partition
    ))]
    SequencerNotFound {
        kafka_topic_id: KafkaTopicId,
        kafka_partition: KafkaPartition,
    },

    #[snafu(display("The persisting is in progress. Cannot accept more persisting batch"))]
    PersistingNotEmpty,

    #[snafu(display("Nothing in the Persisting list to get removed"))]
    PersistingEmpty,

    #[snafu(display("The given batch does not match any in the Persisting list. Nothing is removed from the Persisting list"))]
    PersistingNotMatch,
}

/// A specialized `Error` for Ingester Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Ingester Data: a Map of Shard ID to its Data
#[derive(Default)]
pub struct Sequencers {
    // This map gets set up on initialization of the ingester so it won't ever be modified.
    // The content of each SequenceData will get changed when more namespaces and tables
    // get ingested.
    data: BTreeMap<SequencerId, Arc<SequencerData>>,
}

impl Sequencers {
    /// One time initialize Sequencers of this Ingester
    pub async fn initialize(ingester: &IngestHandlerImpl) -> Result<Self> {
        // Get sequencer ids from the catalog
        let sequencer_repro = ingester.iox_catalog.sequencers();
        let mut sequencers = BTreeMap::default();
        let topic = ingester.get_topic();
        for shard in ingester.get_kafka_partitions() {
            let sequencer = sequencer_repro
                .get_by_topic_id_and_partition(topic.id, shard)
                .await
                .context(ReadSequencerSnafu { id: shard })?
                .context(SequencerNotFoundSnafu {
                    kafka_topic_id: topic.id,
                    kafka_partition: shard,
                })?;
            // Create empty buffer for each sequencer
            sequencers.insert(sequencer.id, Arc::new(SequencerData::default()));
        }

        Ok(Self { data: sequencers })
    }
}

/// Data of a Shard
#[derive(Default)]
pub struct SequencerData {
    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<NamespaceId, Arc<NamespaceData>>>,
}

/// Data of a Namespace that belongs to a given Shard
#[derive(Default)]
pub struct NamespaceData {
    tables: RwLock<BTreeMap<TableId, Arc<TableData>>>,
}

/// Data of a Table in a given Namesapce that belongs to a given Shard
#[derive(Default)]
pub struct TableData {
    // Map pf partition key to its data
    partition_data: RwLock<BTreeMap<String, Arc<PartitionData>>>,
}

/// Data of an IOx Partition of a given Table of a Namesapce that belongs to a given Shard
pub struct PartitionData {
    id: PartitionId,
    inner: RwLock<DataBuffer>,
}

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
#[derive(Default)]
pub struct DataBuffer {
    /// Buffer of incoming writes
    pub buffer: Vec<BufferBatch>,

    /// Buffer of tombstones whose time range may overlap with this partition.
    /// These tombstone first will be written into the Catalog and then here.
    /// When a persist is called, these tombstones will be moved into the
    /// PersistingBatch to get applied in those data.
    pub deletes: Vec<Tombstone>,

    /// Data in `buffer` will be moved to a `snapshot` when one of these happens:
    ///  . A background persist is called
    ///  . A read request from Querier
    /// The `buffer` will be empty when this happens.
    pub snapshots: Vec<Arc<SnapshotBatch>>,
    /// When a persist is called, data in `buffer` will be moved to a `snapshot`
    /// and then all `snapshots` will be moved to a `persisting`.
    /// Both `buffer` and 'snaphots` will be empty when this happens.
    pub persisting: Option<Arc<PersistingBatch>>,
    // Extra Notes:
    //  . In MVP, we will only persist a set of sanpshots at a time.
    //    In later version, multiple perssiting operations may be happenning concurrently but
    //    their persisted info must be added into the Catalog in thier data
    //    ingesting order.
    //  . When a read request comes from a Querier, all data from `snaphots`
    //    and `persisting` must be sent to the Querier.
    //  . After the `persiting` data is persisted and successfully added
    //    into the Catalog, it will be removed from this Data Buffer.
    //    This data might be added into an extra cache to serve up to
    //    Queriers that may not have loaded the parquet files from object
    //    storage yet. But this will be decided after MVP.
}

impl DataBuffer {
    /// Move `BufferBatch`es to a `SnapshotBatch`.
    pub fn snapshot(&mut self) -> Result<(), mutable_batch::Error> {
        if !self.buffer.is_empty() {
            let min_sequencer_number = self
                .buffer
                .first()
                .expect("Buffer isn't empty in this block")
                .sequencer_number;
            let max_sequencer_number = self
                .buffer
                .last()
                .expect("Buffer isn't empty in this block")
                .sequencer_number;
            assert!(min_sequencer_number <= max_sequencer_number);

            let mut batches = self.buffer.iter();
            let first_batch = batches.next().expect("Buffer isn't empty in this block");
            let mut mutable_batch = first_batch.data.clone();

            for batch in batches {
                mutable_batch.extend_from(&batch.data)?;
            }

            self.snapshots.push(Arc::new(SnapshotBatch {
                min_sequencer_number,
                max_sequencer_number,
                data: Arc::new(mutable_batch.to_arrow(Selection::All)?),
            }));

            self.buffer.clear();
        }

        Ok(())
    }

    /// Add a persiting batch into the buffer persisting list
    /// Note: For now, there is at most one persisting batch at a time but
    /// the plan is to process several of them a time as needed
    pub fn add_persisting_batch(&mut self, batch: Arc<PersistingBatch>) -> Result<()> {
        if self.persisting.is_some() {
            return Err(Error::PersistingNotEmpty);
        } else {
            self.persisting = Some(batch);
        }

        Ok(())
    }

    /// Remove the given PersistingBatch that was persisted
    pub fn remove_persisting_batch(&mut self, batch: &Arc<PersistingBatch>) -> Result<()> {
        if let Some(persisting_batch) = &self.persisting {
            if persisting_batch == batch {
                // found. Remove this batch from the memory
                self.persisting = None;
            } else {
                return Err(Error::PersistingNotMatch);
            }
        } else {
            return Err(Error::PersistingEmpty);
        }

        Ok(())
    }
}

/// BufferBatch is a MutauableBatch with its ingesting order, sequencer_number, that
/// helps the ingester keep the batches of data in thier ingesting order
pub struct BufferBatch {
    /// Sequencer number of the ingesting data
    pub sequencer_number: SequenceNumber,
    /// Ingesting data
    pub data: MutableBatch,
}

/// SnapshotBatch contains data of many contiguous BufferBatches
#[derive(Debug, PartialEq)]
pub struct SnapshotBatch {
    /// Min sequencer number of its combined BufferBatches
    pub min_sequencer_number: SequenceNumber,
    /// Max sequencer number of its combined BufferBatches
    pub max_sequencer_number: SequenceNumber,
    /// Data of its comebined BufferBatches kept in one RecordBatch
    pub data: Arc<RecordBatch>,
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
#[derive(Debug, PartialEq)]
pub struct PersistingBatch {
    /// Sesquencer id of the data
    pub sequencer_id: SequencerId,

    /// Table id of the data
    pub table_id: TableId,

    /// Parittion Id of the data
    pub partition_id: PartitionId,

    /// Id of to-be-created parquet file of this data
    pub object_store_id: Uuid,

    /// data
    pub data: Arc<QueryableBatch>,
}

/// Queryable data used for both query and persistence
#[derive(Debug, PartialEq)]
pub struct QueryableBatch {
    /// data
    pub data: Vec<SnapshotBatch>,

    /// Tomstones to be applied on data
    pub deletes: Vec<Tombstone>,

    /// Delete predicates of the tombstones
    /// Note: this is needed here to return its reference for a trait function
    pub delete_predicates: Vec<Arc<DeletePredicate>>,

    /// This is needed to return a reference for a trait function
    pub table_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use test_helpers::assert_error;

    #[test]
    fn snapshot_empty_buffer_adds_no_snapshots() {
        let mut data_buffer = DataBuffer::default();

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.snapshots.is_empty());
    }

    #[test]
    fn snapshot_buffer_one_buffer_batch_moves_to_snapshots() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        let (_, mutable_batch1) =
            lp_to_mutable_batch(r#"foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            sequencer_number: seq_num1,
            data: mutable_batch1,
        };
        let record_batch1 = buffer_batch1.data.to_arrow(Selection::All).unwrap();
        data_buffer.buffer.push(buffer_batch1);

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.buffer.is_empty());
        assert_eq!(data_buffer.snapshots.len(), 1);

        let snapshot = &data_buffer.snapshots[0];
        assert_eq!(snapshot.min_sequencer_number, seq_num1);
        assert_eq!(snapshot.max_sequencer_number, seq_num1);
        assert_eq!(&*snapshot.data, &record_batch1);
    }

    #[test]
    fn snapshot_buffer_multiple_buffer_batches_combines_into_a_snapshot() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        let (_, mut mutable_batch1) =
            lp_to_mutable_batch(r#"foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            sequencer_number: seq_num1,
            data: mutable_batch1.clone(),
        };
        data_buffer.buffer.push(buffer_batch1);

        let seq_num2 = SequenceNumber::new(2);
        let (_, mutable_batch2) =
            lp_to_mutable_batch(r#"foo,t1=aoeu iv=2i,uv=1u,fv=12.0,bv=false,sv="bye" 10000"#);
        let buffer_batch2 = BufferBatch {
            sequencer_number: seq_num2,
            data: mutable_batch2.clone(),
        };
        data_buffer.buffer.push(buffer_batch2);

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.buffer.is_empty());
        assert_eq!(data_buffer.snapshots.len(), 1);

        let snapshot = &data_buffer.snapshots[0];
        assert_eq!(snapshot.min_sequencer_number, seq_num1);
        assert_eq!(snapshot.max_sequencer_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    #[test]
    fn snapshot_buffer_different_but_compatible_schemas() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        // Missing tag `t1`
        let (_, mut mutable_batch1) =
            lp_to_mutable_batch(r#"foo iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            sequencer_number: seq_num1,
            data: mutable_batch1.clone(),
        };
        data_buffer.buffer.push(buffer_batch1);

        let seq_num2 = SequenceNumber::new(2);
        // Missing field `iv`
        let (_, mutable_batch2) =
            lp_to_mutable_batch(r#"foo,t1=aoeu uv=1u,fv=12.0,bv=false,sv="bye" 10000"#);
        let buffer_batch2 = BufferBatch {
            sequencer_number: seq_num2,
            data: mutable_batch2.clone(),
        };
        data_buffer.buffer.push(buffer_batch2);

        data_buffer.snapshot().unwrap();

        assert!(data_buffer.buffer.is_empty());
        assert_eq!(data_buffer.snapshots.len(), 1);

        let snapshot = &data_buffer.snapshots[0];
        assert_eq!(snapshot.min_sequencer_number, seq_num1);
        assert_eq!(snapshot.max_sequencer_number, seq_num2);

        mutable_batch1.extend_from(&mutable_batch2).unwrap();
        let combined_record_batch = mutable_batch1.to_arrow(Selection::All).unwrap();
        assert_eq!(&*snapshot.data, &combined_record_batch);
    }

    #[test]
    fn snapshot_buffer_error_leaves_data_buffer_as_is() {
        let mut data_buffer = DataBuffer::default();

        let seq_num1 = SequenceNumber::new(1);
        let (_, mutable_batch1) =
            lp_to_mutable_batch(r#"foo,t1=asdf iv=1i,uv=774u,fv=1.0,bv=true,sv="hi" 1"#);
        let buffer_batch1 = BufferBatch {
            sequencer_number: seq_num1,
            data: mutable_batch1,
        };
        data_buffer.buffer.push(buffer_batch1);

        let seq_num2 = SequenceNumber::new(2);
        // Create a type mismatch
        let (_, mutable_batch2) = lp_to_mutable_batch(r#"foo iv=false 10000"#);
        let buffer_batch2 = BufferBatch {
            sequencer_number: seq_num2,
            data: mutable_batch2,
        };
        data_buffer.buffer.push(buffer_batch2);

        assert_error!(
            data_buffer.snapshot(),
            mutable_batch::Error::WriterError {
                source: mutable_batch::writer::Error::TypeMismatch { .. }
            }
        );

        assert_eq!(data_buffer.buffer.len(), 2);
        assert!(data_buffer.snapshots.is_empty());
    }
}
