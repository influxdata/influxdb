//! Data for the lifecycle of the Ingeter
//!

use arrow::record_batch::RecordBatch;
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

use crate::server::IngesterServer;
use iox_catalog::interface::{
    KafkaPartition, NamespaceId, PartitionId, RepoCollection, SequencerId, TableId,
};
use mutable_batch::MutableBatch;
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

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
}

/// A specialized `Error` for Ingester Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Ingester Data: a Mapp of Shard ID to its Data
#[derive(Default)]
struct Sequencers {
    // This map gets set up on initialization of the ingester so it won't ever be modified.
    // The content of each SequenceData will get changed when more namespaces and tables
    // get ingested.
    data: BTreeMap<SequencerId, Arc<SequencerData>>,
}

impl Sequencers {
    /// One time initialize Sequencers of this Ingester
    pub async fn initialize<T: RepoCollection + Send + Sync>(
        ingester: &IngesterServer<'_, T>,
    ) -> Result<Self> {
        // Get kafka topic from the catalog
        let topic_name = ingester.get_topic();
        let kafka_topic_repro = ingester.iox_catalog.kafka_topic();
        let topic = kafka_topic_repro
            .create_or_get(topic_name.as_str()) //todo: use `get` instead
            .await
            .context(ReadTopicSnafu { name: topic_name })?;

        // Get sequencer ids from the catalog
        let sequencer_repro = ingester.iox_catalog.sequencer();
        let mut sequencers = BTreeMap::default();
        for shard in ingester.get_kafka_partitions() {
            let sequencer = sequencer_repro
                .create_or_get(&topic, shard) //todo: use `get` instead
                .await
                .context(ReadSequencerSnafu { id: shard })?;
            // Create empty buffer for each sequencer
            sequencers.insert(sequencer.id, Arc::new(SequencerData::default()));
        }

        Ok(Self { data: sequencers })
    }
}

/// Data of a Shard
#[derive(Default)]
struct SequencerData {
    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<NamespaceId, Arc<NamespaceData>>>,
}

/// Data of a Namespace that belongs to a given Shard
#[derive(Default)]
struct NamespaceData {
    tables: RwLock<BTreeMap<TableId, Arc<TableData>>>,
}

/// Data of a Table in a given Namesapce that belongs to a given Shard
#[derive(Default)]
struct TableData {
    // Map pf partition key to its data
    partition_data: RwLock<BTreeMap<String, Arc<PartitionData>>>,
}

/// Data of an IOx Partition of a given Table of a Namesapce that belongs to a given Shard
struct PartitionData {
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
struct DataBuffer {
    /// Buffer of ingesting data
    buffer: Vec<BufferBatch>,

    /// Data in `buffer` will be moved to a `snapshot` when one of these happens:
    ///  . A background persist is called
    ///  . A read request from Querier
    /// The `buffer` will be empty when this happens.
    snapshots: Vec<Arc<SnapshotBatch>>,

    /// When a persist is called, data in `buffer` will be moved to a `snapshot`
    /// and then all `snapshots` will be moved to a `persisting`.
    /// Both `buffer` and 'snaphots` will be empty when this happens.
    persisting: Option<Arc<SnapshotBatch>>,
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
/// BufferBatch is a MutauableBatch with its ingesting order, sequencer_number, that
/// helps the ingester keep the batches of data in thier ingesting order
struct BufferBatch {
    /// Sequencer number of the ingesting data
    pub sequencer_number: u64,
    /// Ingesting data
    pub data: MutableBatch,
}

/// SnapshotBatch contains data of many contiguous BufferBatches
struct SnapshotBatch {
    /// Min sequencer number of its comebined BufferBatches
    pub min_sequencer_number: u64,
    /// Max sequencer number of its comebined BufferBatches
    pub max_sequencer_number: u64,
    /// Data of its comebined BufferBatches kept in one RecordBatch
    pub data: RecordBatch,
}

/// PersistingBatch contains all needed info and data for creating
/// a parquet file for given set of SnapshotBatches
struct PersistingBatch {
    sequencer_id: SequencerId,
    table_id: TableId,
    partition_id: PartitionId,
    object_store_id: Uuid,
    data: Vec<SnapshotBatch>,
}
