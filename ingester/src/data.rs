//! Data for the lifecycle of the Ingeter
//! 

use std::{sync::Arc, collections::BTreeMap};

use crate::server::IngesterServer;
use snafu::{OptionExt, Snafu};
use iox_catalog::interface::{KafkaTopicId, SequencerId, RepoCollection, KafkaTopic, NamespaceId};
use parking_lot::RwLock;
use arrow::datatypes::DataType;

#[derive(Debug, Snafu)]
//#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Topic {} not found", name))]
    TopicNotFound { name: String },
}

/// A specialized `Error` for Ingester Data errors
pub type Result<T, E = Error> = std::result::Result<T, E>;


/// Ingester Data: a Mapp of Shard ID to its Data
struct Sequencers {
    // This map gets set up on initialization of the ingester so it won't ever be modified.
    // The content of each SequenceData will get changed when more namespaces and tables 
    // get ingested.
    data: BTreeMap<SequencerId, Arc<SequencerData>>,
  }

impl Sequencers {
    /// One time initialize Sequencers of this Ingester
    pub async fn initialize(ingester: &IngesterServer) -> Result<Self> {
        // Get kafka topic
        let kafka_topic_repro = ingester.iox_catalog.kafka_topic();
        let topic = kafka_topic_repro.create_or_get(ingester.kafka_topic_name).await?;

        // Get all namespaces of this topic
        let namespace_repo = ingester.iox_catalog.namespace();
        let x = namespace_repro.


        // Get Sequencers
        let sequencer_repro = ingester.iox_catalog.sequencer();
        let sequencers = BTreeMap::default();
        for shard in ingester.kafka_partitions {
            let sequencer = sequencer_repro.create_or_get(&topic, shard).await?;

            sequencers.insert(sequencer.id, )
        }

        Ok(Self {
            data: BTreeMap::default(),
        })
    }
}
  
  /// Data of a Shard
  struct SequencerData {
    // New namespaces can come in at any time so we need to be able to add new ones
    namespaces: RwLock<BTreeMap<NamespaceId, Arc<NamespaceData>>>,
  }

  impl SequencerData {
      pub fn new(seq_id: i32) -> Self {

      }
  }
  
  /// Data of a Namespace that belongs to a given Shard
  struct NamespaceData {
    tables: RwLock<BTreeMap<i64, Arc<TableData>>>,
  }
  

  /// Data of a Table in a given Namesapce that belongs to a given Shard
  struct TableData {
    partitions: RwLock<BTreeMap<i64, Arc<PartitionData>>>,
  }

  /// Data of an IOx Partition of a given Table of a Namesapce that belongs to a given Shard
  struct PartitionData {
    /// Key of this partition
    partition_key: String,
    /// Data
    inner: RwLock<DataBuffer>,
  }
  
/// Data of an IOx partition split into batches
//                       ┌────────────────────────┐            ┌────────────────────────┐
//                       │       Snapshots        │            │       Persisting       │
//                       │                        │            │                        │
//                       │    ┌───────────────┐   │            │   ┌───────────────┐    │
//                       │   ┌┴──────────────┐│   │            │   │  Persisting   │    │
//                       │  ┌┴──────────────┐├┴───┼────────────┼──▶│     Data      │    │
//                       │  │   Snapshot    ├┘    │            │   └───────────────┘    │
//                       │  └───────────────┘     │            │                        │
// ┌────────────┐        │                        │            │         ...            │
// │   Buffer   │───────▶│          ...           │            │                        │
// └────────────┘        │                        │            │                        │
//                       │   ┌───────────────┐    │            │    ┌───────────────┐   │
//                       │  ┌┴──────────────┐│    │            │    │  Persisting   │   │
//                       │ ┌┴──────────────┐├┴────┼────────────┼───▶│     Data      │   │
//                       │ │   Snapshot    ├┘     │            │    └───────────────┘   │
//                       │ └───────────────┘      │            │                        │
//                       │                        │            │                        │
//                       └────────────────────────┘            └────────────────────────┘
  struct DataBuffer {

    /// Buffer of ingesting data
    buffer: Vec<DataBatch>,

    /// Data in `buffer` will be moved to a `snapshot` when one of these happens:
    ///  . A background persist is called
    ///  . A read request from Querier
    /// The `buffer` will be empty when this happens.
    snapshots: Vec<Arc<DataBatch>>,

    /// When a persist is called, data in `buffer` will be moved to a `snapshot`
    /// and then all `snapshots` will be moved to a `persisting`.
    /// Both `buffer` and 'snaphots` will be empty when this happens.
    persisting: Vec<PersistingData>,

    // Extra Notes:
    //  . Multiple perssiting operations may be happenning concurrently but
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
  
  struct PersistingData {
    batches: Vec<Arc<DataBatch>>
  }
  
  struct DataBatch {
    // a map of the unique column name to its data. Every column
    // must have the same number of values.
    column_data: BTreeMap<i64, ColumnData<DataType>>,
  }
  
  struct ColumnData<T> {
    // it might be better to have the raw values and null markers, 
    // but this will probably be easier and faster to get going.
    values: Option<T>
  }
  