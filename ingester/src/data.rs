

//! Data for the lifecycle of the ingeter
//! 

use std::{sync::Arc, collections::BTreeMap};

use mutable_batch::MutableBatch;

//                                                 ┌──────────────┐                                                               
//                                                 │Ingester Data │                                                               
//                                                 │ (in memory)  │                                                               
//                                                 └──────────────┘                                                               
//                                                         │                                                                      
//                                          ┌──────────────┼───────────────┐                                                      
//                                          ▼              ▼               ▼                                                      
//                                    ┌───────────┐                 ┌────────────┐                                                
//                                    │Sequencer 1│       ...       │Sequencer m │                 Sequencers                     
//                                    └───────────┘                 └────────────┘     a map of sequencer_id to Namespaces        
//                                          │                              │                                                      
//                           ┌──────────────┼─────────────┐                │                                                      
//                           ▼              ▼             ▼                ▼                                                      
//                     ┌────────────┐               ┌───────────┐                                   Namespaces                    
//                     │Namespace 1 │     ...       │Namespace n│         ...           a map of namespace_name to Tables         
//                     └────────────┘               └───────────┘                                                                 
//                            │                           │                                                                       
//             ┌──────────────┼──────────────┐            │                                                                       
//             ▼              ▼              ▼            ▼                                                                       
//      ┌────────────┐                ┌────────────┐                                                Tables                        
//      │  Table 1   │       ...      │  Table p   │     ...                           a map of table_name to Partitions          
//      └────────────┘                └────────────┘                                                                              
//             │                             │                                                                                    
//             │              ┌──────────────┼──────────────┐                                                                     
//             ▼              ▼              ▼              ▼                                                                     
//                     ┌────────────┐                ┌────────────┐                                   Partitions                  
//            ...      │Partition 1 │       ...      │Partition q │                    a map of partition_key to PartitionData    
//                     │(2021-12-10)│                │(2021-12-20)│                                                               
//                     └────────────┘                └──────┬─────┘                                                               
//                            │                             │                                                                     
//       ┌───────────┬────────▼────┬─────────────┐          │                                                                     
//       │           │             │             │          ▼                                                                     
//       ▼           ▼             ▼             ▼                                                                                
// ┌──────────┐┌───────────┐ ┌───────────┐ ┌───────────┐   ...                                                                    
// │ Writing  ││  Snaphot  │ │Persisting │ │ Persisted │                        PartitionData: a struct of 4 items                
// │Partition ││ Partition │ │ Partition │ │ Partition │                          . A `Writing Partition Batch`                   
// │  Batch   ││  Batch 1  │ │  Batch 1  │ │  Batch 1  │                          . A vector of `Snapshot Partition Batches`      
// └──────────┘├───────────┤ ├───────────┤ ├───────────┤                          . A vector of `Persisting Partition Batches`    
//             │    ...    │ │    ...    │ │    ...    │                          . A vector of `Persisted Partition batches`     
//             │           │ │           │ │           │                                                                          
//             ├───────────┤ ├───────────┤ ├───────────┤                        1:1 map between `Snapshot`                        
//             │ Snapshot  │ │Persisting │ │ Persisted │                        and `Persisting` Partition Batches                
//             │ Partition │ │ Partition │ │ Partition │                                                                          
//             │  Batch k  │ │  Batch k  │ │  Batch i  │                                                                          
//             └───────────┘ └───────────┘ └───────────┘                                                                          

// All sequencers aiisgned to this Ingester
#[derive(Debug, Clone)]
pub struct Sequencers {
    // A map between a sequencer id to its corresponding Namespaces.
    // A sequencer id is a `kafka_partittion`, a i32 defined in iox_catalog's Sequencer and 
    // represents a shard of data of a Table of a Namesapce. Namespace is equivalent to 
    // a customer db (aka an org's bucket). Depending on the comfiguration of sharding a table,
    // either full data or set of rows of data of the table are included in a shard.
    sequencers : BTreeMap<i32, Vec<Namespace>>,
}

// A Namespace and all of its tables of a sequencer
#[derive(Debug, Clone)]
pub struct Namespace {
    // Name of the namespace which is unique and represents a customer db.
    name: String,

    // Tables of this namesapce
    tables : Vec<Table>,
}

// A Table and all of its partittion
#[derive(Debug, Clone)]
pub struct Table {
    // table name
    name: String,

    // A map of partittion_key to its corresponding partition
    partitions : BTreeMap<String, Partition>,
}

// A Partittion and all of its in-memory data batches
//
// Stages of a batch of a partition:
//  . A partition has only one `Writing Batch`. When is it big or 
//    old enough, defined by IngesterPersistenceSettings, it will
//    be put to `Snaphot Batch` and also copied to `Pesisting Batch`.
//    The new and empty Wrtiting Batch will be created for accpeting new writes
//  . Snapshot and Persisting batches are 1:1 mapped at all times. Snapshot ones are 
//    immutable and used for querying. Persisting ones are modified to sort, 
//    dedupilcate, and apply tombstone and then persited to parquet files. 
//    While many batches can be persisted at the same time, a batch is only marked 
//    in the catalog to be persisted after the batches before 
//    its in the queue are marked persisted.
//  . After the batch are marked persisted in the catalog, its will be removed 
//    from Sanpshot and Persisting and put in Persisted. The Persisted ones 
//    will get evicted based on IngesterPersistenceSettings.
//                       ┌───────────────────┐                      
//                       │    Persisting     │                      
//                       │                   │                      
//                       │ ┌───────────────┐ │                      
// ┌────────────┐        │ │   Snapshot    │ │        ┌────────────┐
// │  Writing   │───────▶│ └───────────────┘ │───────▶│ Persisted  │
// └────────────┘        │ ┌───────────────┐ │        └────────────┘
//                       │ │   Persiting   │ │                      
//                       │ └───────────────┘ │                      
//                       └───────────────────┘                      
// 
#[derive(Debug, Clone)]
pub struct Partition {
    partition_key: String,

    // Writing batch that accepts writes to this partition
    writing_batch: PartitionBatch,

    // Queue of batches that are immutable and used for querying only.
    // The batches are queue contiguously in thier data arrival time
    snapshot_batches: Vec<PartitionBatch>,  // todo: is Vec good enough for hanlding queue?

    // Queue of persisting batches which is a one on one mapping with the snapshot_batches.
    // Data of these batches will be modified to sort, dedupilcate, and apply tombstone and then 
    // persited to parquet files. While many batches can be persisted at the same time, 
    // a batch is only marked in the catalog to be persisted after the batches before 
    // its in the queue are marked persisted
    pesisting_batched: Vec<PartitionBatch>,

    // Persisted batches that are not yet evicted from the in-memory.
    // These are batches moved from persiting_batches after they are fully persisted and marked 
    // so in the catalog
    pesisted_batched: Vec<PartitionBatch>,

}

// A PartitionBatch of contiguous in arrival time of writes
// todo & question: do we want to call this Chunk instead?
#[derive(Debug, Clone)]
pub struct PartitionBatch {
    // To keep the PartitionBtach in order of their 
    // arrived data, we may need this auto created batch id
    batch_id: i32,

    // Data of this partition batch
    data: Arc<MutableBatch>,
}
