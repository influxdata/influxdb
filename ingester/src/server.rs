use std::sync::Arc;

use iox_catalog::mem::MemCatalog;

/// The [`IngesterServer`] manages the lifecycle and contains all state for 
/// an `ingester` server instance.
#[derive(Debug)]
struct IngesterServer<'a> {
    pub kafka_topic_name: String,
    pub kafka_partitions: Vec<i32>, // todo: use KafkaPartitionId when available
    pub iox_catalog: &'a Arc<MemCatalog>
}

impl<'a> IngesterServer<'a>{
    pub fn new(topic_name: String, shard_ids: Vec<i32>, catalog: &'a Arc<MemCatalog>) -> Self {
        Self {
            kafka_topic_name: topic_name,
            kafka_partitions: shard_ids,
            iox_catalog: catalog,
        }
    }
}