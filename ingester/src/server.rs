use std::sync::Arc;

use iox_catalog::{mem::MemCatalog, interface::KafkaPartition};

/// The [`IngesterServer`] manages the lifecycle and contains all state for
/// an `ingester` server instance.
#[derive(Debug)]
pub struct IngesterServer<'a> {
    pub kafka_topic_name: String,
    pub kafka_partitions: Vec<KafkaPartition>, // todo: use KafkaPartitionId when available
    pub iox_catalog: &'a Arc<MemCatalog>,
}

impl<'a> IngesterServer<'a> {
    pub fn new(topic_name: String, shard_ids: Vec<KafkaPartition>, catalog: &'a Arc<MemCatalog>) -> Self {
        Self {
            kafka_topic_name: topic_name,
            kafka_partitions: shard_ids,
            iox_catalog: catalog,
        }
    }

    pub fn get_topic(&self) -> String {
        self.kafka_topic_name.clone()
    }


    pub fn get_kafka_partitions(&self) -> Vec<KafkaPartition> {
        self.kafka_partitions.clone()
    }
}
