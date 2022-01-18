//! Ingester Server
//!

use std::sync::Arc;

use iox_catalog::{interface::KafkaPartition, mem::MemCatalog};

/// The [`IngesterServer`] manages the lifecycle and contains all state for
/// an `ingester` server instance.
#[derive(Debug)]
pub struct IngesterServer<'a> {
    // Kafka Topic assigned to this ingester
    kafka_topic_name: String,
    // Kafka Partitions (Shards) assigned to this INgester
    kafka_partitions: Vec<KafkaPartition>,
    /// Catalog of this ingester
    pub iox_catalog: &'a Arc<MemCatalog>,
}

impl<'a> IngesterServer<'a> {
    /// Initialize the Ingester
    pub fn new(
        topic_name: String,
        shard_ids: Vec<KafkaPartition>,
        catalog: &'a Arc<MemCatalog>,
    ) -> Self {
        Self {
            kafka_topic_name: topic_name,
            kafka_partitions: shard_ids,
            iox_catalog: catalog,
        }
    }

    /// Return a kafka topic name
    pub fn get_topic(&self) -> String {
        self.kafka_topic_name.clone()
    }

    /// Return Kafka Partitions
    pub fn get_kafka_partitions(&self) -> Vec<KafkaPartition> {
        self.kafka_partitions.clone()
    }
}
