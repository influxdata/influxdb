//! Ingester Server
//!

use std::sync::Arc;

use iox_catalog::interface::{KafkaPartition, RepoCollection};

/// The [`IngesterServer`] manages the lifecycle and contains all state for
/// an `ingester` server instance.
pub struct IngesterServer<'a, T>
where
    T: RepoCollection + Send + Sync,
{
    // Kafka Topic assigned to this ingester
    kafka_topic_name: String,
    // Kafka Partitions (Shards) assigned to this INgester
    kafka_partitions: Vec<KafkaPartition>,
    /// Catalog of this ingester
    pub iox_catalog: &'a Arc<T>,
}

impl<'a, T> IngesterServer<'a, T>
where
    T: RepoCollection + Send + Sync,
{
    /// Initialize the Ingester
    pub fn new(topic_name: String, shard_ids: Vec<KafkaPartition>, catalog: &'a Arc<T>) -> Self {
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
