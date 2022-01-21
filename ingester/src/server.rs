//! Ingester Server
//!

use std::sync::Arc;

use iox_catalog::interface::{Catalog, KafkaPartition, KafkaTopic, KafkaTopicId};

/// The [`IngesterServer`] manages the lifecycle and contains all state for
/// an `ingester` server instance.
pub struct IngesterServer<'a, T>
where
    T: Catalog,
{
    /// Kafka Topic assigned to this ingester
    kafka_topic: KafkaTopic,
    /// Kafka Partitions (Shards) assigned to this INgester
    kafka_partitions: Vec<KafkaPartition>,
    /// Catalog of this ingester
    pub iox_catalog: &'a Arc<T>,
}

impl<'a, T> IngesterServer<'a, T>
where
    T: Catalog,
{
    /// Initialize the Ingester
    pub fn new(topic: KafkaTopic, shard_ids: Vec<KafkaPartition>, catalog: &'a Arc<T>) -> Self {
        Self {
            kafka_topic: topic,
            kafka_partitions: shard_ids,
            iox_catalog: catalog,
        }
    }

    /// Return a kafka topic
    pub fn get_topic(&self) -> KafkaTopic {
        self.kafka_topic.clone()
    }

    /// Return a kafka topic id
    pub fn get_topic_id(&self) -> KafkaTopicId {
        self.kafka_topic.id
    }

    /// Return a kafka topic name
    pub fn get_topic_name(&self) -> String {
        self.kafka_topic.name.clone()
    }

    /// Return Kafka Partitions
    pub fn get_kafka_partitions(&self) -> Vec<KafkaPartition> {
        self.kafka_partitions.clone()
    }
}
