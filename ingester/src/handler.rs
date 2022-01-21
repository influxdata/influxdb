//! Ingest handler

use std::sync::Arc;

use iox_catalog::interface::{Catalog, KafkaPartition, KafkaTopic, KafkaTopicId};
use std::fmt::Formatter;

/// The [`IngestHandler`] handles all ingest from kafka, persistence and queries
pub trait IngestHandler {}

/// Implementation of the `IngestHandler` trait to ingest from kafka and manage persistence and answer queries
pub struct IngestHandlerImpl {
    /// Kafka Topic assigned to this ingester
    kafka_topic: KafkaTopic,
    /// Kafka Partitions (Shards) assigned to this INgester
    kafka_partitions: Vec<KafkaPartition>,
    /// Catalog of this ingester
    pub iox_catalog: Arc<dyn Catalog>,
}

impl std::fmt::Debug for IngestHandlerImpl {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl IngestHandlerImpl {
    /// Initialize the Ingester
    pub fn new(
        topic: KafkaTopic,
        shard_ids: Vec<KafkaPartition>,
        catalog: Arc<dyn Catalog>,
    ) -> Self {
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

impl IngestHandler for IngestHandlerImpl {}
