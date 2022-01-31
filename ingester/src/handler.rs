//! Ingest handler

use iox_catalog::interface::{Catalog, KafkaPartition, KafkaTopic, KafkaTopicId};
use object_store::ObjectStore;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

use crate::data::{DataBuffer, PersistingBatch};

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
    /// In-memory data of this ingester
    pub data: Mutex<DataBuffer>,
    /// Object store for persistence of parquet files
    object_store: Arc<ObjectStore>,
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
        object_store: Arc<ObjectStore>,
    ) -> Self {
        Self {
            kafka_topic: topic,
            kafka_partitions: shard_ids,
            iox_catalog: catalog,
            data: Default::default(),
            object_store,
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

    /// Add a persisting batch to the Buffer
    // todo: This should be invoked inside the event `persist`
    pub fn add_persisting_batch(&mut self, batch: Arc<PersistingBatch>) {
        let mut data = self.data.lock().expect("mutex poisoned");

        data.add_persisting_batch(batch)
            // todo: this will be replaced with more appropriate action when this function is invoked inside the `persist` event
            .expect("Cannot add more persisting batch.");
    }

    /// Return true if at least a batch is persisting
    pub fn is_persisting(&self) -> bool {
        let data = self.data.lock().expect("mutex poisoned");
        data.persisting.is_some()
    }
}

impl IngestHandler for IngestHandlerImpl {}
