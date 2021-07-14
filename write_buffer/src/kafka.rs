use std::convert::{TryFrom, TryInto};

use async_trait::async_trait;
use data_types::server_id::ServerId;
use entry::{Entry, Sequence, SequencedEntry};
use futures::{stream::BoxStream, StreamExt};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    ClientConfig, Message,
};

use crate::core::{WriteBufferError, WriteBufferReading, WriteBufferWriting};

pub struct KafkaBufferProducer {
    conn: String,
    database_name: String,
    producer: FutureProducer,
}

// Needed because rdkafka's FutureProducer doesn't impl Debug
impl std::fmt::Debug for KafkaBufferProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBufferProducer")
            .field("conn", &self.conn)
            .field("database_name", &self.database_name)
            .finish()
    }
}

#[async_trait]
impl WriteBufferWriting for KafkaBufferProducer {
    /// Send an `Entry` to Kafka and return the partition ID as the sequencer ID and the offset
    /// as the sequence number.
    async fn store_entry(&self, entry: &Entry) -> Result<Sequence, WriteBufferError> {
        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> =
            FutureRecord::to(&self.database_name).payload(entry.data());

        // Can't use `?` here because `send_result` returns `Err((E: Error, original_msg))` so we
        // have to extract the actual error out with a `match`.
        let (partition, offset) = match self.producer.send_result(record) {
            // Same error structure on the result of the future, need to `match`
            Ok(delivery_future) => match delivery_future.await? {
                Ok((partition, offset)) => (partition, offset),
                Err((e, _returned_record)) => return Err(Box::new(e)),
            },
            Err((e, _returned_record)) => return Err(Box::new(e)),
        };

        Ok(Sequence {
            id: partition.try_into()?,
            number: offset.try_into()?,
        })
    }
}

impl KafkaBufferProducer {
    pub fn new(
        conn: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Result<Self, KafkaError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &conn);
        cfg.set("message.timeout.ms", "5000");
        cfg.set("message.max.bytes", "10000000");

        let producer: FutureProducer = cfg.create()?;

        Ok(Self {
            conn,
            database_name,
            producer,
        })
    }
}

pub struct KafkaBufferConsumer {
    conn: String,
    database_name: String,
    consumer: StreamConsumer,
}

// Needed because rdkafka's StreamConsumer doesn't impl Debug
impl std::fmt::Debug for KafkaBufferConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBufferConsumer")
            .field("conn", &self.conn)
            .field("database_name", &self.database_name)
            .finish()
    }
}

impl WriteBufferReading for KafkaBufferConsumer {
    fn stream<'life0, 'async_trait>(
        &'life0 self,
    ) -> BoxStream<'async_trait, Result<SequencedEntry, WriteBufferError>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.consumer
            .stream()
            .map(|message| {
                let message = message?;
                let entry = Entry::try_from(message.payload().unwrap().to_vec())?;
                let sequence = Sequence {
                    id: message.partition().try_into()?,
                    number: message.offset().try_into()?,
                };

                Ok(SequencedEntry::new_from_sequence(sequence, entry)?)
            })
            .boxed()
    }
}

impl KafkaBufferConsumer {
    pub fn new(
        conn: impl Into<String>,
        server_id: ServerId,
        database_name: impl Into<String>,
    ) -> Result<Self, KafkaError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &conn);
        cfg.set("session.timeout.ms", "6000");
        cfg.set("enable.auto.commit", "false");

        // Create a unique group ID for this database's consumer as we don't want to create
        // consumer groups.
        cfg.set("group.id", &format!("{}-{}", server_id, database_name));

        // When subscribing without a partition offset, start from the smallest offset available.
        cfg.set("auto.offset.reset", "smallest");

        let consumer: StreamConsumer = cfg.create()?;

        // Subscribe to all partitions of this database's topic.
        consumer.subscribe(&[&database_name]).unwrap();

        Ok(Self {
            conn,
            database_name,
            consumer,
        })
    }
}
