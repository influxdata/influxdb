use async_trait::async_trait;
use data_types::database_rules::DatabaseRules;
use entry::{Entry, Sequence};
use rdkafka::{
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use std::{convert::TryInto, sync::Arc};

pub type WriteBufferError = Box<dyn std::error::Error + Sync + Send>;

pub fn new(rules: &DatabaseRules) -> Result<Option<Arc<dyn WriteBufferWriting>>, WriteBufferError> {
    let name = rules.db_name();

    // Right now, `KafkaBuffer` is the only production implementation of the `WriteBufferWriting`
    // trait, so always use `KafkaBuffer` when there is a write buffer connection string
    // specified. If/when there are other kinds of write buffers, additional configuration will
    // be needed to determine what kind of write buffer to use here.
    match rules.write_buffer_connection.as_ref() {
        Some(conn) => {
            let kafka_buffer = KafkaBuffer::new(conn, name)?;

            Ok(Some(Arc::new(kafka_buffer) as _))
        }
        None => Ok(None),
    }
}

/// A Write Buffer takes an `Entry` and returns `Sequence` data that facilitates reading entries
/// from the Write Buffer at a later time.
#[async_trait]
pub trait WriteBufferWriting: Sync + Send + std::fmt::Debug + 'static {
    /// Send an `Entry` to the write buffer and return information that can be used to restore
    /// entries at a later time.
    async fn store_entry(&self, entry: &Entry) -> Result<Sequence, WriteBufferError>;

    // TODO: interface for restoring, will look something like:
    // async fn restore_from(&self, sequence: &Sequence) -> Result<Stream<Entry>, Err>;
}

pub struct KafkaBuffer {
    conn: String,
    database_name: String,
    producer: FutureProducer,
}

// Needed because rdkafka's FutureProducer doesn't impl Debug
impl std::fmt::Debug for KafkaBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBuffer")
            .field("conn", &self.conn)
            .field("database_name", &self.database_name)
            .finish()
    }
}

#[async_trait]
impl WriteBufferWriting for KafkaBuffer {
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

impl KafkaBuffer {
    pub fn new(
        conn: impl Into<String>,
        database_name: impl Into<String>,
    ) -> Result<Self, KafkaError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &conn);
        cfg.set("message.timeout.ms", "5000");

        let producer: FutureProducer = cfg.create()?;

        Ok(Self {
            conn,
            database_name,
            producer,
        })
    }
}

pub mod test_helpers {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Default)]
    pub struct MockBuffer {
        pub entries: Arc<Mutex<Vec<Entry>>>,
    }

    #[async_trait]
    impl WriteBufferWriting for MockBuffer {
        async fn store_entry(&self, entry: &Entry) -> Result<Sequence, WriteBufferError> {
            let mut entries = self.entries.lock().unwrap();
            let offset = entries.len() as u64;
            entries.push(entry.clone());

            Ok(Sequence {
                id: 0,
                number: offset,
            })
        }
    }
}
