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
    /// Send an `Entry` to Kafka using the sequencer ID as a partition.
    async fn store_entry(
        &self,
        entry: &Entry,
        sequencer_id: u32,
    ) -> Result<Sequence, WriteBufferError> {
        let partition = i32::try_from(sequencer_id)?;
        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&self.database_name)
            .payload(entry.data())
            .partition(partition);

        let (partition, offset) = self
            .producer
            .send_result(record)
            .map_err(|(e, _future_record)| Box::new(e))?
            .await?
            .map_err(|(e, _owned_message)| Box::new(e))?;

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

pub mod test_utils {
    /// Get the testing Kafka connection string or return current scope.
    ///
    /// If `TEST_INTEGRATION` and `KAFKA_CONNECT` are set, return the Kafka connection URL to the
    /// caller.
    ///
    /// If `TEST_INTEGRATION` is set but `KAFKA_CONNECT` is not set, fail the tests and provide
    /// guidance for setting `KAFKA_CONNECTION`.
    ///
    /// If `TEST_INTEGRATION` is not set, skip the calling test by returning early.
    #[macro_export]
    macro_rules! maybe_skip_kafka_integration {
        () => {{
            use std::env;
            dotenv::dotenv().ok();

            match (
                env::var("TEST_INTEGRATION").is_ok(),
                env::var("KAFKA_CONNECT").ok(),
            ) {
                (true, Some(kafka_connection)) => kafka_connection,
                (true, None) => {
                    panic!(
                        "TEST_INTEGRATION is set which requires running integration tests, but \
                        KAFKA_CONNECT is not set. Please run Kafka, perhaps by using the command \
                        `docker-compose -f docker/ci-kafka-docker-compose.yml up kafka`, then \
                        set KAFKA_CONNECT to the host and port where Kafka is accessible. If \
                        running the `docker-compose` command and the Rust tests on the host, the \
                        value for `KAFKA_CONNECT` should be `localhost:9093`. If running the Rust \
                        tests in another container in the `docker-compose` network as on CI, \
                        `KAFKA_CONNECT` should be `kafka:9092`."
                    )
                }
                (false, Some(_)) => {
                    eprintln!("skipping Kafka integration tests - set TEST_INTEGRATION to run");
                    return;
                }
                (false, None) => {
                    eprintln!(
                        "skipping Kafka integration tests - set TEST_INTEGRATION and KAFKA_CONNECT to \
                        run"
                    );
                    return;
                }
            }
        }};
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use rdkafka::{
        admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
        client::DefaultClientContext,
    };
    use uuid::Uuid;

    use crate::{
        core::test_utils::{perform_generic_tests, TestAdapter, TestContext},
        maybe_skip_kafka_integration,
    };

    use super::*;

    struct KafkaTestAdapter {
        conn: String,
    }

    impl KafkaTestAdapter {
        fn new(conn: String) -> Self {
            Self { conn }
        }
    }

    #[async_trait]
    impl TestAdapter for KafkaTestAdapter {
        type Context = KafkaTestContext;

        async fn new_context(&self, n_sequencers: u32) -> Self::Context {
            // Common Kafka config
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", &self.conn);
            cfg.set("message.timeout.ms", "5000");

            // Create a topic with `n_partitions` partitions in Kafka
            let database_name = format!("test_topic_{}", Uuid::new_v4());
            let admin: AdminClient<DefaultClientContext> = cfg.clone().create().unwrap();
            let topic = NewTopic::new(
                &database_name,
                n_sequencers as i32,
                TopicReplication::Fixed(1),
            );
            let opts = AdminOptions::default();
            admin.create_topics(&[topic], &opts).await.unwrap();

            KafkaTestContext {
                conn: self.conn.clone(),
                database_name,
                server_id_counter: AtomicU32::new(1),
            }
        }
    }

    struct KafkaTestContext {
        conn: String,
        database_name: String,
        server_id_counter: AtomicU32,
    }

    impl TestContext for KafkaTestContext {
        type Writing = KafkaBufferProducer;

        type Reading = KafkaBufferConsumer;

        fn writing(&self) -> Self::Writing {
            KafkaBufferProducer::new(&self.conn, &self.database_name).unwrap()
        }

        fn reading(&self) -> Self::Reading {
            let server_id = self.server_id_counter.fetch_add(1, Ordering::SeqCst);
            let server_id = ServerId::try_from(server_id).unwrap();
            KafkaBufferConsumer::new(&self.conn, server_id, &self.database_name).unwrap()
        }
    }

    #[tokio::test]
    async fn test_generic() {
        let conn = maybe_skip_kafka_integration!();

        perform_generic_tests(KafkaTestAdapter::new(conn)).await;
    }
}
