use std::{
    collections::BTreeMap,
    convert::{TryFrom, TryInto},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use data_types::server_id::ServerId;
use entry::{Entry, Sequence, SequencedEntry};
use futures::{FutureExt, StreamExt};
use observability_deps::tracing::{debug, info};
use rdkafka::{
    consumer::{BaseConsumer, Consumer, StreamConsumer},
    error::KafkaError,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
    util::Timeout,
    ClientConfig, Message, Offset, TopicPartitionList,
};

use crate::core::{
    EntryStream, FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
    WriteBufferWriting,
};

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
    ) -> Result<(Sequence, DateTime<Utc>), WriteBufferError> {
        let partition = i32::try_from(sequencer_id)?;
        let timestamp = Utc::now();

        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&self.database_name)
            .payload(entry.data())
            .partition(partition)
            .timestamp(timestamp.timestamp_millis());

        debug!(db_name=%self.database_name, partition, size=entry.data().len(), "writing to kafka");

        let (partition, offset) = self
            .producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(e, _owned_message)| Box::new(e))?;

        debug!(db_name=%self.database_name, %offset, %partition, size=entry.data().len(), "wrote to kafka");

        Ok((
            Sequence {
                id: partition.try_into()?,
                number: offset.try_into()?,
            },
            timestamp,
        ))
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
        cfg.set("message.max.bytes", "31457280");
        cfg.set("queue.buffering.max.kbytes", "31457280");
        cfg.set("request.required.acks", "all"); // equivalent to acks=-1

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
    consumers: BTreeMap<u32, Arc<StreamConsumer>>,
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

#[async_trait]
impl WriteBufferReading for KafkaBufferConsumer {
    fn streams(&mut self) -> Vec<(u32, EntryStream<'_>)> {
        let mut streams = vec![];

        for (sequencer_id, consumer) in &self.consumers {
            let sequencer_id = *sequencer_id;
            let consumer_cloned = Arc::clone(consumer);
            let database_name = self.database_name.clone();

            let stream = consumer
                .stream()
                .map(move |message| {
                    let message = message?;
                    let entry = Entry::try_from(message.payload().unwrap().to_vec())?;

                    // Timestamps were added as part of
                    // [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message).
                    // The tracking issue [KAFKA-2511](https://issues.apache.org/jira/browse/KAFKA-2511) states that
                    // this was completed with Kafka 0.10.0.0, for which the
                    // [release page](https://kafka.apache.org/downloads#0.10.0.0) states a release date of 2016-05-22.
                    // Also see https://stackoverflow.com/a/62936145 which also mentions that fact.
                    //
                    // So instead of making the timestamp optional throughout the stack, we just require an
                    // up-to-date Kafka stack.
                    let timestamp_millis = message.timestamp().to_millis().ok_or_else::<WriteBufferError, _>(|| {
                        "The connected Kafka does not seem to support message timestamps (KIP-32). Please upgrade to >= 0.10.0.0".to_string().into()
                    })?;
                    let timestamp = Utc.timestamp_millis_opt(timestamp_millis).single().ok_or_else::<WriteBufferError, _>(|| {
                        format!("Cannot parse timestamp for milliseconds: {}", timestamp_millis).into()
                    })?;

                    let sequence = Sequence {
                        id: message.partition().try_into()?,
                        number: message.offset().try_into()?,
                    };

                    Ok(SequencedEntry::new_from_sequence(sequence, timestamp, entry))
                })
                .boxed();

            let fetch_high_watermark = move || {
                let consumer_cloned = Arc::clone(&consumer_cloned);
                let database_name = database_name.clone();

                let fut = async move {
                    match tokio::task::spawn_blocking(move || {
                        consumer_cloned.fetch_watermarks(
                            &database_name,
                            sequencer_id as i32,
                            Duration::from_secs(60),
                        )
                    })
                    .await
                    .expect("subtask failed")
                    {
                        Ok((_low, high)) => Ok(high as u64),
                        Err(KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownPartition)) => Ok(0),
                        Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                    }
                };

                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.push((
                sequencer_id,
                EntryStream {
                    stream,
                    fetch_high_watermark,
                },
            ));
        }

        streams
    }

    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        if let Some(consumer) = self.consumers.get(&sequencer_id) {
            let consumer = Arc::clone(consumer);
            let database_name = self.database_name.clone();
            let offset = if sequence_number > 0 {
                Offset::Offset(sequence_number as i64)
            } else {
                Offset::Beginning
            };

            tokio::task::spawn_blocking(move || {
                consumer.seek(
                    &database_name,
                    sequencer_id as i32,
                    offset,
                    Duration::from_secs(60),
                )
            })
            .await
            .expect("subtask failed")?;
        }

        Ok(())
    }
}

impl KafkaBufferConsumer {
    pub async fn new(
        conn: impl Into<String> + Send + Sync,
        server_id: ServerId,
        database_name: impl Into<String> + Send + Sync,
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

        // figure out which partitions exists
        let partitions = Self::get_partitions(&database_name, &cfg).await?;
        info!(%database_name, ?partitions, "found Kafka partitions");

        // setup a single consumer per partition, at least until https://github.com/fede1024/rust-rdkafka/pull/351 is
        // merged
        let consumers = partitions
            .into_iter()
            .map(|partition| {
                let consumer: StreamConsumer = cfg.create()?;

                let mut assignment = TopicPartitionList::new();
                assignment.add_partition(&database_name, partition as i32);

                // We must set the offset to `Beginning` here to avoid the following error during seek:
                //     KafkaError (Seek error: Local: Erroneous state)
                //
                // Also see:
                // - https://github.com/Blizzard/node-rdkafka/issues/237
                // - https://github.com/confluentinc/confluent-kafka-go/issues/121#issuecomment-362308376
                assignment
                    .set_partition_offset(&database_name, partition as i32, Offset::Beginning)
                    .expect("partition was set just before");

                consumer.assign(&assignment)?;
                Ok((partition, Arc::new(consumer)))
            })
            .collect::<Result<BTreeMap<u32, Arc<StreamConsumer>>, KafkaError>>()?;

        Ok(Self {
            conn,
            database_name,
            consumers,
        })
    }

    async fn get_partitions(
        database_name: &str,
        cfg: &ClientConfig,
    ) -> Result<Vec<u32>, KafkaError> {
        let database_name = database_name.to_string();
        let probe_consumer: BaseConsumer = cfg.create()?;

        let metadata = tokio::task::spawn_blocking(move || {
            probe_consumer.fetch_metadata(Some(&database_name), Duration::from_secs(60))
        })
        .await
        .expect("subtask failed")?;
        let topic_metadata = metadata.topics().get(0).expect("requested a single topic");

        let mut partitions: Vec<_> = topic_metadata
            .partitions()
            .iter()
            .map(|partition_metdata| partition_metdata.id().try_into().unwrap())
            .collect();
        partitions.sort_unstable();

        Ok(partitions)
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

    #[async_trait]
    impl TestContext for KafkaTestContext {
        type Writing = KafkaBufferProducer;

        type Reading = KafkaBufferConsumer;

        fn writing(&self) -> Self::Writing {
            KafkaBufferProducer::new(&self.conn, &self.database_name).unwrap()
        }

        async fn reading(&self) -> Self::Reading {
            let server_id = self.server_id_counter.fetch_add(1, Ordering::SeqCst);
            let server_id = ServerId::try_from(server_id).unwrap();
            KafkaBufferConsumer::new(&self.conn, server_id, &self.database_name)
                .await
                .unwrap()
        }
    }

    #[tokio::test]
    async fn test_generic() {
        let conn = maybe_skip_kafka_integration!();

        perform_generic_tests(KafkaTestAdapter::new(conn)).await;
    }
}
