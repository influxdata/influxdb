use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use data_types::{sequence::Sequence, write_buffer::WriteBufferCreationConfig};
use dml::{DmlMeta, DmlOperation};
use futures::{FutureExt, StreamExt};
use observability_deps::tracing::debug;
use rskafka::{
    client::{
        consumer::StreamConsumerBuilder,
        error::{Error as RSKafkaError, ProtocolError},
        partition::PartitionClient,
        ClientBuilder,
    },
    record::Record,
};
use time::{Time, TimeProvider};
use trace::TraceCollector;

use crate::{
    codec::{ContentType, IoxHeaders},
    core::{
        FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
        WriteBufferWriting, WriteStream,
    },
};

type Result<T, E = WriteBufferError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RSKafkaProducer {
    database_name: String,
    time_provider: Arc<dyn TimeProvider>,
    // TODO: batched writes
    partition_clients: BTreeMap<u32, PartitionClient>,
}

impl RSKafkaProducer {
    pub async fn new(
        conn: String,
        database_name: String,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
        let partition_clients = setup_topic(conn, database_name.clone(), creation_config).await?;

        Ok(Self {
            database_name,
            time_provider,
            partition_clients,
        })
    }
}

#[async_trait]
impl WriteBufferWriting for RSKafkaProducer {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.partition_clients.keys().copied().collect()
    }

    async fn store_operation(
        &self,
        sequencer_id: u32,
        operation: &DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let partition_client = self
            .partition_clients
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown partition: {}", sequencer_id).into()
            })?;

        // truncate milliseconds from timestamps because that's what Kafka supports
        let now = operation
            .meta()
            .producer_ts()
            .unwrap_or_else(|| self.time_provider.now());

        let timestamp_millis = now.date_time().timestamp_millis();
        let timestamp = Time::from_timestamp_millis(timestamp_millis);

        let headers = IoxHeaders::new(
            ContentType::Protobuf,
            operation.meta().span_context().cloned(),
        );

        let mut buf = Vec::new();
        crate::codec::encode_operation(&self.database_name, operation, &mut buf)?;
        let buf_len = buf.len();

        let record = Record {
            key: Default::default(),
            value: buf,
            headers: headers
                .headers()
                .map(|(k, v)| (k.to_owned(), v.as_bytes().to_vec()))
                .collect(),
            timestamp: rskafka::time::OffsetDateTime::from_unix_timestamp_nanos(
                timestamp_millis as i128 * 1_000_000,
            )?,
        };

        let kafka_write_size = record.approximate_size();

        debug!(db_name=%self.database_name, partition=sequencer_id, size=buf_len, "writing to kafka");

        let offsets = partition_client.produce(vec![record]).await?;
        let offset = offsets[0];

        debug!(db_name=%self.database_name, %offset, partition=sequencer_id, size=buf_len, "wrote to kafka");

        Ok(DmlMeta::sequenced(
            Sequence::new(sequencer_id, offset.try_into()?),
            timestamp,
            operation.meta().span_context().cloned(),
            kafka_write_size,
        ))
    }

    fn type_name(&self) -> &'static str {
        "rskafka"
    }
}

#[derive(Debug)]
struct ConsumerPartition {
    partition_client: Arc<PartitionClient>,
    next_offset: Arc<AtomicI64>,
}

#[derive(Debug)]
pub struct RSKafkaConsumer {
    partitions: BTreeMap<u32, ConsumerPartition>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
}

impl RSKafkaConsumer {
    pub async fn new(
        conn: String,
        database_name: String,
        creation_config: Option<&WriteBufferCreationConfig>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Self> {
        let partition_clients = setup_topic(conn, database_name.clone(), creation_config).await?;

        let partitions = partition_clients
            .into_iter()
            .map(|(partition_id, partition_client)| {
                let partition_client = Arc::new(partition_client);
                let next_offset = Arc::new(AtomicI64::new(0));

                (
                    partition_id,
                    ConsumerPartition {
                        partition_client,
                        next_offset,
                    },
                )
            })
            .collect();

        Ok(Self {
            partitions,
            trace_collector,
        })
    }
}

#[async_trait]
impl WriteBufferReading for RSKafkaConsumer {
    fn streams(&mut self) -> BTreeMap<u32, WriteStream<'_>> {
        let mut streams = BTreeMap::new();

        for (sequencer_id, partition) in &self.partitions {
            let trace_collector = self.trace_collector.clone();
            let next_offset = Arc::clone(&partition.next_offset);
            let stream = StreamConsumerBuilder::new(
                Arc::clone(&partition.partition_client),
                next_offset.load(Ordering::SeqCst),
            )
            .with_max_wait_ms(100)
            .build();
            let stream = stream.map(move |res| {
                let (record, _watermark) = res?;

                let kafka_read_size = record.record.approximate_size();

                let headers =
                    IoxHeaders::from_headers(record.record.headers, trace_collector.as_ref())?;

                let sequence = Sequence {
                    id: *sequencer_id,
                    number: record.offset.try_into()?,
                };

                let timestamp_millis =
                    i64::try_from(record.record.timestamp.unix_timestamp_nanos() / 1_000_000)?;
                let timestamp = Time::from_timestamp_millis_opt(timestamp_millis)
                    .ok_or_else::<WriteBufferError, _>(|| {
                    format!(
                        "Cannot parse timestamp for milliseconds: {}",
                        timestamp_millis
                    )
                    .into()
                })?;

                next_offset.store(record.offset + 1, Ordering::SeqCst);

                crate::codec::decode(
                    &record.record.value,
                    headers,
                    sequence,
                    timestamp,
                    kafka_read_size,
                )
            });
            let stream = stream.boxed();

            let partition_client = Arc::clone(&partition.partition_client);
            let fetch_high_watermark = move || {
                let partition_client = Arc::clone(&partition_client);
                let fut = async move {
                    let watermark = partition_client.get_high_watermark().await?;
                    u64::try_from(watermark).map_err(|e| Box::new(e) as WriteBufferError)
                };

                fut.boxed() as FetchHighWatermarkFut<'_>
            };
            let fetch_high_watermark = Box::new(fetch_high_watermark) as FetchHighWatermark<'_>;

            streams.insert(
                *sequencer_id,
                WriteStream {
                    stream,
                    fetch_high_watermark,
                },
            );
        }

        streams
    }

    async fn seek(
        &mut self,
        sequencer_id: u32,
        sequence_number: u64,
    ) -> Result<(), WriteBufferError> {
        let partition = self
            .partitions
            .get_mut(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown partition: {}", sequencer_id).into()
            })?;

        let offset = i64::try_from(sequence_number)?;
        partition.next_offset.store(offset, Ordering::SeqCst);

        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "rskafka"
    }
}

async fn setup_topic(
    conn: String,
    database_name: String,
    creation_config: Option<&WriteBufferCreationConfig>,
) -> Result<BTreeMap<u32, PartitionClient>> {
    let client = ClientBuilder::new(vec![conn]).build().await?;
    let controller_client = client.controller_client().await?;

    loop {
        // check if topic already exists
        let topics = client.list_topics().await?;
        if let Some(topic) = topics.into_iter().find(|t| t.name == database_name) {
            let mut partition_clients = BTreeMap::new();
            for partition in topic.partitions {
                let c = client.partition_client(&database_name, partition).await?;
                let partition = u32::try_from(partition)?;
                partition_clients.insert(partition, c);
            }
            return Ok(partition_clients);
        }

        // create topic
        if let Some(creation_config) = creation_config {
            match controller_client
                .create_topic(&database_name, creation_config.n_sequencers.get() as i32, 1)
                .await
            {
                Ok(_) => {}
                // race condition between check and creation action, that's OK
                Err(RSKafkaError::ServerError(ProtocolError::TopicAlreadyExists, _)) => {}
                Err(e) => {
                    return Err(e.into());
                }
            }
        } else {
            return Err("no partitions found and auto-creation not requested"
                .to_string()
                .into());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use futures::{stream::FuturesUnordered, TryStreamExt};
    use trace::RingBufferTraceCollector;

    use crate::{
        core::test_utils::{perform_generic_tests, random_topic_name, TestAdapter, TestContext},
        maybe_skip_kafka_integration,
    };

    use super::*;

    struct RSKafkaTestAdapter {
        conn: String,
    }

    impl RSKafkaTestAdapter {
        fn new(conn: String) -> Self {
            Self { conn }
        }
    }

    #[async_trait]
    impl TestAdapter for RSKafkaTestAdapter {
        type Context = RSKafkaTestContext;

        async fn new_context_with_time(
            &self,
            n_sequencers: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            RSKafkaTestContext {
                conn: self.conn.clone(),
                database_name: random_topic_name(),
                n_sequencers,
                time_provider,
            }
        }
    }

    struct RSKafkaTestContext {
        conn: String,
        database_name: String,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
    }

    impl RSKafkaTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_sequencers: self.n_sequencers,
                ..Default::default()
            })
        }
    }

    #[async_trait]
    impl TestContext for RSKafkaTestContext {
        type Writing = RSKafkaProducer;

        type Reading = RSKafkaConsumer;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            RSKafkaProducer::new(
                self.conn.clone(),
                self.database_name.clone(),
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

            RSKafkaConsumer::new(
                self.conn.clone(),
                self.database_name.clone(),
                self.creation_config(creation_config).as_ref(),
                Some(collector),
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_generic() {
        let conn = maybe_skip_kafka_integration!();

        perform_generic_tests(RSKafkaTestAdapter::new(conn)).await;
    }

    #[tokio::test]
    async fn test_setup_topic_race() {
        let conn = maybe_skip_kafka_integration!();
        let topic_name = random_topic_name();
        let n_partitions = NonZeroU32::new(2).unwrap();

        let mut jobs: FuturesUnordered<_> = (0..10)
            .map(|_| {
                let conn = conn.clone();
                let topic_name = topic_name.clone();

                tokio::spawn(async move {
                    setup_topic(
                        conn,
                        topic_name,
                        Some(&WriteBufferCreationConfig {
                            n_sequencers: n_partitions,
                            ..Default::default()
                        }),
                    )
                    .await
                    .unwrap();
                })
            })
            .collect();

        while jobs.try_next().await.unwrap().is_some() {}
    }
}
