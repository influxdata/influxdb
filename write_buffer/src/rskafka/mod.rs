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
use rskafka::client::{
    consumer::StreamConsumerBuilder,
    error::{Error as RSKafkaError, ProtocolError},
    partition::PartitionClient,
    producer::{BatchProducer, BatchProducerBuilder},
    ClientBuilder,
};
use time::{Time, TimeProvider};
use trace::TraceCollector;

use crate::{
    codec::IoxHeaders,
    core::{
        FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
        WriteBufferWriting, WriteStream,
    },
};

use self::{
    aggregator::DmlAggregator,
    config::{ClientConfig, ConsumerConfig, ProducerConfig, TopicCreationConfig},
};

mod aggregator;
mod config;

type Result<T, E = WriteBufferError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RSKafkaProducer {
    producers: BTreeMap<u32, BatchProducer<DmlAggregator>>,
}

impl RSKafkaProducer {
    pub async fn new(
        conn: String,
        database_name: String,
        connection_config: &BTreeMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Self> {
        let partition_clients = setup_topic(
            conn,
            database_name.clone(),
            connection_config,
            creation_config,
        )
        .await?;

        let producer_config = ProducerConfig::try_from(connection_config)?;

        let producers = partition_clients
            .into_iter()
            .map(|(sequencer_id, partition_client)| {
                let mut producer_builder = BatchProducerBuilder::new(Arc::new(partition_client));
                if let Some(linger) = producer_config.linger {
                    producer_builder = producer_builder.with_linger(linger);
                }
                let producer = producer_builder.build(DmlAggregator::new(
                    trace_collector.clone(),
                    database_name.clone(),
                    producer_config.max_batch_size,
                    sequencer_id,
                    Arc::clone(&time_provider),
                ));

                (sequencer_id, producer)
            })
            .collect();

        Ok(Self { producers })
    }
}

#[async_trait]
impl WriteBufferWriting for RSKafkaProducer {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.producers.keys().copied().collect()
    }

    async fn store_operation(
        &self,
        sequencer_id: u32,
        operation: &DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let producer = self
            .producers
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown partition: {}", sequencer_id).into()
            })?;

        // TODO: don't clone!
        Ok(producer.produce(operation.clone()).await?)
    }

    async fn flush(&self) {
        for producer in self.producers.values() {
            producer.flush().await;
        }
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
    consumer_config: ConsumerConfig,
}

impl RSKafkaConsumer {
    pub async fn new(
        conn: String,
        database_name: String,
        connection_config: &BTreeMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Self> {
        let partition_clients = setup_topic(
            conn,
            database_name.clone(),
            connection_config,
            creation_config,
        )
        .await?;

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
            consumer_config: ConsumerConfig::try_from(connection_config)?,
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

            let mut stream_builder = StreamConsumerBuilder::new(
                Arc::clone(&partition.partition_client),
                next_offset.load(Ordering::SeqCst),
            );
            if let Some(max_wait_ms) = self.consumer_config.max_wait_ms {
                stream_builder = stream_builder.with_max_wait_ms(max_wait_ms);
            }
            if let Some(min_batch_size) = self.consumer_config.min_batch_size {
                stream_builder = stream_builder.with_min_batch_size(min_batch_size);
            }
            if let Some(max_batch_size) = self.consumer_config.max_batch_size {
                stream_builder = stream_builder.with_max_batch_size(max_batch_size);
            }
            let stream = stream_builder.build();

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
    connection_config: &BTreeMap<String, String>,
    creation_config: Option<&WriteBufferCreationConfig>,
) -> Result<BTreeMap<u32, PartitionClient>> {
    let client_config = ClientConfig::try_from(connection_config)?;
    let mut client_builder = ClientBuilder::new(vec![conn]);
    if let Some(max_message_size) = client_config.max_message_size {
        client_builder = client_builder.max_message_size(max_message_size);
    }
    let client = client_builder.build().await?;
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
            let topic_creation_config = TopicCreationConfig::try_from(creation_config)?;

            match controller_client
                .create_topic(
                    &database_name,
                    topic_creation_config.num_partitions,
                    topic_creation_config.replication_factor,
                    topic_creation_config.timeout_ms,
                )
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

    use data_types::{delete_predicate::DeletePredicate, timestamp::TimestampRange};
    use dml::{DmlDelete, DmlWrite};
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use trace::{ctx::SpanContext, RingBufferTraceCollector};

    use crate::{
        core::test_utils::{
            assert_span_context_eq_or_linked, map_pop_first, perform_generic_tests,
            random_topic_name, set_pop_first, TestAdapter, TestContext,
        },
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
                trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
            }
        }
    }

    struct RSKafkaTestContext {
        conn: String,
        database_name: String,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
        trace_collector: Arc<RingBufferTraceCollector>,
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
                &BTreeMap::default(),
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
                Some(self.trace_collector() as Arc<_>),
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            RSKafkaConsumer::new(
                self.conn.clone(),
                self.database_name.clone(),
                &BTreeMap::default(),
                self.creation_config(creation_config).as_ref(),
                Some(self.trace_collector() as Arc<_>),
            )
            .await
        }

        fn trace_collector(&self) -> Arc<RingBufferTraceCollector> {
            Arc::clone(&self.trace_collector)
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
                        &BTreeMap::default(),
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

    #[tokio::test]
    async fn test_batching() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = RSKafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;
        let trace_collector = ctx.trace_collector();

        let producer = ctx.writing(true).await.unwrap();

        let sequencer_id = set_pop_first(&mut producer.sequencer_ids()).unwrap();

        let (w1_1, w1_2, w2_1, d1_1, d1_2, w1_3, w1_4, w2_2) = tokio::join!(
            // ns1: batch 1
            write("ns1", &producer, &trace_collector, sequencer_id),
            write("ns1", &producer, &trace_collector, sequencer_id),
            // ns2: batch 1, part A
            write("ns2", &producer, &trace_collector, sequencer_id),
            // ns1: batch 2
            delete("ns1", &producer, &trace_collector, sequencer_id),
            // ns1: batch 3
            delete("ns1", &producer, &trace_collector, sequencer_id),
            // ns1: batch 4
            write("ns1", &producer, &trace_collector, sequencer_id),
            write("ns1", &producer, &trace_collector, sequencer_id),
            // ns2: batch 1, part B
            write("ns2", &producer, &trace_collector, sequencer_id),
        );

        // ensure that write operations were fused
        assert_eq!(w1_1.sequence().unwrap(), w1_2.sequence().unwrap());
        assert_ne!(w1_2.sequence().unwrap(), d1_1.sequence().unwrap());
        assert_ne!(d1_1.sequence().unwrap(), d1_2.sequence().unwrap());
        assert_ne!(d1_2.sequence().unwrap(), w1_3.sequence().unwrap());
        assert_eq!(w1_3.sequence().unwrap(), w1_4.sequence().unwrap());
        assert_ne!(w1_4.sequence().unwrap(), w1_1.sequence().unwrap());

        assert_ne!(w2_1.sequence().unwrap(), w1_1.sequence().unwrap());
        assert_eq!(w2_1.sequence().unwrap(), w2_2.sequence().unwrap());

        let mut consumer = ctx.reading(true).await.unwrap();
        let mut streams = consumer.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();

        // get output, note that the write operations were fused
        let op_w1_12 = stream.stream.next().await.unwrap().unwrap();
        let op_d1_1 = stream.stream.next().await.unwrap().unwrap();
        let op_d1_2 = stream.stream.next().await.unwrap().unwrap();
        let op_w1_34 = stream.stream.next().await.unwrap().unwrap();
        let op_w2_12 = stream.stream.next().await.unwrap().unwrap();

        // ensure that sequence numbers map as expected
        assert_eq!(
            op_w1_12.meta().sequence().unwrap(),
            w1_1.sequence().unwrap(),
        );
        assert_eq!(
            op_w1_12.meta().sequence().unwrap(),
            w1_2.sequence().unwrap(),
        );
        assert_eq!(op_d1_1.meta().sequence().unwrap(), d1_1.sequence().unwrap(),);
        assert_eq!(op_d1_2.meta().sequence().unwrap(), d1_2.sequence().unwrap(),);
        assert_eq!(
            op_w1_34.meta().sequence().unwrap(),
            w1_3.sequence().unwrap(),
        );
        assert_eq!(
            op_w1_34.meta().sequence().unwrap(),
            w1_4.sequence().unwrap(),
        );
        assert_eq!(
            op_w2_12.meta().sequence().unwrap(),
            w2_1.sequence().unwrap(),
        );
        assert_eq!(
            op_w2_12.meta().sequence().unwrap(),
            w2_2.sequence().unwrap(),
        );

        // check tracing span links
        assert_span_context_eq_or_linked(
            w1_1.span_context().unwrap(),
            op_w1_12.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w1_2.span_context().unwrap(),
            op_w1_12.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            d1_1.span_context().unwrap(),
            op_d1_1.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            d1_2.span_context().unwrap(),
            op_d1_2.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w1_3.span_context().unwrap(),
            op_w1_34.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w1_4.span_context().unwrap(),
            op_w1_34.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w2_1.span_context().unwrap(),
            op_w2_12.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w2_2.span_context().unwrap(),
            op_w2_12.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
    }

    async fn write(
        namespace: &str,
        producer: &RSKafkaProducer,
        trace_collector: &Arc<RingBufferTraceCollector>,
        sequencer_id: u32,
    ) -> DmlMeta {
        let span_ctx = SpanContext::new(Arc::clone(trace_collector) as Arc<_>);
        let tables = mutable_batch_lp::lines_to_batches("table foo=1", 0).unwrap();
        let write = DmlWrite::new(namespace, tables, DmlMeta::unsequenced(Some(span_ctx)));
        let op = DmlOperation::Write(write);
        producer.store_operation(sequencer_id, &op).await.unwrap()
    }

    async fn delete(
        namespace: &str,
        producer: &RSKafkaProducer,
        trace_collector: &Arc<RingBufferTraceCollector>,
        sequencer_id: u32,
    ) -> DmlMeta {
        let span_ctx = SpanContext::new(Arc::clone(trace_collector) as Arc<_>);
        let op = DmlOperation::Delete(DmlDelete::new(
            namespace,
            DeletePredicate {
                range: TimestampRange::new(0, 1),
                exprs: vec![],
            },
            None,
            DmlMeta::unsequenced(Some(span_ctx)),
        ));
        producer.store_operation(sequencer_id, &op).await.unwrap()
    }
}
