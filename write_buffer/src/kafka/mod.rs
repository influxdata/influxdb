use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use data_types::{sequence::Sequence, write_buffer::WriteBufferCreationConfig};
use dml::{DmlMeta, DmlOperation};
use futures::{stream::BoxStream, StreamExt};
use parking_lot::Mutex;
use rskafka::client::{
    consumer::StreamConsumerBuilder,
    error::{Error as RSKafkaError, ProtocolError},
    partition::{OffsetAt, PartitionClient},
    producer::{BatchProducer, BatchProducerBuilder},
    ClientBuilder,
};
use time::{Time, TimeProvider};
use trace::TraceCollector;

use crate::{
    codec::IoxHeaders,
    core::{
        WriteBufferError, WriteBufferErrorKind, WriteBufferReading, WriteBufferStreamHandler,
        WriteBufferWriting,
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
        "kafka"
    }
}

#[derive(Debug)]
pub struct RSKafkaStreamHandler {
    partition_client: Arc<PartitionClient>,
    next_offset: Arc<Mutex<Option<i64>>>,
    terminated: Arc<AtomicBool>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    consumer_config: ConsumerConfig,
    sequencer_id: u32,
}

#[async_trait]
impl WriteBufferStreamHandler for RSKafkaStreamHandler {
    async fn stream(&mut self) -> BoxStream<'_, Result<DmlOperation, WriteBufferError>> {
        if self.terminated.load(Ordering::SeqCst) {
            return futures::stream::empty().boxed();
        }

        let trace_collector = self.trace_collector.clone();
        let next_offset = Arc::clone(&self.next_offset);
        let terminated = Arc::clone(&self.terminated);

        let start_offset: Option<i64> = {
            // need to trick a bit to make this async function `Send`
            *next_offset.lock()
        };
        let start_offset = match start_offset {
            Some(x) => x,
            None => {
                // try to guess next offset from upstream
                self.partition_client
                    .get_offset(OffsetAt::Earliest)
                    .await
                    .unwrap_or_default()
            }
        };

        let mut stream_builder =
            StreamConsumerBuilder::new(Arc::clone(&self.partition_client), start_offset);
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
            let (record, _watermark) = match res {
                Ok(x) => x,
                Err(e) => {
                    terminated.store(true, Ordering::SeqCst);
                    let kind = match e {
                        RSKafkaError::ServerError(ProtocolError::OffsetOutOfRange, _) => {
                            WriteBufferErrorKind::UnknownSequenceNumber
                        }
                        _ => WriteBufferErrorKind::Unknown,
                    };
                    return Err(WriteBufferError::new(kind, e));
                }
            };

            // store new offset already so we don't get stuck on invalid records
            *next_offset.lock() = Some(record.offset + 1);

            let kafka_read_size = record.record.approximate_size();

            let headers =
                IoxHeaders::from_headers(record.record.headers, trace_collector.as_ref())?;

            let sequence = Sequence {
                id: self.sequencer_id,
                number: record
                    .offset
                    .try_into()
                    .map_err(WriteBufferError::invalid_data)?,
            };

            let timestamp_millis =
                i64::try_from(record.record.timestamp.unix_timestamp_nanos() / 1_000_000)
                    .map_err(WriteBufferError::invalid_data)?;

            let timestamp = Time::from_timestamp_millis_opt(timestamp_millis)
                .ok_or_else::<WriteBufferError, _>(|| {
                    format!(
                        "Cannot parse timestamp for milliseconds: {}",
                        timestamp_millis
                    )
                    .into()
                })?;

            let value = record
                .record
                .value
                .ok_or_else::<WriteBufferError, _>(|| "Value missing".to_string().into())?;
            crate::codec::decode(&value, headers, sequence, timestamp, kafka_read_size)
        });
        stream.boxed()
    }

    async fn seek(&mut self, sequence_number: u64) -> Result<(), WriteBufferError> {
        let offset = i64::try_from(sequence_number).map_err(WriteBufferError::invalid_input)?;
        *self.next_offset.lock() = Some(offset);
        self.terminated.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
pub struct RSKafkaConsumer {
    partition_clients: BTreeMap<u32, Arc<PartitionClient>>,
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

        let partition_clients = partition_clients
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        Ok(Self {
            partition_clients,
            trace_collector,
            consumer_config: ConsumerConfig::try_from(connection_config)?,
        })
    }
}

#[async_trait]
impl WriteBufferReading for RSKafkaConsumer {
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.partition_clients.keys().copied().collect()
    }

    async fn stream_handler(
        &self,
        sequencer_id: u32,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        let partition_client = self
            .partition_clients
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown partition: {}", sequencer_id).into()
            })?;

        Ok(Box::new(RSKafkaStreamHandler {
            partition_client: Arc::clone(partition_client),
            next_offset: Arc::new(Mutex::new(None)),
            terminated: Arc::new(AtomicBool::new(false)),
            trace_collector: self.trace_collector.clone(),
            consumer_config: self.consumer_config.clone(),
            sequencer_id,
        }))
    }

    async fn fetch_high_watermark(&self, sequencer_id: u32) -> Result<u64, WriteBufferError> {
        let partition_client = self
            .partition_clients
            .get(&sequencer_id)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown partition: {}", sequencer_id).into()
            })?;

        let watermark = partition_client.get_offset(OffsetAt::Latest).await?;
        u64::try_from(watermark).map_err(WriteBufferError::invalid_data)
    }

    fn type_name(&self) -> &'static str {
        "kafka"
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
                let partition = u32::try_from(partition).map_err(WriteBufferError::invalid_data)?;
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
    use dml::{test_util::assert_write_op_eq, DmlDelete, DmlWrite};
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use rskafka::{client::partition::Compression, record::Record};
    use test_helpers::assert_contains;
    use trace::{ctx::SpanContext, RingBufferTraceCollector};

    use crate::{
        core::test_utils::{
            assert_span_context_eq_or_linked, perform_generic_tests, random_topic_name,
            set_pop_first, TestAdapter, TestContext,
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
    async fn test_offset_after_broken_message() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = RSKafkaTestAdapter::new(conn.clone());
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let producer = ctx.writing(true).await.unwrap();

        // write broken message followed by a real one
        let sequencer_id = set_pop_first(&mut producer.sequencer_ids()).unwrap();
        ClientBuilder::new(vec![conn])
            .build()
            .await
            .unwrap()
            .partition_client(ctx.database_name.clone(), sequencer_id as i32)
            .await
            .unwrap()
            .produce(
                vec![Record {
                    key: None,
                    value: None,
                    headers: Default::default(),
                    timestamp: rskafka::time::OffsetDateTime::now_utc(),
                }],
                Compression::NoCompression,
            )
            .await
            .unwrap();
        let w = crate::core::test_utils::write(
            "namespace",
            &producer,
            "table foo=1 1",
            sequencer_id,
            None,
        )
        .await;

        let consumer = ctx.reading(true).await.unwrap();
        let mut handler = consumer.stream_handler(sequencer_id).await.unwrap();

        // read broken message from stream
        let mut stream = handler.stream().await;
        let err = stream.next().await.unwrap().unwrap_err();
        assert_contains!(err.to_string(), "No content type header");

        // re-creating the stream should advance past the broken message
        drop(stream);
        let mut stream = handler.stream().await;
        let op = stream.next().await.unwrap().unwrap();
        assert_write_op_eq(&op, &w);
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

        let consumer = ctx.reading(true).await.unwrap();
        let mut handler = consumer.stream_handler(sequencer_id).await.unwrap();
        let mut stream = handler.stream().await;

        // get output, note that the write operations were fused
        let op_w1_12 = stream.next().await.unwrap().unwrap();
        let op_d1_1 = stream.next().await.unwrap().unwrap();
        let op_d1_2 = stream.next().await.unwrap().unwrap();
        let op_w1_34 = stream.next().await.unwrap().unwrap();
        let op_w2_12 = stream.next().await.unwrap().unwrap();

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
