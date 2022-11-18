use self::{
    config::{ClientConfig, ConsumerConfig, ProducerConfig, TopicCreationConfig},
    instrumentation::KafkaProducerMetrics,
    record_aggregator::RecordAggregator,
};
use crate::{
    codec::IoxHeaders,
    config::WriteBufferCreationConfig,
    core::{
        WriteBufferError, WriteBufferErrorKind, WriteBufferReading, WriteBufferStreamHandler,
        WriteBufferWriting,
    },
};
use async_trait::async_trait;
use data_types::{Sequence, SequenceNumber, ShardIndex};
use dml::{DmlMeta, DmlOperation};
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryStreamExt,
};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::warn;
use parking_lot::Mutex;
use rskafka::{
    client::{
        consumer::{StartOffset, StreamConsumerBuilder},
        error::{Error as RSKafkaError, ProtocolError},
        partition::{Compression, OffsetAt, PartitionClient, UnknownTopicHandling},
        producer::{BatchProducer, BatchProducerBuilder},
        ClientBuilder,
    },
    record::RecordAndOffset,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use trace::TraceCollector;

mod config;
mod instrumentation;
mod record_aggregator;

/// Maximum number of jobs buffered and decoded concurrently.
const CONCURRENT_DECODE_JOBS: usize = 10;

type Result<T, E = WriteBufferError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct RSKafkaProducer {
    producers: BTreeMap<ShardIndex, BatchProducer<RecordAggregator>>,
}

impl RSKafkaProducer {
    #[allow(clippy::too_many_arguments)]
    pub async fn new<'a>(
        conn: String,
        topic_name: String,
        connection_config: &'a BTreeMap<String, String>,
        time_provider: Arc<dyn TimeProvider>,
        creation_config: Option<&'a WriteBufferCreationConfig>,
        partitions: Option<Range<i32>>,
        _trace_collector: Option<Arc<dyn TraceCollector>>,
        metric_registry: &'a metric::Registry,
    ) -> Result<Self> {
        let partition_clients = setup_topic(
            conn,
            topic_name.clone(),
            connection_config,
            creation_config,
            partitions,
        )
        .await?;

        let producer_config = ProducerConfig::try_from(connection_config)?;

        let producers = partition_clients
            .into_iter()
            .map(|(shard_index, partition_client)| {
                // Instrument this kafka partition client.
                let partition_client = KafkaProducerMetrics::new(
                    Box::new(partition_client),
                    topic_name.clone(),
                    shard_index,
                    metric_registry,
                );

                let mut producer_builder =
                    BatchProducerBuilder::new_with_client(Arc::new(partition_client))
                        .with_compression(Compression::Zstd);
                if let Some(linger) = producer_config.linger {
                    producer_builder = producer_builder.with_linger(linger);
                }
                let producer = producer_builder.build(RecordAggregator::new(
                    shard_index,
                    producer_config.max_batch_size,
                    Arc::clone(&time_provider),
                ));

                (shard_index, producer)
            })
            .collect();

        Ok(Self { producers })
    }
}

#[async_trait]
impl WriteBufferWriting for RSKafkaProducer {
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        self.producers.keys().copied().collect()
    }

    async fn store_operation(
        &self,
        shard_index: ShardIndex,
        operation: DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let producer = self
            .producers
            .get(&shard_index)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown shard index: {}", shard_index).into()
            })?;

        Ok(producer.produce(operation).await?)
    }

    async fn flush(&self) -> Result<(), WriteBufferError> {
        for producer in self.producers.values() {
            producer.flush().await?;
        }
        Ok(())
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
    shard_index: ShardIndex,
}

/// Launch a tokio task that attempts to decode a DmlOperation from a
/// record.
///
/// Returns the offset (if a record was read successfully) and the
/// result of decoding. Note that `Some(offset)` is returned even if
/// there is an error decoding the data in the record, but not if
/// there was an error reading the record in the first place.
async fn try_decode(
    record: Result<RecordAndOffset, WriteBufferError>,
    shard_index: ShardIndex,
    trace_collector: Option<Arc<dyn TraceCollector>>,
) -> (Option<i64>, Result<DmlOperation, WriteBufferError>) {
    let offset = match &record {
        Ok(record) => Some(record.offset),
        Err(_) => None,
    };

    // launch a task to try and do the decode (which is CPU intensive)
    // in parallel
    let result = tokio::task::spawn(async move {
        let record = record?;
        let kafka_read_size = record.record.approximate_size();

        let headers = IoxHeaders::from_headers(record.record.headers, trace_collector.as_ref())?;

        let sequence = Sequence {
            shard_index,
            sequence_number: SequenceNumber::new(record.offset),
        };

        let timestamp = Time::from_date_time(record.record.timestamp);

        let value = record
            .record
            .value
            .ok_or_else::<WriteBufferError, _>(|| "Value missing".to_string().into())?;
        crate::codec::decode(&value, headers, sequence, timestamp, kafka_read_size)
    })
    .await;

    // Convert panics in the task to WriteBufferErrors
    let dml_result = match result {
        Err(e) => {
            warn!(%e, "Decode panic");
            // Was a join error (aka the task panic'd()
            Err(WriteBufferError::unknown(e))
        }
        // normal error in the task, use that
        Ok(res) => res,
    };

    (offset, dml_result)
}

#[async_trait]
impl WriteBufferStreamHandler for RSKafkaStreamHandler {
    async fn stream(&mut self) -> BoxStream<'static, Result<DmlOperation, WriteBufferError>> {
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
            Some(x) => StartOffset::At(x),
            None => StartOffset::Earliest,
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

        let shard_index = self.shard_index;

        // Use buffered streams to pipeline the reading of a message from kafka from with its
        // decoding.
        //
        // ┌─────┬──────┬──────┬─────┬──────┬──────┬─────┬──────┬──────┐
        // │ Read│ Read │ Read │ Read│ Read │ Read │ Read│ Read │ Read │
        // │Kafka│Kafka │Kafka │Kafka│Kafka │Kafka │Kafka│Kafka │Kafka │
        // │     │      │      │     │      │      │     │      │      │
        // └─────┴──────┴──────┴─────┴──────┴──────┴─────┴──────┴──────┘
        //
        // ┌──────────────────┐
        // │                  │
        // │      Decode      │
        // │                  │
        // └──────────────────┘
        //  ... up to 10 ..
        //    ┌──────────────────┐
        //    │                  │
        //    │      Decode      │
        //    │                  │
        //    └──────────────────┘
        //
        // ─────────────────────────────────────────────────────────────────────────▶  Time

        // this stream reads `RecordAndOffset` from kafka
        let stream = stream.map(move |res| {
            let (record, _watermark) = match res {
                Ok(x) => x,
                Err(e) => {
                    terminated.store(true, Ordering::SeqCst);
                    let kind = match e {
                        RSKafkaError::ServerError {
                            protocol_error: ProtocolError::OffsetOutOfRange,
                            // NOTE: the high watermark included in this
                            // response is always -1 when reading before/after
                            // valid offsets.
                            ..
                        } => WriteBufferErrorKind::SequenceNumberNoLongerExists,
                        _ => WriteBufferErrorKind::Unknown,
                    };
                    return Err(WriteBufferError::new(kind, e));
                }
            };
            Ok(record)
        });

        // Now decode the records in a second, parallel step by making
        // a stream of futures and [`FuturesExt::buffered`].
        let stream = stream
            .map(move |record| {
                // appease borrow checker
                let trace_collector = trace_collector.clone();
                try_decode(record, shard_index, trace_collector)
            })
            // the decode jobs in parallel
            // (`buffered` does NOT reorder, so the API user still gets an ordered stream)
            .buffered(CONCURRENT_DECODE_JOBS)
            .map(move |(offset, dml_result)| {
                // but only update the offset when a decoded recorded
                // is actually returned to the consumer of the stream
                // (not when it was decoded or when it was read from
                // kafka). This is to ensure that if a new stream is
                // created, we do not lose records that were never
                // consumed.
                //
                // Note that we update the offset as long as a record was
                // read (even if there was an error decoding) so we don't
                // get stuck on invalid records
                if let Some(offset) = offset {
                    *next_offset.lock() = Some(offset + 1);
                }
                dml_result
            });
        stream.boxed()
    }

    async fn seek(&mut self, sequence_number: SequenceNumber) -> Result<(), WriteBufferError> {
        let offset = sequence_number.get();
        let current = self.partition_client.get_offset(OffsetAt::Latest).await?;
        if offset > current {
            return Err(WriteBufferError::sequence_number_after_watermark(format!(
                "attempted to seek to offset {offset}, but current high \
                watermark for partition {p} is {current}",
                p = self.shard_index
            )));
        }

        *self.next_offset.lock() = Some(offset);
        self.terminated.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn reset_to_earliest(&mut self) {
        *self.next_offset.lock() = None;
        self.terminated.store(false, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct RSKafkaConsumer {
    partition_clients: BTreeMap<ShardIndex, Arc<PartitionClient>>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    consumer_config: ConsumerConfig,
}

impl RSKafkaConsumer {
    pub async fn new(
        conn: String,
        topic_name: String,
        connection_config: &BTreeMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        partitions: Option<Range<i32>>,
        trace_collector: Option<Arc<dyn TraceCollector>>,
    ) -> Result<Self> {
        let partition_clients = setup_topic(
            conn,
            topic_name.clone(),
            connection_config,
            creation_config,
            partitions,
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
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        self.partition_clients.keys().copied().collect()
    }

    async fn stream_handler(
        &self,
        shard_index: ShardIndex,
    ) -> Result<Box<dyn WriteBufferStreamHandler>, WriteBufferError> {
        let partition_client = self
            .partition_clients
            .get(&shard_index)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown shard index: {}", shard_index).into()
            })?;

        Ok(Box::new(RSKafkaStreamHandler {
            partition_client: Arc::clone(partition_client),
            next_offset: Arc::new(Mutex::new(None)),
            terminated: Arc::new(AtomicBool::new(false)),
            trace_collector: self.trace_collector.clone(),
            consumer_config: self.consumer_config.clone(),
            shard_index,
        }))
    }

    async fn fetch_high_watermark(
        &self,
        shard_index: ShardIndex,
    ) -> Result<SequenceNumber, WriteBufferError> {
        let partition_client = self
            .partition_clients
            .get(&shard_index)
            .ok_or_else::<WriteBufferError, _>(|| {
                format!("Unknown shard index: {}", shard_index).into()
            })?;

        let watermark = partition_client.get_offset(OffsetAt::Latest).await?;
        Ok(SequenceNumber::new(watermark))
    }

    fn type_name(&self) -> &'static str {
        "kafka"
    }
}

async fn setup_topic(
    conn: String,
    topic_name: String,
    connection_config: &BTreeMap<String, String>,
    creation_config: Option<&WriteBufferCreationConfig>,
    partitions: Option<Range<i32>>,
) -> Result<BTreeMap<ShardIndex, PartitionClient>> {
    let client_config = ClientConfig::try_from(connection_config)?;
    let mut client_builder =
        ClientBuilder::new(conn.split(',').map(|s| s.trim().to_owned()).collect());
    if let Some(client_id) = client_config.client_id {
        client_builder = client_builder.client_id(client_id);
    }
    if let Some(max_message_size) = client_config.max_message_size {
        client_builder = client_builder.max_message_size(max_message_size);
    }
    if let Some(sock5_proxy) = client_config.socks5_proxy {
        client_builder = client_builder.socks5_proxy(sock5_proxy);
    }
    let client = client_builder.build().await?;
    let controller_client = client.controller_client()?;

    loop {
        // check if topic already exists
        let topics = client.list_topics().await?;
        if let Some(topic) = topics.into_iter().find(|t| t.name == topic_name) {
            // Instantiate 10 partition clients concurrently until all are ready
            // speed up server init.
            let client_ref = &client;
            let clients = stream::iter(
                topic
                    .partitions
                    .into_iter()
                    .filter(|p| {
                        partitions
                            .as_ref()
                            .map(|want| want.contains(p))
                            .unwrap_or(true)
                    })
                    .map(|p| {
                        let topic_name = topic_name.clone();
                        async move {
                            let shard_index = ShardIndex::new(p);
                            let c = client_ref
                                .partition_client(&topic_name, p, UnknownTopicHandling::Error)
                                .await?;
                            Result::<_, WriteBufferError>::Ok((shard_index, c))
                        }
                    }),
            )
            .buffer_unordered(10)
            .try_collect::<BTreeMap<_, _>>()
            .await?;

            if let Some(p) = partitions {
                assert_eq!(
                    p.len(),
                    clients.len(),
                    "requested partition clients not initialised"
                );
            }
            return Ok(clients);
        }

        // create topic
        if let Some(creation_config) = creation_config {
            let topic_creation_config = TopicCreationConfig::try_from(creation_config)?;

            match controller_client
                .create_topic(
                    &topic_name,
                    topic_creation_config.num_partitions,
                    topic_creation_config.replication_factor,
                    topic_creation_config.timeout_ms,
                )
                .await
            {
                Ok(_) => {}
                // race condition between check and creation action, that's OK
                Err(RSKafkaError::ServerError {
                    protocol_error: ProtocolError::TopicAlreadyExists,
                    ..
                }) => {}
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
    use super::*;
    use crate::{
        core::test_utils::{
            assert_span_context_eq_or_linked, lp_to_batches, perform_generic_tests,
            random_topic_name, set_pop_first, TestAdapter, TestContext,
        },
        maybe_skip_kafka_integration,
    };
    use data_types::{DeletePredicate, NamespaceId, PartitionKey, TimestampRange};
    use dml::{test_util::assert_write_op_eq, DmlDelete, DmlWrite};
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use iox_time::TimeProvider;
    use rskafka::{client::partition::Compression, record::Record};
    use std::num::NonZeroU32;
    use test_helpers::assert_contains;
    use trace::{ctx::SpanContext, RingBufferTraceCollector};

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
            n_shards: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            RSKafkaTestContext {
                conn: self.conn.clone(),
                topic_name: random_topic_name(),
                n_shards,
                time_provider,
                trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
                metrics: metric::Registry::default(),
            }
        }
    }

    struct RSKafkaTestContext {
        conn: String,
        topic_name: String,
        n_shards: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
        trace_collector: Arc<RingBufferTraceCollector>,
        metrics: metric::Registry,
    }

    impl RSKafkaTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_shards: self.n_shards,
                ..Default::default()
            })
        }

        #[allow(dead_code)]
        fn metrics(&self) -> &metric::Registry {
            &self.metrics
        }
    }

    #[async_trait]
    impl TestContext for RSKafkaTestContext {
        type Writing = RSKafkaProducer;

        type Reading = RSKafkaConsumer;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            RSKafkaProducer::new(
                self.conn.clone(),
                self.topic_name.clone(),
                &BTreeMap::default(),
                Arc::clone(&self.time_provider),
                self.creation_config(creation_config).as_ref(),
                None,
                Some(self.trace_collector() as Arc<_>),
                &self.metrics,
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            RSKafkaConsumer::new(
                self.conn.clone(),
                self.topic_name.clone(),
                &BTreeMap::default(),
                self.creation_config(creation_config).as_ref(),
                None,
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
        let n_shards = NonZeroU32::new(2).unwrap();

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
                            n_shards,
                            ..Default::default()
                        }),
                        None,
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
        let shard_index = set_pop_first(&mut producer.shard_indexes()).unwrap();
        ClientBuilder::new(vec![conn])
            .build()
            .await
            .unwrap()
            .partition_client(
                ctx.topic_name.clone(),
                shard_index.get(),
                UnknownTopicHandling::Retry,
            )
            .await
            .unwrap()
            .produce(
                vec![Record {
                    key: None,
                    value: None,
                    headers: Default::default(),
                    timestamp: rskafka::chrono::Utc::now(),
                }],
                Compression::Zstd,
            )
            .await
            .unwrap();
        let w = crate::core::test_utils::write(
            &producer,
            "table foo=1 1",
            shard_index,
            "bananas".into(),
            None,
        )
        .await;

        let consumer = ctx.reading(true).await.unwrap();
        let mut handler = consumer.stream_handler(shard_index).await.unwrap();

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

        let shard_index = set_pop_first(&mut producer.shard_indexes()).unwrap();

        let (w1_1, w1_2, w2_1, d1_1, d1_2, w1_3, w1_4, w2_2) = tokio::join!(
            // ns1: batch 1
            write(&producer, &trace_collector, shard_index, "bananas"),
            write(&producer, &trace_collector, shard_index, "bananas"),
            // ns2: batch 1, part A
            write(&producer, &trace_collector, shard_index, "bananas"),
            // ns1: batch 2
            delete(&producer, &trace_collector, shard_index),
            // ns1: batch 3
            delete(&producer, &trace_collector, shard_index),
            // ns1: batch 4
            write(&producer, &trace_collector, shard_index, "bananas"),
            write(&producer, &trace_collector, shard_index, "bananas"),
            // ns2: batch 1, part B
            write(&producer, &trace_collector, shard_index, "bananas"),
        );

        // ensure that write operations were NOT fused
        assert_ne!(w1_1.sequence().unwrap(), w1_2.sequence().unwrap());
        assert_ne!(w1_2.sequence().unwrap(), d1_1.sequence().unwrap());
        assert_ne!(d1_1.sequence().unwrap(), d1_2.sequence().unwrap());
        assert_ne!(d1_2.sequence().unwrap(), w1_3.sequence().unwrap());
        assert_ne!(w1_3.sequence().unwrap(), w1_4.sequence().unwrap());
        assert_ne!(w1_4.sequence().unwrap(), w1_1.sequence().unwrap());

        assert_ne!(w2_1.sequence().unwrap(), w1_1.sequence().unwrap());
        assert_ne!(w2_1.sequence().unwrap(), w2_2.sequence().unwrap());

        let consumer = ctx.reading(true).await.unwrap();
        let mut handler = consumer.stream_handler(shard_index).await.unwrap();
        let mut stream = handler.stream().await;

        // get output, note that the write operations were NOT fused
        let op_w1_1 = stream.next().await.unwrap().unwrap();
        let op_w1_2 = stream.next().await.unwrap().unwrap();
        let op_w2_1 = stream.next().await.unwrap().unwrap();
        let op_d1_1 = stream.next().await.unwrap().unwrap();
        let op_d1_2 = stream.next().await.unwrap().unwrap();
        let op_w1_3 = stream.next().await.unwrap().unwrap();
        let op_w1_4 = stream.next().await.unwrap().unwrap();
        let op_w2_2 = stream.next().await.unwrap().unwrap();

        // ensure that sequence numbers map as expected
        assert_eq!(op_w1_1.meta().sequence().unwrap(), w1_1.sequence().unwrap(),);
        assert_eq!(op_w1_2.meta().sequence().unwrap(), w1_2.sequence().unwrap(),);
        assert_eq!(op_d1_1.meta().sequence().unwrap(), d1_1.sequence().unwrap(),);
        assert_eq!(op_d1_2.meta().sequence().unwrap(), d1_2.sequence().unwrap(),);
        assert_eq!(op_w1_3.meta().sequence().unwrap(), w1_3.sequence().unwrap(),);
        assert_eq!(op_w1_4.meta().sequence().unwrap(), w1_4.sequence().unwrap(),);
        assert_eq!(op_w2_1.meta().sequence().unwrap(), w2_1.sequence().unwrap(),);
        assert_eq!(op_w2_2.meta().sequence().unwrap(), w2_2.sequence().unwrap(),);

        // check tracing span links
        assert_span_context_eq_or_linked(
            w1_1.span_context().unwrap(),
            op_w1_1.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w1_2.span_context().unwrap(),
            op_w1_2.meta().span_context().unwrap(),
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
            op_w1_3.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w1_4.span_context().unwrap(),
            op_w1_4.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w2_1.span_context().unwrap(),
            op_w2_1.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
        assert_span_context_eq_or_linked(
            w2_2.span_context().unwrap(),
            op_w2_2.meta().span_context().unwrap(),
            trace_collector.spans(),
        );
    }

    async fn write(
        producer: &RSKafkaProducer,
        trace_collector: &Arc<RingBufferTraceCollector>,
        shard_index: ShardIndex,
        partition_key: impl Into<PartitionKey> + Send,
    ) -> DmlMeta {
        let span_ctx = SpanContext::new(Arc::clone(trace_collector) as Arc<_>);
        let tables = lp_to_batches("table foo=1");
        let write = DmlWrite::new(
            NamespaceId::new(42),
            tables,
            partition_key.into(),
            DmlMeta::unsequenced(Some(span_ctx)),
        );
        let op = DmlOperation::Write(write);
        producer.store_operation(shard_index, op).await.unwrap()
    }

    async fn delete(
        producer: &RSKafkaProducer,
        trace_collector: &Arc<RingBufferTraceCollector>,
        shard_index: ShardIndex,
    ) -> DmlMeta {
        let span_ctx = SpanContext::new(Arc::clone(trace_collector) as Arc<_>);
        let op = DmlOperation::Delete(DmlDelete::new(
            NamespaceId::new(42),
            DeletePredicate {
                range: TimestampRange::new(0, 1),
                exprs: vec![],
            },
            None,
            DmlMeta::unsequenced(Some(span_ctx)),
        ));
        producer.store_operation(shard_index, op).await.unwrap()
    }
}
