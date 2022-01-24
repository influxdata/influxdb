use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use metric::{Metric, U64Gauge, U64Histogram, U64HistogramOptions};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, StreamConsumer},
    error::KafkaError,
    message::{Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
    util::Timeout,
    ClientConfig, ClientContext, Message, Offset, TopicPartitionList,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    convert::{TryFrom, TryInto},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use crate::{
    codec::{ContentType, IoxHeaders},
    core::{
        FetchHighWatermark, FetchHighWatermarkFut, WriteBufferError, WriteBufferReading,
        WriteBufferWriting, WriteStream,
    },
};
use data_types::{
    sequence::Sequence, server_id::ServerId, write_buffer::WriteBufferCreationConfig,
};
use dml::{DmlMeta, DmlOperation};
use observability_deps::tracing::{debug, info};
use time::{Time, TimeProvider};
use trace::TraceCollector;

/// Default timeout supplied to rdkafka client for kafka operations.
///
/// Chosen to be a value less than the default gRPC timeout (30
/// seconds) so we can detect kafka errors and return them prior to
/// the gRPC requests to IOx timing out.
///
/// More context in
/// <https://github.com/influxdata/influxdb_iox/issues/3029>
const KAFKA_OPERATION_TIMEOUT_MS: u64 = 10000;

impl From<&IoxHeaders> for OwnedHeaders {
    fn from(iox_headers: &IoxHeaders) -> Self {
        let mut res = Self::new();

        for (header, value) in iox_headers.headers() {
            res = res.add(header, value.as_ref());
        }

        res
    }
}

pub struct KafkaBufferProducer {
    conn: String,
    database_name: String,
    time_provider: Arc<dyn TimeProvider>,
    producer: FutureProducer<ClientContextImpl>,
    partitions: BTreeSet<u32>,
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
    fn sequencer_ids(&self) -> BTreeSet<u32> {
        self.partitions.clone()
    }

    /// Send an `Entry` to Kafka using the sequencer ID as a partition.
    async fn store_operation(
        &self,
        sequencer_id: u32,
        operation: &DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        let partition = i32::try_from(sequencer_id)?;

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

        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&self.database_name)
            .payload(&buf)
            .partition(partition)
            .timestamp(timestamp_millis)
            .headers((&headers).into());
        let kafka_write_size = estimate_message_size(
            record.payload.map(|v| v.as_ref()),
            record.key.map(|s| s.as_bytes()),
            record.headers.as_ref(),
        );

        debug!(db_name=%self.database_name, partition, size=buf.len(), "writing to kafka");

        let (partition, offset) = self
            .producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(e, _owned_message)| Box::new(e))?;

        debug!(db_name=%self.database_name, %offset, %partition, size=buf.len(), "wrote to kafka");

        Ok(DmlMeta::sequenced(
            Sequence::new(partition.try_into()?, offset.try_into()?),
            timestamp,
            operation.meta().span_context().cloned(),
            kafka_write_size,
        ))
    }

    fn type_name(&self) -> &'static str {
        "kafka"
    }
}

impl KafkaBufferProducer {
    pub async fn new(
        conn: impl Into<String> + Send,
        database_name: impl Into<String> + Send,
        connection_config: &BTreeMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
    ) -> Result<Self, WriteBufferError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();

        // these configs can be overwritten
        cfg.set("message.timeout.ms", "5000");
        cfg.set("message.max.bytes", "31457280");
        cfg.set("message.send.max.retries", "10");
        cfg.set("queue.buffering.max.kbytes", "31457280");
        cfg.set("request.required.acks", "all"); // equivalent to acks=-1
        cfg.set("compression.type", "snappy");
        cfg.set("statistics.interval.ms", "15000");

        // user overrides
        for (k, v) in connection_config {
            cfg.set(k, v);
        }

        // these configs are set in stone
        cfg.set("bootstrap.servers", &conn);
        cfg.set("allow.auto.create.topics", "false");

        // handle auto-creation
        let partitions =
            maybe_auto_create_topics(&conn, &database_name, creation_config, &cfg).await?;

        let context = ClientContextImpl::new(database_name.clone(), metric_registry);
        let producer: FutureProducer<ClientContextImpl> = cfg.create_with_context(context)?;

        Ok(Self {
            conn,
            database_name,
            time_provider,
            producer,
            partitions,
        })
    }
}

pub struct KafkaBufferConsumer {
    conn: String,
    database_name: String,
    consumers: BTreeMap<u32, Arc<StreamConsumer<ClientContextImpl>>>,
    trace_collector: Option<Arc<dyn TraceCollector>>,
    write_buffer_ingest_entry_size: Metric<U64Histogram>,
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

/// Iterate over the kafka messages
fn header_iter<H>(headers: Option<&H>) -> impl Iterator<Item = (&str, &[u8])>
where
    H: Headers,
{
    headers
        .into_iter()
        .flat_map(|headers| (0..headers.count()).map(|idx| headers.get(idx).unwrap()))
}

/// Estimate size of data read from kafka as payload len + key len + headers
fn estimate_message_size<H>(
    payload: Option<&[u8]>,
    key: Option<&[u8]>,
    headers: Option<&H>,
) -> usize
where
    H: Headers,
{
    payload.map(|payload| payload.len()).unwrap_or_default()
        + key.map(|key| key.len()).unwrap_or_default()
        + header_iter(headers)
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
}

#[async_trait]
impl WriteBufferReading for KafkaBufferConsumer {
    fn streams(&mut self) -> BTreeMap<u32, WriteStream<'_>> {
        let mut streams = BTreeMap::new();

        for (sequencer_id, consumer) in &self.consumers {
            let sequencer_id = *sequencer_id;
            let consumer_cloned = Arc::clone(consumer);
            let database_name = self.database_name.clone();
            let trace_collector = self.trace_collector.clone();

            // prepare a metric recorder for this sequencer
            let attributes = metric::Attributes::from([("database", database_name.clone().into())]);
            let write_buffer_ingest_entry_size =
                self.write_buffer_ingest_entry_size.recorder(attributes);

            let stream = consumer
                .stream()
                .map(move |message| {
                    let message = message?;

                    let kafka_headers = header_iter(message.headers());
                    let headers = IoxHeaders::from_headers(kafka_headers, trace_collector.as_ref())?;
                    let payload = message.payload().ok_or_else::<WriteBufferError, _>(|| {
                        "Payload missing".to_string().into()
                    })?;

                    // Estimate size of data read from kafka as
                    // payload len + key len + headers
                    let kafka_read_size = estimate_message_size(Some(payload), message.key(), message.headers());
                    write_buffer_ingest_entry_size
                        .record(kafka_read_size as u64);

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

                    let timestamp = Time::from_timestamp_millis_opt(timestamp_millis).ok_or_else::<WriteBufferError, _>(|| {
                        format!("Cannot parse timestamp for milliseconds: {}", timestamp_millis).into()
                    })?;

                    let sequence = Sequence {
                        id: message.partition().try_into()?,
                        number: message.offset().try_into()?,
                    };

                    crate::codec::decode(payload, headers, sequence, timestamp, kafka_read_size)
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
                            Duration::from_millis(KAFKA_OPERATION_TIMEOUT_MS),
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

            streams.insert(
                sequencer_id,
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
                    Duration::from_millis(KAFKA_OPERATION_TIMEOUT_MS),
                )
            })
            .await
            .expect("subtask failed")?;
        }

        Ok(())
    }

    fn type_name(&self) -> &'static str {
        "kafka"
    }
}

impl KafkaBufferConsumer {
    pub async fn new(
        conn: impl Into<String> + Send + Sync,
        server_id: ServerId,
        database_name: impl Into<String> + Send + Sync,
        connection_config: &BTreeMap<String, String>,
        creation_config: Option<&WriteBufferCreationConfig>,
        // `trace_collector` has to be a reference due to https://github.com/rust-lang/rust/issues/63033
        trace_collector: Option<&Arc<dyn TraceCollector>>,
        metric_registry: &metric::Registry,
    ) -> Result<Self, WriteBufferError> {
        let conn = conn.into();
        let database_name = database_name.into();

        let mut cfg = ClientConfig::new();

        // these configs can be overwritten
        cfg.set("session.timeout.ms", "6000");
        cfg.set("statistics.interval.ms", "15000");
        cfg.set("queued.max.messages.kbytes", "10000");

        // user overrides
        for (k, v) in connection_config {
            cfg.set(k, v);
        }

        // these configs are set in stone
        cfg.set("bootstrap.servers", &conn);
        cfg.set("enable.auto.commit", "false");
        cfg.set("allow.auto.create.topics", "false");

        // Create a unique group ID for this database's consumer as we don't want to create
        // consumer groups.
        cfg.set("group.id", &format!("{}-{}", server_id, database_name));

        // When subscribing without a partition offset, start from the smallest offset available.
        cfg.set("auto.offset.reset", "smallest");

        // figure out which partitions exists
        let partitions =
            maybe_auto_create_topics(&conn, &database_name, creation_config, &cfg).await?;
        info!(%database_name, ?partitions, "found Kafka partitions");

        // setup a single consumer per partition, at least until https://github.com/fede1024/rust-rdkafka/pull/351 is
        // merged
        let consumers = partitions
            .into_iter()
            .map(|partition| {
                let context = ClientContextImpl::new(database_name.clone(), metric_registry);
                let consumer: StreamConsumer<ClientContextImpl> =
                    cfg.create_with_context(context)?;

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
            .collect::<Result<BTreeMap<u32, Arc<StreamConsumer<ClientContextImpl>>>, KafkaError>>(
            )?;

        let write_buffer_ingest_entry_size: Metric<U64Histogram> = metric_registry
            .register_metric_with_options(
                "write_buffer_ingest_entry_size",
                "distribution of ingested Kafka message sizes",
                || {
                    U64HistogramOptions::new([
                        1024,
                        16 * 1024,
                        256 * 1024,
                        768 * 1024,
                        1024 * 1024,
                        3 * 1024 * 1024,
                        10 * 1024 * 1024,
                        u64::MAX,
                    ])
                },
            );

        Ok(Self {
            conn,
            database_name,
            consumers,
            trace_collector: trace_collector.map(Arc::clone),
            write_buffer_ingest_entry_size,
        })
    }
}

/// Get partition IDs for the database-specific Kafka topic.
///
/// Will return `None` if the topic is unknown and has to be created.
///
/// This will check that the partition is is non-empty.
async fn get_partitions(
    database_name: &str,
    cfg: &ClientConfig,
) -> Result<Option<BTreeSet<u32>>, WriteBufferError> {
    let database_name = database_name.to_string();
    let cfg = cfg.clone();

    let metadata = tokio::task::spawn_blocking(move || {
        let probe_consumer: BaseConsumer = cfg.create()?;

        probe_consumer.fetch_metadata(
            Some(&database_name),
            Duration::from_millis(KAFKA_OPERATION_TIMEOUT_MS),
        )
    })
    .await
    .expect("subtask failed")?;

    let topic_metadata = metadata.topics().get(0).expect("requested a single topic");

    match topic_metadata.error() {
        None => {
            let partitions: BTreeSet<_> = topic_metadata
                .partitions()
                .iter()
                .map(|partition_metdata| partition_metdata.id().try_into().unwrap())
                .collect();

            if partitions.is_empty() {
                Err("Topic exists but has no partitions".to_string().into())
            } else {
                Ok(Some(partitions))
            }
        }
        Some(error_code) => {
            let error_code: RDKafkaErrorCode = error_code.into();
            match error_code {
                RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::UnknownTopicOrPartition => {
                    // The caller is responsible for creating the topic, so this is somewhat OK.
                    Ok(None)
                }
                _ => Err(KafkaError::MetadataFetch(error_code).into()),
            }
        }
    }
}

fn admin_client(kafka_connection: &str) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", kafka_connection);
    cfg.set("message.timeout.ms", "5000");
    cfg.create()
}

/// Create Kafka topic based on the provided configs.
///
/// This is create a topic with `n_sequencers` partitions.
///
/// This will NOT fail if the topic already exists!
async fn create_kafka_topic(
    kafka_connection: &str,
    database_name: &str,
    n_sequencers: NonZeroU32,
    cfg: &BTreeMap<String, String>,
) -> Result<(), WriteBufferError> {
    let admin = admin_client(kafka_connection)?;

    let mut topic = NewTopic::new(
        database_name,
        n_sequencers.get() as i32,
        TopicReplication::Fixed(1),
    );
    for (k, v) in cfg {
        topic = topic.set(k, v);
    }

    let opts = AdminOptions::default();
    let mut results = admin.create_topics([&topic], &opts).await?;
    assert_eq!(results.len(), 1, "created exactly one topic");
    let result = results.pop().expect("just checked the vector length");
    match result {
        Ok(topic) | Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
            assert_eq!(topic, database_name);
            Ok(())
        }
        Err((topic, code)) => {
            assert_eq!(topic, database_name);
            Err(format!("Cannot create topic '{}': {}", topic, code).into())
        }
    }
}

async fn maybe_auto_create_topics(
    kafka_connection: &str,
    database_name: &str,
    creation_config: Option<&WriteBufferCreationConfig>,
    cfg: &ClientConfig,
) -> Result<BTreeSet<u32>, WriteBufferError> {
    const N_TRIES: usize = 10;

    for i in 0..N_TRIES {
        if let Some(partitions) = get_partitions(database_name, cfg).await? {
            return Ok(partitions);
        }

        // debounce after first round
        if i > 0 {
            info!(topic=%database_name, "Topic does not have partitions after creating it, wait a bit and try again.");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        if let Some(creation_config) = creation_config {
            create_kafka_topic(
                kafka_connection,
                database_name,
                creation_config.n_sequencers,
                &creation_config.options,
            )
            .await?;
        } else {
            return Err("no partitions found and auto-creation not requested"
                .to_string()
                .into());
        }
    }

    Err(format!("Could not auto-create topic after {} tries.", N_TRIES).into())
}

/// Our own implementation of [`ClientContext`] to overwrite certain logging behavior.
struct ClientContextImpl {
    database_name: String,
    producer_queue_msg_count: Metric<U64Gauge>,
    producer_queue_msg_bytes: Metric<U64Gauge>,
    producer_queue_max_msg_count: Metric<U64Gauge>,
    producer_queue_max_msg_bytes: Metric<U64Gauge>,
    tx_bytes: Metric<U64Gauge>,
    rx_bytes: Metric<U64Gauge>,
    consumer_lag: Metric<U64Gauge>,
}

impl ClientContextImpl {
    fn new(database_name: String, metric_registry: &metric::Registry) -> Self {
        Self {
            database_name,
            producer_queue_msg_count: metric_registry.register_metric(
                "kafka_producer_queue_msg_count",
                "The current number of messages in producer queues.",
            ),
            producer_queue_msg_bytes: metric_registry.register_metric(
                "kafka_producer_queue_msg_bytes",
                "The current total size of messages in producer queues",
            ),
            producer_queue_max_msg_count: metric_registry.register_metric(
                "kafka_producer_queue_max_msg_count",
                "The maximum number of messages allowed in the producer queues.",
            ),
            producer_queue_max_msg_bytes: metric_registry.register_metric(
                "kafka_producer_queue_max_msg_bytes",
                "The maximum total size of messages allowed in the producer queues.",
            ),
            tx_bytes: metric_registry.register_metric(
                "kafka_tx_bytes",
                "The total number of bytes transmitted to brokers.",
            ),
            rx_bytes: metric_registry.register_metric(
                "kafka_rx_bytes",
                "The total number of bytes received from brokers.",
            ),
            consumer_lag: metric_registry.register_metric(
                "kafka_consumer_lag",
                "The difference between `hi_offset` and `max(app_offset,committed_offset)`.",
            ),
        }
    }
}

impl ClientContext for ClientContextImpl {
    fn stats(&self, statistics: rdkafka::Statistics) {
        let attributes = metric::Attributes::from([
            ("database", self.database_name.clone().into()),
            ("client_type", statistics.client_type.into()),
        ]);

        self.producer_queue_msg_count
            .recorder(attributes.clone())
            .set(statistics.msg_cnt as u64);
        self.producer_queue_max_msg_count
            .recorder(attributes.clone())
            .set(statistics.msg_max as u64);

        self.producer_queue_msg_bytes
            .recorder(attributes.clone())
            .set(statistics.msg_size as u64);
        self.producer_queue_max_msg_bytes
            .recorder(attributes.clone())
            .set(statistics.msg_size_max as u64);

        self.tx_bytes
            .recorder(attributes.clone())
            .set(statistics.tx_bytes as u64);
        self.rx_bytes
            .recorder(attributes.clone())
            .set(statistics.rx_bytes as u64);

        for topic in statistics.topics.into_values() {
            let attributes = {
                let mut tmp = attributes.clone();
                tmp.insert("topic", topic.topic);
                tmp
            };

            for partition in topic.partitions.values() {
                let attributes = {
                    let mut tmp = attributes.clone();
                    tmp.insert("partition", partition.partition.to_string());
                    tmp
                };

                self.consumer_lag
                    .recorder(attributes)
                    .set(partition.consumer_lag as u64);
            }
        }
    }
}

impl ConsumerContext for ClientContextImpl {}

pub mod test_utils {
    use super::admin_client;
    use rdkafka::admin::{AdminOptions, AlterConfig, ResourceSpecifier};
    use std::{collections::BTreeMap, time::Duration};

    /// Create topic creation config that is ideal for testing and works with [`purge_kafka_topic`]
    pub fn kafka_sequencer_options() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("cleanup.policy".to_string(), "delete".to_string()),
            ("retention.ms".to_string(), "-1".to_string()),
            ("segment.ms".to_string(), "10".to_string()),
        ])
    }

    /// Purge all records from given topic.
    ///
    /// **WARNING: Until <https://github.com/fede1024/rust-rdkafka/issues/385> is fixed, this requires a server-wide
    ///            `log.retention.check.interval.ms` of 100ms!**
    pub async fn purge_kafka_topic(kafka_connection: &str, database_name: &str) {
        let admin = admin_client(kafka_connection).unwrap();
        let opts = AdminOptions::default();

        let cfg =
            AlterConfig::new(ResourceSpecifier::Topic(database_name)).set("retention.ms", "1");
        admin.alter_configs([&cfg], &opts).await.unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;

        let cfg =
            AlterConfig::new(ResourceSpecifier::Topic(database_name)).set("retention.ms", "-1");
        let mut results = admin.alter_configs([&cfg], &opts).await.unwrap();
        assert_eq!(results.len(), 1, "created exactly one topic");
        let result = results.pop().expect("just checked the vector length");
        result.unwrap();
    }
}

/// Kafka tests (only run when in integration test mode and kafka is running).
/// see [`crate::maybe_skip_kafka_integration`] for more details.
#[cfg(test)]
mod tests {
    use super::{test_utils::kafka_sequencer_options, *};
    use crate::{
        codec::HEADER_CONTENT_TYPE,
        core::test_utils::{
            map_pop_first, perform_generic_tests, random_topic_name, set_pop_first,
            write as write_to_writer, TestAdapter, TestContext,
        },
        maybe_skip_kafka_integration,
    };
    use std::{
        num::NonZeroU32,
        sync::atomic::{AtomicU32, Ordering},
    };
    use time::TimeProvider;
    use trace::{RingBufferTraceCollector, TraceCollector};

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

        async fn new_context_with_time(
            &self,
            n_sequencers: NonZeroU32,
            time_provider: Arc<dyn TimeProvider>,
        ) -> Self::Context {
            KafkaTestContext {
                conn: self.conn.clone(),
                database_name: random_topic_name(),
                server_id_counter: AtomicU32::new(1),
                n_sequencers,
                time_provider,
                metric_registry: metric::Registry::new(),
            }
        }
    }

    struct KafkaTestContext {
        conn: String,
        database_name: String,
        server_id_counter: AtomicU32,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: metric::Registry,
    }

    impl KafkaTestContext {
        fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
            value.then(|| WriteBufferCreationConfig {
                n_sequencers: self.n_sequencers,
                options: kafka_sequencer_options(),
            })
        }

        fn connection_config(&self) -> BTreeMap<String, String> {
            BTreeMap::from([
                // WARNING: Don't set `statistics.interval.ms` to a too lower value, otherwise rdkafka will become
                // overloaded and will not keep up delivering the statistics, leading to very long or infinite thread
                // blocking during process shutdown.
                ("statistics.interval.ms".to_owned(), "1000".to_owned()),
            ])
        }
    }

    #[async_trait]
    impl TestContext for KafkaTestContext {
        type Writing = KafkaBufferProducer;

        type Reading = KafkaBufferConsumer;

        async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
            KafkaBufferProducer::new(
                &self.conn,
                &self.database_name,
                &self.connection_config(),
                self.creation_config(creation_config).as_ref(),
                Arc::clone(&self.time_provider),
                &self.metric_registry,
            )
            .await
        }

        async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
            let server_id = self.server_id_counter.fetch_add(1, Ordering::SeqCst);
            let server_id = ServerId::try_from(server_id).unwrap();

            let collector: Arc<dyn TraceCollector> = Arc::new(RingBufferTraceCollector::new(5));

            KafkaBufferConsumer::new(
                &self.conn,
                server_id,
                &self.database_name,
                &self.connection_config(),
                self.creation_config(creation_config).as_ref(),
                Some(&collector),
                &self.metric_registry,
            )
            .await
        }
    }

    #[tokio::test]
    async fn test_generic() {
        let conn = maybe_skip_kafka_integration!();

        perform_generic_tests(KafkaTestAdapter::new(conn)).await;
    }

    #[tokio::test]
    async fn topic_create_twice() {
        let conn = maybe_skip_kafka_integration!();
        let database_name = random_topic_name();

        create_kafka_topic(
            &conn,
            &database_name,
            NonZeroU32::try_from(1).unwrap(),
            &kafka_sequencer_options(),
        )
        .await
        .unwrap();

        create_kafka_topic(
            &conn,
            &database_name,
            NonZeroU32::try_from(1).unwrap(),
            &kafka_sequencer_options(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn error_no_payload() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let headers = IoxHeaders::new(ContentType::Protobuf, None);
        let mut owned_headers = OwnedHeaders::new();
        for (name, value) in headers.headers() {
            owned_headers = owned_headers.add(name, value.as_bytes());
        }

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let record: FutureRecord<'_, String, [u8]> = FutureRecord::to(&writer.database_name)
            .partition(partition)
            .headers(owned_headers);
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        let err = stream.stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "Payload missing");
    }

    #[tokio::test]
    async fn content_type_header_missing() {
        // Fallback for now https://github.com/influxdata/influxdb_iox/issues/2805
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&writer.database_name)
            .payload(&[0])
            .partition(partition);
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        let err = stream.stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "No content type header");
    }

    #[tokio::test]
    async fn content_type_header_unknown() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let writer = ctx.writing(true).await.unwrap();
        let partition = set_pop_first(&mut writer.sequencer_ids()).unwrap() as i32;
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&writer.database_name)
            .payload(&[0])
            .partition(partition)
            .headers(OwnedHeaders::new().add(HEADER_CONTENT_TYPE, "foo"));
        writer.producer.send(record, Timeout::Never).await.unwrap();

        let mut reader = ctx.reading(true).await.unwrap();
        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
        let err = stream.stream.next().await.unwrap().unwrap_err();
        assert_eq!(err.to_string(), "Unknown message format: foo");
    }

    #[tokio::test]
    async fn metrics() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);
        let ctx = adapter.new_context(NonZeroU32::new(1).unwrap()).await;

        let reader = ctx.reading(true).await.unwrap();

        // It seems that th reader must be used for rdkafka / rdkafka-rs to do anything at all =/
        let background_task = tokio::spawn(async move {
            let mut reader = reader;
            let mut streams = reader.streams();
            assert_eq!(streams.len(), 1);
            let (_sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();
            stream.stream.next().await;
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(instrument) = ctx
                    .metric_registry
                    .get_instrument::<Metric<U64Gauge>>("kafka_rx_bytes")
                {
                    if let Some(observer) = instrument.get_observer(&metric::Attributes::from([
                        ("database", ctx.database_name.clone().into()),
                        ("client_type", "consumer".into()),
                    ])) {
                        let observation = observer.fetch();
                        assert_ne!(observation, 0);
                        break;
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap();

        background_task.abort();
    }

    #[tokio::test]
    async fn test_ingest_metrics() {
        let conn = maybe_skip_kafka_integration!();
        let adapter = KafkaTestAdapter::new(conn);

        let context = adapter.new_context(NonZeroU32::try_from(1).unwrap()).await;

        let entry_1 = "upc user=1 100";

        let writer = context.writing(true).await.unwrap();
        let mut reader = context.reading(true).await.unwrap();

        let mut streams = reader.streams();
        assert_eq!(streams.len(), 1);
        let (sequencer_id, mut stream) = map_pop_first(&mut streams).unwrap();

        // Send some data into the buffer and read it out
        write_to_writer(&writer, entry_1, sequencer_id, None).await;
        stream.stream.next().await.unwrap().unwrap();

        let metric: Metric<U64Histogram> = context
            .metric_registry
            .get_instrument("write_buffer_ingest_entry_size")
            .unwrap();

        let observation = metric
            .get_observer(&metric::Attributes::from([(
                "database",
                context.database_name.clone().into(),
            )]))
            .unwrap()
            .fetch();

        assert_eq!(observation.total, 197, "Observation: {:#?}", observation);
        assert_eq!(
            observation.buckets.len(),
            8,
            "Observation: {:#?}",
            observation
        );
        assert_eq!(
            observation.buckets[0],
            metric::ObservationBucket { le: 1024, count: 1 },
            "Observation: {:#?}",
            observation
        );

        // should be no other observations in this histogram
        observation
            .buckets
            .iter()
            .skip(1)
            .for_each(|bucket| assert_eq!(bucket.count, 0, "{:#?}", observation));
    }
}
