use crate::{
    codec::{ContentType, IoxHeaders},
    core::{WriteBufferError, WriteBufferWriting},
    kafka::WriteBufferCreationConfig,
};
use async_trait::async_trait;
use data_types::{Sequence, SequenceNumber, ShardIndex};
use dml::{DmlMeta, DmlOperation};
use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, Metric};
use observability_deps::tracing::{debug, info};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    message::{Headers, OwnedHeaders},
    producer::{FutureProducer, FutureRecord, Producer},
    types::RDKafkaErrorCode,
    util::Timeout,
    ClientConfig,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

/// Default timeout supplied to rdkafka client for kafka operations.
///
/// Chosen to be a value less than the default gRPC timeout (30
/// seconds) so we can detect kafka errors and return them prior to
/// the gRPC requests to IOx timing out.
///
/// More context in
/// <https://github.com/influxdata/influxdb_iox/issues/3029>
const KAFKA_OPERATION_TIMEOUT_MS: u64 = 20_000;

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
    producer: Arc<FutureProducer<DefaultClientContext>>,
    partitions: BTreeSet<ShardIndex>,
    enqueue: Metric<DurationHistogram>,
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
    fn shard_indexes(&self) -> BTreeSet<ShardIndex> {
        self.partitions.clone()
    }

    /// Send a [`DmlOperation`] to the write buffer using the specified shard index.
    async fn store_operation(
        &self,
        shard_index: ShardIndex,
        operation: DmlOperation,
    ) -> Result<DmlMeta, WriteBufferError> {
        // Sanity check to ensure only partitioned writes are pushed into Kafka.
        if let DmlOperation::Write(w) = &operation {
            assert!(
                w.partition_key().is_some(),
                "enqueuing unpartitioned write into kafka"
            )
        }

        // Only send writes with known shard indexes to Kafka.
        if !self.partitions.contains(&shard_index) {
            return Err(format!("Unknown shard index: {}", shard_index).into());
        }

        let kafka_partition_id = shard_index.get();

        let enqueue_start = self.time_provider.now();

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
            operation.namespace().to_string(),
        );

        let mut buf = Vec::new();
        crate::codec::encode_operation(&self.database_name, &operation, &mut buf)?;

        // This type annotation is necessary because `FutureRecord` is generic over key type, but
        // key is optional and we're not setting a key. `String` is arbitrary.
        let record: FutureRecord<'_, String, _> = FutureRecord::to(&self.database_name)
            .payload(&buf)
            .partition(kafka_partition_id)
            .timestamp(timestamp_millis)
            .headers((&headers).into());
        let kafka_write_size = estimate_message_size(
            record.payload.map(|v| v.as_ref()),
            record.key.map(|s| s.as_bytes()),
            record.headers.as_ref(),
        );

        debug!(db_name=%self.database_name, kafka_partition_id, size=buf.len(), "writing to kafka");

        let res = self.producer.send(record, Timeout::Never).await;

        if let Some(delta) = self
            .time_provider
            .now()
            .checked_duration_since(enqueue_start)
        {
            let result_attr = match &res {
                Ok(_) => "success",
                Err(_) => "error",
            };

            let attr = Attributes::from([
                ("kafka_partition", shard_index.to_string().into()),
                ("kafka_topic", self.database_name.clone().into()),
                ("result", result_attr.into()),
            ]);

            let recorder = self.enqueue.recorder(attr);
            recorder.record(delta);
        }

        let (partition, offset) = res.map_err(|(e, _owned_message)| e)?;

        debug!(db_name=%self.database_name, %offset, %partition, size=buf.len(), "wrote to kafka");

        Ok(DmlMeta::sequenced(
            Sequence::new(shard_index, SequenceNumber::new(offset)),
            timestamp,
            operation.meta().span_context().cloned(),
            kafka_write_size,
        ))
    }

    async fn flush(&self) -> Result<(), WriteBufferError> {
        let producer = Arc::clone(&self.producer);

        tokio::task::spawn_blocking(move || {
            producer.flush(Timeout::Never);
        })
        .await
        .expect("subtask failed");

        Ok(())
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
        if let Some(max_batch_size) = connection_config.get("producer_max_batch_size") {
            cfg.set("batch.size", max_batch_size);
        }
        if let Some(linger) = connection_config.get("producer_linger_ms") {
            cfg.set("linger.ms", linger);
        }
        if let Some(max_message_size) = connection_config.get("max_message_size") {
            cfg.set("message.max.bytes", max_message_size);
        }

        // these configs are set in stone
        cfg.set("bootstrap.servers", &conn);
        cfg.set("allow.auto.create.topics", "false");

        // handle auto-creation
        let partitions =
            maybe_auto_create_topics(&conn, &database_name, creation_config, &cfg).await?;

        let producer = cfg.create()?;

        let enqueue = metric_registry.register_metric::<DurationHistogram>(
            "write_buffer_client_produce_duration",
            "duration of time taken to push a set of records to kafka \
             - includes codec, protocol, and network overhead",
        );

        Ok(Self {
            conn,
            database_name,
            time_provider,
            producer: Arc::new(producer),
            partitions,
            enqueue,
        })
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

/// Get Kafka partition IDs (IOx ShardIndexes) for the database-specific Kafka topic.
///
/// Will return `None` if the topic is unknown and has to be created.
///
/// This will check that the partition is is non-empty.
async fn get_partitions(
    database_name: &str,
    cfg: &ClientConfig,
) -> Result<Option<BTreeSet<ShardIndex>>, WriteBufferError> {
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
                .map(|partition_metdata| ShardIndex::new(partition_metdata.id()))
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
/// This will create a topic with `n_sequencers` Kafka partitions.
///
/// This will NOT fail if the topic already exists! `maybe_auto_create_topics` will only call this
/// if there are no partitions. Production should always have partitions already created, so
/// `create_kafka_topic` shouldn't run in production and is only for test/dev environments.
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

/// If there are no Kafka partitions, then create a topic. Production should have Kafka partitions
/// created already, so this should only create a topic in test/dev environments.
async fn maybe_auto_create_topics(
    kafka_connection: &str,
    database_name: &str,
    creation_config: Option<&WriteBufferCreationConfig>,
    cfg: &ClientConfig,
) -> Result<BTreeSet<ShardIndex>, WriteBufferError> {
    const N_TRIES: usize = 10;

    for i in 0..N_TRIES {
        if let Some(partitions) = get_partitions(database_name, cfg).await? {
            return Ok(partitions);
        }

        // debounce after first round
        if i > 0 {
            info!(
                topic=%database_name,
                "Topic does not have partitions after creating it, wait a bit and try again."
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        if let Some(creation_config) = creation_config {
            create_kafka_topic(
                kafka_connection,
                database_name,
                creation_config.n_shards,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maybe_skip_kafka_integration;
    use iox_time::MockProvider;

    #[tokio::test]
    async fn can_parse_write_buffer_connection_config() {
        let conn = maybe_skip_kafka_integration!();

        let connection_config = BTreeMap::from([
            (
                String::from("consumer_max_batch_size"),
                String::from("5242880"),
            ),
            (String::from("consumer_max_wait_ms"), String::from("10")),
            (
                String::from("consumer_min_batch_size"),
                String::from("1048576"),
            ),
            (String::from("max_message_size"), String::from("10485760")),
            (
                String::from("producer_max_batch_size"),
                String::from("2621440"),
            ),
        ]);
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let metric_registry = Arc::new(metric::Registry::new());

        KafkaBufferProducer::new(
            conn,
            "my_db",
            &connection_config,
            Some(&WriteBufferCreationConfig::default()),
            time_provider,
            &metric_registry,
        )
        .await
        .unwrap();
    }
}
